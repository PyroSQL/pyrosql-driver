# Tier 2 packaging — one-time setup (DONE)

**Status as of 2026-04-21**: fully provisioned on `PyroSQL/pyrosql-driver`.
This doc stays as a rebuild guide for disaster recovery and future
reference.

## What's live

| Item | Value |
|---|---|
| Pages URL | https://pyrosql.github.io/pyrosql-driver/ |
| GPG signing key id | `8C37C73F7A57404E` |
| GPG user id | `Una Partida Mas SRL - PyroSQL release signing <info@pyrosql.com>` |
| Key algorithm | RSA 4096 |
| Key expiry | 3 years from 2026-04-21 |
| Pages source | branch `gh-pages` / path `/` |
| Secrets registered | `GPG_PRIVATE_KEY`, `GPG_PASSPHRASE` (empty), `GPG_KEY_ID` |

Passphrase is deliberately empty — the private key is protected by GitHub
Secrets at-rest encryption, and an empty passphrase is simpler and
equivalent in security posture for this use case. If in doubt, rotate
using the procedure below and set a real passphrase.

## Dual-track packaging

Every `v*` tag publishes two tracks so users can pick the right glibc
floor for their system:

| Track | Built in container | glibc | Linux from | Recommended for |
|---|---|---|---|---|
| `stable` | `manylinux_2_28` | 2.28 | 2018+ | Modern systems — default |
| `stable-legacy` | `manylinux2014` | 2.17 | 2012+ | 10+ year-old boxes — RHEL 7, CentOS 7, Ubuntu 14.04–18.04 |

The binary code is identical (same Rust, same LLVM) — only the linking
floor differs. Users on modern systems get `stable`; users on older
boxes get `stable-legacy`. No performance difference; the choice is
purely about which systems can load the shared library at all.

## Install snippets (for copy-paste into README)

### Modern Debian / Ubuntu

```bash
curl -fsSL https://pyrosql.github.io/pyrosql-driver/gpg.key \
    | sudo gpg --dearmor -o /usr/share/keyrings/pyrosql.gpg
echo "deb [signed-by=/usr/share/keyrings/pyrosql.gpg] https://pyrosql.github.io/pyrosql-driver/apt/stable stable main" \
    | sudo tee /etc/apt/sources.list.d/pyrosql.list
sudo apt update
sudo apt install pyrosql-driver
```

### Legacy Debian / Ubuntu (10+ year-old boxes)

Same as above but substitute `apt/stable-legacy` for `apt/stable`.

### Modern Fedora / RHEL / Rocky / Alma

```bash
sudo tee /etc/yum.repos.d/pyrosql.repo <<EOF
[pyrosql]
name=PyroSQL
baseurl=https://pyrosql.github.io/pyrosql-driver/yum/stable/\$basearch/
enabled=1
gpgcheck=1
gpgkey=https://pyrosql.github.io/pyrosql-driver/gpg.key
EOF
sudo dnf install pyrosql-driver
```

### Legacy RHEL 7 / CentOS 7

Same as above but substitute `yum/stable-legacy/` for `yum/stable/`.

## Package matrix (per release tag)

8 binary packages per release:

| Package file | Track | Arch | Target glibc |
|---|---|---|---|
| `pyrosql-driver_<v>_amd64_stable.deb`    | stable        | linux x86_64 | 2.28 |
| `pyrosql-driver_<v>_arm64_stable.deb`    | stable        | linux arm64  | 2.28 |
| `pyrosql-driver_<v>_amd64_stable-legacy.deb` | stable-legacy | linux x86_64 | 2.17 |
| `pyrosql-driver_<v>_arm64_stable-legacy.deb` | stable-legacy | linux arm64  | 2.17 |
| `pyrosql-driver-<v>.x86_64.stable.rpm`         | stable        | linux x86_64 | 2.28 |
| `pyrosql-driver-<v>.aarch64.stable.rpm`        | stable        | linux arm64  | 2.28 |
| `pyrosql-driver-<v>.x86_64.stable-legacy.rpm`  | stable-legacy | linux x86_64 | 2.17 |
| `pyrosql-driver-<v>.aarch64.stable-legacy.rpm` | stable-legacy | linux arm64  | 2.17 |

Plus existing channels (unchanged):

| Channel | Command | Contents |
|---|---|---|
| crates.io | `cargo add pyrosql` | Rust driver source |
| PyPI | `pip install pyrosql` | Python wheel |
| GitHub Release | browse releases | All of the above plus raw `.so` / `.dylib` / `.dll` / `.whl` |

## Original setup procedure (for disaster recovery / new admins)

### 1. Generate the release signing key

```bash
mkdir -p /tmp/pyrosql-gpg && export GNUPGHOME=/tmp/pyrosql-gpg && chmod 700 $GNUPGHOME
cat > /tmp/gpg-batch <<'EOF'
%no-protection
Key-Type: RSA
Key-Length: 4096
Subkey-Type: RSA
Subkey-Length: 4096
Name-Real: Una Partida Mas SRL - PyroSQL release signing
Name-Email: info@pyrosql.com
Expire-Date: 3y
%commit
EOF
gpg --batch --generate-key /tmp/gpg-batch
gpg --list-secret-keys --keyid-format LONG info@pyrosql.com
# Note the key id (hex after `sec rsa4096/`).

gpg --armor --export-secret-keys info@pyrosql.com > pyrosql-signing.private.asc
gpg --armor --export             info@pyrosql.com > pyrosql-signing.public.asc
```

Keep the private key offline. It is THE signature your users verify.

### 2. GitHub Secrets (via gh CLI or Web UI)

```bash
gh secret set GPG_PRIVATE_KEY --repo PyroSQL/pyrosql-driver < pyrosql-signing.private.asc
printf "" | gh secret set GPG_PASSPHRASE --repo PyroSQL/pyrosql-driver
printf "<KEY_ID>" | gh secret set GPG_KEY_ID --repo PyroSQL/pyrosql-driver
```

### 3. Create gh-pages branch

```bash
mkdir bootstrap && cd bootstrap
git init --initial-branch=gh-pages
echo '# PyroSQL apt/yum repo' > README.md
git add README.md
git -c user.email="info@pyrosql.com" -c user.name="PyroSQL release bot" \
    commit -m "bootstrap"
git remote add origin https://github.com/PyroSQL/pyrosql-driver.git
git push -u origin gh-pages
```

### 4. Enable GitHub Pages

```bash
gh api --method POST /repos/PyroSQL/pyrosql-driver/pages \
    -f 'source[branch]=gh-pages' -f 'source[path]=/'
```

Or Web: Settings → Pages → Source = Deploy from branch `gh-pages` / `(root)`.

### 5. Tag a release to test

```bash
git tag v1.2.0
git push origin v1.2.0
```

## Rotating the signing key

When the key expires (3 years from 2026-04-21):

1. Generate a new key (step 1 above).
2. Publish BOTH old + new public keys in `site/gpg.key` for a 60-day overlap.
3. After overlap drop the old key from `gpg.key`.

## Cost

Zero. GitHub Pages free tier covers it.
