# Tier 2 packaging — one-time setup

**Goal**: users run `sudo apt install pyrosql-driver` / `sudo dnf install
pyrosql-driver` against our GitHub-Pages-hosted repo — no vendor infra.

`.github/workflows/release.yml` runs on every `v*` tag and:

1. Publishes to crates.io (Rust) and PyPI (Python) — already working.
2. Builds `.so` / `.dylib` / `.dll` FFI binaries for Linux/macOS/Windows.
3. Builds `.deb` (amd64 + arm64) and `.rpm` (x86_64 + aarch64).
4. Attaches everything to the GitHub Release.
5. Regenerates apt + yum metadata, signs with GPG, deploys to `gh-pages`.

The package is **`pyrosql-driver`** — the client-side C-ABI shared library
(`libpyrosql_ffi_pwire.so`). The server binary lives in a separate repo and
is NOT part of this package.

## One-time admin setup

### 1. Generate the release signing key

On a trusted workstation (NOT in CI):

```bash
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
# Note the long key id (hex after `sec rsa4096/`).
```

Add a passphrase (`gpg --edit-key info@pyrosql.com` → `passwd`), then:

```bash
gpg --armor --export-secret-keys info@pyrosql.com > pyrosql-signing.private.asc
gpg --armor --export             info@pyrosql.com > pyrosql-signing.public.asc
```

Keep the private key offline. It is THE signature your users verify.

### 2. GitHub Secrets

Repository → Settings → Secrets and variables → Actions:

| Secret | Value |
|---|---|
| `GPG_PRIVATE_KEY` | Full `pyrosql-signing.private.asc` content (including header + footer). |
| `GPG_PASSPHRASE`  | The passphrase from step 1. |
| `GPG_KEY_ID`      | Long-form key id (16 hex chars). |

### 3. Enable GitHub Pages

Settings → Pages → Source = **Deploy from a branch** → Branch `gh-pages` / `(root)`.

After the first release pushes to `gh-pages`, the repo is served at:
`https://pyrosql.github.io/pyrosql-driver/`.

### 4. Tag a release to test

```bash
git tag v1.2.0
git push origin v1.2.0
```

Watch the Actions tab. When `release.yml` finishes:

- GitHub Release has tarballs, wheels, `.deb`, `.rpm` attached.
- `gh-pages` branch has `apt/`, `yum/`, `gpg.key`, `index.html`.

### 5. Verify from a clean Ubuntu/Fedora box

```bash
# Ubuntu 22.04+ / Debian 12+
curl -fsSL https://pyrosql.github.io/pyrosql-driver/gpg.key \
    | sudo gpg --dearmor -o /usr/share/keyrings/pyrosql.gpg
echo "deb [signed-by=/usr/share/keyrings/pyrosql.gpg] https://pyrosql.github.io/pyrosql-driver/apt stable main" \
    | sudo tee /etc/apt/sources.list.d/pyrosql.list
sudo apt update
sudo apt install pyrosql-driver
ls /usr/lib/libpyrosql_ffi_pwire.so     # shared lib landed
```

```bash
# Fedora / RHEL / Rocky / Alma
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

## Package matrix

Every release tag publishes:

| Package | Arch | Contents |
|---|---|---|
| `pyrosql-driver_<ver>_amd64.deb`    | linux x86_64 | `/usr/lib/libpyrosql_ffi_pwire.so` |
| `pyrosql-driver_<ver>_arm64.deb`    | linux arm64  | same |
| `pyrosql-driver-<ver>.x86_64.rpm`   | linux x86_64 | same |
| `pyrosql-driver-<ver>.aarch64.rpm`  | linux arm64  | same |

Non-apt/yum channels (separate release steps, already wired):

| Channel | Command | Contents |
|---|---|---|
| crates.io | `cargo add pyrosql` | Rust driver source |
| PyPI | `pip install pyrosql` | Python wheel |
| GitHub Release | browse releases | raw `.so` / `.dylib` / `.dll` + wheels |

Future channels (not yet wired):

| Channel | Gap |
|---|---|
| Homebrew tap `pyrosql/homebrew-pyrosql` | `brew install pyrosql-driver` → needs separate tap repo + bump script |
| Scoop bucket `pyrosql/scoop-pyrosql` | `scoop install pyrosql-driver` for Windows |
| `libpyrosql-dev` Debian package with C header | needs `cbindgen` step + header asset |

## Rotating the signing key

When the key expires (3 years):

1. Generate a new key (step 1 here).
2. Publish BOTH old + new public keys in `gpg.key` during a 60-day overlap.
3. After overlap drop the old key.

## Cost

Zero. GitHub Pages free tier (100 GB/mo bandwidth, 1 GB repo) covers this.

If traffic grows beyond the free tier, move the same `gh-pages` tree to
S3+CloudFront or Cloudflare R2 behind `apt.pyrosql.com` / `yum.pyrosql.com`.
Only the final deploy step in the workflow changes.
