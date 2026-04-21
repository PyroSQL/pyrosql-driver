#!/usr/bin/env python3
"""Generate the install-instructions page served from GitHub Pages.

Reads env `PAGES_URL` (expected `<org>.github.io/<repo>`) and emits
the HTML for stdout.  Called from `.github/workflows/release.yml`'s
`publish-apt-yum` job.  Standalone so we avoid the yaml-in-bash
heredoc indentation landmine.
"""

import os
import sys

url = os.environ.get(
    "PAGES_URL", "pyrosql.github.io/pyrosql-driver"
)
repo = os.environ.get(
    "GITHUB_REPOSITORY", "PyroSQL/pyrosql-driver"
)

html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Install PyroSQL driver · apt / dnf / zypper</title>
<style>
  :root {{ --fg:#1a1a1a; --muted:#6b6b6b; --bg:#fff; --accent:#ff5a1f; --code-bg:#f4f4f4; --border:#e5e5e5; }}
  * {{ box-sizing:border-box; }}
  body {{ font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif; color:var(--fg); background:var(--bg); line-height:1.55; margin:0; padding:0 16px 80px; max-width:900px; margin-left:auto; margin-right:auto; }}
  header {{ padding:48px 0 32px; border-bottom:1px solid var(--border); margin-bottom:32px; }}
  h1 {{ font-size:2.2rem; margin:0 0 8px; letter-spacing:-0.02em; }}
  h1 span.orange {{ color:var(--accent); }}
  header p {{ color:var(--muted); margin:0; font-size:1.05rem; }}
  h2 {{ font-size:1.3rem; margin-top:48px; margin-bottom:12px; border-bottom:1px solid var(--border); padding-bottom:8px; }}
  h3 {{ font-size:1.05rem; margin-top:28px; margin-bottom:8px; }}
  pre {{ background:var(--code-bg); border:1px solid var(--border); border-radius:6px; padding:14px 16px; overflow-x:auto; font-size:0.87rem; line-height:1.5; }}
  code {{ background:var(--code-bg); padding:1px 5px; border-radius:3px; font-size:0.9em; }}
  pre code {{ background:none; padding:0; }}
  .badge {{ display:inline-block; padding:2px 8px; border-radius:3px; font-size:0.75rem; font-weight:600; text-transform:uppercase; letter-spacing:0.03em; margin-right:6px; }}
  .badge-modern {{ background:#e6f4ea; color:#1e7e34; }}
  .badge-legacy {{ background:#fff3cd; color:#856404; }}
  .note {{ background:#f0f7ff; border-left:3px solid #0969da; padding:12px 16px; margin:16px 0; border-radius:3px; font-size:0.92rem; }}
  .note strong {{ color:#0969da; }}
  table {{ border-collapse:collapse; width:100%; margin:16px 0; font-size:0.9rem; }}
  th, td {{ text-align:left; padding:8px 12px; border-bottom:1px solid var(--border); }}
  th {{ background:#fafafa; font-weight:600; }}
  a {{ color:var(--accent); }}
  footer {{ margin-top:64px; padding-top:24px; border-top:1px solid var(--border); color:var(--muted); font-size:0.85rem; }}
</style>
</head>
<body>

<header>
  <h1><span class="orange">PyroSQL</span> driver &middot; install</h1>
  <p>PostgreSQL-wire-compatible database client. Link your application against <code>libpyrosql_ffi_pwire.so</code> and connect to any PyroSQL server.</p>
</header>

<div class="note">
  <strong>Which track do I choose?</strong><br>
  <span class="badge badge-modern">modern</span> works on every Linux from <strong>2018 or newer</strong> (Ubuntu 20.04+, Debian 11+, RHEL 8+, Fedora, Amazon Linux 2023). Pick this if you&rsquo;re on a current system.<br>
  <span class="badge badge-legacy">legacy</span> works all the way back to <strong>2012</strong> (RHEL 7, CentOS 7, Ubuntu 14.04+, Amazon Linux 2). Pick this only if the modern track fails with a glibc error on install.<br>
  Both tracks ship the same code with the same performance &mdash; only the minimum glibc version differs.
</div>

<h2>Debian &middot; Ubuntu &middot; Linux Mint &middot; Pop!_OS &middot; elementary OS</h2>

<h3><span class="badge badge-modern">modern</span> Ubuntu 20.04+ / Debian 11+</h3>
<pre><code>curl -fsSL https://{url}/gpg.key | sudo gpg --dearmor -o /usr/share/keyrings/pyrosql.gpg
echo "deb [signed-by=/usr/share/keyrings/pyrosql.gpg] https://{url}/apt/stable stable main" | sudo tee /etc/apt/sources.list.d/pyrosql.list
sudo apt update
sudo apt install pyrosql-driver</code></pre>

<h3><span class="badge badge-legacy">legacy</span> Ubuntu 14.04 &ndash; 18.04 / Debian 8 &ndash; 10</h3>
<pre><code>curl -fsSL https://{url}/gpg.key | sudo gpg --dearmor -o /usr/share/keyrings/pyrosql.gpg
echo "deb [signed-by=/usr/share/keyrings/pyrosql.gpg] https://{url}/apt/stable-legacy stable main" | sudo tee /etc/apt/sources.list.d/pyrosql.list
sudo apt update
sudo apt install pyrosql-driver</code></pre>

<h2>Fedora &middot; RHEL &middot; Rocky &middot; Alma &middot; CentOS Stream &middot; Amazon Linux</h2>

<h3><span class="badge badge-modern">modern</span> RHEL 8+ / Rocky 8+ / Alma 8+ / Fedora / Amazon Linux 2023</h3>
<pre><code>sudo tee /etc/yum.repos.d/pyrosql.repo &lt;&lt;'EOF'
[pyrosql]
name=PyroSQL
baseurl=https://{url}/yum/stable/$basearch/
enabled=1
gpgcheck=1
gpgkey=https://{url}/gpg.key
EOF
sudo dnf install pyrosql-driver</code></pre>

<h3><span class="badge badge-legacy">legacy</span> RHEL 7 / CentOS 7 / Amazon Linux 2</h3>
<pre><code>sudo tee /etc/yum.repos.d/pyrosql.repo &lt;&lt;'EOF'
[pyrosql]
name=PyroSQL
baseurl=https://{url}/yum/stable-legacy/$basearch/
enabled=1
gpgcheck=1
gpgkey=https://{url}/gpg.key
EOF
sudo yum install pyrosql-driver</code></pre>

<h2>openSUSE &middot; SUSE Linux Enterprise</h2>
<pre><code>sudo zypper addrepo --gpgcheck --refresh https://{url}/yum/stable/$basearch/ pyrosql
sudo rpm --import https://{url}/gpg.key
sudo zypper install pyrosql-driver</code></pre>

<h2>Other languages &middot; other platforms</h2>

<table>
  <thead><tr><th>Platform</th><th>Command</th></tr></thead>
  <tbody>
    <tr><td>Rust (crates.io)</td><td><code>cargo add pyrosql</code></td></tr>
    <tr><td>Python (PyPI)</td><td><code>pip install pyrosql</code></td></tr>
    <tr><td>macOS &middot; Windows</td><td>Grab the <code>.dylib</code> / <code>.dll</code> from the <a href="https://github.com/{repo}/releases/latest">latest release</a></td></tr>
    <tr><td>Raw library (any Linux)</td><td>Same as above &mdash; <code>.so</code> tarball from Releases</td></tr>
  </tbody>
</table>

<h2>Verify the installation</h2>
<pre><code># Ensure the shared library is loadable
ldconfig -p | grep pyrosql
# -> libpyrosql_ffi_pwire.so (libc6,x86-64) =&gt; /usr/lib/x86_64-linux-gnu/libpyrosql_ffi_pwire.so

# Check the package version
apt show pyrosql-driver 2&gt;/dev/null || rpm -qi pyrosql-driver</code></pre>

<h2>Troubleshooting</h2>

<h3>&ldquo;GLIBC_2.XX not found&rdquo; on install</h3>
<p>Your system&rsquo;s glibc is older than the modern track requires. Switch to the legacy repo (URL above) and reinstall.</p>

<h3>&ldquo;The following signatures couldn&rsquo;t be verified&rdquo;</h3>
<p>The signing key wasn&rsquo;t imported. Re-run the <code>curl &hellip; gpg.key</code> step above.</p>

<h3>&ldquo;Unable to locate package pyrosql-driver&rdquo;</h3>
<p><code>apt update</code> didn&rsquo;t see the new source. Check that <code>/etc/apt/sources.list.d/pyrosql.list</code> exists and re-run <code>sudo apt update</code>.</p>

<h2>Uninstall</h2>
<pre><code># Debian / Ubuntu
sudo apt remove pyrosql-driver
sudo rm /etc/apt/sources.list.d/pyrosql.list /usr/share/keyrings/pyrosql.gpg

# RHEL / Fedora
sudo dnf remove pyrosql-driver
sudo rm /etc/yum.repos.d/pyrosql.repo</code></pre>

<footer>
  PyroSQL driver &middot; MIT licensed &middot; <a href="https://github.com/{repo}">source on GitHub</a>
</footer>
</body>
</html>
"""

sys.stdout.write(html)
