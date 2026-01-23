# Vendoring Workarounds

## Problem
The vendor directory is incomplete (missing `rusqlite`, `sqlx`, and their dependencies). Network access to crates.io is blocked (403 error), preventing automatic vendor updates.

## Workarounds

### ✅ OPTION 1: Wait for Network Access (RECOMMENDED)
**Most reliable solution**

1. When network access to crates.io is available:
   ```bash
   ./fix_vendor.sh
   ```
   
2. This will:
   - Update `Cargo.lock` with all dependencies
   - Download and vendor all missing packages
   - Generate proper checksum files
   - Re-enable vendoring

### ✅ OPTION 2: Use VPN/Proxy
**If you can configure network access**

1. Set up VPN or proxy to access crates.io
2. Run:
   ```bash
   cargo vendor vendor
   ```
3. This will complete the vendor directory

### ✅ OPTION 3: Copy Vendor Directory
**If you have another machine with network access**

1. On the machine with network:
   ```bash
   cd /path/to/RCA-ENGINE
   cargo vendor vendor
   tar czf vendor.tar.gz vendor/
   ```

2. Copy `vendor.tar.gz` to this machine:
   ```bash
   tar xzf vendor.tar.gz
   ```

3. Re-enable vendoring in `.cargo/config.toml`:
   ```toml
   [source.crates-io]
   replace-with = "vendored-sources"
   
   [source.vendored-sources]
   directory = "vendor"
   ```

### ⚠️ OPTION 4: Manual Git Dependencies (PARTIAL)
**Only works if you have network for other dependencies**

1. Disable vendoring (already done in `.cargo/config.toml`)
2. Use git dependencies in `Cargo.toml`:
   ```toml
   rusqlite = { git = "https://github.com/rusqlite/rusqlite", tag = "v0.31.0", features = ["bundled"] }
   sqlx = { git = "https://github.com/launchbadge/sqlx", tag = "v0.7.4", features = [...] }
   ```
3. **Limitation**: Still needs network for other dependencies (polars, etc.)

## Current Status

- ✅ System dependencies installed via Homebrew (sqlite, pkg-config, openssl)
- ✅ `fix_vendor.sh` script created
- ✅ Vendoring temporarily disabled
- ⚠️ Vendor directory incomplete (missing packages and checksums)
- ❌ Network access to crates.io blocked

## Next Steps

**Best approach**: Wait for network access and run `./fix_vendor.sh`

This will properly vendor all dependencies with correct checksums and allow offline builds.

