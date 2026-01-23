#!/bin/bash
# Fix vendor directory - run this when you have network access to crates.io

set -e

echo "=== Fixing vendor directory ==="

# Backup current config
cp .cargo/config.toml .cargo/config.toml.backup

# Disable vendoring temporarily
cat > .cargo/config.toml << 'EOF'
# Temporarily disabled for vendor update
# [source.crates-io]
# replace-with = "vendored-sources"
#
# [source.vendored-sources]
# directory = "vendor"
EOF

echo "1. Generating/updating Cargo.lock..."
# Increase timeout and retry settings
export CARGO_NET_RETRY=10
export CARGO_NET_TIMEOUT=60
cargo generate-lockfile || {
    echo "Warning: Failed to generate lockfile. Trying to continue with existing Cargo.lock..."
}

echo "2. Vendoring all dependencies..."
cargo vendor vendor || {
    echo "Error: Failed to vendor dependencies. Check network connectivity."
    exit 1
}

echo "3. Restoring vendoring config..."
mv .cargo/config.toml.backup .cargo/config.toml

echo ""
echo "✓ Vendor directory updated successfully!"
echo "You can now build with: cargo build --bin server"

