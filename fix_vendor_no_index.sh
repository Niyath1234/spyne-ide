#!/bin/bash
# Fix vendor directory using existing Cargo.lock (skip index update)

set -e

echo "=== Fixing vendor directory (using existing Cargo.lock) ==="

# Backup current config
cp .cargo/config.toml .cargo/config.toml.backup

# Disable vendoring temporarily
cat > .cargo/config.toml << 'CONFIG'
# Temporarily disabled for vendor update
# [source.crates-io]
# replace-with = "vendored-sources"
#
# [source.vendored-sources]
# directory = "vendor"
CONFIG

echo "1. Using existing Cargo.lock (skipping index update)..."

if [ ! -f Cargo.lock ]; then
    echo "Error: Cargo.lock not found. Need to generate it first."
    exit 1
fi

echo "2. Vendoring all dependencies from Cargo.lock..."
export CARGO_NET_RETRY=10
export CARGO_NET_TIMEOUT=60
cargo vendor vendor || {
    echo "Error: Failed to vendor dependencies."
    mv .cargo/config.toml.backup .cargo/config.toml
    exit 1
}

echo "3. Restoring vendoring config..."
mv .cargo/config.toml.backup .cargo/config.toml

echo ""
echo "✓ Vendor directory updated successfully!"
echo "You can now build with: cargo build --bin server"
