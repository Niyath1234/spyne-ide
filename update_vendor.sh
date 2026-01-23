#!/bin/bash
# Script to update the vendor directory with all dependencies
# Run this when you have network access to crates.io

set -e

echo "Updating vendor directory..."

# Temporarily disable vendoring
mv .cargo/config.toml .cargo/config.toml.bak

# Generate/update Cargo.lock
echo "Generating Cargo.lock..."
cargo generate-lockfile

# Vendor all dependencies
echo "Vendoring dependencies..."
cargo vendor vendor

# Restore vendoring config
mv .cargo/config.toml.bak .cargo/config.toml

echo "Vendor directory updated successfully!"
echo "You can now build with: cargo build --bin server"

