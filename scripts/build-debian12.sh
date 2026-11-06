#!/usr/bin/env bash
# Debian 12 (bookworm) binaries and .deb packages, cross-compiled with zig.
# glibc 2.34 as the floor: runs on Debian 12 (2.36) and newer, OpenSSL is vendored.
# Needs: rustup, zig, cargo-zigbuild, cargo-deb.
set -euo pipefail
cd "$(dirname "$0")/.."

GLIBC=2.34
VERSION=$(cargo metadata --no-deps --format-version 1 | sed -E 's/.*"version":"([^"]+)".*/\1/')
mkdir -p dist

for arch in x86_64 aarch64; do
  target="$arch-unknown-linux-gnu"
  rustup target add "$target" >/dev/null
  cargo zigbuild --release --locked --target "$target.$GLIBC"
  cp "target/$target/release/pg-online-schema-change" "dist/pg-online-schema-change-$VERSION-linux-$arch"
  cargo deb --no-build --no-strip --target "$target" --output dist/
done

ls -la dist/
