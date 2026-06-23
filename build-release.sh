#!/bin/bash

# --- PLATFORM-SPECIFIC SETUP ---
#
# Cross-compiles release `foxmq` binaries for Linux, macOS, and Windows from a
# single host using `cargo-zigbuild` (zig provides the cross-linker), then
# packages each into a release-named zip under ./dist, ready to upload to a
# GitHub release.
#
# MacOS:
#   - cargo-zigbuild: `cargo install cargo-zigbuild`
#   - zig:            `brew install zig`
#   - jq:             `brew install jq`
#
# Linux (Ubuntu/Debian):
#   - cargo-zigbuild: `cargo install cargo-zigbuild`
#   - zig:            `sudo snap install zig --classic --beta` or ziglang.org
#   - jq:             `sudo apt install jq`
#
# Artifacts (names match https://github.com/tashigit/foxmq/releases):
#   - Linux   x86_64-unknown-linux-gnu  -> dist/foxmq_<ver>_linux-amd64.zip     (foxmq)
#   - macOS   universal2 (arm64+x86_64) -> dist/foxmq_<ver>_macos-universal.zip (foxmq)
#   - Windows x86_64-pc-windows-gnu     -> dist/foxmq_<ver>_windows-amd64.zip   (foxmq.exe)
#
# Note: Windows builds use the -gnu toolchain (CI uses -msvc); the resulting
# .exe is functionally equivalent but not byte-identical to the CI artifact.
#
# Requires SSH access to github.com to fetch the private
# `tashi-consensus-engine` dependency.
#
# ------------------------------

set -e
set -o pipefail

cd "$(dirname "$0")"

OS_TYPE="unknown"
if [[ "$OSTYPE" == "darwin"* ]]; then
    OS_TYPE="macos"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    OS_TYPE="linux"
elif [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "cygwin" ]]; then
    OS_TYPE="windows"
fi

echo -e "\033[1m :: Detected Host OS: $OS_TYPE \033[0m"

# Binaries successfully built in THIS run, as "src:platform" entries. Only
# these get packaged, so stale artifacts from a prior run are never shipped.
BUILT_ARTIFACTS=()

build_linux_x86() {
    echo -e "\033[1m :: Building for Linux x86_64 \033[0m"
    rustup target add x86_64-unknown-linux-gnu
    cargo zigbuild --target x86_64-unknown-linux-gnu --release -p foxmq
    BUILT_ARTIFACTS+=("target/x86_64-unknown-linux-gnu/release/foxmq:linux-amd64")
}

build_windows() {
    echo -e "\033[1m :: Building for Windows x86_64 \033[0m"
    rustup target add x86_64-pc-windows-gnu
    cargo zigbuild --target x86_64-pc-windows-gnu --release -p foxmq
    BUILT_ARTIFACTS+=("target/x86_64-pc-windows-gnu/release/foxmq.exe:windows-amd64")
}

build_macos_universal() {
    if [[ "$OS_TYPE" == "macos" ]]; then
        echo -e "\033[1m :: Building macOS universal (arm64 + x86_64) \033[0m"
        rustup target add aarch64-apple-darwin x86_64-apple-darwin
        cargo zigbuild --target universal2-apple-darwin --release -p foxmq
        BUILT_ARTIFACTS+=("target/universal2-apple-darwin/release/foxmq:macos-universal")
    else
        echo -e "\033[33m :: Skipping macOS build (Incompatible Host) \033[0m"
    fi
}

case "$OS_TYPE" in
    "macos")
        build_linux_x86
        build_windows
        build_macos_universal
        ;;
    "linux")
        build_linux_x86
        build_windows
        ;;
    "windows")
        build_windows
        build_linux_x86
        ;;
    *)
        echo -e "\033[31m :: Unsupported Host OS for this script \033[0m"
        exit 1
        ;;
esac

echo -e "\033[1m :: Resolving version \033[0m"

if ! command -v jq &> /dev/null; then
    echo "Error: jq is required but not installed."
    exit 1
fi

version=$(cargo metadata --format-version=1 --no-deps | jq -r '.packages[] | select(.name == "foxmq") | .version')
if [[ -z "$version" ]]; then
    echo "Error: could not resolve foxmq version from cargo metadata."
    exit 1
fi
echo -e "\033[1m :: Version $version \033[0m"

echo -e "\033[1m :: Packaging artifacts into ./dist \033[0m"

DIST_DIR="dist"
mkdir -p "$DIST_DIR"

# Package a built binary into a release-named zip with the binary at the zip
# root (-j), matching CI's `zip -j foxmq_<ver>_<platform>.zip`.
package() {
    local src="$1"        # path to built binary
    local platform="$2"   # e.g. linux-amd64
    local zip="$DIST_DIR/foxmq_${version}_${platform}.zip"

    if [[ -f "$src" ]]; then
        rm -f "$zip"
        zip -j "$zip" "$src"
        echo -e "\033[32m :: Packaged $zip \033[0m"
    else
        echo -e "\033[33m :: Skip $platform: $src not found \033[0m"
    fi
}

for entry in "${BUILT_ARTIFACTS[@]}"; do
    package "${entry%%:*}" "${entry##*:}"
done

echo -e "\033[1m :: Done. Artifacts in ./$DIST_DIR: \033[0m"
ls -1 "$DIST_DIR"
