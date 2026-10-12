#!/bin/sh
# Installs the minipaw CLI from its GitHub release.
#
#   curl -fsSL https://paw.minip2p.com/install.sh | sh
#
# MINIPAW_VERSION   version to install, e.g. 0.2.0 (default: latest)
# MINIPAW_INSTALL   install directory (default: ~/.local/bin)
set -eu

repo="deepso7/minipaw"

case "$(uname -s)" in
  Linux) os="linux" ;;
  Darwin) os="darwin" ;;
  *) echo "minipaw: no prebuilt binary for $(uname -s); install with cargo:" >&2
     echo "  cargo install minipaw-cli" >&2
     exit 1 ;;
esac

case "$(uname -m)" in
  x86_64 | amd64) arch="x64" ;;
  arm64 | aarch64) arch="arm64" ;;
  *) echo "minipaw: no prebuilt binary for $(uname -m); install with cargo:" >&2
     echo "  cargo install minipaw-cli" >&2
     exit 1 ;;
esac

if [ -n "${MINIPAW_VERSION:-}" ]; then
  base="https://github.com/$repo/releases/download/v${MINIPAW_VERSION#v}"
else
  base="https://github.com/$repo/releases/latest/download"
fi
file="minipaw-$os-$arch.tar.gz"

dir="${MINIPAW_INSTALL:-$HOME/.local/bin}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

echo "Downloading minipaw for $os-$arch..."
curl -fsSL -o "$tmp/$file" "$base/$file"
curl -fsSL -o "$tmp/SHA256SUMS" "$base/SHA256SUMS"

# Check the tarball against the release's checksums.
if command -v sha256sum >/dev/null 2>&1; then
  sum="sha256sum"
elif command -v shasum >/dev/null 2>&1; then
  sum="shasum -a 256"
else
  echo "minipaw: need sha256sum or shasum to verify the download" >&2
  exit 1
fi
(cd "$tmp" && grep " $file\$" SHA256SUMS | $sum -c - >/dev/null) || {
  echo "minipaw: checksum mismatch for $file" >&2
  exit 1
}

tar -xzf "$tmp/$file" -C "$tmp"
mkdir -p "$dir"
mv "$tmp/minipaw" "$dir/minipaw"
chmod 755 "$dir/minipaw"

echo "Installed $("$dir/minipaw" --version) to $dir/minipaw"
case ":$PATH:" in
  *":$dir:"*) ;;
  *) echo "Add $dir to your PATH to run minipaw, for example:"
     echo "  export PATH=\"$dir:\$PATH\"" ;;
esac
echo "Get started: minipaw --help"
