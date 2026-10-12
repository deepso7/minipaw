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
  tag="v${MINIPAW_VERSION#v}"
else
  # Resolve latest once, so the tarball and SHA256SUMS come from one release.
  latest="$(curl -fsSLI -o /dev/null -w '%{url_effective}' "https://github.com/$repo/releases/latest")"
  tag="${latest##*/}"
fi
base="https://github.com/$repo/releases/download/$tag"
file="minipaw-$os-$arch.tar.gz"

dir="${MINIPAW_INSTALL:-$HOME/.local/bin}"
tmp="$(mktemp -d)"
new=""
trap 'rm -rf "$tmp"; [ -z "$new" ] || rm -f "$new"' EXIT

echo "Downloading minipaw $tag for $os-$arch..."
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
line="$(grep " $file\$" "$tmp/SHA256SUMS" || true)"
if [ -z "$line" ]; then
  echo "minipaw: no checksum listed for $file" >&2
  exit 1
fi
(cd "$tmp" && printf '%s\n' "$line" | $sum -c - >/dev/null) || {
  echo "minipaw: checksum mismatch for $file" >&2
  exit 1
}

tar -xzf "$tmp/$file" -C "$tmp"
# Check it runs before replacing an existing install, from the install
# directory, since the temporary one may be mounted noexec.
mkdir -p "$dir"
new="$dir/.minipaw.new.$$"
mv "$tmp/minipaw" "$new"
chmod 755 "$new"
version="$("$new" --version)" || {
  echo "minipaw: the downloaded binary does not run on this machine" >&2
  exit 1
}
mv "$new" "$dir/minipaw"
new=""

echo "Installed $version to $dir/minipaw"
case ":$PATH:" in
  *":$dir:"*) ;;
  *) echo "Add $dir to your PATH to run minipaw, for example:"
     echo "  export PATH=\"$dir:\$PATH\"" ;;
esac
echo "Get started: minipaw --help"
