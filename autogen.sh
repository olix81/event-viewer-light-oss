#!/usr/bin/env sh

set -eu

if ! command -v autoreconf >/dev/null 2>&1; then
	echo "autoreconf not found; please install autoconf/automake/libtool" >&2
	exit 1
fi

mkdir -p build-aux
autoreconf --install --force
