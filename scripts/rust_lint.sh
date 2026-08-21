#!/bin/sh

# Copyright (c) Aptos Foundation
# Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

# This assumes you have already installed cargo-sort:
# cargo install cargo-sort
#
# The best way to do this however is to run scripts/dev_setup.sh
#
# If you want to run this from anywhere in aptos-core, try adding this wrapper
# script to your path:
# https://gist.github.com/banool/e6a2b85e2fff067d3a215cbfaf808032

# Make sure we're in the root of the repo.
if [ ! -f "scripts/rust_lint.sh" ] 
then
    echo "Please run this from the aptos-indexer-processors-v2 directory." 
    exit 1
fi

# Run in check mode if requested.
CHECK_ARG=""
if [ "$1" = "--check" ]; then
    CHECK_ARG="--check"
fi

set -e
set -x

# Clippy and rustfmt run on nightly, which lints and formats more strictly than
# the stable toolchain in rust-toolchain.toml. The version is pinned so results
# are reproducible and upstream nightly regressions cannot break the build
# without a deliberate bump.
NIGHTLY="$(cat rust-nightly-version)"

# Ensure all source files have the correct license header.
python3 scripts/check_license.py $CHECK_ARG

cargo +"$NIGHTLY" xclippy

cargo +"$NIGHTLY" fmt $CHECK_ARG

# Once cargo-sort correctly handles workspace dependencies,
# we can move to cleaner workspace dependency notation.
# See: https://github.com/DevinR528/cargo-sort/issues/47
cargo sort --grouped --workspace $CHECK_ARG
