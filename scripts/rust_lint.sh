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

# Ensure all source files have the correct license header.
python3 scripts/check_license.py $CHECK_ARG

# Run clippy on the pinned STABLE toolchain (from rust-toolchain.toml), NOT
# nightly. Latest-nightly rustc intermittently segfaults (SIGSEGV) while
# compiling some crates (e.g. the aptos-indexer-test-transactions git dep) and
# introduces brand-new lints without warning, both of which break CI spuriously.
# Stable clippy is deterministic and matches the toolchain the code is built with.
cargo xclippy

# We require the nightly build of cargo fmt to provide stricter rust formatting
# (rustfmt.toml uses nightly-only options like imports_granularity/group_imports).
cargo +nightly fmt $CHECK_ARG

# Once cargo-sort correctly handles workspace dependencies,
# we can move to cleaner workspace dependency notation.
# See: https://github.com/DevinR528/cargo-sort/issues/47
cargo sort --grouped --workspace $CHECK_ARG
