#!/bin/bash

# Copyright (c) Aptos Foundation
# Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

# This script is to build and push aptos indexer processors v2 images.
# You need to execute this from the repository root as working directory
# E.g. scripts/build-and-push-images.sh
# E.g. scripts/build-and-push-images.sh python
# Note that this uses kaniko (https://github.com/GoogleContainerTools/kaniko) instead of vanilla docker to build the images, which has good remote caching support

set -ex

TARGET_REGISTRY="us-docker.pkg.dev/aptos-registry/docker/indexer-client-examples/rust"
# take GIT_SHA from environment variable if set, otherwise use git rev-parse HEAD
GIT_SHA="${GIT_SHA:-$(git rev-parse HEAD)}"
EXAMPLE_TO_BUILD_ARG="${1:-all}"

if [ "$CI" == "true" ]; then
    CREDENTIAL_MOUNT="$HOME/.docker/:/kaniko/.docker/:ro"
else
    # locally we mount gcloud config credentials
    CREDENTIAL_MOUNT="$HOME/.config/gcloud:/root/.config/gcloud:ro"
fi

# Normalize GIT_BRANCH if it's set
if [ -n "${GIT_BRANCH}" ]; then
    export NORMALIZED_GIT_BRANCH=$(printf "${GIT_BRANCH}" | sed -e 's/[^a-zA-Z0-9]/-/g')
fi

# Set DESTINATIONS based on GIT_SHA since that is always set
DESTINATIONS="--destination ${TARGET_REGISTRY}:${GIT_SHA}"

# If GIT_BRANCH is set and not empty, add it as an additional tag
if [ -n "${NORMALIZED_GIT_BRANCH}" ]; then
    DESTINATIONS="${DESTINATIONS} --destination ${TARGET_REGISTRY}:${NORMALIZED_GIT_BRANCH}"
    DESTINATIONS="${DESTINATIONS} --destination ${TARGET_REGISTRY}:${NORMALIZED_GIT_BRANCH}_${GIT_SHA}"
fi

# build and push the image
docker run \
    --rm \
    -v $CREDENTIAL_MOUNT \
    -v $(pwd):/workspace \
    gcr.io/kaniko-project/executor:latest \
    --dockerfile /workspace/Dockerfile \
    $DESTINATIONS \
    --context dir:///workspace/ \
    --cache=true
