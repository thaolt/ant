#!/bin/sh

docker buildx build \
  --target export \
  --build-arg BUILD_GIT_HASH=$(git rev-parse HEAD) \
  --build-arg BUILD_TIMESTAMP=$(date +%s) \
  --output type=local,dest=./dist \
  --progress=plain \
  . 2>&1