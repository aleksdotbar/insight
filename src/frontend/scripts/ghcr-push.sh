#!/usr/bin/env bash
set -euo pipefail

IMAGE="ghcr.io/cyberfabric/insight-front"
TAG="${1:-latest}"

docker build -t "$IMAGE:$TAG" .
echo "$IMAGE:$TAG"
docker push "$IMAGE:$TAG"
