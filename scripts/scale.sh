#!/bin/bash
# Dùng: ./scripts/scale.sh 3
WORKERS=${1:-1}
echo "Scaling workers to $WORKERS..."
docker compose up -d --scale worker=$WORKERS --no-recreate
docker compose ps | grep worker
