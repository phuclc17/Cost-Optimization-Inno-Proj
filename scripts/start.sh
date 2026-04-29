#!/bin/bash
set -e
export $(cat .env | grep -v '#' | xargs)

echo "==================================="
echo " Starting Pipeline Stack..."
echo "==================================="

echo "[1/5] Starting PostgreSQL..."
docker compose up -d postgres

echo "[2/5] Waiting for PostgreSQL..."
until docker compose exec -T postgres \
  pg_isready -U $POSTGRES_USER -d $POSTGRES_DB \
  > /dev/null 2>&1; do
  printf "."
  sleep 2
done
echo " Ready!"

echo "[3/5] Starting Workers..."
docker compose up -d worker

echo "[4/5] Starting Nginx + Monitoring..."
docker compose up -d nginx prometheus grafana

echo "[5/5] Waiting for Nginx..."
sleep 5

echo ""
echo "==================================="
echo " All services running!"
echo "==================================="
echo " Worker:     http://localhost:8000/health"
echo " Prometheus: http://localhost:9090"
echo " Grafana:    http://localhost:3001"
echo "             login: admin / admin"
echo "==================================="