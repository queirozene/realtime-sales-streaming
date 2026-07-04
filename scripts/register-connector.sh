#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "Aguardando Kafka Connect ficar disponivel em localhost:8083..."
until curl -s -o /dev/null http://localhost:8083/connectors; do
  sleep 2
done

echo "Registrando connector Debezium (MySQL -> Kafka)..."
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://localhost:8083/connectors/ -d @connectors/mysql-source.json

echo
echo "Status do connector:"
curl -s http://localhost:8083/connectors/comercial-mysql-connector/status
