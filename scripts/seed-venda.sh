#!/usr/bin/env bash
# Gera venda(s) de teste na FONTE (MySQL). Cupom e produto aleatorios.
# Uso: ./scripts/seed-venda.sh [quantidade]   (padrao: 1)
set -euo pipefail
N="${1:-1}"
for i in $(seq 1 "$N"); do
  CUPOM=$(( (RANDOM % 3) + 1 ))     # 1..3  (ANA10 / BRUNO10 / CARLA10)
  PRODUTO=$(( (RANDOM % 4) + 1 ))   # 1..4  (viagem / celular / residencial / saude)
  CENTAVOS=$(( (RANDOM % 40000) + 5000 ))
  VALOR=$(printf '%d.%02d' $((CENTAVOS / 100)) $((CENTAVOS % 100)))
  SQL="INSERT INTO vendas (cupom_id, produto_id, valor, status) VALUES ($CUPOM, $PRODUTO, $VALOR, 'confirmada');"
  docker exec comercial-mysql mysql -uroot -proot_pw comercial -e "$SQL" 2>/dev/null
  echo "venda inserida -> cupom_id=$CUPOM produto_id=$PRODUTO valor=$VALOR"
done
