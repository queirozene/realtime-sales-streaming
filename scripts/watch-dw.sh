#!/usr/bin/env bash
# Monitor ao vivo do DESTINO (Postgres/DW que o Metabase consome) - a cada 2s.
watch -n 2 -t "docker exec comercial-dw psql -U dw -d dw -c \"
SELECT venda_id, vendedor_nome, canal, cupom_codigo, valor, status, processado_em
FROM comercial.fact_venda_realtime ORDER BY venda_id DESC LIMIT 15;\""
