#!/usr/bin/env bash
# Monitor ao vivo da FONTE (MySQL de "producao") - atualiza a cada 2s.
# Ctrl+C para sair.
watch -n 2 -t "docker exec comercial-mysql mysql -uroot -proot_pw comercial -N -e \"
SELECT v.id, v.criado_em, c.codigo, ve.nome, p.canal, v.valor, v.status
FROM vendas v
JOIN cupons c ON c.id = v.cupom_id
JOIN vendedores ve ON ve.id = c.vendedor_id
JOIN produtos p ON p.id = v.produto_id
ORDER BY v.id DESC LIMIT 15;\" 2>/dev/null"
