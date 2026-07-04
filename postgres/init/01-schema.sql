CREATE SCHEMA IF NOT EXISTS comercial;

CREATE TABLE IF NOT EXISTS comercial.fact_venda_realtime (
  venda_id INT PRIMARY KEY,
  produto_id INT,
  produto_nome VARCHAR(120),
  canal VARCHAR(40),
  cupom_id INT,
  cupom_codigo VARCHAR(40),
  vendedor_id INT,
  vendedor_nome VARCHAR(120),
  valor NUMERIC(10,2),
  status VARCHAR(20),
  venda_criada_em TIMESTAMP,
  venda_atualizada_em TIMESTAMP,
  processado_em TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_venda_vendedor ON comercial.fact_venda_realtime (vendedor_id);
CREATE INDEX IF NOT EXISTS idx_fact_venda_canal ON comercial.fact_venda_realtime (canal);
