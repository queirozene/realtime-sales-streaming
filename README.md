# Ciclic Comercial Streaming (POC)

Streaming de vendas do comercial: captura mudanças no MySQL em tempo real, atribui
cada venda ao cupom/vendedor correto e alimenta um data warehouse consumido pelo Metabase.

## Arquitetura

```
MySQL (comercial)  --CDC-->  Debezium (Kafka Connect)  --topicos-->  Kafka
                                                                        |
                                                                        v
                                                        Consumer Python (enrichment)
                                                        - junta venda + cupom + vendedor + produto
                                                        - grava registro ja enriquecido
                                                                        |
                                                                        v
                                                              Postgres (DW do POC) --> Metabase
```

Tabelas fonte (MySQL, banco `comercial`): `vendedores`, `cupons`, `produtos`, `vendas`.

Cada uma vira um tópico Kafka via Debezium: `comercial.comercial.vendas`,
`comercial.comercial.cupons`, `comercial.comercial.vendedores`, `comercial.comercial.produtos`.

O serviço `enrichment` mantém um cache em memória de vendedores/cupons/produtos (populado
pelo próprio CDC) e, a cada evento de `vendas`, resolve `cupom_id -> vendedor` e
`produto_id -> nome/canal`, gravando o registro já pronto em
`comercial.fact_venda_realtime` no Postgres — essa é a tabela que o Metabase deve consumir.

## Pré-requisito: habilitar Docker no WSL

O Docker Desktop está instalado no Windows mas a integração com a distro `Ubuntu-22.04`
ainda não está ativa. Antes de rodar o `docker compose`:

1. Abra o Docker Desktop no Windows.
2. Settings → Resources → WSL Integration.
3. Ative o toggle da distro `Ubuntu-22.04`.
4. Apply & Restart.

## Subindo o ambiente

```bash
cd ~/repos/ciclic-comercial-streaming
docker compose up -d --build
```

Aguarde o MySQL e o Postgres ficarem `healthy` (docker compose ps), depois registre o
connector do Debezium:

```bash
./scripts/register-connector.sh
```

Isso faz um snapshot inicial das 4 tabelas e começa a seguir o binlog em tempo real.

## Testando o fluxo em tempo real

Abra um shell no MySQL e insira uma venda nova:

```bash
docker exec -it comercial-mysql mysql -uroot -proot_pw comercial -e \
  "INSERT INTO vendas (cupom_id, produto_id, valor, status) VALUES (2, 3, 275.50, 'confirmada');"
```

Acompanhe o log do enrichment em tempo real:

```bash
docker logs -f comercial-enrichment
```

Você deve ver a linha de log com venda -> vendedor -> cupom -> produto -> canal em
poucos segundos. Confira o resultado direto no Postgres:

```bash
docker exec -it comercial-dw psql -U dw -d dw -c \
  "SELECT * FROM comercial.fact_venda_realtime ORDER BY processado_em DESC LIMIT 5;"
```

## Serviços expostos

| Serviço          | URL/Porta               | Uso                                   |
|------------------|--------------------------|----------------------------------------|
| MySQL            | localhost:3306           | Banco fonte (comercial)                |
| Kafka            | localhost:9092           | Broker                                 |
| Kafka Connect    | localhost:8083           | API REST do Debezium                   |
| Kafka UI         | http://localhost:8080    | Ver tópicos/mensagens/connectors       |
| Postgres (DW)    | localhost:5432           | `fact_venda_realtime`                  |
| Metabase         | http://localhost:3000    | Dashboards em cima do Postgres         |

## Próximos passos (rumo à produção / Redshift)

- **Fonte real**: trocar host/usuário/senha do connector (`connectors/mysql-source.json`)
  para apontar para uma réplica de leitura do MySQL de produção da Ciclic (não o primary),
  com um usuário dedicado só com `REPLICATION SLAVE`/`REPLICATION CLIENT`/`SELECT`.
- **Destino real**: trocar o `POSTGRES_DSN` do `enrichment` por uma conexão Redshift
  (via `psycopg2` ou `redshift_connector`) e criar a tabela `fact_venda_realtime` no
  schema do DW existente (mesmo padrão de `fact_lead`, `fact_payment`, `fact_plan`).
- **Idempotência/exactly-once**: hoje o upsert por `venda_id` já é idempotente; ao trocar
  para Redshift, considerar staging + `MERGE` em vez de upsert linha a linha (Redshift não
  tem `ON CONFLICT` nativo tão eficiente em volume alto).
- **dbt**: expor `fact_venda_realtime` como source no seu projeto dbt existente
  (`~/repos/dbt_example`) para já nascer documentada/testada junto com o restante do catálogo.
- **Alertas de lag**: monitorar o consumer group do enrichment (`comercial-enrichment`)
  para saber se o streaming está atrasado em relação ao MySQL.
