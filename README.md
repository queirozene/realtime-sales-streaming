# Realtime Sales Streaming (CDC POC)

Streaming de vendas em tempo real: captura mudanças no MySQL via CDC, atribui
cada venda ao cupom/vendedor correto e alimenta um data warehouse consumido pelo Metabase.

Stack: **MySQL + Debezium + Kafka + Python + Postgres + Metabase**, tudo via Docker Compose.
O cenário de exemplo é uma operação comercial de seguros (viagem, celular, residencial,
saúde) onde vendedores fecham vendas atribuindo o próprio cupom.

> 📖 Quer entender o funcionamento passo a passo, da lógica aos containers?
> Veja **[ARQUITETURA.md](ARQUITETURA.md)**.

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

## Pré-requisitos

- **Docker Desktop** (macOS, Windows ou Linux) com `docker compose`. As imagens são
  multi-arch, então rodam nativo tanto em Intel quanto em Apple Silicon (M1/M2/M3).
- **git** para clonar o repositório.

Não é preciso instalar MySQL, Kafka, Python etc. na máquina — tudo roda em containers.
A única imagem custom (o serviço `enrichment`) é buildada localmente pelo compose; as
demais são imagens oficiais do Docker Hub, baixadas automaticamente no primeiro `up`.

> **No Windows via WSL2:** abra o Docker Desktop → Settings → Resources → WSL Integration
> e ative o toggle da sua distro antes de rodar o `docker compose`.

## Subindo o ambiente

```bash
git clone https://github.com/queirozene/realtime-sales-streaming.git
cd realtime-sales-streaming
cp .env.example .env          # cria o arquivo de variaveis (senhas dos containers)
docker compose up -d --build
```

O primeiro `up` baixa as imagens (alguns minutos, só na primeira vez) e builda o
`enrichment`. Aguarde o MySQL e o Postgres ficarem `healthy` (`docker compose ps`),
depois registre o connector do Debezium:

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
  para apontar para uma réplica de leitura do MySQL de produção (não o primary),
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
