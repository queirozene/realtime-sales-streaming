import json
import logging
import os
import time

import psycopg2
import pymysql
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("enrichment")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_PREFIX = os.environ.get("TOPIC_PREFIX", "comercial")
POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN", "dbname=dw user=dw password=dw_pw host=postgres port=5432"
)
MYSQL_CFG = {
    "host": os.environ.get("MYSQL_HOST", "mysql"),
    "port": int(os.environ.get("MYSQL_PORT", "3306")),
    "user": os.environ.get("MYSQL_USER", "debezium"),
    "password": os.environ.get("MYSQL_PASSWORD", "dbz_pw"),
    "database": os.environ.get("MYSQL_DB", "comercial"),
}

TOPICS = {
    "vendedores": f"{TOPIC_PREFIX}.comercial.vendedores",
    "cupons": f"{TOPIC_PREFIX}.comercial.cupons",
    "produtos": f"{TOPIC_PREFIX}.comercial.produtos",
    "vendas": f"{TOPIC_PREFIX}.comercial.vendas",
}

# Cache local das dimensoes, mantido quente pelo CDC (reflete mudancas em tempo
# real, ex: um cupom reatribuido a outro vendedor). Como o Kafka NAO garante
# ordem entre topicos diferentes, uma venda pode chegar antes da sua dimensao
# ter sido cacheada. Por isso, todo cache miss cai num lookup direto no MySQL
# (fonte da verdade), tornando o enriquecimento correto independente da ordem.
vendedores_cache = {}
cupons_cache = {}
produtos_cache = {}


def connect_postgres():
    while True:
        try:
            return psycopg2.connect(POSTGRES_DSN)
        except psycopg2.OperationalError:
            log.warning("postgres indisponivel, tentando novamente em 3s...")
            time.sleep(3)


def connect_mysql():
    while True:
        try:
            return pymysql.connect(
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True,
                **MYSQL_CFG,
            )
        except pymysql.err.OperationalError:
            log.warning("mysql indisponivel, tentando novamente em 3s...")
            time.sleep(3)


def _mysql_lookup(mysql, table, row_id):
    """Busca uma linha de dimensao no MySQL por id (fallback de cache miss)."""
    try:
        with mysql.cursor() as cur:
            cur.execute(f"SELECT * FROM {table} WHERE id = %s", (row_id,))
            return cur.fetchone()
    except pymysql.err.OperationalError:
        # conexao caiu; reconecta e tenta uma vez
        mysql.ping(reconnect=True)
        with mysql.cursor() as cur:
            cur.execute(f"SELECT * FROM {table} WHERE id = %s", (row_id,))
            return cur.fetchone()


def get_cupom(mysql, cupom_id):
    if cupom_id not in cupons_cache:
        row = _mysql_lookup(mysql, "cupons", cupom_id)
        if row:
            cupons_cache[cupom_id] = row
    return cupons_cache.get(cupom_id, {})


def get_vendedor(mysql, vendedor_id):
    if vendedor_id is None:
        return {}
    if vendedor_id not in vendedores_cache:
        row = _mysql_lookup(mysql, "vendedores", vendedor_id)
        if row:
            vendedores_cache[vendedor_id] = row
    return vendedores_cache.get(vendedor_id, {})


def get_produto(mysql, produto_id):
    if produto_id not in produtos_cache:
        row = _mysql_lookup(mysql, "produtos", produto_id)
        if row:
            produtos_cache[produto_id] = row
    return produtos_cache.get(produto_id, {})


def upsert_venda(conn, mysql, venda):
    cupom = get_cupom(mysql, venda["cupom_id"])
    vendedor = get_vendedor(mysql, cupom.get("vendedor_id"))
    produto = get_produto(mysql, venda["produto_id"])

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO comercial.fact_venda_realtime (
                venda_id, produto_id, produto_nome, canal,
                cupom_id, cupom_codigo, vendedor_id, vendedor_nome,
                valor, status, venda_criada_em, venda_atualizada_em, processado_em
            ) VALUES (
                %(id)s, %(produto_id)s, %(produto_nome)s, %(canal)s,
                %(cupom_id)s, %(cupom_codigo)s, %(vendedor_id)s, %(vendedor_nome)s,
                %(valor)s, %(status)s, to_timestamp(%(criado_em)s / 1000.0),
                to_timestamp(%(atualizado_em)s / 1000.0), now()
            )
            ON CONFLICT (venda_id) DO UPDATE SET
                produto_id = EXCLUDED.produto_id,
                produto_nome = EXCLUDED.produto_nome,
                canal = EXCLUDED.canal,
                cupom_id = EXCLUDED.cupom_id,
                cupom_codigo = EXCLUDED.cupom_codigo,
                vendedor_id = EXCLUDED.vendedor_id,
                vendedor_nome = EXCLUDED.vendedor_nome,
                valor = EXCLUDED.valor,
                status = EXCLUDED.status,
                venda_atualizada_em = EXCLUDED.venda_atualizada_em,
                processado_em = now()
            """,
            {
                "id": venda["id"],
                "produto_id": venda["produto_id"],
                "produto_nome": produto.get("nome"),
                "canal": produto.get("canal"),
                "cupom_id": venda["cupom_id"],
                "cupom_codigo": cupom.get("codigo"),
                "vendedor_id": cupom.get("vendedor_id"),
                "vendedor_nome": vendedor.get("nome"),
                "valor": venda["valor"],
                "status": venda["status"],
                "criado_em": venda["criado_em"],
                "atualizado_em": venda["atualizado_em"],
            },
        )
    conn.commit()
    log.info(
        "venda %s -> vendedor=%s cupom=%s produto=%s canal=%s valor=%s",
        venda["id"],
        vendedor.get("nome"),
        cupom.get("codigo"),
        produto.get("nome"),
        produto.get("canal"),
        venda["valor"],
    )


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "comercial-enrichment",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(list(TOPICS.values()))
    conn = connect_postgres()
    mysql = connect_mysql()

    log.info("consumindo topicos: %s", list(TOPICS.values()))

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                # UNKNOWN_TOPIC_OR_PART e esperado ate o Debezium criar os topicos
                continue
            if not msg.value():
                continue  # tombstone de delete

            try:
                payload = json.loads(msg.value())
                after = payload.get("after")
                if after is None:
                    continue  # evento de delete, ignorado no POC

                topic = msg.topic()
                if topic == TOPICS["vendedores"]:
                    vendedores_cache[after["id"]] = after
                elif topic == TOPICS["cupons"]:
                    cupons_cache[after["id"]] = after
                elif topic == TOPICS["produtos"]:
                    produtos_cache[after["id"]] = after
                elif topic == TOPICS["vendas"]:
                    upsert_venda(conn, mysql, after)
            except Exception:
                # nao deixa uma mensagem problematica derrubar o pipeline;
                # loga, faz rollback da transacao e segue para a proxima
                conn.rollback()
                log.exception("falha ao processar mensagem do topico %s", msg.topic())
    finally:
        consumer.close()
        conn.close()
        mysql.close()


if __name__ == "__main__":
    main()
