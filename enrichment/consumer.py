import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("enrichment")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_PREFIX = os.environ.get("TOPIC_PREFIX", "comercial")
POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN", "dbname=dw user=dw password=dw_pw host=postgres port=5432"
)

TOPICS = {
    "vendedores": f"{TOPIC_PREFIX}.comercial.vendedores",
    "cupons": f"{TOPIC_PREFIX}.comercial.cupons",
    "produtos": f"{TOPIC_PREFIX}.comercial.produtos",
    "vendas": f"{TOPIC_PREFIX}.comercial.vendas",
}

# Cache local das tabelas de dimensao, alimentado pelo proprio CDC.
# Como o Debezium faz snapshot inicial das tabelas antes das vendas,
# essas dimensoes chegam populadas antes (ou junto) dos eventos de venda.
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


def upsert_venda(conn, venda):
    cupom = cupons_cache.get(venda["cupom_id"], {})
    vendedor = vendedores_cache.get(cupom.get("vendedor_id"), {})
    produto = produtos_cache.get(venda["produto_id"], {})

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

    log.info("consumindo topicos: %s", list(TOPICS.values()))

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("erro no kafka: %s", msg.error())
                continue
            if not msg.value():
                continue  # tombstone de delete

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
                upsert_venda(conn, after)
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
