import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone

import mysql.connector
import redis
from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient


load_dotenv()

BATCH_SIZE = 100
INTERVALO_SEGUNDOS = 5
LIMITE_ATRASO_SEGUNDOS = 300

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI não está definido no .env.")

mongo_client = MongoClient(MONGO_URI)
colecao = mongo_client["iot"]["sensores"]

conexao_sql = mysql.connector.connect(
    host=os.getenv("MARIADB_HOST", "localhost"),
    port=int(os.getenv("MARIADB_PORT", "3306")),
    user=os.getenv("MARIADB_USER"),
    password=os.getenv("MARIADB_PASSWORD"),
    database=os.getenv("MARIADB_DATABASE", "fernanda_pipeline_iot"),
)

redis_client = redis.Redis.from_url(
    os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    decode_responses=True,
)

redis_client.ping()


def obter_controle(cursor, chave):
    cursor.execute(
        "SELECT valor FROM fernanda_controle WHERE chave = %s",
        (chave,),
    )
    resultado = cursor.fetchone()
    return resultado[0] if resultado else None


def definir_controle(cursor, chave, valor):
    cursor.execute(
        """
        INSERT INTO fernanda_controle
            (chave, valor, atualizado_em)
        VALUES (%s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            valor = VALUES(valor),
            atualizado_em = NOW()
        """,
        (chave, str(valor)),
    )


def incrementar_contador(cursor, nome, quantidade=1):
    chave = "contador_" + nome
    atual = obter_controle(cursor, chave)
    definir_controle(cursor, chave, int(atual or 0) + quantidade)


def converter_payload(documento):
    payload = documento.get("payload_data")

    if isinstance(payload, dict):
        return payload

    texto = documento.get("payload")
    if isinstance(texto, str):
        try:
            convertido = json.loads(texto)
            if isinstance(convertido, dict):
                return convertido
        except json.JSONDecodeError:
            pass

    return {}


def e_numero(valor):
    return isinstance(valor, (int, float)) and not isinstance(valor, bool)


def normalizar_data(data):
    if not isinstance(data, datetime):
        return None

    if data.tzinfo is not None:
        return data.astimezone(timezone.utc).replace(tzinfo=None)

    return data


def calcular_agregacoes(cursor, no, metrica, instante):
    periodos = [
        (
            "hora",
            instante.replace(minute=0, second=0, microsecond=0),
            timedelta(hours=1),
        ),
        (
            "dia",
            instante.replace(hour=0, minute=0, second=0, microsecond=0),
            timedelta(days=1),
        ),
    ]

    for periodo, inicio, duracao in periodos:
        fim = inicio + duracao

        cursor.execute(
            """
            SELECT MIN(valor), MAX(valor), AVG(valor), COUNT(*)
            FROM fernanda_series
            WHERE no = %s
              AND metrica = %s
              AND instante >= %s
              AND instante < %s
            """,
            (no, metrica, inicio, fim),
        )
        minimo, maximo, media, quantidade = cursor.fetchone()

        cursor.execute(
            """
            INSERT INTO fernanda_agregados
                (no, metrica, periodo, inicio_periodo,
                 valor_minimo, valor_maximo, valor_medio,
                 quantidade, atualizado_em)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                valor_minimo = VALUES(valor_minimo),
                valor_maximo = VALUES(valor_maximo),
                valor_medio = VALUES(valor_medio),
                quantidade = VALUES(quantidade),
                atualizado_em = NOW()
            """,
            (
                no, metrica, periodo, inicio,
                minimo, maximo, media, quantidade,
            ),
        )


def atualizar_redis(no, metrica, valor, instante):
    chave = f"fernanda:{no}:{metrica}"
    registro = {
        "no": no,
        "metrica": metrica,
        "valor": valor,
        "instante": instante.isoformat(),
    }

    existente = redis_client.get(chave)

    if existente:
        try:
            anterior = json.loads(existente)
            instante_anterior = datetime.fromisoformat(
                anterior["instante"]
            )
            if instante_anterior >= instante:
                return
        except (ValueError, KeyError, TypeError, json.JSONDecodeError):
            pass

    redis_client.set(
        chave,
        json.dumps(registro, ensure_ascii=False),
    )


def marcar_documento(cursor, mongo_id, resultado):
    cursor.execute(
        """
        INSERT INTO fernanda_processados
            (mongo_id, resultado)
        VALUES (%s, %s)
        """,
        (mongo_id, resultado),
    )


def processar_documento(documento):
    cursor = conexao_sql.cursor()
    raw_id = str(documento["_id"])
    valores_redis = []
    precisa_atualizar_redis = False

    try:
        # Se este documento já foi registrado, não contabilizamos de novo.
        cursor.execute(
            "SELECT resultado FROM fernanda_processados WHERE mongo_id = %s",
            (raw_id,),
        )
        if cursor.fetchone():
            definir_controle(cursor, "ultimo_id", raw_id)
            conexao_sql.commit()
            return

        payload = converter_payload(documento)
        sender = payload.get("sender") or "desconhecido"
        id_mensagem = payload.get("id")

        if id_mensagem is not None:
            pacote_id = f"{sender}:{id_mensagem}"
        else:
            pacote_id = f"mongo:{raw_id}"

        dados = payload.get("payload")
        if not isinstance(dados, dict):
            dados = {}

        metricas = {
            nome: valor
            for nome, valor in dados.items()
            if nome in ("t", "u") and e_numero(valor)
        }

        # Se o mesmo pacote já foi aceito ou colocado em quarentena,
        # este documento é uma repetição.
        cursor.execute(
            """
            SELECT 1 FROM fernanda_series
            WHERE no = %s AND pacote_id = %s
            LIMIT 1
            """,
            (sender, pacote_id),
        )
        existe_serie = cursor.fetchone() is not None

        cursor.execute(
            """
            SELECT 1 FROM fernanda_quarentena
            WHERE pacote_id = %s
            LIMIT 1
            """,
            (pacote_id,),
        )
        existe_quarentena = cursor.fetchone() is not None

        if existe_serie or existe_quarentena:
            incrementar_contador(cursor, "duplicatas_descartadas")
            marcar_documento(cursor, raw_id, "duplicata_descartada")
            definir_controle(cursor, "ultimo_id", raw_id)
            conexao_sql.commit()
            return

        if not metricas:
            incrementar_contador(cursor, "mensagens_sem_metricas")
            marcar_documento(cursor, raw_id, "sem_metricas")
            definir_controle(cursor, "ultimo_id", raw_id)
            conexao_sql.commit()
            return

        timestamp = payload.get("timestamp")
        recebido = normalizar_data(
            documento.get("received_at") or documento.get("data")
        )

        motivo = None
        instante = None

        if not e_numero(timestamp) or timestamp <= 0:
            motivo = "timestamp_ausente_ou_invalido"
        else:
            try:
                instante = datetime.fromtimestamp(
                    timestamp, tz=timezone.utc
                ).replace(tzinfo=None)
            except (OverflowError, OSError, ValueError):
                motivo = "timestamp_fora_do_intervalo"

        if motivo is None:
            if recebido is None:
                motivo = "horario_recebimento_ausente"
            elif abs((recebido - instante).total_seconds()) > LIMITE_ATRASO_SEGUNDOS:
                motivo = "diferenca_de_horario_superior_a_5_min"

        if motivo:
            cursor.execute(
                """
                INSERT IGNORE INTO fernanda_quarentena
                    (pacote_id, motivo, detalhes, recebido_em)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    pacote_id,
                    motivo,
                    json.dumps(
                        {
                            "mongo_id": raw_id,
                            "sender": sender,
                            "timestamp": timestamp,
                        },
                        ensure_ascii=False,
                        default=str,
                    ),
                    recebido or datetime.utcnow(),
                ),
            )

            if cursor.rowcount:
                incrementar_contador(cursor, "pacotes_quarentena")

                if motivo in (
                    "timestamp_ausente_ou_invalido",
                    "timestamp_fora_do_intervalo",
                ):
                    incrementar_contador(cursor, "timestamps_invalidos")

            marcar_documento(cursor, raw_id, "quarentena")
            definir_controle(cursor, "ultimo_id", raw_id)
            conexao_sql.commit()
            return

        inseridas = 0

        for metrica, valor in metricas.items():
            cursor.execute(
                """
                INSERT IGNORE INTO fernanda_series
                    (no, instante, metrica, valor, pacote_id, recebido_em)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    sender, instante, metrica, float(valor),
                    pacote_id, recebido,
                ),
            )

            if cursor.rowcount:
                inseridas += 1
                calcular_agregacoes(cursor, sender, metrica, instante)
                valores_redis.append((metrica, float(valor)))

        if inseridas == 0:
            incrementar_contador(cursor, "duplicatas_descartadas")
            resultado = "duplicata_descartada"
        else:
            incrementar_contador(cursor, "pacotes_validos")
            resultado = "redis_pendente"
            precisa_atualizar_redis = True

        marcar_documento(cursor, raw_id, resultado)
        definir_controle(cursor, "ultimo_id", raw_id)
        conexao_sql.commit()

    except Exception:
        conexao_sql.rollback()
        raise
    finally:
        cursor.close()

    # O MariaDB já confirmou as séries e o controle antes de atualizar
    # o Redis. Se o Redis falhar, o registro pendente será tentado de novo.
    if precisa_atualizar_redis:
        try:
            for metrica, valor in valores_redis:
                atualizar_redis(sender, metrica, valor, instante)

            cursor = conexao_sql.cursor()
            cursor.execute(
                """
                UPDATE fernanda_processados
                SET resultado = 'processado_valido'
                WHERE mongo_id = %s AND resultado = 'redis_pendente'
                """,
                (raw_id,),
            )
            conexao_sql.commit()
            cursor.close()

        except Exception as erro:
            conexao_sql.rollback()
            print(
                f"Aviso: Redis não confirmado para {raw_id}. "
                f"O registro continuará pendente: {erro}"
            )


def recuperar_redis_pendente():
    cursor = conexao_sql.cursor()
    cursor.execute(
        """
        SELECT mongo_id
        FROM fernanda_processados
        WHERE resultado = 'redis_pendente'
        ORDER BY processado_em
        LIMIT 100
        """
    )
    pendentes = [linha[0] for linha in cursor.fetchall()]
    cursor.close()

    for mongo_id in pendentes:
        cursor = None
        try:
            documento = colecao.find_one({"_id": ObjectId(mongo_id)})

            if documento is None:
                print(f"Aviso: documento pendente não encontrado: {mongo_id}")
                continue

            payload = converter_payload(documento)
            sender = payload.get("sender") or "desconhecido"
            timestamp = payload.get("timestamp")

            if not e_numero(timestamp) or timestamp <= 0:
                continue

            instante = datetime.fromtimestamp(
                timestamp, tz=timezone.utc
            ).replace(tzinfo=None)

            dados = payload.get("payload")
            if not isinstance(dados, dict):
                continue

            for metrica, valor in dados.items():
                if metrica in ("t", "u") and e_numero(valor):
                    atualizar_redis(sender, metrica, float(valor), instante)

            cursor = conexao_sql.cursor()
            cursor.execute(
                """
                UPDATE fernanda_processados
                SET resultado = 'processado_valido'
                WHERE mongo_id = %s AND resultado = 'redis_pendente'
                """,
                (mongo_id,),
            )
            conexao_sql.commit()

        except Exception as erro:
            conexao_sql.rollback()
            print(f"Aviso: recuperação do Redis pendente {mongo_id}: {erro}")

        finally:
            if cursor is not None:
                cursor.close()


def obter_lote():
    """
    Busca documentos que ainda não foram registrados em
    fernanda_processados, inclusive se tiverem IDs antigos.
    A coleção MongoDB é utilizada somente para leitura.
    """
    cursor_sql = conexao_sql.cursor()
    ids_pendentes = []
    lote_ids = []

    try:
        cursor_mongo = colecao.find(
            {},
            {"_id": 1},
        ).sort("_id", 1)

        for documento in cursor_mongo:
            lote_ids.append(documento["_id"])

            if len(lote_ids) >= 500:
                placeholders = ",".join(["%s"] * len(lote_ids))
                cursor_sql.execute(
                    f"""
                    SELECT mongo_id
                    FROM fernanda_processados
                    WHERE mongo_id IN ({placeholders})
                    """,
                    [str(item) for item in lote_ids],
                )

                registrados = {
                    linha[0] for linha in cursor_sql.fetchall()
                }

                ids_pendentes.extend(
                    item for item in lote_ids
                    if str(item) not in registrados
                )
                lote_ids.clear()

                if len(ids_pendentes) >= BATCH_SIZE:
                    break

        if lote_ids and len(ids_pendentes) < BATCH_SIZE:
            placeholders = ",".join(["%s"] * len(lote_ids))
            cursor_sql.execute(
                f"""
                SELECT mongo_id
                FROM fernanda_processados
                WHERE mongo_id IN ({placeholders})
                """,
                [str(item) for item in lote_ids],
            )

            registrados = {
                linha[0] for linha in cursor_sql.fetchall()
            }

            ids_pendentes.extend(
                item for item in lote_ids
                if str(item) not in registrados
            )

    finally:
        cursor_sql.close()

    if not ids_pendentes:
        return []

    ids_pendentes = ids_pendentes[:BATCH_SIZE]

    return list(
        colecao.find(
            {"_id": {"$in": ids_pendentes}}
        ).sort("_id", 1)
    )

def executar(uma_vez=False):
    print("Pipeline iniciado. Origem MongoDB: iot.sensores")
    print("A coleção de origem será utilizada somente para leitura.")

    try:
        while True:
            recuperar_redis_pendente()
            lote = obter_lote()

            if not lote:
                if uma_vez:
                    break
                time.sleep(INTERVALO_SEGUNDOS)
                continue

            for documento in lote:
                processar_documento(documento)

            print(f"Lote processado: {len(lote)} documentos")

            if uma_vez:
                continue

    except KeyboardInterrupt:
        print("\nPipeline interrompido pelo usuário.")
    finally:
        mongo_client.close()
        conexao_sql.close()
        redis_client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--once",
        action="store_true",
        help="Processa todos os documentos pendentes e encerra.",
    )
    argumentos = parser.parse_args()
    executar(uma_vez=argumentos.once)
