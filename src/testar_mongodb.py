
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

load_dotenv()

mongo_uri = os.getenv("MONGO_URI")

if not mongo_uri:
    raise SystemExit(
        "Erro: configure MONGO_URI no arquivo .env"
    )

try:
    client = MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=5000
    )

    # Verifica se o servidor responde.
    client.admin.command("ping")
    print("Conexão com MongoDB estabelecida!")

    # Acessa o banco e a coleção definidos na atividade.
    db = client["iot"]
    colecao = db["sensores"]

    print("Banco:", db.name)
    print("Coleção:", colecao.name)

    # Consulta somente um documento para inspecionar a estrutura.
    documento = colecao.find_one(
        {"payload_data.payload.t": {"$exists": True}},
        {"payload": 0}
    )

    if documento:
        print("Encontramos um documento com temperatura.")
        print("Campos disponíveis:", list(documento.keys()))
        print("Remetente:", documento.get(
            "payload_data", {}
        ).get("sender"))
        print("Recebido em:", documento.get("received_at"))
    else:
        print("Nenhum documento com temperatura foi encontrado.")

except PyMongoError as erro:
    print("Não foi possível consultar o MongoDB.")
    print("Verifique o endereço, a rede e as credenciais.")
    print("Detalhe técnico:", type(erro).__name__)

finally:
    if "client" in globals():
        client.close()
