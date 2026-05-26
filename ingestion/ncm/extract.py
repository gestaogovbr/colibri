import hashlib
import io
import json
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3

import shared.carregar_json_da_url as cju
from shared.carregar_segredo import carregar_segredo


NCM_URL = "https://portalunico.siscomex.gov.br/classif/api/publico/nomenclatura/download/json"
PREFIXO_RAW = "raw/ncm/"


def _criar_cliente(nome_segredo: str):
    config = carregar_segredo(nome_segredo)
    return boto3.client(
        "s3",
        endpoint_url=config["endpoint"],
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        region_name="auto",
    )


def _calcular_hash(dados: dict) -> str:
    conteudo = json.dumps(dados, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(conteudo.encode()).hexdigest()


def _buscar_dados_anteriores(cliente, bucket: str) -> dict | None:
    resposta = cliente.list_objects_v2(Bucket=bucket, Prefix=PREFIXO_RAW)
    objetos = resposta.get("Contents", [])
    if not objetos:
        return None
    ultimo = max(objetos, key=lambda o: o["LastModified"])
    buffer = io.BytesIO()
    cliente.download_fileobj(bucket, ultimo["Key"], buffer)
    buffer.seek(0)
    return json.loads(buffer.read().decode("utf-8"))["dados"]


# Baixar os dados de NCM
def main(bucket: str, nome_segredo: str) -> dict | None:
    dados = cju.carregar_json_da_url(NCM_URL)
    hash_atual = _calcular_hash(dados)

    cliente = _criar_cliente(nome_segredo)
    dados_anteriores = _buscar_dados_anteriores(cliente, bucket)

    if dados_anteriores is not None and _calcular_hash(dados_anteriores) == hash_atual:
        print("Sem mudanças nos dados de NCM. Pipeline encerrado.")
        return None

    agora = datetime.now(ZoneInfo("America/Sao_Paulo"))
    json_final = {
        "metadados": {
            "hora_da_extracao": agora.strftime("%Y-%m-%dT%H:%M:%S"),
            "url": NCM_URL,
        },
        "dados": dados,
    }

    chave = f"{PREFIXO_RAW}ncm_{agora.strftime('%Y-%m-%d-%H%M%S')}.json"
    buffer = io.BytesIO(json.dumps(json_final, indent=4, ensure_ascii=False).encode("utf-8"))
    cliente.upload_fileobj(buffer, bucket, chave)
    print(f"Novo arquivo salvo em: {chave}")

    return dados

if __name__ == "__main__":
    main("colibri-arquivos", "colibri-token-desenvolvedor")