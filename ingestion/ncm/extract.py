from datetime import datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import shared.carregar_json_da_url as cju
import shared.salvar_arquivo as sa


NCM_URL = (
    "https://portalunico.siscomex.gov.br/classif/api/publico/nomenclatura/download/json"
)


def main():
    dados = cju.carregar_json_da_url(NCM_URL)
    timestamp = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    timestamp_limpo = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime(
        "%Y-%m-%d-%H%M%S"
    )
    metadados = {
        "hora_da_extracao": timestamp,
        "url": NCM_URL,
    }
    json_final = {
        "metadados": metadados,
        "dados": dados,
    }

    with open("ncm.json", "w", encoding="utf-8") as f:
        json.dump(json_final, f, indent=4, ensure_ascii=False)

    sa.enviar_com_timestamp(
        "ncm.json",
        "colibri-arquivos",
        "colibri-token-desenvolvedor",
        timestamp=timestamp_limpo,
    )

    arquivo = Path("ncm.json")

    if arquivo.exists():
        arquivo.unlink()


if __name__ == "__main__":
    main()
