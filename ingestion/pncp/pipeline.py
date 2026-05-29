from ingestion.pncp.bronze import extract_compra, extract_compra_item, extract_item_resultado


NOME_SEGREDO = "colibri-token-desenvolvedor"
BUCKET = "colibri-dev"
CAMINHO_META = f"s3://{BUCKET}/meta.ducklake"
DATA_PATH = f"s3://{BUCKET}/lake/"
ANOS = range(2021, 2027)

VIEWS_COMPRA = [
    "VW_FT_PNCP_COMPRA",
]

VIEWS_COMPRA_ITEM = [
    "VW_FT_PNCP_COMPRA_ITEM",
]

VIEWS_ITEM_RESULTADO = [
    "VW_DM_PNCP_ITEM_RESULTADO",
]


def main():
    extract_compra.main(VIEWS_COMPRA, ANOS, CAMINHO_META, DATA_PATH, NOME_SEGREDO)
    extract_compra_item.main(VIEWS_COMPRA_ITEM, ANOS, CAMINHO_META, DATA_PATH, NOME_SEGREDO)
    extract_item_resultado.main(VIEWS_ITEM_RESULTADO, ANOS, CAMINHO_META, DATA_PATH, NOME_SEGREDO)


if __name__ == "__main__":
    main()
