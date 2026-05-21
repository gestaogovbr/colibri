from ingestion.pncp import load


NOME_SEGREDO = "colibri-token-desenvolvedor"
BUCKET = "colibri-dev"
CAMINHO_META = f"s3://{BUCKET}/meta.ducklake"
DATA_PATH = f"s3://{BUCKET}/lake/"
ANOS = range(2021, 2027)

VIEWS = [
    "VW_FT_PNCP_COMPRA",
]


def main():
    load.main(VIEWS, ANOS, CAMINHO_META, DATA_PATH, NOME_SEGREDO)


if __name__ == "__main__":
    main()
