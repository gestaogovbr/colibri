from pipe import Pipe

from ingestion.ncm.bronze import extract
from ingestion.ncm.bronze import load as bronze_load
from ingestion.ncm.silver import transform
from ingestion.ncm.silver import load as silver_load
from ingestion.ncm.gold import transform_prefixos
from ingestion.ncm.gold import load as gold_load


BUCKET_RAW = "colibri-arquivos"
BUCKET_LAKE = "colibri-dev"
NOME_SEGREDO = "colibri-token-desenvolvedor"
CAMINHO_META = f"s3://{BUCKET_LAKE}/meta.ducklake"
DATA_PATH = f"s3://{BUCKET_LAKE}/lake/"


def main():
    dados = extract.main(BUCKET_LAKE, NOME_SEGREDO)
    if dados is None:
        return

    bronze_load.main(dados, CAMINHO_META, DATA_PATH, NOME_SEGREDO)

    enriquecido = (
        dados
        | Pipe(transform.criar_mapa_ncm)
        | Pipe(transform.criar_arvore_ncm)
        | Pipe(transform.gerar_ncm_enriquecido)
    )

    silver_load.main(enriquecido, CAMINHO_META, DATA_PATH, NOME_SEGREDO)

    prefixos = enriquecido | Pipe(transform_prefixos.gerar_prefixos)

    gold_load.main(prefixos, CAMINHO_META, DATA_PATH, NOME_SEGREDO)


if __name__ == "__main__":
    main()
