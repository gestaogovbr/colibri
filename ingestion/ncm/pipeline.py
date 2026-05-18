from pipe import Pipe

from ingestion.ncm import extract, transform, transform_prefixos, load


BUCKET_RAW = "colibri-arquivos"
BUCKET_LAKE = "colibri-dev"
NOME_SEGREDO = "colibri-token-desenvolvedor"
CAMINHO_META = f"s3://{BUCKET_LAKE}/meta.ducklake"
DATA_PATH = f"s3://{BUCKET_LAKE}/lake/"


def main():
    dados = extract.main(BUCKET_RAW, NOME_SEGREDO)
    if dados is None:
        return

    prefixos = (
        dados
        | Pipe(transform.criar_mapa_ncm)
        | Pipe(transform.criar_arvore_ncm)
        | Pipe(transform.gerar_ncm_enriquecido)
        | Pipe(transform_prefixos.gerar_prefixos)
    )

    load.main(prefixos, CAMINHO_META, DATA_PATH, NOME_SEGREDO)


if __name__ == "__main__":
    main()
