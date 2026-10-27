"""Extração PNCP/DELOG isolada, com upload opcional ao R2 de desenvolvimento.

Uso:
  python -m ingestion.pncp_comprasgov.extract_delog --ano 2025 --mes 8
  python -m ingestion.pncp_comprasgov.extract_delog --ano 2025 --mes 8 --upload --bucket colibri-dev

Não modifica o pipeline legado, não executa dbt nem escreve no DuckLake.
"""

import argparse
import csv
import logging
import os
import shutil
import tempfile
import zipfile
from pathlib import Path

import duckdb
import requests
from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError

from utils.carregar_segredo import carregar_segredo
from utils.constantes import BUCKET_DESENVOLVIMENTO, NOME_SEGREDO_DESENVOLVEDOR
from utils.criar_cliente import criar_cliente
from utils.salvar_arquivo_no_bucket import salvar_arquivo_no_bucket

URL_BASE = "https://repositorio.dados.gov.br/seges/comprasgov/pncp"
TIPOS = {"compras": "compra", "itens": "compra_item", "resultados": "item_resultado"}
DIRETORIO_SAIDA = Path("dados/pncp_comprasgov_delog")
LOGGER = logging.getLogger(__name__)


def caminho_destino(tipo: str, ano: int, mes: int) -> Path:
    return DIRETORIO_SAIDA / tipo / str(ano) / f"{mes:02d}" / f"{tipo}_{ano}_{mes:02d}.parquet"


def validar_csv(caminho):
    """Valida a estrutura do CSV antes da conversão para PArquet."""
    with open(caminho, encoding="utf-8-sig", newline="") as arquivo:
        leitor = csv.reader(arquivo, delimiter=";", strict=True)

        # Obtém o cabeçalho e avança o iterador.
        cabecalho = next(leitor, None)

        if not cabecalho:
            raise ValueError("CSV sem cabeçalho")

        # Descobre quantas colunas cada registro deve possuir.
        total_colunas = len(cabecalho)

        # Percorre os registros, começando no 2.
        for numero_linha, registro in enumerate(leitor, start=2):
            # Confere se a quantidade de registros está correta.
            if len(registro) != total_colunas:
                raise ValueError(
                    f"CSV inválido na linha {numero_linha}: "
                    f"esperadas {total_colunas} colunas, encontradas {len(registro)}"
                )


def processar_tipo(sessao: requests.Session, ano: int, mes: int, tipo: str) -> str:
    """Baixa ZIP e converte CSV em Parquet, sem sobrescrever arquivos locais."""
    if not 1 <= mes <= 12:
        raise ValueError("O mês deve estar entre 1 e 12")

    destino = caminho_destino(tipo, ano, mes)
    if destino.exists():
        try:
            with duckdb.connect(":memory:") as conexao:
                conexao.execute(
                    "SELECT COUNT(*) FROM read_parquet(?)",
                    [str(destino)],
                ).fetchone()

            LOGGER.info("Parquet existente e válido: %s", destino)
            return "existente"

        except (duckdb.Error, OSError) as erro:
            LOGGER.warning(
                "Parquet existente inválido, será recuperado: %s (%s)",
                destino,
                erro,
            )
        destino.unlink()

    prefixo = TIPOS[tipo]
    nome_zip = f"{prefixo}_{ano}_{mes:02d}.csv.zip"
    url = f"{URL_BASE}/{ano}/{mes:02d}/{nome_zip}"

    with tempfile.TemporaryDirectory(prefix="colibri_delog_") as pasta:
        pasta_temp = Path(pasta)
        arquivo_zip = pasta_temp / nome_zip
        arquivo_csv = pasta_temp / f"{prefixo}_{ano}_{mes:02d}.csv"
        arquivo_parquet = pasta_temp / "resultado.parquet"

        try:
            with sessao.get(url, stream=True, timeout=(15, 120)) as resposta:
                if resposta.status_code == 404:
                    LOGGER.warning("Não disponível (404): %s", url)
                    return "indisponivel"
                resposta.raise_for_status()
                with arquivo_zip.open("wb") as saida:
                    for bloco in resposta.iter_content(chunk_size=1024 * 1024):
                        if bloco:
                            saida.write(bloco)
        except requests.RequestException as erro:
            LOGGER.error("Falha no download de %s: %s", url, erro)
            return "erro"

        try:
            with zipfile.ZipFile(arquivo_zip) as compactado:
                csvs = [n for n in compactado.namelist() if n.lower().endswith(".csv") and not n.endswith("/")]
                if len(csvs) != 1:
                    raise ValueError(f"Esperado um CSV no ZIP; encontrados {len(csvs)}")
                with compactado.open(csvs[0]) as origem, arquivo_csv.open("wb") as saida:
                    shutil.copyfileobj(origem, saida)
            if arquivo_csv.stat().st_size == 0:
                raise ValueError("CSV Vazio: arquivo sem cabeçalho ou registros")

            validar_csv(arquivo_csv)

            with duckdb.connect(":memory:") as conexao:
                caminho_csv = str(arquivo_csv).replace("'", "''")
                caminho_parquet = str(arquivo_parquet).replace("'", "''")
                conexao.execute(f"""
                    CREATE TEMP VIEW dados AS
                    SELECT * FROM read_csv('{caminho_csv}', delim=';', header=true, all_varchar=true)
                """)
                quantidade = conexao.execute("SELECT COUNT(*) FROM dados").fetchone()[0]
                conexao.execute(f"""
                    COPY (SELECT * FROM dados)
                    TO '{caminho_parquet}'
                    (FORMAT PARQUET, COMPRESSION ZSTD)
                """)

            destino.parent.mkdir(parents=True, exist_ok=True)
            temporario = None

            try:
                with tempfile.NamedTemporaryFile(
                    mode="wb",
                    suffix=".parquet.part",
                    prefix=f".{destino.stem}_",
                    dir=destino.parent,
                    delete=False,
                ) as saida:
                    temporario = Path(saida.name)

                    with arquivo_parquet.open("rb") as origem:
                        shutil.copyfileobj(origem, saida)

                # Verifica se o Parquet temporário é legível.
                with duckdb.connect(":memory:") as conexao:
                    conexao.execute(
                        "SELECT COUNT(*) FROM read_parquet(?)",
                        [str(temporario)],
                    ).fetchone()

                # Publica o arquivo sem sobrescrever outro existente.
                try:
                    os.link(temporario, destino)

                except FileExistsError:
                    LOGGER.info("Já criado por outra execução: %s", destino)
                    return "existente"

                LOGGER.info("%s: %s linhas -> %s", tipo, quantidade, destino)
                return "criado"

            finally:
                if temporario is not None:
                    temporario.unlink(missing_ok=True)

        except (OSError, ValueError, csv.Error, zipfile.BadZipFile, duckdb.Error) as erro:
            LOGGER.error("Falha ao processar %s: %s", nome_zip, erro)
            return "erro"


def enviar_para_r2(tipo: str, ano: int, mes: int, bucket: str) -> str:
    """Envia um Parquet local para o R2 de desenvolvimento."""

    if bucket != BUCKET_DESENVOLVIMENTO:
        raise ValueError(f"Upload permitido apenas no bucket {BUCKET_DESENVOLVIMENTO}")

    arquivo = caminho_destino(tipo, ano, mes)

    if not arquivo.is_file():
        LOGGER.error("Arquivo local não encontrado: %s", arquivo)
        return "erro"

    try:
        with duckdb.connect(":memory:") as conexao:
            conexao.execute(
                "SELECT COUNT(*) FROM read_parquet(?)",
                [str(arquivo)],
            ).fetchone()

    except (duckdb.Error, OSError) as erro:
        LOGGER.error("Parquet inválido, upload cancelado: %s", erro)
        return "erro"

    chave = arquivo.relative_to(DIRETORIO_SAIDA.parent).as_posix()

    config = carregar_segredo(NOME_SEGREDO_DESENVOLVEDOR)
    cliente = criar_cliente(config)

    try:
        cliente.head_object(Bucket=bucket, Key=chave)
        LOGGER.info("Arquivo já existe no R2: s3://%s/%s", bucket, chave)
        return "existente"

    except ClientError as erro:
        codigo = erro.response.get("Error", {}).get("Code", "")

        if codigo not in ("404", "NoSuchKey", "NotFound"):
            LOGGER.error("Erro ao consultar R2: %s", erro)
            return "erro"

    except (NoCredentialsError, EndpointConnectionError) as erro:
        LOGGER.error("Falha de autenticação ou conexão com R2: %s", erro)
        return "erro"

    try:
        salvar_arquivo_no_bucket(
            caminho_arquivo=str(arquivo),
            bucket_name=bucket,
            nome_segredo=NOME_SEGREDO_DESENVOLVEDOR,
            nome_no_bucket=chave,
        )

        cliente.head_object(Bucket=bucket, Key=chave)

        LOGGER.info("Upload confirmado: s3://%s/%s", bucket, chave)
        return "enviado"

    except Exception as erro:
        LOGGER.error("Falha no upload de %s: %s", arquivo, erro)
        return "erro"


def main():
    parser = argparse.ArgumentParser(description="Extração PNCP/DELOG com upload opcional ao R2")

    parser.add_argument("--ano", type=int, required=True)
    parser.add_argument("--mes", type=int, choices=range(1, 13), required=True)
    parser.add_argument(
        "--tipo",
        choices=list(TIPOS),
        help="Processa apenas um tipo de arquivo",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Envia os Parquets ao R2 de desenvolvimento",
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help="Bucket de destino do upload",
    )

    args = parser.parse_args()

    if args.ano < 2021:
        parser.error("O ano deve ser igual ou superior a 2021")

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    if args.upload and args.bucket != BUCKET_DESENVOLVIMENTO:
        parser.error(f"Upload permitido apenas com --bucket {BUCKET_DESENVOLVIMENTO}")

    if args.bucket and not args.upload:
        parser.error("--bucket exige --upload")

    tipos = [args.tipo] if args.tipo else list(TIPOS)
    resumo = {}

    with requests.Session() as sessao:
        for tipo in tipos:
            resultado = processar_tipo(
                sessao,
                args.ano,
                args.mes,
                tipo,
            )

            if args.upload and resultado in ("criado", "existente"):
                try:
                    resultado_upload = enviar_para_r2(
                        tipo,
                        args.ano,
                        args.mes,
                        args.bucket,
                    )
                except Exception:
                    LOGGER.exception("Erro inesperado no upload do tipo %s", tipo)
                    resultado_upload = "erro"

                resumo[tipo] = {
                    "extracao": resultado,
                    "upload": resultado_upload,
                }
            else:
                resumo[tipo] = {"extracao": resultado}

    LOGGER.info("Resumo: %s", resumo)

    if any(resultado.get("extracao") == "erro" or resultado.get("upload") == "erro" for resultado in resumo.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
