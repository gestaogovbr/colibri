"""Extração local e isolada dos arquivos PNCP publicados pela DELOG.

Exemplo (a partir da raiz do Colibri):
    python -m ingestion.comprasgov.extract --ano 2025 --mes 8

Não acessa o R2, não executa dbt e não modifica o pipeline legado.
"""

import argparse
import csv
import hashlib
import logging
import os
import shutil
import tempfile
import zipfile
from pathlib import Path

import duckdb
import requests

from utils.manifesto_duckdb import consultar_entrada, inicializar_manifesto, registrar_versao

URL_BASE = "https://repositorio.dados.gov.br/seges/comprasgov/pncp"
TIPOS = {
    "compras": "compra",
    "itens": "compra_item",
    "resultados": "item_resultado",
}
DIRETORIO_SAIDA = Path("dados/pncp_comprasgov_delog")
LOGGER = logging.getLogger(__name__)


def validar_csv(caminho):
    """Valida a estrutura do CSV antes da conversão para Parquet."""
    with open(caminho, encoding="utf-8-sig", newline="") as arquivo:
        leitor = csv.reader(arquivo, delimiter=";", strict=True)

        # //---- Obtem o cabecalho e avanca o iterador ----//
        cabecalho = next(leitor, None)

        if not cabecalho:
            raise ValueError("CSV sem cabeçalho")

        # //---- Descobre quantas colunas cada registro deve possuir ----//
        total_colunas = len(cabecalho)

        # //---- Percorre os registros, comecando no 2 ----//
        for numero_linha, registro in enumerate(leitor, start=2):
            if len(registro) != total_colunas:
                raise ValueError(
                    f"CSV inválido na linha {numero_linha}: "
                    f"esperadas {total_colunas} colunas, encontradas {len(registro)}"
                )


def identificar_snapshot(caminho: Path, tipo: str, ano: int, mes: int):
    """Calcular o SHA-256 e definir o nome da versão imutável do arquivo Parquet."""

    with caminho.open("rb") as arquivo:
        hash_sha256 = hashlib.file_digest(
            arquivo,
            "sha256",
        ).hexdigest()

    nome = f"{tipo}_{ano}_{mes:02d}_{hash_sha256[:10]}.parquet"

    destino = DIRETORIO_SAIDA / tipo / str(ano) / f"{mes:02d}" / "versoes" / nome

    return destino, hash_sha256


def publicar_snapshot(
    arquivo_parquet: Path,
    tipo: str,
    ano: int,
    mes: int,
):
    """Publica uma versão imutável, identificada pelo SHA-256."""

    destino, hash_256 = identificar_snapshot(arquivo_parquet, tipo, ano, mes)

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

        # Validar o Parquet
        with duckdb.connect(":memory:") as conexao:
            conexao.execute(
                "SELECT COUNT(*) FROM read_parquet(?)",
                [str(temporario)],
            ).fetchone()

        # Validar a integridade da cópia
        with temporario.open("rb") as arquivo:
            hash_temporario = hashlib.file_digest(
                arquivo,
                "sha256",
            ).hexdigest()

        if hash_temporario != hash_256:
            raise ValueError(f"Falha de integridade na cópia: {temporario}")

        # Publicar o snapshot sem sobrescrever
        try:
            os.link(temporario, destino)

        except FileExistsError as err:
            with destino.open("rb") as existente:
                hash_existente = hashlib.file_digest(
                    existente,
                    "sha256",
                ).hexdigest()

            if hash_existente != hash_256:
                raise ValueError(f"Colisão de hash ou snapshot corrompido: {destino}") from err

            LOGGER.info("Snapshot já armazenado: %s", destino)

        return destino, hash_256

    finally:
        if temporario is not None:
            temporario.unlink(missing_ok=True)


def _parquet_valido(caminho: Path) -> bool:
    """Verifica se o arquivo Parquet existe e pode ser lido pelo DuckDB."""

    try:
        with duckdb.connect(":memory:") as conexao:
            conexao.execute(
                "SELECT COUNT(*) FROM read_parquet(?)",
                [str(caminho)],
            ).fetchone()
        return True
    except (duckdb.Error, OSError) as erro:
        LOGGER.warning(
            "Parquet inválido: %s (%s)",
            caminho,
            erro,
        )
        return False


def processar_tipo_detalhado(
    sessao: requests.Session,
    ano: int,
    mes: int,
    tipo: str,
) -> dict:
    """Baixa um ZIP, converte o CSV em Parquet e arquiva o snapshot por SHA-256."""
    if not 1 <= mes <= 12:
        raise ValueError("O mês deve estar entre 1 e 12")

    prefixo = TIPOS[tipo]
    nome_zip = f"{prefixo}_{ano}_{mes:02d}.csv.zip"
    url = f"{URL_BASE}/{ano}/{mes:02d}/{nome_zip}"

    # //---- 1. Consulta metadados remotos ----//
    try:
        metadados_http = consultar_metadados(sessao, url)
    except requests.RequestException as erro:
        LOGGER.error("Falha ao consultar metadados de %s: %s", url, erro)
        return {"status": "erro"}

    if metadados_http is None:
        return {"status": "indisponivel"}

    # //---- 3. Consulta o manifesto local para comparar com os metadados remotos ----//
    inicializar_manifesto(pipeline="comprasgov_pncp")
    entrada_manifesto = consultar_entrada(pipeline="comprasgov_pncp", tipo=tipo, ano=ano, mes=mes)

    caminho_baseline = entrada_manifesto.get("caminho_snapshot") if entrada_manifesto else None

    hash_baseline = entrada_manifesto.get("hash_sha256") if entrada_manifesto else None

    if caminho_baseline and hash_baseline and not precisa_atualizar(metadados_http, entrada_manifesto):
        baseline = Path(caminho_baseline)

        if baseline.is_file():
            with baseline.open("rb") as arquivo:
                hash_local = hashlib.file_digest(
                    arquivo,
                    "sha256",
                ).hexdigest()

            if hash_local == hash_baseline and _parquet_valido(baseline):
                LOGGER.info(
                    "Baseline confirmado e ETag inalterado: %s",
                    baseline,
                )
                return {
                    "status": "existente",
                    "hash_sha256": hash_baseline,
                    "caminho_snapshot": str(baseline),
                    "etag": metadados_http.get("etag"),
                    "last_modified": metadados_http.get("last_modified"),
                }

    # //---- 4. Download e conversao em diretorio temporario isolado ----//
    with tempfile.TemporaryDirectory(prefix="colibri_comprasgov_") as pasta:
        pasta_temp = Path(pasta)
        arquivo_zip = pasta_temp / nome_zip
        arquivo_csv = pasta_temp / f"{prefixo}_{ano}_{mes:02d}.csv"
        arquivo_parquet = pasta_temp / "resultado.parquet"

        try:
            with sessao.get(url, stream=True, timeout=(15, 120)) as resposta:
                if resposta.status_code == 404:
                    LOGGER.warning("Não disponível (404): %s", url)
                    return {"status": "indisponivel"}
                resposta.raise_for_status()
                with arquivo_zip.open("wb") as saida:
                    for bloco in resposta.iter_content(chunk_size=1024 * 1024):
                        if bloco:
                            saida.write(bloco)
        except requests.RequestException as erro:
            LOGGER.error("Falha no download de %s: %s", url, erro)
            return {"status": "erro"}

        try:
            with zipfile.ZipFile(arquivo_zip) as compactado:
                csvs = [
                    nome for nome in compactado.namelist() if nome.lower().endswith(".csv") and not nome.endswith("/")
                ]
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
                    SELECT *
                    FROM read_csv(
                        '{caminho_csv}',
                        delim=';',
                        header=true,
                        all_varchar=true
                    )
                """)

                quantidade = conexao.execute("SELECT COUNT(*) FROM dados").fetchone()[0]

                conexao.execute(f"""
                    COPY (SELECT * FROM dados)
                    TO '{caminho_parquet}'
                    (FORMAT PARQUET, COMPRESSION ZSTD)
                """)

            destino, hash_sha256 = publicar_snapshot(
                arquivo_parquet,
                tipo,
                ano,
                mes,
            )

            registrar_versao(
                pipeline="comprasgov_pncp",
                tipo=tipo,
                ano=ano,
                mes=mes,
                hash_sha256=hash_sha256,
                caminho_snapshot=str(destino),
                etag=metadados_http.get("etag"),
                last_modified=metadados_http.get("last_modified"),
            )

            LOGGER.info(
                "%s: %s linhas -> %s (SHA_256: %s)",
                tipo,
                quantidade,
                destino,
                hash_sha256,
            )

            return {
                "status": "criado",
                "hash_sha256": hash_sha256,
                "caminho_snapshot": str(destino),
                "etag": metadados_http.get("etag"),
                "last_modified": metadados_http.get("last_modified"),
            }

        except (OSError, ValueError, csv.Error, zipfile.BadZipFile, duckdb.Error) as erro:
            LOGGER.error("Falha ao processar %s: %s", nome_zip, erro)
            return {"status": "erro"}


def processar_tipo(
    sessao: requests.Session,
    ano: int,
    mes: int,
    tipo: str,
) -> str:
    """Processa um arquivo e retorna seu status."""

    resultado = processar_tipo_detalhado(sessao, ano, mes, tipo)

    return resultado["status"]


def consultar_metadados(sessao: requests.Session, url: str) -> dict | None:
    """Consulta metadados HTTP sem baixar o arquivo. Levanta RequestException em caso de falha."""
    resposta = sessao.head(
        url,
        allow_redirects=True,
        timeout=(15, 30),
    )

    if resposta.status_code == 404:
        LOGGER.warning("Arquivo não disponível: %s", url)
        return None

    resposta.raise_for_status()

    return {
        "etag": resposta.headers.get("ETag"),
        "last_modified": resposta.headers.get("Last-Modified"),
        "content_length": resposta.headers.get("Content-Length"),
    }


def precisa_atualizar(
    metadados_remotos: dict,
    entrada_manifesto: dict | None,
) -> bool:
    """Determina se uma versão remota precisa ser processada."""
    if entrada_manifesto is None:
        return True

    etag_remoto = metadados_remotos.get("etag")
    etag_processado = entrada_manifesto.get("etag")

    if not etag_remoto or not etag_processado:
        return True

    return etag_remoto != etag_processado


def main() -> None:
    parser = argparse.ArgumentParser(description="Teste local da nova ingestão PNCP/DELOG")
    parser.add_argument("--ano", type=int, required=True)
    parser.add_argument("--mes", type=int, choices=range(1, 13), metavar="1..12", required=True)
    parser.add_argument("--tipo", choices=TIPOS, help="Opcional: processar somente um tipo")
    argumentos = parser.parse_args()
    if argumentos.ano < 2021:
        parser.error("Informe um ano válido (2021 ou posterior).")

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    tipos = [argumentos.tipo] if argumentos.tipo else list(TIPOS)
    with requests.Session() as sessao:
        sessao.headers.update({"User-Agent": "colibri-ingestao-comprasgov/1.0"})
        resultados = {tipo: processar_tipo(sessao, argumentos.ano, argumentos.mes, tipo) for tipo in tipos}
    LOGGER.info("Resumo: %s", resultados)
    if "erro" in resultados.values():
        raise SystemExit(1)


if __name__ == "__main__":
    main()
