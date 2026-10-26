"""Extração local e isolada dos arquivos PNCP publicados pela DELOG.

Exemplo (a partir da raiz do Colibri):
    python -m ingestion.pncp_comprasgov.extract_delog --ano 2025 --mes 8

Não acessa o R2, não executa dbt e não modifica o pipeline legado.
"""

import argparse
import logging
import os
import shutil
import tempfile
import zipfile
from pathlib import Path

import duckdb
import requests

URL_BASE = "https://repositorio.dados.gov.br/seges/comprasgov/pncp"
TIPOS = {
    "compras": "compra",
    "itens": "compra_item",
    "resultados": "item_resultado",
}
DIRETORIO_SAIDA = Path("dados/pncp_comprasgov_delog")
LOGGER = logging.getLogger(__name__)


def processar_tipo(sessao: requests.Session, ano: int, mes: int, tipo: str) -> str:
    """Baixa um ZIP e converte o CSV interno em Parquet, sem sobrescrever dados."""
    prefixo = TIPOS[tipo]
    nome_zip = f"{prefixo}_{ano}_{mes:02d}.csv.zip"
    url = f"{URL_BASE}/{ano}/{mes:02d}/{nome_zip}"
    destino = DIRETORIO_SAIDA / tipo / str(ano) / f"{mes:02d}" / f"{tipo}_{ano}_{mes:02d}.parquet"

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

    # Todos os temporários são apagados automaticamente, inclusive após erros.
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
                csvs = [
                    nome for nome in compactado.namelist() if nome.lower().endswith(".csv") and not nome.endswith("/")
                ]
                if len(csvs) != 1:
                    raise ValueError(f"Esperado um CSV no ZIP; encontrados {len(csvs)}")
                with compactado.open(csvs[0]) as origem, arquivo_csv.open("wb") as saida:
                    shutil.copyfileobj(origem, saida)

            # all_varchar evita inferências de tipo inconsistentes entre meses.
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

            destino.parent.mkdir(parents=True, exist_ok=True)
            temporario = None
            # Sem sobrescrita de arquivos existentes; testes usam período isolado.

            try:
                # 1. Grava em arquivo temporário no mesmo diretório.
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

                # 2. Valida o Parquet antes da publicação.
                with duckdb.connect(":memory:") as conexao:
                    conexao.execute(
                        "SELECT COUNT(*) FROM read_parquet(?)",
                        [str(temporario)],
                    ).fetchone()

                # 3. Publica sem sobrescrever arquivos existentes.
                try:
                    os.link(temporario, destino)

                except FileExistsError:
                    LOGGER.info(
                        "Arquivo já criado por outra execução: %s",
                        destino,
                    )
                    return "existente"

                LOGGER.info(
                    "%s: %s linhas -> %s",
                    tipo,
                    quantidade,
                    destino,
                )
                return "criado"

            finally:
                if temporario is not None:
                    temporario.unlink(missing_ok=True)

        except (OSError, ValueError, zipfile.BadZipFile, duckdb.Error) as erro:
            # Não deixa um arquivo incompleto ser confundido com saída válida.
            if destino.exists() and destino.stat().st_size == 0:
                destino.unlink()
            LOGGER.error("Falha ao processar %s: %s", nome_zip, erro)
            return "erro"


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
        sessao.headers.update({"User-Agent": "colibri-ingestao-delog/1.0"})
        resultados = {tipo: processar_tipo(sessao, argumentos.ano, argumentos.mes, tipo) for tipo in tipos}
    LOGGER.info("Resumo: %s", resultados)
    if "erro" in resultados.values():
        raise SystemExit(1)


if __name__ == "__main__":
    main()
