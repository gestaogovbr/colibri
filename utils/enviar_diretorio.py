import fnmatch
import html
import logging
import mimetypes
import os
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import click
from boto3.s3.transfer import TransferConfig

import utils.configurar_logging as log
from utils.carregar_segredo import carregar_segredo
from utils.criar_cliente import criar_cliente

log.setup_logging()
logger = logging.getLogger(__name__)

# Destino padrão: repositório indicadores-encp (ao lado do colibri) -> bucket indicadores-encp
DIRETORIO_PADRAO = "../indicadores-encp"
BUCKET_PADRAO = "indicadores-encp"
SEGREDO_PADRAO = "colibri-token-indicadores-encp"
ARQUIVO_SEGREDOS_PADRAO = ".segredos_indicadores_encp.yml"

# Padrões ignorados por padrão (comparados com cada parte do caminho relativo)
IGNORAR_PADRAO = (".git", ".DS_Store", ".Rproj.user", "Thumbs.db", "~$*")

# Arquivos grandes sobem em partes de 64 MB, com até 4 partes simultâneas por arquivo
CONFIG_TRANSFERENCIA = TransferConfig(
    multipart_threshold=64 * 1024 * 1024,
    multipart_chunksize=64 * 1024 * 1024,
    max_concurrency=4,
)


# Extensões já comprimidas: entram no zip sem recompressão
SEM_COMPRESSAO = {".parquet", ".pdf", ".zip", ".gz", ".png", ".jpg", ".jpeg", ".xlsx", ".docx", ".pptx"}

mimetypes.add_type("text/markdown", ".md")
mimetypes.add_type("text/plain", ".R")
mimetypes.add_type("text/plain", ".Rproj")
mimetypes.add_type("application/vnd.apache.parquet", ".parquet")


def _tipo_conteudo(caminho: str) -> str:
    """
    Content-Type a partir da extensão, para o navegador abrir HTML/PDF/MD em vez de baixar
    """
    tipo = mimetypes.guess_type(caminho)[0] or "application/octet-stream"
    return f"{tipo}; charset=utf-8" if tipo.startswith("text/") else tipo


def _tamanho(n: int) -> str:
    for unidade in ("B", "KB", "MB", "GB"):
        if n < 1024 or unidade == "GB":
            return f"{n:.0f} {unidade}" if unidade == "B" else f"{n:.1f} {unidade}"
        n /= 1024


def _ignorado(caminho_relativo: Path, padroes: tuple[str, ...]) -> bool:
    return any(fnmatch.fnmatch(parte, p) for parte in caminho_relativo.parts for p in padroes)


def _listar_remotos(cliente, bucket_name: str, prefixo: str) -> dict[str, int]:
    """
    Retorna {chave: tamanho} dos objetos já presentes no bucket sob o prefixo
    """
    paginator = cliente.get_paginator("list_objects_v2")
    return {
        obj["Key"]: obj["Size"]
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefixo)
        for obj in page.get("Contents", [])
    }


def _listar_locais(diretorio: Path, padroes: tuple[str, ...]) -> dict[str, Path]:
    """
    Retorna {caminho relativo POSIX: caminho absoluto} dos arquivos não ignorados
    """
    locais = {}
    for raiz, pastas, arquivos in os.walk(diretorio):
        relativo_raiz = Path(raiz).relative_to(diretorio)
        # Poda as pastas ignoradas para não descer nelas (ex: .git)
        pastas[:] = sorted(p for p in pastas if not _ignorado(relativo_raiz / p, padroes))
        for nome in sorted(arquivos):
            relativo = relativo_raiz / nome
            if not _ignorado(relativo, padroes):
                locais[relativo.as_posix()] = Path(raiz) / nome
    return locais


def _gerar_zip(locais: dict[str, Path], destino: Path, pasta_raiz: str) -> None:
    with zipfile.ZipFile(destino, "w", allowZip64=True) as zf:
        for relativo, caminho in locais.items():
            comprimir = caminho.suffix.lower() not in SEM_COMPRESSAO
            zf.write(
                caminho,
                f"{pasta_raiz}/{relativo}",
                compress_type=zipfile.ZIP_DEFLATED if comprimir else zipfile.ZIP_STORED,
                compresslevel=6 if comprimir else None,
            )


def _gerar_indice(titulo: str, arquivos: dict[str, int], nome_zip: str, tamanho_zip: int) -> str:
    """
    Monta o index.html com os arquivos agrupados pela pasta de primeiro nível
    """
    grupos: dict[str, list[str]] = {}
    for relativo in arquivos:
        pasta = relativo.split("/", 1)[0] if "/" in relativo else ""
        grupos.setdefault(pasta, []).append(relativo)

    def _linha(relativo: str) -> str:
        return (
            f'<li><a href="{quote(relativo)}">{html.escape(relativo)}</a>'
            f"<span>{_tamanho(arquivos[relativo])}</span></li>"
        )

    secoes = []
    for pasta in sorted(grupos, key=lambda p: (p != "", p.lower())):
        itens = "".join(_linha(r) for r in grupos[pasta])
        if pasta == "":
            secoes.append(f'<ul class="arquivos">{itens}</ul>')
        else:
            total = sum(arquivos[r] for r in grupos[pasta])
            secoes.append(
                f"<details><summary><strong>{html.escape(pasta)}/</strong>"
                f"<span>{len(grupos[pasta])} arquivo(s) · {_tamanho(total)}</span></summary>"
                f'<ul class="arquivos">{itens}</ul></details>'
            )

    atualizado = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%d/%m/%Y %H:%M")
    return f"""<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(titulo)}</title>
<style>
  :root {{
    --fundo:#fafaf9; --texto:#1c1917; --suave:#78716c; --borda:#e7e5e4;
    --link:#1d4ed8; --botao:#1d4ed8; --botao-texto:#fff;
  }}
  @media (prefers-color-scheme: dark) {{
    :root {{
      --fundo:#1c1917; --texto:#f5f5f4; --suave:#a8a29e; --borda:#44403c;
      --link:#93c5fd; --botao:#3b82f6; --botao-texto:#fff;
    }}
  }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; background:var(--fundo); color:var(--texto); font:15px/1.5 system-ui,-apple-system,sans-serif; }}
  main {{ max-width:860px; margin:0 auto; padding:40px 16px 64px; }}
  h1 {{ margin:0 0 4px; font-size:1.6rem; }}
  .meta {{ color:var(--suave); margin:0 0 24px; }}
  .baixar {{
    display:inline-block; padding:10px 18px; border-radius:8px; margin-bottom:32px;
    background:var(--botao); color:var(--botao-texto); text-decoration:none; font-weight:600;
  }}
  .baixar span {{ font-weight:400; opacity:.85; }}
  ul.arquivos {{ list-style:none; margin:0; padding:0; }}
  ul.arquivos li {{
    display:flex; justify-content:space-between; gap:16px; padding:6px 0; border-bottom:1px solid var(--borda);
  }}
  ul.arquivos a {{ color:var(--link); text-decoration:none; overflow-wrap:anywhere; }}
  ul.arquivos a:hover {{ text-decoration:underline; }}
  li span, summary span {{ color:var(--suave); white-space:nowrap; font-variant-numeric:tabular-nums; }}
  details {{ margin-top:16px; }}
  summary {{
    cursor:pointer; display:flex; justify-content:space-between; gap:16px;
    padding:8px 0; border-bottom:1px solid var(--borda);
  }}
  details ul.arquivos {{ padding-left:16px; }}
</style>
</head>
<body>
<main>
<h1>{html.escape(titulo)}</h1>
<p class="meta">{len(arquivos)} arquivos · {_tamanho(sum(arquivos.values()))} · atualizado em {atualizado}</p>
<a class="baixar" href="{quote(nome_zip)}">Baixar tudo (.zip) <span>· {_tamanho(tamanho_zip)}</span></a>
{"".join(secoes)}
</main>
</body>
</html>
"""


@click.command()
@click.argument(
    "diretorio",
    default=DIRETORIO_PADRAO,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
)
@click.argument("bucket_name", default=BUCKET_PADRAO)
@click.option("--segredo", default=SEGREDO_PADRAO, show_default=True)
@click.option("--arquivo-segredos", default=ARQUIVO_SEGREDOS_PADRAO, show_default=True)
@click.option("--prefixo", default="", help="Prefixo das chaves no bucket (ex: 'backup/')")
@click.option("--ignorar", multiple=True, help="Padrão extra a ignorar (pode repetir)")
@click.option("--forcar", is_flag=True, help="Reenvia arquivos mesmo que já existam com o mesmo tamanho")
@click.option("--workers", default=8, show_default=True, help="Arquivos enviados em paralelo")
@click.option("--dry-run", is_flag=True, help="Só lista o que seria enviado")
@click.option("--sem-indice", is_flag=True, help="Não gera o index.html nem o zip com todos os arquivos")
def enviar_diretorio(
    diretorio: Path,
    bucket_name: str,
    segredo: str,
    arquivo_segredos: str,
    prefixo: str,
    ignorar: tuple[str, ...],
    forcar: bool,
    workers: int,
    dry_run: bool,
    sem_indice: bool,
):
    """
    Envia recursivamente DIRETORIO para BUCKET_NAME, preservando a estrutura de pastas.
    Arquivos que já existem no bucket com o mesmo tamanho são pulados.
    Ao final, publica um index.html com links para os arquivos e um zip com tudo
    (o zip só é refeito quando algo mudou).
    """
    padroes = IGNORAR_PADRAO + ignorar
    cliente = criar_cliente(carregar_segredo(segredo, arquivo_segredos))

    relativos = _listar_locais(diretorio, padroes)
    locais = {prefixo + r: c for r, c in relativos.items()}

    remotos = {} if forcar else _listar_remotos(cliente, bucket_name, prefixo)
    pendentes = {chave: caminho for chave, caminho in locais.items() if remotos.get(chave) != caminho.stat().st_size}
    total_bytes = sum(c.stat().st_size for c in pendentes.values())
    logger.info(
        f"{len(locais)} arquivo(s) encontrados, {len(locais) - len(pendentes)} já no bucket, "
        f"{len(pendentes)} a enviar ({total_bytes / 1024**3:.2f} GB)"
    )

    if dry_run:
        for chave in sorted(pendentes):
            print(chave)
        return

    def _enviar(chave: str, caminho: Path) -> str:
        cliente.upload_file(
            str(caminho),
            bucket_name,
            chave,
            ExtraArgs={"ContentType": _tipo_conteudo(chave)},
            Config=CONFIG_TRANSFERENCIA,
        )
        return chave

    falhas = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futuros = {executor.submit(_enviar, k, c): k for k, c in pendentes.items()}
        for i, futuro in enumerate(as_completed(futuros), 1):
            chave = futuros[futuro]
            try:
                futuro.result()
                logger.info(f"[{i}/{len(pendentes)}] Upload concluído: {chave}")
            except Exception as e:
                logger.error(f"[{i}/{len(pendentes)}] Falha ao enviar {chave}: {e}")
                falhas.append(chave)

    if falhas:
        raise click.ClickException(f"{len(falhas)} arquivo(s) falharam. Rode de novo para reenviar só os pendentes.")

    if sem_indice:
        return

    nome_zip = f"{diretorio.name}.zip"
    chave_zip = prefixo + nome_zip
    tamanho_zip = remotos.get(chave_zip) if not forcar else None
    if pendentes or tamanho_zip is None:
        with tempfile.TemporaryDirectory() as tmp:
            caminho_zip = Path(tmp) / nome_zip
            logger.info(f"Gerando {nome_zip}...")
            _gerar_zip(relativos, caminho_zip, diretorio.name)
            tamanho_zip = caminho_zip.stat().st_size
            _enviar(chave_zip, caminho_zip)
        logger.info(f"Upload concluído: {chave_zip} ({_tamanho(tamanho_zip)})")

    tamanhos = {r: c.stat().st_size for r, c in relativos.items()}
    indice = _gerar_indice(diretorio.name, tamanhos, nome_zip, tamanho_zip)
    cliente.put_object(
        Bucket=bucket_name,
        Key=prefixo + "index.html",
        Body=indice.encode("utf-8"),
        ContentType="text/html; charset=utf-8",
        CacheControl="no-cache",
    )
    logger.info(f"Upload concluído: {prefixo}index.html")


if __name__ == "__main__":
    enviar_diretorio()
