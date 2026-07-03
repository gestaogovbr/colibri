import os
import shutil
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3
import click
from botocore.exceptions import ClientError
from rich.console import Console
from rich.padding import Padding
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.text import Text
from rich import box
from ingestion.pncp_comprasgov import pipeline as pncp_comprasgov_pipeline
from utils.constantes import NOME_SEGREDO, NOME_SEGREDO_VISUALIZADOR, CATALOGO_LOCAL
from utils.carregar_segredo import carregar_segredo

console = Console()
NOME_SEGREDO = "colibri-token-desenvolvedor"
FUSO = ZoneInfo("America/Sao_Paulo")

NOME = "colibri"
SUBTITULO = "Lakehouse e orquestrador de pipelines"

# Paleta do colibri: verde + azul
VERDE = "#2ee6a6"
AZUL = "#1e9bff"
VERDE_RGB = (46, 230, 166)
AZUL_RGB = (30, 155, 255)

BANNER = r"""

 $$$$$$\            $$\ $$\ $$\                 $$\ 
$$  __$$\           $$ |\__|$$ |                \__|
$$ /  \__| $$$$$$\  $$ |$$\ $$$$$$$\   $$$$$$\  $$\ 
$$ |      $$  __$$\ $$ |$$ |$$  __$$\ $$  __$$\ $$ |
$$ |      $$ /  $$ |$$ |$$ |$$ |  $$ |$$ |  \__|$$ |
$$ |  $$\ $$ |  $$ |$$ |$$ |$$ |  $$ |$$ |      $$ |
\$$$$$$  |\$$$$$$  |$$ |$$ |$$$$$$$  |$$ |      $$ |
 \______/  \______/ \__|\__|\_______/ \__|      \__|
"""


def _mistura(ini, fim, p):
    """Interpola duas cores RGB (p de 0.0 a 1.0) e devolve em hex"""
    r, g, b = (round(ini[i] + (fim[i] - ini[i]) * p) for i in range(3))
    return f"#{r:02x}{g:02x}{b:02x}"


def _gradiente_v(linhas, ini, fim):
    """Gradiente vertical: cada linha recebe uma cor entre `ini` e `fim`"""
    texto = Text(no_wrap=True)
    n = max(len(linhas) - 1, 1)
    for i, linha in enumerate(linhas):
        texto.append(linha + "\n", style=f"bold {_mistura(ini, fim, i / n)}")
    return texto


def _gradiente_h(s, ini, fim):
    """Gradiente horizontal: cada caractere recebe uma cor entre `ini` e `fim`"""
    texto = Text(no_wrap=True)
    n = max(len(s) - 1, 1)
    for i, ch in enumerate(s):
        texto.append(ch, style=f"bold {_mistura(ini, fim, i / n)}")
    return texto


class ColibriGroup(click.Group):
    """Grupo Click que mantém o mesmo cabeçalho (banner + comandos) em todos os menus"""

    def list_commands(self, ctx):
        # Mantém a ordem de definição em vez da ordem alfabética padrão do Click
        return list(self.commands)

    def _banner(self):
        # Linhas da arte sem espaços à direita (para o centro ficar correto).
        linhas = [l.rstrip() for l in BANNER.splitlines() if l.strip()]
        largura = max((len(l) for l in linhas), default=0)

        # Se o banner não couber na janela atual, usa um título compacto que não quebra
        if console.size.width < largura:
            return _gradiente_h(NOME, VERDE_RGB, AZUL_RGB), len(NOME)

        return _gradiente_v(linhas, VERDE_RGB, AZUL_RGB), largura

    def format_help(self, ctx, formatter):
        # Na raiz usa o subtítulo global; nos subgrupos, a própria docstring do grupo
        subtitulo = SUBTITULO if ctx.parent is None else (self.help or "").strip()

        # Tabela construída primeiro para medir a largura e calcular o recuo do banner
        tabela = Table(
            box=box.ROUNDED,
            show_header=False,
            border_style=AZUL,
            padding=(0, 2),
            title="Comandos",
            title_style=f"bold {VERDE}",
        )
        tabela.add_column(style=f"bold {VERDE}", no_wrap=True)
        tabela.add_column(style="white")
        for nome in self.list_commands(ctx):
            tabela.add_row(nome, self.get_command(ctx, nome).get_short_help_str())

        largura_tabela = console.measure(tabela).maximum
        banner, largura = self._banner()
        regua = "-" * min(largura, console.size.width)
        left_pad = max(0, (largura_tabela - largura) // 2)

        console.print()
        console.print(Padding(banner, (0, 0, 0, left_pad)))
        console.print(Padding(_gradiente_h(regua, VERDE_RGB, AZUL_RGB), (0, 0, 0, left_pad)))
        if subtitulo:
            sub_pad = max(0, (largura_tabela - len(subtitulo)) // 2)
            console.print(Padding(Text(subtitulo, style="italic dim"), (0, 0, 0, sub_pad)))
        console.print()
        console.print(tabela)
        console.print()

        # Dica de uso
        dica = Text(no_wrap=True)
        dica.append("Use ", style="dim")
        dica.append(f"{ctx.command_path} <comando> --help", style=AZUL)
        dica.append(" para ver as opções.", style="dim")
        dica_str = f"Use {ctx.command_path} <comando> --help para ver as opções."
        dica_pad = max(0, (largura_tabela - len(dica_str)) // 2)
        console.print(Padding(dica, (0, 0, 0, dica_pad)))


def _cliente(nome_segredo: str):
    c = carregar_segredo(nome_segredo)
    return boto3.client(
        "s3",
        endpoint_url=c["endpoint"],
        aws_access_key_id=c["access_key"],
        aws_secret_access_key=c["secret_key"],
        region_name="auto",
    )


def _tamanho(bytes: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if bytes < 1024:
            return f"{bytes:.1f} {unit}"
        bytes /= 1024
    return f"{bytes:.1f} TB"


def _cor_tamanho(bytes: int) -> str:
    if bytes < 1_000_000:
        return "green"
    if bytes < 100_000_000:
        return "yellow"
    return "red"


def _tabela_dados(*colunas):
    """Tabela de dados com o tema verde/azul. Cada item: nome ou (nome, kwargs)"""
    tabela = Table(box=box.ROUNDED, header_style=f"bold {VERDE}", border_style=AZUL)
    for coluna in colunas:
        if isinstance(coluna, tuple):
            tabela.add_column(coluna[0], **coluna[1])
        else:
            tabela.add_column(coluna)
    return tabela


@click.group(cls=ColibriGroup)
def cli():
    """Lakehouse e orquestrador de pipelines"""
    pass


@cli.group(cls=ColibriGroup)
def lake():
    """Explora o catálogo e executa queries SQL"""
    pass


@cli.group(cls=ColibriGroup)
def pipeline():
    """Orquestra os pipelines de ingestão"""
    pass


@cli.group(cls=ColibriGroup)
def bucket():
    """Gerencia arquivos no object storage (R2)"""
    pass


@bucket.command("list")
@click.argument("bucket_name")
@click.option("--segredo", default=NOME_SEGREDO, show_default=True)
@click.option("--prefixo", default="", help="Filtrar por prefixo")
def listar(bucket_name: str, segredo: str, prefixo: str):
    """Lista arquivos no bucket"""
    s3 = _cliente(segredo)

    with Progress(SpinnerColumn(style=VERDE), TextColumn(f"[{AZUL}]Buscando arquivos..."), transient=True) as p:
        p.add_task("")
        paginator = s3.get_paginator("list_objects_v2")
        objetos = [
            obj
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefixo)
            for obj in page.get("Contents", [])
        ]

    if not objetos:
        console.print(Panel(f"[yellow]Bucket [bold]{bucket_name}[/bold] vazio.[/yellow]", border_style="yellow"))
        return

    tabela = _tabela_dados(
        ("Arquivo", {"style": "white", "no_wrap": False, "ratio": 3}),
        ("Tamanho", {"justify": "right", "ratio": 1}),
        ("Modificado", {"style": "dim", "ratio": 1}),
    )

    total_bytes = 0
    for obj in objetos:
        size = obj["Size"]
        total_bytes += size
        modificado = obj["LastModified"].astimezone(FUSO).strftime("%Y-%m-%d %H:%M")
        tabela.add_row(
            obj["Key"],
            f"[{_cor_tamanho(size)}]{_tamanho(size)}[/]",
            modificado,
        )

    console.print(tabela)
    console.print(
        f"  [dim]{len(objetos)} arquivo(s)[/dim]  [{AZUL}]•[/]  "
        f"[bold]{_tamanho(total_bytes)}[/bold] total  [{AZUL}]•[/]  "
        f"[dim]bucket:[/dim] [{AZUL}]{bucket_name}[/]"
    )


@bucket.command("delete")
@click.argument("arquivo")
@click.argument("bucket_name")
@click.option("--segredo", default=NOME_SEGREDO, show_default=True)
def deletar(arquivo: str, bucket_name: str, segredo: str):
    """Remove um arquivo do bucket"""
    s3 = _cliente(segredo)
    try:
        s3.head_object(Bucket=bucket_name, Key=arquivo)
    except ClientError:
        console.print(f"[red]x[/red] Arquivo não encontrado: [bold]{arquivo}[/bold]")
        return

    s3.delete_object(Bucket=bucket_name, Key=arquivo)
    console.print(f"[{VERDE}]+[/] Deletado: [bold]{arquivo}[/bold]")


@bucket.command("purge")
@click.argument("bucket_name")
@click.option("--segredo", default=NOME_SEGREDO, show_default=True)
@click.option("--prefixo", default="", help="Limitar a um prefixo")
@click.confirmation_option(prompt="!  Isso vai deletar todos os objetos. Confirma?")
def deletar_tudo(bucket_name: str, segredo: str, prefixo: str):
    """Remove todos os arquivos do bucket (ou de um prefixo)"""
    s3 = _cliente(segredo)
    paginator = s3.get_paginator("list_objects_v2")
    objetos = [
        {"Key": obj["Key"]}
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefixo)
        for obj in page.get("Contents", [])
    ]

    if not objetos:
        console.print("[yellow]Nenhum arquivo encontrado.[/yellow]")
        return

    with Progress(SpinnerColumn(style="red"), TextColumn(f"[red]Deletando {len(objetos)} arquivo(s)..."), transient=True) as p:
        p.add_task("")
        for i in range(0, len(objetos), 1000):
            s3.delete_objects(Bucket=bucket_name, Delete={"Objects": objetos[i:i + 1000]})

    console.print(f"[{VERDE}]+[/] [bold]{len(objetos)}[/bold] arquivo(s) deletado(s) de [{AZUL}]{bucket_name}[/]")


@bucket.command("download")
@click.argument("arquivo")
@click.argument("bucket_name")
@click.option("--segredo", default=NOME_SEGREDO, show_default=True)
@click.option("--destino", default=None, help="Caminho local de destino (padrao: ./<arquivo>)")
def download(arquivo: str, bucket_name: str, segredo: str, destino: str | None):
    """Baixa um arquivo do bucket"""
    s3 = _cliente(segredo)
    nome_arquivo = os.path.basename(arquivo)
    if destino is None:
        destino = os.path.join(bucket_name, arquivo)
    elif os.path.isdir(destino):
        destino = os.path.join(destino, nome_arquivo)
    os.makedirs(os.path.dirname(destino) or ".", exist_ok=True)
    try:
        s3.download_file(bucket_name, arquivo, destino)
        tamanho = os.path.getsize(destino)
        console.print(f"[{VERDE}]+[/] Salvo em: [bold]{destino}[/bold] [dim]({_tamanho(tamanho)})[/dim]")
    except ClientError:
        console.print(f"[red]x[/red] Arquivo não encontrado: [bold]{arquivo}[/bold]")


@bucket.command("upload")
@click.argument("caminho_arquivo")
@click.argument("bucket_name")
@click.option("--segredo", default=NOME_SEGREDO, show_default=True)
@click.option("--chave", default=None, help="Nome no bucket (padrão: nome do arquivo com timestamp)")
def upload(caminho_arquivo: str, bucket_name: str, segredo: str, chave: str | None):
    """Faz upload de um arquivo para o bucket"""
    if not os.path.exists(caminho_arquivo):
        console.print(f"[red]x[/red] Arquivo não encontrado: [bold]{caminho_arquivo}[/bold]")
        return

    if chave is None:
        nome, ext = os.path.splitext(os.path.basename(caminho_arquivo))
        ts = datetime.now(FUSO).strftime("%Y-%m-%d-%H%M%S")
        chave = f"{nome}_{ts}{ext}"

    tamanho = os.path.getsize(caminho_arquivo)
    s3 = _cliente(segredo)

    with Progress(SpinnerColumn(style=VERDE), TextColumn(f"[{AZUL}]Enviando {_tamanho(tamanho)}..."), transient=True) as p:
        p.add_task("")
        with open(caminho_arquivo, "rb") as f:
            s3.upload_fileobj(f, bucket_name, chave)

    console.print(f"[{VERDE}]+[/] Enviado: [bold]{chave}[/bold] [dim]({_tamanho(tamanho)})[/dim]")


def _conectar_lake(bucket: str, segredo: str):
    import utils.ducklake as dl
    caminho_meta = f"s3://{bucket}/meta.ducklake"
    data_path = f"s3://{bucket}/lake/"
    return dl.conectar(caminho_meta, data_path, segredo)


@lake.command("tables")
@click.option("--bucket", default='colibri-prod', show_default=True, help="Bucket do lake")
@click.option("--segredo", default=NOME_SEGREDO_VISUALIZADOR, show_default=True, help="Segredo com acesso ao bucket")
def tabelas(bucket: str, segredo: str):
    """Lista as tabelas registradas no catalogo"""
    con = _conectar_lake(bucket, segredo)
    rows = con.execute("""
        SELECT t.table_name,
               'tabela' AS tipo,
               s.record_count AS linhas,
               count(f.data_file_id) AS parquets
        FROM __ducklake_metadata_lake.main.ducklake_table t
        LEFT JOIN __ducklake_metadata_lake.main.ducklake_table_stats s USING (table_id)
        LEFT JOIN __ducklake_metadata_lake.main.ducklake_data_file f USING (table_id)
        WHERE t.end_snapshot IS NULL
        GROUP BY t.table_name, s.record_count

        UNION ALL

        SELECT v.view_name,
               'view' AS tipo,
               NULL AS linhas,
               NULL AS parquets
        FROM __ducklake_metadata_lake.main.ducklake_view v
        WHERE v.end_snapshot IS NULL

        ORDER BY tipo, table_name
    """).fetchall()
    con.close()

    tabela = _tabela_dados(
        "Tabela",
        "Tipo",
        ("Linhas", {"justify": "right"}),
        ("Parquets", {"justify": "right"}),
    )
    for nome, tipo, linhas, parquets in rows:
        tabela.add_row(
            nome,
            tipo,
            f"{linhas:,}" if linhas else "—",
            f"{parquets:,}" if parquets else "—",
        )
    console.print(tabela)


@lake.command("years")
@click.argument("tabela")
@click.option("--bucket", default='colibri-prod', show_default=True, help="Bucket do lake")
@click.option("--segredo", default=NOME_SEGREDO_VISUALIZADOR, show_default=True, help="Segredo com acesso ao bucket")
def anos(tabela: str, bucket: str, segredo: str):
    """Mostra contagem de linhas por ano de uma tabela"""
    con = _conectar_lake(bucket, segredo)
    try:
        rows = con.execute(f"""
            SELECT ano, count(*) AS n
            FROM lake.main.{tabela}
            GROUP BY ano ORDER BY ano
        """).fetchall()
    except Exception as e:
        console.print(f"[red]x[/red] {e}")
        con.close()
        return
    con.close()

    tb = _tabela_dados("Ano", ("Linhas", {"justify": "right"}))
    total = 0
    for row in rows:
        tb.add_row(str(row[0]), f"{row[1]:,}")
        total += row[1]
    tb.add_section()
    tb.add_row("[bold]Total[/bold]", f"[bold {AZUL}]{total:,}[/]")
    console.print(tb)


@lake.command("download")
@click.argument("tabela")
@click.option("--destino", default=None, help="Arquivo de saída (padrão: ./<tabela>.parquet)")
@click.option("--bucket", default='colibri-prod', show_default=True, help="Bucket do lake")
@click.option("--segredo", default=NOME_SEGREDO_VISUALIZADOR, show_default=True, help="Segredo com acesso ao bucket")
def lake_download(tabela: str, destino: str | None, bucket: str, segredo: str):
    """Baixa os parquets de uma tabela do catálogo para o disco"""
    con = _conectar_lake(bucket, segredo)
    try:
        row = con.execute("""
            SELECT s.schema_name
            FROM __ducklake_metadata_lake.main.ducklake_schema s
            JOIN __ducklake_metadata_lake.main.ducklake_table t USING (schema_id)
            WHERE t.table_name = ? AND t.end_snapshot IS NULL

            UNION ALL

            SELECT s.schema_name
            FROM __ducklake_metadata_lake.main.ducklake_schema s
            JOIN __ducklake_metadata_lake.main.ducklake_view v USING (schema_id)
            WHERE v.view_name = ? AND v.end_snapshot IS NULL

            LIMIT 1
        """, [tabela, tabela]).fetchone()
    except Exception as e:
        console.print(f"[red]x[/red] Erro ao consultar catálogo: {e}")
        con.close()
        return

    if not row:
        console.print(f"[red]x[/red] Tabela não encontrada: [bold]{tabela}[/bold]")
        con.close()
        return

    schema_name = row[0]
    saida = destino or f"{tabela}.parquet"

    with Progress(SpinnerColumn(style=VERDE), TextColumn(f"[{AZUL}]Exportando..."), transient=True) as p:
        p.add_task("")
        try:
            con.execute(f"COPY (SELECT * FROM lake.{schema_name}.{tabela}) TO '{saida}' (FORMAT PARQUET)")
        except Exception as e:
            console.print(f"[red]x[/red] Erro: {e}")
            con.close()
            return

    con.close()
    console.print(f"[{VERDE}]+[/] Salvo em [bold]{saida}[/bold]")


@lake.command("query")
@click.argument("sql")
@click.option("--bucket", default='colibri-prod', show_default=True, help="Bucket do lake")
@click.option("--segredo", default=NOME_SEGREDO_VISUALIZADOR, show_default=True, help="Segredo com acesso ao bucket")
def query(sql: str, bucket: str, segredo: str):
    """Executa uma query SQL no lake"""
    con = _conectar_lake(bucket, segredo)
    try:
        resultado = con.execute(sql).fetchdf()
    except Exception as e:
        console.print(f"[red]x[/red] {e}")
        con.close()
        return
    con.close()

    if resultado.empty:
        console.print("[yellow]Sem resultados.[/yellow]")
        return

    tb = _tabela_dados(*[str(col) for col in resultado.columns])
    for _, row in resultado.iterrows():
        tb.add_row(*[str(v) for v in row])
    console.print(tb)



@lake.command("ui")
@click.option("--bucket", default='colibri-prod', show_default=True, help="Bucket do lake")
@click.option("--segredo", default=NOME_SEGREDO_VISUALIZADOR, show_default=True, help="Segredo com acesso ao bucket")
def ui(bucket: str, segredo: str):
    """Abre DuckDB UI conectado ao catálogo"""
    con = _conectar_lake(bucket, segredo)
    con.execute("CALL start_ui();")
    input("Pressione Enter para fechar o servidor e encerrar o script...")


@pipeline.command("run")
@click.option("--apenas", type=click.Choice(["ncm", "pncp-comprasgov", "catmats", "nfe-cgu", "margem-preferencia"]), default=None, help="Rodar só um pipeline")
@click.option("--bucket", default=None, help="Bucket de destino (padrão: definido nas constantes)")
def run(apenas: str | None, bucket: str | None):
    """Roda o pipeline completo ou apenas um modulo"""
    import ingestion.ncm.pipeline as ncm
    import ingestion.pncp_comprasgov.pipeline as pncp_comprasgov
    import ingestion.catmats.pipeline as catmats
    import ingestion.nfe_cgu.pipeline as nfe_cgu
    import ingestion.margem_preferencia.pipeline as margem_preferencia

    pipelines = {
        "ncm": (ncm, "[cyan]>>> NCM[/cyan]"),
        "pncp-comprasgov": (pncp_comprasgov,  "[cyan]>>> PNCP ComprasGOV[/cyan]"),
        "catmats": (catmats, "[cyan]>>> CATMATS[/cyan]"),
        "nfe-cgu": (nfe_cgu, "[cyan]>>> NFe-CGU[/cyan]"),
        "margem-preferencia": (margem_preferencia, "[cyan]>>> Margem de Preferência[/cyan]"),
    }

    ordem = list(pipelines.keys()) if apenas is None else [apenas]

    for nome in ordem:
        modulo, label = pipelines[nome]
        console.print(label)
        modulo.main(bucket=bucket)

    console.print("[green]+[/green] Pipeline concluido.")


_MANIFESTOS = [
    "ncm_manifesto.csv",
    "pncp_comprasgov_manifesto.csv",
    "catmats_manifesto.csv",
    "nfe_cgu_manifesto.csv",
    "margem_preferencia_manifesto.csv",
]


@cli.command("sincronizar")
@click.option("--segredo", default=NOME_SEGREDO, show_default=True)
def sincronizar(segredo: str):
    """Baixa os manifestos e o catálogo DuckLake do bucket para a máquina local"""
    import os
    from pathlib import Path

    config = carregar_segredo(segredo)
    bucket = config["bucket_lake"]
    s3 = _cliente(segredo)
    raiz = Path(CATALOGO_LOCAL).parent

    itens = [
        (nome, str(raiz / "dados" / "manifestos" / nome))
        for nome in _MANIFESTOS
    ] + [
        ("meta.ducklake",  CATALOGO_LOCAL),
    ]

    for chave, destino in itens:
        os.makedirs(os.path.dirname(destino), exist_ok=True)
        try:
            with Progress(SpinnerColumn(), TextColumn(f"[cyan]Baixando {chave}..."), transient=True) as p:
                p.add_task("")
                s3.download_file(bucket, chave, destino)
            tamanho = os.path.getsize(destino)
            console.print(f"[green]+[/green] {chave} -> [bold]{destino}[/bold] [dim]({_tamanho(tamanho)})[/dim]")
        except Exception as e:
            console.print(f"[red]x[/red] {chave}: {e}")


@cli.command("docs")
@click.option("--sem-servidor", is_flag=True, help="Gera a documentação sem abrir o servidor")
def docs(sem_servidor: bool):
    """Gera e exibe a documentação automática do dbt"""
    import subprocess
    from pathlib import Path

    dbt_dir = Path(__file__).resolve().parent / "dbt"

    console.print("[cyan]>>> Gerando documentação...[/cyan]")
    subprocess.run(
        ["dbt", "docs", "generate", "--project-dir", str(dbt_dir), "--profiles-dir", str(dbt_dir)],
        cwd=str(dbt_dir),
        check=True,
    )

    if not sem_servidor:
        console.print("[cyan]>>> Servindo documentação em http://localhost:8080[/cyan]")
        subprocess.run(
            ["dbt", "docs", "serve", "--project-dir", str(dbt_dir), "--profiles-dir", str(dbt_dir)],
            cwd=str(dbt_dir),
            check=True,
        )


if __name__ == "__main__":
    cli()