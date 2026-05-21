import os
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3
import click
from botocore.exceptions import ClientError
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich import box

from shared.carregar_segredo import carregar_segredo

console = Console()
SEGREDO_PADRAO = "colibri-token-desenvolvedor"
FUSO = ZoneInfo("America/Sao_Paulo")


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


@click.group()
def cli():
    """Colibri: ferramentas de gestao do bucket R2"""
    pass


@cli.group()
def bucket():
    """Operações no bucket R2"""
    pass


@bucket.command("ls")
@click.argument("bucket_name")
@click.option("--segredo", default=SEGREDO_PADRAO, show_default=True)
@click.option("--prefixo", default="", help="Filtrar por prefixo")
def listar(bucket_name: str, segredo: str, prefixo: str):
    """Lista arquivos no bucket."""
    s3 = _cliente(segredo)

    with Progress(SpinnerColumn(), TextColumn("[cyan]Buscando arquivos..."), transient=True) as p:
        p.add_task("")
        paginator = s3.get_paginator("list_objects_v2")
        objetos = [
            obj
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefixo)
            for obj in page.get("Contents", [])
        ]

    if not objetos:
        console.print(Panel(f"[yellow]Bucket [bold]{bucket_name}[/bold] vazio.[/yellow]"))
        return

    tabela = Table(box=box.ROUNDED, show_header=True, header_style="bold cyan")
    tabela.add_column("Arquivo", style="white", no_wrap=False, ratio=3)
    tabela.add_column("Tamanho", justify="right", ratio=1)
    tabela.add_column("Modificado", style="dim", ratio=1)

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
        f"  [dim]{len(objetos)} arquivo(s)[/dim]  •  "
        f"[bold]{_tamanho(total_bytes)}[/bold] total  •  "
        f"[dim]bucket:[/dim] [cyan]{bucket_name}[/cyan]"
    )


@bucket.command("rm")
@click.argument("arquivo")
@click.argument("bucket_name")
@click.option("--segredo", default=SEGREDO_PADRAO, show_default=True)
def deletar(arquivo: str, bucket_name: str, segredo: str):
    """Remove um arquivo do bucket."""
    s3 = _cliente(segredo)
    try:
        s3.head_object(Bucket=bucket_name, Key=arquivo)
    except ClientError:
        console.print(f"[red]✗[/red] Arquivo não encontrado: [bold]{arquivo}[/bold]")
        return

    s3.delete_object(Bucket=bucket_name, Key=arquivo)
    console.print(f"[green]✓[/green] Deletado: [bold]{arquivo}[/bold]")


@bucket.command("rm-all")
@click.argument("bucket_name")
@click.option("--segredo", default=SEGREDO_PADRAO, show_default=True)
@click.option("--prefixo", default="", help="Limitar a um prefixo")
@click.confirmation_option(prompt="⚠  Isso vai deletar todos os objetos. Confirma?")
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

    with Progress(SpinnerColumn(), TextColumn(f"[red]Deletando {len(objetos)} arquivo(s)..."), transient=True) as p:
        p.add_task("")
        for i in range(0, len(objetos), 1000):
            s3.delete_objects(Bucket=bucket_name, Delete={"Objects": objetos[i:i + 1000]})

    console.print(f"[green]✓[/green] [bold]{len(objetos)}[/bold] arquivo(s) deletado(s) de [cyan]{bucket_name}[/cyan]")


@bucket.command("download")
@click.argument("arquivo")
@click.argument("bucket_name")
@click.option("--segredo", default=SEGREDO_PADRAO, show_default=True)
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
        console.print(f"[green]✓[/green] Salvo em: [bold]{destino}[/bold] [dim]({_tamanho(tamanho)})[/dim]")
    except ClientError:
        console.print(f"[red]✗[/red] Arquivo não encontrado: [bold]{arquivo}[/bold]")


@bucket.command("upload")
@click.argument("caminho_arquivo")
@click.argument("bucket_name")
@click.option("--segredo", default=SEGREDO_PADRAO, show_default=True)
@click.option("--chave", default=None, help="Nome no bucket (padrão: nome do arquivo com timestamp)")
def upload(caminho_arquivo: str, bucket_name: str, segredo: str, chave: str | None):
    """Faz upload de um arquivo para o bucket."""
    if not os.path.exists(caminho_arquivo):
        console.print(f"[red]✗[/red] Arquivo não encontrado: [bold]{caminho_arquivo}[/bold]")
        return

    if chave is None:
        nome, ext = os.path.splitext(os.path.basename(caminho_arquivo))
        ts = datetime.now(FUSO).strftime("%Y-%m-%d-%H%M%S")
        chave = f"{nome}_{ts}{ext}"

    tamanho = os.path.getsize(caminho_arquivo)
    s3 = _cliente(segredo)

    with Progress(SpinnerColumn(), TextColumn(f"[cyan]Enviando {_tamanho(tamanho)}..."), transient=True) as p:
        p.add_task("")
        with open(caminho_arquivo, "rb") as f:
            s3.upload_fileobj(f, bucket_name, chave)

    console.print(f"[green]✓[/green] Enviado: [bold]{chave}[/bold] [dim]({_tamanho(tamanho)})[/dim]")


@cli.group()
def lake():
    """Consulta o catalogo DuckLake"""
    pass


def _conectar_lake():
    import shared.ducklake as dl
    from ingestion.pncp.pipeline import NOME_SEGREDO, CAMINHO_META, DATA_PATH
    return dl.conectar(CAMINHO_META, DATA_PATH, NOME_SEGREDO)


@lake.command("tabelas")
def tabelas():
    """Lista as tabelas registradas no catalogo."""
    con = _conectar_lake()
    rows = con.execute("""
        SELECT t.table_name,
               s.record_count AS linhas,
               count(f.data_file_id) AS parquets
        FROM __ducklake_metadata_lake.main.ducklake_table t
        LEFT JOIN __ducklake_metadata_lake.main.ducklake_table_stats s USING (table_id)
        LEFT JOIN __ducklake_metadata_lake.main.ducklake_data_file f USING (table_id)
        WHERE t.end_snapshot IS NULL
        GROUP BY t.table_name, s.record_count
        ORDER BY t.table_name
    """).fetchall()
    con.close()

    tabela = Table(box=box.ROUNDED, header_style="bold cyan")
    tabela.add_column("Tabela")
    tabela.add_column("Linhas", justify="right")
    tabela.add_column("Parquets", justify="right")
    for row in rows:
        tabela.add_row(row[0], f"{row[1]:,}" if row[1] else "—", str(row[2]))
    console.print(tabela)


@lake.command("anos")
@click.argument("tabela")
def anos(tabela: str):
    """Mostra contagem de linhas por ano de uma tabela"""
    con = _conectar_lake()
    try:
        rows = con.execute(f"""
            SELECT ano, count(*) AS n
            FROM lake.main.{tabela}
            GROUP BY ano ORDER BY ano
        """).fetchall()
    except Exception as e:
        console.print(f"[red]✗[/red] {e}")
        con.close()
        return
    con.close()

    tb = Table(box=box.ROUNDED, header_style="bold cyan")
    tb.add_column("Ano")
    tb.add_column("Linhas", justify="right")
    total = 0
    for row in rows:
        tb.add_row(str(row[0]), f"{row[1]:,}")
        total += row[1]
    tb.add_section()
    tb.add_row("[bold]Total[/bold]", f"[bold]{total:,}[/bold]")
    console.print(tb)


@lake.command("q")
@click.argument("sql")
def query(sql: str):
    """Executa uma query SQL no lake"""
    con = _conectar_lake()
    try:
        resultado = con.execute(sql).fetchdf()
    except Exception as e:
        console.print(f"[red]✗[/red] {e}")
        con.close()
        return
    con.close()

    if resultado.empty:
        console.print("[yellow]Sem resultados.[/yellow]")
        return

    tb = Table(box=box.ROUNDED, header_style="bold cyan")
    for col in resultado.columns:
        tb.add_column(str(col))
    for _, row in resultado.iterrows():
        tb.add_row(*[str(v) for v in row])
    console.print(tb)


@cli.group()
def pipeline():
    """Executa os pipelines de ingestao"""
    pass


@pipeline.command("run")
@click.option("--apenas", type=click.Choice(["ncm", "pncp"]), default=None, help="Rodar só um pipeline")
def run(apenas: str | None):
    """Roda o pipeline completo (NCM + PNCP) ou apenas um modulo."""
    import ingestion.ncm.pipeline as ncm
    import ingestion.pncp.pipeline as pncp

    if apenas == "ncm":
        console.print("[cyan]>>> NCM[/cyan]")
        ncm.main()
    elif apenas == "pncp":
        console.print("[cyan]>>> PNCP[/cyan]")
        pncp.main()
    else:
        console.print("[cyan]>>> NCM[/cyan]")
        ncm.main()
        console.print("[cyan]>>> PNCP[/cyan]")
        pncp.main()

    console.print("[green]✓[/green] Pipeline concluido.")


if __name__ == "__main__":
    cli()
