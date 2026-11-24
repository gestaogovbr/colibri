import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3
import click
import yaml
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError
from rich import box
from rich.console import Console
from rich.padding import Padding
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from rich.text import Text

from utils.carregar_segredo import carregar_segredo
from utils.constantes import (
    BUCKET_ARQUIVOS,
    BUCKET_DESENVOLVIMENTO,
    BUCKET_PRODUCAO,
    CAMINHO_SEGREDOS,
    CATALOGO_LOCAL,
    NOME_SEGREDO_DESENVOLVEDOR,
    NOME_SEGREDO_VISUALIZADOR,
)

console = Console()
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
        linhas = [linha.rstrip() for linha in BANNER.splitlines() if linha.strip()]
        largura = max((len(linha) for linha in linhas), default=0)

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


def _cliente(nome_segredo: str, max_conexoes: int = 10):
    c = carregar_segredo(nome_segredo)
    return boto3.client(
        "s3",
        endpoint_url=c["endpoint"],
        aws_access_key_id=c["access_key"],
        aws_secret_access_key=c["secret_key"],
        region_name="auto",
        config=Config(max_pool_connections=max_conexoes),
    )


def _tamanho(bytes: int) -> str:
    """Tamanho em unidades decimais (1 KB = 1000 B), como o painel do Cloudflare"""
    for unit in ["B", "KB", "MB", "GB"]:
        if bytes < 1000:
            return f"{bytes:.1f} {unit}"
        bytes /= 1000
    return f"{bytes:.1f} TB"


def _duracao(segundos: float) -> str:
    minutos, segundos = divmod(int(segundos), 60)
    return f"{minutos}min {segundos}s" if minutos else f"{segundos}s"


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
@click.option("--segredo", default=NOME_SEGREDO_DESENVOLVEDOR, show_default=True)
@click.option("--prefixo", default="", help="Filtrar por prefixo")
def listar(bucket_name: str, segredo: str, prefixo: str):
    """Lista arquivos no bucket"""
    s3 = _cliente(segredo)

    with Progress(
        SpinnerColumn(style=VERDE),
        TextColumn(f"[{AZUL}]Buscando arquivos..."),
        transient=True,
    ) as p:
        p.add_task("")
        paginator = s3.get_paginator("list_objects_v2")
        objetos = [
            obj for page in paginator.paginate(Bucket=bucket_name, Prefix=prefixo) for obj in page.get("Contents", [])
        ]

    if not objetos:
        console.print(
            Panel(
                f"[yellow]Bucket [bold]{bucket_name}[/bold] vazio.[/yellow]",
                border_style="yellow",
            )
        )
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
@click.option("--segredo", default=NOME_SEGREDO_DESENVOLVEDOR, show_default=True)
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
@click.option("--segredo", default=NOME_SEGREDO_DESENVOLVEDOR, show_default=True)
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

    with Progress(
        SpinnerColumn(style="red"),
        TextColumn(f"[red]Deletando {len(objetos)} arquivo(s)..."),
        transient=True,
    ) as p:
        p.add_task("")
        for i in range(0, len(objetos), 1000):
            s3.delete_objects(Bucket=bucket_name, Delete={"Objects": objetos[i : i + 1000]})

    console.print(f"[{VERDE}]+[/] [bold]{len(objetos)}[/bold] arquivo(s) deletado(s) de [{AZUL}]{bucket_name}[/]")


@bucket.command("download")
@click.argument("arquivo")
@click.argument("bucket_name")
@click.option("--segredo", default=NOME_SEGREDO_DESENVOLVEDOR, show_default=True)
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
@click.option("--segredo", default=NOME_SEGREDO_DESENVOLVEDOR, show_default=True)
@click.option(
    "--chave",
    default=None,
    help="Nome no bucket (padrão: nome do arquivo com timestamp)",
)
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

    with Progress(
        SpinnerColumn(style=VERDE),
        TextColumn(f"[{AZUL}]Enviando {_tamanho(tamanho)}..."),
        transient=True,
    ) as p:
        p.add_task("")
        with open(caminho_arquivo, "rb") as f:
            s3.upload_fileobj(f, bucket_name, chave)

    console.print(f"[{VERDE}]+[/] Enviado: [bold]{chave}[/bold] [dim]({_tamanho(tamanho)})[/dim]")


# Limite do CopyObject no S3/R2; acima disso a cópia precisa ser multipart.
LIMITE_COPY_OBJECT = 5 * 1024**3
# Metadado gravado no destino quando o ETag da cópia não bate com o da origem
# (origem multipart ou cópia multipart), para reconhecer o arquivo como já copiado.
META_ETAG_ORIGEM = "colibri-etag-origem"
CABECALHOS_COPIADOS = ("ContentType", "CacheControl", "ContentDisposition", "ContentEncoding", "ContentLanguage")


def _interativo() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


def _listar_segredos() -> list[str]:
    """Nomes dos segredos (chaves de primeiro nível) do .segredos.yml"""
    try:
        with open(CAMINHO_SEGREDOS, encoding="utf-8") as f:
            return list(yaml.safe_load(f) or {})
    except OSError:
        return []


def _listar_buckets(s3) -> list[str]:
    """Buckets visíveis pela credencial; tokens restritos a buckets não podem
    listar, então nesse caso caímos nos buckets conhecidos do projeto"""
    try:
        nomes = sorted(b["Name"] for b in s3.list_buckets().get("Buckets", []))
    except ClientError:
        nomes = []
    return nomes or [BUCKET_PRODUCAO, BUCKET_DESENVOLVIMENTO, BUCKET_ARQUIVOS]


def _escolher(titulo: str, opcoes: list[str], padrao: str, livre: bool = False) -> str:
    """Mostra uma lista numerada e devolve a opção escolhida (por número ou
    nome). Com `livre`, aceita também um nome fora da lista."""
    console.print(f"\n[bold]{titulo}[/bold]")
    for i, opcao in enumerate(opcoes, 1):
        marca = " [dim](padrão)[/dim]" if opcao == padrao else ""
        console.print(f"  [{AZUL}]{i}[/] {opcao}{marca}")
    dica = "Número ou nome" if livre else "Número"
    padrao_prompt = str(opcoes.index(padrao) + 1) if padrao in opcoes else None
    while True:
        resposta = click.prompt(dica, default=padrao_prompt).strip()
        if resposta.isdigit() and 1 <= int(resposta) <= len(opcoes):
            return opcoes[int(resposta) - 1]
        if resposta in opcoes or (livre and resposta and not resposta.isdigit()):
            return resposta
        console.print("[red]x[/red] Opção inválida.")


def _etag(obj: dict) -> str:
    return obj["ETag"].strip('"')


def _ja_copiado(s3, bucket_destino: str, origem: dict, destino: dict) -> bool:
    """O objeto do destino é igual ao da origem? Compara tamanho e ETag; se o
    ETag divergir, procura o ETag da origem gravado nos metadados do destino"""
    if origem["Size"] != destino["Size"]:
        return False
    if _etag(origem) == _etag(destino):
        return True
    try:
        meta = s3.head_object(Bucket=bucket_destino, Key=destino["Key"]).get("Metadata", {})
    except Exception:
        # Na dúvida, trata como diferente: o arquivo é copiado de novo
        return False
    return meta.get(META_ETAG_ORIGEM) == _etag(origem)


def _copiar_objeto(s3, bucket_origem: str, bucket_destino: str, obj: dict, config_multipart) -> None:
    chave = obj["Key"]
    fonte = {"Bucket": bucket_origem, "Key": chave}
    etag = _etag(obj)

    # Caso comum: uma única requisição server-side, e o destino herda o ETag da origem
    if obj["Size"] <= LIMITE_COPY_OBJECT and "-" not in etag:
        s3.copy_object(Bucket=bucket_destino, Key=chave, CopySource=fonte)
        return

    # Origem multipart ou acima de 5GB: o ETag do destino vai diferir do da
    # origem. Regravamos os metadados com o ETag original para que a próxima
    # execução reconheça o arquivo como já copiado.
    cabecalho = s3.head_object(**fonte)
    extra = {c: cabecalho[c] for c in CABECALHOS_COPIADOS if c in cabecalho}
    extra["Metadata"] = {**cabecalho.get("Metadata", {}), META_ETAG_ORIGEM: etag}

    if obj["Size"] <= LIMITE_COPY_OBJECT:
        s3.copy_object(Bucket=bucket_destino, Key=chave, CopySource=fonte, MetadataDirective="REPLACE", **extra)
    else:
        # Acima do limite do CopyObject: cópia multipart (UploadPartCopy), ainda server-side
        s3.copy(fonte, bucket_destino, chave, ExtraArgs=extra, Config=config_multipart)


@bucket.command("clone")
@click.argument("bucket_origem", required=False)
@click.argument("bucket_destino", required=False)
@click.option("--segredo", default=None, help=f"Segredo do .segredos.yml  [padrão: {NOME_SEGREDO_DESENVOLVEDOR}]")
@click.option(
    "--prefixo",
    default="",
    help="Só considera objetos cuja chave comece com esse prefixo (ex.: lake/); vazio = bucket inteiro",
)
@click.option("--paralelismo", default=16, show_default=True, help="Cópias simultâneas")
@click.option("--dry-run", is_flag=True, help="Mostra o que seria copiado, pulado e apagado, sem alterar nada")
@click.option("--yes", is_flag=True, help="Não pedir confirmação nem perguntar origem/destino/segredo")
def clonar(
    bucket_origem: str | None,
    bucket_destino: str | None,
    segredo: str | None,
    prefixo: str,
    paralelismo: int,
    dry_run: bool,
    yes: bool,
):
    """
    Deixa o bucket de destino igual ao de origem.

    Arquivos que já existem no destino com o mesmo conteúdo (mesmo tamanho e
    ETag) são pulados, então rodar o comando de novo só copia o que mudou e
    retoma uma clonagem interrompida. Arquivos que existem no destino mas não
    na origem são apagados, só depois que a cópia termina: o destino nunca
    fica vazio no meio do processo.

    A cópia é feita no servidor, sem baixar nem subir dados. Cada arquivo de
    até 5GB é copiado com uma única requisição (copy_object); só os maiores
    que 5GB usam cópia multipart (s3.copy). Quando o ETag do destino não puder
    ser igual ao da origem (origem enviada em partes ou arquivo acima de 5GB),
    o ETag original é gravado no metadado colibri-etag-origem do destino.

    Sem ORIGEM, DESTINO ou --segredo, o comando pergunta em uma lista
    numerada. Fora de um terminal interativo, ou com --yes, usa os padrões:
    colibri-prod -> colibri-dev com o segredo de desenvolvedor.

    --prefixo restringe a cópia e a exclusão às chaves que começam com esse
    valor (ex.: --prefixo lake/tabela sincroniza só aquela subpasta, sem tocar
    no resto do bucket).

    Se o destino for colibri-prod, é preciso digitar o nome do bucket para
    confirmar, mesmo com --yes.
    """
    interativo = _interativo() and not yes

    if segredo is None:
        segredo = NOME_SEGREDO_DESENVOLVEDOR
        if interativo:
            segredo = _escolher("Segredo", _listar_segredos() or [segredo], segredo)

    s3 = _cliente(segredo, max_conexoes=paralelismo + 10)

    buckets = []
    if interativo and (bucket_origem is None or bucket_destino is None):
        buckets = _listar_buckets(s3)
    if bucket_origem is None:
        bucket_origem = BUCKET_PRODUCAO
        if interativo:
            bucket_origem = _escolher("Bucket de origem", buckets, BUCKET_PRODUCAO, livre=True)
    if bucket_destino is None:
        bucket_destino = BUCKET_DESENVOLVIMENTO
        if interativo:
            opcoes = [b for b in buckets if b != bucket_origem]
            bucket_destino = _escolher("Bucket de destino", opcoes, BUCKET_DESENVOLVIMENTO, livre=True)

    if bucket_origem == bucket_destino:
        console.print("[red]x[/red] Origem e destino não podem ser o mesmo bucket.")
        return

    paginator = s3.get_paginator("list_objects_v2")

    with Progress(
        SpinnerColumn(style=AZUL),
        TextColumn(f"[{AZUL}]Comparando arquivos..."),
        transient=True,
    ) as p:
        p.add_task("")
        objetos_origem = [
            obj
            for page in paginator.paginate(Bucket=bucket_origem, Prefix=prefixo)
            for obj in page.get("Contents", [])
        ]
        objetos_destino = {
            obj["Key"]: obj
            for page in paginator.paginate(Bucket=bucket_destino, Prefix=prefixo)
            for obj in page.get("Contents", [])
        }

        if not objetos_origem:
            p.stop()
            console.print(f"[yellow]Bucket [bold]{bucket_origem}[/bold] vazio. Nada para clonar.[/yellow]")
            return

        # Só precisa de HEAD quando o tamanho bate mas o ETag não; fazemos em paralelo
        em_ambos = [obj for obj in objetos_origem if obj["Key"] in objetos_destino]
        with ThreadPoolExecutor(max_workers=paralelismo) as executor:
            iguais = executor.map(
                lambda obj: _ja_copiado(s3, bucket_destino, obj, objetos_destino[obj["Key"]]), em_ambos
            )
            chaves_iguais = {obj["Key"] for obj, igual in zip(em_ambos, iguais, strict=True) if igual}

    chaves_origem = {obj["Key"] for obj in objetos_origem}
    a_copiar = [obj for obj in objetos_origem if obj["Key"] not in chaves_iguais]
    a_apagar = [{"Key": chave} for chave in objetos_destino if chave not in chaves_origem]
    bytes_copiar = sum(obj["Size"] for obj in a_copiar)
    bytes_pulados = sum(obj["Size"] for obj in objetos_origem) - bytes_copiar
    grandes = sum(1 for obj in a_copiar if obj["Size"] > LIMITE_COPY_OBJECT)

    resumo = (
        f"De [{AZUL}]{bucket_origem}[/] para [{AZUL}]{bucket_destino}[/]"
        + (f" [dim](prefixo {prefixo})[/dim]" if prefixo else "")
        + f"\n[{VERDE}]{len(a_copiar)}[/] arquivo(s) serão copiados [dim]({_tamanho(bytes_copiar)})[/dim]"
        + (f"[dim], {grandes} acima de 5GB via cópia multipart[/dim]" if grandes else "")
        + f"\n[bold]{len(chaves_iguais)}[/bold] arquivo(s) iguais serão pulados "
        f"[dim]({_tamanho(bytes_pulados)})[/dim]"
        + f"\n[red]{len(a_apagar)}[/red] arquivo(s) que não existem na origem serão apagados de "
        f"[bold]{bucket_destino}[/bold] depois da cópia"
    )
    console.print(Panel(resumo, title="Resumo da clonagem", border_style="yellow"))

    if not a_copiar and not a_apagar:
        console.print(f"[{VERDE}]+[/] [bold]{bucket_destino}[/bold] já está igual a [bold]{bucket_origem}[/bold].")
        return

    if dry_run:
        console.print("[dim]--dry-run: nada foi alterado.[/dim]")
        return

    if bucket_destino == BUCKET_PRODUCAO:
        console.print("[red]![/red] O destino é o bucket de [bold]produção[/bold].")
        resposta = click.prompt(f"Digite '{bucket_destino}' para confirmar", default="", show_default=False)
        if resposta.strip() != bucket_destino:
            console.print("[yellow]Operação cancelada.[/yellow]")
            return
    elif not yes and not click.confirm("Continuar?", default=False):
        console.print("[yellow]Operação cancelada.[/yellow]")
        return

    inicio = time.monotonic()
    config_multipart = TransferConfig(
        multipart_threshold=LIMITE_COPY_OBJECT,
        multipart_chunksize=256 * 1024**2,
        max_concurrency=4,
    )

    falhas = []
    if a_copiar:
        with Progress(
            SpinnerColumn(style=VERDE),
            TextColumn(f"[{AZUL}]Copiando arquivos..."),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
        ) as p:
            task = p.add_task("", total=len(a_copiar))
            with ThreadPoolExecutor(max_workers=paralelismo) as executor:
                futuros = {
                    executor.submit(_copiar_objeto, s3, bucket_origem, bucket_destino, obj, config_multipart): obj
                    for obj in a_copiar
                }
                for futuro in as_completed(futuros):
                    obj = futuros[futuro]
                    try:
                        futuro.result()
                    except Exception as e:
                        falhas.append(obj)
                        p.console.print(f"[red]x[/red] Falha ao copiar [bold]{obj['Key']}[/bold]: {e}")
                    p.advance(task)

    falhas_exclusao = []
    if a_apagar:
        with Progress(
            SpinnerColumn(style="red"),
            TextColumn(f"[red]Apagando {len(a_apagar)} arquivo(s) de {bucket_destino}..."),
            TimeElapsedColumn(),
            transient=True,
        ) as p:
            p.add_task("")
            for i in range(0, len(a_apagar), 1000):
                lote = a_apagar[i : i + 1000]
                try:
                    resposta = s3.delete_objects(Bucket=bucket_destino, Delete={"Objects": lote})
                except Exception as e:
                    # A requisição do lote inteiro falhou: registra cada arquivo e segue para o próximo lote
                    falhas_exclusao.extend({"Key": obj["Key"], "Message": str(e)} for obj in lote)
                    continue
                falhas_exclusao.extend(resposta.get("Errors", []))

    duracao = time.monotonic() - inicio
    copiados = len(a_copiar) - len(falhas)
    bytes_copiados = bytes_copiar - sum(obj["Size"] for obj in falhas)
    console.print(
        f"[{VERDE}]+[/] [bold]{copiados}[/bold] arquivo(s) copiado(s) ([bold]{_tamanho(bytes_copiados)}[/bold]), "
        f"[bold]{len(chaves_iguais)}[/bold] pulado(s), "
        f"[bold]{len(a_apagar) - len(falhas_exclusao)}[/bold] apagado(s) "
        f"de [{AZUL}]{bucket_origem}[/] para [{AZUL}]{bucket_destino}[/] "
        f"em [bold]{_duracao(duracao)}[/bold]"
    )
    if falhas:
        console.print(
            f"[red]![/red] {len(falhas)} arquivo(s) falharam ao copiar. Rode o comando de novo para tentar só esses."
        )
    for erro in falhas_exclusao:
        console.print(f"[red]x[/red] Falha ao apagar [bold]{erro.get('Key')}[/bold]: {erro.get('Message')}")

    console.print(
        Panel(
            "O armazenamento é faturado por espaço usado e tempo armazenado. Quando os dados "
            f"copiados para [bold]{bucket_destino}[/bold] não forem mais necessários, apague-os "
            "(colibri bucket purge) para evitar pagar em dobro pelo mesmo dado.",
            title="👀 De olho no faturamento... 💸",
            border_style="dim",
        )
    )


def _conectar_lake(bucket: str, segredo: str):
    import utils.ducklake as dl

    caminho_meta = f"s3://{bucket}/meta.ducklake"
    data_path = f"s3://{bucket}/lake/"
    return dl.conectar(caminho_meta, data_path, segredo)


@lake.command("tables")
@click.option("--bucket", default="colibri-prod", show_default=True, help="Bucket do lake")
@click.option(
    "--segredo",
    default=NOME_SEGREDO_VISUALIZADOR,
    show_default=True,
    help="Segredo com acesso ao bucket",
)
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
@click.option("--bucket", default="colibri-prod", show_default=True, help="Bucket do lake")
@click.option(
    "--segredo",
    default=NOME_SEGREDO_VISUALIZADOR,
    show_default=True,
    help="Segredo com acesso ao bucket",
)
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
@click.option("--bucket", default="colibri-prod", show_default=True, help="Bucket do lake")
@click.option(
    "--segredo",
    default=NOME_SEGREDO_VISUALIZADOR,
    show_default=True,
    help="Segredo com acesso ao bucket",
)
def lake_download(tabela: str, destino: str | None, bucket: str, segredo: str):
    """Baixa os parquets de uma tabela do catálogo para o disco"""
    con = _conectar_lake(bucket, segredo)
    try:
        row = con.execute(
            """
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
        """,
            [tabela, tabela],
        ).fetchone()
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
@click.option("--bucket", default="colibri-prod", show_default=True, help="Bucket do lake")
@click.option(
    "--segredo",
    default=NOME_SEGREDO_VISUALIZADOR,
    show_default=True,
    help="Segredo com acesso ao bucket",
)
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
@click.option("--bucket", default="colibri-prod", show_default=True, help="Bucket do lake")
@click.option(
    "--segredo",
    default=NOME_SEGREDO_VISUALIZADOR,
    show_default=True,
    help="Segredo com acesso ao bucket",
)
def ui(bucket: str, segredo: str):
    """Abre DuckDB UI conectado ao catálogo"""
    con = _conectar_lake(bucket, segredo)
    con.execute("CALL start_ui();")
    input("Pressione Enter para fechar o servidor e encerrar o script...")


@lake.command("drop-table")
@click.argument("tabela")
@click.option("--bucket", default="colibri-prod", show_default=True, help="Bucket do lake")
@click.option(
    "--segredo",
    default=NOME_SEGREDO_DESENVOLVEDOR,
    show_default=True,
    help="Segredo com acesso de escrita ao bucket",
)
def deletar_tabela(tabela: str, bucket: str, segredo: str):
    """Remove uma tabela do catálogo (exclusão lógica, recuperável até a próxima manutenção)"""
    import utils.ducklake as dl

    if segredo == NOME_SEGREDO_VISUALIZADOR:
        console.print(
            "[red]x[/red] Esse comando exige um segredo com acesso de escrita (o de visualizador é somente leitura)."
        )
        return

    caminho_meta = f"s3://{bucket}/meta.ducklake"
    con = _conectar_lake(bucket, segredo)

    row = con.execute(
        """
        SELECT s.schema_name
        FROM __ducklake_metadata_lake.main.ducklake_schema s
        JOIN __ducklake_metadata_lake.main.ducklake_table t USING (schema_id)
        WHERE t.table_name = ? AND t.end_snapshot IS NULL

        UNION

        SELECT s.schema_name
        FROM __ducklake_metadata_lake.main.ducklake_schema s
        JOIN __ducklake_metadata_lake.main.ducklake_view v USING (schema_id)
        WHERE v.view_name = ? AND v.end_snapshot IS NULL

        ORDER BY schema_name DESC
        LIMIT 1
        """,
        [tabela, tabela],
    ).fetchone()

    if not row:
        console.print(f"[red]x[/red] Tabela não encontrada: [bold]{tabela}[/bold]")
        con.close()
        return

    schema_name = row[0]
    console.print(
        f"Isso vai remover [bold]{schema_name}.{tabela}[/bold] do catálogo em "
        f"[bold]{bucket}[/bold] (exclusão lógica — recuperável via time travel "
        "até a próxima manutenção)."
    )
    resposta = click.prompt("Digite 'sim' para confirmar", default="", show_default=False)
    if resposta.strip().lower() != "sim":
        console.print("[yellow]Operação cancelada.[/yellow]")
        con.close()
        return

    try:
        con.execute(f'DROP TABLE lake."{schema_name}"."{tabela}"')
    except Exception:
        try:
            con.execute(f'DROP VIEW lake."{schema_name}"."{tabela}"')
        except Exception as e:
            console.print(f"[red]x[/red] {e}")
            con.close()
            return

    dl.fechar(con, caminho_meta, segredo)
    console.print(f"[{VERDE}]+[/] Tabela [bold]{schema_name}.{tabela}[/bold] removida do catálogo")


@lake.command("maintenance")
@click.option(
    "--dias",
    default=1,
    show_default=True,
    type=int,
    help="Dias de retenção do histórico antes de expirar snapshots",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Apenas mostra o que seria removido, sem excluir nada",
)
@click.option("--bucket", default="colibri-prod", show_default=True, help="Bucket do lake")
@click.option(
    "--segredo",
    default=NOME_SEGREDO_DESENVOLVEDOR,
    show_default=True,
    help="Segredo com acesso de escrita ao bucket",
)
def manutencao(dias: int, dry_run: bool, bucket: str, segredo: str):
    """Expira snapshots antigos e remove os arquivos órfãos do catálogo"""
    import utils.ducklake as dl

    caminho_meta = f"s3://{bucket}/meta.ducklake"
    con = _conectar_lake(bucket, segredo)

    def _executar(sql: str, params: list, titulo: str):
        resultado = con.execute(sql, params).fetchdf()
        console.print(f"[bold]{titulo}[/bold]")
        if resultado.empty:
            console.print("[yellow]Nada a fazer.[/yellow]")
            return
        tb = _tabela_dados(*[str(c) for c in resultado.columns])
        for _, row in resultado.iterrows():
            tb.add_row(*[str(v) for v in row])
        console.print(tb)

    try:
        _executar(
            "CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL (?) DAY, dry_run => true)",
            [dias],
            "Snapshots que seriam expirados",
        )
        _executar(
            "CALL ducklake_cleanup_old_files('lake', cleanup_all => true, dry_run => true)",
            [],
            "Arquivos que seriam removidos",
        )
    except Exception as e:
        console.print(f"[red]x[/red] {e}")
        con.close()
        return

    if dry_run:
        con.close()
        return

    resposta = click.prompt("\nDigite 'sim' para confirmar a execução", default="", show_default=False)
    if resposta.strip().lower() != "sim":
        console.print("[yellow]Operação cancelada.[/yellow]")
        con.close()
        return

    try:
        # cleanup_all, não older_than: o "older_than" do cleanup mede a idade do
        # agendamento da exclusão (feito agora mesmo pelo expire acima), não a
        # idade do snapshot original — usá-lo aqui faria o cleanup nunca apagar
        # nada expirado na mesma execução.
        _executar(
            "CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL (?) DAY)",
            [dias],
            "Snapshots expirados",
        )
        _executar(
            "CALL ducklake_cleanup_old_files('lake', cleanup_all => true)",
            [],
            "Arquivos removidos",
        )
    except Exception as e:
        console.print(f"[red]x[/red] {e}")
        con.close()
        return

    dl.fechar(con, caminho_meta, segredo)
    console.print(f"[{VERDE}]+[/] Catálogo sincronizado de volta para o bucket")


@pipeline.command("run")
@click.option(
    "--apenas",
    type=click.Choice(
        [
            "ncm",
            "pncp-comprasgov",
            "catmats",
            "nfe-cgu",
            "margem-preferencia",
            "tradutor-catmat-ncm",
        ]
    ),
    default=None,
    help="Rodar só um pipeline",
)
@click.option("--bucket", default=None, help="Bucket de destino (padrão: definido nas constantes)")
def run(apenas: str | None, bucket: str | None):
    """Roda o pipeline completo ou apenas um modulo"""
    import ingestion.catmats.pipeline as catmats
    import ingestion.margem_preferencia.pipeline as margem_preferencia
    import ingestion.ncm.pipeline as ncm
    import ingestion.nfe_cgu.pipeline as nfe_cgu
    import ingestion.pncp_comprasgov.pipeline as pncp_comprasgov
    import ingestion.tradutor_catmat_ncm.pipeline as tradutor_catmat_ncm

    pipelines = {
        "ncm": (ncm, "[cyan]>>> NCM[/cyan]"),
        "pncp-comprasgov": (pncp_comprasgov, "[cyan]>>> PNCP ComprasGOV[/cyan]"),
        "catmats": (catmats, "[cyan]>>> CATMATS[/cyan]"),
        "nfe-cgu": (nfe_cgu, "[cyan]>>> NFe-CGU[/cyan]"),
        "margem-preferencia": (
            margem_preferencia,
            "[cyan]>>> Margem de Preferência[/cyan]",
        ),
        "tradutor-catmat-ncm": (
            tradutor_catmat_ncm,
            "[cyan]>>> Tradutor CATMAT -> NCM[/cyan]",
        ),
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
]


@cli.command("sincronizar")
@click.option("--segredo", default=NOME_SEGREDO_DESENVOLVEDOR, show_default=True)
def sincronizar(segredo: str):
    """Baixa os manifestos e o catálogo DuckLake do bucket para a máquina local"""
    import os
    from pathlib import Path

    config = carregar_segredo(segredo)
    bucket = config["bucket_lake"]
    s3 = _cliente(segredo)
    raiz = Path(CATALOGO_LOCAL).parent

    itens = [(nome, str(raiz / "dados" / "manifestos" / nome)) for nome in _MANIFESTOS] + [
        ("meta.ducklake", CATALOGO_LOCAL),
    ]

    ausentes = []
    for chave, destino in itens:
        os.makedirs(os.path.dirname(destino) or ".", exist_ok=True)
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn(f"[cyan]Baixando {chave}..."),
                transient=True,
            ) as p:
                p.add_task("")
                s3.download_file(bucket, chave, destino)
            tamanho = os.path.getsize(destino)
            console.print(f"[green]+[/green] {chave} -> [bold]{destino}[/bold] [dim]({_tamanho(tamanho)})[/dim]")
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                ausentes.append(chave)
                console.print(f"[yellow]![/yellow] {chave}: não encontrado no bucket")
            else:
                console.print(f"[red]x[/red] {chave}: {e}")
        except Exception as e:
            console.print(f"[red]x[/red] {chave}: {e}")

    if ausentes:
        console.print(f"\n[yellow]Arquivos não encontrados no bucket:[/yellow] {', '.join(ausentes)}")


@cli.command("docs")
@click.option("--sem-servidor", is_flag=True, help="Gera a documentação sem abrir o servidor")
def docs(sem_servidor: bool):
    """Gera e exibe a documentação automática do dbt"""
    import subprocess
    from pathlib import Path

    dbt_dir = Path(__file__).resolve().parent / "dbt"

    console.print("[cyan]>>> Gerando documentação...[/cyan]")
    subprocess.run(
        [
            "dbt",
            "docs",
            "generate",
            "--project-dir",
            str(dbt_dir),
            "--profiles-dir",
            str(dbt_dir),
        ],
        cwd=str(dbt_dir),
        check=True,
    )

    if not sem_servidor:
        console.print("[cyan]>>> Servindo documentação em http://localhost:8080[/cyan]")
        subprocess.run(
            [
                "dbt",
                "docs",
                "serve",
                "--project-dir",
                str(dbt_dir),
                "--profiles-dir",
                str(dbt_dir),
            ],
            cwd=str(dbt_dir),
            check=True,
        )


if __name__ == "__main__":
    cli()
