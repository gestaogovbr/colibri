"""Utilitários compartilhados pelos testes da extração DELOG (sem rede, sem R2, sem credenciais reais)."""

import io
import zipfile
from pathlib import Path

import duckdb
import requests

# Credenciais claramente falsas: qualquer aparição disso em log/saída é detectável.
FAKE_CONFIG = {
    "endpoint": "https://fake-account-id.r2.cloudflarestorage.com",
    "access_key": "FAKE_ACCESS_KEY_NAO_E_REAL_123",
    "secret_key": "FAKE_SECRET_KEY_NAO_E_REAL_456",
}

PREFIXOS = {"compras": "compra", "itens": "compra_item", "resultados": "item_resultado"}
URL_BASE = "https://repositorio.dados.gov.br/seges/comprasgov/pncp"

CSV_PADRAO = (
    "cod_compra;descricao;cnpj;valor\n"
    "001;Aquisição de café;00123456000199;10,50\n"
    "002;;00000000000001;\n"
    '003;"Texto; com ponto e vírgula";12345678000100;0007\n'
).encode()


class FakeResposta:
    """Imita requests.Response em modo stream (context manager + iter_content)."""

    def __init__(self, status_code=200, conteudo=b"", erro_no_stream=None):
        self.status_code = status_code
        self.conteudo = conteudo
        self.erro_no_stream = erro_no_stream

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Error", response=self)

    def iter_content(self, chunk_size=1):
        passo = max(1, len(self.conteudo) // 3)
        pedacos = [self.conteudo[i : i + passo] for i in range(0, len(self.conteudo), passo)]
        for i, pedaco in enumerate(pedacos):
            yield pedaco
            if self.erro_no_stream and i == 0:
                raise self.erro_no_stream


class FakeSessao:
    """Imita requests.Session. `efeito(url, **kw)` devolve FakeResposta ou levanta exceção."""

    def __init__(self, efeito):
        self.efeito = efeito
        self.headers = {}
        self.chamadas = []

    @property
    def urls(self):
        return [c[0] for c in self.chamadas]

    def get(self, url, **kwargs):
        self.chamadas.append((url, kwargs))
        return self.efeito(url, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    # construtores de conveniência
    @classmethod
    def status(cls, codigo):
        return cls(lambda url, **kw: FakeResposta(codigo))

    @classmethod
    def com_zip(cls, conteudo_zip: bytes):
        return cls(lambda url, **kw: FakeResposta(200, conteudo_zip))

    @classmethod
    def com_csv(cls, csv_bytes: bytes, nome="dados.csv"):
        return cls.com_zip(zip_bytes({nome: csv_bytes}))

    @classmethod
    def levantando(cls, excecao):
        def _efeito(url, **kw):
            raise excecao

        return cls(_efeito)


def zip_bytes(arquivos: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as z:
        for nome, conteudo in arquivos.items():
            z.writestr(nome, conteudo)
    return buffer.getvalue()


def executar(modulo, sessao, tipo="compras", ano=2025, mes=8):
    return modulo.processar_tipo(sessao, ano, mes, tipo)


def caminho_saida(tipo="compras", ano=2025, mes=8) -> Path:
    """Caminho local esperado (relativo ao cwd, que os testes isolam em tmp_path)."""
    return Path("dados/pncp_comprasgov_delog") / tipo / str(ano) / f"{mes:02d}" / f"{tipo}_{ano}_{mes:02d}.parquet"


def chave_r2(tipo="compras", ano=2025, mes=8) -> str:
    return f"pncp_comprasgov_delog/{tipo}/{ano}/{mes:02d}/{tipo}_{ano}_{mes:02d}.parquet"


def ler_parquet(caminho: Path):
    """Devolve (nomes_colunas, tipos, linhas) do Parquet."""
    with duckdb.connect(":memory:") as con:
        desc = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(caminho)]).fetchall()
        linhas = con.execute("SELECT * FROM read_parquet(?)", [str(caminho)]).fetchall()
    return [d[0] for d in desc], [d[1] for d in desc], linhas


def compressoes(caminho: Path) -> set[str]:
    with duckdb.connect(":memory:") as con:
        return {
            r[0]
            for r in con.execute("SELECT DISTINCT compression FROM parquet_metadata(?)", [str(caminho)]).fetchall()
        }


def criar_parquet_local(tipo="compras", ano=2025, mes=8, conteudo=None) -> Path:
    """Cria um Parquet VÁLIDO (ou bytes arbitrários) no caminho de saída, dentro do cwd isolado."""
    destino = caminho_saida(tipo, ano, mes)
    destino.parent.mkdir(parents=True, exist_ok=True)
    if conteudo is None:
        with duckdb.connect(":memory:") as con:
            con.execute(f"COPY (SELECT '001' AS a) TO '{destino}' (FORMAT PARQUET)")
    else:
        destino.write_bytes(conteudo)
    return destino
