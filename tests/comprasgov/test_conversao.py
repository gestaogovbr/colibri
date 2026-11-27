"""Grupo C — extração do ZIP e conversão CSV -> Parquet (arquivos fictícios criados em runtime)."""

import pytest

from tests.comprasgov.apoio import CSV_PADRAO, FakeSessao, caminho_saida, compressoes, executar, ler_parquet, zip_bytes


def converter(modulo, csv_bytes, nome="dados.csv"):
    status = executar(modulo, FakeSessao.com_csv(csv_bytes, nome))
    return status


def test_extrai_csv_valido(modulo):
    assert converter(modulo, CSV_PADRAO) == "criado"
    assert caminho_saida().is_file()


@pytest.mark.parametrize("nome", ["dados.csv", "DADOS.CSV", "pasta/sub/dados.csv"])
def test_csv_com_nome_ou_pasta_diferente_no_zip(modulo, nome):
    assert converter(modulo, CSV_PADRAO, nome) == "criado"


def test_arquivo_extra_nao_csv_no_zip_e_ignorado(modulo):
    z = zip_bytes({"dados.csv": CSV_PADRAO, "LEIAME.txt": b"x"})
    assert executar(modulo, FakeSessao.com_zip(z)) == "criado"


def test_delimitador_ponto_e_virgula(modulo):
    converter(modulo, CSV_PADRAO)
    colunas, _, linhas = ler_parquet(caminho_saida())
    assert colunas == ["cod_compra", "descricao", "cnpj", "valor"]
    assert len(linhas) == 3


def test_todas_colunas_varchar(modulo):
    converter(modulo, CSV_PADRAO)
    _, tipos, _ = ler_parquet(caminho_saida())
    assert set(tipos) == {"VARCHAR"}


def test_preserva_texto_acentos_e_ponto_e_virgula_entre_aspas(modulo):
    converter(modulo, CSV_PADRAO)
    _, _, linhas = ler_parquet(caminho_saida())
    assert linhas[0][1] == "Aquisição de café"
    assert linhas[2][1] == "Texto; com ponto e vírgula"


def test_preserva_quebra_de_linha_dentro_de_aspas(modulo):
    csv = b'a;b\n1;"linha1\nlinha2"\n2;x\n'
    converter(modulo, csv)
    _, _, linhas = ler_parquet(caminho_saida())
    assert linhas == [("1", "linha1\nlinha2"), ("2", "x")]


def test_preserva_zeros_a_esquerda(modulo):
    converter(modulo, CSV_PADRAO)
    _, _, linhas = ler_parquet(caminho_saida())
    assert [linha[0] for linha in linhas] == ["001", "002", "003"]
    assert linhas[0][2] == "00123456000199"
    assert linhas[1][2] == "00000000000001"
    assert linhas[2][3] == "0007"


def test_preserva_decimal_com_virgula_como_texto(modulo):
    converter(modulo, CSV_PADRAO)
    _, _, linhas = ler_parquet(caminho_saida())
    assert linhas[0][3] == "10,50"


def test_campos_vazios_viram_null(modulo):
    converter(modulo, CSV_PADRAO)
    _, _, linhas = ler_parquet(caminho_saida())
    assert linhas[1][1] is None and linhas[1][3] is None


def test_bom_utf8_nao_contamina_nome_da_coluna(modulo):
    converter(modulo, b"\xef\xbb\xbf" + CSV_PADRAO)
    colunas, _, _ = ler_parquet(caminho_saida())
    assert colunas[0] == "cod_compra"


def test_compressao_zstd(modulo):
    converter(modulo, CSV_PADRAO)
    assert compressoes(caminho_saida()) == {"ZSTD"}


@pytest.mark.parametrize("n", [1, 5, 1000])
def test_quantidade_de_linhas(modulo, n):
    csv = "id;v\n" + "".join(f"{i:05d};x\n" for i in range(n))
    converter(modulo, csv.encode())
    _, _, linhas = ler_parquet(caminho_saida())
    assert len(linhas) == n


def test_csv_so_com_cabecalho_gera_parquet_vazio(modulo):
    """Caracterização: mês sem registros vira Parquet com 0 linhas e 'criado'."""
    assert converter(modulo, b"a;b;c\n") == "criado"
    colunas, _, linhas = ler_parquet(caminho_saida())
    assert colunas == ["a", "b", "c"] and linhas == []


@pytest.mark.limitacao
def test_LIMITACAO_csv_totalmente_vazio_deveria_retornar_erro(modulo):
    """DuckDB lê 0 bytes como 1 coluna 'column0' sem linhas: vira 'criado' (Parquet inútil que bloqueia reexecução)."""
    assert converter(modulo, b"") == "erro"
    assert not caminho_saida().exists()


def test_csv_com_linha_de_menos_colunas_retorna_erro_sem_arquivo(modulo):
    assert converter(modulo, b"a;b;c\n1;2\n") == "erro"
    assert not caminho_saida().exists()


@pytest.mark.limitacao
@pytest.mark.parametrize(
    "csv",
    [b"a;b;c\n1;2;3;4\n", b'a;b\n1;"abc\n2;3\n'],
    ids=["colunas_a_mais", "aspas_nao_fechadas"],
)
def test_LIMITACAO_csv_malformado_deveria_retornar_erro_sem_arquivo(modulo, csv):
    """Caracterização: read_csv aceita silenciosamente CSVs malformados.

    'colunas_a_mais' perde o cabeçalho e gera 0 linhas.
    'aspas_nao_fechadas' interpreta aspas não fechadas como conteúdo.
    """
    assert converter(modulo, csv) == "erro"
    assert not caminho_saida().exists()


def test_caracterizacao_csv_latin1_retorna_erro(modulo):
    """Caracterização: o código assume UTF-8; Latin-1 com acento é rejeitado (erro), não corrompido."""
    csv = "a;b\n1;Aquisição\n".encode("latin-1")
    assert converter(modulo, csv) == "erro"
    assert not caminho_saida().exists()


def test_caracterizacao_delimitador_errado_nao_e_detectado(modulo):
    """Caracterização: CSV separado por vírgula vira 1 coluna 'criada' sem alerta (sem validação de schema)."""
    assert converter(modulo, b"a,b,c\n1,2,3\n") == "criado"
    colunas, _, _ = ler_parquet(caminho_saida())
    assert len(colunas) == 1
