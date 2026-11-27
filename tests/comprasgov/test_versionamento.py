import duckdb

from ingestion.comprasgov.extract import publicar_snapshot
from tests.comprasgov.apoio import CSV_PADRAO, FakeSessao
from utils.manifesto_duckdb import consultar_entrada, consultar_versoes


def test_publicacao_imutavel(tmp_path, monkeypatch):
    import ingestion.comprasgov.extract as extract

    monkeypatch.setattr(extract, "DIRETORIO_SAIDA", tmp_path)

    origem = tmp_path / "origem.parquet"

    duckdb.sql("SELECT 1 AS id").write_parquet(str(origem))

    destino1, hash1 = publicar_snapshot(origem, "compras", 2025, 8)

    assert destino1.exists()
    assert hash1[:10] in destino1.name

    # Publicar novamente o mesmo conteúdo
    destino_repetido, hash_repetido = publicar_snapshot(origem, "compras", 2025, 8)

    assert destino_repetido == destino1
    assert hash_repetido == hash1

    # Simular uma nova versão da fonte
    origem.unlink()
    duckdb.sql("SELECT 2 AS id").write_parquet(str(origem))

    destino2, hash2 = publicar_snapshot(origem, "compras", 2025, 8)

    assert destino2 != destino1
    assert hash2 != hash1
    assert destino1.exists()
    assert destino2.exists()

    assert len(list(destino1.parent.glob("*.parquet"))) == 2


def test_extract_publica_snapshot(tmp_path, monkeypatch):
    import ingestion.comprasgov.extract as extract

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(extract, "DIRETORIO_SAIDA", tmp_path / "snapshots")

    sessao = FakeSessao.com_csv("id;descricao\n1;original\n")

    resultado = extract.processar_tipo(sessao, 2025, 8, "compras")

    assert resultado == "criado"

    pasta = tmp_path / "snapshots/compras/2025/08/versoes"
    arquivos = list(pasta.glob("*.parquet"))

    assert len(arquivos) == 1
    assert arquivos[0].name.startswith("compras_2025_08_")
    assert len(arquivos[0].stem.split("_")[-1]) == 10


def test_extract_preserva_versoes(tmp_path, monkeypatch):
    import ingestion.comprasgov.extract as extract

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(extract, "DIRETORIO_SAIDA", tmp_path / "snapshots")

    sessao1 = FakeSessao.com_csv("id;descricao\n1;original\n")
    sessao2 = FakeSessao.com_csv("id;descricao\n1;atualizado\n")

    assert extract.processar_tipo(sessao1, 2025, 8, "compras") == "criado"

    assert extract.processar_tipo(sessao2, 2025, 8, "compras") == "criado"

    pasta = tmp_path / "snapshots/compras/2025/08/versoes"
    arquivos = list(pasta.glob("*.parquet"))

    assert len(arquivos) == 2
    assert len({arquivo.name for arquivo in arquivos}) == 2


def test_extract_registra_versao_sem_confirmar_baseline(local):
    sessao = FakeSessao.com_csv(CSV_PADRAO)

    assert local.processar_tipo(sessao, 2025, 8, "compras") == "criado"

    versoes = consultar_versoes("comprasgov_pncp", "compras", 2025, 8)

    assert len(versoes) == 1
    assert versoes[0]["processado_em"] is None

    assert consultar_entrada("comprasgov_pncp", "compras", 2025, 8) is None
