"""Testes do extrator de UASGs/órgãos, sem rede: a API é substituída por respostas simuladas."""

import csv

import pytest

import ingestion.uasg.extract as ex


def _resposta(registros: list[dict], total: int, paginas: int) -> dict:
    return {"resultado": registros, "totalRegistros": total, "totalPaginas": paginas, "paginasRestantes": 0}


def test_normalizar_ordena_pela_chave_e_mantem_so_campos_esperados():
    campos = ["codigoUasg", "nomeUasg"]
    linhas = ex.normalizar(
        [{"codigoUasg": "2", "nomeUasg": "B", "extra": 1}, {"codigoUasg": "10", "nomeUasg": "A"}],
        campos,
        "codigoUasg",
    )
    assert linhas == [{"codigoUasg": "10", "nomeUasg": "A"}, {"codigoUasg": "2", "nomeUasg": "B"}]


def test_hash_nao_depende_da_ordem_de_paginacao():
    a = [{"codigoUasg": "1", "nomeUasg": "A"}, {"codigoUasg": "2", "nomeUasg": "B"}]
    campos, chave = ["codigoUasg", "nomeUasg"], "codigoUasg"
    h1 = ex.calcular_hash(ex.normalizar(a, campos, chave))
    h2 = ex.calcular_hash(ex.normalizar(list(reversed(a)), campos, chave))
    assert h1 == h2
    alterado = [{"codigoUasg": "1", "nomeUasg": "A2"}, a[1]]
    assert ex.calcular_hash(ex.normalizar(alterado, campos, chave)) != h1


def test_deduplicar_mantem_movimento_mais_recente():
    registros = [
        {"codigoUasg": "1", "nomeUasg": "velho", "dataHoraMovimento": "2020-01-01T00:00:00"},
        {"codigoUasg": "1", "nomeUasg": "novo", "dataHoraMovimento": "2026-01-01T00:00:00"},
        {"codigoUasg": "2", "nomeUasg": "B", "dataHoraMovimento": None},
    ]
    unicos = {r["codigoUasg"]: r["nomeUasg"] for r in ex.deduplicar(registros, "codigoUasg")}
    assert unicos == {"1": "novo", "2": "B"}


def test_ultimo_hash_por_entidade():
    manifesto = [
        {"entidade": "uasgs", "hash_sha256": "a", "extraido_em": "2026-01-01T00:00:00"},
        {"entidade": "uasgs", "hash_sha256": "b", "extraido_em": "2026-02-01T00:00:00"},
        {"entidade": "orgaos", "hash_sha256": "c", "extraido_em": "2026-03-01T00:00:00"},
    ]
    assert ex.ultimo_hash(manifesto, "uasgs") == "b"
    assert ex.ultimo_hash(manifesto, "orgaos") == "c"
    assert ex.ultimo_hash([], "uasgs") is None


def test_extrair_entidade_percorre_ativos_inativos_e_paginas(monkeypatch):
    chamadas = []

    def falso(session, endpoint, params):
        chamadas.append((params["statusUasg"], params["pagina"]))
        if params["statusUasg"] == "true":
            pagina = {1: [{"codigoUasg": "1"}], 2: [{"codigoUasg": "2"}]}[params["pagina"]]
            return _resposta(pagina, total=2, paginas=2)
        return _resposta([{"codigoUasg": "3", "statusUasg": False}], total=1, paginas=1)

    monkeypatch.setattr(ex, "_buscar_pagina", falso)
    linhas = ex.extrair_entidade(None, "uasgs")
    assert [linha["codigoUasg"] for linha in linhas] == ["1", "2", "3"]
    assert chamadas == [("true", 1), ("true", 2), ("false", 1)]


def test_extrair_entidade_falha_com_snapshot_incompleto(monkeypatch):
    # A API informa 2 registros ativos, mas a paginação devolve só 1: gravar esse snapshot
    # criaria uma exclusão falsa no histórico.
    def falso(session, endpoint, params):
        if params["statusUasg"] == "true":
            return _resposta([{"codigoUasg": "1"}], total=2, paginas=1)
        return _resposta([], total=0, paginas=0)

    monkeypatch.setattr(ex, "_buscar_pagina", falso)
    with pytest.raises(RuntimeError, match="incompleto"):
        ex.extrair_entidade(None, "uasgs")


def test_executar_ingestao_grava_so_quando_o_conteudo_muda(monkeypatch, tmp_path):
    monkeypatch.setattr(ex, "DIRETORIO_SAIDA", tmp_path / "uasg")
    monkeypatch.setattr(ex, "DIRETORIO_MANIFESTOS", tmp_path / "manifestos")
    monkeypatch.setattr(ex, "DIRETORIO_ALTERACOES", tmp_path / "alteracoes")

    cadastro = {"uasgs": [{"codigoUasg": "1", "nomeUasg": "A"}], "orgaos": [{"codigoOrgao": "9"}]}
    monkeypatch.setattr(ex, "extrair_entidade", lambda session, entidade: list(cadastro[entidade]))

    assert ex.executar_ingestao(sincronizar_manifesto=False) is True
    assert len(list((tmp_path / "uasg").glob("uasgs_*.csv"))) == 1
    assert len(list((tmp_path / "uasg").glob("orgaos_*.csv"))) == 1

    # Segunda execução com o mesmo conteúdo: nada novo, alterações vazias
    assert ex.executar_ingestao(sincronizar_manifesto=False) is False
    with open(tmp_path / "alteracoes" / ex.NOME_ALTERACOES, encoding="utf-8") as f:
        assert list(csv.DictReader(f)) == []

    # Só as UASGs mudam: grava um snapshot novo só delas
    cadastro["uasgs"] = [{"codigoUasg": "1", "nomeUasg": "A alterada"}]
    monkeypatch.setattr(ex, "datetime", _DatetimeFixo)
    assert ex.executar_ingestao(sincronizar_manifesto=False) is True
    with open(tmp_path / "alteracoes" / ex.NOME_ALTERACOES, encoding="utf-8") as f:
        alteracoes = list(csv.DictReader(f))
    assert [a["entidade"] for a in alteracoes] == ["uasgs"]
    with open(tmp_path / "uasg" / alteracoes[0]["arquivo_csv"], encoding="utf-8") as f:
        linhas = list(csv.DictReader(f))
    assert linhas[0]["nomeUasg"] == "A alterada"
    assert linhas[0][ex.COLUNA_EXTRAIDO_EM] == "2030-01-01 00:00:00"


class _DatetimeFixo:
    """Garante um nome de arquivo diferente do snapshot anterior (gravado no mesmo segundo)"""

    @staticmethod
    def now():
        from datetime import datetime

        return datetime(2030, 1, 1)
