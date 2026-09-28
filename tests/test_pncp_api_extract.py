"""Testes do extrator de cabeçalhos da API do PNCP (ingestion/pncp/extract.py).

Tudo é simulado: sessão HTTP falsa e relógio falso. Nada toca rede nem disco.
O foco é o que custou caro descobrir na mão: ritmo diante do 429 e, principalmente,
a diferença entre "página vazia" e "página que falhou" — confundir as duas fez a
primeira versão perder 150 registros em silêncio.
"""

from datetime import date

import pytest

from ingestion.pncp import extract as ex


class RespostaFake:
    def __init__(self, status_code=200, corpo=None):
        self.status_code = status_code
        self._corpo = corpo or {}
        self.text = ""

    def json(self):
        return self._corpo


class SessaoFake:
    """Devolve as respostas na ordem configurada e registra as páginas pedidas."""

    def __init__(self, respostas):
        self.respostas = list(respostas)
        self.paginas_pedidas = []

    def get(self, url, params=None, timeout=None):
        self.paginas_pedidas.append(params["pagina"])
        return self.respostas.pop(0) if self.respostas else RespostaFake(500)


def pagina(registros, total_registros, total_paginas):
    return RespostaFake(
        200,
        {
            "data": [{"numeroControlePNCP": f"x-{i}"} for i in range(registros)],
            "totalRegistros": total_registros,
            "totalPaginas": total_paginas,
            "empty": registros == 0,
        },
    )


@pytest.fixture(autouse=True)
def sem_espera(monkeypatch):
    """Nenhum teste paga o preço real das pausas."""
    monkeypatch.setattr(ex.time, "sleep", lambda _: None)


# Ritmo


def test_ritmo_recua_no_429_e_respeita_o_teto():
    ritmo = ex.Ritmo(pausa=2.0)
    ritmo.recuo()
    assert ritmo.pausa == 4.0
    for _ in range(10):
        ritmo.recuo()
    assert ritmo.pausa == ex.PAUSA_MAXIMA


def test_ritmo_nunca_acelera_abaixo_do_piso_calibrado():
    ritmo = ex.Ritmo(pausa=ex.PAUSA_MINIMA)
    for _ in range(ex.ACERTOS_PARA_ACELERAR * 5):
        ritmo.sucesso()
    assert ritmo.pausa == ex.PAUSA_MINIMA


def test_ritmo_so_acelera_depois_da_sequencia_limpa():
    ritmo = ex.Ritmo(pausa=4.0)
    for _ in range(ex.ACERTOS_PARA_ACELERAR - 1):
        ritmo.sucesso()
    assert ritmo.pausa == 4.0
    ritmo.sucesso()
    assert ritmo.pausa < 4.0


# pedir_pagina


def test_pagina_vazia_e_falha_nao_se_confundem():
    custo = ex.Custo()
    sessao = SessaoFake([RespostaFake(204), RespostaFake(500)])
    corpo, situacao = ex.pedir_pagina(sessao, "publicacao", {"pagina": 1}, ex.Ritmo(), custo)
    assert (corpo, situacao) == (None, "vazia")
    corpo, situacao = ex.pedir_pagina(sessao, "publicacao", {"pagina": 2}, ex.Ritmo(), custo)
    assert (corpo, situacao) == (None, "falha")


def test_429_e_repetido_e_contabilizado():
    custo = ex.Custo()
    sessao = SessaoFake([RespostaFake(429), RespostaFake(429), pagina(3, 3, 1)])
    corpo, situacao = ex.pedir_pagina(sessao, "publicacao", {"pagina": 1}, ex.Ritmo(), custo)
    assert situacao == "ok"
    assert len(corpo["data"]) == 3
    assert custo.erros_429 == 2
    assert custo.requisicoes == 3


def test_erro_de_servidor_e_repetido_mas_4xx_desiste_na_hora():
    """5xx é blip; 4xx que não é 429 é erro nosso — repetir só gastaria tempo."""
    custo = ex.Custo()
    sessao = SessaoFake([RespostaFake(502), pagina(2, 2, 1)])
    _, situacao = ex.pedir_pagina(sessao, "publicacao", {"pagina": 1}, ex.Ritmo(), custo)
    assert situacao == "ok"
    assert custo.requisicoes == 2

    sessao = SessaoFake([RespostaFake(400), pagina(2, 2, 1)])
    custo = ex.Custo()
    _, situacao = ex.pedir_pagina(sessao, "publicacao", {"pagina": 1}, ex.Ritmo(), custo)
    assert situacao == "falha"
    assert custo.requisicoes == 1


# varrer


def test_pagina_que_falhou_volta_na_repescagem():
    """Falha transitória não pode virar buraco: a página é pedida de novo no fim."""
    custo = ex.Custo()
    sessao = SessaoFake(
        [pagina(50, 100, 2)]  # página 1
        + [RespostaFake(500)] * ex.MAX_TENTATIVAS  # página 2 falha até desistir
        + [pagina(50, 100, 2)]  # repescagem da página 2
    )
    registros = ex.varrer(sessao, "publicacao", 8, date(2026, 8, 1), date(2026, 8, 31), ex.Ritmo(), custo)
    assert len(registros) == 100
    assert sessao.paginas_pedidas[-1] == 2
    assert custo.paginas_perdidas == []
    assert custo.divergencias == []


def test_pagina_perdida_de_vez_e_denunciada():
    """Se nem a repescagem resolve, o prejuízo aparece nas métricas — não em silêncio."""
    custo = ex.Custo()
    sessao = SessaoFake([pagina(50, 100, 2)] + [RespostaFake(500)] * (ex.MAX_TENTATIVAS * 2))
    registros = ex.varrer(sessao, "publicacao", 8, date(2026, 8, 1), date(2026, 8, 31), ex.Ritmo(), custo)
    assert len(registros) == 50
    assert custo.paginas_perdidas == [(8, 2)]
    assert custo.divergencias == [{"modalidade": 8, "esperado": 100, "obtido": 50}]


def test_periodo_sem_dados_nao_gera_arquivo():
    custo = ex.Custo()
    sessao = SessaoFake([pagina(0, 0, 0)])
    assert ex.varrer(sessao, "publicacao", 7, date(2026, 8, 1), date(2026, 8, 31), ex.Ritmo(), custo) == []


# normalizar


def test_normalizar_achata_aninhados_e_serializa_listas():
    quadro = ex.normalizar(
        [
            {
                "numeroControlePNCP": "abc-1",
                "orgaoEntidade": {"cnpj": "123", "razaoSocial": "MUNICIPIO X"},
                "fontesOrcamentarias": [{"codigo": 1}],
            }
        ]
    )
    assert quadro.loc[0, "orgaoEntidade_cnpj"] == "123"
    assert quadro.loc[0, "fontesOrcamentarias"] == '[{"codigo": 1}]'


def test_normalizar_aceita_lista_ausente():
    quadro = ex.normalizar([{"numeroControlePNCP": "abc-1", "fontesOrcamentarias": None}])
    assert quadro.loc[0, "fontesOrcamentarias"] is None


# Página curta — a API às vezes devolve 200 com 20 registros em vez de 50


def pagina_com_ids(ids, total_registros, total_paginas):
    resposta = pagina(0, total_registros, total_paginas)
    resposta._corpo.update({"data": [{"numeroControlePNCP": i} for i in ids], "empty": not ids})
    return resposta


def test_esperado_na_pagina_considera_a_ultima():
    assert ex.esperado_na_pagina(1, 847) == 50
    assert ex.esperado_na_pagina(17, 847) == 47
    assert ex.esperado_na_pagina(18, 847) == 0


def test_pagina_curta_e_pedida_de_novo():
    """O caso de 23/09: página 2 veio com 20 de 50 e ninguém percebeu."""
    custo = ex.Custo()
    ids_p2 = [f"b-{i}" for i in range(50)]
    sessao = SessaoFake(
        [
            pagina_com_ids([f"a-{i}" for i in range(50)], 100, 2),
            pagina_com_ids(ids_p2[:20], 100, 2),  # curta
            pagina_com_ids(ids_p2, 100, 2),  # inteira
        ]
    )
    registros = ex.varrer(sessao, "publicacao", 7, date(2026, 8, 1), date(2026, 8, 31), ex.Ritmo(), custo)
    assert len(registros) == 100
    assert len({r["numeroControlePNCP"] for r in registros}) == 100
    assert sessao.paginas_pedidas == [1, 2, 2]
    assert custo.paginas_curtas == 1
    assert custo.paginas_curtas_aceitas == []
    assert custo.divergencias == []


def test_primeira_pagina_curta_tambem_e_conferida():
    custo = ex.Custo()
    sessao = SessaoFake([pagina(20, 60, 2), pagina(50, 60, 2), pagina(10, 60, 2)])
    registros = ex.varrer(sessao, "publicacao", 7, date(2026, 8, 1), date(2026, 8, 31), ex.Ritmo(), custo)
    assert len(registros) == 60
    assert sessao.paginas_pedidas == [1, 1, 2]


def test_pagina_que_segue_curta_fica_com_a_maior_versao_e_e_denunciada():
    """Pode ser dado vivo (registro removido no meio); jogar a página fora perderia mais."""
    custo = ex.Custo()
    tentativas = ex.TENTATIVAS_PAGINA_CURTA + 1
    sessao = SessaoFake([pagina(50, 100, 2), pagina(20, 100, 2)] + [pagina(49, 100, 2)] * (tentativas - 1))
    registros = ex.varrer(sessao, "publicacao", 7, date(2026, 8, 1), date(2026, 8, 31), ex.Ritmo(), custo)
    assert len(registros) == 99
    assert custo.paginas_curtas == tentativas
    assert custo.paginas_curtas_aceitas == [{"modalidade": 7, "pagina": 2, "obtido": 49, "esperado": 50}]
    assert custo.divergencias == [{"modalidade": 7, "esperado": 100, "obtido": 99}]


def test_pagina_curta_seguida_de_falha_nao_perde_o_que_ja_veio():
    custo = ex.Custo()
    sessao = SessaoFake([pagina(50, 100, 2), pagina(20, 100, 2)] + [RespostaFake(400)])
    registros = ex.varrer(sessao, "publicacao", 7, date(2026, 8, 1), date(2026, 8, 31), ex.Ritmo(), custo)
    assert len(registros) == 70
    assert custo.paginas_curtas_aceitas == [{"modalidade": 7, "pagina": 2, "obtido": 20, "esperado": 50}]
    assert custo.paginas_perdidas == []
