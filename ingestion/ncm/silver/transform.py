from shared.no import No


def criar_mapa_ncm(ncm_json: dict):
    """
    Cria um dicionário que mapeia códigos NCM para seus caminhos.
    """
    ncm_mapa = {}
    for entrada in ncm_json["Nomenclaturas"]:
        codigo = entrada["Codigo"]
        descricao = entrada["Descricao"]
        ncm_mapa[codigo] = descricao
    return ncm_mapa


def obter_nivel_do_codigo(codigo):
    """
    Obtém o nível do código NCM baseado no número de dígitos,
    separados ou não por pontos.
    """
    num_digitos = len([caractere for caractere in codigo if caractere.isdigit()])
    mapa_digitos_nivel = {
        2: "Capítulo",
        4: "Posição",
        5: "Subposição 1",
        6: "Subposição 2",
        7: "Item",
        8: "Subitem",
    }
    return (
        num_digitos if num_digitos in mapa_digitos_nivel else None
    ), mapa_digitos_nivel.get(num_digitos, "Desconhecido")


def obter_pai_do_codigo(codigo, mapa_ncm: dict):
    """
    Obtém o pai do código NCM, retornando o código do pai.
    """
    if not codigo:
        return None
    if not mapa_ncm:
        return None
    if not isinstance(codigo, str):
        raise TypeError("O código deve ser uma string.")
    if not isinstance(mapa_ncm, dict):
        raise TypeError("O mapa_ncm deve ser informado em forma de dicionário.")
    codigo_sem_pontos = codigo.replace(
        ".", ""
    )  # Remove pontos do código para facilitar a busca
    mapa_ncm_sem_ponto = {
        codigo.replace(".", ""): codigo for codigo in mapa_ncm.keys()
    }  # Mapeia o código sem ponto para o código original
    for tamanho in range(
        len(codigo_sem_pontos) - 1, 0, -1
    ):  # Itera do tamanho máximo para o mínimo
        codigo_reduzido = codigo_sem_pontos[0:tamanho]
        if codigo_reduzido in mapa_ncm_sem_ponto.keys():
            return mapa_ncm_sem_ponto[codigo_reduzido]


def criar_arvore_ncm(mapa_ncm: dict):
    """
    Cria a árvore NCM a partir do arquivo JSON.
    """
    Ncm = No("Raiz NCM", "Nomenclatura Comum do Mercosul")  # Cria a raiz da árvore NCM

    for codigo, descricao in mapa_ncm.items():
        if obter_nivel_do_codigo(codigo)[1] == "Capítulo":
            Ncm.adicionar_filho(No(codigo, descricao))
    for codigo, descricao in mapa_ncm.items():
        if obter_nivel_do_codigo(codigo)[1] == "Posição":
            pai_codigo = obter_pai_do_codigo(codigo, mapa_ncm)
            if pai_codigo:
                Ncm.selecionar_no(pai_codigo).adicionar_filho(No(codigo, descricao))
    for codigo, descricao in mapa_ncm.items():
        if obter_nivel_do_codigo(codigo)[1] == "Subposição 1":
            pai_codigo = obter_pai_do_codigo(codigo, mapa_ncm)
            if pai_codigo:
                Ncm.selecionar_no(pai_codigo).adicionar_filho(No(codigo, descricao))
    for codigo, descricao in mapa_ncm.items():
        if obter_nivel_do_codigo(codigo)[1] == "Subposição 2":
            pai_codigo = obter_pai_do_codigo(codigo, mapa_ncm)
            if pai_codigo:
                Ncm.selecionar_no(pai_codigo).adicionar_filho(No(codigo, descricao))
    for codigo, descricao in mapa_ncm.items():
        if obter_nivel_do_codigo(codigo)[1] == "Item":
            pai_codigo = obter_pai_do_codigo(codigo, mapa_ncm)
            if pai_codigo:
                Ncm.selecionar_no(pai_codigo).adicionar_filho(No(codigo, descricao))
    for codigo, descricao in mapa_ncm.items():
        if obter_nivel_do_codigo(codigo)[1] == "Subitem":
            pai_codigo = obter_pai_do_codigo(codigo, mapa_ncm)
            if pai_codigo:
                Ncm.selecionar_no(pai_codigo).adicionar_filho(No(codigo, descricao))
    return Ncm


def gerar_ncm_enriquecido(arvore):
    nomenclaturas = []
    for no in arvore:
        if no.codigo == "Raiz NCM":
            continue
        _, nivel = obter_nivel_do_codigo(no.codigo)
        capitulo = next(
            (
                n
                for n in no.caminho[1:]
                if obter_nivel_do_codigo(n.codigo)[1] == "Capítulo"
            ),
            None,
        )
        posicao = next(
            (
                n
                for n in no.caminho[1:]
                if obter_nivel_do_codigo(n.codigo)[1] == "Posição"
            ),
            None,
        )
        subposicao_1 = next(
            (
                n
                for n in no.caminho[1:]
                if obter_nivel_do_codigo(n.codigo)[1] == "Subposição 1"
            ),
            None,
        )
        subposicao_2 = next(
            (
                n
                for n in no.caminho[1:]
                if obter_nivel_do_codigo(n.codigo)[1] == "Subposição 2"
            ),
            None,
        )
        item = next(
            (n for n in no.caminho[1:] if obter_nivel_do_codigo(n.codigo)[1] == "Item"),
            None,
        )
        subitem = next(
            (
                n
                for n in no.caminho[1:]
                if obter_nivel_do_codigo(n.codigo)[1] == "Subitem"
            ),
            None,
        )
        nomenclaturas.append(
            {
                "Codigo": no.codigo,
                "Descricao": no.descricao,
                "Nivel": nivel,
                "Caminho": no.transformar_caminho_em_string(),
                "Capitulo_Codigo": capitulo.codigo if capitulo else None,
                "Capitulo_Descricao": capitulo.descricao if capitulo else None,
                "Posicao_Codigo": posicao.codigo if posicao else None,
                "Posicao_Descricao": posicao.descricao if posicao else None,
                "Subposicao_1_Codigo": subposicao_1.codigo if subposicao_1 else None,
                "Subposicao_1_Descricao": (
                    subposicao_1.descricao if subposicao_1 else None
                ),
                "Subposicao_2_Codigo": subposicao_2.codigo if subposicao_2 else None,
                "Subposicao_2_Descricao": (
                    subposicao_2.descricao if subposicao_2 else None
                ),
                "Item_Codigo": item.codigo if item else None,
                "Item_Descricao": item.descricao if item else None,
                "Subitem_Codigo": subitem.codigo if subitem else None,
                "Subitem_Descricao": subitem.descricao if subitem else None,
            }
        )
    return nomenclaturas


# mapa_ncm = criar_mapa_ncm(ncm_json)
# arvore_ncm = criar_arvore_ncm(mapa_ncm)
# ncm_enriquecido = gerar_ncm_enriquecido(arvore_ncm)
