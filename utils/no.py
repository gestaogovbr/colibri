class No:
    def __init__(self, codigo, descricao):
        """
        Classe que representa um nó de uma árvore taxonômica, com código, descrição e filhos.
        Os filhos são armazenados em uma lista de outros nós, e também em um dicionário para acesso rápido.

        :param codigo: Código do nó
        :param descricao: Descrição do nó
        :param filhos: Lista de nós filhos

        :type codigo: str
        :type descricao: str
        :type filhos: list
        """
        self.codigo = codigo
        self.descricao = descricao
        self.filhos = []
        self.caminho = [self]  # Lista de nós, da raiz até este nó
        self.caminho_tamanho = 0  # Número de itens no caminho

    def __repr__(self):
        return f"No({self.codigo}, {self.descricao})"

    def __str__(self):
        listagem = ""
        for no in self:
            listagem += f"{no.codigo.ljust(10)}: {no.descricao}\n"
        return listagem.strip()  # Remove a última quebra de linha desnecessária

    def __len__(self):
        """
        Retorna o número total de descendentes diretos e indiretos deste nó,
        subtraindo 1 para não contar a si mesmo.
        Se não houver descendentes, retorna 0.

        :return: Número total de descendentes diretos e indiretos
        :rtype: int
        """
        contagem = 0
        for _filho in self:
            contagem += 1
        return contagem - 1 if contagem > 0 else 0

    def __iter__(self):
        yield self
        for filho in self.filhos:
            yield from filho

    def adicionar_filho(self, filho):
        """
        Adiciona um nó filho a este nó, autoproclama-se seu pai
        e transfere-lhe sua genealogia (caminho) desde a raiz até o filho.

        :param filho: Nó filho a ser adicionado
        :type filho: No
        """
        if not isinstance(filho, No):
            raise TypeError("O filho deve ser uma instância da classe No.")
        if hasattr(filho, "pai"):
            raise ValueError("O nó filho já possui um pai.")
        if filho in self.filhos:
            raise ValueError("O nó filho já está adicionado a este nó.")

        self.filhos.append(filho)
        filho.caminho_tamanho = self.caminho_tamanho + 1
        self.adicionar_este_no_como_pai(filho)

    def adicionar_este_no_como_pai(self, no):
        if hasattr(no, "pai"):
            raise ValueError("Este nó já possui um pai.")
        no.pai = self
        no.caminho = self.caminho + [no]  # Herda o caminho do pai e adiciona a si mesmo

    def contar_filhos_diretos(self):
        """
        Conta o número de filhos diretos deste nó.

        :return: Número de filhos diretos
        :rtype: int
        """
        return len(self.filhos)

    def selecionar_no(self, codigo):
        """
        Seleciona um nó pelo seu código, retornando o nó correspondente ou None se não encontrado.

        :param codigo: Código do nó a ser selecionado
        :type codigo: str
        :return: Nó correspondente ao código ou None
        :rtype: No or None
        """
        if self.codigo == codigo:
            return self
        for filho in self.filhos:
            no_selecionado = filho.selecionar_no(codigo)
            if no_selecionado is not None:
                return no_selecionado
        return None

    def podar_por_tamanho_do_caminho(self, tamanho):
        """
        Remove todos os nós cujo caminho é maior que o tamanho especificado.

        :param tamanho: Tamanho máximo do caminho para manter os nós
        :type tamanho: int
        """
        self.filhos = [filho for filho in self.filhos if filho.caminho_tamanho <= tamanho]
        for filho in self.filhos:
            filho.podar_por_tamanho_do_caminho(tamanho)

    def pesquisar(self, expressao):
        """
        Pesquisa por nós que contenham a expressão na descrição, retornando uma lista de nós correspondentes.

        :param expressao: Expressão a ser pesquisada na descrição dos nós
        :type expressao: str
        :return: Lista de nós que contêm a expressão na descrição
        :rtype: list
        """
        nos_encontrados = []
        if expressao.lower() in self.descricao.lower():
            nos_encontrados.append(self)
        for filho in self.filhos:
            nos_encontrados.extend(filho.pesquisar(expressao))
        return nos_encontrados

    def transformar_caminho_em_string(self, sep=" > "):
        """
        Transforma o caminho do nó em uma string, separando os códigos pelo separador informado como argumento.
        Ignora a raiz.

        :return: String representando o caminho do nó
        :rtype: str
        """
        return sep.join(no.descricao for no in self.caminho[1:])
