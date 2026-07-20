from pathlib import Path
from typing import NamedTuple


class Tarefa(NamedTuple):
    """
    NamedTuple que guarda qual é o nome do registro no manifesto (tabela:identificador),
    qual URL baixar, onde salvar no bucket e quais alterações salvar no manifesto:
        .`identificador` normalmente é um período de calendário ("2022-01"), mas
         não precisa ser — no catmats, por exemplo, é o número da página.
    """

    tabela: str
    identificador: str
    url: str
    chave_bucket: Path
    alteracoes: list[tuple[str, ...]]