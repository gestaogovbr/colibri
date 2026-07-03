"""
Gera dados/dim_margem_ncm_utf8.csv a partir dos CSVs de resolução em dados/margem_preferencia/CICS.

Uso:
  python -m ingestion.margem_preferencia.pipeline
"""

import runpy


ETAPAS = [
    "ingestion.margem_preferencia.CICS.dim_margem_resolucoes",
    "ingestion.margem_preferencia.CICS.fato_margem_eventos",
    "ingestion.margem_preferencia.CICS.dim_margem_ncm_prefixos",
    "ingestion.margem_preferencia.CICS.dim_margem_ncm",
]


def main():
    for modulo in ETAPAS:
        runpy.run_module(modulo, run_name="__main__")


if __name__ == "__main__":
    main()
