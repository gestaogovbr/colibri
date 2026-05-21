# colibri
![](colibri_fundo_verde.png)
Data lakehouse do Observatório de Contratações Públicas

---

## Setup

```bash
pip install -e .
```

Cria o comando `colibri` globalmente. Requer `.segredos.yaml` na raiz do projeto com as credenciais do R2.

---

## Pipeline

```bash
colibri pipeline run               # NCM + PNCP
colibri pipeline run --apenas ncm
colibri pipeline run --apenas pncp
```

O pipeline é incremental — anos já carregados são pulados. O ano corrente é sempre atualizado via upsert por `data_atualizacao`.

---

## Bucket (R2)

```bash
colibri bucket ls <bucket>
colibri bucket ls <bucket> --prefixo lake/

colibri bucket download <arquivo> <bucket>
colibri bucket download <arquivo> <bucket> --destino ./local.parquet

colibri bucket upload <arquivo> <bucket>
colibri bucket upload <arquivo> <bucket> --chave pasta/nome.csv

colibri bucket rm <arquivo> <bucket>
colibri bucket rm-all <bucket>
colibri bucket rm-all <bucket> --prefixo lake/
```

---

## Lake (DuckLake)

```bash
colibri lake tabelas                    # tabelas no catálogo com contagem de linhas
colibri lake anos <tabela>              # linhas por ano
colibri lake q "<sql>"                  # query livre
```

Requer `meta.ducklake` na raiz do projeto (gerado automaticamente pelo pipeline).

---

## Arquitetura

```
R2 (colibri-dev)
├── meta.ducklake          ← catálogo DuckLake (metadados)
└── lake/
    └── main/
        └── pncp_compra/
            ├── ducklake-*.parquet        ← dados
            └── ducklake-*-delete.parquet ← deleções (upsert)

R2 (colibri-arquivos)
└── raw/ncm/               ← JSONs brutos do NCM
```

## Tabelas

| Tabela | Fonte | Atualização |
|--------|-------|-------------|
| `ncm_prefixos` | Portal Único Siscomex | Full replace quando há mudança |
| `pncp_compra`  | comprasGOV anual | Incremental por ano, upsert por `cod_compra` |
