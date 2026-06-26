# colibri
![](colibri_fundo_verde.png)
Data lakehouse do Observatório de Contratações Públicas

# Instalação

Requer Python 3.12 ou superior.

1. Clone o repositório
```bash
git clone https://github.com/heitorgama/colibri
cd colibri
```

2. Instale a CLI do `colibri` localmente (instala as dependências do Python)
```bash
pip install -e .
```

3. Instale as dependências do dbt
```bash
cd dbt
dbt deps
cd ..
```

4. Configure os segredos do bucket R2: copie o template e preencha com suas
   credenciais
```bash
cp .segredos_template.yml .segredos.yml
```

5. Configure o `profiles.yml` do dbt: copie o template (já vem pronto, sem
   caminhos para editar — os caminhos são relativos ao diretório `dbt/`)
```bash
cp dbt/profiles_template.yml dbt/profiles.yml
```

## Desinstalação da CLI
```bash
pip uninstall colibri
```

# Comandos da CLI

## Pipeline

```bash
colibri pipeline run                        # NCM + PNCP ComprasGOV
colibri pipeline run --apenas ncm
colibri pipeline run --apenas pncp-comprasgov
```

O pipeline ComprasGOV é incremental: arquivos já baixados (mesmo hash do manifesto)
são pulados, e somente os registros novos/atualizados entram no histórico
versionado (SCD2) das tabelas `compras`, `itens` e `resultados`.

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
colibri lake tabelas                    # tabelas no catálogo com contagem de linhas/parquets
colibri lake download <tabela>          # exporta uma tabela para parquet/csv local
colibri lake query "<sql>"                  # query livre
```

Requer `meta.ducklake` na raiz do projeto (gerado/baixado automaticamente pelo
pipeline, ou via `colibri sincronizar`).

---

## Documentação do dbt

```bash
colibri docs                # gera e abre a documentação dos modelos dbt
colibri docs --sem-servidor # apenas gera, sem subir o servidor local
```

---

# Arquitetura

```
R2 (colibri-dev)
├── meta.ducklake          ← catálogo DuckLake (metadados)
└── lake/
    ├── main_staging/
    ├── main_intermediate/
    └── main_marts/

R2 (colibri-arquivos)
└── raw/ncm/               ← JSONs brutos do NCM
```

# Tabelas

| Tabela | Fonte | Atualização |
|--------|-------|-------------|
| `ncm_prefixos` | Portal Único Siscomex | Full replace quando há mudança |
| `int_pncp_comprasgov__compras` | comprasGOV (diário/mensal/anual) | Incremental, histórico versionado (SCD2) por `cod_compra` |
| `int_pncp_comprasgov__itens` | comprasGOV (diário/mensal/anual) | Incremental, histórico versionado (SCD2) por `id_compra_item` |
| `int_pncp_comprasgov__resultados` | comprasGOV (diário/mensal/anual) | Incremental, histórico versionado (SCD2) por `(id_compra_item, sequencial_resultado)` |
| `mrt_pncp_comprasgov__resumo_anual` | agregação de `int_pncp_comprasgov__compras` | Recalculada a cada execução |

# Validação

A tabela `mrt_pncp_comprasgov__resumo_anual` traz, por ano, a quantidade de compras
e os valores totais estimado/homologado. Esses números podem ser comparados com os
totais publicados no painel ["PNCP em Números"](https://www.gov.br/pncp/pt-br/painel-pncp)
para conferir a consistência da ingestão.

```bash
colibri lake query "SELECT * FROM lake.main_marts.mrt_pncp_comprasgov__resumo_anual ORDER BY ano_compra"
```
