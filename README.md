# colibri
![](colibri_fundo_verde.png)
Data lakehouse do Observatório de Contratações Públicas

# Instalação da CLI

Requer `.segredos.yaml` na raiz do projeto com as credenciais do R2. Utilize `.segredos_template.yaml` como modelo.

1. Instale o python 3.12 ou superior
2. Clone o repositório
```bash
git clone https://github.com/heitorgama/colibri
```
<!-- 3. Instale as dependências do python
```bash
pip install -r requirements.txt
``` -->
4. Instale as dependências do dbt
```bash
dbt deps
```
5. Configure o arquivo `profiles.yml` do dbt com as credenciais do seu banco
6. Instale a CLI do `colibri` localmente
```bash
pip install -e .
```

## Desinstalação da CLI
```bash
pip uninstall colibri
```

# Comandos da CLI

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

# Arquitetura

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

# Tabelas

| Tabela | Fonte | Atualização |
|--------|-------|-------------|
| `ncm_prefixos` | Portal Único Siscomex | Full replace quando há mudança |
| `pncp_compra`  | comprasGOV anual | Incremental por ano, upsert por `cod_compra` |


# Pipelines

## ComprasGov

1. Executar o extrator de dados do ComprasGov, que salvará os dados por padrão no diretório `dados/pncp_comprasgov`. Esse diretório pode ser alterado usando a opção `--diretorio-saida` do comando abaixo. Outras 
```bash
python -m extracao.origens.pncp_comprasgov --data_fim 2025-12-31
```
2. Executar o dbt para transformar os dados extraídos e criar views no banco de dados. Para executar somente os modelos relacionados ao ComprasGov, use a opção `--select pncp_comprasgov` do comando abaixo. Se quiser executar todos os modelos, basta rodar o comando sem a opção `--select`.
```bash
cd dbt
dbt run --select staging.pncp_comprasgov
```
3. Em breve, serão desenvolvidos modelos adicionais para criar tabelas de fatos e dimensões a partir dos dados do ComprasGov.
