# colibri
![](colibri_fundo_verde.png)
Mini data lakehouse do Observatório de Contratações Públicas

# Instalação


## Python

O Colibri requer Python 3.12 ou superior.

Se ainda não tiver o Python instalado:

<details>
<summary>Mac</summary>

Recomendamos instalar via [Homebrew](https://brew.sh/) em vez de baixar o
instalador do site oficial, pois este último exige rodar manualmente o
`Install Certificates.command` para que o Python reconheça certificados SSL
corretamente.

Se já tiver o Homebrew instalado, rode:

```bash
brew install python@3.12
```

</details>

<details>
<summary>Windows/Linux</summary>

Baixe o instalador em
[python.org/downloads](https://www.python.org/downloads/) e siga as
instruções.

</details>

## Colibri

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
colibri bucket list <bucket>
colibri bucket list <bucket> --prefixo lake/

colibri bucket download <arquivo> <bucket>
colibri bucket download <arquivo> <bucket> --destino ./local.parquet

colibri bucket upload <arquivo> <bucket>
colibri bucket upload <arquivo> <bucket> --chave pasta/nome.csv

colibri bucket delete <arquivo> <bucket>
colibri bucket purge <bucket>              # pede confirmação antes de apagar tudo
colibri bucket purge <bucket> --prefixo lake/ --yes
```

---

## Lake (DuckLake)

```bash
colibri lake tables                     # tabelas/views no catálogo com contagem de linhas/parquets
colibri lake years <tabela>             # contagem de linhas por ano de uma tabela
colibri lake download <tabela>          # exporta uma tabela para parquet local
colibri lake query "<sql>"              # query livre
colibri lake ui                         # abre a DuckDB UI conectada ao catálogo

colibri lake drop-table <tabela>        # remove uma tabela do catálogo (exige credencial de escrita)

colibri lake maintenance                # expira snapshots antigos e apaga os arquivos órfãos
colibri lake maintenance --dry-run      # só mostra o que seria expirado/apagado, sem alterar nada
colibri lake maintenance --dias 7       # mantém 7 dias de histórico em vez do padrão (1 dia)
```

Requer `meta.ducklake` na raiz do projeto (gerado/baixado automaticamente pelo
pipeline, ou via `colibri sincronizar`).

`colibri lake drop-table` recusa rodar com o segredo de visualizador (só funciona
com credencial de escrita), resolve o schema da tabela automaticamente, pede
confirmação (digite `sim`) antes de executar e sincroniza o catálogo de volta
para o bucket ao final. É uma exclusão lógica — os dados continuam recuperáveis
via time travel até a próxima `colibri lake maintenance`.

DuckLake nunca apaga dados antigos automaticamente: todo `DELETE`/`DROP`/refresh de
tabela fica preservado como um snapshot navegável (time travel), então o espaço no
bucket só é recuperado quando alguém roda a manutenção. `colibri lake maintenance`
expira os snapshots mais antigos que a retenção configurada e em seguida apaga os
arquivos parquet que ficaram órfãos, sincronizando o catálogo atualizado de volta
para o bucket. Rode periodicamente (cron/GitHub Action) para controlar o custo de
armazenamento — use `--dry-run` primeiro para conferir o que seria removido.

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
colibri lake q "SELECT * FROM lake.main_marts.mrt_pncp_comprasgov__resumo_anual ORDER BY ano_compra"
```
