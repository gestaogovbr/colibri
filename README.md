# colibri
![](colibri_fundo_verde.png)
Mini data lakehouse do Observatório de Contratações Públicas

Este README é o caminho mais curto para **instalar o colibri e consultar os dados**.
A documentação completa está no site — instalação passo a passo, guia do analista
(SQL, R, Python, DuckDB UI) e guia do desenvolvedor:
**https://gestaogovbr.github.io/colibri/**

# Instalação

## 1. Python

O colibri requer Python 3.12 ou superior.

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
instruções. No Windows, marque a opção *Add python.exe to PATH* na primeira
tela do instalador.

</details>

## 2. Clonar o repositório e criar o ambiente virtual

Um ambiente virtual isola as dependências do colibri do resto do sistema. Crie-o
dentro da pasta do repositório, com o nome `env` — é o que o `.gitignore` e o
restante da documentação assumem.

**Mac / Linux**

```bash
git clone https://github.com/gestaogovbr/colibri
cd colibri
python3 -m venv env
source env/bin/activate
```

**Windows (PowerShell)**

```powershell
git clone https://github.com/gestaogovbr/colibri
cd colibri
python -m venv env
.\env\Scripts\Activate.ps1
```

Se o PowerShell bloquear a ativação com um erro de política de execução, rode
uma vez (sem precisar de administrador) e ative de novo:

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

Com o ambiente ativo, o prompt passa a mostrar `(env)`. Ative-o sempre que abrir
um terminal novo para usar o colibri.

## 3. Instalar a CLI

```bash
pip install -e .
colibri --help
```

## 4. Credenciais

Copie o template e preencha com as credenciais que você recebeu:

```bash
cp .segredos_template.yml .segredos.yml     # Windows: copy .segredos_template.yml .segredos.yml
```

O arquivo tem dois perfis:

- `colibri-token-visualizador` — **somente leitura**. É o que os comandos
  `colibri lake` usam por padrão, e tudo de que você precisa para consultar os dados.
- `colibri-token-desenvolvedor` — leitura e escrita, para rodar pipelines e alterar
  o lake. Só quem desenvolve precisa dele (veja [Para desenvolver](#para-desenvolver)).

Enquanto o colibri está em fase de testes, as credenciais são concedidas pela equipe
do projeto. Sem elas, é possível apontar o colibri para um bucket S3 próprio — veja
[Instalar o ambiente em um S3 próprio](https://gestaogovbr.github.io/colibri/04-guia-instalacao.html#instalar-o-ambiente-em-um-s3-próprio).

Pronto. Não há mais nada a baixar: os comandos `lake` buscam o catálogo do lake
(`meta.ducklake`) no bucket a cada execução.

# Consultar os dados

```bash
colibri lake tables                     # tabelas e views do catálogo, com contagem de linhas
colibri lake query "<sql>"              # consulta SQL livre
colibri lake download <tabela>          # exporta uma tabela para parquet local
colibri lake years <tabela>             # contagem de linhas por ano
colibri lake ui                         # abre a DuckDB UI no navegador, conectada ao lake
```

## No SQL, o nome completo; nos outros comandos, só o nome

Numa consulta SQL a tabela precisa do caminho completo, `lake.<schema>.<tabela>`:

```bash
colibri lake query "SELECT count(*) FROM lake.main_marts.mrt_pncp_comprasgov_compras"
```

Se faltar o caminho, o erro já traz o nome certo:

```
x Catalog Error: Table with name mrt_pncp_comprasgov_compras does not exist!
Did you mean "lake.main_marts.mrt_pncp_comprasgov_compras"?
```

Nos demais comandos (`download`, `years`) basta o nome da tabela — o colibri
descobre o schema:

```bash
colibri lake download mrt_pncp_comprasgov_compras                                  # salva ./mrt_pncp_comprasgov_compras.parquet
colibri lake download mrt_pncp_comprasgov_compras --destino ~/dados/compras.parquet
```

Os schemas seguem as camadas do dbt:

| Schema | O que tem |
|--------|-----------|
| `main_marts` | tabelas finais para análise — **comece por aqui** |
| `main_intermediate` | dados tratados, com histórico versionado (SCD2) |
| `main_staging` | dados como chegaram da fonte |

## O que existe hoje

Views em `lake.main_marts`:

| Tabela | Conteúdo |
|--------|----------|
| `mrt_pncp_comprasgov_compras` | compras do ComprasGOV/PNCP — versão atual de cada compra |
| `mrt_pncp_comprasgov_itens` | itens dessas compras — versão atual |
| `mrt_pncp_comprasgov_resultados` | resultados dos itens — versão atual |
| `mrt_margem__ncms_cics`, `mrt_margem__ncms_ciiapac` | NCMs com margem de preferência em vigor, por resolução (CICS / CIIAPAC) |
| `mrt_tradutor_catmat_ncm__mapeamento_ia` | tradutor CATMAT → NCM (classificação humana com apoio de IA) |

A lista completa e atual é a do `colibri lake tables`. O histórico de cada
registro (todas as versões, não só a atual) está nas tabelas
`int_pncp_comprasgov__*` em `main_intermediate`.

## DuckDB UI

`colibri lake ui` abre a [DuckDB UI](https://duckdb.org/2025/03/12/duckdb-ui)
no navegador, já conectada ao lake — um caderno de SQL com resultado imediato,
tipo de dado e percentual de nulos por coluna. Pressione Enter no terminal para
encerrar.

Na primeira abertura a UI carrega um caderno-exemplo do próprio DuckDB
("DuckDB UI basics"); a última célula dele grava um `trains.csv` de 33 MB na
pasta de onde você rodou o comando. É só um conjunto de dados de amostra — pode
apagar (`trains.csv` está no `.gitignore` para não entrar em commit por engano).

## Bucket e credencial

Todos os comandos `lake` aceitam `--bucket` (padrão `colibri-prod`) e `--segredo`
(padrão `colibri-token-visualizador`) — útil para apontar a um ambiente próprio.

# Validação

Uma checagem rápida de sanidade: compras publicadas por ano, para comparar com o
painel ["PNCP em Números"](https://pncp.gov.br/app/pncp-em-numeros).

```bash
colibri lake query "SELECT substr(data_publicacao_pncp, 1, 4) AS ano, count(*) AS compras FROM lake.main_marts.mrt_pncp_comprasgov_compras GROUP BY 1 ORDER BY 1"
```

# Para desenvolver

Rodar pipelines, escrever no bucket e manter o lake exigem o
`colibri-token-desenvolvedor`, o dbt configurado (`dbt deps`, `dbt/profiles.yml`)
e, para não tocar a produção, um ambiente próprio. Os comandos:

```bash
colibri pipeline run --apenas <fonte>   # ncm, pncp-comprasgov, catmats, nfe-cgu, margem-preferencia, tradutor-catmat-ncm
colibri bucket <comando>                # list, download, upload, delete, purge
colibri sincronizar                     # baixa manifestos e catálogo do bucket
colibri lake drop-table <tabela>        # exclusão lógica (recuperável via time travel)
colibri lake maintenance                # expira snapshots antigos e apaga parquets órfãos
colibri docs                            # documentação dos modelos dbt (exige o dbt configurado)
```

`colibri <comando> --help` mostra as opções. O passo a passo — instalação do dbt,
ambiente de desenvolvimento, camadas, testes e checklist de PR — está no site:

- [Guia de instalação](https://gestaogovbr.github.io/colibri/04-guia-instalacao.html) — passos 4 a 6: dbt e `profiles.yml`
- [Guia do desenvolvedor](https://gestaogovbr.github.io/colibri/05-guia-desenvolvedor.html)
- [Guia do analista](https://gestaogovbr.github.io/colibri/06-guia-analista.html) — conexão direta via R e Python

# Arquitetura

```
R2 (colibri-prod)
├── meta.ducklake          ← catálogo DuckLake (metadados)
└── lake/
    ├── main_staging/
    ├── main_intermediate/
    └── main_marts/        ← parquets das tabelas

R2 (colibri-arquivos)
└── ...                    ← arquivos brutos das fontes, por pipeline
```

# Desinstalação

```bash
pip uninstall colibri
```

Ou simplesmente apague a pasta `env/` — o ambiente virtual inteiro vai junto.
