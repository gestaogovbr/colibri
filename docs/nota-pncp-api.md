# Ligar o colibri à API pública do PNCP: o que ela aguenta

**Resultado em uma linha: o cabeçalho de contratação do PNCP inteiro — 4.085.829 registros de 2021 a 2026 — cabe num backfill de ~84 h (três dias e meio) em ritmo seguro e depois custa ~7 min por dia; o item, não: a API só entrega item de uma compra por vez, e isso são 1,08 milhão de requisições só para 2026.**

23/09/2026, revista em 27/09/2026 · André Maia · branch `feat/pncp-api-cabecalhos` ·
fonte real `pncp.gov.br/api/consulta/v1` e `pncp.gov.br/api/pncp/v1` · bancada: MinIO local

## 1 · Por que

O pipeline `pncp_comprasgov` não lê o PNCP: lê o dump do Compras.gov publicado em `repositorio.dados.gov.br`, que cobre o que passa pelo sistema federal. Município que publica por sistema próprio fica de fora.

O tamanho do buraco, medido em São José dos Campos: a API do PNCP devolve **3.171 dispensas em 2026** para o município (código IBGE 3549904), e as primeiras linhas são `FUNDO MUNICIPAL DE SAUDE` e `MUNICIPIO DE SAO JOSE DOS CAMPOS`. No lake, a mesma cidade no mesmo ano tem **461 compras e 180 dispensas** (mart, cópia local de setembro). O painel de 15/09, esse sim medido contra produção, já tinha achado a Prefeitura com 72 compras e **zero** dispensas na série inteira.

Some-se a isso que a fonte atual está parada desde 16/07/2026.

## 2 · O que a API entrega

| endpoint | serve para |
|---|---|
| `GET /consulta/v1/contratacoes/publicacao` | backfill de período fechado (data de publicação) |
| `GET /consulta/v1/contratacoes/atualizacao` | carga incremental (data de atualização) — é o eixo que o staging + SCD2 do colibri já sabem consumir |
| `GET /pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens` | itens de **uma** compra |
| `GET .../itens/{n}/resultados` | resultados de **um** item |

O cabeçalho vem com 35 campos, incluindo `valorTotalEstimado`, `valorTotalHomologado`, `dataAtualizacao`, `numeroControlePNCP` e o bloco `unidadeOrgao` com município e código IBGE. Filtros úteis: modalidade (obrigatório), UF e `codigoMunicipioIbge`.

**Limites medidos:**

- `tamanhoPagina` máximo é **50**; 100, 500 e 1000 devolvem `400 Tamanho de página inválido`.
- Janela de datas livre: 1 dia, 30 dias e o ano inteiro responderam 200.
- `codigoModalidadeContratacao` é obrigatório — cada período é varrido uma vez por modalidade.
- O `429` vem **sem `Retry-After` e sem cabeçalho de quota**. Não há como perguntar qual é o limite; só medindo.

## 3 · Ritmo sustentável (calibração)

25 requisições por ritmo, pausa fixa, contando o status de cada uma:

| pausa entre requisições | HTTP 200 | HTTP 429 | páginas boas/s |
|---|---:|---:|---:|
| 1,0 s | 9 | **16** | 0,18 |
| 2,0 s | 25 | 0 | **0,43** |
| 3,0 s | 25 | 0 | 0,31 |

Ou seja: **1 requisição a cada 2 s passa limpa; a 1 s, dois terços voltam 429.** Correr mais rápido rende menos, porque o castigo do 429 é maior que o tempo economizado. O extrator fixa esse piso de 2 s e só recua a partir dele.

A calibração de 25 requisições foi otimista quanto à vazão. Na varredura completa de agosto/2026 (2.583 requisições, 2 h 38 min, zero 429) o ritmo sustentado foi de **0,27 página/s**: aos 2 s de pausa soma-se a latência da própria API, ~1,7 s por página. É esse número que vale para orçar.

## 4 · Custo do backfill de cabeçalho

Censo por ano e modalidade (`totalRegistros` da própria API, 36 requisições):

| ano | Concorrência-E | Pregão-E | Pregão-Pres | Dispensa | Inexigibilidade | Credenciamento | total |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2021 | 0 | 0 | 0 | 7.234 | 1 | 0 | 7.235 |
| 2022 | 87 | 11.261 | 48 | 48.517 | 2.238 | 16 | 62.167 |
| 2023 | 3.162 | 55.044 | 863 | 162.939 | 29.050 | 1.142 | 252.200 |
| 2024 | 42.043 | 329.672 | 8.631 | 657.210 | 177.160 | 11.737 | 1.226.453 |
| 2025 | 48.166 | 396.635 | 11.000 | 717.937 | 261.339 | 21.614 | 1.456.691 |
| 2026* | 51.134 | 276.457 | 7.180 | 514.609 | 213.689 | 18.014 | 1.081.083 |
| **total** | | | | | | | **4.085.829** |

\* até 23/09/2026.

4.085.829 registros ÷ 50 por página = **81.717 páginas**; a 0,27 página/s, **~84 horas**. É um backfill de três dias e meio rodando sozinho, retomável — não é obstáculo, é agenda. A carga diária depois disso é de ~6 mil registros, ~120 páginas, **~7 minutos**.

A curva também explica por que isso não foi feito antes: até 2023 o PNCP tinha pouco volume; o salto é de 2024 em diante.

## 5 · Página curta: a API às vezes devolve menos do que diz

A primeira varredura de agosto/2026 (23/09) veio com furo em quase toda modalidade:

| modalidade | anunciados | obtidos em 23/09 | obtidos em 27/09 |
|---|---:|---:|---:|
| Concorrência-Eletrônica | 5.508 | 5.318 (−3,4 %) | 5.507 de 5.507 |
| Pregão-Eletrônico | 33.861 | 31.251 (−7,7 %) | 33.841 de 33.841 |
| Pregão-Presencial | 847 | 787 (−7,1 %) | 847 de 847 |

Sem nenhum 429 e sem duplicata, a primeira leitura foi "paginação sobre dado vivo". **Estava errada.** Comparando registro a registro as duas varreduras, a ordem de entrega é a mesma nos dois dias, e o que faltou em 23/09 são blocos inteiros de páginas específicas: a API respondeu **HTTP 200 com 20 registros em vez de 50** — no Pregão-Presencial, as páginas 4 e 5; na Concorrência-Eletrônica, as páginas 2, 10, 11 e 86, mais um trecho em torno da 50 (a comparação página a página foi feita nessas duas modalidades; o parquet de 23/09 do Pregão-Eletrônico foi sobrescrito antes de ser conferido, mas o tamanho do furo é do mesmo tipo). Parece que o servidor, de vez em quando, ignora o `tamanhoPagina` e cai no tamanho padrão, mantendo o número da página.

É intermitente: em 23/09 aconteceu em todas as modalidades medidas; em 27/09, em nenhuma das 2.583 páginas. Como não há erro para capturar, o extrator agora confere cada página contra o tamanho esperado (50, ou o resto na última), repede a página curta até três vezes e, se ela continuar curta, fica com a maior versão e registra o caso nas métricas (`paginas_curtas_aceitas`) — pode ser uma remoção de verdade, e descartar a página perderia mais.

A diferença que sobra entre anunciado e obtido é, essa sim, dado vivo, e é pequena: entre 23 e 27/09 o total anunciado de agosto caiu 1 na Concorrência-Eletrônica (5.508 → 5.507), 20 no Pregão-Eletrônico (33.861 → 33.841) e 4 na Dispensa (61.009 → 61.005) — contratações removidas ou republicadas, não registro perdido.

**Agosto/2026, varredura completa com o extrator corrigido (27/09):** 128.994 registros anunciados, 128.994 obtidos, todos distintos, de 01/08 00:00 a 31/08 23:59; carregados no lake do sandbox pelo `stg_pncp__contratacoes`. Deles, **86.398 (67 %) são de órgãos municipais, de 4.793 municípios** — a cobertura que o dump do Compras.gov não tem. São José dos Campos sozinho tem 1.010 contratações no mês, 841 delas dispensas.

## 6 · O que a API não entrega

**Item não tem endpoint em lote.** É uma requisição por compra, com latência mediana medida de 2,68 s. Para 2026 inteiro seriam 1,08 milhão de requisições — no ritmo seguro, mais de 600 horas só de itens, e resultados multiplicam de novo, por item.

**E o item não vem classificado.** Amostra de 70 itens (10 dispensas + 10 pregões eletrônicos de 01/09/2026):

| campo | preenchido |
|---|---|
| `ncmNbsCodigo` | 0/16 em dispensas · 10/54 em pregões (19 %) |
| `catalogoCodigoItem` | 0/70 |
| `catalogo` | nulo em 70/70 |

Ou seja: **o tradutor CATMAT→NCM continua no caminho crítico** — o PNCP não resolve a classificação por nós, e a acurácia do tradutor (medida no painel de 15/09: só 20 % dos itens dentro de 0,7–1,4× da mediana NF-e) segue sendo o gargalo analítico.

## 7 · Desenho proposto

Duas velocidades, em vez de um extrator que tenta espelhar o PNCP inteiro:

1. **Cabeçalho nacional, completo**, pelo eixo `atualizacao` — cabe no orçamento e já dá cobertura municipal, que é o que falta hoje.
2. **Item e resultado só no recorte que importa** (um município, uma modalidade, um período), puxados a partir dos cabeçalhos já carregados. Para SJC inteiro é da ordem de milhares de requisições: uma noite.

O código desta nota está em `ingestion/pncp/extract.py` (extrator de cabeçalho, ritmo calibrado, manifesto e `alteracoes.csv` no mesmo contrato dos outros pipelines) e em `dbt/modelos/staging/pncp/stg_pncp__contratacoes.sql`.

## 8 · Como reproduzir

```bash
python -m ingestion.pncp.extract --ano 2026 --mes 8 --eixo publicacao
python -m ingestion.pncp.extract --ano 2026 --mes 8 --eixo publicacao --modalidades 12  # recorte pequeno
```

O extrator grava `dados/pncp/metricas-{eixo}-{periodo}.json` com requisições, 429, páginas curtas, tempo e divergências de cada varredura. Para uma varredura longa, bloquear a suspensão da máquina (a primeira de agosto morreu na página 500 da Dispensa quando o computador dormiu):

```bash
systemd-inhibit --what=sleep:idle --who=colibri --why="varredura PNCP" \
  python -m ingestion.pncp.extract --ano 2026 --mes 8 --eixo publicacao
```
