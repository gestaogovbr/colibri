# Como contribuir com o colibri

Obrigado pelo interesse! Este guia resume o fluxo de trabalho do repositório.
O passo a passo técnico (instalação, camadas do dbt, pipelines de ingestão)
está no [site do projeto](https://gestaogovbr.github.io/colibri/).

## 1. Comece por uma issue

- Antes de escrever código, procure uma [issue](https://github.com/gestaogovbr/colibri/issues)
  sobre o assunto ou abra uma usando um dos modelos: **bug**, **proposta de fonte
  de dados** ou **tarefa**.
- Para pegar uma issue, comente nela. Issues com `good first issue` são um bom
  ponto de partida, e as com `precisa-decisão` esperam uma definição antes de
  alguém começar.
- Os labels seguem três eixos: **tipo** (`bug`, `melhoria`, `docs`,
  `fonte-de-dados`, `infra`), **área** (`comprasgov`, `nfe`, `ncm`, `margem`,
  `tradutor`, `uasg`, `pncp`, `dbt`, `lake`, `cli`) e **estado**
  (`precisa-decisão`, `bloqueada`).

## 2. Branches: `develop` e `main`

| Branch | Papel |
|---|---|
| `develop` | homologação: todo PR entra aqui |
| `main` | produção: recebe a `develop` em lotes, depois de testada |

- Crie sua branch a partir da `develop` e abra o PR **contra a `develop`**.
- Nome da branch: `<tipo>/<descricao-curta>`, por exemplo
  `fix/nfe-chave-texto`, `feat/pipeline-uasg`, `docs/guia-analista` ou
  `chore/templates-issue-pr`.
- A passagem `develop` → `main` é feita pelos mantenedores, em lote, e cada lote
  vira uma versão (`v0.1.0`, `v0.2.0`…).
- `Resolve #N` no PR fecha a issue quando a mudança chegar à `main`, ou seja, no
  lote seguinte, e não no merge na `develop`.

## 3. Commits

O repositório usa mensagens **no imperativo e em português**:

```text
Ler chave de acesso e NCM da NF-e como texto

A chave de acesso tem 44 dígitos e era inferida como DOUBLE, perdendo os
29 últimos. Agora CHAVE DE ACESSO e CÓDIGO NCM/SH são gravadas como
VARCHAR no Parquet; as demais colunas seguem com a inferência atual.
```

- Título curto (até ~70 caracteres), sem ponto final, dizendo o que o commit faz.
- Corpo, quando ajudar, explicando **por que** a mudança foi feita.
- Um assunto por commit: fica mais fácil revisar e, se preciso, reverter.

## 4. Ambiente de desenvolvimento

Siga o [guia de instalação](https://gestaogovbr.github.io/colibri/04-guia-instalacao.html)
e depois instale as dependências de desenvolvimento e os hooks:

```bash
pip install -e ".[dev]"
pre-commit install
```

**Não rode pipelines nem `dbt run` contra o `colibri-prod` enquanto desenvolve.**
Use um ambiente isolado:

- **bucket próprio** no R2 ou em outro S3: veja
  [Instalar o ambiente em um S3 próprio](https://gestaogovbr.github.io/colibri/04-guia-instalacao.html#instalar-o-ambiente-em-um-s3-próprio);
- **MinIO local**, sem depender de nenhuma credencial do projeto: veja o capítulo
  *Ambiente de desenvolvimento* do site.

Nunca faça commit de credenciais. `.segredos.yml` e `dbt/profiles.yml` já estão
no `.gitignore`; confira o `git status` antes de cada commit.

## 5. Antes de abrir o PR

Na raiz do repositório:

```bash
pre-commit run --all-files   # ruff e yamlfix, os mesmos da CI
pytest
cd dbt && ../env/bin/dbt test --project-dir . --profiles-dir . --select <modelos>+ && cd ..
```

- Use o `dbt` do ambiente virtual (`env/bin/dbt`), e não um `dbt` do sistema:
  o projeto usa o **dbt-core**, e o dbt Fusion não é compatível com o catálogo
  do lake.
- Se você mudou as colunas de um modelo **incremental**, avise no PR que o deploy
  vai precisar de `--full-refresh`. Sem isso, o próximo `dbt run` em produção falha.
- Preencha o template do PR: o que muda, como testar e pontos de atenção.

## 6. Revisão

- A CI (pre-commit e pytest) precisa passar.
- Os comentários de revisão são conversa, não ordem: responda no próprio
  comentário, ajuste com commits novos na mesma branch e marque a conversa como
  resolvida quando terminar.
- Quem revisa pode sugerir mudanças com *suggestion* do GitHub; aceitar a
  sugestão gera o commit direto na branch.

## Licença

Ao contribuir, você concorda que sua contribuição será distribuída sob a
licença do projeto, GPL-3.0 (definida na #99).
