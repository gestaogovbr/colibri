<!-- PRs entram na `develop`. A passagem `develop` → `main` é feita em lote. -->
<!-- Indique a issue atendida. Ela é fechada automaticamente quando a mudança chegar à `main`. -->
Resolve #<Número da Issue>

<!-- Busque preencher todas as seções abaixo. Se alguma não fizer sentido, informe que não se aplica (N/A) -->
## O que muda com esse PR

## Como testar

## Pontos de atenção
<!-- Ex.: precisa de `--full-refresh` no deploy, muda colunas de um mart, depende de outro PR -->

<!-- Apague os itens que não fizerem sentido -->
## Checklist
- [ ] PR aberto contra a `develop`
- [ ] Lint e formatação com `pre-commit run --all-files` (ruff e yamlfix, os mesmos da CI)
- [ ] Testes unitários com `pytest`
- [ ] Testes dbt dos modelos alterados com `cd dbt && ../env/bin/dbt test --project-dir . --profiles-dir . --select <modelos>+ && cd ..`
- [ ] Modelo incremental com mudança de colunas: `--full-refresh` indicado em "Pontos de atenção"
