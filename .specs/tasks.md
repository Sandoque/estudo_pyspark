# Tasks — fatia 1

Estado: `[ ]` a fazer · `[x]` feito, com evidência.

- [x] T01 (FR-01) Contêiner com Spark 4.0.1, Delta 4.0.0, JupyterLab e pytest; jars do Delta baixados no build — `docker compose build` ok, "Delta ok" no build (23/10)
- [x] T02 (FR-01) `pytest` verde dentro do contêiner — 27 passed (23/10)
- [x] T03 (FR-02) `scripts/exportar_dw.py`: CSV + manifesto + controles do SQL, sessão READ ONLY com snapshot consistente — 31.625 clientes (23/10 15:38)
- [x] T04 (FR-03) Bronze idempotente pelo sha256 do lote — 2ª execução "já ingerido: True", histórico com uma só escrita
- [x] T05 (FR-04) Silver: documento, telefones, cancelado, um registro por código — 23.295 CPF, 8.317 CNPJ, 13 desconhecidos
- [x] T06 (FR-05) Quarentena com motivo e a conta `bronze = silver + quarentena + duplicatas` — 31.625 = 31.625 + 0 + 0
- [x] T07 (FR-06) Seed de/para com vigência — teste da troca 131/134 em 21/10
- [x] T08 (FR-07) Gold de reconhecimento por base e dia — `replaceWhere` por dia
- [x] T09 (FR-08) Reconciliação com os controles do SQL — 12 de 12 iguais
- [x] T10 (FR-09, FR-10) Tempos e relatório JSON — `reports/fatia1_*.json`
- [x] T11 Checkpoint com o critério do teste de uso (spec §4) — ver checkpoints.md, 23/10

## Fatia 1b (entregue em 23/10)

- [x] T12 MERGE sem reescrever o que não mudou: comparar (a) deletion vectors ligados, (b) filtrar antes só as linhas novas/alteradas por anti-join no `_hash_linha` -- `delta.enableDeletionVectors = true` zerou a reescrita sem mudança (version não avança, 0 arquivos) e em mudança pontual (1 alt / 1 ins / 1 del) gravou só 2 linhas (`numTargetRowsCopied = 0`) + 1 deletion vector em 2,43 s, contra 8 arquivos / 31.625 linhas do MERGE antigo e 1 arquivo / 3.953 linhas do pré-filtro sem DV (23/10)
- [x] T13 Provar os testes por mutação (estragar a regra do documento, do fim8, da unicidade) e registrar quais pegaram — `scripts/testar_mutacoes.py`: 8/8 mutações mortas (23/10)
- [x] T14 Amostra sintética com o mesmo formato (sem dado real) para rodar na Databricks Free Edition — `scripts/gerar_amostra_sintetica.py` + `seeds/amostra_sintetica/clientes_denver_sintetico.{csv,json}` (185 linhas: 180 válidas, 4 quarentena, 1 duplicata exata, 12/12 reconciliados em `tests/test_fim_a_fim_sintetico.py`) (23/10)

## Ambiente uv (plan.md, "Ambiente Python (uv)")

- [x] T15 Python 3.12 no projeto uv (`uv python pin 3.12`, `requires-python = ">=3.12,<3.13"`) e as dependências do plan com `uv add` (principal: pyspark, delta-spark; dev: pytest, jupyterlab; opcional: pandas, pyarrow) — evidência: `uv.lock` gerado e `uv run python -c "import pyspark, delta"` ok (23/10)
- [x] T16 Dockerfile instalando pelo `uv.lock` (`uv sync --frozen`) no lugar de `docker/requirements.txt`, que sai — evidência: `docker compose build` + `pytest` (28 passed) + `scripts/testar_mutacoes.py` (8/8 mortas) verdes no contêiner novo (23/10)

## Fatia 2 — Livro de Saída da Denver (1,59 milhão de linhas e janela `replaceWhere`)

- [x] T17 (FR-11) `scripts/exportar_livro_saida.py`: exportação `READ ONLY` de `livros_fiscais.raw_livro_saida` (Denver completo: 1.592.890 linhas / 30 competências em 42,3 s; janela 3 meses: 118.308 linhas / 3 competências em 2,4 s) com manifesto SHA-256 e controles mensais do SQL (23/10)
- [x] T18 (FR-12, FR-13) `src/estudo/livro_saida.py`: Bronze idempotente + Silver tipada em `DECIMAL(15,2)` (`try_cast` compatível com ANSI do Spark 4.0), chave da nota `(revenda, nf, livro, chave_nf)`, quarentena com motivo e gravação particionada por `(ano, mes)` com `repartition('ano', 'mes')` + `replaceWhere` (23/10)
- [x] T19 (FR-14, FR-15) Gold mensal (`gold_livro_saida_mensal`), cálculo da deriva de `float32` e reconciliação ao centavo (`R$ 0,00` de diferença em todas as 30 competências × 7 indicadores = 210 valores) com os controles do SQL (23/10)
- [x] T20 (FR-17) Amostra sintética `seeds/amostra_sintetica/livro_saida_denver_sintetico.{csv,json}` (95 linhas: 90 válidas, 4 quarentena, 1 duplicata exata), testes em `tests/test_livro_saida.py` (32 passed no total) e novas mutações fiscais em `scripts/testar_mutacoes.py` (10/10 mortas) (23/10)
- [x] T21 (FR-16) `jobs/fatia2_livro_saida.py`: execução sobre os 1.592.890 registros reais da Denver (55,1 s) + benchmark de reprocessamento da janela de 3 meses (`replaceWhere` em 19,1 s gravando só 3 arquivos / 118.308 linhas e mantendo os outros 27 meses intactos) e checkpoint em `checkpoints.md` (23/10)
