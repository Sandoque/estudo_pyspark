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

## Próximas (fatia 1b, antes da fatia 2)

- [ ] T12 MERGE sem reescrever o que não mudou: comparar (a) deletion vectors ligados, (b) filtrar antes só as linhas novas/alteradas por anti-join no `_hash_linha` -- medir arquivos e linhas reescritos
- [ ] T13 Provar os testes por mutação (estragar a regra do documento, do fim8, da unicidade) e registrar quais pegaram
- [ ] T14 Amostra sintética com o mesmo formato (sem dado real) para rodar na Databricks Free Edition
