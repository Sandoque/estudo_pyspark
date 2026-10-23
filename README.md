# Estudo PySpark + Delta — cadastro da revenda XPTO

Estudo de PySpark e Delta Lake sobre dado real da operação, e ao mesmo tempo um
**teste de uso**: o resultado tem de bater com o SQL de produção, e o tempo é
medido. Regras em [.specs/constitution.md](.specs/constitution.md); o que a
fatia 1 faz e o critério de aceite em [.specs/spec.md](.specs/spec.md).

```text
DW (MySQL, só SELECT) ──► data/landing (CSV + manifesto + controles do SQL)
   └─► bronze (Delta, texto) ──► silver (normalizada) + quarentena ──► gold (reconhecimento por base/dia)
                                                                        └─► reconciliação com o SQL
```

## Rodar

Uma vez, para construir o ambiente (Spark 4.0, Delta 4.0, JupyterLab, pytest):

```powershell
cd C:\webservice\estudo_pyspark; docker compose build
```

1. **Exportar** o cadastro do DW (no Windows, com o Python do webservices_flask; só leitura):

```powershell
& C:\webservice\dbA\webservices_flask\.venv\Scripts\python.exe -B scripts\exportar_dw.py
```

2. **Rodar a fatia** (bronze → silver → gold → reconciliação; relatório em `reports/`):

```powershell
# Sobre a exportação real mais recente em data/landing/:
docker compose run --rm spark python jobs/fatia1_clientes.py

# Ou sobre a amostra 100% sintética versionada (sem precisar do DW):
docker compose run --rm spark python jobs/fatia1_clientes.py --arquivo seeds/amostra_sintetica/clientes_denver_sintetico.csv
```

3. **Fatia 2 — Livro de Saída da Denver (1,59 milhão de linhas + janela `replaceWhere`)**:

```powershell
# Exportar completo (30 meses) e janela de 3 meses do DW (só leitura):
& C:\webservice\dbA\webservices_flask\.venv\Scripts\python.exe -B scripts\exportar_livro_saida.py
& C:\webservice\dbA\webservices_flask\.venv\Scripts\python.exe -B scripts\exportar_livro_saida.py --data-inicial 2026-08-01 --prefixo livro_saida_denver_janela

# Rodar a fatia 2 (ou com --arquivo seeds/amostra_sintetica/livro_saida_denver_sintetico.csv --sem-janela):
docker compose run --rm spark python jobs/fatia2_livro_saida.py
```

4. **Testes e prova de mutação**:

```powershell
docker compose run --rm spark pytest -q
docker compose run --rm spark python scripts/testar_mutacoes.py
```

4. **Explorar** no JupyterLab (http://127.0.0.1:8888, só nesta máquina) e na Spark UI (http://127.0.0.1:4040 enquanto houver sessão):

```powershell
docker compose up
```

## Pastas

| Pasta / Arquivo | O que tem |
| --- | --- |
| `.specs/` | `constitution.md`, `spec.md`, `plan.md`, `tasks.md` e `checkpoints.md` (com síntese completa do benchmark e recomendações) |
| `scripts/exportar_dw.py` | Exportação `READ ONLY` do cadastro de clientes + controles do SQL |
| `scripts/exportar_livro_saida.py` | Exportação `READ ONLY` do Livro de Saída (completo ou janela) + controles mensais do SQL |
| `scripts/gerar_amostra_sintetica.py` | Gera amostras 100% sintéticas de clientes e do Livro de Saída (`seeds/amostra_sintetica/`) |
| `scripts/testar_mutacoes.py` | Prova da suíte de testes por mutação (10/10 mutações mortas) |
| `scripts/historico_delta.py` | Exibe o histórico (`DESCRIBE HISTORY`) e métricas de reescrita das tabelas Delta |
| `seeds/de_para_vendedor_base.csv` | Vendedor → base, com vigência (131 até 20/10, 134 desde 21/10) |
| `seeds/amostra_sintetica/` | Dados sintéticos versionados para rodar sem acesso ao DW ou na Databricks Free Edition |
| `src/estudo/` | Módulos `bronze`, `silver_clientes`, `de_para`, `gold_reconhecimento`, `qualidade`, `livro_saida` e `sessao` |
| `jobs/fatia1_clientes.py` | Fatia 1 ponta a ponta (31.625 clientes), medida e reconciliada |
| `jobs/fatia2_livro_saida.py` | Fatia 2 ponta a ponta (1.592.890 itens do Livro de Saída + janela `replaceWhere` de 3 meses) |
| `tests/` | Suíte `pytest` (32 testes unitários e fim-a-fim com Spark local) |
| `notebooks/` | `01_clientes.ipynb` e `02_livro_saida.ipynb` para exploração interativa no JupyterLab |
| `data/`, `reports/` | **Dado real da operação: fora do git (`.gitignore`)** |

## Resumo do Benchmark e Recomendação (ver [.specs/checkpoints.md](.specs/checkpoints.md))

| Cenário Medido (Denver) | Volume | MySQL (Produção) | PySpark + Delta (Local) | Conclusão |
| --- | ---: | ---: | ---: | --- |
| **Fatia 1 — Cadastro de Clientes** | `31.625` linhas | **`5,7 s`** | `30,7 s` | **Manter no MySQL.** Em tabelas pequenas (< 200 mil linhas), o custo fixo da JVM não se paga. |
| **Fatia 2 — Livro de Saída (30 meses)** | `1.592.890` linhas | `58,0 s` (`GROUP BY`: `15,7 s`) | **`55,1 s`** (Gold: **`5,5 s`**) | **Empate no total e agregação Gold 2,8× mais rápida no Parquet/Delta** (`5,5 s` vs `15,7 s`), com reconciliação exata ao centavo em `DECIMAL(15,2)` nas 30 competências (`R$ 690,84M`). |
| **Fatia 2 — Janela 3 meses (`replaceWhere`)** | `118.308` linhas | Regrava a tabela inteira no consolidado atual | **`19,1 s`** (só **3 arquivos Parquet** tocados) | **Redução de 65% no tempo e 90% na escrita de disco**, mantendo os outros 27 meses intactos. |

- **Para o pipeline atual (curto prazo):** manter cadastros e modelos finais da DRE em MySQL/dbt; aplicar já no `Livros_Fiscais_geral` a substituição transacional restrita à **janela de 3 meses** (reduz o volume processado em 92,6% sem precisar regravar 5 milhões de linhas toda noite no disco `C:`) e manter `DECIMAL(15,2)` em todas as somas fiscais (o `float32` perde centavos nas 30 competências medidas).
- **Onde PySpark + Delta entra (médio prazo):** como motor de processamento pesado em lote para tabelas multimilionárias (`raw_livro_saida`, `livros_fiscais_geral` e `raw_livro_contabil` com 12,3M linhas) usando `replaceWhere` por `(revenda, ano, mes)` e publicando apenas a camada Gold resumida no MySQL para leitura do Flask.
