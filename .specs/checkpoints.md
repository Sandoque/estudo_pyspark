# Checkpoints

Cada checkpoint registra o que foi entregue, com números da execução real.

## 23/10/2026 — fatia 2 entregue (Livro de Saída da Denver: 1,59 milhão de linhas e janela `replaceWhere`)

**Execução** sobre `livro_saida_denver_20261023_165440.csv` (**1.592.890 itens**, 30
competências de `05/2024` a `10/2026`, **R$ 690.836.525,78** em `VLR_TOTAL_NOTA_CALC`)
e janela de lookback de 3 meses `livro_saida_denver_janela_20261023_165540.csv`
(**118.308 itens**, competências `2026-08`, `2026-09` e `2026-10`).

### 1. Reconciliação ao centavo e prova da perda em `float32` (FR-14, FR-15)

- **30 de 30 competências (210 indicadores)** bateram com **R$ 0,00 de diferença**
  entre a Gold do Spark (`DECIMAL(15,2)`) e o SQL de produção (`CAST(ROUND(..., 2) AS DECIMAL(15,2))`).
- Em **todas** as 30 competências (todas acima de R$ 12 milhões/mês), a acumulação em
  `float32` perdeu todos os centavos (arredondando para múltiplos de R$ 1,00 ou R$ 2,00,
  com erro de até R$ 1,00 no mês só no `VLR_TOTAL_NOTA_CALC`, ex.: em `2026-09`
  `20.632.843,00` vs `20.632.844,00` no `float32`, e em `2026-08` `18.792.584,98` vs
  `18.792.584,00`).

### 2. Carga Completa vs. Reprocessamento de Janela de 3 Meses com `replaceWhere` (FR-16)

| Métrica | Carga Completa (30 meses: `05/2024`–`10/2026`) | Janela de 3 meses (`08/2026`–`10/2026` via `replaceWhere`) | Redução na Janela |
| --- | ---: | ---: | ---: |
| **Linhas processadas (Bronze = Silver)** | 1.592.890 | 118.308 | **-92,6%** |
| **Arquivos Parquet gravados na Silver** | 30 arquivos (1 por mês) | **3 arquivos** (`numRemovedFiles = 3`) | **-90,0%** (27 meses intocados) |
| **Tempo Spark — Bronze** | 11,70 s | 5,35 s | -54,3% |
| **Tempo Spark — Silver (`DECIMAL` + `replaceWhere`)** | 37,75 s | **10,46 s** | **-72,3%** |
| **Tempo Spark — Gold + Reconciliação** | 5,64 s | 3,32 s | -41,1% |
| **Tempo Spark Total (sem sessão)** | **55,09 s** | **19,13 s** | **-65,3%** |
| **Tempo MySQL (exportação + agregação SQL)** | 57,97 s (42,27 s + 15,70 s) | 3,74 s (2,42 s + 1,33 s) | -93,5% |
| **Integridade dos 30 meses após `replaceWhere` parcial** | 30/30 ok | **30/30 ok** (`tabela_completa_integra_apos_janela = true`) | — |

### 3. Achado técnico de memória JVM em escritas particionadas (`OutOfMemoryError`)

Na primeira tentativa sobre 1,59 milhão de linhas, segurar `silver.cache()` na heap
padrão de 1 GB enquanto 8 tarefas escreviam simultaneamente em 30 partições
`(ano, mes)` abria até `8 × 30 = 240` buffers Parquet concorrentes na RAM e estourava
`java.lang.OutOfMemoryError: Java heap space`. Três ajustes eliminaram o problema:
1. **`repartition("ano", "mes")` antes do `partitionBy("ano", "mes")`:** cada tarefa
   escreve uma competência por vez (1 buffer Parquet ativo por tarefa e **1 arquivo por
   competência**, gerando exatos 30 arquivos em vez de 240);
2. **Ler a Silver do Delta recém-gravado em vez de `.cache()` em RAM:** o Parquet no
   disco já faz o papel de materialização sem ocupar a heap;
3. **`spark.driver.memory = "4g"`** em `src/estudo/sessao.py`.

### 4. Critério do teste de uso em escala de milhões de linhas

1. **Bateu com a produção?** Sim: 1.592.890 linhas, 483.703 NFs e R$ 690.836.525,78
   distribuídos em 30 meses bateram ao centavo (`R$ 0,00` de diferença).
2. **Tempo e onde o jogo virou:** em 31 mil linhas (Fatia 1), o Spark era 5× mais lento
   que o SQL (~30 s vs ~6 s). Em **1,59 milhão de linhas** (Fatia 2), o pipeline
   completo Bronze → Silver → Gold → Reconciliação no Spark levou **55,1 s**, empatando
   com o tempo de leitura + agregação do MySQL (**58,0 s**) — e a agregação Gold sobre a
   Silver em Parquet/Delta levou apenas **5,54 s** para varrer 1,59 milhão de linhas
   contra **15,70 s** do `GROUP BY` no MySQL (**~2,8× mais rápido no Delta/Parquet**).
3. **O ganho da janela (`replaceWhere`):** reprocessar os 3 meses de lookback levou
   **19,1 s** e reescreveu apenas **3 arquivos Parquet (118 mil linhas)**, preservando
   os outros 27 arquivos intactos — sem precisar do `livros_geral_batch.py` abrindo
   subprocessos por revenda nem regravar milhões de linhas toda noite no MySQL.

---

## Síntese do Benchmark, Vantagens, Custos e Recomendações para a Meggal

### 1. Quadro comparativo: onde o Spark perde e onde vira o jogo

| Cenário Medido | Volume (Denver) | MySQL (Produção) | PySpark + Delta (Local) | Veredito |
| --- | ---: | ---: | ---: | --- |
| **Fatia 1 — Cadastro de Clientes** (`dw_previsao_financeira.clientes`) | `31.625` linhas (~4,5 MB) | **`5,7 s`** (export `0,4 s` + 3 queries `5,3 s`) | **`30,7 s`** (+ `5,6 s` de sessão JVM) | **MySQL 5,4× mais rápido.** O custo fixo da JVM e do log transacional do Delta domina em dezenas de milhares de linhas. |
| **Fatia 2 — Livro de Saída Completo** (`livros_fiscais.raw_livro_saida`, 30 meses) | `1.592.890` linhas (`483.703` NFs, R$ 690,84M) | **`58,0 s`** (leitura `42,3 s` + `GROUP BY` `15,7 s`) | **`55,1 s`** (Bronze `11,7 s` + Silver `37,8 s` + Gold **`5,5 s`**) | **Empate no pipeline completo (`55 s` vs `58 s`) e Gold 2,8× mais rápida no Delta (`5,5 s` vs `15,7 s`).** |
| **Fatia 2 — Reprocessamento da Janela de 3 meses (`replaceWhere`)** | `118.308` linhas (`08/2026` a `10/2026`) | No consolidado atual (`Livros_Fiscais_geral`), **regrava a tabela inteira** (~18 min nas 11 revendas) | **`19,1 s`** (trocando só **3 arquivos Parquet** e mantendo 27 meses intactos) | **Redução de 65% no tempo e 90% na escrita de disco** em relação à carga completa da revenda. |

### 2. As 4 vantagens reais do Delta Lake sobre o modelo atual (MySQL + Pandas + dbt)

1. **Substituição atômica por partição (`replaceWhere` vs. Full Refresh):**
   - Enquanto o consolidado fiscal atual reconstrói e regrava os ~5 milhões de linhas desde maio/2024 toda noite (gerando gigabytes de *binlog* e arquivos temporários no disco `C:` do servidor), a Silver e a Gold particionadas por `(ano, mes)` com `replaceWhere` trocam apenas os **3 arquivos `.parquet`** da janela de lookback (`numFiles = 3`, `numRemovedFiles = 3`), mantendo os 27 meses anteriores intocados e sem risco de deixar a tabela vazia se o processo cair no meio.
2. **Atualização incremental sem reescrever arquivos (`Deletion Vectors` — T12):**
   - Em tabelas de retrato cadastral (`silver_clientes`), ativar `delta.enableDeletionVectors = true` fez uma reexecução sem mudanças custar **zero bytes gravados** (*no-op*), e uma alteração em 2 clientes gravar **apenas 2 linhas** (`numTargetRowsCopied = 0`) em vez de reescrever 31.625 linhas.
3. **Gestão de memória sem subprocessos manuais:**
   - No pipeline em Pandas (`Livros_Fiscais_geral.py`), o volume das 11 revendas exigiu criar o `livros_geral_batch.py` abrindo um subprocesso Python por revenda com `gc.collect()`. No Spark, combinando `.repartition("ano", "mes")` antes do `.partitionBy("ano", "mes")` e leitura/escrita em Parquet, cada tarefa mantém apenas 1 buffer Parquet ativo por vez e faz *spill* automático em disco se necessário.
4. **Governança e rastreabilidade nativas:**
   - Idempotência pelo SHA-256 do lote (`_lote`), fechamento contábil explícito entre camadas (`bronze = silver + quarentena + duplicatas`), histórico de métricas por commit (`DESCRIBE HISTORY`) e viagem no tempo (`versionAsOf`) sem precisar manter tabelas auxiliares de snapshot.

### 3. Onde o PySpark + Delta é pior (Custos e Armadilhas)

- **Peso operacional e latência de partida:** exige JVM (Java 17+), contêiner Linux (ou `winutils` no Windows) e ~6 segundos só para iniciar a sessão. Para tabelas pequenas (< 200 mil linhas), é mais lento e mais complexo de operar que SQL puro.
- **O consumidor final (`webservices_flask`) lê MySQL:** as telas do Flask (DRE, DOP, Opt-in WhatsApp) fazem consultas transacionais rápidas no MySQL. O Delta Lake não substitui o banco de dados de aplicação; qualquer resultado processado no Delta precisaria publicar a camada Gold (resumos) no MySQL.
- **Exige atenção às métricas físicas (`_delta_log`):** sem olhar o histórico do Delta, um `MERGE` sem *deletion vectors* reescreve a tabela inteira (T12) e um `partitionBy` com `.cache()` sem `repartition` abre centenas de escritores Parquet simultâneos na heap da JVM (T21).

### 4. Recomendações práticas para o ambiente de produção da Meggal

#### A. O que manter exatamente como está (MySQL + Python + dbt)
- **Cadastros e tabelas pequenas/médias (`clientes`, `produtos`, `fornecedor`, `despesas_operacionais`, `optin_service`):** o SQL responde entre `0,5 s` e `6 s`. Migrar para Spark multiplicaria o tempo por 5× sem nenhum benefício.
- **Modelos analíticos finais da DRE no dbt (`dre_mensal`, `dre_por_marca`, `dre_linhas`):** operam sobre tabelas já agregadas e entregam diretamente no MySQL que o Flask consome.

#### B. O que aplicar imediatamente no pipeline atual (sem precisar de Spark em produção)
1. **Reprocessamento por janela (`ano, mes`) no `Livros_Fiscais_geral.py` e `notas_fiscais_resumo`:**
   - Como provado na Fatia 2, processar apenas a janela de lookback de **3 meses** corta o volume em **92,6%** (`118.308` linhas contra `1.592.890` na Denver).
   - Aplicar a troca transacional por janela de competência (como no `replaceWhere`, apagando e reinserindo apenas os 3 meses da janela em vez de reconstruir desde maio/2024 toda noite) deve reduzir a etapa de 18 minutos do `Livros_Fiscais_geral` para **~2 minutos** e eliminar o pico diário de *binlog* e espaço temporário no disco `C:` do servidor.
2. **Aritmética fiscal estrita em `DECIMAL(15,2)`:**
   - A Fatia 2 provou que em **30 de 30 meses** da Denver (todos acima de R$ 12 milhões) o `float32` perde os centavos. Manter `DECIMAL(15,2)` (`CAST(ROUND(..., 2) AS DECIMAL(15,2))`) em todas as somas fiscais elimina de vez divergências de arredondamento entre livro fiscal, relatório contábil e DRE.

#### C. Onde PySpark + Delta faz sentido se o volume continuar crescendo
- Se as tabelas históricas de **Livros Fiscais (`raw_livro_saida` + `raw_livro_entrada` + `livros_fiscais_geral`, ~5M linhas)** e **Livro Contábil (`raw_livro_contabil`, ~12,3M linhas)** continuarem crescendo e pressionando memória/disco do MySQL na janela noturna:
  - Usar **PySpark + Delta em contêiner** apenas como **motor de processamento pesado (Bronze → Silver → Gold)** particionado por `(revenda, ano, mes)` com `replaceWhere` na janela, publicando no MySQL **somente a tabela Gold agregada** dos meses alterados para consumo do dbt e do Flask. Assim o MySQL deixa de fazer *hash join* e *filesort* sobre milhões de linhas com colunas `TEXT` e volta a atuar apenas como banco relacional rápido de leitura.

## 23/10/2026 — decisão: projeto gerido pelo uv (T15 e T16 entregues)

O dono iniciou o projeto com `uv init`. Documentado em `plan.md` ("Ambiente
Python (uv)"): Python 3.12 (com `requires-python = ">=3.12,<3.13"`, pois `<3.13`
sem limite inferior fazia o resolvedor universal do `uv` tentar resolver para
Python `<3.9`), as dependências travadas em `uv.lock`, o que fica de fora
(`sqlalchemy`/driver do MySQL, que são do `webservices_flask`) e o contêiner
como ambiente de execução instalando via `uv sync --frozen` em `/opt/venv` (sem
`docker/requirements.txt`).

## 23/10/2026 — fatia 1b entregue (MERGE com Deletion Vectors, mutação e amostra sintética)

### T12 — Comparativo de estratégias de MERGE na Silver (31.625 linhas, 8 arquivos Parquet)

Medido no contêiner (Spark 4.0.1 + Delta 4.0.0) em dois cenários sobre a `silver_clientes`:

| Estratégia | Cenário 1: Mesmo retrato (0 mudanças) | Cenário 2: 1 alterado + 1 inserido + 1 removido |
| --- | --- | --- |
| **0. MERGE padrão (sem DV, sem pré-filtro)** | **8 arquivos** reescritos, **31.625 linhas** copiadas (4,07 s) | **8 arquivos** reescritos, **31.623 linhas** copiadas + 2 gravadas (2,21 s) |
| **(a) MERGE com `delta.enableDeletionVectors = true`** | **0 arquivos**, **0 linhas** copiadas — vira *no-op* sem avançar versão (1,83 s) | **0 linhas copiadas**, **2 linhas** gravadas em 2 arquivos pequenos + **1 DV** (2,43 s) |
| **(b) Pré-filtro por `_hash_linha` (sem DV)** | **0 arquivos**, *no-op* via `isEmpty()` (1,14 s) | **1 arquivo** reescrito (**3.951 linhas** copiadas + 2 gravadas) (2,39 s) |
| **(a + b) Pré-filtro + Deletion Vectors** | **0 arquivos**, *no-op* (1,58 s) | **0 linhas copiadas**, **2 linhas** gravadas + **1 DV**, porém mais lento por ler o alvo 2× (5,02 s) |

**Decisão aplicada em `src/estudo/silver_clientes.py`:** ativar `delta.enableDeletionVectors = true` na tabela `silver_clientes` (estratégia **a**). Resolve tanto a reexecução idempotente quanto a atualização incremental diária sem reescrever arquivos Parquet inteiros e sem código extra de anti-join.

### T13 — Testes de mutação (`scripts/testar_mutacoes.py`)

Oito mutações cirúrgicas aplicadas em memória sobre as regras da Silver, De/Para e Gold — **8/8 mutações mortas**:

| Mutação | Regra sabotada | Testes que pegaram |
| --- | --- | --- |
| `documento_sem_filial_0000` | Ignora filial `0000` como CPF de 15 dígitos do Control | `test_documento_no_formato_do_control` |
| `documento_sem_lpad_15` | Não recompõe zero à esquerda engolido pelo Control (14/13 dígitos) | `test_documento_no_formato_do_control` |
| `dv_sempre_true` | Aceita qualquer dígito verificador como válido | `test_digitos_verificadores` |
| `fim8_primeiros_8` | Extrai os 8 primeiros dígitos em vez dos 8 últimos | `test_fim8` |
| `fim8_aceita_curto` | Aceita telefone com menos de 8 dígitos | `test_fim8` |
| `de_para_ate_inclusivo` | Usa `<=` em `valido_ate` (mantendo a carteira 131 ativa em 21/10) | `test_a_131_sai_e_a_134_entra_em_21_10` |
| `unicidade_ignora_cancelado` | Filtra cancelados antes de checar unicidade do número na Gold | `test_reconhecimento_pela_regra_da_producao` |
| `unicidade_aceita_repetido` | Reconhece número com `donos >= 1` em vez de `donos == 1` | `test_reconhecimento_pela_regra_da_producao` |

### T14 — Amostra sintética (`scripts/gerar_amostra_sintetica.py`)

Gerada em `seeds/amostra_sintetica/clientes_denver_sintetico.{csv,json}` com `seed=42` (185 linhas: 180 clientes válidos nas 3 bases, 1 duplicata exata e 4 registros de quarentena cobrindo `sem_codigo`, `revenda_diferente` e `codigo_duplicado_conflitante`), validada ponta a ponta em `tests/test_fim_a_fim_sintetico.py` com reconciliação 12/12.

## 23/10/2026 — fatia 1 entregue (cadastro de clientes da Denver)

**Execução** sobre `clientes_denver_20261023_153850.csv` (31.625 clientes, retrato
do DW desta máquina), dia de referência 23/10/2026.

| Camada | Linhas |
| --- | --- |
| Bronze | 31.625 |
| Silver | 31.625 |
| Quarentena | 0 |
| Duplicatas exatas | 0 |

Silver: 23.295 CPF e 8.317 CNPJ depois da regra do formato do Control; 13
documentos fora de qualquer formato; 54 com dígito verificador errado; 2.677
clientes sem telefone nem celular; 1 documento em mais de um cliente; código
único. O cadastro do DW não traz clientes cancelados (0).

**Reconciliação (FR-08): 12 de 12 indicadores iguais ao SQL de produção.**

| Base | Ativos | Só telefone | Telefone ou celular | Número repetido |
| --- | --- | --- | --- | --- |
| Cobertura (132–134) | 1.132 | 431 | 1.012 | 119 |
| SKU | 3.259 | 1.298 | 2.800 | 470 |
| Volume | 1.441 | 423 | 999 | 487 |

**Idempotência:** a 2ª execução reconheceu o lote ("já ingerido"), o MERGE da
silver não inseriu, atualizou nem apagou nada, e os números se repetiram.

**Achado de Delta:** mesmo sem mudança, o MERGE regravou as 31.625 linhas
(`numOutputRows`). A cláusula `whenNotMatchedBySourceDelete` obriga o MERGE a
olhar todos os arquivos, e sem deletion vectors todo arquivo tocado é reescrito.
Em tabela grande isso é custo de verdade. Próximo passo em T12.

### Critério do teste de uso (spec §4)

1. **Bateu com a produção?** Sim, os 12 indicadores, no mesmo retrato.
2. **Tempo.** Spark ~30 s por execução (bronze 9–13 s, silver 11–15 s, gold
   5–6 s, mais ~6 s para subir a sessão) contra **~6 s** do SQL de produção
   (exportação 0,4 s + 1,6–1,8 s por base). Nesse tamanho, Spark é ~5 vezes
   mais lento: o custo fixo (JVM, planejamento, escrita Delta, um processo só
   numa máquina) domina 31 mil linhas.
3. **Mais simples × mais complexo.** Mais simples: regra testável coluna a
   coluna (documento, fim8, unicidade) em vez de SQL longo; quarentena e
   reconciliação explícitas; histórico (time travel) de graça. Mais complexo:
   contêiner, JVM, e o cuidado com MERGE/regravação que o MySQL não exige.
4. **Recomendação.** Para o cadastro (dezenas de milhares de linhas), **não**
   vale trocar o pipeline atual: o SQL já é rápido e certo. O lugar onde Spark +
   Delta podem valer é onde o pipeline atual sofre por memória e por
   reprocessamento: o Livro de Saída e o livro contábil (milhões de linhas, o
   full refresh por revenda que precisou de subprocessos para caber na memória).
   Isso é a fatia 3 (Q-01) — e o teste de uso só vale se lá também bater e for
   medido.

## 23/10/2026 — criação

Constitution, spec, plan e tasks da fatia 1 (cadastro de clientes da Denver).
