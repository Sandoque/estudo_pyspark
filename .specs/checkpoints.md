# Checkpoints

Cada checkpoint registra o que foi entregue, com números da execução real.

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
