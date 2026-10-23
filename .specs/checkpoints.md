# Checkpoints

Cada checkpoint registra o que foi entregue, com números da execução real.

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
