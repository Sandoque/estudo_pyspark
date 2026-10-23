# Spec — Estudo PySpark + Delta (fatia 1: cadastro de clientes da Denver)

Versão: 0.1 — 23/10/2026\
Regras: [constitution.md](constitution.md)

## 1. Problema real que a fatia reproduz

Em 20 e 21/10/2026, no segundo dia de produção do WhatsApp da Denver, quatro
clientes que existiam no Control caíram no formulário manual porque:

- o número do WhatsApp estava no `celular`, e o reconhecimento só olhava o
  `telefone`;
- o CPF digitado não casava com o cadastro, que guarda CPF com **15 dígitos**
  (9 primeiros + `0000` + 2 verificadores);
- a Base Cobertura mudou de composição (131 saiu, 134 entrou em 21/10).

Medido no DW em 21/10, nas carteiras liberadas: reconhecidos sozinhos pelo número
**270** só pelo `telefone` contra **676** por `telefone` ou `celular`, com **78**
clientes com número repetido em mais de um cliente.

A fatia 1 refaz esse caminho em PySpark + Delta: ingestão, normalização, de/para,
validação e o indicador final — e o resultado tem de bater com o SQL de produção.

## 2. Requisitos

| ID | Requisito | Critério de aceite |
| --- | --- | --- |
| FR-01 | Ambiente local reproduzível: PySpark + Delta Lake + JupyterLab + pytest num contêiner | `docker compose build` e `docker compose run --rm spark pytest` passam numa máquina limpa; nada instalado no Python do Windows |
| FR-02 | Exportação do cadastro da Denver do DW para arquivo bruto | Script só `SELECT` em `READ ONLY`; CSV em `data/landing/` com **manifesto** (linhas, sha256, consulta, horário) e **controles** calculados pelo SQL de produção |
| FR-03 | Bronze: arquivo bruto → tabela Delta, tudo como texto, com metadados de ingestão | Contagem bronze = linhas do manifesto; rodar de novo o mesmo arquivo não duplica (idempotente pelo sha256 do lote) |
| FR-04 | Silver: tipagem e normalização do cadastro | Documento no formato normal (CPF de 15 → 11 dígitos; CNPJ com 14); `telefone`, `celular` e `telefone_b2b` nos 8 últimos dígitos; `cancelado` booleano; um registro por `codigo_cliente` |
| FR-05 | Quarentena com motivo | Sem código, revenda diferente, código duplicado com conteúdo diferente: vão para `quarentena_clientes` com `motivo`; `bronze = silver + quarentena + duplicatas exatas` |
| FR-06 | De/para vendedor → base, com vigência | Seed versionado com `valido_de` / `valido_ate`; a base de um vendedor depende do dia de referência (131 até 20/10, 134 a partir de 21/10) |
| FR-07 | Gold: reconhecimento pelo número | Por dia de referência e base: clientes ativos, reconhecíveis só pelo `telefone`, por `telefone` ou `celular`, e com número repetido em mais de um cliente |
| FR-08 | Reconciliação com a produção | Os números do FR-07 **iguais** aos controles do FR-02 (mesma regra em SQL); qualquer diferença é relatada, não arredondada |
| FR-09 | Medição | Tempo de cada etapa e do total no relatório da execução, ao lado do tempo do SQL de produção |
| FR-10 | Relatório da execução | JSON em `reports/` com contagens por camada, quarentena por motivo, reconciliação e tempos |

## 3. Fora do escopo da fatia 1

- Escrever em qualquer banco (MySQL, ERP, DW).
- Rodar na nuvem. A Databricks Free Edition entra numa fatia futura, só com dado
  sintético.
- Orquestração (Jobs/Workflows): fatia 2.
- Vendas fiscais e o reprocessamento por janela com `replaceWhere`/`MERGE`: fatia 3.

## 4. Critério do teste de uso

Ao fim da fatia 1, registrar em `checkpoints.md`:

1. Os números bateram com a produção? (FR-08)
2. Quanto tempo levou cada etapa, contra o SQL de produção? (FR-09)
3. O que ficou mais simples e o que ficou mais complexo do que no pipeline
   atual (código, testes, operação)?
4. Recomendação: onde (se em algum lugar) Spark + Delta valeriam no pipeline da
   Meggal, e o que precisaria ser verdade para isso.

## 5. Perguntas decididas

- **Q-01 (decidida em 23/10/2026):** a fatia de volume (milhões de linhas) usa o
  **Livro de Saída (`livros_fiscais.raw_livro_saida`)**, começando pela Denver
  (**1.592.890 linhas**, 30 competências de `05/2024` a `10/2026`, R$ 690,84
  milhões em `VLR_TOTAL_NOTA_CALC`).

---

# Spec — Fatia 2: Livro de Saída da Denver (1,59 milhão de linhas e janela `replaceWhere`)

Versão: 0.2 — 23/10/2026

## 1. Problema real que a fatia reproduz

Na fatia 1 (31.625 clientes), o SQL de produção levou ~5,7 s contra ~30,7 s do
Spark: para tabelas pequenas o custo fixo da JVM não se paga. O gargalo real da
operação está nos **livros fiscais** (`livros_fiscais.raw_livro_saida`, com
1.592.890 linhas só na Denver e 4,91 milhões nas 11 revendas):

1. **Reprocessamento completo toda noite vs. janela de 3 meses:** a extração do
   ERP relê uma janela (`LIVRO_SAIDA_LOOKBACK_MESES = 3`), mas a consolidação e
   os modelos seguintes reprocessam e regravam os ~5 milhões de linhas desde
   maio/2024 (precisando de subprocessos por revenda em `livros_geral_batch.py`
   para caber na memória e gerando GBs de temporários no disco `C:`).
2. **Perda de centavos em `float32` / ruído em `DOUBLE`:** os resumos fiscais em
   precisão simples perdiam até R$ 78 por célula acima de R$ 2 milhões (ex.: em
   09/2026 na Denver, `15.958.107,00` virava `15.958.100`), e somar `DOUBLE` no
   MySQL acumula ruído binário (`6640104.08000004`).
3. **Grão da nota fiscal:** agrupar apenas por `(revenda, nf)` fundia notas
   diferentes; o grão correto da nota é `(revenda, nf, livro, chave_nf)`.

Retrato medido no DW em 23/10/2026 (`livros_fiscais.raw_livro_saida`, Denver):

| Ano | Linhas (itens) | NFs distintas | Soma `VLR_TOTAL_NOTA_CALC` (`DECIMAL(15,2)`) |
| --- | ---: | ---: | ---: |
| 2024 | 472.093 | 162.960 | R$ 208.606.653,41 |
| 2025 | 644.568 | 193.686 | R$ 275.304.256,53 |
| 2026 | 476.229 | 127.057 | R$ 206.925.615,84 |
| **Total** | **1.592.890** | **483.703** | **R$ 690.836.525,78** |

## 2. Requisitos da Fatia 2

| ID | Requisito | Critério de aceite |
| --- | --- | --- |
| FR-11 | Exportação `READ ONLY` do Livro de Saída da Denver (`scripts/exportar_livro_saida.py`) | Exporta `raw_livro_saida` da Denver (completo ou janela `--data-inicial`/`--data-final`) para CSV + manifesto SHA-256 + controles mensais do SQL (`DECIMAL(15,2)` vs `FLOAT`/`DOUBLE`) |
| FR-12 | Bronze idempotente por lote (`bronze_livro_saida`) | Confere SHA-256 e contagem do manifesto; reexecutar o mesmo arquivo não duplica |
| FR-13 | Silver tipada em `DECIMAL(15,2)` e particionada por `(ano, mes)` com `replaceWhere` | Valores fiscais como `DECIMAL(15,2)`, chave da nota `(revenda, nf, livro, chave_nf)`, quarentena com motivo (`sem_nf`, `sem_data_emissao`, `revenda_diferente`, `valor_invalido`) fechando `bronze = silver + quarentena + duplicatas_exatas`, e gravação substituindo apenas as partições `(ano, mes)` presentes no lote |
| FR-14 | Gold mensal (`gold_livro_saida_mensal`) | Por `(revenda, ano, mes)`: `itens`, `nfs_distintas` (pela chave completa), somas em `DECIMAL(15,2)` (`vlr_produto`, `vlr_icms`, `vlr_pis`, `vlr_cofins`, `vlr_total_nota_calc`) e diferença medida contra `float32`, gravada com `replaceWhere` nos meses do lote |
| FR-15 | Reconciliação ao centavo com o SQL de produção | 100% das competências exportadas batem com diferença `R$ 0,00` nos indicadores em `DECIMAL(15,2)` e nas contagens de itens e NFs |
| FR-16 | Benchmark: Carga Completa (30 meses, 1,59M linhas) vs. Janela de 3 meses (`replaceWhere`) | Relatório JSON em `reports/` medindo tempo, linhas gravadas e arquivos Parquet tocados na carga completa vs. reprocessamento da janela de lookback (3 meses), comparado ao tempo do SQL no MySQL |
| FR-17 | Amostra sintética + Testes e Mutação | `seeds/amostra_sintetica/livro_saida_denver_sintetico.{csv,json}` versionado no Git, suíte `pytest` fim-a-fim e novas mutações em `scripts/testar_mutacoes.py` |
