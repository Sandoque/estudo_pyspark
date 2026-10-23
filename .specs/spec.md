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

## 5. Perguntas em aberto

- Q-01: a fatia 3 deve usar o Livro de Saída (maior tabela fiscal) ou as vendas
  do `bd_shiny_bruto`? Decidir antes de começar a fatia 3.
