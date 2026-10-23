# Plan — fatia 1

Versão: 0.1 — 23/10/2026. Atende a [spec.md](spec.md).

## Arquitetura

```text
Windows (host)                             Contêiner "spark" (Linux)
───────────────                            ─────────────────────────
scripts/exportar_dw.py                     src/estudo/  (pacote)
  SELECT READ ONLY no DW                     bronze.py        landing → Delta bronze
  (conexão do webservices_flask)             silver_clientes.py  tipagem, normalização,
        │                                                       quarentena, dedupe
        ▼                                    de_para.py       vendedor → base com vigência
data/landing/clientes_denver_<ts>.csv        gold_reconhecimento.py  indicador por base/dia
data/landing/clientes_denver_<ts>.json       qualidade.py     reconciliação e checagens
  (manifesto + controles do SQL)           jobs/fatia1_clientes.py   roda tudo e mede
        │                                  tests/  pytest com Spark local
        └──────── volume /work ───────────► data/delta/{bronze,silver,gold,quarentena}
                                           reports/fatia1_<ts>.json
```

## Decisões

| Decisão | Por quê |
| --- | --- |
| Spark **4.0** + Delta **4.0** + Python 3.12 + Java 17 | Mesma geração do Databricks atual (ANSI ligado por padrão), para o código rodar depois na Free Edition sem surpresa |
| Contêiner em vez de instalar no Windows | Spark no Windows exige `winutils`/`HADOOP_HOME` para gravar; o contêiner é Linux e não toca o Python do Windows |
| Exportação pelo host, em CSV | O MySQL escuta só no loopback do host (o contêiner não alcança) e o Python do `webservices_flask` não tem `pyarrow`; CSV com todas as colunas como texto é o "arquivo bruto" da camada bronze, como na vida real |
| Controles calculados pelo SQL no mesmo momento da exportação | A reconciliação compara Spark com a regra de produção sobre **o mesmo retrato** do dado |
| Bronze tudo texto + `_lote` (sha256 do arquivo) + `_ingerido_em` | Bronze é cópia fiel; tipar é trabalho da silver. O sha256 do arquivo torna a ingestão idempotente (`MERGE` por `_lote` + linha) |
| Silver com `MERGE` por `codigo_cliente` | Idempotente e preparada para lotes novos (o cadastro muda todo dia) |
| De/para em seed CSV versionado | É regra comercial que muda por decisão (como a 131/134 em 21/10); vive em arquivo legível, não em `CASE` espalhado |
| Gold recalculada inteira por dia de referência (`replaceWhere`) | Indicador de um dia é função do cadastro e da vigência; recalcular o dia é simples e idempotente |

## Regras de normalização (FR-04)

- **Documento:** só dígitos, completados com zeros à esquerda até 15 (o
  cadastro engole o zero da frente). Com 15 dígitos, as posições 10–13 são a
  "filial": **`0000` é CPF guardado no formato do Control** (9 primeiros + 2
  últimos = CPF de 11) — todo CNPJ real tem filial a partir de `0001`; senão é
  CNPJ (os 14 últimos). Exemplos sintéticos: `529982247000025` → CPF 529.982.247-25;
  `12345678000090` (14, zero engolido) → CPF 012.345.678-90; `1222333000128` →
  CNPJ 01.222.333/0001-28. Com 11 dígitos sem o formato, CPF como está. O resto:
  `documento_tipo = 'desconhecido'`. Os dígitos verificadores são conferidos e
  viram a coluna `documento_valido` (marca, não quarentena: o cadastro tem
  documento errado de verdade e o cliente existe).
- **Telefones:** só dígitos; `fim8` = 8 últimos dígitos quando há pelo menos 8.
- **Reconhecimento (FR-07):** um número (fim8) reconhece o cliente quando
  pertence a **um só** cliente da Denver, olhando os campos considerados
  (`telefone`, ou `telefone` + `celular`). Mesma regra do `optin_service`.

## Testes

Unitários com Spark local (`local[2]`) e DataFrames pequenos, um arquivo por
módulo. A reconciliação com a produção é o teste de integração: roda no job,
sobre o arquivo exportado, e vai para o relatório.

## Comandos

Ver `README.md`.
