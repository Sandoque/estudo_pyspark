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
| Projeto Python gerido pelo **uv** (`pyproject.toml` + `uv.lock`), Python **3.12** | Uma lista de dependências só, com versões travadas; mesma versão de Python do contêiner. Ver "Ambiente Python (uv)" |

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

## Ambiente Python (uv)

Decidido em 23/10/2026: o projeto é gerido pelo **uv** (`uv init` feito pelo
dono). O contêiner continua sendo o ambiente de **execução** (job, testes,
mutação); o ambiente uv no Windows serve ao editor (autocomplete, lint, tipos) e
a experimentos rápidos.

**Python 3.12, e não 3.14.** O `uv init` criou `requires-python = ">=3.14"`, mas
o PySpark 4.0 suporta só até o Python 3.13. Fixar na versão do contêiner:

```bash
uv python pin 3.12
```

e, no `pyproject.toml`, `requires-python = ">=3.12,<3.13"`.

**Dependências** — as mesmas versões de `docker/requirements.txt`, para os dois
ambientes não divergirem:

| Biblioteca | Versão | Para quê | Grupo |
| --- | --- | --- | --- |
| `pyspark` | 4.0.1 | Spark | principal |
| `delta-spark` | 4.0.0 | Delta Lake (MERGE, deletion vectors, time travel) | principal |
| `pytest` | 8.4.2 | testes e prova de mutação | dev |
| `jupyterlab` | 4.4.9 | notebooks | dev |
| `pandas` + `pyarrow` | 2.3.3 / 21.0.0 | opcionais: `toPandas()` e exibição no notebook; o código da fatia 1 não usa | opcional |

```bash
uv add pyspark==4.0.1 delta-spark==4.0.0
```

```bash
uv add --dev pytest==8.4.2 jupyterlab==4.4.9
```

```bash
uv add pandas==2.3.3 pyarrow==21.0.0
```

**Fica de fora, de propósito:** `sqlalchemy` e o driver do MySQL. O
`scripts/exportar_dw.py` roda com o Python do `webservices_flask` e usa a conexão
de lá (`core.database`), para nenhuma senha nem driver de banco morar nesta
pasta (constitution).

**Spark direto no Windows:** o Java 21 da máquina serve ao Spark 4, mas gravar
Delta em disco local no Windows costuma exigir `winutils.exe` + `HADOOP_HOME`.
Por isso o job e os testes rodam no contêiner.

**Uma lista só (T16):** o `docker/Dockerfile` instala direto pelo `uv.lock`
(`uv sync --frozen` em `/opt/venv`, fora de `/work` para não colidir com o
`.venv` do Windows no bind mount), e o antigo `docker/requirements.txt` foi
removido, mantendo contêiner e ambiente local sempre nas mesmas versões.

## Testes

Unitários com Spark local (`local[2]`) e DataFrames pequenos, um arquivo por
módulo. A reconciliação com a produção é o teste de integração: roda no job,
sobre o arquivo exportado, e vai para o relatório.

## Comandos

Ver `README.md`.
