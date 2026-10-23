# Estudo PySpark + Delta — cadastro da Denver

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

3. **Testes e prova de mutação**:

```powershell
docker compose run --rm spark pytest -q
docker compose run --rm spark python scripts/testar_mutacoes.py
```

4. **Explorar** no JupyterLab (http://127.0.0.1:8888, só nesta máquina) e na Spark UI (http://127.0.0.1:4040 enquanto houver sessão):

```powershell
docker compose up
```

## Pastas

| Pasta | O que tem |
| --- | --- |
| `.specs/` | constitution, spec, plan, tasks e checkpoints (SDD) |
| `scripts/exportar_dw.py` | exportação só leitura + controles do SQL de produção |
| `seeds/de_para_vendedor_base.csv` | vendedor → base, com vigência (131 até 20/10, 134 desde 21/10) |
| `src/estudo/` | bronze, silver, de/para, gold, qualidade |
| `jobs/fatia1_clientes.py` | a fatia de ponta a ponta, medida |
| `tests/` | pytest com Spark local |
| `notebooks/` | exploração |
| `data/`, `reports/` | **dado real: fora do git** |
