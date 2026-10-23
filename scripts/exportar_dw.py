"""Exporta o cadastro de clientes da Denver do DW para a camada landing (FR-02).

Roda no WINDOWS, com o Python do webservices_flask (que já sabe conectar no DW):

    C:\\webservice\\dbA\\webservices_flask\\.venv\\Scripts\\python.exe -B scripts\\exportar_dw.py

O que faz, numa transação só de leitura e num mesmo retrato do dado
(START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY):

1. exporta TODOS os clientes da Denver (cancelados inclusive: a regra de
   "número de um cliente só" olha o cadastro inteiro) para CSV, todas as colunas
   como texto, sem tratamento -- é o arquivo bruto da bronze;
2. calcula pelo SQL de produção os CONTROLES que a gold do Spark tem de repetir
   (reconhecimento pelo número, por base, no dia de referência);
3. grava o manifesto: linhas, sha256 do CSV, consulta, horário e controles.

Nada é escrito no banco. A senha não passa por esta pasta: vem da configuração
do webservices_flask.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
WEBSERVICES = Path(r"C:\webservice\dbA\webservices_flask")
sys.path.insert(0, str(WEBSERVICES))

from sqlalchemy import text  # noqa: E402

from core.database import get_dw_engine  # noqa: E402

REVENDA = "Denver"
COLUNAS = (
    "revenda", "codigo_cliente", "nome", "nome_fantasia", "cidade", "cnpj_cpf",
    "vendedor", "categoria", "tabela_preco", "telefone", "celular", "telefone_b2b",
    "cancelado",
)
CONSULTA_CLIENTES = (
    "SELECT " + ", ".join(COLUNAS) + " FROM dw_previsao_financeira.clientes "
    "WHERE BINARY TRIM(revenda) = BINARY :revenda ORDER BY codigo_cliente"
)

# A MESMA regra do optin_service (0.198): um número (8 últimos dígitos) reconhece
# o cliente quando pertence a UM só cliente da Denver nos campos considerados.
CONSULTA_CONTROLES = """
WITH nums AS (
  SELECT TRIM(codigo_cliente) cod, TRIM(vendedor) ven, cancelado,
         RIGHT(REGEXP_REPLACE(COALESCE(telefone,''),'[^0-9]',''),8) t,
         RIGHT(REGEXP_REPLACE(COALESCE(celular,''),'[^0-9]',''),8) c
    FROM dw_previsao_financeira.clientes
   WHERE BINARY TRIM(revenda) = BINARY :revenda),
pares AS (
  SELECT cod, ven, cancelado, t AS fim FROM nums WHERE CHAR_LENGTH(t) = 8
  UNION SELECT cod, ven, cancelado, c FROM nums WHERE CHAR_LENGTH(c) = 8),
so_tel AS (SELECT cod, ven, cancelado, t AS fim FROM nums WHERE CHAR_LENGTH(t) = 8),
dono_ambos AS (SELECT fim, COUNT(DISTINCT cod) n FROM pares GROUP BY fim),
dono_tel AS (SELECT fim, COUNT(DISTINCT cod) n FROM so_tel GROUP BY fim)
SELECT
 (SELECT COUNT(*) FROM nums WHERE ven IN :vendedores AND cancelado = 0) AS clientes_ativos,
 (SELECT COUNT(DISTINCT s.cod) FROM so_tel s JOIN dono_tel d USING(fim)
   WHERE s.ven IN :vendedores AND s.cancelado = 0 AND d.n = 1) AS reconhecidos_so_telefone,
 (SELECT COUNT(DISTINCT p.cod) FROM pares p JOIN dono_ambos d USING(fim)
   WHERE p.ven IN :vendedores AND p.cancelado = 0 AND d.n = 1) AS reconhecidos_telefone_ou_celular,
 (SELECT COUNT(DISTINCT p.cod) FROM pares p JOIN dono_ambos d USING(fim)
   WHERE p.ven IN :vendedores AND p.cancelado = 0 AND d.n > 1) AS com_numero_repetido
"""


def vendedores_da_base(base: str, dia: date) -> list[str]:
    """Os vendedores da base no dia, pelo mesmo seed que o Spark usa (FR-06)."""

    seed = RAIZ / "seeds" / "de_para_vendedor_base.csv"
    with seed.open(encoding="utf-8") as arquivo:
        linhas = list(csv.DictReader(arquivo))
    vale = []
    for linha in linhas:
        de = date.fromisoformat(linha["valido_de"]) if linha["valido_de"] else date.min
        ate = date.fromisoformat(linha["valido_ate"]) if linha["valido_ate"] else date.max
        if linha["base"] == base and de <= dia < ate:
            vale.append(linha["vendedor"])
    return vale


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dia", default=date.today().isoformat(),
                        help="Dia de referência dos controles (AAAA-MM-DD). Padrão: hoje.")
    args = parser.parse_args()
    dia = date.fromisoformat(args.dia)

    destino = RAIZ / "data" / "landing"
    destino.mkdir(parents=True, exist_ok=True)
    carimbo = datetime.now().strftime("%Y%m%d_%H%M%S")
    caminho_csv = destino / f"clientes_denver_{carimbo}.csv"

    from sqlalchemy import bindparam

    engine = get_dw_engine(pool_pre_ping=True)
    inicio = time.perf_counter()
    with engine.connect() as conexao:
        conexao.execute(text("SET SESSION TRANSACTION READ ONLY"))
        conexao.execute(text("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"))
        try:
            linhas = conexao.execute(text(CONSULTA_CLIENTES), {"revenda": REVENDA}).all()
            t_export = time.perf_counter() - inicio

            controles = {}
            consulta = text(CONSULTA_CONTROLES).bindparams(bindparam("vendedores", expanding=True))
            for base in ("sku", "volume", "cobertura"):
                vendedores = vendedores_da_base(base, dia)
                t0 = time.perf_counter()
                resultado = conexao.execute(consulta, {"revenda": REVENDA, "vendedores": vendedores}).mappings().first()
                controles[base] = {
                    "vendedores": vendedores,
                    **{k: int(v) for k, v in dict(resultado).items()},
                    "segundos_sql": round(time.perf_counter() - t0, 3),
                }
        finally:
            conexao.execute(text("ROLLBACK"))

    with caminho_csv.open("w", encoding="utf-8", newline="") as arquivo:
        escritor = csv.writer(arquivo)
        escritor.writerow(COLUNAS)
        for linha in linhas:
            escritor.writerow(["" if v is None else str(v) for v in linha])

    sha256 = hashlib.sha256(caminho_csv.read_bytes()).hexdigest()
    manifesto = {
        "arquivo": caminho_csv.name,
        "linhas": len(linhas),
        "sha256": sha256,
        "revenda": REVENDA,
        "consulta": CONSULTA_CLIENTES,
        "exportado_em": datetime.now().isoformat(timespec="seconds"),
        "segundos_exportacao": round(t_export, 3),
        "dia_referencia": dia.isoformat(),
        "controles": controles,
    }
    caminho_csv.with_suffix(".json").write_text(json.dumps(manifesto, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"{len(linhas)} clientes -> {caminho_csv}")
    for base, valores in controles.items():
        print(f"  {base:9} {valores}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
