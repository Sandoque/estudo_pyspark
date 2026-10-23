"""Mostra o histórico (time travel) das tabelas Delta da fatia. Uso: python scripts/historico_delta.py"""

from estudo.sessao import criar_sessao

spark = criar_sessao("historico")
for tabela in ("bronze_clientes", "silver_clientes", "gold_reconhecimento"):
    print(f"== {tabela}")
    (
        spark.sql(f"DESCRIBE HISTORY delta.`/work/data/delta/{tabela}`")
        .selectExpr(
            "version", "operation",
            "operationMetrics['numOutputRows'] AS gravadas",
            "operationMetrics['numTargetRowsInserted'] AS inseridas",
            "operationMetrics['numTargetRowsUpdated'] AS atualizadas",
            "operationMetrics['numTargetRowsDeleted'] AS apagadas",
        )
        .orderBy("version")
        .show(truncate=False)
    )
spark.stop()
