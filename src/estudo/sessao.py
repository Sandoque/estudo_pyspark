"""Sessão Spark com Delta Lake, igual no job, nos testes e no notebook."""

from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def criar_sessao(nome: str = "estudo", master: str = "local[*]", particoes: int = 8) -> SparkSession:
    builder = (
        SparkSession.builder.master(master)
        .appName(nome)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # 31 mil linhas não pedem as 200 partições padrão do shuffle.
        .config("spark.sql.shuffle.partitions", str(particoes))
        .config("spark.sql.session.timeZone", "America/Maceio")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
