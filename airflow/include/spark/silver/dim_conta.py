from pathlib import Path
import sys


sys.path.append(str(Path(__file__).resolve().parents[1]))

from clients.spark_builder import SparkBuilder
from delta.tables import DeltaTable
from pyspark.sql.functions import col


spark = SparkBuilder().get_session()

df = spark.read.format("delta").load("s3a://mairon-pipeline-delta-s3-landing/bronze/contas")

df = df.select(
    "conta_id",
    "tipo_conta",
    "agencia",
    col("numero_conta").cast("string"),
    col("data_abertura").cast("date"),
    col("data_encerramento").cast("date"),
    "status_conta",
    "banco_codigo"
)
caminho_silver = "s3a://mairon-pipeline-delta-s3-landing/silver/dim_conta"

if DeltaTable.isDeltaTable(spark, caminho_silver):
    delta_bronze = DeltaTable.forPath(spark, caminho_silver)

    delta_bronze.alias("destino").merge(
        df.alias("origem"),
        "destino.conta_id = origem.conta_id"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

else:
    df.write\
    .format("delta")\
    .mode("append")\
    .save(caminho_silver)
