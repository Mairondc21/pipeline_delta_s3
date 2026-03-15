import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from clients.spark_builder import SparkBuilder
from delta.tables import DeltaTable
from pyspark.sql.functions import col

spark = SparkBuilder().get_session()

df = spark.read.format("delta").load("s3a://mairon-pipeline-delta-s3-landing/bronze/cartoes")

df = df.select(
    col("cartao_id").cast("long"),
    "tipo_cartao",
    "bandeira",
    col("data_emissao").cast("date"),
    col("data_validade").cast("date"),
    "status_cartao",
    col("internacional").cast("boolean")
)
caminho_silver = "s3a://mairon-pipeline-delta-s3-landing/silver/dim_cartao"

if DeltaTable.isDeltaTable(spark, caminho_silver):
    delta_bronze = DeltaTable.forPath(spark, caminho_silver)

    delta_bronze.alias("destino").merge(
        df.alias("origem"),
        "destino.cartao_id = origem.cartao_id"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

else:

    df.write\
    .format("delta")\
    .mode("append")\
    .save(caminho_silver)