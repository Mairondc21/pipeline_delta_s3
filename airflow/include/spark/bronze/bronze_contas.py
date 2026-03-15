import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from clients.spark_builder import SparkBuilder
from delta.tables import DeltaTable
from pyspark.sql.functions import col, upper

spark = SparkBuilder().get_session()

df = spark.read.format("parquet").load("s3a://mairon-pipeline-delta-s3-landing/contas_landing.parquet")

df_validos = df.where(col("conta_id").isNotNull())
df_rejeitados = df.where(col("conta_id").isNull())

if df_rejeitados.count() > 0:
    df_rejeitados\
    .write\
    .format("delta")\
    .mode("append")\
    .save("s3a://mairon-pipeline-delta-s3-landing/bronze_quarentena/contas")

df_validos = df_validos\
            .withColumn("status_conta", upper("status_conta"))

caminho_bronze = "s3a://mairon-pipeline-delta-s3-landing/bronze/contas"

if DeltaTable.isDeltaTable(spark, caminho_bronze):
    delta_bronze = DeltaTable.forPath(spark, caminho_bronze)

    delta_bronze.alias("destino").merge(
        df_validos.alias("origem"),
        "destino.conta_id = origem.conta_id"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

else:
    df_validos\
        .write\
        .format("delta") \
        .mode("append") \
        .save(caminho_bronze)