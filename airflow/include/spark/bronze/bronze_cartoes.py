import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from clients.spark_builder import SparkBuilder
from delta.tables import DeltaTable
from pyspark.sql.functions import col, upper

spark = SparkBuilder().get_session()

df = spark.read.format("csv").load("s3a://mairon-pipeline-delta-s3-landing/cartoes_landing.csv", 
                    header=True)

df_validos = df.where(col("cartao_id").isNotNull())
df_rejeitados = df.where(col("cartao_id").isNull())


df_validos = df_validos\
    .withColumn("tipo_cartao", upper("tipo_cartao"))\
    .withColumn("status_cartao", upper("status_cartao"))

if df_rejeitados.count() > 0:
    df_rejeitados\
        .write\
        .format("delta")\
        .mode("append")\
        .save("s3a://mairon-pipeline-delta-s3-landing/bronze_quarentena/cartoes/")
    
caminho_bronze = "s3a://mairon-pipeline-delta-s3-landing/bronze/cartoes"

if DeltaTable.isDeltaTable(spark, caminho_bronze):
    delta_bronze = DeltaTable.forPath(spark, caminho_bronze)

    delta_bronze.alias("destino").merge(
        df_validos.alias("origem"),
        "destino.cartao_id = origem.cartao_id"
    ).whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

else:
    df_validos.write \
        .format("delta") \
        .mode("append") \
        .save("s3a://mairon-pipeline-delta-s3-landing/bronze/cartoes")
