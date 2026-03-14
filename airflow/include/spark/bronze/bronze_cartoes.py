import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from clients.spark_builder import SparkBuilder
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


df_validos.write \
    .format("delta") \
    .mode("append") \
    .save("s3a://mairon-pipeline-delta-s3-landing/bronze/cartoes")
