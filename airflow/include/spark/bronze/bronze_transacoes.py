import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from clients.spark_builder import SparkBuilder
from pyspark.sql.functions import col, upper

spark = SparkBuilder().get_session()

df = spark.read \
    .option("multiline", "true") \
    .format("json")\
    .load("s3a://mairon-pipeline-delta-s3-landing/transacoes_landing.json")

df_validos = df.where(col("transacao_id").isNotNull() | col("conta_id").isNotNull())
df_rejeitados = df.where(col("transacao_id").isNull() | col("conta_id").isNull())

df_validos = df_validos\
            .withColumn("status_transacao", upper("status_transacao"))\
            .withColumn("tipo_transacao", upper("tipo_transacao"))

if df_rejeitados.count() > 0:
    df_rejeitados\
    .write\
    .format("delta")\
    .mode("append")\
    .save("s3a://mairon-pipeline-delta-s3-landing/bronze_quarentena/transacoes")

df_validos\
    .write\
    .format("delta") \
    .mode("append") \
    .save("s3a://mairon-pipeline-delta-s3-landing/bronze/transacoes")