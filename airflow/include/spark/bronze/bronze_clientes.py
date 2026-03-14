import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from clients.spark_builder import SparkBuilder
from pyspark.sql.functions import col, upper, initcap

spark = SparkBuilder().get_session()

df = spark.read.format("csv").load("s3a://mairon-pipeline-delta-s3-landing/clientes_landing.csv", 
                    header=True)

df_validos = df.where(col("cliente_id").isNotNull())
df_rejeitados = df.where(col("cliente_id").isNull())

df_validos = df_validos\
    .withColumn("status_cliente", upper("status_cliente"))\
    .withColumn("nome", initcap("nome"))\
    .withColumn("cpf", col("cpf").cast("string"))

if df_rejeitados.count() > 0:
    df_rejeitados\
        .write\
        .format("delta")\
        .mode("append")\
        .save("s3a://mairon-pipeline-delta-s3-landing/bronze_quarentena/clientes")
    
df_validos.write \
    .format("delta") \
    .mode("append") \
    .save("s3a://mairon-pipeline-delta-s3-landing/bronze/clientes")