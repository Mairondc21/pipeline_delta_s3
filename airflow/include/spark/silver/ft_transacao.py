import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from clients.spark_builder import SparkBuilder
from delta.tables import DeltaTable
from pyspark.sql.functions import col

spark = SparkBuilder().get_session()

bronze_transacao = spark.read.format("delta").load("s3a://mairon-pipeline-delta-s3-landing/bronze/transacoes")
bronze_contas = spark.read.format("delta").load("s3a://mairon-pipeline-delta-s3-landing/bronze/contas")
bronze_cartao = spark.read.format("delta").load("s3a://mairon-pipeline-delta-s3-landing/bronze/cartoes")

df_join = bronze_transacao.join(
        bronze_contas,
        on="conta_id",
        how="left"
).select(
    bronze_contas.cliente_id,
    bronze_contas.saldo_atual,
    bronze_contas.limite_credito,
    *bronze_transacao,
         )

df_join = df_join.join(
    bronze_cartao,
    on="conta_id",
    how="left"
).select(
    *df_join,
    bronze_cartao.cartao_id,
    bronze_cartao.limite_total,
    bronze_cartao.limite_utilizado
)

df = df_join.select(
    "transacao_id",
    "cliente_id",
    "cartao_id",
    "conta_id",
    "conta_destino_id",
    "canal",
    "categoria",
    "descricao",
    "cidade_transacao",
    "tipo_transacao",
    "valor",
    "saldo_atual",
    "limite_credito",
    "limite_utilizado",
    "limite_total"
)

df_invalido = df.where(col("valor") <= 0)
df_validos = df.where(col("valor") > 0)

if df_invalido.count() > 0:
    df_invalido\
    .write\
    .format("delta")\
    .mode("append")\
    .save("s3a://mairon-pipeline-delta-s3-landing/silver_quarentena/ft_transacao")

caminho_silver = "s3a://mairon-pipeline-delta-s3-landing/silver/ft_transacao"

if DeltaTable.isDeltaTable(spark, caminho_silver):

    delta_silver = DeltaTable.forPath(spark, caminho_silver)

    delta_silver.alias("destino").merge(
        df_validos.alias("origem"),
        "origem.transacao_id == destino.transacao_id"
    ).whenNotMatchedInsertAll()\
    .execute()
    

else:
    df_validos\
    .write\
    .format("delta")\
    .mode("overwrite")\
    .save(caminho_silver)