import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from clients.spark_builder import SparkBuilder
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, lit, current_timestamp, row_number, md5, concat_ws,regexp_replace, when, try_to_date, coalesce
)

spark = SparkBuilder().get_session()

df = spark.read.format("parquet").load("s3a://mairon-pipeline-delta-s3-landing/bronze/clientes")

# Tipagem correta
df = df.select(
    col("cliente_id").cast("int"),
    col("nome"),
    col("cpf"),
    col("data_nascimento"),
    col("genero"),
    col("email"),
    col("telefone"),
    col("cidade"),
    col("estado"),
    col("renda_mensal").cast("float"),
    col("score_credito").cast("int"),
    col("data_cadastro"),
    col("status_cliente")
)

# Tratamento de caracteres
df = df.withColumn("cpf", regexp_replace(col("cpf"),"[.-]",""))\
    .withColumn("telefone", regexp_replace(col("telefone"),"[(\\s+)-]",""))\
    .withColumn("data_nascimento", regexp_replace(col("data_nascimento"),"/","-"))

df = df.withColumn("data_nascimento", coalesce(try_to_date(col("data_nascimento"), "yyyy-MM-dd"),
                                               try_to_date(col("data_nascimento"), "dd-MM-yyyy")))
# Ajuste no campo genero
df = df.withColumn("genero_new", when((col("genero") == "Masculino") | (col("genero") == "MASCULINO"), lit("M"))\
                            .when(col("genero") == "Feminino", lit("F"))\
                            .when(col("genero") == "male", lit("M"))\
                            .when(col("genero") == "female", lit("F"))\
                            .when(col("genero") == "M", lit("M"))\
                            .when(col("genero") == "F", lit("F"))\
                            .otherwise(lit("N/A")))

df_invalidos = df.where((~col("email").endswith("@email.com")) | (col("genero_new") == "N/A"))
df_validos = df.where((col("email").endswith("@email.com")) & ( col("genero_new") != "N/A"))

# if df_invalidos.count() > 0:
#     df_invalidos\
#     .write\
#     .format("delta")\
#     .mode("append")\
#     .save("s3a://mairon-pipeline-delta-s3-landing/silver_quarentena/cliente")

window_spec = Window().orderBy("cliente_id")

# adicao de colunas SCD
df_validos = df_validos.withColumn("data_alteracao", lit(None))\
    .withColumn("flag_atual", lit(True))\
    .withColumn("sk_cliente", row_number().over(window_spec))

colunas_monitoradas = ["email", "telefone", "cidade",
                       "estado", "renda_mensal", "status_cliente"]

df_novos = df_validos.withColumn(
    "hash_registro",
    md5(concat_ws("|", *[col(c) for c in colunas_monitoradas]))
)

caminho_silver = "s3a://mairon-pipeline-delta-s3-landing/silver/dim_cliente"

if DeltaTable.isDeltaTable(spark, caminho_silver):

    delta_table = DeltaTable.forPath(spark, caminho_silver)

    # ── 3. Detecta registros que mudaram ──
    df_existentes = delta_table.toDF().filter(col("flag_atual") == True)

    df_mudancas = df_novos.alias("novo").join(
        df_existentes.alias("existente"),
        on="cliente_id",
        how="left"
    ).filter(
        col("existente.cliente_id").isNull() |
        (col("novo.hash_registro") != col("existente.hash_registro")) 
    ).select("novo.*")

    delta_table.alias("destino").merge(
        df_mudancas.alias("origem"),
        "destino.cliente_id = origem.cliente_id AND destino.flag_atual = true"
    ) \
    .whenMatchedUpdate(values={
        "flag_atual":      lit(False),
        "data_alteracao":  current_timestamp()
     }).execute() \
  
    maxTableKey = DeltaTable.forPath(spark, caminho_silver).toDF().agg({"sk_cliente":"max"}).collect()[0][0]

    df_mudancas.withColumn("flag_atual", lit(True)) \
        .withColumn("data_alteracao", lit(None).cast("timestamp")) \
        .withColumn("sk_cliente", col("sk_cliente") + maxTableKey) \
        .write \
        .format("delta") \
        .mode("append") \
        .save(caminho_silver)

else:
    window_spec = Window.orderBy("cliente_id")

    df_validos \
        .withColumn("flag_atual", lit(True)) \
        .withColumn("data_alteracao", lit(None).cast("timestamp")) \
        .withColumn("hash_registro", md5(concat_ws("|", *[col(c) for c in colunas_monitoradas]))) \
        .withColumn("sk_cliente", row_number().over(window_spec)) \
        .write \
        .format("delta") \
        .mode("overwrite") \
        .save(caminho_silver)