from pathlib import Path
import sys


sys.path.append(str(Path(__file__).resolve().parents[1]))

from clients.spark_builder import SparkBuilder
from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    date_format,
    lit,
    row_number,
    when,
)
from pyspark.sql.window import Window


spark = SparkBuilder().get_session()

start_date = "2024-01-01"
end_date = "2024-12-31"

calendar_df = spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as date")

window_spec = Window.orderBy(lit(1))

df = calendar_df.withColumns({
    "sk_date": row_number().over(window_spec),
    "ano": date_format("date","yyyy"),
    "mes": date_format("date","MM"),
    "dia": date_format("date","d"),
    "trimestre": date_format("date","q"),
    "semestre": when(col("trimestre").isin([1,2]), lit(1)).otherwise(lit(2)),
    "nome_mes": date_format("date","MMM"),
    "dia_semana": date_format("date","E"),
    "flag_fim_semana": when(col("dia_semana").isin(["Sat","Sun"]),lit(True)).otherwise(lit(False))
}
)
caminho_silver = "s3a://mairon-pipeline-delta-s3-landing/silver/dim_data"

if DeltaTable.isDeltaTable(spark, caminho_silver):
    delta_bronze = DeltaTable.forPath(spark, caminho_silver)

    delta_bronze.alias("destino").merge(
        df.alias("origem"),
        "destino.sk_date = origem.sk_date"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

else:
    df.write\
    .format("delta")\
    .mode("append")\
    .save(caminho_silver)
