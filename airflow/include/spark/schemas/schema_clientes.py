from pyspark.sql.types import (
    StructField, StructType,
    StringType, IntegerType, FloatType
)

schema_clientes_bronze = StructType([
    StructField("cliente_id",      IntegerType(), nullable=False),
    StructField("nome",            StringType(),  nullable=True),
    StructField("cpf",             StringType(),  nullable=True),
    StructField("data_nascimento", StringType(),  nullable=True),
    StructField("genero",          StringType(),  nullable=True),
    StructField("email",           StringType(),  nullable=True),
    StructField("telefone",        StringType(),  nullable=True),
    StructField("cidade",          StringType(),  nullable=True),
    StructField("estado",          StringType(),  nullable=True),
    StructField("renda_mensal",    FloatType(),   nullable=True),
    StructField("score_credito",   IntegerType(), nullable=True),
    StructField("data_cadastro",   StringType(),  nullable=True),
    StructField("status_cliente",  StringType(),  nullable=True),
])