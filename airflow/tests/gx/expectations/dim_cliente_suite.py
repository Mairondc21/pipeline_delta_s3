from pathlib import Path
import sys


sys.path.append(str(Path(__file__).resolve().parents[3]))

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from include.spark.clients.spark_builder import SparkBuilder


BUCKET = "s3a://mairon-pipeline-delta-s3-landing/silver/dim_cliente"

spark = SparkBuilder().get_session()

df = spark.read.format("delta").load(BUCKET)

context = gx.get_context()

suite_name = "dim_cliente_suite"

lista_suite = [c.expectation_suite_name for c in context.list_expectation_suites()]

if suite_name not in lista_suite:
    context.add_expectation_suite(expectation_suite_name=suite_name)
else:
    print("suite já existente carregando...")

batch_request = RuntimeBatchRequest(
    datasource_name="spark_datasource",
    data_connector_name="runtime_data_connector",
    data_asset_name="dim_cliente",
    runtime_parameters={"batch_data": df},
    batch_identifiers={"run_id":"run_003"}
)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name
)

validator.expect_column_values_to_not_be_null("cliente_id")

validator.expect_column_values_to_match_regex(
    column="email",
    regex=r"^[\w\.-]+@[\w\.-]+\.\w+$"
)

validator.expect_column_values_to_be_between("renda_mensal", min_value=0.01)

validator.expect_column_value_lengths_to_be_between(
    column="cpf",
    min_value=11
)

validator.expect_column_value_lengths_to_be_between(
    column="telefone",
    min_value=11
)

validator.expect_column_values_to_be_in_set(
    "status_cliente",
    ["ATIVO", "INATIVO", "BLOQUEADO"]
)

validator.expect_column_values_to_be_in_set(
    "genero_new",
    ["M", "F"]
)

validator.expect_column_values_to_be_unique("sk_cliente")

validator.save_expectation_suite(discard_failed_expectations=False)

results = validator.validate()

if not results["success"]:
    for result in results["results"]:
        if not result["success"]:
            print(
                f"{result['expectation_config']['expectation_type']} "
                f"— coluna: {result['expectation_config']['kwargs'].get('column')}"
            )
    raise ValueError("Quality check falhou — pipeline interrompido.")
    

print("Suite validado com sucesso")