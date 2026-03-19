from pathlib import Path
import sys


sys.path.append(str(Path(__file__).resolve().parents[3]))

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from include.spark.clients.spark_builder import SparkBuilder


spark = SparkBuilder().get_session()
context = gx.get_context()

BUCKET = "s3a://mairon-pipeline-delta-s3-landing"

df_transacao = spark.read.format("delta")\
    .load(f"{BUCKET}/silver/ft_transacao")

suite_name = "ft_transacao_suite"

suites_existentes = [s.expectation_suite_name 
                     for s in context.list_expectation_suites()]

if suite_name not in suites_existentes:
    context.add_expectation_suite(expectation_suite_name=suite_name)
else:
    print(f"Suite '{suite_name}' já existe, carregando...")

batch_request = RuntimeBatchRequest(
    datasource_name="spark_datasource",
    data_connector_name="runtime_data_connector",
    data_asset_name="ft_transacao",
    runtime_parameters={"batch_data": df_transacao},
    batch_identifiers={"run_id": "run_001"}
)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name
)

validator.expect_column_values_to_not_be_null("transacao_id")
validator.expect_column_values_to_not_be_null("conta_id")
validator.expect_column_values_to_be_unique("transacao_id")
validator.expect_column_values_to_be_between("valor", min_value=0.01)
validator.expect_column_values_to_be_in_set(
    "status_transacao",
    ["CONCLUIDA", "PENDENTE", "CANCELADA", "ERRO"]
)
validator.expect_column_values_to_be_in_set(
    "tipo_transacao",
    ["PIX", "TED", "DOC", "DEBITO", "CREDITO", "SAQUE"]
)
validator.expect_table_row_count_to_be_between(min_value=1)

validator.save_expectation_suite(discard_failed_expectations=False)
print(f"Suite '{suite_name}' salva.")

results = validator.validate()

if not results["success"]:
    for result in results["results"]:
        if not result["success"]:
            print(
                f"{result['expectation_config']['expectation_type']} "
                f"— coluna: {result['expectation_config']['kwargs'].get('column')}"
            )
    raise ValueError("Quality check falhou — pipeline interrompido.")

print("Todas as validações passaram.")