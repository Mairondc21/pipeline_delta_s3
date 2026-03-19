from pathlib import Path
import sys


sys.path.append(str(Path(__file__).resolve().parents[3]))

import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch import RuntimeBatchRequest
from include.spark.clients.spark_builder import SparkBuilder


def build_pipeline_checkpoint():
    context = gx.get_context()

    checkpoint = Checkpoint(
        name="pipeline_checkpoint",
        data_context=context,
        run_name_template="%Y%m%d-%H%M%S-pipeline-gx",
        action_list=[
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction"},
            },
        ],
    )

    context.add_or_update_checkpoint(checkpoint=checkpoint)
    print("Checkpoint salvo no S3.")
    return checkpoint


def run_checkpoint():

    spark = SparkBuilder().get_session()
    context = gx.get_context()
    base_path = "s3a://mairon-pipeline-delta-s3-landing"

    dim_cartao = spark.read.format("delta").load(f"{base_path}/silver/dim_cartao")
    dim_cliente = spark.read.format("delta").load(f"{base_path}/silver/dim_cliente")
    dim_conta = spark.read.format("delta").load(f"{base_path}/silver/dim_conta")
    ft_transacao = spark.read.format("delta").load(f"{base_path}/silver/ft_transacao")

    batch_request_dim_cartao = RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="dim_cartao",
            runtime_parameters={"batch_data": dim_cartao},
            batch_identifiers={"run_id": "pipeline_checkpoint_run"},
        )
    batch_request_dim_cliente = RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="dim_cliente",
            runtime_parameters={"batch_data": dim_cliente},
            batch_identifiers={"run_id": "pipeline_checkpoint_run"},
        )
    batch_request_dim_conta = RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="dim_conta",
            runtime_parameters={"batch_data": dim_conta},
            batch_identifiers={"run_id": "pipeline_checkpoint_run"},
        )
    batch_request_ft_transacao = RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="ft_transacao",
            runtime_parameters={"batch_data": ft_transacao},
            batch_identifiers={"run_id": "pipeline_checkpoint_run"},
        )
    

    validations = [
        {
            "batch_request": batch_request_dim_cartao,
            "expectation_suite_name": "dim_cartao_suite",
        },
        {
            "batch_request": batch_request_dim_cliente,
            "expectation_suite_name": "dim_cliente_suite",
        },
        {
            "batch_request": batch_request_dim_conta,
            "expectation_suite_name": "dim_conta_suite",
        },
        {
            "batch_request": batch_request_ft_transacao,
            "expectation_suite_name": "ft_transacao_suite",
        }
    ]

    results = context.run_checkpoint(
        checkpoint_name="pipeline_checkpoint",
        validations=validations,
    )

    if not results.success:
        failed = [
            str(key)
            for key, vr in results.run_results.items()
            if not vr["success"]
        ]
        raise ValueError(f"GX falhou nas validações: {failed}")

    print("Todas as validações passaram.")
    return results


if __name__ == "__main__":
    SparkBuilder().get_session()
    build_pipeline_checkpoint()
    run_checkpoint()