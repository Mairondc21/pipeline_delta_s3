from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


PROJECT_ROOT = "/usr/local/airflow"
SPARK_SCRIPT  = f"{PROJECT_ROOT}/include/spark"
GE_PATH = f"{PROJECT_ROOT}/tests/gx"
DBT_PATH = f"{PROJECT_ROOT}/include/dbt_gold"


default_args = {
    "owner": "mairon",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


with DAG(
    dag_id="pipeline_delta_s3",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pyspark", "delta", "s3", "great_expectations"],
) as dag:

    upload_to_s3 = BashOperator(
        task_id="upload_to_s3",
        bash_command=f"python {PROJECT_ROOT}/include/s3/ingestion_s3.py",
    )

    with TaskGroup(
        "spark",
        tooltip="Processamento Spark",
        default_args={"retries": 1}
    ) as spark_group:
        with TaskGroup(
        "bronze",
        tooltip="Ingestão Bronze — Landing → S3 Delta"
    ) as bronze_group:
            bronze_cartoes = BashOperator(
                task_id="spark_etl_bronze_cartoes",
                bash_command=f"python {SPARK_SCRIPT}/bronze/bronze_cartoes.py"
            )

            bronze_clientes = BashOperator(
                task_id="spark_etl_bronze_clientes",
                bash_command=f"python {SPARK_SCRIPT}/bronze/bronze_clientes.py"
            )

            bronze_contas = BashOperator(
                task_id="spark_etl_bronze_contas",
                bash_command=f"python {SPARK_SCRIPT}/bronze/bronze_contas.py"
            )

            bronze_transacoes = BashOperator(
                task_id="spark_etl_bronze_transacao",
                bash_command=f"python {SPARK_SCRIPT}/bronze/bronze_transacoes.py"
            )
            [bronze_clientes, bronze_contas, bronze_cartoes] >> bronze_transacoes

        with TaskGroup(
            "silver",
            tooltip="Transformação Silver"
        ) as silver_group:   
            silver_cartoes = BashOperator(
                task_id="spark_etl_silver_dim_cartao",
                bash_command=f"python {SPARK_SCRIPT}/silver/dim_cartao.py"
            )

            silver_clientes = BashOperator(
                task_id="spark_etl_silver_dim_cliente",
                bash_command=f"python {SPARK_SCRIPT}/silver/dim_cliente.py"
            )

            silver_contas = BashOperator(
                task_id="spark_etl_silver_dim_conta",
                bash_command=f"python {SPARK_SCRIPT}/silver/dim_conta.py"
            )

            silver_transacoes = BashOperator(
                task_id="spark_etl_silver_ft_transacao",
                bash_command=f"python {SPARK_SCRIPT}/silver/ft_transacao.py"
            )
            silver_data = BashOperator(
                task_id="spark_etl_silver_dim_data",
                bash_command=f"python {SPARK_SCRIPT}/silver/dim_data.py"
            )
            [silver_cartoes, silver_clientes, silver_contas,silver_data] >> silver_transacoes
        bronze_group >> silver_group

    with TaskGroup(
            "quality",
            tooltip="Great Expectations — validação Silver → Gold"
        ) as quality_group:
        ge_task = BashOperator(
            task_id="gx_validate_pipeline",
            bash_command=(
                """cd {GE_PATH} && 
                python checkpoints/pipeline_checkpoint.py"""
            ),
        )

    with TaskGroup(
        "Gold",
        tooltip="Processamento camada Gold",
        default_args={"retries": 1}
    ) as gold_group:
        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=(
                    f"cd {DBT_PATH} && "
                    f"dbt run"
                ),
        )
    with TaskGroup(
        "datahub",
        tooltip="Processamento Spark",
        default_args={"retries": 1}
    ) as datahub_group:
        datahub_ingest_s3 = BashOperator(
            task_id="datahub_ingest_s3",
            bash_command="""
            source /usr/local/airflow/dt_venv/bin/activate && \
            datahub ingest -c /usr/local/airflow/include/recipes/s3_recipe.yml
            """
        )
        datahub_ingest_dbt = BashOperator(
            task_id="datahub_ingest_dbt",
            bash_command="""
            source /usr/local/airflow/dt_venv/bin/activate && \
            datahub ingest -c /usr/local/airflow/include/recipes/dbt_recipe.yml
            """
        )
        datahub_ingest_s3 >> datahub_ingest_dbt

    chain(
    upload_to_s3, spark_group , quality_group, gold_group, datahub_group
    )
    

