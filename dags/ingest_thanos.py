"""
## Generates commons tables based on ingestions.
"""


from airflow.decorators import dag, task_group, task
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from airflow.datasets import Dataset
from pendulum import datetime
from include.custom_operators import (
    DfToGCSOperator,
    LatestOnlyOperator,
    MigrationOperator,
    SnowflakeOperator,
    DagDoneOperator,
)
from include.utils import df_to_gcs_callable, get_thanos_parameters


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="0 0 * * *",
    catchup=False,
    description="Ingesting task counts from Thanos.",
    doc_md=__doc__,
    tags=["Dynamic Task Group Mapping"],
    default_args={"owner": "Marcy", "retries": 3},
)
def ingest_thanos():
    @task
    def retrieve_thanos_parameters_from_config():
        """Retrieve thanos parameters from config, number of config
        changes daily."""
        thanos_parameters = get_thanos_parameters()

        return thanos_parameters

    # dynamically mapped task group
    @task_group
    def thanos_parameter_processing(schema, source):
        callable_to_gcs = DfToGCSOperator(
            task_id="callable_to_gcs",
            python_callable=df_to_gcs_callable,
            gcs_conn_id="gcs_analytics",
            data_source=source,
            security_group="api-19",
            bucket="infra-data-team",
            table=f"api_{source}",
            pipeline_phase="ingest",
            partition=True,
            partition_depth="date",
            partition_delta=None,
            is_snapshot=False,
            col_types=None,
            check_cols=True,
            check_cols_omit=None,
            file_name_from_task_id=False,
            return_dataframe=False,
        )

        @task_group()
        def gcs_to_snowflake():
            latest_only = LatestOnlyOperator(
                task_id="latest_only",
            )

            migrate_table = MigrationOperator(
                task_id="migrate_table",
                schema=schema,
                table=source,
            )

            create_external_stage = SnowflakeOperator(
                task_id="create_external_stage",
                snowflake_conn_id="snowflake_analytics",
                sql=f"CREATE EXTERNAL STAGE IF NOT EXISTS {source}_stage;",
            )

            copy_into_table = SnowflakeOperator(
                task_id="copy_into_table",
                snowflake_conn_id="snowflake_analytics",
                sql=f"""
                COPY INTO {source}
                FROM (
                    SELECT *
                    FROM @{source}_stage/{source}.csv.gz
                    )
                FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1);
                """,
            )

            drop_duplicates = SnowflakeOperator(
                task_id="drop_duplicates",
                snowflake_conn_id="snowflake_analytics",
                sql=f"""
                    DELETE FROM {source} 
                    WHERE ROW_ID NOT IN (
                        SELECT MIN(ROW_ID) 
                        FROM {source} 
                        GROUP BY *
                    );""",
            )

            done = EmptyOperator(
                task_id="done",
            )

            chain(
                latest_only,
                [migrate_table, create_external_stage],
                copy_into_table,
                drop_duplicates,
                done,
            )

        done = EmptyOperator(
            task_id="done",
        )

        chain(callable_to_gcs, gcs_to_snowflake(), done)

    dagset_done = DagDoneOperator(
        task_id=f"dagset_done", outlets=[Dataset("ingest_thanos")]
    )

    chain(
        thanos_parameter_processing.partial(schema="IN_CLOUD_THANOS").expand(
            source=retrieve_thanos_parameters_from_config(),
        ),
        dagset_done,
    )


ingest_thanos()
