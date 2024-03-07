"""
## Ingest API

Ingesting API service data.

-------------------------

Upstream Data Source
[infra-data-team GCS Bucket](https://http.cat/404)

-------------------------

View the code for this DAG [here]((https://http.cat/404)). 
"""


from airflow.decorators import dag, task_group
from airflow.operators.bash import BashOperator
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
from include.utils import df_to_gcs_callable
from include.utils import api_ingestion_source_list


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="0 0 * * *",
    catchup=False,
    description="Ingesting API service data",
    doc_md=__doc__,
    default_args={"owner": "Grant", "retries": 3},
)
def ingest_api():
    sleep = BashOperator(
        task_id="sleep",
        bash_command="sleep 5",
    )

    ingest_task_groups = []

    for source in api_ingestion_source_list:

        @task_group(
            group_id=source,
        )
        def outer_tg():
            callable_to_gcs = DfToGCSOperator(
                task_id="callable_to_gcs",
                python_callable=df_to_gcs_callable,
                data_source=source,
                security_group=f"api-23",
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
                    schema="IN_CLOUD",
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

        ingest_task_groups.append(outer_tg())

    dagset_done = DagDoneOperator(
        task_id=f"dagset_done", outlets=[Dataset("ingest_api")]
    )

    chain(sleep, ingest_task_groups, dagset_done)


ingest_api()
