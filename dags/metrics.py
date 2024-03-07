"""
## Metrics

Metric tables based on product data.
"""


from airflow.decorators import dag, task_group
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from airflow.datasets import Dataset
from pendulum import datetime
from include.custom_operators import (
    SnowflakeOperator,
    DagDoneOperator,
    SQLColumnCheckOperator,
    SQLTableCheckOperator,
    TestTableOperator,
    TypeSetOperator,
    PrimaryKeyOperator,
)
from include.utils import (
    metrics_tables,
    metrics_tables_column_checks_test,
    metrics_tables_column_checks_validate,
    metrics_tables_table_checks_test,
    metrics_tables_table_checks_validate,
    metrics_tables_primary_key_list,
    metrics_tables_data_types,
    METRICS_SQL_PATHS,
)

SNOWFLAKE_CONN_ID = "snowflake_analytics"


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=[Dataset("product")],
    catchup=False,
    description="Product metrics",
    doc_md=__doc__,
    tags=["Complex DAG graph", "Task Groups"],
    default_args={"owner": "Trevor", "retries": 3},
)
def metrics():
    start = EmptyOperator(
        task_id="start",
    )

    dagset_done = DagDoneOperator(
        task_id=f"dagset_done", outlets=[Dataset("metrics")]
    )

    metrics_task_groups = {}

    for table in metrics_tables:

        @task_group(
            group_id=table,
        )
        def create_table():
            create_tmp = SnowflakeOperator(
                task_id="create_tmp",
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                schema="PRODUCT",
                sql=METRICS_SQL_PATHS["create_tmp"],
                params={"table": table},
                queue="heavy-etl",
            )

            @task_group
            def test_tmp():
                SQLColumnCheckOperator(
                    task_id="test_cols",
                    conn_id=SNOWFLAKE_CONN_ID,
                    table=table,
                    column_mapping=metrics_tables_column_checks_test[table],
                )

                SQLTableCheckOperator(
                    task_id="test_table",
                    conn_id=SNOWFLAKE_CONN_ID,
                    table=table,
                    checks=metrics_tables_table_checks_test[table],
                )

                TestTableOperator(
                    task_id="custom_test",
                    schema="PRODUCT",
                    table=table,
                )

            swap = SnowflakeOperator(
                task_id="swap",
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                schema="PRODUCT",
                sql=METRICS_SQL_PATHS["swap"],
                params={"table": table},
                queue="heavy-etl",
            )

            @task_group
            def add_docs():
                SnowflakeOperator(
                    task_id="document_table",
                    snowflake_conn_id=SNOWFLAKE_CONN_ID,
                    sql=METRICS_SQL_PATHS["document_table"],
                    schema="PRODUCT",
                    params={"table": table},
                )

                SnowflakeOperator(
                    task_id="document_fields",
                    snowflake_conn_id=SNOWFLAKE_CONN_ID,
                    sql=METRICS_SQL_PATHS["document_fields"],
                    schema="PRODUCT",
                    params={"table": table},
                )

                set_data_types = TypeSetOperator(
                    task_id="set_data_types",
                    schema="PRODUCT",
                    table=table,
                    type_map=metrics_tables_data_types[table],
                )

                set_primary_key = PrimaryKeyOperator(
                    task_id="set_primary_key",
                    schema="PRODUCT",
                    table=table,
                    primary_key=metrics_tables_primary_key_list[table],
                )

                chain(set_data_types, set_primary_key)

            drop_tmp = SnowflakeOperator(
                task_id="drop_tmp",
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                sql=METRICS_SQL_PATHS["drop_tmp"],
                schema="PRODUCT",
                params={"table": table},
            )

            done = EmptyOperator(
                task_id="done",
            )

            create_tmp >> test_tmp() >> swap >> [add_docs(), drop_tmp] >> done

            @task_group
            def validate():
                test_cols = SQLColumnCheckOperator(
                    task_id="test_cols",
                    conn_id=SNOWFLAKE_CONN_ID,
                    table=table,
                    column_mapping=metrics_tables_column_checks_validate[table],
                    partition_clause="WHERE _PARTITIONDATE = CURRENT_DATE()",
                    retries=0,
                )

                test_table = SQLTableCheckOperator(
                    task_id="test_table",
                    table=table,
                    checks=metrics_tables_table_checks_validate[table],
                    retries=0,
                )

                custom_test = TestTableOperator(
                    task_id="custom_test",
                    schema="PRODUCT",
                    table=table,
                )

                sql_check_done = EmptyOperator(
                    task_id="sql_check_done",
                    trigger_rule="all_done",
                )

                custom_done = EmptyOperator(
                    task_id="custom_done",
                    trigger_rule="all_done",
                )

                chain([test_cols, test_table], sql_check_done)
                chain(custom_test, custom_done)

            swap >> validate()

        create_table_obj = create_table()

        metrics_task_groups[table] = create_table_obj

        chain(
            start,
            create_table_obj,
        )

    for table_task_group in metrics_task_groups.values():
        if not table_task_group.downstream_task_ids:
            chain(table_task_group, dagset_done)

    chain(
        start,
        metrics_task_groups["feature_adoption"],
        metrics_task_groups["feature_adoption_multi"],
        metrics_task_groups["feature_adoption_docs"],
    )

    chain(metrics_task_groups["feature_adoption"], dagset_done)

    chain(
        start,
        metrics_task_groups["user_workspace_airflow_daily"],
        metrics_task_groups["metrics_long"],
        metrics_task_groups["feature_adoption"],
    )

    chain(
        start,
        metrics_task_groups["user_org_visits_daily"],
        metrics_task_groups["metrics_long"],
    )

    chain(
        start,
        metrics_task_groups["user_workspace_airflow_daily"],
        metrics_task_groups["metrics_long"],
    )

    chain(
        start,
        metrics_task_groups["object_counts_multi"],
        [
            metrics_task_groups["feature_adoption"],
            metrics_task_groups["user_counts_daily"],
        ],
    )

    chain(
        start,
        metrics_task_groups["deployment_dates"],
        [
            metrics_task_groups["deployment_ops_daily"],
            metrics_task_groups["deployment_tasks_daily"],
            metrics_task_groups["deployment_activity_daily"],
            metrics_task_groups["object_counts_multi"],
            metrics_task_groups["org_weekly"],
            metrics_task_groups["org_monthly"],
            metrics_task_groups["org_daily"],
            metrics_task_groups["org_dates"],
        ],
    )

    chain(
        metrics_task_groups["deployment_ops_daily"],
        metrics_task_groups["metrics_long"],
    )
    chain(
        metrics_task_groups["deployment_tasks_daily"],
        [
            metrics_task_groups["deployment_tasks_weekly"],
            metrics_task_groups["deployment_tasks_monthly"],
            metrics_task_groups["deployment_tasks_multi"],
            metrics_task_groups["org_weekly"],
            metrics_task_groups["org_monthly"],
            metrics_task_groups["org_daily"],
        ],
    )

    chain(
        [
            metrics_task_groups["deployment_tasks_weekly"],
            metrics_task_groups["deployment_tasks_monthly"],
        ],
        metrics_task_groups["deployment_tasks_multi"],
    )

    chain(
        [
            metrics_task_groups["org_weekly"],
            metrics_task_groups["org_monthly"],
            metrics_task_groups["org_daily"],
        ],
        metrics_task_groups["org_multi"],
    )

    chain(
        metrics_task_groups["org_daily"],
        [
            metrics_task_groups["org_weekly"],
            metrics_task_groups["org_monthly"],
        ],
    )

    chain(
        metrics_task_groups["deployment_activity_daily"],
        metrics_task_groups["metrics_long"],
    )

    chain(
        start,
        metrics_task_groups["workspace_dates"],
        [
            metrics_task_groups["object_counts_multi"],
            metrics_task_groups["org_weekly"],
            metrics_task_groups["org_daily"],
            metrics_task_groups["org_monthly"],
        ],
    )

    chain(
        start,
        metrics_task_groups["org_dates"],
        [
            metrics_task_groups["org_ops_daily"],
            metrics_task_groups["object_counts_multi"],
            metrics_task_groups["org_weekly"],
            metrics_task_groups["org_daily"],
            metrics_task_groups["org_monthly"],
            metrics_task_groups["org_users_daily"],
            metrics_task_groups["org_multi"],
            metrics_task_groups["user_org_counts_daily"],
        ],
    )

    chain(
        metrics_task_groups["org_ops_daily"],
        metrics_task_groups["metrics_long"],
    )

    chain(
        metrics_task_groups["user_workspace_ide_daily"],
        metrics_task_groups["metrics_long"],
    )

    chain(
        metrics_task_groups["cluster_dates"],
        [
            metrics_task_groups["org_dates"],
            metrics_task_groups["org_weekly"],
            metrics_task_groups["org_daily"],
            metrics_task_groups["org_monthly"],
        ],
    )


metrics()
