"""
## Product

**Transformation tables for the based on Splunk, Thanos, and API data.**

-------------------------

#### Upstream DAGs

- ingest_api
- ingest_thanos
- ingest_splunk

View the code for this DAG [here](https://http.cat/404).

This DAG primarily produces data in the `DWH_DEV.PRODUCT` schema in Snowflake.

Documentation of the data produced by the DAG can be found in our [Data Catalogue](https://http.cat/404).
"""


from airflow.decorators import dag, task_group
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain, chain_linear
from airflow.datasets import Dataset
from pendulum import datetime
from include.custom_operators import (
    SnowflakeOperator,
    DagDoneOperator,
    ExternalTaskSensorAsync,
    LastRunSensor,
    SQLColumnCheckOperator,
    SQLTableCheckOperator,
    TestTableOperator,
    TypeSetOperator,
    PrimaryKeyOperator,
)
from include.utils import (
    product_primary_tables,
    product_downstream_tables,
    product_tables_column_checks_test,
    product_tables_column_checks_validate,
    product_tables_table_checks_test,
    product_tables_table_checks_validate,
    product_tables_primary_key_list,
    product_tables_data_types,
    PRODUCT_PRIMARY_SQL_PATHS,
)

SNOWFLAKE_CONN_ID = "snowflake_analytics"


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=[
        Dataset("ingest_splunk"),
        Dataset("ingest_api"),
        Dataset("ingest_thanos"),
    ],
    catchup=False,
    description="Product models",
    doc_md=__doc__,
    tags=["Datasets", "Task Groups"],
    default_args={"owner": "Philip", "retries": 3},
)
def product():
    wait_for_DAG_ingest_thanos = ExternalTaskSensorAsync(
        task_id="wait_for_DAG_ingest_thanos",
        external_dag_id="ingest_thanos",
    )

    wait_for_DAG_commons = ExternalTaskSensorAsync(
        task_id="wait_for_DAG_commons",
        external_dag_id="commons",
    )

    wait_for_DAG_calendar_tables = ExternalTaskSensorAsync(
        task_id="wait_for_DAG_calendar_tables",
        external_dag_id="calendar_tables",
    )

    wait_for_last_product_hourly = LastRunSensor(
        task_id="wait_for_last_product_hourly",
        external_dag_id="product_hourly",
    )

    wait_for_last_ingest_segment = LastRunSensor(
        task_id="wait_for_last_ingest_segment",
        external_dag_id="ingest_segmentation",
    )

    wait_for_last_ingest_splunk = LastRunSensor(
        task_id="wait_for_last_ingest_splunk",
        external_dag_id="ingest_splunk",
    )

    dagset_done = DagDoneOperator(
        task_id=f"dagset_done", outlets=[Dataset("product")]
    )

    product_primary_tables_task_groups = {}

    for table in product_primary_tables:

        @task_group(
            group_id=table,
        )
        def create_table():
            create_tmp = SnowflakeOperator(
                task_id="create_tmp",
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                schema="PRODUCT",
                sql=PRODUCT_PRIMARY_SQL_PATHS["create_tmp"],
                params={"table": table},
                queue="heavy-etl",
            )

            @task_group
            def test_tmp():
                SQLColumnCheckOperator(
                    task_id="test_cols",
                    conn_id=SNOWFLAKE_CONN_ID,
                    table=table,
                    column_mapping=product_tables_column_checks_test[table],
                )

                SQLTableCheckOperator(
                    task_id="test_table",
                    conn_id=SNOWFLAKE_CONN_ID,
                    table=table,
                    checks=product_tables_table_checks_test[table],
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
                sql=PRODUCT_PRIMARY_SQL_PATHS["swap"],
                params={"table": table},
                queue="heavy-etl",
            )

            @task_group
            def add_docs():
                SnowflakeOperator(
                    task_id="document_table",
                    snowflake_conn_id=SNOWFLAKE_CONN_ID,
                    sql=PRODUCT_PRIMARY_SQL_PATHS["document_table"],
                    schema="PRODUCT",
                    params={"table": table},
                )

                SnowflakeOperator(
                    task_id="document_fields",
                    snowflake_conn_id=SNOWFLAKE_CONN_ID,
                    sql=PRODUCT_PRIMARY_SQL_PATHS["document_fields"],
                    schema="PRODUCT",
                    params={"table": table},
                )

                set_data_types = TypeSetOperator(
                    task_id="set_data_types",
                    schema="PRODUCT",
                    table=table,
                    type_map=product_tables_data_types[table],
                )

                set_primary_key = PrimaryKeyOperator(
                    task_id="set_primary_key",
                    schema="PRODUCT",
                    table=table,
                    primary_key=product_tables_primary_key_list[table],
                )

                chain(set_data_types, set_primary_key)

            drop_tmp = SnowflakeOperator(
                task_id="drop_tmp",
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                sql=PRODUCT_PRIMARY_SQL_PATHS["drop_tmp"],
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
                    column_mapping=product_tables_column_checks_validate[table],
                    partition_clause="WHERE _PARTITIONDATE = CURRENT_DATE()",
                    retries=0,
                )

                test_table = SQLTableCheckOperator(
                    task_id="test_table",
                    table=table,
                    checks=product_tables_table_checks_validate[table],
                    partition_clause="WHERE _PARTITIONDATE = CURRENT_DATE()",
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

        product_primary_tables_task_groups[table] = create_table_obj

        chain(
            [
                wait_for_last_product_hourly,
                wait_for_last_ingest_segment,
                wait_for_last_ingest_splunk,
            ],
            create_table_obj,
        )

    @task_group
    def model_web():
        model_web_task_groups = {}

        for table in ["visits", "airflow_visits"]:

            @task_group(
                group_id=table,
            )
            def create_table():
                create_tmp = SnowflakeOperator(
                    task_id="create_tmp",
                    snowflake_conn_id=SNOWFLAKE_CONN_ID,
                    schema="PRODUCT",
                    sql=PRODUCT_PRIMARY_SQL_PATHS["create_tmp"],
                    params={"table": table},
                    queue="heavy-etl",
                )

                @task_group
                def test_tmp():
                    SQLColumnCheckOperator(
                        task_id="test_cols",
                        conn_id=SNOWFLAKE_CONN_ID,
                        table=table,
                        column_mapping=product_tables_column_checks_test[table],
                    )

                    SQLTableCheckOperator(
                        task_id="test_table",
                        conn_id=SNOWFLAKE_CONN_ID,
                        table=table,
                        checks=product_tables_table_checks_test[table],
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
                    sql=PRODUCT_PRIMARY_SQL_PATHS["swap"],
                    params={"table": table},
                    queue="heavy-etl",
                )

                @task_group
                def add_docs():
                    SnowflakeOperator(
                        task_id="document_table",
                        snowflake_conn_id=SNOWFLAKE_CONN_ID,
                        sql=PRODUCT_PRIMARY_SQL_PATHS["document_table"],
                        schema="PRODUCT",
                        params={"table": table},
                    )

                    SnowflakeOperator(
                        task_id="document_fields",
                        snowflake_conn_id=SNOWFLAKE_CONN_ID,
                        sql=PRODUCT_PRIMARY_SQL_PATHS["document_fields"],
                        schema="PRODUCT",
                        params={"table": table},
                    )

                    set_data_types = TypeSetOperator(
                        task_id="set_data_types",
                        schema="PRODUCT",
                        table=table,
                        type_map=product_tables_data_types[table],
                    )

                    set_primary_key = PrimaryKeyOperator(
                        task_id="set_primary_key",
                        schema="PRODUCT",
                        table=table,
                        primary_key=product_tables_primary_key_list[table],
                    )

                    chain(set_data_types, set_primary_key)

                drop_tmp = SnowflakeOperator(
                    task_id="drop_tmp",
                    snowflake_conn_id=SNOWFLAKE_CONN_ID,
                    sql=PRODUCT_PRIMARY_SQL_PATHS["drop_tmp"],
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
                        column_mapping=product_tables_column_checks_validate[
                            table
                        ],
                    )

                    test_table = SQLTableCheckOperator(
                        task_id="test_table",
                        table=table,
                        checks=product_tables_table_checks_validate[table],
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

            model_web_task_groups[table] = create_table_obj

    product_downstream_tables_task_groups = {}

    for table in product_downstream_tables:

        @task_group(
            group_id=table,
        )
        def create_table():
            create_tmp = SnowflakeOperator(
                task_id="create_tmp",
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                schema="PRODUCT",
                sql=PRODUCT_PRIMARY_SQL_PATHS["create_tmp"],
                params={"table": table},
                queue="heavy-etl",
            )

            @task_group
            def test_tmp():
                SQLColumnCheckOperator(
                    task_id="test_cols",
                    conn_id=SNOWFLAKE_CONN_ID,
                    table=table,
                    column_mapping=product_tables_column_checks_test[table],
                )

                SQLTableCheckOperator(
                    task_id="test_table",
                    conn_id=SNOWFLAKE_CONN_ID,
                    table=table,
                    checks=product_tables_table_checks_test[table],
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
                sql=PRODUCT_PRIMARY_SQL_PATHS["swap"],
                params={"table": table},
                queue="heavy-etl",
            )

            @task_group
            def add_docs():
                SnowflakeOperator(
                    task_id="document_table",
                    snowflake_conn_id=SNOWFLAKE_CONN_ID,
                    sql=PRODUCT_PRIMARY_SQL_PATHS["document_table"],
                    schema="PRODUCT",
                    params={"table": table},
                )

                SnowflakeOperator(
                    task_id="document_fields",
                    snowflake_conn_id=SNOWFLAKE_CONN_ID,
                    sql=PRODUCT_PRIMARY_SQL_PATHS["document_fields"],
                    schema="PRODUCT",
                    params={"table": table},
                )

                set_data_types = TypeSetOperator(
                    task_id="set_data_types",
                    schema="PRODUCT",
                    table=table,
                    type_map=product_tables_data_types[table],
                )

                set_primary_key = PrimaryKeyOperator(
                    task_id="set_primary_key",
                    schema="PRODUCT",
                    table=table,
                    primary_key=product_tables_primary_key_list[table],
                )

                chain(set_data_types, set_primary_key)

            drop_tmp = SnowflakeOperator(
                task_id="drop_tmp",
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                sql=PRODUCT_PRIMARY_SQL_PATHS["drop_tmp"],
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
                    column_mapping=product_tables_column_checks_validate[table],
                    partition_clause="WHERE _PARTITIONDATE = CURRENT_DATE()",
                )

                test_table = SQLTableCheckOperator(
                    task_id="test_table",
                    table=table,
                    checks=product_tables_table_checks_validate[table],
                    partition_clause="WHERE _PARTITIONDATE = CURRENT_DATE()",
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

        product_downstream_tables_task_groups[table] = create_table_obj

    chain_linear(
        [
            wait_for_DAG_ingest_thanos,
            wait_for_DAG_commons,
            wait_for_DAG_calendar_tables,
        ],
        [
            wait_for_last_product_hourly,
            wait_for_last_ingest_segment,
            wait_for_last_ingest_splunk,
        ],
        model_web(),
        dagset_done,
    )

    chain(
        product_primary_tables_task_groups["node_pools"],
        product_downstream_tables_task_groups["worker_queues"],
    )

    chain(
        product_primary_tables_task_groups["deployment_operator_log"],
        product_downstream_tables_task_groups["task_runs"],
        [
            product_downstream_tables_task_groups["dag_summary"],
            product_downstream_tables_task_groups["org_events"],
        ],
    )

    chain(
        product_primary_tables_task_groups["ide_builtin_cell_types"],
        [
            product_downstream_tables_task_groups["ide_cell_runs"],
            product_downstream_tables_task_groups["ide_cells"],
        ],
        product_downstream_tables_task_groups["org_events"],
    )

    chain(
        product_primary_tables_task_groups["code_pushes"],
        product_downstream_tables_task_groups["org_events"],
    )

    for table_task_group in product_primary_tables_task_groups.values():
        if not table_task_group.downstream_task_ids:
            chain(table_task_group, dagset_done)

    for table_task_group in product_downstream_tables_task_groups.values():
        if not table_task_group.downstream_task_ids:
            chain(table_task_group, dagset_done)


product()
