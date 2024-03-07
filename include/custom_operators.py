from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
import time
import random
from textwrap import dedent


class DfToGCSOperator(BaseOperator):
    """
    Operator for moving data into GCS.

    Users pass in a python_callable that returns a dataframe,
    and the operator takes care of the rest.
    """

    ui_color = "#ee6da9"

    def __init__(
        self,
        task_id,
        python_callable,
        data_source,
        security_group,
        gcs_conn_id="gcs_analytics",
        bucket=None,
        table=None,
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
        **kwargs,
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.data_source = data_source
        self.bucket = bucket

    def execute(self, context):
        self.log.info(f"{self.data_source} was ingested into GCS bucket {self.bucket}.")


class LatestOnlyOperator(BaseOperator):
    """
    Skip tasks that are not running during the most recent schedule interval.

    If the task is run outside the latest schedule interval (i.e. external_trigger),
    all directly downstream tasks will be skipped.

    Note that downstream tasks are never skipped if the given DAG_Run is
    marked as externally triggered.
    """

    ui_color = "#e9ffdb"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        self.log.info("Downstream proceeds.")


class MigrationOperator(BaseOperator):
    """A class for handling table migrations."""

    ui_color = "#D1CAB0"

    def __init__(
        self,
        schema,
        table,
        col_types=None,
        migration_type="replace",
        fetch_col_types_from_xcom_task=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.table = table

    def execute(self, context):
        time.sleep(5)
        self.log.info(f"Table {self.table} was migrated.")


class SnowflakeOperator(BaseOperator):
    """Execute SQL in Snowflake."""

    ui_color = "#ededed"

    def __init__(
        self,
        *,
        sql,
        snowflake_conn_id: str = "snowflake_analytics",
        warehouse: str | None = None,
        database: str | None = None,
        role: str | None = None,
        schema: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.database = database
        self.schema = schema
        self.sql = sql

    def execute(self, context):
        self.log.info("Executing custom SQL in Snowflake.")
        time.sleep(random.randint(5, 20))
        self.log.info(f"SQL: {self.sql}")
        self.log.info(f"Database: {self.database}, Schema: {self.schema}")


class DagDoneOperator(BaseOperator):
    """An operator for making a generic dag__dagname dataset."""

    ui_color = "#41A146"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        print("The DAG is done!")


class ExternalTaskSensorAsync(BaseOperator):
    """
    Waits for a different DAG, task group, or task to complete for a specific logical date.
    """

    ui_color = "#19647e"

    def __init__(
        self,
        *,
        external_dag_id: str,
        external_task_id: str | None = None,
        external_task_ids=None,
        external_task_group_id=None,
        allowed_states=None,
        skipped_states=None,
        failed_states=None,
        execution_delta=None,
        execution_date_fn=None,
        check_existence: bool = False,
        poll_interval: float = 2.0,
        deferrable: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id

    def execute(self, context):
        time.sleep(5)
        self.log.info("DAG {self.external_dag_id} just ran.")


class LastRunSensor(BaseOperator):
    """A sensor for finding the last execution_date for a given DAG."""

    ui_color = "#DB7C76"

    def __init__(
        self,
        external_dag_id,
        external_task_id=None,
        external_task_ids=None,
        poke_interval=60,
        retries=0,
        timeout=2700,
        mode="poke",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id

    def execute(self, context):
        time.sleep(5)
        self.log.info(
            "Looking for the last logical_date of the DAG {self.external_dag_id}."
        )


class TestTableOperator(BaseOperator):
    """An operator for sanity checking tables with simple tests."""

    ui_color = "#FFA701"

    def __init__(self, location=None, schema=None, table=None, tests=None, **kwargs):
        super().__init__(**kwargs)
        self.table = table

    def execute(self, context):
        self.log.info(f"Table {self.table} was tested.")


class SQLColumnCheckOperator(BaseOperator):
    """An operator for running data quality checks on columns in a table."""

    def __init__(
        self,
        *,
        table: str,
        column_mapping: dict,
        partition_clause: str | None = None,
        conn_id: str | None = None,
        database: str | None = None,
        accept_none: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.table = table
        self.partition_clause = partition_clause

    def execute(self, context):
        time.sleep(random.randint(2, 5))
        if self.partition_clause and random.random() < 0.01:
            self.log.info(
                f"Table {self.table} was checked and failed a data quality check."
            )
            self.log.info(
                dedent(
                    """Results:
                    (-1, 789, 0)
                    The following tests have failed:
                    Column: record_version
                    Check: min,
                    Check Values: {'geq_to': 0, 'result': -1, 'success': False}"""
                )
            )
            raise AirflowException(
                "SQL Column Check failed please view in the data quality dashboard."
            )
        if self.table == "feature_adoption" and random.random() < 0.25:
            self.log.info(
                f"Table {self.table} was checked and failed a data quality check."
            )
            self.log.info(
                dedent(
                    """Results:
                    (-2, 1254, 0)
                    The following tests have failed:
                    Column: record_version
                    Check: min,
                    Check Values: {'geq_to': 0, 'result': -2, 'success': False}"""
                )
            )
            raise AirflowException(
                "SQL Column Check failed please view in the data quality dashboard."
            )
        self.log.info(f"Table {self.table} was checked.")


class SQLTableCheckOperator(BaseOperator):
    """An operator for running data quality checks on tables."""

    def __init__(
        self,
        *,
        table: str,
        checks: dict,
        partition_clause: str | None = None,
        conn_id: str | None = None,
        database: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.table = table
        self.partition_clause = partition_clause

    def execute(self, context):
        time.sleep(random.randint(2, 5))
        self.log.info(f"Table {self.table} was checked.")


class TypeSetOperator(BaseOperator):
    """Explicitly set the data types for a list of fields."""

    ui_color = "#E4DBC4"

    def __init__(
        self,
        task_id,
        schema,
        fields=None,
        type_map={"text": "varchar"},
        table=None,
        **kwargs,
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.schema = schema
        self.table = table

    def execute(self, context):
        time.sleep(random.randint(2, 5))
        self.log.info(f"Fields set for schema {self.schema} and table {self.table}.")


class PrimaryKeyOperator(BaseOperator):
    """Add a primary key constraint to an existing table."""

    ui_color = "#8b8dea"

    def __init__(self, task_id, schema, primary_key=None, table=None, **kwargs):
        super().__init__(task_id=task_id, **kwargs)
        self.schema = schema
        self.table = table

    def execute(self, context):
        time.sleep(random.randint(2, 5))
        self.log.info(
            f"Primary key set for schema {self.schema} and table {self.table}."
        )
