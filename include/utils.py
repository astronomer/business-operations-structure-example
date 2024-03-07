import random

def df_to_gcs_callable():
    return "hi :)"

def get_thanos_parameters():
    """Select a random number of options from the given list."""
    options_list = [
        "thanos_daily_dagbag_size",
        "thanos_hourly_dagbag_size",
        "thanos_weekly_dagbag_size",
        "thanos_hourly_failure_counts",
        "thanos_daily_failure_counts",
        "thanos_weekly_failure_counts",
        "thanos_hourly_success_counts",
        "thanos_daily_success_counts",
        "thanos_weekly_success_counts",
    ]
    num_options = random.randint(2, 9)
    selected_options = random.sample(options_list, min(num_options, len(options_list)))
    return selected_options



api_ingestion_source_list = [
    "workspace_history",
    "user_history",
    "user_organization_history",
    "virtual_runtime_history",
    "worker_queue_history",
    "pii",
    "api_key_history",
    "runtime_history",
    "cell_history",
    "cell_type_history",
    "cluster_history",
    "deployment_history",
    "deployment_spec_history",
    "domain_history",
    "env_var_history",
    "environment_object",
    "environment_object_link",
    "node_pool_history",
    "organization_history",
    "organization_user_relation_history",
    "pipeline_history",
    "project_history",
    "project_vars_conns_history",
]

splunk_ingestion_source_list = [
    "pr_preview_cluster_history",
    "splunk_dag_run_finished",
    "splunk_task_instance_details",
    "splunk_task_instance_finished",
]


product_primary_tables = [
    "node_pools",
    "cluster_log",
    "dag_runs",
    "deployment_dags_only_log",
    "deployment_env_var_log",
    "deployment_env_vars",
    "deployment_executor_log",
    "deployment_runtime_log",
    "ide_cell_types",
    "ide_pipelines",
    "ide_project_commits",
    "deployment_operator_log",
    "ide_builtin_cell_types",
    "code_pushes",
    "ide_projects",
    "org_product_log",
    "org_product_tier_log",
    "thanos_daily_dagbag_size",
    "thanos_hourly_tasks",
    "worker_queue_log",
]

product_downstream_tables = [
    "worker_queues",
    "task_runs",
    "dag_summary",
    "ide_cell_runs",
    "ide_cells",
    "org_events",
]


all_product_tables = (
    product_primary_tables
    + product_downstream_tables
    + [
        "airflow_visits",
        "visits",
    ]
)

product_tables_column_checks_test = {
    table: {"a": 1} for table in all_product_tables
}

product_tables_column_checks_validate = {
    table: {"a": 1} for table in all_product_tables
}

product_tables_table_checks_test = {
    table: {"a": 1} for table in all_product_tables
}

product_tables_table_checks_validate = {
    table: {"a": 1} for table in all_product_tables
}

product_tables_primary_key_list = {
    table: "a" for table in all_product_tables
}

product_tables_data_types = {
    table: {"a": "varchar"} for table in all_product_tables
}


PRODUCT_PRIMARY_SQL_PATHS = {
    "create_tmp": "sql/create_table.sql",
    "swap": "sql/swap.sql",
    "drop_tmp": "sql/drop_table.sql",
    "document_table": "sql/document_table.sql",
    "document_fields": "sql/document_fields.sql",
}

metrics_tables = [
    "metrics_long",
    "deployment_activity_daily",
    "deployment_ops_daily",
    "deployment_tasks_daily",
    "deployment_tasks_monthly",
    "deployment_tasks_multi",
    "deployment_tasks_weekly",
    "feature_adoption_docs",
    "feature_adoption_multi",
    "org_daily",
    "org_monthly",
    "org_multi",
    "org_ops_daily",
    "org_users_daily",
    "org_weekly",
    "user_counts_daily",
    "user_org_counts_daily",
    "feature_adoption",
    "user_org_visits_daily",
    "user_workspace_airflow_daily",
    "user_workspace_ide_daily",
    "workspace_dates",
    "cluster_dates",
    "deployment_dates",
    "object_counts_multi",
    "org_dates",
]

metrics_tables_column_checks_test = {
    table: {"a": 1} for table in metrics_tables
}

metrics_tables_column_checks_validate = {
    table: {"a": 1} for table in metrics_tables
}

metrics_tables_table_checks_test = {
    table: {"a": 1} for table in metrics_tables
}

metrics_tables_table_checks_validate = {
    table: {"a": 1} for table in metrics_tables
}

metrics_tables_primary_key_list = {table: "a" for table in metrics_tables}

metrics_tables_data_types = {
    table: {"a": "varchar"} for table in metrics_tables
}

METRICS_SQL_PATHS = {
    "create_tmp": "sql/create_table.sql",
    "swap": "sql/swap.sql",
    "drop_tmp": "sql/drop_table.sql",
    "document_table": "sql/document_table.sql",
    "document_fields": "sql/document_fields.sql",
}
