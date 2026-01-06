from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "siddartha",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False
}

with DAG(
    dag_id="student_retention_databricks_orchestration",
    description="Triggers Databricks Workflow for Student Retention Medallion Pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,          
    catchup=False,
    tags=["student-retention", "databricks", "medallion", "orchestration"],
) as dag:

    trigger_databricks_pipeline = DatabricksRunNowOperator(
        task_id="trigger_databricks_medallion_job",
        databricks_conn_id="databricks_default",
        job_id=1005427844888227,
        polling_period_seconds=60
    )
