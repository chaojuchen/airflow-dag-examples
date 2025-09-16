from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
import psycopg2

def test_postgres_conn():
    # Get Airflow connection
    conn = BaseHook.get_connection("tangram_sql")
    # Build connection string
    conn_str = f"host={conn.host} port={conn.port or 5432} dbname={conn.schema} user={conn.login} password={conn.password}"
    # Connect and run query
    with psycopg2.connect(conn_str) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()
            print("Postgres version:", version)

with DAG(
    dag_id="test_postgres_conn_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["test"],
) as dag:
    test_postgres_conn_task = PythonOperator(
        task_id="test_postgres_conn",
        python_callable=test_postgres_conn,
    )