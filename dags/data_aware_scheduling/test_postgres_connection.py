from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import psycopg2

def test_postgres_conn():
    conn = BaseHook.get_connection("tangram_sql")
    conn_str = f"host={conn.host} port={conn.port or 5432} dbname={conn.schema} user={conn.login} password={conn.password}"
    with psycopg2.connect(conn_str) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()
            print("Postgres version:", version)

with DAG(
    dag_id="test_postgres_conn_dag",
    start_date=datetime.now() - timedelta(days=1),   # dynamic start date: yesterday
    schedule="* * * * *",                  # every minute
    catchup=False,
    tags=["test"],
) as dag:
    test_postgres_conn_task = PythonOperator(
        task_id="test_postgres_conn",
        python_callable=test_postgres_conn,
    )