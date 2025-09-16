from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import psycopg2

def test_postgres_conn():
    conn = BaseHook.get_connection("tangram_sql")
    
    conn_str = f"host={conn.host} port={conn.port or 5432} dbname={conn.schema} user={conn.login} password={conn.password}"
    print(f"Connecting to Postgres with connection string: {conn_str}")
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            print(cur.fetchone())

with DAG(
    dag_id="test_postgres_conn_dag",
    start_date=datetime(2025, 9, 15),
    schedule="* * * * *",                  # every minute
    catchup=False,
    tags=["test"],
) as dag:
    test_postgres_conn_task = PythonOperator(
        task_id="test_postgres_conn",
        python_callable=test_postgres_conn,
    )