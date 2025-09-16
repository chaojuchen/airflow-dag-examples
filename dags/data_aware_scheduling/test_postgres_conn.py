from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.sdk import Connection
import psycopg2

@dag(
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["test"],
)
def test_postgres_conn_dag():
    @task
    def test_postgres_conn():
        # Get Airflow connection
        conn = Connection.get("sqlmesh_postgres")
        # Build connection string
        conn_str = f"host={conn.host} port={conn.port or 5432} dbname={conn.schema} user={conn.login} password={conn.password}"
        # Connect and run query
        with psycopg2.connect(conn_str) as pg_conn:
            with pg_conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()
                print("Postgres version:", version)
    test_postgres_conn()

test_postgres_conn_dag = test_postgres_conn_dag()
