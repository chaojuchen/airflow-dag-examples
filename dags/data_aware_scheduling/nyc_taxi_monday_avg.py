from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id='nyc_monday_zone_earnings_postgres',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["nyc", "postgres", "sql"]
) as dag:

    # Step 1: Filter for Mondays (creates a temporary table)
    monday_rides = PostgresOperator(
        task_id='create_monday_rides_table',
        postgres_conn_id='tangram_sql',
        sql="""
        DROP TABLE IF EXISTS monday_rides;
        CREATE TEMP TABLE monday_rides AS
        SELECT *
        FROM nyc_yellow_taxi_trips
        WHERE EXTRACT(DOW FROM tpep_pickup_datetime) = 1;
        """,
    )

    # Step 2: Calculate earnings by zone (creates a temporary table)
    zone_earnings = PostgresOperator(
        task_id='create_zone_earnings_table',
        postgres_conn_id='tangram_sql',
        sql="""
        DROP TABLE IF EXISTS zone_earnings;
        CREATE TEMP TABLE zone_earnings AS
        SELECT 
            PULocationID,
            COUNT(*) AS num_trips,
            SUM(total_amount) AS total_earnings,
            AVG(total_amount) AS avg_earnings_per_trip
        FROM monday_rides
        GROUP BY PULocationID;
        """,
    )

    # Step 3: Join with zone names and return the top 10 zones
    top_10_zones_query = PostgresOperator(
        task_id='get_top_10_zones',
        postgres_conn_id='tangram_sql',
        sql="""
        SELECT 
            z.PULocationID,
            l.Zone,
            l.Borough,
            z.num_trips,
            z.total_earnings,
            z.avg_earnings_per_trip
        FROM zone_earnings z
        JOIN taxi_zone_lookup l
          ON z.PULocationID = l.LocationID
        ORDER BY z.total_earnings DESC
        LIMIT 10;
        """,
    )

    monday_rides >> zone_earnings >> top_10_zones_query