from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id='nyc_monday_zone_earnings',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["nyc", "sql", "generic"]
) as dag:

    # Step 1: Create a view for Monday rides
    monday_rides_view = SQLExecuteQueryOperator(
        task_id='create_monday_rides_view',
        conn_id='tangram_sql',
        sql="""
        DROP VIEW IF EXISTS monday_rides;
        CREATE VIEW monday_rides AS
        SELECT *
        FROM nyc_yellow_taxi_trips
        WHERE EXTRACT(DOW FROM tpep_pickup_datetime) = 1;
        """,
    )

    # Step 2: Create a view for earnings by zone
    zone_earnings_view = SQLExecuteQueryOperator(
        task_id='create_zone_earnings_view',
        conn_id='tangram_sql',
        sql="""
        DROP VIEW IF EXISTS zone_earnings;
        CREATE VIEW zone_earnings AS
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
    top_10_zones_query = SQLExecuteQueryOperator(
        task_id='get_top_10_zones',
        conn_id='tangram_sql',
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

    monday_rides_view >> zone_earnings_view >> top_10_zones_query