from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 7, 1),
}

with DAG(
    dag_id='nyc_day_zone_earnings',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["nyc", "sql", "generic"],
    params={"day_of_week": 1},  # Default: 1=Monday, override when triggering DAG
) as dag:

    # Create a view for rides on the specified day of week
    create_day_rides_view = SQLExecuteQueryOperator(
        task_id='create_day_rides_view',
        conn_id='tangram_sql',
        sql="""
        DROP VIEW IF EXISTS iceberg.demo.day_rides;
        CREATE VIEW iceberg.demo.day_rides AS
        SELECT *
        FROM iceberg.demo.nyc_yellow_taxi_trips
        WHERE EXTRACT(DOW FROM tpep_pickup_datetime) = {{ params.day_of_week }};
        """,
    )

    # Create a view for earnings by zone
    zone_earnings_view = SQLExecuteQueryOperator(
        task_id='create_zone_earnings_view',
        conn_id='tangram_sql',
        sql="""
        DROP VIEW IF EXISTS iceberg.demo.zone_earnings;
        CREATE VIEW iceberg.demo.zone_earnings AS
        SELECT 
            PULocationID,
            COUNT(*) AS num_trips,
            SUM(total_amount) AS total_earnings,
            AVG(total_amount) AS avg_earnings_per_trip
        FROM iceberg.demo.day_rides
        GROUP BY PULocationID;
        """,
    )

    # Branch: Calculate total driving time and distance for the day
    driving_time_distance_view = SQLExecuteQueryOperator(
        task_id='create_day_driving_time_distance_view',
        conn_id='tangram_sql',
        sql="""
        DROP VIEW IF EXISTS iceberg.demo.day_driving_time_distance;
        CREATE VIEW iceberg.demo.day_driving_time_distance AS
        SELECT
            PULocationID,
            DOLocationID,
            COUNT(*) AS num_trips,
            SUM(trip_distance) AS total_distance,
            AVG(trip_distance) AS avg_distance_per_trip,
            SUM(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))) AS total_driving_time_seconds,
            AVG(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))) AS avg_driving_time_seconds
        FROM iceberg.demo.day_rides
        GROUP BY PULocationID, DOLocationID;
        """,
    )

    # Join with zone names and return the top 10 zones
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
        FROM iceberg.demo.zone_earnings z
        JOIN iceberg.demo.taxi_zone_lookup l
          ON z.PULocationID = l.LocationID
        ORDER BY z.total_earnings DESC
        LIMIT 10;
        """,
    )

    create_day_rides_view >> [zone_earnings_view, driving_time_distance_view]
    zone_earnings_view >> top_10_zones_query