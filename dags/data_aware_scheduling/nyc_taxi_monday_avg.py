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
    # cleanup_day_rides = SQLExecuteQueryOperator(
    #     task_id='cleanup_day_rides',
    #     conn_id='tangram_sql',
    #     sql="DROP TABLE IF EXISTS iceberg.demo.day_rides;",
    # )

    # cleanup_zone_earnings = SQLExecuteQueryOperator(
    #     task_id='cleanup_zone_earnings',
    #     conn_id='tangram_sql',
    #     sql="DROP TABLE IF EXISTS iceberg.demo.zone_earnings;",
    # )

    # cleanup_driving_stats = SQLExecuteQueryOperator(
    #     task_id='cleanup_driving_stats',
    #     conn_id='tangram_sql',
    #     sql="DROP TABLE IF EXISTS iceberg.demo.day_driving_time_distance;",
    # )


    # Create a view for rides on the specified day of week
    create_day_rides_table = SQLExecuteQueryOperator(
        task_id='create_day_rides_table',
        conn_id='tangram_sql',
        sql="""
        CREATE TABLE iceberg.demo.day_rides AS
        SELECT *
        FROM iceberg.demo.nyc_yellow_taxi_trips
        WHERE EXTRACT(DOW FROM tpep_pickup_datetime) = {{ params.day_of_week }};
        """,
    )

    # Create a view for earnings by zone
    zone_earnings_table = SQLExecuteQueryOperator(
        task_id='create_zone_earnings_table',
        conn_id='tangram_sql',
        sql="""
        CREATE TABLE iceberg.demo.zone_earnings AS
        SELECT 
            PULocationID,
            COUNT(*) AS num_trips,
            SUM(total_amount) AS total_earnings,
            AVG(total_amount) AS avg_earnings_per_trip
        FROM iceberg.demo.day_rides
        GROUP BY PULocationID;
        """,
    )

    create_zone_driving_stats_table = SQLExecuteQueryOperator(
        task_id='create_zone_driving_stats_table',
        conn_id='tangram_sql',
        sql="""
        CREATE TABLE iceberg.demo.zone_driving_stats (
            PULocationID INT,
            zone STRING,
            num_trips BIGINT,
            avg_distance_per_trip DOUBLE,
            avg_driving_time_minutes DOUBLE
        );
        """
    )

    # Branch: Calculate total driving time and distance for the day
    calculate_zone_driving_metrics = SQLExecuteQueryOperator(
        task_id='calculate_zone_driving_metrics',
        conn_id='tangram_sql',
        sql="""
        INSERT INTO iceberg.demo.zone_driving_stats
        SELECT
            PULocationID,
            l.zone,
            COUNT(*) AS num_trips,
            AVG(trip_distance) AS avg_distance_per_trip,
            AVG(tpep_dropoff_datetime - tpep_pickup_datetime) / 60.0 AS avg_driving_time_minutes
        FROM iceberg.demo.day_rides dr
        JOIN iceberg.demo.taxi_zone_lookup l
          ON dr.PULocationID = l.LocationID
        GROUP BY PULocationID, l.zone;
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

    # [cleanup_day_rides, cleanup_zone_earnings, cleanup_driving_stats] >> 
    create_day_rides_table >> [zone_earnings_table, create_zone_driving_stats_table] >> calculate_zone_driving_metrics >>
    zone_earnings_table >> top_10_zones_query