from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from utils import TangramSQLExecutionOperator
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
    params={"day_of_week": 1, "tangram_workspace": "demo"},  # Default: 1=Monday, override when triggering DAG
) as dag:
    cleanup_day_rides = TangramSQLExecutionOperator(
        task_id='cleanup_day_rides',
        conn_id='tangram_sql',
        sql="DROP TABLE IF EXISTS iceberg.demo.day_rides;",
        tangram_workspace="{{ params.tangram_workspace }}"
    )

    cleanup_zone_earnings = TangramSQLExecutionOperator(
        task_id='cleanup_zone_earnings',
        conn_id='tangram_sql',
        sql="DROP TABLE IF EXISTS iceberg.demo.zone_earnings;",
        tangram_workspace="{{ params.tangram_workspace }}"
    )

    cleanup_zone_driving_stats = TangramSQLExecutionOperator(
        task_id='cleanup_zone_driving_stats',
        conn_id='tangram_sql',
        sql="DROP TABLE IF EXISTS iceberg.demo.zone_driving_stats;",
        tangram_workspace="{{ params.tangram_workspace }}"
    )


    # Create a view for rides on the specified day of week
    create_day_rides_table = TangramSQLExecutionOperator(
        task_id='create_day_rides_table',
        conn_id='tangram_sql',
        sql=f"""
        CREATE TABLE IF NOT EXISTS iceberg.demo.day_rides AS
        SELECT *, {{ params.day_of_week }} as day_of_week
        FROM iceberg.demo.nyc_yellow_taxi_trips
        WHERE EXTRACT(DOW FROM tpep_pickup_datetime) = {{ params.day_of_week }};
        """,
        tangram_workspace="{{ params.tangram_workspace }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Create a view for earnings by zone
    zone_earnings_table = TangramSQLExecutionOperator(
        task_id='create_zone_earnings_table',
        conn_id='tangram_sql',
        sql=f"""
        CREATE TABLE IF NOT EXISTS iceberg.demo.zone_earnings AS
        SELECT
            day_of_week,
            PULocationID,
            COUNT(*) AS num_trips,
            SUM(total_amount) AS total_earnings,
            AVG(total_amount) AS avg_earnings_per_trip
        FROM iceberg.demo.day_rides
        GROUP BY day_of_week, PULocationID;
        """,
        tangram_workspace="{{ params.tangram_workspace }}",
    )

    create_zone_driving_stats_table = TangramSQLExecutionOperator(
        task_id='create_zone_driving_stats_table',
        conn_id='tangram_sql',
        sql=f"""
        CREATE TABLE IF NOT EXISTS iceberg.demo.zone_driving_stats (
            day_of_week INT,
            PULocationID INT,
            zone STRING,
            num_trips BIGINT,
            avg_distance_per_trip DOUBLE,
            avg_driving_time_minutes DOUBLE
        );
        """,
        tangram_workspace="{{ params.tangram_workspace }}",
    )

    # Branch: Calculate total driving time and distance for the day
    calculate_zone_driving_metrics = TangramSQLExecutionOperator(
        task_id='calculate_zone_driving_metrics',
        conn_id='tangram_sql',
        sql=f"""
        INSERT INTO iceberg.demo.zone_driving_stats
        SELECT
            dr.day_of_week,
            dr.PULocationID,
            l.zone,
            COUNT(*) AS num_trips,
            AVG(trip_distance) AS avg_distance_per_trip,
            AVG(DATE_DIFF('second', tpep_pickup_datetime, tpep_dropoff_datetime)) / 60.0 AS avg_driving_time_minutes
        FROM iceberg.demo.day_rides dr
        JOIN iceberg.demo.taxi_zone_lookup l
          ON dr.PULocationID = l.LocationID
        GROUP BY dr.day_of_week, dr.PULocationID, l.zone;
        """,
        tangram_workspace="{{ params.tangram_workspace }}",
    )

    # Join with zone names and return the top 10 zones
    top_10_zones_query = TangramSQLExecutionOperator(
        task_id='get_top_10_zones',
        conn_id='tangram_sql',
        sql=f"""
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
        tangram_workspace="{{ params.tangram_workspace }}",
    )

    [cleanup_day_rides, cleanup_zone_earnings, cleanup_zone_driving_stats] >> create_day_rides_table >> [zone_earnings_table, create_zone_driving_stats_table]
    zone_earnings_table >> top_10_zones_query
    create_zone_driving_stats_table >> calculate_zone_driving_metrics