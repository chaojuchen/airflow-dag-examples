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
    params={"day_of_week": 1, "tangram_workspace": "demo", "conn_id": "tangram_sql"},  # Default: 1=Monday, override when triggering DAG
) as dag:
    cleanup_day_rides = TangramSQLExecutionOperator(
        task_id='cleanup_day_rides',
        conn_id="{{ params.conn_id }}",
        sql="DELETE FROM iceberg.demo.day_rides WHERE day_of_week = {{ params.day_of_week }};",
        retry_on_failure=False,  # Don't retry when the the cleanup operation fails(ex. table doesn't exist)
        tangram_workspace="{{ params.tangram_workspace }}"
    )

    cleanup_zone_earnings = TangramSQLExecutionOperator(
        task_id='cleanup_zone_earnings',
        conn_id="{{ params.conn_id }}",
        sql="DELETE FROM iceberg.demo.zone_earnings WHERE day_of_week = {{ params.day_of_week }};",
        retry_on_failure=False,
        tangram_workspace="{{ params.tangram_workspace }}"
    )

    cleanup_zone_driving_stats = TangramSQLExecutionOperator(
        task_id='cleanup_zone_driving_stats',
        conn_id="{{ params.conn_id }}",
        sql="DELETE FROM iceberg.demo.zone_driving_stats WHERE day_of_week = {{ params.day_of_week }};",
        retry_on_failure=False,
        tangram_workspace="{{ params.tangram_workspace }}"
    )

    # cleanup_top_zones = TangramSQLExecutionOperator(
    #     task_id='cleanup_top_zones',
    #     conn_id="{{ params.conn_id }}",
    #     sql="DELETE FROM iceberg.demo.top_zones_by_day WHERE day_of_week = {{ params.day_of_week }};",
    #     retry_on_failure=False,
    #     tangram_workspace="{{ params.tangram_workspace }}"
    # )

    # Create day_rides table schema
    create_day_rides_schema = TangramSQLExecutionOperator(
        task_id='create_day_rides_schema',
        conn_id="{{ params.conn_id }}",
        sql="""
        CREATE TABLE IF NOT EXISTS iceberg.demo.day_rides (
            VendorID BIGINT,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            RatecodeID DOUBLE,
            store_and_fwd_flag STRING,
            PULocationID BIGINT,
            DOLocationID BIGINT,
            payment_type BIGINT,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            congestion_surcharge DOUBLE,
            Airport_fee DOUBLE,
            day_of_week INT
        );
        """,
        tangram_workspace="{{ params.tangram_workspace }}",
    )

    # Insert data into day_rides table
    insert_day_rides_data = TangramSQLExecutionOperator(
        task_id='insert_day_rides_data',
        conn_id="{{ params.conn_id }}",
        sql="""
        INSERT INTO iceberg.demo.day_rides
        SELECT *, {{ params.day_of_week }} as day_of_week
        FROM iceberg.demo.nyc_yellow_taxi_trips
        WHERE EXTRACT(DOW FROM tpep_pickup_datetime) = {{ params.day_of_week }};
        """,
        tangram_workspace="{{ params.tangram_workspace }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_zone_earnings_schema = TangramSQLExecutionOperator(
        task_id='create_zone_earnings_schema',
        conn_id="{{ params.conn_id }}",
        sql="""
        CREATE TABLE IF NOT EXISTS iceberg.demo.zone_earnings (
            day_of_week INT,
            PULocationID INT,
            num_trips BIGINT,
            total_earnings DOUBLE,
            avg_earnings_per_trip DOUBLE
        );
        """,
        tangram_workspace="{{ params.tangram_workspace }}",
    )

    insert_zone_earnings_data = TangramSQLExecutionOperator(
        task_id='insert_zone_earnings_data',
        conn_id="{{ params.conn_id }}",
        sql="""
        INSERT INTO iceberg.demo.zone_earnings
        SELECT
            day_of_week,
            PULocationID,
            COUNT(*) AS num_trips,
            SUM(total_amount) AS total_earnings,
            AVG(total_amount) AS avg_earnings_per_trip
        FROM iceberg.demo.day_rides
        WHERE day_of_week = {{ params.day_of_week }}
        GROUP BY day_of_week, PULocationID;
        """,
        tangram_workspace="{{ params.tangram_workspace }}",
    )

    create_zone_driving_stats_table = TangramSQLExecutionOperator(
        task_id='create_zone_driving_stats_table',
        conn_id="{{ params.conn_id }}",
        sql="""
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
    insert_zone_driving_metrics = TangramSQLExecutionOperator(
        task_id='insert_zone_driving_metrics',
        conn_id="{{ params.conn_id }}",
        sql="""
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
        WHERE dr.day_of_week = {{ params.day_of_week }}
        GROUP BY dr.day_of_week, dr.PULocationID, l.zone;
        """,
        tangram_workspace="{{ params.tangram_workspace }}",
    )

    # create_top_zones_schema = TangramSQLExecutionOperator(
    #     task_id='create_top_zones_schema',
    #     conn_id="{{ params.conn_id }}",
    #     sql="""
    #     CREATE TABLE IF NOT EXISTS iceberg.demo.top_zones_by_day (
    #         day_of_week INT,
    #         PULocationID BIGINT,
    #         zone_name STRING,
    #         borough STRING,
    #         num_trips BIGINT,
    #         total_earnings DOUBLE,
    #         avg_earnings_per_trip DOUBLE,
    #         earnings_rank INT
    #     );
    #     """,
    #     tangram_workspace="{{ params.tangram_workspace }}",
    # )

    # Insert top 10 zones data into dedicated table
    # top_10_zones_query = TangramSQLExecutionOperator(
    # task_id='insert_top_10_zones',
    # conn_id="{{ params.conn_id }}",
    #     sql="""
    #     INSERT INTO iceberg.demo.top_zones_by_day
    #     SELECT 
    #         z.day_of_week,
    #         z.PULocationID,
    #         l.Zone as zone_name,
    #         l.Borough as borough,
    #         z.num_trips,
    #         z.total_earnings,
    #         z.avg_earnings_per_trip,
    #         ROW_NUMBER() OVER (ORDER BY z.total_earnings DESC) as earnings_rank
    #     FROM iceberg.demo.zone_earnings z
    #     JOIN iceberg.demo.taxi_zone_lookup l
    #       ON z.PULocationID = l.LocationID
    #     WHERE z.day_of_week = {{ params.day_of_week }}
    #     ORDER BY z.total_earnings DESC
    #     LIMIT 10;
    #     """,
    #     tangram_workspace="{{ params.tangram_workspace }}",
    # )

    from airflow.utils.helpers import chain

    schema_tasks = [create_day_rides_schema, create_zone_earnings_schema, create_zone_driving_stats_table]
    cleanup_tasks = [cleanup_day_rides, cleanup_zone_earnings, cleanup_zone_driving_stats]

    # Set up dependencies: all schemas (parallel) -> all cleanups (parallel) -> insert_day_rides_data
    chain(schema_tasks, cleanup_tasks, insert_day_rides_data)

    # Insert data in parallel branches after day_rides data is inserted
    insert_day_rides_data >> [insert_zone_earnings_data, insert_zone_driving_metrics]