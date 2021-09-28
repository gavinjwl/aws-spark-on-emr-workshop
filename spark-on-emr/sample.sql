CREATE DATABASE IF NOT EXISTS ${target_database};

DROP TABLE IF EXISTS ${target_database}.${target_table};

CREATE TABLE ${target_database}.${target_table}
USING PARQUET
PARTITIONED BY (_year, _month, _day, _hour)
LOCATION '${target_host}/parquet/${target_table}'
TBLPROPERTIES ('mode'='overwrite')
AS (
    SELECT
        *,
        YEAR(lpep_pickup_datetime) AS _year,
        MONTH(lpep_pickup_datetime) AS _month,
        DAYOFMONTH(lpep_pickup_datetime) AS _day,
        HOUR(lpep_pickup_datetime) AS _hour
    FROM (
        SELECT
            VendorID,
            from_unixtime(to_unix_timestamp(lpep_pickup_datetime, 'M/d/yy H:mm'), 'yyyy-MM-dd HH:mm:ss') AS lpep_pickup_datetime,
            from_unixtime(to_unix_timestamp(lpep_dropoff_datetime, 'M/d/yy H:mm'), 'yyyy-MM-dd HH:mm:ss') AS lpep_dropoff_datetime,
            store_and_fwd_flag,
            RatecodeID,
            PULocationID,
            DOLocationID,
            passenger_count,
            trip_distance,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            COALESCE(ehail_fee, '__NULL__') AS ehail_fee,
            improvement_surcharge,
            total_amount,
            payment_type,
            trip_type
        FROM ${source_database}.${source_table}
    )
    WHERE
        lpep_pickup_datetime IS NOT NULL AND lpep_dropoff_datetime IS NOT NULL
);

DESCRIBE TABLE ${target_database}.${target_table};
