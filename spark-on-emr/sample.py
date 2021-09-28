import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, dayofmonth, from_unixtime, hour,
                                   isnull, month, unix_timestamp, year)


def main():

    source_database = sys.argv[1].strip() if len(sys.argv) > 1 else 'default'
    source_table = sys.argv[2].strip() if len(sys.argv) > 2 else 'tripdata'
    target_database = sys.argv[3].strip() if len(sys.argv) > 3 else 'curated'
    target_table = sys.argv[4].strip() if len(sys.argv) > 4 else 'tripdata'
    target_host = sys.argv[5].strip() if len(sys.argv) > 5 else ''

    print(f'source_database: "{source_database}"')
    print(f'source_table: "{source_table}"')
    print(f'target_database: "{target_database}"')
    print(f'target_table: "{target_table}"')
    print(f'target_host: "{target_host}"')

    spark = SparkSession.builder\
        .enableHiveSupport()\
        .appName('pyspark-sample').getOrCreate()

    spark.catalog.setCurrentDatabase(source_database)

    df = spark.read.table(source_table)

    df = df\
        .withColumn(
            'lpep_pickup_datetime',
            from_unixtime(unix_timestamp(
                col('lpep_pickup_datetime'), 'M/d/yy H:mm'))
        ).withColumn(
            'lpep_dropoff_datetime',
            from_unixtime(unix_timestamp(
                col('lpep_dropoff_datetime'), 'M/d/yy H:mm'))
        )

    df = df.na.drop(how='any', subset=['lpep_pickup_datetime', 'lpep_dropoff_datetime'])

    null_metadata = dict()

    for column in df.columns:
        count = df.filter(isnull(col(column))).count()
        if count > 0 and df.schema[column].dataType.typeName() == 'string':
            null_metadata[column] = '__NULL__'
        elif count > 0 and df.schema[column].dataType.typeName() in ['integer', 'double']:
            null_metadata[column] = -1

    print(null_metadata)

    df = df.na.fill(null_metadata)

    df = df\
        .withColumn('_year', year(col('lpep_pickup_datetime')))\
        .withColumn('_month', month(col('lpep_pickup_datetime')))\
        .withColumn('_day', dayofmonth(col('lpep_pickup_datetime')))\
        .withColumn('_hour', hour(col('lpep_pickup_datetime')))

    # df.write.parquet(
    #     f'{target_host}/parquet/{target_table}',
    #     mode='overwrite',
    #     partitionBy=['_year', '_month', '_day', '_hour'],
    #     compression='snappy'
    # )

    spark.sql(f'CREATE DATABASE IF NOT EXISTS {target_database}')
    df.write\
        .partitionBy('_year', '_month', '_day', '_hour')\
        .format('parquet')\
        .option('path', f'{target_host}/parquet/{target_table}')\
        .mode('overwrite')\
        .saveAsTable(f'{target_database}.{target_table}')

    tables = spark.catalog.listTables(target_database)

    print('Show tables')
    for tb in tables:
        print(tb, '\n')


if __name__ == '__main__':
    main()
