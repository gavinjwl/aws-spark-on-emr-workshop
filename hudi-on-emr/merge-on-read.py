import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (col, dayofmonth, from_unixtime, hour, month,
                                   row_number, unix_timestamp, year)

if __name__ == "__main__":

    if (len(sys.argv) != 2):
        print("Usage: merge-on-read [bucket_name]")
        sys.exit(0)

    spark = SparkSession\
        .builder\
        .appName("merge-on-read")\
        .getOrCreate()

    raw = spark.read.csv(
        's3://aws-data-analytics-blog/emrimmersionday/tripdata.csv',
        header=True,
        inferSchema=True,
    )

    raw.cache()

    df = raw.na.fill(
        {'ehail_fee': '0'}
    ).withColumn(
        "ehail_fee", col("ehail_fee").cast("int")
    ).withColumn(
        'lpep_pickup_datetime',
        from_unixtime(unix_timestamp(
            col('lpep_pickup_datetime'), 'M/d/yy H:mm'))
    ).withColumn(
        'lpep_dropoff_datetime',
        from_unixtime(unix_timestamp(
            col('lpep_dropoff_datetime'), 'M/d/yy H:mm'))
    ).withColumn(
        'lpep_pickup_datetime', col(
            'lpep_pickup_datetime').cast('timestamp')
    ).withColumn(
        'lpep_dropoff_datetime', col(
            'lpep_dropoff_datetime').cast('timestamp')
    ).withColumn(
        '_year', year(col('lpep_pickup_datetime'))
    ).withColumn(
        '_month', month(col('lpep_pickup_datetime'))
    ).withColumn(
        '_day', dayofmonth(col('lpep_pickup_datetime'))
    ).withColumn(
        '_hour', hour(col('lpep_pickup_datetime'))
    ).orderBy(
        ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    ).withColumn(
        "id", row_number().over(Window().orderBy(
            ['lpep_pickup_datetime', 'lpep_dropoff_datetime']))
    )

    df.select(
        'id', 'VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', '_year', '_month', '_day', '_hour', 'ehail_fee',
        'passenger_count', 'trip_distance'
    ).show(10)

    empty_df = spark.createDataFrame(
        spark.sparkContext.emptyRDD(),
        df.schema
    )

    database_name = 'default'
    table_name = 'mor_tripdata'
    table_type = 'MERGE_ON_READ'

    bucket_name = sys.argv[1]
    base_path = f's3://{bucket_name}/hudi/{table_name}'

    # hoodie options
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.table.type': table_type,
        'hoodie.metadata.enable': 'true',

        # Default Value: 3600, since v0.9.0
        'hoodie.compact.inline.max.delta.seconds': 3600,
        'hoodie.compact.inline.max.delta.commits': 10,        # Default Value: 10

        # Default Value: false, since v0.9.0
        'hoodie.cleaner.delete.bootstrap.base.file': 'false',
        'hoodie.cleaner.commits.retained': 10,                # Default Value: 10
        'hoodie.commits.archival.batch': 10,                  # Default Value: 10

        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.table.type': table_type,
        'hoodie.datasource.write.recordkey.field': 'id',
        'hoodie.datasource.write.partitionpath.field': '_year,_month,_day',
        'hoodie.datasource.write.precombine.field': 'lpep_pickup_datetime',
        'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.ComplexKeyGenerator',
        'hoodie.datasource.write.hive_style_partitioning': 'true',

        # default: 104857600 Bytes (100 MB)
        'hoodie.parquet.small.file.limit': 104857600,
        # default: 125829120 Bytes (120 MB)
        'hoodie.parquet.max.file.size': 125829120,
        # default: 125829120 Bytes (120 MB)
        'hoodie.parquet.block.size': 125829120,
        # default: 1048576 Bytes (1 MB)
        'hoodie.parquet.page.size': 1048576,
        'hoodie.parquet.compression.codec': 'snappy',

        'hoodie.datasource.hive_sync.enable': 'true',
        'hoodie.datasource.hive_sync.database': database_name,
        'hoodie.datasource.hive_sync.table': table_name,
        'hoodie.datasource.hive_sync.partition_fields': '_year,_month,_day',
        'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',

        'hoodie.insert.shuffle.parallelism': 2,
        'hoodie.upsert.shuffle.parallelism': 2,
        'hoodie.bulkinsert.shuffle.parallelism': 2,
        'hoodie.delete.shuffle.parallelism': 2,
    }

    empty_df.write.format('hudi')\
        .options(**hudi_options)\
        .option('hoodie.datasource.write.operation', 'insert')\
        .mode("overwrite")\
        .save(base_path)

    df.write.format('hudi')\
        .options(**hudi_options)\
        .option('hoodie.datasource.write.operation', 'upsert')\
        .mode("append")\
        .save(base_path)

    tripsSnapshotDF = spark.read.format("hudi")\
        .load(f'{base_path}')

    print(tripsSnapshotDF.count())

    tripsSnapshotDF.printSchema()
