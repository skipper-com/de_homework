from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATH_GREEN, INPUT_DATA_PATH_FHV,\
    PRODUCE_TOPIC_GREEN_RIDES, PRODUCE_TOPIC_FHV_RIDES, CONSUME_TOPIC_GREEN_RIDES, CONSUME_TOPIC_FHV_RIDES, RIDE_SCHEMA, TOPIC_RIDES_ALL


def read_from_kafka(consume_topic: str):
    # Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .option("subscribe", consume_topic) \
        .option("startingOffsets", "earliest") \
        .option("checkpointLocation", "checkpoint") \
        .load()
    return df_stream


def parse_ride_from_kafka_message(df, schema):
    """ take a Spark Streaming df and parse value col based on <schema>, return streaming df cols in schema """
    assert df.isStreaming is True, "DataFrame doesn't receive streaming data"

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # split attributes to nested array in one Column
    col = F.split(df['value'], ', ')

    # expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return df.select([field.name for field in schema])

def sink_console(df, output_mode: str = 'complete', processing_time: str = '5 seconds'):
    write_query = df.writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime=processing_time) \
        .format("console") \
        .option("truncate", False) \
        .start()
    return write_query  # pyspark.sql.streaming.StreamingQuery

# union df
def concat_df(df1, df2):
    result_df = df1.union(df2)
    return result_df

# def sink_memory(df, query_name, query_template):
#     query_df = df \
#         .writeStream \
#         .queryName(query_name) \
#         .format("memory") \
#         .start()
#     query_str = query_template.format(table_name=query_name)
#     query_results = spark.sql(query_str)
#     return query_results, query_df


def sink_kafka(df, topic):
    write_query = df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .outputMode('append') \
        .option("topic", topic) \
        .option("checkpointLocation", "checkpoint") \
        .start()
    return write_query


def prepare_df_to_kafka_sink(df, value_columns, key_column=None):
    columns = df.columns

    df = df.withColumn("value", F.concat_ws(', ', *value_columns))
    if key_column:
        df = df.withColumnRenamed(key_column, "key")
        df = df.withColumn("key", df.key.cast('string'))
    return df.select(['key', 'value'])


def agg_popular(df, column_names):
    df_aggregation = df.groupBy(column_names).count().limit(1)
    return df_aggregation

# def op_windowed_groupby(df, window_duration, slide_duration):
#     df_windowed_aggregation = df.groupBy(
#         F.window(timeColumn=df.tpep_pickup_datetime, windowDuration=window_duration, slideDuration=slide_duration),
#         df.vendor_id
#     ).count()
#     return df_windowed_aggregation


if __name__ == "__main__":
    spark = SparkSession.builder.appName('streaming-examples').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # read_streaming data
    df_consume_stream_green = read_from_kafka(consume_topic=CONSUME_TOPIC_GREEN_RIDES)
    df_consume_stream_fhv = read_from_kafka(consume_topic=CONSUME_TOPIC_FHV_RIDES)
    print(df_consume_stream_green.printSchema())
    print(df_consume_stream_fhv.printSchema())

    # parse streaming data
    df_green_rides = parse_ride_from_kafka_message(df_consume_stream_green, RIDE_SCHEMA)
    df_fhv_rides = parse_ride_from_kafka_message(df_consume_stream_fhv, RIDE_SCHEMA)
    print(df_green_rides.printSchema())
    print(df_fhv_rides.printSchema())

    # write the output to the kafka topic
    df_rides_prepared = prepare_df_to_kafka_sink(df=df_green_rides.union(df_fhv_rides), value_columns=['PULocationID'], key_column='vendor_id')
    kafka_sink_query = sink_kafka(df=df_rides_prepared, topic=TOPIC_RIDES_ALL)
    
    # write the output out to the console for debugging / testin
    df_most_popular_loc_id = agg_popular(df_green_rides.union(df_fhv_rides), ['PULocationID'])
    sink_console(df_most_popular_loc_id)
    # df_trip_count_by_pickup_date_vendor_id = op_windowed_groupby(df_rides, window_duration="10 minutes", slide_duration='5 minutes')
    
    spark.streams.awaitAnyTermination()
