import pyspark.sql.types as T

INPUT_DATA_PATH_GREEN = '../../resources/green_tripdata_2019-01.csv'
INPUT_DATA_PATH_FHV = '../../resources/fhv_tripdata_2019-01.csv'
BOOTSTRAP_SERVERS = 'localhost:9092'

TOPIC_RIDES_ALL = 'rides_all '

PRODUCE_TOPIC_GREEN_RIDES = CONSUME_TOPIC_GREEN_RIDES = 'rides_green'
PRODUCE_TOPIC_FHV_RIDES = CONSUME_TOPIC_FHV_RIDES = 'rides_fhv'

RIDE_SCHEMA = T.StructType(
    [
        T.StructField("vendor_id", T.IntegerType()),
        T.StructField("PULocationID", T.IntegerType())
    ]
)
