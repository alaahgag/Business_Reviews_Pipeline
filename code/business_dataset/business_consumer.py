import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, col, when, split, explode, sum, lit,  expr, col, array
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from decimal import Decimal

# Define Kafka topic and server configuration
KAFKA_TOPIC_NAME = "business"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Define the schema for the incoming Kafka messages
schema = StructType([
    StructField("business_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("stars", DoubleType(), True),
    StructField("review_count", IntegerType(), True),
    StructField("is_open", IntegerType(), True),
    StructField("categories", StringType(), True),
])

# Define the DynamoDB table name and client
DYNAMODB_TABLE_NAME = "business_table"
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

# Define a UDF to convert float values to Decimal
decimal_udf = udf(lambda value: Decimal(value) if isinstance(value, float) else value)

# Define the micro-batch write function
def write_to_dynamodb(rows):
    # Apply the UDF to convert float values to Decimal
    rows = rows.withColumn("latitude", decimal_udf("latitude"))
    rows = rows.withColumn("longitude", decimal_udf("longitude"))
    rows = rows.withColumn("stars", decimal_udf("stars"))

    state_mapping = {
        "XMS": "Some_State_Name_XMS",
        "WA": "Washington",
        "VT": "Vermont",
        "VI": "Virgin Islands",
        "UT": "Utah",
        "TX": "Texas",
        "TN": "Tennessee",
        "SD": "South Dakota",
        "PA": "Pennsylvania",
        "NV": "Nevada",
        "NJ": "New Jersey",
        "NC": "North Carolina",
        "MT": "Montana",
        "MO": "Missouri",
        "MI": "Michigan",
        "MA": "Massachusetts",
        "LA": "Louisiana",
        "IN": "Indiana",
        "IL": "Illinois",
        "ID": "Idaho",
        "HI": "Hawaii",
        "FL": "Florida",
        "DE": "Delaware",
        "CO": "Colorado",
        "CA": "California",
        "AZ": "Arizona",
        "AB": "Alberta",
    }

    # Convert state_mapping keys to a list
    valid_states = list(state_mapping.keys())

    # Apply the state mapping using expr and array
    rows = rows.withColumn("state", expr("CASE WHEN state IN ({}) THEN {} ELSE state END".format(
        ", ".join(["'{}'".format(state) for state in valid_states]),
        "CASE WHEN state IN ({}) THEN {} ELSE state END".format(
            ", ".join(["'{}'".format(state) for state in valid_states]),
            "state"
        )
    )))


    # Split and explode categories
    rows = rows.withColumn("categories_array", split(col("categories"), ", "))
    rows = rows.select("business_id", "name", "city", "state", "latitude", "longitude", "stars", "review_count", "is_open", explode("categories_array").alias("category"))

    # Filter out rows where latitude is null
    rows = rows.filter(col("latitude").isNotNull())

    # Drop duplicates
    rows = rows.dropDuplicates(["business_id"])


    # Loop through each data row and write to DynamoDB
    with table.batch_writer() as batch:
        for row in rows.collect():
            batch.put_item(Item=row.asDict())

# Define the custom trigger and write function
def trigger_and_write(df, epoch_id):
    # If there are rows, write to DynamoDB
    if not df.rdd.isEmpty():
        write_to_dynamodb(df)

# Configure the Spark session with Kafka libraries
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.kafka:kafka-clients:2.8.0") \
    .getOrCreate()

# Read data from Kafka and parse JSON messages
parsed_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC_NAME) \
    .option("failOnDataLoss", "false")\
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Start the streaming query with the custom trigger function
query = parsed_df.writeStream.foreachBatch(trigger_and_write).option("checkpointLocation", "/tmp/checkpoint").start()

# Wait for the streaming query to finish
query.awaitTermination()
