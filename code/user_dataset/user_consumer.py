import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, size, split
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from decimal import Decimal

# Define Kafka topic and server configuration
KAFKA_TOPIC_NAME = "users"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Define the schema for the incoming Kafka messages
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("review_count", IntegerType(), True),
    StructField("useful", IntegerType(), True),
    StructField("funny", IntegerType(), True),
    StructField("cool", IntegerType(), True),
    StructField("friends", StringType(), True),
    StructField("fans", IntegerType(), True),
    StructField("average_stars", DoubleType(), True),
    StructField("compliment_hot", IntegerType(), True),
    StructField("compliment_more", IntegerType(), True),
    StructField("compliment_profile", IntegerType(), True),
    StructField("compliment_cute", IntegerType(), True),
    StructField("compliment_list", IntegerType(), True),
    StructField("compliment_note", IntegerType(), True),
    StructField("compliment_plain", IntegerType(), True),
    StructField("compliment_cool", IntegerType(), True),
    StructField("compliment_funny", IntegerType(), True),
    StructField("compliment_writer", IntegerType(), True),
    StructField("compliment_photos", IntegerType(), True)
])

# Define the DynamoDB table name and client
DYNAMODB_TABLE_NAME = "user_table"
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

# Define a UDF to convert float values to Decimal
decimal_udf = udf(lambda value: Decimal(value) if isinstance(value, float) else value)

# Define the micro-batch write function
def write_to_dynamodb(rows):
    # Apply the UDF to convert float values to Decimal
    rows = rows.withColumn("average_stars", decimal_udf("average_stars"))
    # Calculate friends_count and drop the friends column
    rows = rows.withColumn(
        "friends_count",
        size(split(col("friends"), ","))
    ).drop("friends")

    # Loop through each data row and write to DynamoDB
    with table.batch_writer() as batch:
        for row in rows.collect():
            batch.put_item(Item=row.asDict())

# Define the custom trigger and write function
def trigger_and_write(df, epoch_id):
    # If there are rows, write to DynamoDB after ensuring user_id uniqueness
    if not df.rdd.isEmpty():
        # Ensure user_id uniqueness within the micro-batch
        unique_df = df.dropDuplicates(["user_id"])

        # Check if there are duplicates
        if df.count() != unique_df.count():
            print("Warning: Duplicate user_id detected within the micro-batch. Skipping write to DynamoDB.")
        else:
            write_to_dynamodb(unique_df)


# Configure the Spark session with Kafka libraries
spark = SparkSession.builder \
    .appName("UsersKafkaConsumer") \
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
