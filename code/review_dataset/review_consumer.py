import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from decimal import Decimal

# Define Kafka topic and server configuration
KAFKA_TOPIC_NAME = "reviews"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Define the schema for the incoming Kafka messages
schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("business_id", StringType(), True),
    StructField("stars", IntegerType(), True),
    StructField("date", StringType(), True),  # Assuming date is stored as string
    StructField("useful", IntegerType(), True),
    StructField("funny", IntegerType(), True),
    StructField("cool", IntegerType(), True)
])


# Define the DynamoDB table name and client
DYNAMODB_TABLE_NAME = "review_table"
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

# Define a UDF to convert float values to Decimal
decimal_udf = udf(lambda value: Decimal(value) if isinstance(value, float) else value)

# Define the micro-batch write function
def write_to_dynamodb(rows):

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
    .appName("ReviewsKafkaConsumer") \
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
