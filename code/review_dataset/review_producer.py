from kafka import KafkaProducer
from json import dumps
import pandas as pd
import time
from pathlib import Path

# Define Kafka topic and server configuration
KAFKA_TOPIC_NAME = 'reviews'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

def generate_data():
    # Create a Kafka producer
    kafka_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: dumps(x, default=str).encode('utf-8')  # Convert Timestamp to string
    )
    
    # Path to the JSON file containing processed review data
    file_path = Path('/home/alaa-haggag/Projects/graducation_project/data/dataset_review.json')

    # Read the review data from the JSON file into a DataFrame
    review_df = pd.read_json(file_path, lines=True)

    # Convert the DataFrame to a list of dictionaries (one dictionary per review)
    review_list = review_df.to_dict(orient='records')

    for element in review_list:
        # Prepare the message to be sent
        message = element
        print(f"Message to be sent: {message}")

        # Send the message to the Kafka topic
        kafka_producer.send(KAFKA_TOPIC_NAME, message)

        time.sleep(1)

if __name__ == '__main__':
    generate_data()
