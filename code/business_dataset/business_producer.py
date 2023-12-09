from kafka import KafkaProducer
from json import dumps
import pandas as pd
import time
from pathlib import Path

# Define Kafka topic and server configuration
KAFKA_TOPIC_NAME = 'business'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

def generate_data():
    # Create a Kafka producer
    kafka_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )
    
    # Path to the CSV file containing processed business data
    file_path = Path('/home/alaa-haggag/Projects/graducation_project/data/dataset_business.csv')

    # Read the business data from the CSV file into a DataFrame
    business_df = pd.read_csv(file_path)

    # Convert the DataFrame to a list of dictionaries (one dictionary per sale)
    business_list = business_df.to_dict(orient='records')

    for element in business_list:
        # Prepare the message to be sent
        message = element
        print(f"Message to be sent: {message}")

        # Send the message to the Kafka topic
        kafka_producer.send(KAFKA_TOPIC_NAME, message)

        time.sleep(1)

if __name__ == '__main__':
    generate_data()