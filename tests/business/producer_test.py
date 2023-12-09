import unittest
import pandas as pd
from unittest.mock import patch, MagicMock
from kafka import KafkaProducer
import sys
sys.path.append('/home/alaa-haggag/Projects/graducation_project/code/business_dataset')
from business_producer import generate_data  # Replace 'your_module_name' with the actual module name

class TestGenerateData(unittest.TestCase):

    @patch('business_producer.KafkaProducer', autospec=True)
    @patch('pandas.read_csv', autospec=True)
    @patch('time.sleep', autospec=True)
    def test_generate_data(self, mock_sleep, mock_read_csv, mock_kafka_producer):
        # Mock the KafkaProducer instance
        mock_producer_instance = MagicMock(spec=KafkaProducer)
        mock_kafka_producer.return_value = mock_producer_instance

        # Mock the return value of pandas.read_csv
        mock_read_csv.return_value = pd.DataFrame({'column1': [1, 2], 'column2': ['a', 'b']})

        # Call the function to be tested
        generate_data()

        # Assert that KafkaProducer.send was called with the expected arguments
        mock_producer_instance.send.assert_called_with('business', {'column1': 1, 'column2': 'a'})

        # Assert that time.sleep was called as expected (you may need to adjust this based on your use case)
        mock_sleep.assert_called_with(1)

if __name__ == '__main__':
    unittest.main()
