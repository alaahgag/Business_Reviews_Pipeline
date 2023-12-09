import boto3
import json
import datetime

def lambda_handler(event, context):
    # Create an S3 client
    s3 = boto3.client('s3')

    # Set the name of the S3 bucket
    bucket_name = 'review-daily-data'
    
     # Set the name of the DynamoDB table
    table_name = 'review_table'

    # Get the current date and time
    now = datetime.datetime.now()

    # Set the name of the S3 object
    s3_object_name = 'dynamodb-backup-' + now.strftime('%Y-%m-%d-%H-%M-%S') + '.json'

    # Create a new file in the /tmp directory to store the data
    file_path = '/tmp/' + s3_object_name
    with open(file_path, 'w') as file:
        # Create a DynamoDB client
        dynamodb = boto3.client('dynamodb')

        # Scan the DynamoDB table and write the data to the file
        response = dynamodb.scan(TableName=table_name)
        items = response['Items']
        while 'LastEvaluatedKey' in response:
            response = dynamodb.scan(TableName=table_name, ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
        file.write(json.dumps(items))

    # Upload the file to the S3 bucket
    s3.upload_file(file_path, bucket_name, s3_object_name)

    # Return a success message
    return {
        'statusCode': 200,
        'body': json.dumps('DynamoDB table backed up to S3 successfully!')
    }
