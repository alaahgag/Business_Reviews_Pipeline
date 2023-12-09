import boto3
import psycopg2
import json
import datetime

def lambda_handler(event, context):
    # Set the name of the S3 bucket
    bucket_name = 'user-daily-data'

    # Set the name of the S3 object
    s3_object_name = 'dynamodb-backup-' + now.strftime('%Y-%m-%d-%H-%M-%S') + '.json'

    # Set the name of the Redshift table
    table_name = 'user_dest'

    # Set the Redshift cluster credentials
    redshift_host = ''
    redshift_port = ''
    redshift_dbname = ''
    redshift_user = ''
    redshift_password = ''

    # Create an S3 client
    s3 = boto3.client('s3')

    # Create a Redshift connection
    conn = psycopg2.connect(
        host=redshift_host,
        port=redshift_port,
        dbname=redshift_dbname,
        user=redshift_user,
        password=redshift_password
    )

    # Create a cursor object
    cur = conn.cursor()

    # Get the current date and time
    now = datetime.datetime.now()

    # Set the name of the Redshift table
    redshift_table_name = 'dim_user'

    # Set the name of the S3 object
    s3_object_name = 'dynamodb-backup-' + now.strftime('%Y-%m-%d-%H-%M-%S') + '.json'

    # Set the S3 object URL
    s3_object_url = f's3://{bucket_name}/{s3_object_name}'

    # Set the COPY command
    copy_command = f"COPY {redshift_table_name} FROM '{s3_object_url}' CREDENTIALS 'aws_access_key_id=AKIAWJNQSSSUWJGF2DBK;aws_secret_access_key=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' DELIMITER ',' CSV IGNOREHEADER 1;"

    # Execute the COPY command
    cur.execute(copy_command)

    # Commit the transaction
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()

    # Return a success message
    return {
        'statusCode': 200,
        'body': json.dumps('Data loaded from S3 to Redshift successfully!')
    }
