import boto3
import csv

# Initialize a session using your AWS credentials
session = boto3.Session(
    aws_access_key_id='YOUR_ACCESS_KEY',
    aws_secret_access_key='YOUR_SECRET_KEY',
    region_name='YOUR_REGION'
)

# Create an S3 client
s3 = session.client('s3')

# The name of the bucket and the key (path) of the CSV file
bucket_name = 'your-bucket-name'
csv_file_key = 'path/to/your/csv-file.csv'

# The name of the output file
output_file_key = 'path/to/your/output-file.csv'

# Read the CSV file row-wise
with open(csv_file_key, 'r') as csv_file:
    csv_reader = csv.reader(csv_file)
    for row in csv_reader:
        # Write each row to the output file in the S3 bucket
        s3.put_object(
            Bucket=bucket_name,
            Key=output_file_key,
            Body=('\n').join(row)
        )