import boto3
import os
from   dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve AWS credentials from environment variables
aws_access_key_id     = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
region_name           = os.getenv('AWS_REGION')

# Create a session using your AWS credentials from environment variables
session                   = boto3.Session(
    aws_access_key_id     = aws_access_key_id,
    aws_secret_access_key = aws_secret_access_key,
    region_name           = region_name
)

# Create an S3 client
s3_client = session.client('s3')

# List all buckets
response = s3_client.list_buckets()

# Print bucket names
for bucket in response['Buckets']:
    print(f'Bucket Name: {bucket["Name"]}')