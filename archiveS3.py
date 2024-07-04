import boto3
import os
import sys
from   dotenv import load_dotenv
from   typing import Any

def main() -> None:
    # Load environment variables from .env file
    load_dotenv()

    try:
        # Retrieve AWS credentials from environment variables
        aws_access_key_id:     str = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_access_key: str = os.getenv('AWS_SECRET_ACCESS_KEY')
        region_name:           str = os.getenv('AWS_REGION')

        if not aws_access_key_id or not aws_secret_access_key or not region_name:
            raise ValueError("ERROR: AWS credentials or region not found in environment variables")

        # Create a session using your AWS credentials from environment variables
        session:     boto3.Session = boto3.Session(
            aws_access_key_id      = aws_access_key_id,
            aws_secret_access_key  = aws_secret_access_key,
            region_name            = region_name
        )

        # Create an S3 client
        s3_client: boto3.client = session.client('s3')

    except Exception as e:
        print(f"ERROR: Failed to create AWS session or client: {e}")
        sys.exit(1)

    try:
        # List all buckets
        response: Any = s3_client.list_buckets()

        # Print bucket names
        for bucket in response['Buckets']:
            print(f'Bucket Name: {bucket["Name"]}')
    except Exception as e:
        print(f"ERROR: Failed to list S3 buckets: {e}")
        sys.exit(1)

    try:
        # Upload a file
        local_file_path: str = 'path/to/local/file'
        bucket_name:     str = 'your-bucket-name'
        s3_key:          str = 's3/object/key'

        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        print(f"File uploaded successfully to {bucket_name}/{s3_key}")

    except FileNotFoundError:
        print(f"ERROR: Local file {local_file_path} not found.")
        sys.exit(1)
    except boto3.exceptions.S3UploadFailedError as e:
        print(f"ERROR: Failed to upload file to S3: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
