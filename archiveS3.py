import boto3
import os
import sys
from   dotenv import load_dotenv
from   typing import Any

def list_buckets(s3_client: boto3.client) -> None:
    """
    Lists all buckets in the S3 account.

    :param s3_client: Boto3 S3 client.
    """
    try:
        response: Any = s3_client.list_buckets()
        for bucket in response['Buckets']:
            print(f'Bucket Name: {bucket["Name"]}')
    except Exception as e:
        print(f"ERROR: Failed to list S3 buckets: {e}")
        sys.exit(1)

def upload_file(local_file_path: str, bucket_name: str, s3_client: boto3.client) -> None:
    """
    Uploads a file to an S3 bucket.

    :param local_file_path: Path to the local file.
    :param bucket_name: Name of the S3 bucket.
    :param s3_client: Boto3 S3 client.
    """
    try:
        filename: str = os.path.basename(local_file_path)  # Extract filename from local file path
        s3_key:   str = filename  # Use filename as the S3 key

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

def upload_directory(directory_path: str, bucket_name: str, s3_client: boto3.client) -> None:
    """
    Uploads all files from a local directory to an S3 bucket.

    :param directory_path: Path to the local directory.
    :param bucket_name: Name of the S3 bucket.
    :param s3_client: Boto3 S3 client.
    """
    for root, _, files in os.walk(directory_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path   = os.path.relpath(local_file_path, directory_path)
            s3_key          = relative_path.replace("\\", "/")  # Ensure correct S3 key format

            try:
                s3_client.upload_file(local_file_path, bucket_name, s3_key)
                print(f"File {local_file_path} uploaded successfully to {bucket_name}/{s3_key}")
            except Exception as e:
                print(f"ERROR: Failed to upload {local_file_path} to {bucket_name}/{s3_key}: {e}")

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
        list_buckets(s3_client)
    except Exception as e:
        print(f"ERROR: Failed to list S3 buckets: {e}")
        sys.exit(1)

    try:
        # Upload a file
        upload_file('/home/seqcenter/archiveS3/testfile', 'seqcenter-ubuntu-image-backup', s3_client)
    except Exception as e:
        print(f"ERROR: Failed to upload file: {e}")
        sys.exit(1)
        
    try:
        # Upload a directory
        upload_directory('/home/seqcenter/archiveS3', 'seqcenter-ubuntu-image-backup', s3_client)
    except Exception as e:
        print(f"ERROR: Failed to upload directory /home/seqcenter/archiveS3 to S3: {e}")
        sys.exit(1)

# main entry point
if __name__ == "__main__":
    main()
