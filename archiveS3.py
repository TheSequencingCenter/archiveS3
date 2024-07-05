# Author:  Richard Casey
# Date:    04-07-2024 (DD-MM-YYYY)
# Purpose: Archive files and directories in AWS S3 bucket.

# Standard library imports
import boto3
import os
import sys
from   datetime  import datetime
from   datetime  import timedelta
from   dotenv    import load_dotenv
from   typing    import Any

# Local application/library specific imports
from utilities.loggerUtil import logger

def list_buckets(s3_client: boto3.client) -> None:
    """
    Lists all buckets in the S3 account.

    :param s3_client: Boto3 S3 client.
    """
    try:
        response: Any = s3_client.list_buckets()
        for bucket in response['Buckets']:
            logger.info(f'Bucket Name: {bucket["Name"]}')
    except Exception as e:
        logger.error(f"ERROR: Failed to list S3 buckets: {e}")
        sys.exit(1)

def upload_file(local_file_path: str, bucket_name: str, s3_client: boto3.client) -> None:
    """
    Uploads a file to an S3 bucket.

    :param local_file_path: Path to the local file.
    :param bucket_name:     Name of the S3 bucket.
    :param s3_client:       Boto3 S3 client.
    """
    try:
        filename: str = os.path.basename(local_file_path)  # Extract filename from local file path
        s3_key:   str = filename  # Use filename as the S3 key

        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        logger.info(f"File uploaded successfully to {bucket_name}/{s3_key}")

    except FileNotFoundError:
        logger.error(f"ERROR: Local file {local_file_path} not found.")
        sys.exit(1)
    except boto3.exceptions.S3UploadFailedError as e:
        logger.error(f"ERROR: Failed to upload file to S3: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"ERROR: An unexpected error occurred: {e}")
        sys.exit(1)

def upload_directory(directory_path: str, bucket_name: str, s3_client: boto3.client) -> None:
    """
    Uploads an entire directory to an S3 bucket, preserving the directory structure.

    :param directory_path: Path to the local directory.
    :param bucket_name:    Name of the S3 bucket.
    :param s3_client:      Boto3 S3 client.
    """
    base_dir = os.path.basename(directory_path.rstrip('/'))
    for root, _, files in os.walk(directory_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path   = os.path.relpath(local_file_path, os.path.dirname(directory_path))
            s3_key          = os.path.join(base_dir, relative_path).replace("\\", "/")  # Ensure correct S3 key format

            try:
                s3_client.upload_file(local_file_path, bucket_name, s3_key)
                logger.info(f"File {local_file_path} uploaded successfully to {bucket_name}/{s3_key}")
            except Exception as e:
                logger.error(f"ERROR: Failed to upload {local_file_path} to {bucket_name}/{s3_key}: {e}")

def main() -> None:
    # Load environment variables from .env file
    load_dotenv()

    try:
        # Retrieve AWS credentials from environment variables
        aws_access_key_id:     str = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_access_key: str = os.getenv('AWS_SECRET_ACCESS_KEY')
        region_name:           str = os.getenv('AWS_REGION')

        if not aws_access_key_id or not aws_secret_access_key or not region_name:
            logger.error("ERROR: AWS credentials or region not found in environment variables")
            sys.exit(1)

        # Create a session using your AWS credentials from environment variables
        session:     boto3.Session = boto3.Session(
            aws_access_key_id      = aws_access_key_id,
            aws_secret_access_key  = aws_secret_access_key,
            region_name            = region_name
        )

        # Create an S3 client with transfer acceleration enabled
        s3_client: boto3.client = session.client('s3', config=boto3.session.Config(s3={'use_accelerate_endpoint': True}))

    except Exception as e:
        logger.error(f"ERROR: Failed to create AWS session or client: {e}")
        sys.exit(1)

    try:
        # List all buckets
        list_buckets(s3_client)
    except Exception as e:
        logger.error(f"ERROR: Failed to list S3 buckets: {e}")
        sys.exit(1)

    # try:
    #     # Upload a file
    #     upload_file('/home/seqcenter/archiveS3/testfile', 'seqcenter-ubuntu-image-backup', s3_client)
    # except Exception as e:
    #     logger.error(f"ERROR: Failed to upload file: {e}")
    #     sys.exit(1)

    try:
        # upload a directory

        # Determine the directory for the day before the current date
        yesterday     = datetime.now() - timedelta(1)
        yesterday_str = yesterday.strftime('%Y-%m-%d')

        # Find the subdirectory with the naming convention for the day before
        TIMESHIFT_DIR          = "/media/seqcenter/ebd27e92-e0fe-47d2-aa71-95c278cf17af/timeshift/snapshots-daily" # constant directory path
        subdirectory_to_upload = None

        for subdir in os.listdir(TIMESHIFT_DIR):
            if subdir.startswith(yesterday_str):
                subdirectory_to_upload = os.path.join(TIMESHIFT_DIR, subdir)
                break

        if subdirectory_to_upload:
            upload_directory(subdirectory_to_upload, 'seqcenter-ubuntu-image-backup', s3_client) # upload the directory
        else:
            logger.error(f"ERROR: No subdirectory found for date {yesterday_str}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"ERROR: Failed to upload directory: {e}")
        sys.exit(1)

# main entry point
if __name__ == "__main__":
    logger.info("Start archiveS3...")
    main()
    logger.info("Finish archiveS3.")