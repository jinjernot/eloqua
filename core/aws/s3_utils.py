import boto3
import os
from botocore.exceptions import NoCredentialsError, ClientError
from config import S3_REGION # Import the new region variable

def ping_s3_bucket(bucket_name):
    """
    Checks S3 connectivity and validates access to a specific bucket.

    This function uses the credentials automatically discovered by Boto3
    (presumably exposed by an external tool like Authly).

    Args:
        bucket_name (str): The name of the S3 bucket to check.

    Returns:
        tuple: (bool, str) where bool is True for success and str is a status message.
    """
    print(f"[INFO] Pinging S3 bucket '{bucket_name}' in region '{S3_REGION}' to check status...")
    try:
        # Instantiate the client with the specified region
        s3_client = boto3.client('s3', region_name=S3_REGION)
        
        # 'head_bucket' is a lightweight operation to check for bucket existence and permissions.
        s3_client.head_bucket(Bucket=bucket_name)
        
        print("[INFO] S3 Ping successful. Bucket exists and credentials are valid.")
        return True, "S3 connection successful."

    except ClientError as e:
        # Handle specific client errors from AWS.
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == '404':
            message = f"S3 bucket '{bucket_name}' not found."
        elif error_code == '403':
            message = f"Access to S3 bucket '{bucket_name}' is forbidden. Check permissions."
        else:
            message = f"S3 Client Error: {e}"
        print(f"[ERROR] {message}")
        return False, message
        
    except NoCredentialsError:
        message = "Could not find AWS credentials. Please ensure you are logged into Authly."
        print(f"[ERROR] {message}")
        return False, message
        
    except Exception as e:
        message = f"An unexpected error occurred during S3 ping: {e}"
        print(f"[ERROR] {message}")
        return False, str(e)


def upload_to_s3(file_path, bucket_name, s3_folder):
    """
    Uploads a file to a specific folder in an S3 bucket.
    """
    try:
        # Also specify the region here for consistency
        s3_client = boto3.client('s3', region_name=S3_REGION)
        file_name = os.path.basename(file_path)
        s3_key = f"{s3_folder}/{file_name}"
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"[INFO] Successfully uploaded {file_name} to s3://{bucket_name}/{s3_key}")
        return True
    except NoCredentialsError:
        print("[ERROR] Could not find AWS credentials during upload.")
        return False
    except ClientError as e:
        print(f"[ERROR] An S3 client error occurred during upload: {e}")
        return False
    except FileNotFoundError:
        print(f"[ERROR] The file to upload was not found at: {file_path}")
        return False
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred during upload: {e}")
        return False