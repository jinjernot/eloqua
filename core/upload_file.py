import boto3

def upload_to_s3(local_file, bucket_name, s3_file_key, aws_access_key_id=None, aws_secret_access_key=None, region_name="us-west-2"):
    # Create an S3 client
    session_params = {}
    if aws_access_key_id and aws_secret_access_key:
        session_params["aws_access_key_id"] = aws_access_key_id
        session_params["aws_secret_access_key"] = aws_secret_access_key
    session = boto3.Session(**session_params)
    
    s3 = session.client('s3', region_name=region_name)

    try:
        s3.upload_file(local_file, bucket_name, s3_file_key)
        print(f"✅ Uploaded {local_file} to s3://{bucket_name}/{s3_file_key}")
        return True
    except Exception as e:
        print(f"❌ Failed to upload to S3: {e}")
        return False