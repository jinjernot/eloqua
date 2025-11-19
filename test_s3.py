"""
Test S3 connectivity and check folder structure
"""
from config import S3_BUCKET_NAME, S3_FOLDER_PATH, S3_REGION
from core.aws.s3_utils import ping_s3_bucket
import boto3

print("="*60)
print("S3 Configuration Test")
print("="*60)
print(f"Bucket: {S3_BUCKET_NAME}")
print(f"Folder: {S3_FOLDER_PATH}")
print(f"Region: {S3_REGION}")
print()

# Test bucket connectivity
print("Testing S3 bucket connectivity...")
success, msg = ping_s3_bucket(S3_BUCKET_NAME)
print()

if success:
    print("Testing folder structure...")
    s3 = boto3.client('s3', region_name=S3_REGION)
    
    try:
        # List objects in the target folder
        response = s3.list_objects_v2(
            Bucket=S3_BUCKET_NAME, 
            Prefix=S3_FOLDER_PATH + '/',
            MaxKeys=20
        )
        
        if "Contents" in response:
            print(f"✓ Folder '{S3_FOLDER_PATH}/' exists")
            print(f"  Found {len(response['Contents'])} file(s)")
            print(f"\n  Recent files:")
            for obj in response.get('Contents', [])[:10]:
                size_mb = obj['Size'] / (1024 * 1024)
                print(f"    - {obj['Key']} ({size_mb:.2f} MB) - {obj['LastModified']}")
        else:
            print(f"✓ Folder '{S3_FOLDER_PATH}/' exists but is empty")
            print("  Ready to upload files")
            
    except Exception as e:
        print(f"✗ Error checking folder: {e}")
else:
    print(f"✗ Cannot connect to S3: {msg}")
