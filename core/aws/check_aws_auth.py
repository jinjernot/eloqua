"""
Utility to check if AWS credentials are valid and provide helpful messages
"""
import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from config import AWS_PROFILE, S3_REGION

def check_aws_credentials():
    """
    Check if AWS credentials are valid and not expired.
    
    Returns:
        tuple: (bool, str) - (is_valid, message)
    """
    # Get config values for error messages
    role_arn = os.getenv("AWS_ROLE_ARN", "<AWS_ROLE_ARN>")
    region = os.getenv("S3_REGION", "<S3_REGION>")
    user_email = os.getenv("AWS_USER_EMAIL", "<AWS_USER_EMAIL>")
    profile = os.getenv("AWS_PROFILE", "<AWS_PROFILE>")
    
    try:
        session = boto3.Session(profile_name=AWS_PROFILE)
        sts_client = session.client('sts', region_name=S3_REGION)
        
        # Try to get caller identity
        identity = sts_client.get_caller_identity()
        
        user_arn = identity.get('Arn', 'Unknown')
        account = identity.get('Account', 'Unknown')
        
        return True, f"✓ Authenticated as: {user_arn} (Account: {account})"
        
    except NoCredentialsError:
        return False, (
            "✗ No AWS credentials found.\n"
            "  Run authly to authenticate:\n"
            "  > cd path\\to\\authly\n"
            f"  > poetry run python src\\authly.py --rolearn {role_arn} --region {region} --user {user_email} --profile {profile}"
        )
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        
        if error_code == 'ExpiredToken':
            return False, (
                "✗ AWS credentials have EXPIRED.\n"
                "  Run authly again to refresh:\n"
                "  > cd path\\to\\authly\n"
                f"  > poetry run python src\\authly.py --rolearn {role_arn} --region {region} --user {user_email} --profile {profile}"
            )
        else:
            return False, f"✗ AWS authentication error: {e}"
            
    except Exception as e:
        return False, f"✗ Unexpected error checking credentials: {e}"


if __name__ == "__main__":
    print("=" * 60)
    print("AWS Credentials Check")
    print("=" * 60)
    
    is_valid, message = check_aws_credentials()
    print(f"\n{message}\n")
    
    exit(0 if is_valid else 1)
