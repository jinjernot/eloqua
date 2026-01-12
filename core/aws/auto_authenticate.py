"""
Automatic AWS authentication wrapper for authly.
Automatically refreshes AWS credentials when expired without user intervention.
"""
import os
import sys
import subprocess
import json
from pathlib import Path
from dotenv import load_dotenv
from core.aws.check_aws_auth import check_aws_credentials

# Load environment variables
load_dotenv()

# Import authly configuration from config
from config import (
    AUTHLY_USE_POETRY, 
    AUTHLY_AUTO_REFRESH, 
    AUTHLY_TIMEOUT_SECONDS,
    AUTHLY_PATH,
    AUTHLY_SCRIPT,
    AUTHLY_CONFIG_FILE
)

# Use imported paths
CONFIG_FILE = AUTHLY_CONFIG_FILE


def load_config():
    """Load configuration from environment variables with fallback to auth_config.json"""
    # Try to load from environment variables first
    config = {
        "authly": {
            "rolearn": os.getenv("AWS_ROLE_ARN"),
            "region": os.getenv("S3_REGION"),
            "user": os.getenv("AWS_USER_EMAIL"),
            "profile": os.getenv("AWS_PROFILE"),
            "duration": int(os.getenv("AWS_SESSION_DURATION", "14400"))
        },
        "options": {
            "use_poetry": AUTHLY_USE_POETRY,
            "auto_refresh": AUTHLY_AUTO_REFRESH,
            "timeout_seconds": AUTHLY_TIMEOUT_SECONDS
        }
    }
    
    # If config file exists and has values, use those (for backward compatibility)
    try:
        if CONFIG_FILE.exists():
            with open(CONFIG_FILE, 'r') as f:
                file_config = json.load(f)
                # Override only if file has real values (not placeholders)
                if not any("${" in str(v) for v in str(file_config).split()):
                    return file_config
    except Exception as e:
        print(f"Warning: Could not load config file: {e}")
        print("Using environment variable configuration")
    
    return config


# Load configuration
CONFIG = load_config()
AUTHLY_CONFIG = CONFIG["authly"]
OPTIONS = CONFIG.get("options", {})


def run_authly(use_poetry=None, silent=False):
    """
    Run authly to refresh AWS credentials.
    
    Args:
        use_poetry: If True, runs using poetry. If False, runs with system python.
                   If None, uses config file setting.
        silent: If True, suppresses authly output (not recommended for first run)
    
    Returns:
        bool: True if authly ran successfully, False otherwise
    """
    if use_poetry is None:
        use_poetry = OPTIONS.get("use_poetry", True)
    
    timeout = OPTIONS.get("timeout_seconds", 120)
    try:
        # Build the authly command
        if use_poetry:
            cmd = [
                "poetry", "run", "python",
                str(AUTHLY_SCRIPT)
            ]
        else:
            cmd = [
                sys.executable,  # Use current Python interpreter
                str(AUTHLY_SCRIPT)
            ]
        
        # Add authly arguments
        cmd.extend([
            "--rolearn", AUTHLY_CONFIG["rolearn"],
            "--region", AUTHLY_CONFIG["region"],
            "--user", AUTHLY_CONFIG["user"],
            "--profile", AUTHLY_CONFIG["profile"]
        ])
        
        # Add duration if specified in config
        if "duration" in AUTHLY_CONFIG:
            cmd.extend(["--duration", str(AUTHLY_CONFIG["duration"])])
        
        print("\n" + "="*80)
        print("  RUNNING AUTHLY - AWS AUTHENTICATION")
        print("="*80)
        print(f"Role:    {AUTHLY_CONFIG['rolearn']}")
        print(f"Region:  {AUTHLY_CONFIG['region']}")
        print(f"User:    {AUTHLY_CONFIG['user']}")
        print(f"Profile: {AUTHLY_CONFIG['profile']}")
        if "duration" in AUTHLY_CONFIG:
            duration_hours = AUTHLY_CONFIG["duration"] / 3600
            print(f"Duration: {AUTHLY_CONFIG['duration']}s ({duration_hours:.1f} hours)")
        print("="*80 + "\n")
        
        # Change to authly directory
        original_dir = os.getcwd()
        os.chdir(AUTHLY_PATH)
        
        # Run authly
        if silent:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )
        else:
            result = subprocess.run(
                cmd,
                timeout=timeout
            )
        
        # Change back to original directory
        os.chdir(original_dir)
        
        if result.returncode == 0:
            print("\n" + "="*80)
            print("  ✓ AWS AUTHENTICATION SUCCESSFUL")
            print("="*80 + "\n")
            return True
        else:
            print("\n" + "="*80)
            print("  ✗ AWS AUTHENTICATION FAILED")
            print("="*80)
            if silent and result.stderr:
                print(f"Error: {result.stderr}")
            print("\n")
            return False
            
    except subprocess.TimeoutExpired:
        print("\n✗ Authly timed out. This might be waiting for your input.")
        print("  Try running authly manually or check your network connection.\n")
        return False
    except FileNotFoundError as e:
        print(f"\n✗ Error: Could not find required file: {e}")
        print(f"  Make sure authly is installed at: {AUTHLY_PATH}")
        print(f"  And that poetry is installed (if use_poetry=True)\n")
        return False
    except Exception as e:
        print(f"\n✗ Unexpected error running authly: {e}\n")
        return False


def ensure_authenticated(auto_refresh=None, use_poetry=None):
    """
    Check if AWS credentials are valid, and optionally auto-refresh if expired.
    
    Args:
        auto_refresh: If True, automatically runs authly if credentials are invalid.
                     If None, uses config file setting.
        use_poetry: If True, runs authly using poetry (requires poetry in PATH).
                   If None, uses config file setting.
    
    Returns:
        bool: True if authenticated (or successfully refreshed), False otherwise
    """
    if auto_refresh is None:
        auto_refresh = OPTIONS.get("auto_refresh", True)
    if use_poetry is None:
        use_poetry = OPTIONS.get("use_poetry", True)
    is_valid, message = check_aws_credentials()
    
    if is_valid:
        print("\n✓ AWS credentials are valid")
        print(f"  {message}\n")
        return True
    
    print("\n" + "="*80)
    print("  AWS CREDENTIALS INVALID OR EXPIRED")
    print("="*80)
    print(message)
    print("="*80 + "\n")
    
    if not auto_refresh:
        print("Auto-refresh is disabled. Please authenticate manually.\n")
        return False
    
    # Try to refresh automatically
    print("Attempting automatic authentication...\n")
    
    success = run_authly(use_poetry=use_poetry)
    
    if success:
        # Verify the credentials are now valid
        is_valid, message = check_aws_credentials()
        if is_valid:
            print("✓ Credentials successfully refreshed and verified\n")
            return True
        else:
            print("✗ Authly completed but credentials still invalid")
            print(f"  {message}\n")
            return False
    else:
        print("✗ Automatic authentication failed")
        print("  Please run authly manually:\n")
        print(f"  > cd {AUTHLY_PATH}")
        print(f"  > poetry run python src\\authly.py \\")
        print(f"      --rolearn {AUTHLY_CONFIG['rolearn']} \\")
        print(f"      --region {AUTHLY_CONFIG['region']} \\")
        print(f"      --user {AUTHLY_CONFIG['user']} \\")
        print(f"      --profile {AUTHLY_CONFIG['profile']}\n")
        return False


if __name__ == "__main__":
    """
    Test the auto-authentication functionality.
    
    Usage:
        python auto_authenticate.py           # Check and auto-refresh if needed
        python auto_authenticate.py --check   # Just check, don't refresh
        python auto_authenticate.py --force   # Force refresh even if valid
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Automatic AWS authentication")
    parser.add_argument('--check', action='store_true', 
                       help='Only check credentials, do not auto-refresh')
    parser.add_argument('--force', action='store_true',
                       help='Force refresh even if credentials are valid')
    parser.add_argument('--no-poetry', action='store_true',
                       help='Use system Python instead of poetry')
    
    args = parser.parse_args()
    
    if args.force:
        print("Force refresh requested...\n")
        success = run_authly(use_poetry=not args.no_poetry)
        sys.exit(0 if success else 1)
    else:
        success = ensure_authenticated(
            auto_refresh=not args.check,
            use_poetry=not args.no_poetry
        )
        sys.exit(0 if success else 1)
