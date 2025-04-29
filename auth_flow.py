import requests
import base64

CLIENT_ID = 'your_client_id'
CLIENT_SECRET = 'your_client_secret'
TOKEN_URL = 'https://login.eloqua.com/token'  # Eloqua's token URL

def get_access_token():
    """Get an access token using the client credentials flow."""
    # Basic authentication (base64 encoded)
    credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"
    basic_auth = base64.b64encode(credentials.encode()).decode()

    headers = {
        'Authorization': f'Basic {basic_auth}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    data = {
        'grant_type': 'client_credentials',
        'scope': 'your_scopes_here'  # This depends on what you're accessing
    }

    response = requests.post(TOKEN_URL, headers=headers, data=data)

    if response.status_code != 200:
        print(f"Failed to get access token: {response.status_code}")
        print(response.text)
        return None

    token_info = response.json()
    access_token = token_info['access_token']
    expires_in = token_info['expires_in']

    return access_token, expires_in