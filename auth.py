import os
from dotenv import load_dotenv
from authlib.integrations.flask_client import OAuth
from flask import Flask

# Load environment variables from ht_api repository root
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

def create_ht_token():
    """
    Creates and returns Hattrick OAuth token from environment variables.
    
    Returns:
        dict: Dictionary with oauth_token and oauth_token_secret
    """
    return {
        'oauth_token': os.getenv("HT_OAUTH_TOKEN"),
        'oauth_token_secret': os.getenv("HT_OAUTH_TOKEN_SECRET")
    }

def create_oauth_client():
    """
    Creates and configures OAuth client for Hattrick API.
    
    Returns:
        OAuth: Configured OAuth client
    """
    # Create Flask app (needed for OAuth)
    app = Flask(__name__)
    app.secret_key = os.urandom(24)
    
    # Initialize OAuth
    oauth = OAuth()
    oauth.init_app(app)
    
    # Register Hattrick OAuth application
    oauth.register(
        name='hattrick',
        client_id=os.getenv("HT_CLIENT_ID"),
        client_secret=os.getenv("HT_CLIENT_SECRET"),
        request_token_url="https://chpp.hattrick.org/oauth/request_token.ashx",
        request_token_params=None,
        access_token_url="https://chpp.hattrick.org/oauth/access_token.ashx",
        access_token_params=None,
        authorize_url='https://chpp.hattrick.org/oauth/authorize.aspx',
        api_base_url='https://chpp.hattrick.org/chppxml.ashx',
        client_kwargs=None,
    )
    
    return oauth

def get_ht_oauth():
    """
    Main function to get OAuth client and token.
    
    Returns:
        tuple: (oauth_client, token)
    """
    oauth = create_oauth_client()
    token = create_ht_token()
    return oauth, token

# For direct usage
if __name__ == "__main__":
    oauth, token = get_ht_oauth()
    print("OAuth client and token were successfully created.")
    print(f"Token: {token}")
