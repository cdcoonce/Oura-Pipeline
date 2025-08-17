import os, json, time, requests
from dotenv import load_dotenv

load_dotenv()

TOKEN_URL = "https://api.ouraring.com/oauth/token"
CLIENT_ID = os.environ["OURA_CLIENT_ID"]
CLIENT_SECRET = os.environ["OURA_CLIENT_SECRET"]
TOKEN_PATH = os.environ.get("OURA_TOKEN_PATH", "data/tokens/oura_tokens.json")

def _read_tokens():
    if not os.path.exists(TOKEN_PATH):
        raise FileNotFoundError(f"Token file not found: {TOKEN_PATH}")
    with open(TOKEN_PATH) as f:
        return json.load(f)

def _write_tokens(tokens: dict):
    os.makedirs(os.path.dirname(TOKEN_PATH), exist_ok=True)
    with open(TOKEN_PATH, "w") as f:
        json.dump(tokens, f, indent=2)

def get_access_token() -> str:
    """
    Returns a valid access token. Refreshes using refresh_token when needed.
    The Oura token response includes: access_token, expires_in (s), refresh_token.
    We add a local 'obtained_at' to track expiry.
    """
    tokens = _read_tokens()
    access_token = tokens["access_token"]
    expires_in = tokens.get("expires_in", 0)
    obtained_at = tokens.get("obtained_at", int(time.time()))
    if (obtained_at + int(expires_in) - 60) > int(time.time()):
        return access_token  # still valid

    # refresh
    data = {
        "grant_type": "refresh_token",
        "refresh_token": tokens["refresh_token"],
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    resp = requests.post(TOKEN_URL, data=data, timeout=30)
    resp.raise_for_status()
    new_tokens = resp.json()
    new_tokens["obtained_at"] = int(time.time())
    _write_tokens(new_tokens)
    return new_tokens["access_token"]

def save_initial_obtained_at():
    """Call once right after you first saved tokens to add obtained_at."""
    try:
        tokens = _read_tokens()
        if "obtained_at" not in tokens:
            tokens["obtained_at"] = int(time.time())
            _write_tokens(tokens)
    except FileNotFoundError:
        pass