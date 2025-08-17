import json, os, webbrowser, time
from urllib.parse import urlencode
from fastapi import FastAPI, Request
from dotenv import load_dotenv
import requests

load_dotenv()

AUTH_URL = "https://cloud.ouraring.com/oauth/authorize"
TOKEN_URL = "https://api.ouraring.com/oauth/token"

CLIENT_ID = os.environ["OURA_CLIENT_ID"]
CLIENT_SECRET = os.environ["OURA_CLIENT_SECRET"]
REDIRECT_URI = os.environ["OURA_REDIRECT_URI"]
SCOPES = os.environ.get("OURA_SCOPES", "daily").split()
TOKEN_PATH = os.environ.get("OURA_TOKEN_PATH", "data/tokens/oura_tokens.json")

app = FastAPI()

@app.get("/start")
def start_oauth():
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "scope": " ".join(SCOPES),
        "state": "localdev",  # for local use; randomize for real apps
    }
    url = f"{AUTH_URL}?{urlencode(params)}"
    webbrowser.open(url)
    return {"message": "Opened browser for Oura OAuth", "authorize_url": url}

@app.get("/callback")
def oauth_callback(request: Request):
    code = request.query_params.get("code")
    error = request.query_params.get("error")
    if error:
        return {"error": error}

    # Exchange code -> tokens
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    resp = requests.post(TOKEN_URL, data=data, timeout=30)
    resp.raise_for_status()

    token_json = resp.json()
    # Add obtained_at so refresh logic knows when tokens were issued
    token_json["obtained_at"] = int(time.time())

    os.makedirs(os.path.dirname(TOKEN_PATH), exist_ok=True)
    with open(TOKEN_PATH, "w") as f:
        json.dump(token_json, f, indent=2)

    return {
        "status": "ok",
        "saved_to": TOKEN_PATH,
        "scopes": SCOPES,
        "expires_in": token_json.get("expires_in"),
        "has_refresh_token": "refresh_token" in token_json
    }