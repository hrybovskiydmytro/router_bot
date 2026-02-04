# google_creds.py
import json
from google.oauth2 import service_account

def load_creds_from_env(sa_json: str, scopes: list[str]):
    if not sa_json:
        raise RuntimeError("GOOGLE_SA_JSON is empty or not set")

    info = json.loads(sa_json)
    return service_account.Credentials.from_service_account_info(
        info,
        scopes=scopes
    )