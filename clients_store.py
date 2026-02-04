from dataclasses import dataclass
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google_creds import load_creds_from_env
from config import GOOGLE_SA_JSON

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

@dataclass
class ClientConfig:
    welcome_text: str
    menu_title: str
    logo_url: str
    timezone: str

class ClientsStore:
    def __init__(self, sheet_id: str, credentials_path: str, client_key: str, ttl_seconds: int = 60):
        self.sheet_id = sheet_id
        self.client_key = client_key
        self.ttl = ttl_seconds
        self._cache = None
        self._cache_ts = 0

        creds = load_creds_from_env(GOOGLE_SA_JSON, SCOPES)
        self.service = build("sheets", "v4", credentials=creds)

    def get_client(self) -> ClientConfig:
        now = time.time()
        if self._cache and (now - self._cache_ts) < self.ttl:
            return self._cache

        res = self.service.spreadsheets().values().get(
            spreadsheetId=self.sheet_id,
            range="clients!A1:Z1000"
        ).execute()

        rows = res.get("values", [])
        header = rows[0]
        idx = {h: i for i, h in enumerate(header)}

        for row in rows[1:]:
            if (row[idx["client_key"]] if idx["client_key"] < len(row) else "").strip() != self.client_key.strip():
                continue


            cfg = ClientConfig(
                welcome_text=row[idx["welcome_text"]],
                menu_title=row[idx["menu_title"]],
                logo_url=row[idx["logo_url"]] if "logo_url" in idx else "",
                timezone=row[idx["timezone"]] if "timezone" in idx else "UTC",
            )
            self._cache = cfg
            self._cache_ts = now
            return cfg

        raise RuntimeError(f"client_key {self.client_key} not found in clients sheet")
