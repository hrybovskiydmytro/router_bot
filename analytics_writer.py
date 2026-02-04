from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
from google_creds import load_creds_from_env
from config import GOOGLE_SA_JSON

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

class AnalyticsWriter:
    def __init__(self, sheet_id: str, credentials_path: str):
        creds = load_creds_from_env(GOOGLE_SA_JSON, SCOPES)
        self.service = build("sheets", "v4", credentials=creds)
        self.sheet_id = sheet_id

    def write_event(self, telegram_id, client_key, source, event_type, topic_key=""):
        values = [[
            datetime.utcnow().isoformat(),
            telegram_id,
            client_key,
            source,
            event_type,
            topic_key
        ]]

        self.service.spreadsheets().values().append(
            spreadsheetId=self.sheet_id,
            range="events_raw!A:F",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
