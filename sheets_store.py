from dataclasses import dataclass
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google_creds import load_creds_from_env
from config import GOOGLE_SA_JSON

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

@dataclass
class Topic:
    topic_key: str
    title: str
    url: str
    emoji: str
    sort: int

class TopicsStore:
    def __init__(self, sheet_id: str, credentials_path: str, client_key: str, ttl_seconds: int = 60):
        self.sheet_id = sheet_id
        self.client_key = client_key
        self.ttl = ttl_seconds
        self._cache = {}
        self._cache_ts = 0

        creds = load_creds_from_env(GOOGLE_SA_JSON, SCOPES)
        self.service = build("sheets", "v4", credentials=creds)

    def get_topics(self):
        now = time.time()
        if self._cache and (now - self._cache_ts) < self.ttl:
            return self._cache

        res = self.service.spreadsheets().values().get(
            spreadsheetId=self.sheet_id,
            range="topics!A1:Z1000"
        ).execute()

        rows = res.get("values", [])
        if not rows:
            self._cache = {}
            self._cache_ts = now
            return self._cache

        header = [h.strip() for h in rows[0]]
        idx = {h: i for i, h in enumerate(header)}

        def val(row, name, default=""):
            i = idx.get(name)
            if i is None or i >= len(row):
                return default
            return (row[i] or "").strip()

        topics = {}

        for row in rows[1:]:
            if val(row, "client_key") != self.client_key:
                continue

            is_active = val(row, "is_active").lower()
            if is_active not in ("true", "1", "yes", "y"):
                continue

            topic_key = val(row, "topic_key")
            title = val(row, "title")
            url = val(row, "url")
            if not topic_key or not title or not url:
                continue

            sort_str = val(row, "sort", "100")
            try:
                sort = int(sort_str)
            except ValueError:
                sort = 100

            emoji = val(row, "emoji", "")

            t = Topic(
                topic_key=topic_key,
                title=title,
                url=url,
                emoji=emoji,
                sort=sort
            )
            topics[t.topic_key] = t

        self._cache = dict(sorted(topics.items(), key=lambda x: x[1].sort))
        self._cache_ts = now
        return self._cache
