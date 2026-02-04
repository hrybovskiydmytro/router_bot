import aiosqlite
from datetime import datetime

DB_PATH = "bot.db"

CREATE_USERS = """
CREATE TABLE IF NOT EXISTS users (
  telegram_id INTEGER PRIMARY KEY,
  first_name TEXT,
  username TEXT,
  created_at TEXT
);
"""

CREATE_EVENTS = """
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  telegram_id INTEGER,
  client_key TEXT,
  source TEXT,
  event_type TEXT,
  topic_key TEXT,
  ts TEXT
);
"""

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_USERS)
        await db.execute(CREATE_EVENTS)
        await db.commit()

async def upsert_user(telegram_id: int, first_name: str | None, username: str | None):
    now = datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO users (telegram_id, first_name, username, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(telegram_id) DO UPDATE SET
              first_name=excluded.first_name,
              username=excluded.username
            """,
            (telegram_id, first_name, username, now),
        )
        await db.commit()

async def log_event(telegram_id: int, topic_key: str):
    ts = datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO events (telegram_id, topic_key, ts) VALUES (?, ?, ?)",
            (telegram_id, topic_key, ts),
        )
        await db.commit()

        from datetime import datetime
import aiosqlite

async def log_event(
    telegram_id: int,
    client_key: str,
    source: str,
    event_type: str,
    topic_key: str = ""
):
    ts = datetime.utcnow().isoformat()

    async with aiosqlite.connect("bot.db") as db:
        await db.execute(
            """
            INSERT INTO events
            (telegram_id, client_key, source, event_type, topic_key, ts)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (telegram_id, client_key, source, event_type, topic_key, ts),
        )
        await db.commit()