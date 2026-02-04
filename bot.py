import asyncio
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import CommandStart
from aiogram.utils.keyboard import InlineKeyboardBuilder
from urllib.parse import urlparse

from config import BOT_TOKEN, CLIENT_KEY, GOOGLE_SHEET_ID, GOOGLE_SHEETS_CREDENTIALS
from clients_store import ClientsStore
from sheets_store import TopicsStore
from analytics_writer import AnalyticsWriter


USER_SOURCE = {}

clients_store = ClientsStore(GOOGLE_SHEET_ID, GOOGLE_SHEETS_CREDENTIALS, CLIENT_KEY)
topics_store = TopicsStore(GOOGLE_SHEET_ID, GOOGLE_SHEETS_CREDENTIALS, CLIENT_KEY)
analytics = AnalyticsWriter(GOOGLE_SHEET_ID, GOOGLE_SHEETS_CREDENTIALS)


def topics_keyboard():
    kb = InlineKeyboardBuilder()
    topics = topics_store.get_topics()
    for t in topics.values():
        text = f"{t.emoji} {t.title}".strip()
        kb.button(text=text, callback_data=f"topic:{t.topic_key}")
    kb.adjust(2)
    return kb.as_markup()


def normalize_tg_url(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    if url.startswith("t.me/"):
        url = "https://" + url
    if url.startswith("@"):
        url = "https://t.me/" + url[1:]
    return url


def is_valid_url(url: str) -> bool:
    try:
        u = urlparse(url)
        return u.scheme in ("http", "https") and bool(u.netloc)
    except Exception:
        return False


def go_keyboard(url: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="Перейти в канал", url=url)
    kb.button(text="Назад", callback_data="menu")
    kb.adjust(1)
    return kb.as_markup()


async def main():
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()

    @dp.message(CommandStart())
    async def start(message: Message):
        source = "direct"
        if message.text and len(message.text.split()) > 1:
            source = message.text.split(maxsplit=1)[1]

        USER_SOURCE[message.from_user.id] = source
        client = clients_store.get_client()

        analytics.write_event(message.from_user.id, CLIENT_KEY, source, "start")
        analytics.write_event(message.from_user.id, CLIENT_KEY, source, "menu_view")

        await message.answer(client.welcome_text, reply_markup=topics_keyboard())

    @dp.callback_query(F.data == "menu")
    async def menu(call: CallbackQuery):
        src = USER_SOURCE.get(call.from_user.id, "direct")
        client = clients_store.get_client()

        analytics.write_event(call.from_user.id, CLIENT_KEY, src, "menu_view")

        await call.message.edit_text(client.menu_title, reply_markup=topics_keyboard())
        await call.answer()

    @dp.callback_query(F.data.startswith("topic:"))
    async def topic(call: CallbackQuery):
        topic_key = call.data.split(":", 1)[1]
        topic = topics_store.get_topics().get(topic_key)
        if not topic:
            await call.answer("Тема не найдена", show_alert=True)
            return

        src = USER_SOURCE.get(call.from_user.id, "direct")

        analytics.write_event(call.from_user.id, CLIENT_KEY, src, "topic_click", topic_key)
        analytics.write_event(call.from_user.id, CLIENT_KEY, src, "go_click", topic_key)

        url = normalize_tg_url(topic.url)
        if not is_valid_url(url):
            await call.answer("URL в topics неверный. Проверь колонку url.", show_alert=True)
            return

        await call.message.edit_text(
            f"Вы выбрали: **{topic.title}**",
            reply_markup=go_keyboard(url),
            parse_mode="Markdown"
        )
        await call.answer()

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
