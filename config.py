import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
ADMIN_IDS = [int(id.strip()) for id in os.getenv("ADMIN_IDS", "").split(",") if id.strip()]
DATABASE_PATH = os.getenv("DATABASE_PATH", "bot.db")

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в .env файле")

# Нормализация и базовая проверка формата токена (например: 123456789:ABCdefGhIJKlmNoPQRsTUVwxyz_12345678)
import re

BOT_TOKEN = BOT_TOKEN.strip().strip('"').strip("'")
# Если случайно добавлен префикс "Bot ", удалим его
if BOT_TOKEN.lower().startswith("bot "):
    BOT_TOKEN = BOT_TOKEN.split(" ", 1)[1]

_token_re = re.compile(r"^\d+:[A-Za-z0-9_\-]{35,}$")
if not _token_re.match(BOT_TOKEN):
    raise ValueError("BOT_TOKEN имеет неверный формат. Проверьте значение в .env (формат должен быть <id>:<token>, без префикса 'Bot ').")

if not CHANNEL_ID:
    raise ValueError("CHANNEL_ID не найден в .env файле")

