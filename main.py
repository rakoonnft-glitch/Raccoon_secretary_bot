import os
import logging
import re
from contextlib import closing
from collections import defaultdict

import psycopg2
from psycopg2.extras import DictCursor

from aiohttp import web
from aiogram import Bot, Dispatcher, types, F
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application


# --------------------
# 환경 변수
# --------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
FORM_URL = os.getenv("FORM_URL", "https://forms.gle/your-form-url")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # 예: https://xxx.onrender.com/webhook

ADMIN_IDS = []
raw_admin_ids = os.getenv("ADMIN_IDS", "")
for v in raw_admin_ids.split(","):
    v = v.strip()
    if v.isdigit():
        ADMIN_IDS.append(int(v))


if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable missing")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable missing")
if not WEBHOOK_URL:
    raise RuntimeError("WEBHOOK_URL environment variable missing")


# --------------------
# 로그 설정
# --------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --------------------
# DB 연결
# --------------------
def get_conn():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn


def init_db():
    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS winners (
                id SERIAL PRIMARY KEY,
                product_name TEXT NOT NULL,
                handle TEXT NOT NULL,
                phone_number TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_winners_product_handle
            ON winners (product_name, handle);
        """)


# DB CRUD 함수는 이전 코드 그대로 사용
def add_winners(product_name, handles):
    if not handles:
        return
    with closing(get_conn()) as conn, conn.cursor() as cur:
        for handle in handles:
            handle = handle.strip()
            if not handle:
                continue
            if not handle.startswith("@"):
                handle = "@" + handle
            cur.execute("""
                INSERT INTO winners (product_name, handle)
                VALUES (%s, %s)
                ON CONFLICT (product_name, handle) DO NOTHING;
            """, (product_name, handle))


def delete_product_winners(product_name):
    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM winners WHERE product_name = %s;", (product_name,))


def delete_winner_by_handle(handle):
    if not handle.startswith("@"):
        handle = "@" + handle
    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM winners WHERE handle = %s;", (handle,))


def clear_all_phones():
    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute("UPDATE winners SET phone_number = NULL;")


def clear_product_phones(product_name):
    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute("UPDATE winners SET phone_number = NULL WHERE product_name = %s;", (product_name,))


def get_winners_grouped():
    with closing(get_conn()) as conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
            SELECT product_name, handle
            FROM winners
            ORDER BY product_name, id;
        """)
        rows = cur.fetchall()

    grouped = defaultdict(list)
    for row in rows:
        grouped[row["product_name"]].append(row["handle"])
    return grouped


def find_pending_handle_for_user(username):
    if not username:
        return None
    handle = "@" + username if not username.startswith("@") else username

    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, product_name, handle
            FROM winners
            WHERE handle = %s
            LIMIT 1;
        """, (handle,))
        return cur.fetchone()


def update_phone_for_handle(handle, phone_number):
    if not handle.startswith("@"):
        handle = "@" + handle
    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE winners
            SET phone_number = %s
            WHERE handle = %s;
        """, (phone_number, handle))


def get_winners_with_phones():
    with closing(get_conn()) as conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
            SELECT product_name, handle, phone_number
            FROM winners
            ORDER BY product_name, id;
        """)
        rows = cur.fetchall()

    grouped = defaultdict(list)
    for row in rows:
        grouped[row["product_name"]].append((row["handle"], row["phone_number"]))
    return grouped


# --------------------
# Aiogram Bot & Dispatcher
# --------------------
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher()

pending_phone_users = {}


def is_admin(uid):
    return uid in ADMIN_IDS


# --------------------
# 핸들러들 (기존 그대로)
# --------------------
@dp.message(commands=["start"])
async def start(message: types.Message):
    await message.answer("봇이 정상적으로 작동합니다.\n/help 로 명령어를 확인하세요.")


@dp.message(commands=["help"])
async def help_cmd(message: types.Message):
    USER_HELP = (
        "/start\n"
        "/form\n"
        "/list_winners\n"
        "/submit_winner\n"
    )
    ADMIN_HELP = (
        "\n\n[관리자 명령어]\n"
        "/add_winner\n"
        "/delete_product_winners\n"
        "/delete_winner\n"
        "/show_winners\n"
        "/clear_phones_product\n"
        "/clear_phones_all\n"
    )
    text = USER_HELP + (ADMIN_HELP if is_admin(message.from_user.id) else "")
    await message.answer(text)


@dp.message(commands=["form"])
async def form(message: types.Message):
    await message.answer(f"폼 링크:\n{FORM_URL}")


@dp.message(commands=["list_winners"])
async def list_winners(message: types.Message):
    grouped = get_winners_grouped()
    if not grouped:
        await message.answer("등록된 당첨자가 없습니다.")
        return

    text = "상품별 당첨자 목록:\n"
    for prod, handles in grouped.items():
        text += f"\n{prod}:\n"
        for i, h in enumerate(handles, 1):
            text += f"{i}. {h}\n"
    await message.answer(text)


# 당첨자 전화번호 제출
def is_valid_phone(text):
    return re.match(r"^01[016789]-\d{3,4}-\d{4}$", text)


@dp.message(commands=["submit_winner"])
async def submit(message: types.Message):
    user = message.from_user
    if not user.username:
        await message.answer("유저네임(@username)이 필요합니다.")
        return

    row = find_pending_handle_for_user(user.username)
    if not row:
        await message.answer("당첨자로 등록되어 있지 않습니다.")
        return

    pending_phone_users[user.id] = row[2]
    await message.answer("전화번호를 입력해주세요.\n예: 010-1234-5678")


@dp.message()
async def handle_phone(message: types.Message):
    uid = message.from_user.id
    if uid not in pending_phone_users:
        return

    phone = message.text.strip()
    if not is_valid_phone(phone):
        await message.answer("형식이 잘못되었습니다. 예: 010-1234-5678")
        return

    handle = pending_phone_users.pop(uid)
    update_phone_for_handle(handle, phone)
    await message.answer("전화번호가 등록되었습니다.")


# --------------------
# Webhook 설정
# --------------------
async def on_startup(app):
    init_db()
    await bot.set_webhook(WEBHOOK_URL)
    logger.info("Webhook 설정 완료")


async def on_shutdown(app):
    await bot.delete_webhook()


def main():
    app = web.Application()
    SimpleRequestHandler(dp, bot).register(app, path="/webhook")
    setup_application(app, dp, bot=bot)

    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    web.run_app(app, host="0.0.0.0", port=int(os.getenv("PORT", 10000)))


if __name__ == "__main__":
    main()
