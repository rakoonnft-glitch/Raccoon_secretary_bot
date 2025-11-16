import os
import logging
import re
from contextlib import closing
from collections import defaultdict

import psycopg2
from psycopg2.extras import DictCursor

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

# --------------------
# 환경 변수
# --------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
FORM_URL = os.getenv("FORM_URL", "https://forms.gle/your-form-url")

ADMIN_IDS = []
raw_admin_ids = os.getenv("ADMIN_IDS", "")
for v in raw_admin_ids.split(","):
    v = v.strip()
    if v.isdigit():
        ADMIN_IDS.append(int(v))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not set")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

# --------------------
# Logging
# --------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------------------
# DB
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
        cur.execute(
            "UPDATE winners SET phone_number = NULL WHERE product_name = %s;",
            (product_name,)
        )


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
    handle = "@" + username

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
# Bot
# --------------------
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)

pending_phone_users = {}


def is_admin(uid):
    return uid in ADMIN_IDS


# --------------------
# Commands
# --------------------
@dp.message_handler(commands=["start"])
async def start_cmd(message: types.Message):
    await message.reply("봇이 정상적으로 작동 중입니다.\n/help 로 명령어를 확인하세요.")


@dp.message_handler(commands=["help"])
async def help_cmd(message: types.Message):
    USER_HELP = (
        "/start\n"
        "/form\n"
        "/list_winners\n"
        "/submit_winner\n"
    )

    ADMIN_HELP = (
        "\n[관리자 전용]\n"
        "/add_winner\n"
        "/delete_product_winners\n"
        "/delete_winner\n"
        "/show_winners\n"
        "/clear_phones_product\n"
        "/clear_phones_all\n"
    )

    text = USER_HELP + (ADMIN_HELP if is_admin(message.from_user.id) else "")
    await message.reply(text)


@dp.message_handler(commands=["form"])
async def form_cmd(message: types.Message):
    await message.reply(f"폼 링크:\n{FORM_URL}")


@dp.message_handler(commands=["list_winners"])
async def list_cmd(message: types.Message):
    grouped = get_winners_grouped()
    if not grouped:
        await message.reply("등록된 당첨자가 없습니다.")
        return

    text = "📦 상품별 당첨자 목록\n"
    for prod, handles in grouped.items():
        text += f"\n{prod}:\n"
        for i, h in enumerate(handles, 1):
            text += f"{i}. {h}\n"

    await message.reply(text)


# --------------------
# 전화번호 제출
# --------------------
def is_valid_phone(text):
    return re.match(r"^01[016789]-\d{3,4}-\d{4}$", text)


@dp.message_handler(commands=["submit_winner"])
async def submit_cmd(message: types.Message):
    user = message.from_user
    if not user.username:
        await message.reply("유저네임(@username)이 필요합니다.")
        return

    row = find_pending_handle_for_user(user.username)
    if not row:
        await message.reply("당첨자 명단에 없습니다.")
        return

    pending_phone_users[user.id] = row[2]
    await message.reply("전화번호를 입력해주세요.\n예: 010-1234-5678")


@dp.message_handler()
async def phone_handler(message: types.Message):
    uid = message.from_user.id
    if uid not in pending_phone_users:
        return

    phone = message.text.strip()
    if not is_valid_phone(phone):
        await message.reply("형식 오류! 예: 010-1234-5678")
        return

    handle = pending_phone_users.pop(uid)
    update_phone_for_handle(handle, phone)

    await message.reply("전화번호가 등록되었습니다.")


# --------------------
# 관리자 명령어
# --------------------
@dp.message_handler(commands=["add_winner"])
async def add_winner_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await message.reply("상품명을 입력하세요.")
    dp.register_message_handler(process_product_name, state=None, user_id=message.from_user.id)


async def process_product_name(message: types.Message):
    product_name = message.text.strip()

    await message.reply(
        "당첨자 핸들을 한 줄에 하나씩 입력하세요.\n끝내려면 /end"
    )

    dp.register_message_handler(
        process_handles,
        state=None,
        user_id=message.from_user.id,
        product_name=product_name
    )
    dp.unregister_message_handler(process_product_name)


async def process_handles(message: types.Message, product_name: str):
    if message.text.strip() == "/end":
        await message.reply("등록 완료!")
        dp.unregister_message_handler(process_handles)
        return

    handles = [h.strip() for h in message.text.splitlines() if h.strip()]
    add_winners(product_name, handles)

    await message.reply("\n".join(handles) + "\n추가 완료.")


@dp.message_handler(commands=["delete_product_winners"])
async def delete_product(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    await message.reply("삭제할 상품명을 입력하세요.")
    dp.register_message_handler(
        process_delete_product,
        state=None,
        user_id=message.from_user.id
    )


async def process_delete_product(message: types.Message):
    product_name = message.text.strip()
    delete_product_winners(product_name)
    await message.reply(f"{product_name} 당첨자 전체 삭제됨.")
    dp.unregister_message_handler(process_delete_product)


@dp.message_handler(commands=["delete_winner"])
async def delete_winner(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await message.reply("삭제할 핸들을 입력하세요.")
    dp.register_message_handler(
        process_delete_winner,
        state=None,
        user_id=message.from_user.id
    )


async def process_delete_winner(message: types.Message):
    handle = message.text.strip()
    delete_winner_by_handle(handle)
    await message.reply(f"{handle} 삭제됨.")
    dp.unregister_message_handler(process_delete_winner)


@dp.message_handler(commands=["show_winners"])
async def show_winners(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    grouped = get_winners_with_phones()
    if not grouped:
        await message.reply("데이터 없음.")
        return

    text = "📦 상세 당첨자 목록\n\n"
    for prod, items in grouped.items():
        text += f"{prod}:\n"
        for handle, phone in items:
            phone = phone if phone else "전화번호 없음"
            text += f"- {handle} / {phone}\n"
        text += "\n"

    await message.reply(text)


@dp.message_handler(commands=["clear_phones_all"])
async def clear_all(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    clear_all_phones()
    await message.reply("전체 전화번호 삭제됨.")


@dp.message_handler(commands=["clear_phones_product"])
async def clear_prod(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    await message.reply("상품명을 입력하세요.")
    dp.register_message_handler(
        process_clear_prod,
        state=None,
        user_id=message.from_user.id
    )


async def process_clear_prod(message: types.Message):
    prod = message.text.strip()
    clear_product_phones(prod)

    await message.reply(f"{prod} 상품 전화번호 삭제됨.")
    dp.unregister_message_handler(process_clear_prod)


# --------------------
# 시작
# --------------------
async def on_startup(dp):
    init_db()
    logger.info("DB 초기화 완료")


if __name__ == "__main__":
    init_db()
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
