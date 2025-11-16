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

# "123456,234567" 형태
ADMIN_IDS = []
raw_admin_ids = os.getenv("ADMIN_IDS", "")
for v in raw_admin_ids.split(","):
    v = v.strip()
    if not v:
        continue
    try:
        ADMIN_IDS.append(int(v))
    except ValueError:
        pass

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN 환경 변수가 설정되어 있지 않습니다.")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL 환경 변수가 설정되어 있지 않습니다.")

# --------------------
# 로그 설정
# --------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------------------
# DB 유틸
# --------------------


def get_conn():
    # Supabase Session Pooler / IPv4 용 DSN 이 DATABASE_URL 에 들어있다고 가정
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn


def init_db():
    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS winners (
                id SERIAL PRIMARY KEY,
                product_name TEXT NOT NULL,
                handle TEXT NOT NULL,
                phone_number TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_winners_product_handle
            ON winners (product_name, handle);
            """
        )


def add_winners(product_name: str, handles: list[str]):
    if not handles:
        return
    with closing(get_conn()) as conn, conn.cursor() as cur:
        for handle in handles:
            handle = handle.strip()
            if not handle:
                continue
            if not handle.startswith("@"):
                handle = "@" + handle
            try:
                cur.execute(
                    """
                    INSERT INTO winners (product_name, handle)
                    VALUES (%s, %s)
                    ON CONFLICT (product_name, handle) DO NOTHING;
                    """,
                    (product_name, handle),
                )
            except Exception as e:
                logger.exception("add_winners insert 실패: %s", e)


def delete_product_winners(product_name: str):
    """해당 상품의 기록 전체 삭제 (전화번호 포함)"""
    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM winners WHERE product_name = %s;", (product_name,))


def delete_winner_by_handle(handle: str):
    """특정 핸들의 모든 기록 삭제 (전화번호 포함)"""
    if not handle.startswith("@"):
        handle = "@" + handle
    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM winners WHERE handle = %s;", (handle,))


def clear_all_phones():
    """모든 당첨자의 전화번호만 삭제"""
    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute("UPDATE winners SET phone_number = NULL;")


def clear_product_phones(product_name: str):
    """특정 상품의 전화번호만 삭제"""
    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE winners SET phone_number = NULL WHERE product_name = %s;",
            (product_name,),
        )


def get_winners_grouped():
    """상품별 당첨자 핸들 리스트 (전화번호 제외)"""
    with closing(get_conn()) as conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT product_name, handle
            FROM winners
            ORDER BY product_name, id;
            """
        )
        rows = cur.fetchall()

    grouped: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        grouped[row["product_name"]].append(row["handle"])
    return grouped


def find_pending_handle_for_user(username: str):
    """해당 유저 핸들이 winners 테이블에 있는지 확인"""
    if not username:
        return None
    handle = "@" + username if not username.startswith("@") else username

    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, product_name, handle
            FROM winners
            WHERE handle = %s
            ORDER BY id
            LIMIT 1;
            """,
            (handle,),
        )
        row = cur.fetchone()
    return row  # None 또는 (id, product_name, handle)


def update_phone_for_handle(handle: str, phone_number: str):
    if not handle.startswith("@"):
        handle = "@" + handle
    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE winners
               SET phone_number = %s
             WHERE handle = %s;
            """,
            (phone_number, handle),
        )


def get_winners_with_phones():
    """관리자용: 상품별 (handle, phone_number) 리스트"""
    with closing(get_conn()) as conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT product_name, handle, phone_number
            FROM winners
            ORDER BY product_name, id;
            """
        )
        rows = cur.fetchall()

    grouped: dict[str, list[tuple[str, str | None]]] = defaultdict(list)
    for row in rows:
        grouped[row["product_name"]].append(
            (row["handle"], row["phone_number"])
        )
    return grouped


# --------------------
# 텔레그램 봇 설정
# --------------------
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)

# 전화번호 입력 대기 상태: user_id -> handle
pending_phone_users: dict[int, str] = {}


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


# --------------------
# 명령어/헬프 텍스트
# --------------------
USER_HELP_TEXT = (
    "💡 사용 가능한 명령어 목록\n\n"
    "/start - 봇 소개 메시지\n"
    "/form - 구글 폼 링크 요청\n"
    "/list_winners - 상품별 당첨자 리스트 확인\n"
    "/submit_winner - (당첨자 전용) 전화번호 제출\n"
    "/help - 이 도움말 보기\n"
)

ADMIN_HELP_TEXT = (
    "\n\n🔒 관리자 전용 명령어\n"
    "/add_winner - 새로운 상품 및 당첨자 등록\n"
    "/delete_product_winners - 특정 상품의 당첨자 전체 삭제\n"
    "/delete_winner - 특정 당첨자 삭제\n"
    "/show_winners - 상품별 당첨자 전화번호 보기\n"
    "/clear_phones_product - 상품별 전화번호만 삭제\n"
    "/clear_phones_all - 전체 전화번호 삭제\n"
)


def build_help_text(user_id: int) -> str:
    text = USER_HELP_TEXT
    if is_admin(user_id):
        text += ADMIN_HELP_TEXT
    return text


# --------------------
# 핸들러들
# --------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    text = (
        "이 봇은 라쿤(@Kooncrypto) 라쿤님의 비서 라비입니다.\n"
        "아래 명령어를 클릭하여 실행하시면 됩니다.\n\n"
        + build_help_text(message.from_user.id)
    )
    await message.reply(text)


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    await message.reply(build_help_text(message.from_user.id))


@dp.message_handler(commands=["form"])
async def cmd_form(message: types.Message):
    await message.reply(f"📋 구글 폼 링크입니다.\n{FORM_URL}")


@dp.message_handler(commands=["list_winners"])
async def cmd_list_winners(message: types.Message):
    grouped = get_winners_grouped()
    if not grouped:
        await message.reply("아직 등록된 당첨자가 없습니다.")
        return

    lines = ["상품별 당첨자 목록:"]
    for product, handles in grouped.items():
        lines.append(f"\n{product}:")
        for idx, handle in enumerate(handles, start=1):
            lines.append(f"{idx}. {handle}")

    await message.reply("\n".join(lines))


# ---------- 관리자 전용 ----------


@dp.message_handler(commands=["add_winner"])
async def cmd_add_winner(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    await message.reply("상품명을 입력하세요.")
    dp.register_message_handler(
        process_add_winner_product,
        state=None,
        content_types=types.ContentTypes.TEXT,
        user_id=message.from_user.id,
    )


async def process_add_winner_product(message: types.Message):
    product_name = message.text.strip()
    if not product_name:
        await message.reply("상품명이 비어 있습니다. 다시 /add_winner 를 입력해주세요.")
        dp.unregister_message_handler(process_add_winner_product)
        return

    await message.reply(
        "당첨자 핸들을 한 줄에 하나씩 입력해주세요.\n"
        "입력이 끝나면 /end 를 입력하세요."
    )

    dp.register_message_handler(
        process_add_winner_handles,
        state=None,
        content_types=types.ContentTypes.TEXT,
        user_id=message.from_user.id,
        product_name=product_name,
    )
    dp.unregister_message_handler(process_add_winner_product)


async def process_add_winner_handles(message: types.Message, product_name: str):
    text = message.text.strip()
    if text == "/end":
        await message.reply("등록이 완료되었습니다.")
        dp.unregister_message_handler(process_add_winner_handles)
        return

    handles = [line.strip() for line in text.splitlines() if line.strip()]
    add_winners(product_name, handles)
    await message.reply(
        f"다음 당첨자들이 '{product_name}' 상품에 추가되었습니다.\n" + "\n".join(handles)
    )


@dp.message_handler(commands=["delete_product_winners"])
async def cmd_delete_product_winners(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    await message.reply("당첨자를 모두 삭제할 상품명을 입력해주세요.")
    dp.register_message_handler(
        process_delete_product_winners_product,
        state=None,
        content_types=types.ContentTypes.TEXT,
        user_id=message.from_user.id,
    )


async def process_delete_product_winners_product(message: types.Message):
    product_name = message.text.strip()
    if not product_name:
        await message.reply("상품명이 비어 있습니다. 다시 /delete_product_winners 를 입력해주세요.")
        dp.unregister_message_handler(process_delete_product_winners_product)
        return

    delete_product_winners(product_name)
    await message.reply(f"'{product_name}' 상품의 당첨자 정보가 모두 삭제되었습니다.")
    dp.unregister_message_handler(process_delete_product_winners_product)


@dp.message_handler(commands=["delete_winner"])
async def cmd_delete_winner(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    await message.reply("삭제할 당첨자의 텔레그램 핸들을 입력해주세요. (예: @example)")
    dp.register_message_handler(
        process_delete_winner_handle,
        state=None,
        content_types=types.ContentTypes.TEXT,
        user_id=message.from_user.id,
    )


async def process_delete_winner_handle(message: types.Message):
    handle = message.text.strip()
    if not handle:
        await message.reply("핸들이 비어 있습니다. 다시 /delete_winner 를 입력해주세요.")
        dp.unregister_message_handler(process_delete_winner_handle)
        return

    delete_winner_by_handle(handle)
    await message.reply(f"{handle} 당첨자 정보가 삭제되었습니다.")
    dp.unregister_message_handler(process_delete_winner_handle)


@dp.message_handler(commands=["show_winners"])
async def cmd_show_winners(message: types.Message):
    """관리자 전용: 상품별 핸들 + 전화번호"""
    if not is_admin(message.from_user.id):
        return

    grouped = get_winners_with_phones()
    if not grouped:
        await message.reply("아직 등록된 당첨자가 없습니다.")
        return

    lines = ["📦 상품별 당첨자 상세 목록 (관리자 전용)\n"]
    for product, items in grouped.items():
        lines.append(f"{product}:")
        for idx, (handle, phone) in enumerate(items, start=1):
            phone_display = phone if phone else "전화번호 미등록"
            lines.append(f"{idx}. {handle} - {phone_display}")
        lines.append("")  # 공백 줄

    await message.reply("\n".join(lines))


@dp.message_handler(commands=["clear_phones_all"])
async def cmd_clear_phones_all(message: types.Message):
    """모든 당첨자의 전화번호 초기화 (행은 유지)"""
    if not is_admin(message.from_user.id):
        return

    clear_all_phones()
    await message.reply("모든 상품의 당첨자 전화번호가 삭제되었습니다.")


@dp.message_handler(commands=["clear_phones_product"])
async def cmd_clear_phones_product(message: types.Message):
    """특정 상품의 전화번호만 초기화"""
    if not is_admin(message.from_user.id):
        return

    await message.reply("전화번호를 삭제할 상품명을 입력해주세요.")
    dp.register_message_handler(
        process_clear_phones_product,
        state=None,
        content_types=types.ContentTypes.TEXT,
        user_id=message.from_user.id,
    )


async def process_clear_phones_product(message: types.Message):
    product_name = message.text.strip()
    if not product_name:
        await message.reply("상품명이 비어 있습니다. 다시 /clear_phones_product 를 입력해주세요.")
        dp.unregister_message_handler(process_clear_phones_product)
        return

    clear_product_phones(product_name)
    await message.reply(f"'{product_name}' 상품의 당첨자 전화번호가 모두 삭제되었습니다.")
    dp.unregister_message_handler(process_clear_phones_product)


# ---------- 당첨자 전화번호 제출 ----------


def is_valid_phone(text: str) -> bool:
    # 010-1234-5678 형태만 허용
    pattern = r"^01[016789]-\d{3,4}-\d{4}$"
    return re.match(pattern, text) is not None


@dp.message_handler(commands=["submit_winner"])
async def cmd_submit_winner(message: types.Message):
    user = message.from_user
    if not user.username:
        await message.reply(
            "텔레그램 계정에 @사용자명(유저네임)이 설정되어 있어야 합니다.\n"
            "유저네임을 먼저 설정한 뒤 다시 시도해주세요."
        )
        return

    row = find_pending_handle_for_user(user.username)
    if not row:
        await message.reply(
            "당첨자 목록에서 회원님의 텔레그램 핸들을 찾지 못했습니다.\n"
            "관리자에게 당첨 여부를 먼저 확인해주세요."
        )
        return

    handle = row[2]
    pending_phone_users[user.id] = handle

    text = (
        "상품 발송을 위해 휴대폰 번호를 수집합니다.\n"
        "입력하신 정보는 상품 발송 후 관리자 명령어를 통해 즉시 삭제할 수 있습니다.\n\n"
        "동의하시면 아래 형식으로 휴대폰 번호를 입력해주세요.\n"
        "<code>010-1234-5678</code>"
    )
    await message.reply(text)


@dp.message_handler()
async def handle_phone_input(message: types.Message):
    user_id = message.from_user.id
    if user_id not in pending_phone_users:
        # 전화번호 입력 대기중이 아니면 무시
        return

    phone = message.text.strip()
    if not is_valid_phone(phone):
        await message.reply(
            "휴대폰 번호 형식이 올바르지 않습니다.\n"
            "예: <code>010-1234-5678</code>\n"
            "다시 입력해주세요."
        )
        return

    handle = pending_phone_users.pop(user_id)
    update_phone_for_handle(handle, phone)

    await message.reply(
        "전화번호가 정상적으로 등록되었습니다.✅\n"
        "상품 발송이 완료된 후 모든 개인정보는 일괄 삭제됩니다. 문자 메시지를 확인해주세요."
    )


# --------------------
# 시작
# --------------------
async def on_startup(dp_: Dispatcher):
    init_db()
    logger.info("DB 초기화 완료")


if __name__ == "__main__":
    init_db()
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
