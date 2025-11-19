import os
import logging
import re
from contextlib import closing
from collections import defaultdict

from dotenv import load_dotenv  # ← .env 로더 추가
load_dotenv()                   # ← .env 파일 읽기

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
            cur.execute(
                """
                INSERT INTO winners (product_name, handle)
                VALUES (%s, %s)
                ON CONFLICT (product_name, handle) DO NOTHING;
            """,
                (product_name, handle),
            )


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
            (product_name,),
        )


def get_winners_grouped():
    with closing(get_conn()) as conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT product_name, handle
            FROM winners
            ORDER BY product_name, id;
        """
        )
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
        cur.execute(
            """
            SELECT id, product_name, handle
            FROM winners
            WHERE handle = %s
            LIMIT 1;
        """,
            (handle,),
        )
        return cur.fetchone()


def update_phone_for_handle(handle, phone_number):
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
    with closing(get_conn()) as conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT product_name, handle, phone_number
            FROM winners
            ORDER BY product_name, id;
        """
        )
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

# 전화번호 제출 대기 유저: user_id -> handle
pending_phone_users = {}

# 관리자 상태: user_id -> dict(type=..., step=..., data=...)
admin_states = {}

# 봇 전체 ON/OFF 상태 (True = 동작, False = 유저 메시지 무시)
BOT_ACTIVE = True


def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS


def is_user_blocked(uid: int) -> bool:
    """
    봇이 OFF 상태이고, 그리고 관리자가 아닌 경우 → True (메시지 처리 막기)
    """
    return (not BOT_ACTIVE) and (uid not in ADMIN_IDS)


# --------------------
# Commands (일반 사용자)
# --------------------
@dp.message_handler(commands=["start"])
async def start_cmd(message: types.Message):
    if is_user_blocked(message.from_user.id):
        return
    await message.reply("봇이 정상적으로 작동 중입니다.\n/help 로 명령어를 확인하세요.")


@dp.message_handler(commands=["help"])
async def help_cmd(message: types.Message):
    uid = message.from_user.id

    USER_HELP = (
        "/start - 봇 상태 확인\n"
        "/form - 구글 폼 링크 안내\n"
        "/list_winners - 상품별 당첨자 목록\n"
        "/submit_winner - 본인 전화번호 제출\n"
    )

    ADMIN_HELP = (
        "\n[관리자 전용]\n"
        "/add_winner - 상품/핸들 등록\n"
        "/delete_product_winners - 상품별 당첨자 전체 삭제\n"
        "/delete_winner - 특정 핸들 삭제\n"
        "/show_winners - 상세 당첨자+전화번호 조회\n"
        "/clear_phones_product - 특정 상품 전화번호만 삭제\n"
        "/clear_phones_all - 전체 전화번호 삭제\n"
        "/bot_on - 봇 동작 재개\n"
        "/bot_off - 봇 동작 일시 중지\n"
        "/bot_status - 봇 상태 확인\n"
    )

    text = USER_HELP + (ADMIN_HELP if is_admin(uid) else "")
    await message.reply(text)


@dp.message_handler(commands=["form"])
async def form_cmd(message: types.Message):
    if is_user_blocked(message.from_user.id):
        return
    await message.reply(f"폼 링크:\n{FORM_URL}")


@dp.message_handler(commands=["list_winners"])
async def list_cmd(message: types.Message):
    if is_user_blocked(message.from_user.id):
        return

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
PHONE_PATTERN = re.compile(r"^01[016789]-\d{3,4}-\d{4}$")


def is_valid_phone(text: str) -> bool:
    return bool(PHONE_PATTERN.match(text))


@dp.message_handler(commands=["submit_winner"])
async def submit_cmd(message: types.Message):
    if is_user_blocked(message.from_user.id):
        return

    user = message.from_user
    if not user.username:
        await message.reply(
            "유저네임(@username)이 필요합니다.\n텔레그램 설정에서 유저네임을 먼저 설정해주세요."
        )
        return

    row = find_pending_handle_for_user(user.username)
    if not row:
        await message.reply("당첨자 명단에 없습니다.")
        return

    pending_phone_users[user.id] = row[2]  # handle

    # ← 여기 문자열 구조가 문제였어서 안전하게 분리
    await message.reply(
        "축하드립니다! 상품 전달을 위해 휴대폰 번호 제출에 동의하시는 경우 번호를 입력해주세요.\n"
        "제출된 개인정보는 상품 발송 목적 외에는 사용되지 않으며, 발송 완료 후 즉시 삭제됩니다.\n\n"
        "예: 010-1234-5678"
    )


# --------------------
# 관리자: 봇 ON/OFF/STATUS
# --------------------
@dp.message_handler(commands=["bot_off"])
async def bot_off_cmd(message: types.Message):
    global BOT_ACTIVE
    uid = message.from_user.id
    if not is_admin(uid):
        return

    BOT_ACTIVE = False
    await message.reply("📴 봇 동작이 일시 중지되었습니다.\n(관리자 명령어는 계속 사용 가능합니다.)")


@dp.message_handler(commands=["bot_on"])
async def bot_on_cmd(message: types.Message):
    global BOT_ACTIVE
    uid = message.from_user.id
    if not is_admin(uid):
        return

    BOT_ACTIVE = True
    await message.reply("🟢 봇 동작이 다시 활성화되었습니다.")


@dp.message_handler(commands=["bot_status"])
async def bot_status_cmd(message: types.Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return

    status = "ON (동작 중)" if BOT_ACTIVE else "OFF (일시 중지)"
    await message.reply(f"현재 봇 상태: {status}")


# --------------------
# 관리자 명령어 (상태 기반 플로우)
# --------------------
@dp.message_handler(commands=["add_winner"])
async def add_winner_cmd(message: types.Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return

    admin_states[uid] = {
        "type": "add_winner",
        "step": "product_name",
        "product_name": None,
    }
    await message.reply("상품명을 입력하세요.")


@dp.message_handler(commands=["delete_product_winners"])
async def delete_product_cmd(message: types.Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return

    admin_states[uid] = {
        "type": "delete_product",
        "step": "product_name",
    }
    await message.reply("삭제할 상품명을 입력하세요.")


@dp.message_handler(commands=["delete_winner"])
async def delete_winner_cmd(message: types.Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return

    admin_states[uid] = {
        "type": "delete_winner",
        "step": "handle",
    }
    await message.reply("삭제할 핸들을 입력하세요. (예: @username)")


@dp.message_handler(commands=["show_winners"])
async def show_winners_cmd(message: types.Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return

    grouped = get_winners_with_phones()
    if not grouped:
        await message.reply("데이터 없음.")
        return

    text = "📦 상세 당첨자 목록\n\n"
    for prod, items in grouped.items():
        text += f"{prod}:\n"
        for handle, phone in items:
            phone_display = phone if phone else "전화번호 없음"
            text += f"- {handle} / {phone_display}\n"
        text += "\n"

    await message.reply(text)


@dp.message_handler(commands=["clear_phones_all"])
async def clear_all_cmd(message: types.Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return
    clear_all_phones()
    await message.reply("전체 전화번호가 삭제되었습니다.")


@dp.message_handler(commands=["clear_phones_product"])
async def clear_phones_product_cmd(message: types.Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return

    admin_states[uid] = {
        "type": "clear_phones_product",
        "step": "product_name",
    }
    await message.reply("전화번호를 삭제할 상품명을 입력하세요.")


# --------------------
# 공통 텍스트 핸들러 (전화번호 + 관리자 상태)
# --------------------
@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def text_handler(message: types.Message):
    uid = message.from_user.id
    text = message.text.strip()

    # 봇이 OFF 상태면, 관리자만 계속 처리
    if is_user_blocked(uid):
        return

    # 1) 전화번호 입력 대기 상태인 경우
    if uid in pending_phone_users:
        phone = text
        if not is_valid_phone(phone):
            await message.reply("형식 오류! 예: 010-1234-5678")
            return

        handle = pending_phone_users.pop(uid)
        update_phone_for_handle(handle, phone)
        await message.reply("전화번호가 등록되었습니다.")
        return

    # 2) 관리자 상태 처리
    state = admin_states.get(uid)
    if not state:
        # 별도의 상태가 없는 일반 텍스트는 무시
        return

    stype = state.get("type")
    step = state.get("step")

    # add_winner 플로우
    if stype == "add_winner":
        if step == "product_name":
            state["product_name"] = text
            state["step"] = "handles"
            await message.reply(
                "당첨자 핸들을 한 줄에 하나씩 입력하세요.\n"
                "입력을 마치려면 /end 를 보내주세요."
            )
            return

        if step == "handles":
            if text == "/end":
                admin_states.pop(uid, None)
                await message.reply("등록을 완료했습니다.")
                return

            product_name = state.get("product_name")
            handles = [h.strip() for h in text.splitlines() if h.strip()]
            add_winners(product_name, handles)

            await message.reply("\n".join(handles) + "\n위 핸들이 추가되었습니다.")
            return

    # delete_product 플로우
    elif stype == "delete_product" and step == "product_name":
        product_name = text
        delete_product_winners(product_name)
        admin_states.pop(uid, None)
        await message.reply(f"'{product_name}' 상품의 당첨자가 모두 삭제되었습니다.")
        return

    # delete_winner 플로우
    elif stype == "delete_winner" and step == "handle":
        handle = text
        delete_winner_by_handle(handle)
        admin_states.pop(uid, None)
        await message.reply(f"{handle} 삭제되었습니다.")
        return

    # clear_phones_product 플로우
    elif stype == "clear_phones_product" and step == "product_name":
        product_name = text
        clear_product_phones(product_name)
        admin_states.pop(uid, None)
        await message.reply(f"'{product_name}' 상품의 전화번호가 모두 삭제되었습니다.")
        return

    # 그 외는 상태 초기화
    admin_states.pop(uid, None)


# --------------------
# 시작
# --------------------
async def on_startup(dp: Dispatcher):
    init_db()
    logger.info("DB 초기화 완료")


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
