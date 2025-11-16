import logging
import os
import re
from contextlib import closing

import psycopg2
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

# -------------------
# 환경변수
# -------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")  # Supabase postgres://... URI
FORM_URL = os.getenv("FORM_URL", "")

if not BOT_TOKEN:
    raise RuntimeError("환경변수 BOT_TOKEN 이 설정되지 않았습니다.")
if not DATABASE_URL:
    raise RuntimeError("환경변수 DATABASE_URL 이 설정되지 않았습니다.")

# ADMIN_IDS 예: "123456789,987654321"
_admin_ids_raw = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = set()
for x in _admin_ids_raw.split(","):
    x = x.strip()
    if x:
        try:
            ADMIN_IDS.add(int(x))
        except ValueError:
            pass

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# 간단한 상태 관리용
user_states = {}  # {user_id: {"mode": "...", ...}}


# -------------------
# DB 연결 / 초기화
# -------------------
def get_conn():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn


def init_db():
    with closing(get_conn()) as conn:
        with conn.cursor() as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS products (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE
                );
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS winners (
                    id SERIAL PRIMARY KEY,
                    product_id INTEGER REFERENCES products(id) ON DELETE CASCADE,
                    telegram_handle TEXT,
                    phone_number TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )


# -------------------
# DB 유틸 함수
# -------------------
def normalize_handle(handle: str) -> str:
    h = handle.strip()
    if not h:
        return ""
    if h.startswith("@"):
        h = h[1:]
    return "@" + h.lower()


def get_or_create_product(name: str) -> int:
    name = name.strip()
    with closing(get_conn()) as conn:
        with conn.cursor() as c:
            c.execute("SELECT id FROM products WHERE name = %s", (name,))
            row = c.fetchone()
            if row:
                return row[0]
            c.execute("INSERT INTO products (name) VALUES (%s) RETURNING id", (name,))
            product_id = c.fetchone()[0]
            return product_id


def add_winner_to_db(product_name: str, handle: str):
    handle = normalize_handle(handle)
    if not handle:
        return
    product_id = get_or_create_product(product_name)
    with closing(get_conn()) as conn:
        with conn.cursor() as c:
            c.execute(
                """
                SELECT id FROM winners
                WHERE product_id = %s AND telegram_handle = %s
                """,
                (product_id, handle),
            )
            if c.fetchone():
                return
            c.execute(
                """
                INSERT INTO winners (product_id, telegram_handle)
                VALUES (%s, %s)
                """,
                (product_id, handle),
            )


def list_all_winners():
    with closing(get_conn()) as conn:
        with conn.cursor() as c:
            c.execute(
                """
                SELECT p.name, w.telegram_handle
                FROM winners w
                JOIN products p ON p.id = w.product_id
                ORDER BY p.name, w.id
                """
            )
            rows = c.fetchall()

    result = {}
    for product_name, handle in rows:
        result.setdefault(product_name, []).append(handle)
    return result


def delete_product_winners(product_name: str) -> int:
    with closing(get_conn()) as conn:
        with conn.cursor() as c:
            c.execute("SELECT id FROM products WHERE name = %s", (product_name.strip(),))
            row = c.fetchone()
            if not row:
                return 0
            product_id = row[0]
            c.execute("DELETE FROM winners WHERE product_id = %s", (product_id,))
            deleted = c.rowcount
            return deleted


def delete_one_winner(product_name: str, handle: str) -> int:
    handle = normalize_handle(handle)
    with closing(get_conn()) as conn:
        with conn.cursor() as c:
            c.execute("SELECT id FROM products WHERE name = %s", (product_name.strip(),))
            row = c.fetchone()
            if not row:
                return 0
            product_id = row[0]
            c.execute(
                """
                DELETE FROM winners
                WHERE product_id = %s AND telegram_handle = %s
                """,
                (product_id, handle),
            )
            deleted = c.rowcount
            return deleted


def winner_exists_handle(handle: str) -> bool:
    """상품 구분 없이, 이 텔레그램 핸들이 당첨자로 등록돼 있는지 확인"""
    handle = normalize_handle(handle)
    with closing(get_conn()) as conn:
        with conn.cursor() as c:
            c.execute(
                """
                SELECT 1 FROM winners
                WHERE telegram_handle = %s
                """,
                (handle,),
            )
            return c.fetchone() is not None


def update_phone_for_handle(handle: str, phone: str):
    """해당 텔레그램 핸들의 모든 상품 레코드에 전화번호 저장"""
    handle = normalize_handle(handle)
    with closing(get_conn()) as conn:
        with conn.cursor() as c:
            c.execute(
                """
                UPDATE winners
                SET phone_number = %s
                WHERE telegram_handle = %s
                """,
                (phone, handle),
            )


# -------------------
# 유틸
# -------------------
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def phone_valid(phone: str) -> bool:
    # 010-1234-5678 형식
    return bool(re.fullmatch(r"\d{3}-\d{4}-\d{4}", phone.strip()))


# -------------------
# 기본 명령어
# -------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    text = (
        "이 봇은 이벤트 상품 발송을 위한 당첨자 관리 봇입니다.\n"
        "아래 명령어를 사용해 주세요.\n\n"
        "💡 사용 가능한 명령어\n"
        "/start - 안내 메시지 보기\n"
        "/form - 구글 폼 링크 요청\n"
        "/list_winners - 상품별 당첨자 리스트 확인\n"
        "/submit_winner - 상품 발송을 위한 전화번호 제출\n"
        "/help - 명령어 설명 보기"
    )
    await message.answer(text)


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    base = (
        "💡 사용 가능한 명령어\n"
        "/start - 안내 메시지 보기\n"
        "/form - 구글 폼 링크 요청\n"
        "/list_winners - 상품별 당첨자 리스트 확인\n"
        "/submit_winner - 상품 발송을 위한 전화번호 제출\n"
        "/help - 명령어 설명 보기\n"
    )
    if is_admin(message.from_user.id):
        admin_text = (
            "\n🔒 관리자 전용 명령어\n"
            "/add_winner - 새로운 상품 및 당첨자 등록\n"
            "/delete_winner - 특정 당첨자 삭제\n"
            "/delete_product_winners - 상품별 당첨자 전체 삭제\n"
        )
        await message.answer(base + admin_text)
    else:
        await message.answer(base)


@dp.message_handler(commands=["form"])
async def cmd_form(message: types.Message):
    if not FORM_URL:
        await message.answer("아직 구글 폼 링크가 설정되지 않았습니다.")
    else:
        await message.answer(f"구글 폼 링크입니다:\n{FORM_URL}")


@dp.message_handler(commands=["list_winners"])
async def cmd_list_winners(message: types.Message):
    data = list_all_winners()
    if not data:
        await message.answer("등록된 당첨자 목록이 없습니다.")
        return

    lines = ["상품별 당첨자 목록:"]
    for product, handles in data.items():
        lines.append(f"\n{product}:")
        for i, h in enumerate(handles, start=1):
            lines.append(f"{i}. {h}")
    await message.answer("\n".join(lines))


# -------------------
# 관리자 전용 명령어
# -------------------
@dp.message_handler(commands=["add_winner"])
async def cmd_add_winner(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    user_states[message.from_user.id] = {"mode": "add_product"}
    await message.answer("상품명을 입력하세요.")


@dp.message_handler(commands=["delete_product_winners"])
async def cmd_delete_product_winners(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    user_states[message.from_user.id] = {"mode": "delete_product"}
    await message.answer("당첨자를 모두 삭제할 상품명을 입력하세요.")


@dp.message_handler(commands=["delete_winner"])
async def cmd_delete_winner(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    user_states[message.from_user.id] = {"mode": "delete_one_product"}
    await message.answer("당첨자를 삭제할 상품명을 입력하세요.")


# -------------------
# 유저용: /submit_winner (상품명 묻지 않고 바로 핸들 확인 → 전화번호 입력)
# -------------------
@dp.message_handler(commands=["submit_winner"])
async def cmd_submit_winner(message: types.Message):
    if not message.from_user.username:
        await message.answer(
            "당첨자 확인을 위해 텔레그램 @username 이 필요합니다.\n"
            "설정에서 사용자 이름을 등록한 후 다시 시도해 주세요."
        )
        return

    username = message.from_user.username
    handle = "@" + username.lower()

    # 이 핸들이 winners 테이블에 존재하는지(어떤 상품이든) 확인
    if not winner_exists_handle(handle):
        await message.answer(
            "당첨자 명단에서 텔레그램 핸들을 찾을 수 없습니다.\n"
            "이벤트 공지의 당첨자 리스트를 다시 확인해 주세요."
        )
        return

    # 존재하면 바로 전화번호 입력 단계로
    user_states[message.from_user.id] = {"mode": "submit_phone"}

    await message.answer(
        "상품 발송을 위해 전화번호가 필요합니다.\n\n"
        "[개인정보 안내]\n"
        "- 수집 항목: 전화번호\n"
        "- 이용 목적: 당첨 확인 및 상품 발송\n"
        "- 보관 기간: 상품 발송 완료 후 즉시 삭제\n"
        "- 동의하지 않으셔도 되지만, 이 경우 상품 발송이 어렵습니다.\n\n"
        "위 내용에 동의하시면 아래 형식으로 전화번호를 입력해주세요.\n"
        "예시) 010-1234-5678"
    )


# -------------------
# 상태 기반 텍스트 처리
# -------------------
@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_text(message: types.Message):
    state = user_states.get(message.from_user.id)
    if not state:
        return

    mode = state.get("mode")

    # 1) /add_winner – 상품명 입력
    if mode == "add_product":
        product_name = message.text.strip()
        if not product_name:
            await message.answer("상품명이 비어 있습니다. 다시 입력해 주세요.")
            return
        state["product_name"] = product_name
        state["mode"] = "add_handles"
        state["handles"] = []
        await message.answer(
            "당첨자 핸들을 입력하세요. (@포함, 한 줄에 하나씩)\n"
            "모두 입력한 후에는 /end 를 입력하면 완료됩니다."
        )
        return

    # 1-2) /add_winner – 핸들 입력
    if mode == "add_handles":
        if message.text.strip() == "/end":
            product_name = state["product_name"]
            handles = state["handles"]
            if not handles:
                await message.answer("등록된 핸들이 없습니다. /add_winner 부터 다시 시도해 주세요.")
            else:
                for h in handles:
                    add_winner_to_db(product_name, h)
                await message.answer("등록이 완료되었습니다.")
            user_states.pop(message.from_user.id, None)
            return

        lines = message.text.splitlines()
        for line in lines:
            h = line.strip()
            if h:
                state["handles"].append(h)
        await message.answer("추가 등록되었습니다. 더 입력하거나 /end 로 완료해 주세요.")
        return

    # 2) /delete_product_winners – 상품 전체 삭제
    if mode == "delete_product":
        product_name = message.text.strip()
        deleted = delete_product_winners(product_name)
        await message.answer(f"{product_name} 상품의 당첨자 {deleted}명을 삭제했습니다.")
        user_states.pop(message.from_user.id, None)
        return

    # 3) /delete_winner – 상품명 먼저
    if mode == "delete_one_product":
        product_name = message.text.strip()
        if not product_name:
            await message.answer("상품명이 비어 있습니다. 다시 입력해 주세요.")
            return
        state["product_name"] = product_name
        state["mode"] = "delete_one_handle"
        await message.answer("삭제할 당첨자의 텔레그램 핸들을 입력하세요. (@포함)")
        return

    # 3-2) /delete_winner – 핸들 삭제
    if mode == "delete_one_handle":
        product_name = state["product_name"]
        handle = message.text.strip()
        deleted = delete_one_winner(product_name, handle)
        if deleted:
            await message.answer(f"{product_name} 상품에서 {handle} 당첨자를 삭제했습니다.")
        else:
            await message.answer(
                f"{product_name} 상품에서 {handle} 당첨자를 찾을 수 없습니다."
            )
        user_states.pop(message.from_user.id, None)
        return

    # 4) /submit_winner – 전화번호 입력
    if mode == "submit_phone":
        phone = message.text.strip()
        if not phone_valid(phone):
            await message.answer(
                "⚠️ 올바른 전화번호 형식이 아닙니다.\n\n"
                "아래 예시처럼 다시 입력해주세요.\n"
                "예시) 010-1234-5678"
            )
            return

        if not message.from_user.username:
            await message.answer(
                "당첨자 확인을 위해 텔레그램 @username 이 필요합니다.\n"
                "설정에서 사용자 이름을 등록한 후 다시 시도해 주세요."
            )
            user_states.pop(message.from_user.id, None)
            return

        username = message.from_user.username
        handle = "@" + username.lower()

        # 안전하게 한 번 더 당첨자 여부 확인
        if not winner_exists_handle(handle):
            await message.answer(
                "당첨자 명단에서 당신의 텔레그램 핸들을 찾을 수 없습니다.\n"
                "이벤트 공지의 당첨자 리스트를 다시 확인해 주세요."
            )
            user_states.pop(message.from_user.id, None)
            return

        update_phone_for_handle(handle, phone)

        await message.answer(
            "전화번호가 정상적으로 제출되었습니다. ✅\n"
            "상품 발송이 완료되면, 제출해 주신 전화번호는 즉시 삭제됩니다.\n"
            "좋은 하루 되세요:)"
        )
        user_states.pop(message.from_user.id, None)
        return


if __name__ == "__main__":
    init_db()
    executor.start_polling(dp, skip_updates=True)
