import logging
import os
import re
import sqlite3
from contextlib import closing

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

# -------------------
# 환경변수 설정
# -------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("환경변수 BOT_TOKEN 이 설정되지 않았습니다.")

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

FORM_URL = os.getenv("FORM_URL", "")  # /form 에서 보내줄 구글폼 주소
DB_PATH = os.getenv("DB_PATH", "winners.db")  # Railway Volume 쓰면 /data/winners.db 로 설정

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# 간단한 상태 관리 (유저별 대화 흐름용)
user_states = {}  # {user_id: {"mode": "...", ...}}


# -------------------
# DB 관련 함수
# -------------------
def init_db():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE
            )
        """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS winners (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER,
                telegram_handle TEXT,
                phone_number TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        conn.commit()


def normalize_handle(handle: str) -> str:
    h = handle.strip()
    if not h:
        return ""
    if h.startswith("@"):
        h = h[1:]
    # 전부 소문자로 통일
    return "@" + h.lower()


def get_or_create_product(conn, name: str) -> int:
    name = name.strip()
    c = conn.cursor()
    c.execute("SELECT id FROM products WHERE name = ?", (name,))
    row = c.fetchone()
    if row:
        return row[0]
    c.execute("INSERT INTO products (name) VALUES (?)", (name,))
    conn.commit()
    return c.lastrowid


def add_winner_to_db(product_name: str, handle: str):
    handle = normalize_handle(handle)
    if not handle:
        return
    with closing(sqlite3.connect(DB_PATH)) as conn:
        product_id = get_or_create_product(conn, product_name)
        c = conn.cursor()
        # 중복 방지
        c.execute(
            "SELECT id FROM winners WHERE product_id = ? AND telegram_handle = ?",
            (product_id, handle),
        )
        if c.fetchone():
            return
        c.execute(
            "INSERT INTO winners (product_id, telegram_handle) VALUES (?, ?)",
            (product_id, handle),
        )
        conn.commit()


def list_all_winners():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        c = conn.cursor()
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
    with closing(sqlite3.connect(DB_PATH)) as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM products WHERE name = ?", (product_name.strip(),))
        row = c.fetchone()
        if not row:
            return 0
        product_id = row[0]
        c.execute("DELETE FROM winners WHERE product_id = ?", (product_id,))
        deleted = c.rowcount
        conn.commit()
        return deleted


def delete_one_winner(product_name: str, handle: str) -> int:
    handle = normalize_handle(handle)
    with closing(sqlite3.connect(DB_PATH)) as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM products WHERE name = ?", (product_name.strip(),))
        row = c.fetchone()
        if not row:
            return 0
        product_id = row[0]
        c.execute(
            "DELETE FROM winners WHERE product_id = ? AND telegram_handle = ?",
            (product_id, handle),
        )
        deleted = c.rowcount
        conn.commit()
        return deleted


def winner_exists(product_name: str, handle: str) -> bool:
    handle = normalize_handle(handle)
    with closing(sqlite3.connect(DB_PATH)) as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM products WHERE name = ?", (product_name.strip(),))
        row = c.fetchone()
        if not row:
            return False
        product_id = row[0]
        c.execute(
            "SELECT 1 FROM winners WHERE product_id = ? AND telegram_handle = ?",
            (product_id, handle),
        )
        return c.fetchone() is not None


def update_phone(product_name: str, handle: str, phone: str):
    handle = normalize_handle(handle)
    with closing(sqlite3.connect(DB_PATH)) as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM products WHERE name = ?", (product_name.strip(),))
        row = c.fetchone()
        if not row:
            return
        product_id = row[0]
        c.execute(
            """
            UPDATE winners
            SET phone_number = ?
            WHERE product_id = ? AND telegram_handle = ?
        """,
            (phone, product_id, handle),
        )
        conn.commit()


# -------------------
# 유틸
# -------------------
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def phone_valid(phone: str) -> bool:
    # 010-1234-5678 형식
    return bool(re.fullmatch(r"\d{3}-\d{4}-\d{4}", phone.strip()))


# -------------------
# 명령어 핸들러
# -------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    text = (
        "이 봇은 경품 이벤트 당첨자 관리를 위한 봇입니다.\n"
        "아래 명령어를 사용해 주세요.\n\n"
        "💡 사용 가능한 명령어\n"
        "/start - 안내 메시지 보기\n"
        "/form - 구글 폼 링크 요청\n"
        "/list_winners - 상품별 당첨자 리스트 확인\n"
        "/submit_winner - 당첨자 정보(전화번호) 제출\n"
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
        "/submit_winner - 당첨자 정보(전화번호) 제출\n"
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
# 관리자 전용: /add_winner
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
# 유저용: /submit_winner
# -------------------
@dp.message_handler(commands=["submit_winner"])
async def cmd_submit_winner(message: types.Message):
    if not message.from_user.username:
        await message.answer(
            "당첨자 확인을 위해 텔레그램 @username 이 필요합니다.\n"
            "설정에서 사용자 이름을 등록한 후 다시 시도해 주세요."
        )
        return

    user_states[message.from_user.id] = {"mode": "submit_product"}
    await message.answer("참여한 상품명을 입력해주세요.\n예시) 소프트콘, 커피, 초코송이")


# -------------------
# 상태 기반 일반 메시지 처리
# -------------------
@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_text(message: types.Message):
    state = user_states.get(message.from_user.id)
    if not state:
        return  # 아무 상태도 아닐 때는 무시

    mode = state.get("mode")

    # 1) /add_winner – 상품명 받기
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

    # 1-2) /add_winner – 핸들들 받기
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

        # 핸들 누적
        lines = message.text.splitlines()
        for line in lines:
            h = line.strip()
            if h:
                state["handles"].append(h)
        await message.answer("추가 등록되었습니다. 더 입력하거나 /end 로 완료해 주세요.")
        return

    # 2) /delete_product_winners – 상품명 받고 삭제
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

    # 3-2) /delete_winner – 핸들 입력 후 삭제
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

    # 4) /submit_winner – 상품명 받기
    if mode == "submit_product":
        product_name = message.text.strip()
        username = message.from_user.username
        handle = "@" + username.lower()

        if not winner_exists(product_name, handle):
            await message.answer(
                "당첨자 명단에서 당신의 텔레그램 핸들을 찾을 수 없습니다.\n"
                "이벤트 공지의 당첨자 리스트를 다시 확인해 주세요."
            )
            user_states.pop(message.from_user.id, None)
            return

        # 상품명 저장 후 전화번호 요청
        state["product_name"] = product_name
        state["mode"] = "submit_phone"
        await message.answer(
            "경품 발송을 위해 전화번호가 필요합니다.\n\n"
            "[개인정보 안내]\n"
            "- 수집 항목: 전화번호\n"
            "- 이용 목적: 당첨 확인 및 경품 발송\n"
            "- 보관 기간: 경품 발송 완료 후 즉시 삭제\n"
            "- 동의하지 않으셔도 되지만, 이 경우 경품 발송이 어렵습니다.\n\n"
            "위 내용에 동의하시면 아래 형식으로 전화번호를 입력해주세요.\n"
            "예시) 010-1234-5678"
        )
        return

    # 4-2) /submit_winner – 전화번호 받기
    if mode == "submit_phone":
        phone = message.text.strip()
        if not phone_valid(phone):
            await message.answer(
                "⚠️ 올바른 전화번호 형식이 아닙니다.\n\n"
                "아래 예시처럼 다시 입력해주세요.\n"
                "예시) 010-1234-5678"
            )
            return

        product_name = state["product_name"]
        username = message.from_user.username
        handle = "@" + username.lower()

        update_phone(product_name, handle, phone)
        await message.answer(
            "전화번호가 정상적으로 제출되었습니다. ✅\n"
            "경품 발송이 완료되면, 제출해 주신 전화번호는 즉시 삭제됩니다.\n"
            "참여해 주셔서 감사합니다!"
        )
        user_states.pop(message.from_user.id, None)
        return


if __name__ == "__main__":
    init_db()
    executor.start_polling(dp, skip_updates=True)
