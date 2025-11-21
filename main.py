import os
import logging
import re
import io
import csv
import random
from contextlib import closing
from collections import defaultdict
from datetime import datetime, timedelta

from dotenv import load_dotenv
load_dotenv()

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2 import IntegrityError

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.dispatcher.filters import Command # Command 필터 사용을 위해 추가

# --------------------
# 환경 변수
# --------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
FORM_URL = os.getenv("FORM_URL", "https://forms.gle/your-form-url")

# 환경 변수에서 관리자 ID를 가져오던 기존 로직은 DB 로직으로 대체됨
ADMIN_IDS = [] 

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


def load_admin_ids():
    """DB에서 관리자 ID를 ADMIN_IDS 전역 변수에 로드"""
    global ADMIN_IDS
    ADMIN_IDS.clear()
    try:
        with closing(get_conn()) as conn, conn.cursor() as cur:
            cur.execute("SELECT user_id FROM admins;")
            ADMIN_IDS.extend([row[0] for row in cur.fetchall()])
            logger.info(f"관리자 ID 로드 완료: {ADMIN_IDS}")
    except Exception as e:
        logger.error(f"관리자 ID 로드 중 오류 발생: {e}")


def init_db():
    with closing(get_conn()) as conn, conn.cursor() as cur:
        # winners 테이블 (기존)
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
        
        # admins 테이블 (관리자 ID 관리)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                added_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )

        # lotteries 테이블 (진행 중인 추첨 관리)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS lotteries (
                chat_id BIGINT PRIMARY KEY,
                start_time TIMESTAMPTZ DEFAULT NOW(),
                duration_minutes INTEGER,
                winner_count INTEGER,
                required_groups TEXT,
                state TEXT DEFAULT 'ACTIVE',
                message_id BIGINT
            );
            """
        )

        # lottery_participants 테이블 (추첨 참가자)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS lottery_participants (
                chat_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                username TEXT,
                joined_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (chat_id, user_id)
            );
            """
        )
        
        # admin_config 테이블 (관리자별 설정 저장)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_config (
                user_id BIGINT PRIMARY KEY,
                required_groups TEXT
            );
            """
        )

        load_admin_ids()
        logger.info("DB 스키마 초기화 완료 및 관리자 ID 로드 완료")


# --- 당첨자 관리 함수 (기존) ---

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

def change_product_name_for_handle(handle, new_product_name):
    """특정 핸들의 상품명을 변경합니다."""
    if not handle.startswith("@"):
        handle = "@" + handle
    with closing(get_conn()) as conn, conn.cursor() as cur:
        # 변경하려는 상품명과 기존 핸들 조합이 이미 존재하는지 확인 (UNIQUE 제약 조건 위반 방지)
        cur.execute(
            """
            SELECT 1 FROM winners WHERE product_name = %s AND handle = %s;
            """,
            (new_product_name, handle),
        )
        if cur.fetchone():
            return False  # 이미 해당 상품에 등록된 핸들이 있음

        cur.execute(
            """
            UPDATE winners
            SET product_name = %s
            WHERE handle = %s
            RETURNING id;
            """,
            (new_product_name, handle),
        )
        return cur.rowcount > 0


def get_winners_grouped():
    """전화번호 여부 상관없이 전체 (list_winners용)"""
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


def get_winners_with_phones_grouped():
    """전화번호 여부 포함 전체 (show_winners용)"""
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


def get_winners_with_phone_only():
    """전화번호를 제출한 사람만"""
    with closing(get_conn()) as conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT product_name, handle, phone_number
            FROM winners
            WHERE phone_number IS NOT NULL
            ORDER BY product_name, id;
            """
        )
        rows = cur.fetchall()

    grouped = defaultdict(list)
    for row in rows:
        grouped[row["product_name"]].append((row["handle"], row["phone_number"]))
    return grouped


def get_winners_without_phone_only():
    """전화번호를 아직 제출하지 않은 사람만"""
    with closing(get_conn()) as conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT product_name, handle
            FROM winners
            WHERE phone_number IS NULL
            ORDER BY product_name, id;
            """
        )
        rows = cur.fetchall()

    grouped = defaultdict(list)
    for row in rows:
        grouped[row["product_name"]].append(row["handle"])
    return grouped


def get_all_rows_for_export():
    """CSV 내보내기용 전체 데이터"""
    with closing(get_conn()) as conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT id, product_name, handle, phone_number, created_at
            FROM winners
            ORDER BY product_name, id;
            """
        )
        return cur.fetchall()


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


# --- 관리자 명단 관리 함수 ---

def add_admin_to_db(user_id: int, username: str):
    with closing(get_conn()) as conn, conn.cursor() as cur:
        try:
            cur.execute(
                "INSERT INTO admins (user_id, username) VALUES (%s, %s) ON CONFLICT (user_id) DO NOTHING;",
                (user_id, username),
            )
            load_admin_ids()
        except Exception as e:
            logger.error(f"관리자 추가 오류: {e}")

def delete_admin_from_db(user_id: int):
    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM admins WHERE user_id = %s;", (user_id,))
        load_admin_ids()

def get_all_admin_ids():
    with closing(get_conn()) as conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT user_id, username FROM admins ORDER BY added_at;")
        return cur.fetchall()

# --- 관리자 설정 (필수 그룹) 관리 함수 ---

def set_admin_required_groups(user_id: int, groups_str: str):
    """관리자의 기본 필수 그룹 설정을 저장/업데이트합니다."""
    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO admin_config (user_id, required_groups)
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO UPDATE SET required_groups = EXCLUDED.required_groups;
            """,
            (user_id, groups_str)
        )

def get_admin_required_groups(user_id: int) -> str:
    """관리자의 기본 필수 그룹 설정을 불러옵니다."""
    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT required_groups FROM admin_config WHERE user_id = %s;",
            (user_id,)
        )
        result = cur.fetchone()
        return result[0] if result and result[0] else ""

# --- 추첨 관련 DB 함수 ---

def get_current_lottery(chat_id: int):
    """현재 진행 중인 추첨 정보를 가져옵니다."""
    with closing(get_conn()) as conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            "SELECT * FROM lotteries WHERE chat_id = %s AND state = 'ACTIVE';",
            (chat_id,)
        )
        return cur.fetchone()


def start_new_lottery(chat_id: int, duration: int, winner_count: int, required_groups: str, message_id: int):
    """새로운 추첨을 시작합니다. 이미 진행 중인 경우 False를 반환합니다."""
    if get_current_lottery(chat_id):
        return False

    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO lotteries (chat_id, duration_minutes, winner_count, required_groups, message_id)
            VALUES (%s, %s, %s, %s, %s);
            """,
            (chat_id, duration, winner_count, required_groups, message_id)
        )
        return True


def end_lottery(chat_id: int):
    """추첨을 비활성화 상태로 변경합니다."""
    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE lotteries SET state = 'ENDED' WHERE chat_id = %s AND state = 'ACTIVE';",
            (chat_id,)
        )


def add_participant(chat_id: int, user_id: int, username: str):
    """추첨 참가자를 추가합니다. 이미 참가한 경우 False를 반환합니다."""
    with closing(get_conn()) as conn, conn.cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO lottery_participants (chat_id, user_id, username)
                VALUES (%s, %s, %s);
                """,
                (chat_id, user_id, username)
            )
            return True
        except IntegrityError:
            return False


def get_participants(chat_id: int):
    """현재 추첨의 참가자 목록을 가져옵니다."""
    with closing(get_conn()) as conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            "SELECT user_id, username FROM lottery_participants WHERE chat_id = %s ORDER BY joined_at;",
            (chat_id,)
        )
        return cur.fetchall()

def clear_participants(chat_id: int):
    """추첨 참가자 목록을 삭제합니다. (종료 후 정리용)"""
    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM lottery_participants WHERE chat_id = %s;", (chat_id,))


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


async def is_user_member_of_group(user_id: int, group_link_or_id: str) -> bool:
    """
    유저가 해당 그룹의 멤버인지 확인합니다.
    그룹 링크 대신 Chat ID (예: -1001234567890)를 사용하는 것이 가장 좋습니다.
    """
    group = group_link_or_id.strip()
    
    if not group:
        return True # 조건이 없으면 통과

    # 1. Chat ID로 확인
    if group.startswith("-100") and group[1:].isdigit():
        chat_id = int(group)
    # 2. @username 또는 t.me/username 형태를 그대로 사용 (봇이 해당 채널/그룹에 있어야 함)
    else:
        # T.me 링크에서 username만 추출
        match = re.search(r't\.me/([a-zA-Z0-9_]+)', group)
        if match:
            group = "@" + match.group(1)
        elif not group.startswith("@"):
            group = "@" + group

        chat_id = group # 봇 API가 username도 처리할 수 있음

    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in [
            types.ChatMemberStatus.MEMBER, 
            types.ChatMemberStatus.CREATOR, 
            types.ChatMemberStatus.ADMINISTRATOR
        ]
    except Exception as e:
        # 그룹을 찾을 수 없거나 (400 Bad Request) 봇이 그룹에 없는 경우
        logger.warning(f"그룹 멤버 확인 오류 for {group}: {e}")
        return False

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
    is_private = message.chat.type == types.ChatType.PRIVATE

    USER_HELP = (
        "/start - 봇 상태 확인\n"
        "/form - 구글 폼 링크 안내\n"
        "/list_winners - 상품별 당첨자 목록\n"
        "/submit_winner - 본인 전화번호 제출\n"
        "/join - 그룹 추첨에 참가\n"
    )

    ADMIN_HELP = (
        "\n[관리자 전용]\n"
        "/add_winner - 상품/핸들 등록\n"
        "/delete_product_winners - 상품별 당첨자 전체 삭제\n"
        "/delete_winner - 특정 핸들 삭제\n"
        "/change_product_name - 특정 당첨자의 상품명 변경\n"
        "/show_winners - 전체 상세(전화번호 포함)\n"
        "/show_winners_with_phone - 전화번호 제출자만 보기\n"
        "/show_winners_without_phone - 전화번호 미제출자만 보기\n"
        "/clear_phones_product - 특정 상품 전화번호만 삭제\n"
        "/clear_phones_all - 전체 전화번호 삭제\n"
        "/export_winners - 전체 데이터를 winners_export.csv 로 받기\n"
        "/add_admin <ID> - 관리자 추가\n"
        "/del_admin <ID> - 관리자 삭제\n"
        "/list_admins - 관리자 목록 보기\n"
        "/set_groups - DM에서 추첨 시 필수 그룹 목록 설정\n"
        "/lottery [분] [당첨수] - 추첨 시작 (그룹채팅)\n"
        "/lottery_end [당첨수] - 추첨 종료 및 추첨 (그룹채팅)\n"
        "/bot_on - 봇 동작 재개\n"
        "/bot_off - 봇 동작 일시 중지\n"
        "/bot_status - 봇 상태 확인\n"
        "/cancel - 현재 관리자 입력 플로우 취소\n"
    )

    if is_private and is_admin(uid):
        text = USER_HELP + ADMIN_HELP
    else:
        text = USER_HELP

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
    await message.reply(
        "축하드립니다! 상품 전달을 위해 휴대폰 번호 제출에 동의하시는 경우 번호를 입력해주세요.\n"
        "제출된 개인정보는 상품 발송 목적 외에는 사용되지 않으며, 발송 완료 후 즉시 삭제됩니다.\n\n"
        "예: 010-1234-5678"
    )

# --------------------
# 관리자: 관리자 명단 관리
# --------------------
@dp.message_handler(Command("add_admin", prefixes="/"))
async def add_admin_cmd(message: types.Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return
    
    args = message.get_args().split()
    if not args or not args[0].isdigit():
        await message.reply("사용법: /add_admin <숫자로 된 유저 ID>")
        return
    
    target_id = int(args[0])
    
    # ID가 실제 유저인지 확인이 어려우므로 일단 DB에 추가
    add_admin_to_db(target_id, f"ID:{target_id}")
    await message.reply(f"✅ 관리자 명단에 ID **{target_id}**를 추가했습니다.")


@dp.message_handler(Command("del_admin", prefixes="/"))
async def del_admin_cmd(message: types.Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return
    
    args = message.get_args().split()
    if not args or not args[0].isdigit():
        await message.reply("사용법: /del_admin <숫자로 된 유저 ID>")
        return

    target_id = int(args[0])

    if target_id == uid:
        await message.reply("본인을 관리자 명단에서 삭제할 수 없습니다.")
        return

    delete_admin_from_db(target_id)
    await message.reply(f"✅ 관리자 명단에서 ID **{target_id}**를 삭제했습니다.")


@dp.message_handler(commands=["list_admins"])
async def list_admins_cmd(message: types.Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return

    admins = get_all_admin_ids()
    if not admins:
        await message.reply("등록된 관리자가 없습니다.")
        return

    text = "👑 현재 등록된 관리자 목록:\n\n"
    for admin in admins:
        text += f"- ID: **{admin['user_id']}** (User: {admin['username']})\n"
        
    await message.reply(text)

# --------------------
# 관리자: 봇 ON/OFF/STATUS (기존)
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
# 관리자: 조회 계열 (기존)
# --------------------
@dp.message_handler(commands=["show_winners"])
async def show_winners_cmd(message: types.Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return

    grouped = get_winners_with_phones_grouped()
    if not grouped:
        await message.reply("데이터 없음.")
        return

    text = "📦 상세 당첨자 목록 (전체)\n\n"
    for prod, items in grouped.items():
        text += f"{prod}:\n"
        for handle, phone in items:
            phone_display = phone if phone else "전화번호 없음"
            text += f"- {handle} / {phone_display}\n"
        text += "\n"

    await message.reply(text)


@dp.message_handler(commands=["show_winners_with_phone"])
async def show_winners_with_phone_cmd(message: types.Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return

    grouped = get_winners_with_phone_only()
    if not grouped:
        await message.reply("전화번호를 제출한 사용자가 없습니다.")
        return

    text = "✅ 전화번호 제출 완료자 목록\n\n"
    for prod, items in grouped.items():
        text += f"{prod}:\n"
        for handle, phone in items:
            text += f"- {handle} / {phone}\n"
        text += "\n"

    await message.reply(text)


@dp.message_handler(commands=["show_winners_without_phone"])
async def show_winners_without_phone_cmd(message: types.Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return

    grouped = get_winners_without_phone_only()
    if not grouped:
        await message.reply("전화번호를 아직 제출하지 않은 사용자가 없습니다.")
        return

    text = "⏳ 전화번호 미제출자 목록\n\n"
    for prod, handles in grouped.items():
        text += f"{prod}:\n"
        for h in handles:
            text += f"- {h}\n"
        text += "\n"

    await message.reply(text)


# --------------------
# 관리자: CRUD 계열 (기존 + 변경)
# --------------------
@dp.message_handler(commands=["cancel"])
async def cancel_cmd(message: types.Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return

    if uid in admin_states:
        admin_states.pop(uid)
        await message.reply("✅ 관리자 상태가 취소되었습니다.")
    else:
        await message.reply("현재 진행 중인 관리자 상태 플로우가 없습니다.")


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


@dp.message_handler(commands=["change_product_name"])
async def change_product_name_cmd(message: types.Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return

    admin_states[uid] = {
        "type": "change_product",
        "step": "handle",
        "handle": None,
        "new_product_name": None,
    }
    await message.reply("상품명을 변경할 **당첨자의 핸들**을 입력하세요. (예: @username)")


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


@dp.message_handler(commands=["set_groups"])
async def set_groups_cmd(message: types.Message):
    uid = message.from_user.id
    if not is_admin(uid) or message.chat.type != types.ChatType.PRIVATE:
        if not is_admin(uid):
            return
        await message.reply("⚠️ 이 명령어는 **1:1 DM**에서만 사용 가능합니다.")
        return

    admin_states[uid] = {
        "type": "set_groups",
        "step": "groups_input",
        "groups": [] # 누적할 그룹 목록
    }
    
    current_groups = get_admin_required_groups(uid)
    
    await message.reply(
        "🔗 **필수 그룹 설정 모드**\n"
        "추첨 시 조건으로 설정할 그룹 링크 또는 ID를 한 줄에 하나씩 입력하세요.\n"
        "(예: https://t.me/Kooncrypto 또는 -1001234567890)\n\n"
        f"**현재 설정:** {current_groups.replace(',', '\n') if current_groups else '없음'}\n\n"
        "입력을 완료하려면 `/end`를 보내거나 `/cancel`을 보내 취소하세요."
    )

# --------------------
# 관리자: 추첨 기능
# --------------------

@dp.message_handler(commands=["lottery"])
async def lottery_start_cmd(message: types.Message):
    uid = message.from_user.id
    chat_id = message.chat.id
    
    if not is_admin(uid) or message.chat.type not in [types.ChatType.GROUP, types.ChatType.SUPERGROUP]:
        return

    # 이미 진행 중인 추첨 확인
    if get_current_lottery(chat_id):
        await message.reply("⚠️ 이 채팅방에는 이미 추첨이 진행 중입니다.")
        return

    args = message.get_args().split()
    duration_min = 0
    winner_count = 1
    
    if args and args[0].isdigit():
        duration_min = int(args[0])
        
    if len(args) > 1 and args[1].isdigit():
        winner_count = int(args[1])

    # DM에서 설정된 필수 그룹 목록 가져오기
    required_groups = get_admin_required_groups(uid)
        
    if not required_groups:
        await message.reply(
            "⚠️ **필수 그룹 설정 누락.** DM에서 `/set_groups` 명령어로 먼저 필수 그룹 목록을 설정해주세요."
        )
        return
        
    # DB에 추첨 정보 기록
    start_success = start_new_lottery(
        chat_id=chat_id, 
        duration=duration_min, 
        winner_count=winner_count, 
        required_groups=required_groups,
        message_id=message.message_id # 임시 메시지 ID
    )

    if not start_success:
        await message.reply("⚠️ 추첨 시작 중 오류가 발생했습니다.")
        return

    # 안내 메시지 구성
    if duration_min > 0:
        end_time = datetime.now() + timedelta(minutes=duration_min)
        time_text = f"⏳ **{duration_min}분** 동안 진행됩니다. (예상 종료: {end_time.strftime('%H:%M')})"
    else:
        time_text = "⏳ **관리자가 /lottery_end 로 종료할 때까지** 진행됩니다."

    winner_text = ""
    if winner_count > 0:
        winner_text = f"\n🎁 **총 {winner_count}명** 당첨 예정"

    group_list = "\n".join([f"- {g.strip()}" for g in required_groups.split(',')])
    group_text = f"\n\n🚨 **참여 조건:** 다음 그룹에 **모두 입장**해야 합니다.\n{group_list}"

    final_text = (
        "🎉 **새로운 추첨이 시작되었습니다!** 🎉\n\n"
        f"{time_text}{winner_text}{group_text}\n\n"
        "참여하려면 '/join'을 입력해주세요."
    )

    sent_message = await message.reply(final_text)

    # 메시지 ID 업데이트
    with closing(get_conn()) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE lotteries SET message_id = %s WHERE chat_id = %s;",
            (sent_message.message_id, chat_id)
        )


@dp.message_handler(commands=["lottery_end"])
async def lottery_end_cmd(message: types.Message):
    uid = message.from_user.id
    chat_id = message.chat.id
    
    if not is_admin(uid) or message.chat.type not in [types.ChatType.GROUP, types.ChatType.SUPERGROUP]:
        return

    lottery = get_current_lottery(chat_id)
    if not lottery:
        await message.reply("⚠️ 현재 진행 중인 추첨이 없습니다.")
        return

    args = message.get_args().split()
    winner_count = lottery['winner_count']
    if args and args[0].isdigit():
        winner_count = int(args[0])

    participants = get_participants(chat_id)
    
    if not participants:
        await message.reply("😥 참가자가 없습니다. 추첨을 종료합니다.")
        end_lottery(chat_id)
        clear_participants(chat_id)
        return

    if winner_count > len(participants):
        winner_count = len(participants)

    # 추첨 로직
    winners = random.sample(participants, winner_count)
    winner_handles = [f"@{w['username']}" if w['username'] else f"ID:{w['user_id']}" for w in winners]
    
    # DB 종료 처리
    end_lottery(chat_id)
    clear_participants(chat_id)
    
    # 결과 메시지
    result_text = (
        "🎉 **추첨 종료! 당첨자를 발표합니다!** 🎉\n\n"
        f"총 참가자: **{len(participants)}명**\n"
        f"당첨 인원: **{winner_count}명**\n\n"
        "👑 **당첨자 목록:**\n"
    )
    for handle in winner_handles:
        result_text += f"- {handle}\n"

    result_text += "\n✅ 당첨자께서는 개인 DM으로 `/submit_winner` 명령을 사용해주세요!"
    
    await message.reply(result_text)

# --------------------
# 일반 사용자: 추첨 참가 (/join)
# --------------------

@dp.message_handler(commands=["join"])
async def lottery_join_cmd(message: types.Message):
    user = message.from_user
    chat_id = message.chat.id

    if is_user_blocked(user.id) or message.chat.type not in [types.ChatType.GROUP, types.ChatType.SUPERGROUP]:
        return

    lottery = get_current_lottery(chat_id)
    if not lottery:
        await message.reply("⚠️ 현재 이 채팅방에서 진행 중인 추첨이 없습니다.")
        return
        
    if not user.username:
         await message.reply("⚠️ 참여하려면 **텔레그램 유저네임(@username)**을 설정해야 합니다.")
         return

    # 그룹 가입 조건 확인
    required_groups = [g.strip() for g in lottery['required_groups'].split(',') if g.strip()]
    is_qualified = True
    
    # 모든 필수 그룹에 가입했는지 확인
    for group in required_groups:
        if not await is_user_member_of_group(user.id, group):
            is_qualified = False
            break
            
    if not is_qualified:
        await message.reply("⚠️ **참여 조건 미달:** 모든 필수 그룹에 가입해야 참여할 수 있습니다. 먼저 가입해주세요.")
        return

    
    # 참가자 추가
    join_success = add_participant(chat_id, user.id, user.username)

    if join_success:
        await message.reply(f"🎉 @{user.username} 님, 추첨에 참가했습니다!")
    else:
        await message.reply("⚠️ 이미 추첨에 참가했습니다.")


# --------------------
# 관리자: CSV 내보내기 (기존)
# --------------------
@dp.message_handler(commands=["export_winners"])
async def export_winners_cmd(message: types.Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return

    rows = get_all_rows_for_export()
    if not rows:
        await message.reply("내보낼 데이터가 없습니다.")
        return

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "product_name", "handle", "phone_number", "created_at"])

    for row in rows:
        writer.writerow(
            [
                row["id"],
                row["product_name"],
                row["handle"],
                row["phone_number"] or "",
                row["created_at"],
            ]
        )

    csv_data = output.getvalue().encode("utf-8-sig")
    bio = io.BytesIO(csv_data)
    bio.name = "winners_export.csv"

    await message.reply_document(types.InputFile(bio), caption="전체 당첨자 CSV 내보내기")


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

    # change_product 플로우
    elif stype == "change_product":
        if step == "handle":
            handle = text
            state["handle"] = handle
            state["step"] = "new_product_name"
            await message.reply(f"'{handle}'에 대해 변경할 **새로운 상품명**을 입력하세요.")
            return

        elif step == "new_product_name":
            handle = state["handle"]
            new_product_name = text
            
            result = change_product_name_for_handle(handle, new_product_name)
            admin_states.pop(uid, None)

            if result is False:
                 await message.reply(
                    f"⚠️ 오류: 당첨자 '{handle}'은(는) 이미 '{new_product_name}' 상품에 등록되어 있거나 핸들을 찾을 수 없습니다."
                )
            elif result is True:
                await message.reply(
                    f"✅ 당첨자 '{handle}'의 상품명이 '{new_product_name}'(으)로 변경되었습니다."
                )
            else:
                await message.reply(
                    f"⚠️ 오류: 당첨자 '{handle}'을(를) 찾을 수 없거나 변경된 사항이 없습니다."
                )
            return

    # set_groups 플로우
    elif stype == "set_groups" and step == "groups_input":
        
        # 이전 입력값 포함하여 현재 입력된 그룹 목록에 추가
        if text != "/end":
            new_groups = [line.strip() for line in text.splitlines() if line.strip()]
            state["groups"].extend(new_groups)

        if text.lower() == "/end" or message.text.startswith('/'):
            groups_str = ",".join(state["groups"])
            
            if not groups_str:
                await message.reply("❌ 필수 그룹 목록이 비어 있습니다. 취소하려면 /cancel을 사용하세요.")
                return

            set_admin_required_groups(uid, groups_str)
            admin_states.pop(uid, None)
            
            # ⭐️ 문법 오류 수정 부분: f-string 내부에서 줄바꿈(\n) 처리를 분리합니다.
            # 기존 오류 코드: await message.reply(f"✅ 필수 그룹이 다음으로 설정되었습니다:\n{groups_str.replace(',', '\n')}")
            await message.reply(
                "✅ 필수 그룹이 다음으로 설정되었습니다:\n" + 
                groups_str.replace(',', '\n')
            )
            return
        
        await message.reply("계속 입력하거나, 완료하려면 `/end`를 보내주세요.")
        return

    # 그 외는 상태 초기화 (다른 명령어가 아닌 경우)
    if text.startswith('/') and text not in ["/start", "/form", "/list_winners", "/submit_winner", "/join"]:
        admin_states.pop(uid, None)
        
    elif not text.startswith('/'):
         # 상태가 없는 일반 텍스트는 무시
        return


# --------------------
# 시작
# --------------------
async def on_startup(dp: Dispatcher):
    init_db()


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
