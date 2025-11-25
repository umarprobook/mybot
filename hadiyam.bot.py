import logging
import os
import sqlite3
import traceback
from datetime import datetime
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, CallbackQuery, ChatJoinRequest,
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton
)
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties

# === 🔧 SOZLAMALAR ===
load_dotenv(".env")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = 8380378054  # ADMIN ID ni o'zingiznikiga almashtiring

if not BOT_TOKEN:
    raise SystemExit("❌ Iltimos, .env faylda BOT_TOKEN qiymatini kiriting!")

# === LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# === DATABASE ===
class Database:
    def __init__(self, path: str = "bot_full.db"):
        self.path = path
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.path, check_same_thread=False)
        cur = conn.cursor()
        cur.executescript("""
                          CREATE TABLE IF NOT EXISTS users
                          (
                              user_id
                              INTEGER
                              PRIMARY
                              KEY,
                              username
                              TEXT,
                              full_name
                              TEXT,
                              points
                              INTEGER
                              DEFAULT
                              0,
                              referrals
                              INTEGER
                              DEFAULT
                              0,
                              referrer_id
                              INTEGER,
                              joined_ts
                              TIMESTAMP
                              DEFAULT
                              CURRENT_TIMESTAMP
                          );
                          CREATE TABLE IF NOT EXISTS channels
                          (
                              chat_id
                              TEXT
                              PRIMARY
                              KEY,
                              name
                              TEXT,
                              invite_link
                              TEXT,
                              is_private
                              INTEGER
                              DEFAULT
                              1
                          );
                          CREATE TABLE IF NOT EXISTS contests
                          (
                              id
                              INTEGER
                              PRIMARY
                              KEY
                              AUTOINCREMENT,
                              is_active
                              INTEGER
                              DEFAULT
                              1,
                              start_ts
                              TIMESTAMP
                              DEFAULT
                              CURRENT_TIMESTAMP,
                              end_ts
                              TIMESTAMP
                          );
                          CREATE TABLE IF NOT EXISTS points_given
                          (
                              id
                              INTEGER
                              PRIMARY
                              KEY
                              AUTOINCREMENT,
                              user_id
                              INTEGER,
                              channel_id
                              TEXT,
                              contest_id
                              INTEGER,
                              points
                              INTEGER,
                              given_ts
                              TIMESTAMP
                              DEFAULT
                              CURRENT_TIMESTAMP,
                              UNIQUE
                          (
                              user_id,
                              channel_id,
                              contest_id
                          )
                              );
                          CREATE TABLE IF NOT EXISTS referrals_awarded
                          (
                              referrer_id
                              INTEGER,
                              referred_id
                              INTEGER,
                              points
                              INTEGER,
                              PRIMARY
                              KEY
                          (
                              referrer_id,
                              referred_id
                          )
                              );
                          CREATE TABLE IF NOT EXISTS gifts
                          (
                              id
                              INTEGER
                              PRIMARY
                              KEY
                              AUTOINCREMENT,
                              name
                              TEXT,
                              points_required
                              INTEGER
                          );
                          """)
        conn.commit()
        conn.close()

    def get_connection(self):
        return sqlite3.connect(self.path, check_same_thread=False)


db = Database()

# === KONSTANTALAR ===
JOIN_REQUEST_POINTS = 10
REFERRAL_POINTS = 10
POINTS_PER_JOIN = 10


# === STATES ===
# === STATES NI YANGILAYMIZ ===
class AdminStates(StatesGroup):
    broadcast = State()
    confirm_reset = State()
    add_channel = State()
    delete_channel = State()
    add_gift = State()
    delete_gift = State()


# === KEYBOARDS ===
def user_menu():
    """Oddiy foydalanuvchilar uchun tugmali menyu"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Mening ballarim"), KeyboardButton(text="👥 Referal")],
            [KeyboardButton(text="🏆 Reyting"), KeyboardButton(text="📺 Kanallar")],
            [KeyboardButton(text="🎁 Sovg'alar"), KeyboardButton(text="ℹ️ Yordam")]
        ],
        resize_keyboard=True,
        input_field_placeholder="Quyidagi tugmalardan birini tanlang..."
    )


def admin_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📢 Kanallar"), KeyboardButton(text="🎁 Sovg'alar")],
            [KeyboardButton(text="📩 Xabar yuborish")],
            [KeyboardButton(text="🏁 Konkursni yakunlash"), KeyboardButton(text="🔁 Yangi konkurs")],
            [KeyboardButton(text="📊 Top 10"), KeyboardButton(text="🧹 Tozalash")],
        ],
        resize_keyboard=True,
    )


def build_channels_keyboard():
    """Kanallar ro'yxatini URL tugmalari bilan qaytaradi."""
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT chat_id, name, invite_link FROM channels")
    rows = cur.fetchall()
    conn.close()

    buttons = []
    for chat_id, name, invite_link in rows:
        if invite_link:
            buttons.append([InlineKeyboardButton(text=f"➡️ {name}", url=invite_link)])
        else:
            buttons.append([InlineKeyboardButton(text=f"➡️ {name} (havola yo'q)", callback_data="noop")])

    # Tekshirish tugmasini qo'shamiz
    buttons.append([InlineKeyboardButton(text="✅ Tekshirish", callback_data="check_sub")])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


# === DATABASE FUNCTIONS ===
def get_active_contest_id() -> int:
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id FROM contests WHERE is_active = 1 ORDER BY id DESC LIMIT 1")
    r = cur.fetchone()
    if not r:
        cur.execute("INSERT INTO contests (is_active) VALUES (1)")
        conn.commit()
        last_id = cur.lastrowid
        conn.close()
        return last_id
    conn.close()
    return r[0]


def add_or_update_user(user_id: int, username: str, full_name: str, referrer_id: int = None):
    try:
        conn = db.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
        existed = cur.fetchone()
        if existed:
            cur.execute(
                "UPDATE users SET username = ?, full_name = ? WHERE user_id = ?",
                (username, full_name, user_id),
            )
            conn.commit()
            conn.close()
            return
        cur.execute(
            "INSERT INTO users (user_id, username, full_name, referrer_id) VALUES (?, ?, ?, ?)",
            (user_id, username, full_name, referrer_id),
        )
        conn.commit()
        conn.close()
        if referrer_id:
            give_referral_points_if_needed(user_id)
    except Exception:
        logger.error(traceback.format_exc())


def give_points_once_for_channel(user_id: int, channel_id: str, points: int) -> bool:
    try:
        conn = db.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM points_given WHERE user_id = ? AND channel_id = ?", (user_id, channel_id))
        if cur.fetchone():
            conn.close()
            return False

        contest_id = get_active_contest_id()
        cur.execute(
            "INSERT INTO points_given (user_id, channel_id, contest_id, points) VALUES (?, ?, ?, ?)",
            (user_id, channel_id, contest_id, points),
        )
        cur.execute("UPDATE users SET points = points + ? WHERE user_id = ?", (points, user_id))
        conn.commit()
        conn.close()
        logger.info(f"✅ {user_id} foydalanuvchiga {channel_id} kanali uchun {points} ball berildi")
        return True
    except Exception as e:
        logger.error(f"Ball berishda xatolik: {e}")
        return False


def give_referral_points_if_needed(referred_id: int):
    try:
        conn = db.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT referrer_id FROM users WHERE user_id = ?", (referred_id,))
        r = cur.fetchone()
        if not r or not r[0]:
            conn.close()
            return
        referrer_id = r[0]

        cur.execute(
            "SELECT 1 FROM referrals_awarded WHERE referrer_id = ? AND referred_id = ?",
            (referrer_id, referred_id),
        )
        if cur.fetchone():
            conn.close()
            return

        cur.execute(
            "INSERT INTO referrals_awarded (referrer_id, referred_id, points) VALUES (?, ?, ?)",
            (referrer_id, referred_id, REFERRAL_POINTS),
        )
        cur.execute(
            "UPDATE users SET points = points + ?, referrals = referrals + 1 WHERE user_id = ?",
            (REFERRAL_POINTS, referrer_id),
        )
        conn.commit()
        conn.close()
        logger.info(f"🎁 Referral ball berildi: {referrer_id} -> {referred_id}")
    except Exception:
        logger.error(traceback.format_exc())


# === MAJBURIY OBUNA TEKSHIRISH ===
async def check_subscription(user_id: int, bot: Bot) -> bool:
    """Foydalanuvchi barcha kanallarga obuna bo'lganini tekshiradi"""
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT chat_id, invite_link FROM channels")
    channels = cur.fetchall()
    conn.close()

    if not channels:
        return True  # kanal yo'q bo'lsa, obuna talab qilinmaydi

    all_subscribed = True
    new_points_given = 0

    for chat_id, invite_link in channels:
        try:
            member = await bot.get_chat_member(chat_id, user_id)
            if member.status in ["member", "administrator", "creator"]:
                # Obuna bo'lgan bo'lsa, ball berilganligini tekshiramiz
                conn = db.get_connection()
                cur = conn.cursor()
                cur.execute("SELECT 1 FROM points_given WHERE user_id=? AND channel_id=?", (user_id, str(chat_id)))
                if not cur.fetchone():
                    # Agar hali ball berilmagan bo'lsa, ball beramiz
                    if give_points_once_for_channel(user_id, str(chat_id), JOIN_REQUEST_POINTS):
                        new_points_given += JOIN_REQUEST_POINTS
                        logger.info(
                            f"✅ {user_id} foydalanuvchi {chat_id} kanaliga obuna bo'ldi - {JOIN_REQUEST_POINTS} ball berildi")
                conn.close()
            else:
                all_subscribed = False  # Obuna bo'lmagan
        except Exception as e:
            logger.error(f"Obuna tekshirishda xatolik {chat_id}: {e}")
            # Agar tekshirish imkoni bo'lmasa, bazadagi ball berilganligiga qaraymiz
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM points_given WHERE user_id=? AND channel_id=?", (user_id, str(chat_id)))
            if not cur.fetchone():
                all_subscribed = False
            conn.close()

    # Agar yangi ball berilgan bo'lsa, foydalanuvchiga xabar beramiz
    if new_points_given > 0:
        try:
            await bot.send_message(
                chat_id=user_id,
                text=f"🎉 Tabriklaymiz! Siz {new_points_given} ball qo'lga kiritdingiz!"
            )
        except Exception as e:
            logger.error(f"Ball haqida xabar berishda xatolik: {e}")

    return all_subscribed


# === ROUTERS ===
router = Router()
admin_router = Router()

# Apply admin filter to admin router
admin_router.message.filter(F.from_user.id == ADMIN_ID)
admin_router.callback_query.filter(F.from_user.id == ADMIN_ID)


# === START HANDLER ===
@router.message(CommandStart())
async def start_handler(message: Message, bot: Bot):
    user = message.from_user

    # Referral parametrni tekshiramiz
    referrer_id = None
    if message.text and len(message.text.split()) > 1:
        try:
            referrer_id = int(message.text.split()[1])
        except ValueError:
            pass

    conn = db.get_connection()
    cur = conn.cursor()

    # Foydalanuvchi bazada bormi?
    cur.execute("SELECT user_id FROM users WHERE user_id = ?", (user.id,))
    existing_user = cur.fetchone()

    # AGAR: Yangi foydalanuvchi VA referral mavjud VA o'zini taklif qilmagan bo'lsa
    if not existing_user and referrer_id and referrer_id != user.id:
        print(f"🎯 YANGI REFERAL: {user.id} -> {referrer_id}")

        # 1. Referral egasiga ball qo'shish
        cur.execute("UPDATE users SET points = points + ?, referrals = referrals + 1 WHERE user_id = ?",
                    (REFERRAL_POINTS, referrer_id))

        # 2. Yangi foydalanuvchini qo'shish
        cur.execute("INSERT INTO users (user_id, username, full_name, points, referrals) VALUES (?, ?, ?, ?, ?)",
                    (user.id, user.username, user.full_name, 0, 0))

        conn.commit()
        print(f"✅ Referral {referrer_id} ga {REFERRAL_POINTS} ball qo'shildi")

        # Referral egasiga xabar yuborish
        try:
            await bot.send_message(
                referrer_id,
                f"🎊 Tabriklaymiz! Sizning taklif havolangiz orqali yangi foydalanuvchi qo'shildi.\n"
                f"📊 Sizga {REFERRAL_POINTS} ball qo'shildi!"
            )
        except Exception as e:
            print(f"⚠️ Referral egasiga xabar yuborishda xatolik: {e}")

    elif not existing_user:
        # Oddiy yangi foydalanuvchi
        cur.execute("INSERT INTO users (user_id, username, full_name, points, referrals) VALUES (?, ?, ?, ?, ?)",
                    (user.id, user.username, user.full_name, 0, 0))
        conn.commit()
        print(f"✅ Yangi foydalanuvchi qo'shildi: {user.id}")

    else:
        # Mavjud foydalanuvchi - faqat ma'lumotlarni yangilash
        cur.execute("UPDATE users SET username = ?, full_name = ? WHERE user_id = ?",
                    (user.username, user.full_name, user.id))
        conn.commit()
        print(f"✅ Foydalanuvchi yangilandi: {user.id}")

    conn.close()

    # 🔒 Majburiy obuna tekshiruvi
    is_subscribed = await check_subscription(user.id, bot)
    if not is_subscribed:
        keyboard = build_channels_keyboard()
        await message.answer(
            "❌ Iltimos, quyidagi kanallarga obuna bo'ling yoki qo'shilish so'rovini yuboring, so'ngra tekshirish tugmasini bosing:",
            reply_markup=keyboard
        )
        return

    # ✅ Obuna bo'lgan foydalanuvchi uchun ASOSIY XABAR
    if user.id == ADMIN_ID:
        await message.answer("👑 Xush kelibsiz, Admin!", reply_markup=admin_menu())
    else:
        conn = db.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT points FROM users WHERE user_id = ?", (user.id,))
        pts = cur.fetchone()
        conn.close()
        pts = pts[0] if pts else 0

        await message.answer(
            f"👋 Xush kelibsiz, {user.full_name}!\n\n"
            f"📊 Sizning ballaringiz: {pts} ball\n\n"
            f"Quyidagi menyu orqali botning barcha imkoniyatlaridan foydalaning:",
            reply_markup=user_menu()
        )

# === BALL KO'RISH ===
@router.message(Command("ball"))
@router.message(F.text == "📊 Mening ballarim")
async def my_points_cmd(message: Message, bot: Bot):
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT points, referrals FROM users WHERE user_id = ?", (message.from_user.id,))
    row = cur.fetchone()
    conn.close()

    pts = row[0] if row else 0
    refs = row[1] if row else 0

    await message.answer(
        f"📊 Sizning ballaringiz: {pts} ball\n"
        f"👥 Sizning referrallaringiz: {refs} ta\n\n"
        f"ℹ️ Do'stlaringizni taklif qilish uchun \"👥 Referal\" bo'limiga o'ting"
    )


# === YANGI: HELP COMMAND ===
@router.message(Command("help"))
@router.message(F.text == "ℹ️ Yordam")
async def help_handler(message: Message):
    """Yordam buyrug'i"""
    help_text = (
        "🤖 Botdan foydalanish bo'yicha ko'rsatma:\n\n"
        "📊 Ball to'plash usullari:\n"
        "• Kanalga qo'shilish so'rovi yuborish\n"
        "• Do'stlaringizni taklif qilish\n"
        "• Barcha kanallarga obuna bo'lish\n\n"
        "🎁 Ballaringizni sovg'alarga almashtirishingiz mumkin!\n\n"
        "📞 Qo'shimcha savollar bo'lsa: @admin"
    )
    await message.answer(help_text)


# === REFERAL HANDLER ===
@router.message(F.text == "👥 Referal")
async def referral_handler(message: Message, bot: Bot):
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT points, referrals FROM users WHERE user_id = ?", (message.from_user.id,))
    row = cur.fetchone()
    conn.close()

    pts = row[0] if row else 0
    refs = row[1] if row else 0

    bot_username = (await bot.get_me()).username
    ref_link = f"https://t.me/{bot_username}?start={message.from_user.id}"


    await message.answer(
        f"👥 Referal tizimi\n\n"
        f"📊 Jami taklif qilganlar: {refs} ta\n"
        f"🎁 Har bir referal uchon: {REFERRAL_POINTS} ball\n\n"
        f"📨 Do'stlaringizni taklif qilish uchun havola:\n{ref_link}\n\n"
        f"🔗 Havolani nusxalab, do'stlaringizga yuboring. "
        f"Ular botdan foydalanishni boshlaganda siz {REFERRAL_POINTS} ball olasiz!"
    )


# === DEBUG REFERAL ===
@router.message(Command("test_ref"))
async def test_ref_handler(message: Message, bot: Bot):
    user_id = message.from_user.id

    conn = db.get_connection()
    cur = conn.cursor()

    # Foydalanuvchi ma'lumotlari
    cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    user_data = cur.fetchone()

    # Referral havola
    bot_username = (await bot.get_me()).username
    ref_link = f"https://t.me/{bot_username}?start={user_id}"

    if user_data:
        response = (
            f"🔍 DEBUG MA'LUMOTLARI:\n"
            f"👤 User ID: {user_id}\n"
            f"📛 Ism: {user_data[2]}\n"
            f"📊 Ball: {user_data[3]}\n"
            f"👥 Referrallar: {user_data[4]}\n"
            f"🔗 Referral havola: {ref_link}\n"
            f"🎯 REFERRAL_POINTS: {REFERRAL_POINTS}\n"
            f"📝 Database: {user_data}"
        )
    else:
        response = f"❌ Foydalanuvchi bazada topilmadi: {user_id}"

    conn.close()
    await message.answer(response)

# === REYTING HANDLER ===
@router.message(F.text == "🏆 Reyting")
async def rating_handler(message: Message):
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT full_name, points FROM users ORDER BY points DESC LIMIT 20")
    rows = cur.fetchall()

    # Foydalanuvchining o'z o'rni
    cur.execute("SELECT points FROM users WHERE user_id = ?", (message.from_user.id,))
    up = cur.fetchone()
    user_points = up[0] if up else 0
    cur.execute("SELECT COUNT(*) + 1 FROM users WHERE points > ?", (user_points,))
    user_rank = cur.fetchone()[0]
    conn.close()

    msg = f"🏆 Umumiy reyting\n\n"
    msg += f"📊 Sizning o'rningiz: {user_rank}\n"
    msg += f"🎯 Sizning ballaringiz: {user_points}\n\n"
    msg += "TOP 10 talaba:\n"

    for i, (name, points) in enumerate(rows[:10], 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        msg += f"{medal} {name} - {points} ball\n"

    await message.answer(msg)


# === KANALLAR HANDLER ===
@router.message(F.text == "📺 Kanallar")
async def channels_handler(message: Message):
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT name, invite_link FROM channels")
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await message.answer("📺 Hozircha kanallar mavjud emas.")
        return

    msg = "📺 Obuna bo'lish kerak bo'lgan kanallar:\n\n"
    for name, link in rows:
        msg += f"➡️ {name}\n"
        if link:
            msg += f"🔗 {link}\n"
        msg += "\n"
    msg += "ℹ️ Kanallarga obuna bo'lib, qo'shimcha ball to'plashingiz mumkin!"
    await message.answer(msg)


# === SOVG'ALAR HANDLER ===
@router.message(F.text == "🎁 Sovg'alar")
async def gifts_handler(message: Message):
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT name, points_required FROM gifts ORDER BY points_required")
    rows = cur.fetchall()
    cur.execute("SELECT points FROM users WHERE user_id = ?", (message.from_user.id,))
    user_points = cur.fetchone()
    conn.close()

    user_points = user_points[0] if user_points else 0

    if not rows:
        await message.answer(
            f"🎁 Hozircha sovg'alar mavjud emas.\n\n"
            f"📊 Sizda {user_points} ball to'plagansiz."
        )
        return

    msg = f"🎁 Mavjud sovg'alar:\n\n"
    msg += f"💰 Sizning ballaringiz: {user_points}\n\n"
    for name, points_req in rows:
        status = "✅ Sotib olish mumkin" if user_points >= points_req else f"❌ Yetarli ball yo'q"
        msg += f"🎯 {name}\n"
        msg += f"💰 Narxi: {points_req} ball\n"
        msg += f"📊 Holat: {status}\n\n"
    msg += "ℹ️ Sovg'a olish uchun admin bilan bog'laning: @admin"
    await message.answer(msg)


# === INLINE TUGMALAR HANDLER ===
@router.callback_query(F.data == "check_sub")
async def check_subscription_callback(query: CallbackQuery, bot: Bot):
    user_id = query.from_user.id

    # Obunani tekshiramiz
    is_subscribed = await check_subscription(user_id, bot)
    if is_subscribed:
        await query.message.edit_text(
            "✅ Tabriklaymiz! Siz barcha kanallarga obuna bo'lgansiz.\n\n"
            "Quyidagi menyu orqali botning barcha imkoniyatlaridan foydalaning:"
        )
        # Foydalanuvchiga alohida xabar yuboramiz — reply keyboard ko'rsatish uchun
        try:
            await bot.send_message(chat_id=user_id, text="Quyidagi menyu:", reply_markup=user_menu())
        except Exception as e:
            logger.error(f"Reply menu yuborishda xatolik: {e}")
    else:
        keyboard = build_channels_keyboard()
        await query.message.edit_text(
            "❌ Hali barcha kanallarga obuna bo'lmagansiz. Iltimos, quyidagi kanallarga obuna bo'ling va yana tekshirish tugmasini bosing:",
            reply_markup=keyboard
        )

    await query.answer()


@router.callback_query(F.data == "noop")
async def noop_callback(query: CallbackQuery):
    await query.answer("Bu kanal hozircha faol emas", show_alert=True)


# === JOIN REQUEST HANDLER ===
@router.chat_join_request()
async def join_request_handler(chat_join: ChatJoinRequest, bot: Bot):
    """Kanalga qo'shilish so'rovini qayta ishlash"""
    try:
        user = chat_join.from_user
        chat = chat_join.chat

        # Avvalo foydalanuvchini bazaga qo'shamiz (agar yo'q bo'lsa)
        add_or_update_user(user.id, user.username, user.full_name)

        # Avval tekshiramiz, bu user bu kanal uchun hali ball olganmi
        conn = db.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM points_given WHERE user_id = ? AND channel_id = ?", (user.id, str(chat.id)))
        already_given = cur.fetchone()
        conn.close()

        if not already_given:
            # Agar hali ball berilmagan bo'lsa, bir marta beramiz
            if give_points_once_for_channel(user.id, str(chat.id), POINTS_PER_JOIN):
                await bot.send_message(
                    chat_id=user.id,
                    text=(
                        f"📩 Assalomu alaykum, {user.full_name}!\n"
                        f"Siz {chat.title} kanaliga qo'shilish so'rovi yubordingiz.\n"
                        f"🎉 Sizga {POINTS_PER_JOIN} ball berildi!"
                    ),
                )
                logger.info(f"✅ {user.id} foydalanuvchiga {POINTS_PER_JOIN} ball berildi (join request)")
            else:
                await bot.send_message(
                    chat_id=user.id,
                    text=(
                        f"📩 Siz {chat.title} kanaliga so'rov yuborgansiz.\n"
                        f"❌ Ball berishda xatolik yuz berdi."
                    ),
                )
        else:
            # Agar avval berilgan bo'lsa, xabarni takrorlaymiz, lekin ball qo'shmaymiz
            await bot.send_message(
                chat_id=user.id,
                text=(
                    f"📩 Siz {chat.title} kanaliga so'rov yuborgansiz.\n"
                    f"Bu kanal uchun ball allaqachon berilgan ✅"
                ),
            )

        # YANGI QO'SHILGAN QISM: menyu ochish
        if user.id != ADMIN_ID:
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT points FROM users WHERE user_id = ?", (user.id,))
            pts = cur.fetchone()
            conn.close()
            pts = pts[0] if pts else 0

            await bot.send_message(
                chat_id=user.id,
                text=(
                    f"📊 Sizda jami {pts} ball bor.\n"
                    f"Quyidagi menyu orqali davom eting:"
                ),
                reply_markup=user_menu()
            )
        else:
            await bot.send_message(
                chat_id=user.id,
                text="👑 Admin menyusi:",
                reply_markup=admin_menu()
            )

        logger.info(f"[+] {user.full_name} ({user.id}) kanalga so'rov yubordi (bir martalik ball).")
    except Exception as e:
        logger.error(f"Join request handler xatosi: {e}")




# === ADMIN HANDLERS ===
@admin_router.message(Command("new_contest"))
@admin_router.message(F.text == "🔁 Yangi konkurs")
async def new_contest_cmd(message: Message):
    """Yangi konkursni boshlash"""
    conn = db.get_connection()
    cur = conn.cursor()
    # Eski konkursni yakunlash va yangi boshlash
    cur.execute("UPDATE contests SET is_active = 0 WHERE is_active = 1")
    cur.execute("INSERT INTO contests (is_active) VALUES (1)")
    cur.execute("UPDATE users SET points = 0, referrals = 0")
    # 🔥 Yangi qo'shildi — eski "kanalga qo'shilish so'rovi" ma'lumotlarini tozalash:
    cur.execute("DELETE FROM points_given")
    conn.commit()
    conn.close()

    await message.answer("🔁 Yangi konkurs boshlandi! Barcha ballar va obuna yozuvlari yangilandi.")


@admin_router.message(F.text == "📢 Kanallar")
async def admin_channels_handler(message: Message):
    """Admin kanallar menyusi"""
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT chat_id, name, invite_link FROM channels")
    rows = cur.fetchall()
    conn.close()

    channels_list = "📢 <b>Kanallar boshqaruvi</b>\n\n"
    if not rows:
        channels_list += "❌ Hozircha kanallar mavjud emas\n"
    else:
        channels_list += "📋 <b>Mavjud kanallar:</b>\n"
        for i, (chat_id, name, link) in enumerate(rows, 1):
            channels_list += f"{i}. <b>{name}</b>\n   ID: <code>{chat_id}</code>\n   Havola: {link if link else 'Havola yo''q'}\n\n"

    # Kanallar boshqaruv tugmalari
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Kanal qo'shish"), KeyboardButton(text="➖ Kanal o'chirish")],
            [KeyboardButton(text="📋 Kanallar ro'yxati")],
            [KeyboardButton(text="🔙 Orqaga")]
        ],
        resize_keyboard=True
    )

    await message.answer(channels_list, reply_markup=keyboard)


@admin_router.message(F.text == "➕ Kanal qo'shish")
async def add_channel_prompt(message: Message, state: FSMContext):
    """Kanal qo'shish uchun formani boshlash"""
    await message.answer(
        "📝 <b>Kanal qo'shish</b>\n\n"
        "Quyidagi formatda ma'lumotlarni kiriting:\n"
        "<code>kanal_id, kanal_nomi, invite_link</code>\n\n"
        "📍 <b>Misol:</b>\n"
        "<code>-100123456789, Test Kanal, https://t.me/testkanal</code>\n\n"
        "❌ Bekor qilish uchun <b>🔙 Orqaga</b> tugmasini bosing.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🔙 Orqaga")]],
            resize_keyboard=True
        )
    )
    await state.set_state(AdminStates.add_channel)


@admin_router.message(AdminStates.add_channel)
async def process_add_channel(message: Message, state: FSMContext):
    """Kanal qo'shishni qayta ishlash"""
    if message.text == "🔙 Orqaga":
        await state.clear()
        await admin_channels_handler(message)
        return

    try:
        args = message.text.split(",")
        if len(args) != 3:
            await message.answer("❌ Noto'g'ri format. Iltimos, 3 ta parametr kiriting: chat_id, name, invite_link")
            return

        chat_id, name, link = [arg.strip() for arg in args]

        conn = db.get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO channels (chat_id, name, invite_link) VALUES (?, ?, ?)",
            (chat_id, name, link),
        )
        conn.commit()
        conn.close()

        await message.answer(f"✅ <b>{name}</b> kanali muvaffaqiyatli qo'shildi!")
        await state.clear()
        await admin_channels_handler(message)

    except Exception as e:
        await message.answer(f"❌ Xatolik yuz berdi: {str(e)}")


@admin_router.message(F.text == "➖ Kanal o'chirish")
async def delete_channel_prompt(message: Message, state: FSMContext):
    """Kanal o'chirish uchun formani boshlash"""
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT chat_id, name FROM channels")
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await message.answer("❌ Hozircha kanallar mavjud emas.")
        return

    # Kanallar ro'yxatini tugmalar shaklida chiqaramiz
    keyboard_buttons = []
    for chat_id, name in rows:
        keyboard_buttons.append([KeyboardButton(text=f"🗑️ {name}")])

    keyboard_buttons.append([KeyboardButton(text="🔙 Orqaga")])

    keyboard = ReplyKeyboardMarkup(
        keyboard=keyboard_buttons,
        resize_keyboard=True
    )

    await message.answer(
        "🗑️ <b>O'chirmoqchi bo'lgan kanalni tanlang:</b>",
        reply_markup=keyboard
    )
    await state.set_state(AdminStates.delete_channel)


@admin_router.message(AdminStates.delete_channel)
async def process_delete_channel(message: Message, state: FSMContext):
    """Kanal o'chirishni qayta ishlash"""
    if message.text == "🔙 Orqaga":
        await state.clear()
        await admin_channels_handler(message)
        return

    if message.text.startswith("🗑️ "):
        channel_name = message.text[3:]  # "🗑️ " ni olib tashlaymiz

        conn = db.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT chat_id FROM channels WHERE name = ?", (channel_name,))
        result = cur.fetchone()

        if result:
            chat_id = result[0]
            cur.execute("DELETE FROM channels WHERE name = ?", (channel_name,))
            conn.commit()
            conn.close()

            await message.answer(f"✅ <b>{channel_name}</b> kanali muvaffaqiyatli o'chirildi!")
        else:
            await message.answer("❌ Kanal topilmadi!")

        await state.clear()
        await admin_channels_handler(message)


@admin_router.message(F.text == "📋 Kanallar ro'yxati")
async def show_channels_list(message: Message):
    """Kanallar ro'yxatini ko'rsatish"""
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT chat_id, name, invite_link FROM channels")
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await message.answer("📭 Hozircha kanallar mavjud emas.")
        return

    message_text = "📋 <b>Mavjud kanallar:</b>\n\n"
    for i, (chat_id, name, link) in enumerate(rows, 1):
        message_text += f"<b>{i}. {name}</b>\n"
        message_text += f"   ID: <code>{chat_id}</code>\n"
        message_text += f"   Havola: {link if link else 'Havola yo''q'}\n\n"

    await message.answer(message_text)


@admin_router.message(F.text == "🎁 Sovg'alar")
async def admin_gifts_handler(message: Message):
    """Admin sovg'alar menyusi"""
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name, points_required FROM gifts")
    rows = cur.fetchall()
    conn.close()

    gifts_list = "🎁 <b>Sovg'alar boshqaruvi</b>\n\n"
    if not rows:
        gifts_list += "❌ Hozircha sovg'alar mavjud emas\n"
    else:
        gifts_list += "📋 <b>Mavjud sovg'alar:</b>\n"
        for id, name, points in rows:
            gifts_list += f"{id}. <b>{name}</b> - {points} ball\n"

    # Sovg'alar boshqaruv tugmalari
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🎁➕ Sovg'a qo'shish"), KeyboardButton(text="🎁➖ Sovg'a o'chirish")],
            [KeyboardButton(text="📜 Sovg'alar ro'yxati")],
            [KeyboardButton(text="🔙 Orqaga")]
        ],
        resize_keyboard=True
    )

    await message.answer(gifts_list, reply_markup=keyboard)


@admin_router.message(F.text == "🎁➕ Sovg'a qo'shish")
async def add_gift_prompt(message: Message, state: FSMContext):
    """Sovg'a qo'shish uchun formani boshlash"""
    await message.answer(
        "🎁 <b>Sovg'a qo'shish</b>\n\n"
        "Quyidagi formatda ma'lumotlarni kiriting:\n"
        "<code>sovg'a_nomi, ball_miqdori</code>\n\n"
        "📍 <b>Misol:</b>\n"
        "<code>Telefon, 1000</code>\n\n"
        "❌ Bekor qilish uchun <b>🔙 Orqaga</b> tugmasini bosing.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🔙 Orqaga")]],
            resize_keyboard=True
        )
    )
    await state.set_state(AdminStates.add_gift)


@admin_router.message(AdminStates.add_gift)
async def process_add_gift(message: Message, state: FSMContext):
    """Sovg'a qo'shishni qayta ishlash"""
    if message.text == "🔙 Orqaga":
        await state.clear()
        await admin_gifts_handler(message)
        return

    try:
        args = message.text.split(",")
        if len(args) != 2:
            await message.answer("❌ Noto'g'ri format. Iltimos, 2 ta parametr kiriting: nomi, ball_miqdori")
            return

        name, points = [arg.strip() for arg in args]

        conn = db.get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO gifts (name, points_required) VALUES (?, ?)",
            (name, int(points)),
        )
        conn.commit()
        conn.close()

        await message.answer(f"✅ <b>{name}</b> sovg'asi muvaffaqiyatli qo'shildi! ({points} ball)")
        await state.clear()
        await admin_gifts_handler(message)

    except ValueError:
        await message.answer("❌ Ball miqdori raqam bo'lishi kerak!")
    except Exception as e:
        await message.answer(f"❌ Xatolik yuz berdi: {str(e)}")


@admin_router.message(F.text == "🎁➖ Sovg'a o'chirish")
async def delete_gift_prompt(message: Message, state: FSMContext):
    """Sovg'a o'chirish uchun formani boshlash"""
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM gifts")
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await message.answer("❌ Hozircha sovg'alar mavjud emas.")
        return

    # Sovg'alar ro'yxatini tugmalar shaklida chiqaramiz
    keyboard_buttons = []
    for gift_id, name in rows:
        keyboard_buttons.append([KeyboardButton(text=f"🗑️ {name}")])

    keyboard_buttons.append([KeyboardButton(text="🔙 Orqaga")])

    keyboard = ReplyKeyboardMarkup(
        keyboard=keyboard_buttons,
        resize_keyboard=True
    )

    await message.answer(
        "🗑️ <b>O'chirmoqchi bo'lgan sovg'ani tanlang:</b>",
        reply_markup=keyboard
    )
    await state.set_state(AdminStates.delete_gift)


@admin_router.message(AdminStates.delete_gift)
async def process_delete_gift(message: Message, state: FSMContext):
    """Sovg'a o'chirishni qayta ishlash"""
    if message.text == "🔙 Orqaga":
        await state.clear()
        await admin_gifts_handler(message)
        return

    if message.text.startswith("🗑️ "):
        gift_name = message.text[3:]  # "🗑️ " ni olib tashlaymiz

        conn = db.get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM gifts WHERE name = ?", (gift_name,))
        conn.commit()
        conn.close()

        await message.answer(f"✅ <b>{gift_name}</b> sovg'asi muvaffaqiyatli o'chirildi!")
        await state.clear()
        await admin_gifts_handler(message)


@admin_router.message(F.text == "📜 Sovg'alar ro'yxati")
async def show_gifts_list(message: Message):
    """Sovg'alar ro'yxatini ko'rsatish"""
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name, points_required FROM gifts ORDER BY points_required")
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await message.answer("📭 Hozircha sovg'alar mavjud emas.")
        return

    message_text = "📜 <b>Mavjud sovg'alar:</b>\n\n"
    for id, name, points in rows:
        message_text += f"<b>{id}. {name}</b>\n"
        message_text += f"   Narxi: {points} ball\n\n"

    await message.answer(message_text)


@admin_router.message(F.text == "🔙 Orqaga")
async def back_to_admin_menu(message: Message, state: FSMContext):
    """Admin menyusiga qaytish"""
    await state.clear()
    await message.answer("👑 Admin menyusi:", reply_markup=admin_menu())


@admin_router.message(F.text == "📩 Xabar yuborish")
async def start_broadcast(message: Message, state: FSMContext):
    """Xabar yuborishni boshlash"""
    await message.answer(
        "📢 <b>Xabar yuborish</b>\n\n"
        "Yubormoqchi bo'lgan xabaringizni kiriting:\n\n"
        "⚠️ <i>Xabar BARCHA obunachilarga yuboriladi (faol bo'lmaganlariga ham)!</i>",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🔙 Bekor qilish")]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
    )
    await state.set_state(AdminStates.broadcast)


@admin_router.message(AdminStates.broadcast, F.text == "🔙 Bekor qilish")
async def cancel_broadcast(message: Message, state: FSMContext):
    """Xabar yuborishni bekor qilish"""
    await state.clear()
    await message.answer("✅ Xabar yuborish bekor qilindi.", reply_markup=admin_menu())


@admin_router.message(AdminStates.broadcast)
async def process_broadcast(message: Message, state: FSMContext, bot: Bot):
    """Xabarni barcha foydalanuvchilarga yuborish"""
    msg_text = message.text

    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users")
    users = cur.fetchall()
    conn.close()

    sent = 0
    failed = 0

    # Barcha foydalanuvchilarga xabar yuborish
    for (uid,) in users:
        try:
            await bot.send_message(uid, msg_text)
            sent += 1
        except Exception as e:
            failed += 1
            logger.error(f"Xabar yuborishda xatolik {uid}: {e}")

    await state.clear()
    await message.answer(
        f"✅ Xabar {sent} foydalanuvchiga yuborildi. {failed} ta xatolik.\n\n"
        f"📊 Jami obunachilar: {len(users)} ta",
        reply_markup=admin_menu()
    )


@admin_router.message(F.text == "📊 Top 10")
async def admin_top10_handler(message: Message):
    """Admin uchun top 10"""
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT full_name, points FROM users ORDER BY points DESC LIMIT 10")
    rows = cur.fetchall()
    conn.close()

    msg = "🏆 <b>TOP 10 talaba:</b>\n\n" + "\n".join(
        [f"{i + 1}. {n} — {p} ball" for i, (n, p) in enumerate(rows)]
    )
    await message.answer(msg, reply_markup=admin_menu())


@admin_router.message(F.text == "🏁 Konkursni yakunlash")
async def end_contest_cmd(message: Message):
    """Konkursni yakunlash"""
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT full_name, points FROM users ORDER BY points DESC LIMIT 10")
    winners = cur.fetchall()
    conn.close()

    text = "🏁 <b>Konkurs yakunlandi! G'oliblar:</b>\n\n"
    for i, (n, p) in enumerate(winners, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        text += f"{medal} {n} — {p} ball\n"

    await message.answer(text, reply_markup=admin_menu())


@admin_router.message(F.text == "🧹 Tozalash")
async def reset_all_data_cmd(message: Message, state: FSMContext):
    """🧹 Barcha ma'lumotlarni tozalash (faqat admin uchun)"""
    await message.answer(
        "⚠️ <b>DIQQAT! Bu barcha ma'lumotlarni butunlay o'chiradi:</b>\n\n"
        "• Barcha foydalanuvchilar ballari\n"
        "• Barcha kanallar\n"
        "• Barcha sovg'alar\n"
        "• Barcha konkurslar tarixi\n"
        "• Barcha referal ma'lumotlari\n\n"
        "✅ <b>Saqlanadigan ma'lumotlar:</b>\n"
        "• Foydalanuvchilar ro'yxati\n"
        "• Obuna ma'lumotlari\n\n"
        "🗑️ <b>Davom etishni xohlaysizmi?</b>\n\n"
        "✅ Ha - barcha ma'lumotlarni tozalash\n"
        "❌ Yo'q - bekor qilish",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="✅ Ha, barchasini tozalash"), KeyboardButton(text="❌ Yo'q, bekor qilish")]
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        )
    )
    await state.set_state(AdminStates.confirm_reset)


@admin_router.message(AdminStates.confirm_reset, F.text == "✅ Ha, barchasini tozalash")
async def confirm_reset(message: Message, state: FSMContext):
    """Tozalashni tasdiqlash"""
    try:
        conn = db.get_connection()
        cur = conn.cursor()

        # 📊 Avval foydalanuvchilar sonini olamiz
        cur.execute("SELECT COUNT(*) FROM users")
        user_count_before = cur.fetchone()[0]

        # ⚠️ Barcha ma'lumotlarni tozalash, lekin users jadvalidagi asosiy ma'lumotlarni saqlab qolamiz
        cur.executescript("""
                          -- Faqat ballar va referal ma'lumotlarini tozalash
                          UPDATE users
                          SET points              = 0,
                              referrals           = 0,
                              total_points_earned = 0,
                              last_active         = CURRENT_TIMESTAMP;

                          -- Boshqa jadvallarni tozalash
                          DELETE
                          FROM channels;
                          DELETE
                          FROM contests;
                          DELETE
                          FROM points_given;
                          DELETE
                          FROM referrals_awarded;
                          DELETE
                          FROM gifts;

                          -- Database ni optimallashtirish
                          VACUUM;
                          """)

        # 🔄 Yangi bo'sh konkurs yaratamiz
        cur.execute("INSERT INTO contests (is_active) VALUES (1)")
        conn.commit()

        # 📊 Tozalashdan keyin foydalanuvchilar sonini tekshiramiz
        cur.execute("SELECT COUNT(*) FROM users")
        user_count_after = cur.fetchone()[0]

        conn.close()

        await message.answer(
            f"🧹 <b>Barcha ma'lumotlar muvaffaqiyatli tozalandi!</b>\n\n"
            f"📊 Statistika:\n"
            f"• Tozalashdan oldin: {user_count_before} foydalanuvchi\n"
            f"• Tozalashdan keyin: {user_count_after} foydalanuvchi\n"
            f"• Barcha ballar nolga tayinlandi\n"
            f"• Barcha referal ma'lumotlari tozalandi\n"
            f"• Yangi konkurs yaratildi\n\n"
            f"✅ <b>Foydalanuvchilar ro'yxati saqlandi!</b>\n"
            f"Endi siz barcha foydalanuvchilarga xabar yuborishingiz mumkin.",
            reply_markup=admin_menu()
        )
        logger.info(f"🧹 Ma'lumotlar tozalandi. Foydalanuvchilar: {user_count_before} -> {user_count_after}")

    except Exception as e:
        logger.error(f"Tozalashda xatolik: {e}")
        await message.answer(
            "❌ Tozalashda xatolik yuz berdi.",
            reply_markup=admin_menu()
        )

    await state.clear()
# === MAIN FUNCTION ===
async def main():
    """Asosiy funksiya"""
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher()

    # Routerlarni qo'shamiz
    dp.include_router(admin_router)
    dp.include_router(router)

    logger.info("🤖 Bot ishga tushdi...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())