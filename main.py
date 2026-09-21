import os
import asyncio
import random
import logging
import time
import aiohttp
import hmac
import hashlib
import html
import urllib.parse
import json
import re
import socket
from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Dict, Any

import aiomysql
from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message, Dice, BufferedInputFile
)

# ==============================================================================
# 1. BOT CONFIGURATION & CONSTANTS
# ==============================================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8665609831:AAFVjWHhbtBRqX1SQtsfLZ97XJDE4Ica4Q8")
ADMIN_ID = int(os.getenv("ADMIN_ID", "7191428925"))
BOT_USERNAME = "@RoniResellerbot"

# ==================== EXTERNAL MySQL DATABASE ====================
MYSQL_HOST     = os.getenv("MYSQL_HOST",     "82.25.121.156")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER     = os.getenv("MYSQL_USER",     "u512584062_RoniStore")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "RoniStore62")
MYSQL_DB       = os.getenv("MYSQL_DB",       "u512584062_RoniModMenu")
# ================================================================

USDT_TO_INR = 90.0
VIP_DISCOUNT_PERCENTAGE = 5.0
VIP_PRICE_INR = 299.0

WELCOME_STICKER_ID = "CAACAgIAAxkBAAEU-WZmH_..."  # Replace with your sticker ID
SPIN_DELAY_SECONDS = 2.5

FIXED_CATEGORIES = [
    "ANDROID NON ROOT PANEL",
    "ANDROID ROOT PANEL",
    "I PHONE PANEL",
    "PC PANEL",
    "GUILD GLORY CREDIT",
    "CARROM PANEL"
]

DEFAULT_EMOJIS = {
    'product_store': '6163205892834598715',
    'profile': '6035084557378654059',
    'add_balance': '5278467510604160626',
    'history': '6160968017304888311',
    'referral': '6032609071373226027',
    'support': '6161112036148255813',
    'ludo_spin': '6147764669361692707',
    'back': '6039539366177541657',
    'upi': '5807750375033278838',
    'binance': '5843689746538173057',
    'reseller': '6120436698695338614',
    'tutorial': '5368653135101310687',
    'download': '6161336001512874965',
    'telegram': '6161096071754818473',
    'whatsapp': '6118193823823698862',
    'welcome': '5312361253610475399',
    'vip': '6086672466132865380',
    'category_android_non_root': '6161172706856282588',
    'category_android_root': '6161449831031118974',
    'category_iphone': '6161399700172840408',
    'category_pc': '5350554349074391003',
    'grid_id': '5474625972751837256',
    'name': '5215399540814781035',
    'account_level': '6129584162992034014',
    'regular_user': '5904630315946611415',
    'wallet': '6210859306602995217',
    'current_balance': '5316711376876485361',
    'global_stats': '6161437856662298090',
    'total_orders': '6160968017304888311',
    'total_spent': '5197503331215361533',
    'total_referrals': '5938196735200333756',
    'joined_grid': '5433614043006903194',
    'info_icon': '6037421444789440735',
    'check_icon': '6161241250239356403',
    'checkbox_icon': '6161437856662298090',
    'shield_icon': '6086672466132865380',
    'money_icon': '5890848474563352982',
    'redeem_icon': '5377624166436445368',
    'wallet_left': '6210859306602995217',
    'wallet_right': '5305699699204837855',
    'point_down': '6161302621027049305',
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("bot_activity.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

def fmt_curr(amount: float) -> str:
    return f"₹{amount:,.2f}"

def natural_sort_key(value: Any) -> List[Any]:
    text = str(value or "").strip()
    return [int(part) if part.isdigit() else part.casefold()
            for part in re.split(r"(\d+)", text)]

# ==============================================================================
# 2. MYSQL CONNECTION POOL & DB FUNCTIONS
# ==============================================================================
_mysql_pool: Optional[aiomysql.Pool] = None

async def init_mysql_pool():
    global _mysql_pool
    if _mysql_pool is None:
        _mysql_pool = await aiomysql.create_pool(
            host=MYSQL_HOST, port=MYSQL_PORT,
            user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB,
            autocommit=True, minsize=1, maxsize=10,
            charset='utf8mb4', connect_timeout=15,
        )
        logger.info(f"✅ MySQL pool connected: {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")
    return _mysql_pool

async def close_mysql_pool():
    global _mysql_pool
    if _mysql_pool:
        _mysql_pool.close()
        await _mysql_pool.wait_closed()
        _mysql_pool = None

def _to_mysql(query: str) -> str:
    """Convert SQLite '?' placeholders to MySQL '%s'."""
    return query.replace('?', '%s')

async def db_query(query: str, params: tuple = (), fetchone: bool = False,
                   fetchall: bool = False, commit: bool = True) -> Any:
    global _mysql_pool
    if _mysql_pool is None:
        await init_mysql_pool()
    try:
        async with _mysql_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(_to_mysql(query), params)
                if fetchone:
                    row = await cur.fetchone()
                    return tuple(row.values()) if row else None
                if fetchall:
                    rows = await cur.fetchall()
                    return [tuple(r.values()) for r in rows]
                return cur.rowcount
    except Exception as e:
        logger.error(f"MySQL Error: {e} | Query: {query} | Params: {params}")
        return None

async def db_update_count(query: str, params: tuple = ()) -> int:
    global _mysql_pool
    if _mysql_pool is None:
        await init_mysql_pool()
    try:
        async with _mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(_to_mysql(query), params)
                return cur.rowcount
    except Exception as e:
        logger.error(f"MySQL Update Error: {e} | Query: {query} | Params: {params}")
        return 0

async def get_setting(key: str, default: str = "") -> str:
    val = await db_query("SELECT value FROM settings WHERE `key`=%s", (key,), fetchone=True)
    return val[0] if val and val[0] else default

async def set_setting(key: str, value: str) -> None:
    await db_query("REPLACE INTO settings (`key`, value) VALUES (%s, %s)", (key, value))

async def log_activity(user_id: int, action: str, details: str = "") -> None:
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        await db_query(
            "INSERT INTO activity_logs (user_id, action, details, timestamp) VALUES (%s, %s, %s, %s)",
            (user_id, action, details, timestamp)
        )
    except Exception as e:
        logger.error(f"Failed to log activity: {e}")

async def get_emoji(slot: str, default_id: str = None) -> str:
    stored = await get_setting(f"emoji_{slot}", "")
    emoji_id = stored if stored and stored.isdigit() else (default_id or DEFAULT_EMOJIS.get(slot, ""))
    if emoji_id:
        return f'<tg-emoji emoji-id="{emoji_id}">✨</tg-emoji>'
    return "✨"

async def get_emoji_icon(slot: str, default_id: str = None) -> str:
    stored = await get_setting(f"emoji_{slot}", "")
    emoji_id = stored if stored and stored.isdigit() else (default_id or DEFAULT_EMOJIS.get(slot, ""))
    return emoji_id

# ==============================================================================
# 3. STRING RESOURCES
# ==============================================================================
UI_TEXTS = {
    "start_menu": (
        "✨ <b>WELCOME TO THE STORE</b>\n\n"
        "{product_store} Product Store : all key purchase & instantly delivery\n"
        "{profile} My Profile : check your account information\n"
        "{add_balance} Add Balance : deposit balance & secure service\n"
        "{history} All History : check all key purchase history\n"
        "{referral} Referral : invite friends & earn rewards\n"
        "{tutorial} Tutorial : view tutorial and work this bot\n"
        "{support} Support : bot problem fixed for support admin\n"
        "{ludo_spin} Ludo Spin : play game and win balance\n"
        "{download} Download Files : download latest apk for safety."
    ),
    "download_files": (
        "🗂 <b><u>DOWNLOAD PREMIUM APK & FILES 📊</u></b>\n\n"
        "🌐 All our highly secured, premium, and updated files\n"
        "are securely hosted on our private channel! ⚠️⛔️\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📱 <b>WHAT YOU GET:</b> 📌\n\n"
        "✔️ Latest APK Updates 🔔\n"
        "✔️ 100% Virus Free & Secure ‼️\n"
        "✔️ All Configs & Scripts 🌸\n"
        "✔️ Complete Installation Guides 🔺\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "⌨️ Tap the button below to access the Download Channel! 📝"
    ),
    "lucky_dice_result": (
        "{ludo_spin} <b><u>LUCKY DICE RESULT 🔨 💯</u></b>\n\n"
        "🎲 <b>Dice Value:</b> {dice_value}\n\n"
        "💸 <b>You Won:</b> {won_amount}\n"
        "💰 <b>Total Balance:</b> {new_balance}\n\n"
        "Congratulations! Come back after 24 hours."
    ),
    "vip_menu": (
        "🌟 <b><u>VIP MEMBERSHIP CLUB</u></b> 🌟\n\n"
        "Unlock premium benefits and permanent discounts!\n\n"
        "💎 <b>VIP Benefits:</b>\n"
        "• Flat 15% off on ALL products (Stacks with Reseller!)\n"
        "• Priority Support\n"
        "• Exclusive VIP-only giveaways\n\n"
        "💳 <b>VIP Price:</b> ₹299.00 (Lifetime)\n"
        "👤 <b>Your Status:</b> {vip_status}"
    ),
    "add_balance_menu": (
        "{add_balance} <b>ADD BALANCE</b> {info_icon}\n\n"
        "{info_icon} Select your preferred payment method. {check_icon}\n\n"
        "┣ {upi} UPI — Fast Indian payments {checkbox_icon}\n"
        "┣ {binance} Binance — Crypto payments {checkbox_icon}\n\n"
        "{shield_icon} Payments are verified securely. {check_icon}"
    )
}

async def get_ui_text(key: str, **kwargs) -> str:
    val = await db_query("SELECT value FROM settings WHERE `key`=%s", (f"ui_{key}",), fetchone=True)
    template = val[0] if val and val[0] else UI_TEXTS.get(key, "")

    emoji_map = {
        '{product_store}': await get_emoji('product_store'),
        '{profile}': await get_emoji('profile'),
        '{add_balance}': await get_emoji('add_balance'),
        '{history}': await get_emoji('history'),
        '{referral}': await get_emoji('referral'),
        '{tutorial}': await get_emoji('tutorial'),
        '{support}': await get_emoji('support'),
        '{ludo_spin}': await get_emoji('ludo_spin'),
        '{download}': await get_emoji('download'),
        '{telegram}': await get_emoji('telegram'),
        '{whatsapp}': await get_emoji('whatsapp'),
        '{upi}': await get_emoji('upi'),
        '{binance}': await get_emoji('binance'),
        '{info_icon}': await get_emoji('info_icon'),
        '{check_icon}': await get_emoji('check_icon'),
        '{checkbox_icon}': await get_emoji('checkbox_icon'),
        '{shield_icon}': await get_emoji('shield_icon'),
        '{money_icon}': await get_emoji('money_icon'),
        '{redeem_icon}': await get_emoji('redeem_icon'),
        '{wallet_left}': await get_emoji('wallet_left'),
        '{wallet_right}': await get_emoji('wallet_right'),
        '{point_down}': await get_emoji('point_down'),
    }
    for placeholder, emoji_tag in emoji_map.items():
        template = template.replace(placeholder, emoji_tag)

    if kwargs:
        try:
            return template.format(**kwargs)
        except KeyError as e:
            logger.warning(f"Missing formatting key for template {key}: {e}")
    return template

# ==============================================================================
# 4. DATABASE INITIALISATION & MIGRATION (MySQL)
# ==============================================================================
async def init_db() -> None:
    """Create all tables using MySQL syntax."""
    await init_mysql_pool()

    create_statements = [
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            phone VARCHAR(32),
            first_name VARCHAR(255),
            username VARCHAR(255),
            balance DOUBLE DEFAULT 0.0,
            account_type VARCHAR(32) DEFAULT 'Regular',
            orders_count INT DEFAULT 0,
            spent DOUBLE DEFAULT 0.0,
            referrals_count INT DEFAULT 0,
            referral_earned DOUBLE DEFAULT 0.0,
            referred_by BIGINT,
            last_spin VARCHAR(32),
            joined_date VARCHAR(32),
            is_reseller INT DEFAULT 0,
            reseller_since VARCHAR(32),
            total_saved DOUBLE DEFAULT 0.0,
            is_banned INT DEFAULT 0,
            warnings INT DEFAULT 0,
            is_vip INT DEFAULT 0,
            vip_since VARCHAR(32)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS products (
            id INT AUTO_INCREMENT PRIMARY KEY,
            category VARCHAR(255),
            panel_name VARCHAR(255) DEFAULT '',
            name VARCHAR(255),
            price_inr DOUBLE,
            reseller_price DOUBLE DEFAULT 0.0,
            stock INT,
            apk_link TEXT,
            validity VARCHAR(255) DEFAULT 'Lifetime',
            device_limit VARCHAR(255) DEFAULT '1 Device',
            is_active INT DEFAULT 1,
            external_enabled INT DEFAULT 0,
            external_product_id VARCHAR(255) DEFAULT '',
            requires_android_id INT DEFAULT 0,
            external_duration VARCHAR(255) DEFAULT ''
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS product_keys (
            id INT AUTO_INCREMENT PRIMARY KEY,
            product_id INT,
            key_text TEXT,
            is_used INT DEFAULT 0
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS orders (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id BIGINT,
            product_name TEXT,
            price_paid DOUBLE,
            delivered_key TEXT,
            purchase_date VARCHAR(32)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS tickets (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id BIGINT,
            message TEXT,
            status VARCHAR(32) DEFAULT 'Open',
            created_at VARCHAR(32)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS settings (
            `key` VARCHAR(191) PRIMARY KEY,
            value TEXT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS coupons (
            code VARCHAR(191) PRIMARY KEY,
            amount DOUBLE,
            uses_left INT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS redeemed (
            user_id BIGINT,
            code VARCHAR(191)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS transactions (
            order_id VARCHAR(191) PRIMARY KEY,
            user_id BIGINT,
            amount_inr DOUBLE,
            status VARCHAR(32),
            timestamp BIGINT,
            purpose VARCHAR(32) DEFAULT 'wallet',
            product_id INT,
            quantity INT DEFAULT 1,
            payment_method VARCHAR(32) DEFAULT 'wallet'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS purchase_locks (
            user_id BIGINT PRIMARY KEY,
            product_id INT NOT NULL,
            quantity INT NOT NULL,
            created_at BIGINT NOT NULL,
            expires_at BIGINT NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS crypto_txns (
            txid VARCHAR(191) PRIMARY KEY,
            user_id BIGINT,
            amount_usdt DOUBLE,
            timestamp BIGINT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS spin_rewards (
            id INT AUTO_INCREMENT PRIMARY KEY,
            amount DOUBLE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS activity_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id BIGINT,
            action VARCHAR(255),
            details TEXT,
            timestamp VARCHAR(32)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    ]

    for stmt in create_statements:
        await db_query(stmt)

    # MySQL-safe migrations (ignore duplicate column errors)
    migrations = [
        "ALTER TABLE users ADD COLUMN is_vip INT DEFAULT 0",
        "ALTER TABLE users ADD COLUMN vip_since VARCHAR(32)",
        "ALTER TABLE products ADD COLUMN is_active INT DEFAULT 1",
        "ALTER TABLE tickets ADD COLUMN created_at VARCHAR(32)",
        "ALTER TABLE users ADD COLUMN is_banned INT DEFAULT 0",
        "ALTER TABLE users ADD COLUMN warnings INT DEFAULT 0",
        "ALTER TABLE products ADD COLUMN panel_name VARCHAR(255) DEFAULT ''",
        "ALTER TABLE products ADD COLUMN external_enabled INT DEFAULT 0",
        "ALTER TABLE products ADD COLUMN external_product_id VARCHAR(255) DEFAULT ''",
        "ALTER TABLE products ADD COLUMN requires_android_id INT DEFAULT 0",
        "ALTER TABLE products ADD COLUMN external_duration VARCHAR(255) DEFAULT ''",
        "ALTER TABLE transactions ADD COLUMN purpose VARCHAR(32) DEFAULT 'wallet'",
        "ALTER TABLE transactions ADD COLUMN product_id INT",
        "ALTER TABLE transactions ADD COLUMN quantity INT DEFAULT 1",
        "ALTER TABLE transactions ADD COLUMN payment_method VARCHAR(32) DEFAULT 'wallet'",
    ]
    for mig in migrations:
        try:
            await db_query(mig)
        except Exception:
            pass

    # Backfill API duration for existing products
    try:
        await db_query("UPDATE products SET external_duration = validity WHERE COALESCE(external_duration, '') = ''")
    except Exception:
        pass

    cnt = await db_query("SELECT COUNT(*) FROM spin_rewards", fetchone=True)
    if cnt and cnt[0] == 0:
        for amt in [0.0, 1.0, 2.0, 5.0, 10.0]:
            await db_query("INSERT INTO spin_rewards (amount) VALUES (%s)", (amt,))

    default_settings = [
        ('spin_status', 'ON'),
        ('daily_spin_limit', '50.0'),
        ('reseller_system_status', 'ON'),
        ('bot_status', 'ON'),
        ('how_to_video', 'None'),
        ('all_files_link', 'None'),
        ('pay0_api', '5eb9d997852e235c88eb8c3ee6524766'),
        ('binance_api', ''),
        ('binance_secret', ''),
        ('binance_address', ''),
        ('vip_status', 'OFF'),
        ('reseller_setup_fee', '200.0'),
        ('reseller_min_balance', '500.0'),
        ('migration_done', '0'),
        ('support_telegram', 'https://t.me/YOUR_SUPPORT'),
        ('support_whatsapp', 'https://wa.me/YOUR_NUMBER'),
        ('ui_start_menu', UI_TEXTS['start_menu']),
        ('ui_download_files', UI_TEXTS['download_files']),
        ('ui_lucky_dice_result', UI_TEXTS['lucky_dice_result']),
        ('ui_vip_menu', UI_TEXTS['vip_menu']),
        ('ui_add_balance_menu', UI_TEXTS['add_balance_menu']),
        ('external_api_url', 'https://adminpanels.shop/api/reseller_v1.php'),
        ('external_api_key', ''),
        ('external_master_key', ''),
    ]
    for slot, emoji_id in DEFAULT_EMOJIS.items():
        default_settings.append((f"emoji_{slot}", emoji_id))

    for key, val in default_settings:
        await db_query("INSERT IGNORE INTO settings (`key`, value) VALUES (%s, %s)", (key, val))

    logger.info("✅ MySQL database initialised.")


async def migrate_categories() -> None:
    done = await get_setting("migration_done", "0")
    logger.info("Forcing emoji and UI text updates...")
    for slot, emoji_id in DEFAULT_EMOJIS.items():
        await set_setting(f"emoji_{slot}", emoji_id)
    await set_setting("ui_start_menu", UI_TEXTS['start_menu'])
    await set_setting("ui_add_balance_menu", UI_TEXTS['add_balance_menu'])
    await set_setting("ui_download_files", UI_TEXTS['download_files'])
    await set_setting("ui_lucky_dice_result", UI_TEXTS['lucky_dice_result'])
    await set_setting("ui_vip_menu", UI_TEXTS['vip_menu'])
    logger.info("UI texts and emojis updated.")

    if done == "1":
        return

    logger.info("Running category migration...")
    mapping = {
        "android non root panel": "ANDROID NON ROOT PANEL",
        "android root panel": "ANDROID ROOT PANEL",
        "iphone panel": "IPHONE PANEL",
        "pc panel": "PC PANEL",
        "guild calory credit": "GUILD CALORY CREDIT",
        "carrom panel": "CARROM PANEL",
    }
    for old, new in mapping.items():
        await db_query("UPDATE products SET category = %s WHERE LOWER(category) = %s", (new, old))

    await db_query(
        "UPDATE products SET category = 'ANDROID NON ROOT PANEL' "
        "WHERE LOWER(category) NOT IN (%s, %s, %s, %s, %s, %s)",
        ("android non root panel", "android root panel", "iphone panel",
         "pc panel", "guild calory credit", "carrom panel")
    )
    await set_setting("migration_done", "1")
    logger.info("Category migration complete.")

# ==============================================================================
# 5. MIDDLEWARES & SECURITY
# ==============================================================================
async def hacker_loading(message: Message, text: str = "Decrypting Data") -> Message:
    msg = await message.answer(f"⚡ {text}\n[□□□] 0%")
    await asyncio.sleep(0.3)
    await msg.edit_text(f"⚡ {text}\n[■□□] 33%", parse_mode='HTML')
    await asyncio.sleep(0.3)
    await msg.edit_text(f"⚡ {text}\n[■■□] 66%", parse_mode='HTML')
    await asyncio.sleep(0.3)
    await msg.edit_text(f"⚡ {text}\n[■■■] 100%", parse_mode='HTML')
    return msg

class GlobalSecurityMiddleware(BaseMiddleware):
    def __init__(self):
        super().__init__()
        self.last_action_times = {}

    async def __call__(self, handler, event, data):
        user_id = event.from_user.id
        now = time.time()
        if user_id in self.last_action_times:
            if now - self.last_action_times[user_id] < 0.3:
                return
        self.last_action_times[user_id] = now

        if user_id != ADMIN_ID:
            user_info = await db_query("SELECT is_banned FROM users WHERE user_id=%s", (user_id,), fetchone=True)
            if user_info and user_info[0] == 1:
                msg = "🚫 <b>ACCESS DENIED</b>\nYou have been banned from using this bot.\nContact support if you think this is a mistake."
                if isinstance(event, Message):
                    await event.answer(msg)
                elif isinstance(event, CallbackQuery):
                    await event.answer(msg, show_alert=True)
                return

            status_check = await db_query("SELECT value FROM settings WHERE `key`='bot_status'", fetchone=True)
            status = status_check[0] if status_check else 'ON'
            if status == 'OFF':
                msg = "⚠️ <b>Store Maintenance</b>\n\nThe store is currently offline for updates. Please check back later!"
                if isinstance(event, Message):
                    await event.answer(msg)
                elif isinstance(event, CallbackQuery):
                    await event.answer("⚠️ Bot is currently OFF for Maintenance.", show_alert=True)
                return

        return await handler(event, data)

dp.message.middleware(GlobalSecurityMiddleware())
dp.callback_query.middleware(GlobalSecurityMiddleware())

# ==============================================================================
# 6. FSM STATES
# ==============================================================================
class UserStates(StatesGroup):
    wait_for_ticket = State()
    wait_for_redeem = State()
    wait_for_crypto_txid = State()
    wait_for_product_binance_txid = State()
    custom_amount_input = State()

class AdminStates(StatesGroup):
    add_prod_category = State()
    add_prod_panel_name = State()
    add_prod_name = State()
    add_prod_validity = State()
    add_prod_device_limit = State()
    add_prod_price = State()
    add_prod_reseller_price = State()
    add_prod_apk = State()
    add_prod_keys = State()
    edit_prod_field = State()
    wait_for_new_value = State()
    wait_for_add_keys = State()
    wait_for_delete_key = State()
    broadcast_msg = State()
    add_coupon_code = State()
    add_coupon_amount = State()
    add_coupon_uses = State()
    wait_for_pay0_api = State()
    wait_for_binance_api = State()
    wait_for_binance_secret = State()
    wait_for_binance_address = State()
    ticket_reply_msg = State()
    reseller_manage_id = State()
    manage_target_user = State()
    wait_for_add_money = State()
    wait_for_minus_money = State()
    wait_for_warning = State()
    spin_add_reward = State()
    spin_set_limit = State()
    wait_for_howto_video = State()
    wait_for_all_files_link = State()
    edit_ui_text = State()
    edit_reseller_price = State()
    wait_for_reseller_setup_fee = State()
    wait_for_reseller_min_balance = State()
    confirm_ban = State()
    wait_for_support_telegram = State()
    wait_for_support_whatsapp = State()
    wait_for_category_emoji = State()
    wait_for_panel_emoji_id = State()
    wait_for_emoji_slot = State()
    wait_for_ext_url = State()
    wait_for_ext_key = State()
    wait_for_ext_master = State()
    add_prod_external = State()
    add_prod_external_product_id = State()
    add_prod_external_duration = State()

# ==============================================================================
# 7. KEYBOARDS
# ==============================================================================
async def get_category_emoji(category: str) -> str:
    slot_map = {
        "ANDROID NON ROOT PANEL": "category_android_non_root",
        "ANDROID ROOT PANEL": "category_android_root",
        "IPHONE PANEL": "category_iphone",
        "PC PANEL": "category_pc",
        "GUILD CALORY CREDIT": "product_store",
        "CARROM PANEL": "product_store",
    }
    slot = slot_map.get(category)
    if slot:
        return await get_emoji_icon(slot, DEFAULT_EMOJIS.get(slot, ""))
    return ""

async def get_panel_emoji(panel_name: str) -> str:
    stored = await get_setting(f"panel_emoji_{panel_name}", "")
    if stored and stored.isdigit():
        return stored
    return await get_emoji_icon("product_store")

def contact_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Verify Contact", request_contact=True)]],
        resize_keyboard=True, one_time_keyboard=True
    )

async def main_menu_kb(user_id: Optional[int] = None) -> InlineKeyboardMarkup:
    status_check = await db_query("SELECT value FROM settings WHERE `key`='reseller_system_status'", fetchone=True)
    sys_status = status_check[0] if status_check else 'ON'
    vip_sys_check = await db_query("SELECT value FROM settings WHERE `key`='vip_status'", fetchone=True)
    vip_system = vip_sys_check[0] if vip_sys_check else 'OFF'

    is_reseller = False
    if user_id:
        user_check = await db_query("SELECT is_reseller FROM users WHERE user_id=%s", (user_id,), fetchone=True)
        if user_check:
            is_reseller = bool(user_check[0])

    kb = InlineKeyboardMarkup(inline_keyboard=[])
    kb.inline_keyboard.append([InlineKeyboardButton(
        text="Product Store", callback_data="menu_shop",
        icon_custom_emoji_id=await get_emoji_icon("product_store"), style="primary")])
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="My Profile", callback_data="menu_profile",
                             icon_custom_emoji_id=await get_emoji_icon("profile"), style="success"),
        InlineKeyboardButton(text="Add Balance", callback_data="menu_add_balance",
                             icon_custom_emoji_id=await get_emoji_icon("add_balance"), style="success")])
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="All History", callback_data="menu_orders",
                             icon_custom_emoji_id=await get_emoji_icon("history"), style="success"),
        InlineKeyboardButton(text="Referral", callback_data="menu_referral",
                             icon_custom_emoji_id=await get_emoji_icon("referral"), style="success")])
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="Tutorials", callback_data="menu_how_to",
                             icon_custom_emoji_id=await get_emoji_icon("tutorial"), style="success"),
        InlineKeyboardButton(text="Support", callback_data="menu_support",
                             icon_custom_emoji_id=await get_emoji_icon("support"), style="success")])
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="Ludo Spin", callback_data="menu_spin_landing",
                             icon_custom_emoji_id=await get_emoji_icon("ludo_spin"), style="success"),
        InlineKeyboardButton(text="Download Files", callback_data="menu_all_files",
                             icon_custom_emoji_id=await get_emoji_icon("download"), style="success")])

    extras_row = []
    if sys_status == 'ON' or is_reseller:
        extras_row.append(InlineKeyboardButton(text="Reseller Panel", callback_data="menu_reseller_dash",
                                               icon_custom_emoji_id=await get_emoji_icon("reseller"), style="success"))
    if vip_system == 'ON':
        extras_row.append(InlineKeyboardButton(text="VIP Club", callback_data="menu_vip_dash",
                                               icon_custom_emoji_id=await get_emoji_icon("vip"), style="success"))
    if extras_row:
        kb.inline_keyboard.append(extras_row)
    return kb

async def back_kb(callback: str = "back_main") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="BACK", callback_data=callback,
                             icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")
    ]])

async def admin_kb() -> InlineKeyboardMarkup:
    status = await db_query("SELECT value FROM settings WHERE `key`='bot_status'", fetchone=True)
    status_val = status[0] if status else 'ON'
    vip_status = await db_query("SELECT value FROM settings WHERE `key`='vip_status'", fetchone=True)
    vip_val = vip_status[0] if vip_status else 'OFF'

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Bot Statistics", callback_data="admin_view_stats",
                              icon_custom_emoji_id=await get_emoji_icon("global_stats"), style="success")],
        [InlineKeyboardButton(text="👥 User Control Panel", callback_data="admin_user_control_start",
                              icon_custom_emoji_id=await get_emoji_icon("profile"), style="success")],
        [InlineKeyboardButton(text="➕ Add Product", callback_data="admin_add_prod",
                              icon_custom_emoji_id=await get_emoji_icon("product_store"), style="success"),
         InlineKeyboardButton(text="📦 Manage Products", callback_data="admin_manage_prods",
                              icon_custom_emoji_id=await get_emoji_icon("product_store"), style="success")],
        [InlineKeyboardButton(text="👑 Reseller Mgmt", callback_data="admin_reseller_menu",
                              icon_custom_emoji_id=await get_emoji_icon("reseller"), style="success"),
         InlineKeyboardButton(text="🎰 Spin Settings", callback_data="admin_spin_menu",
                              icon_custom_emoji_id=await get_emoji_icon("ludo_spin"), style="success")],
        [InlineKeyboardButton(text="🎟 Create Coupon", callback_data="admin_create_coupon",
                              icon_custom_emoji_id=await get_emoji_icon("redeem_icon"), style="success"),
         InlineKeyboardButton(text="📢 Broadcast", callback_data="admin_broadcast_btn",
                              icon_custom_emoji_id=await get_emoji_icon("telegram"), style="success")],
        [InlineKeyboardButton(text="🎫 View Tickets", callback_data="admin_view_tickets",
                              icon_custom_emoji_id=await get_emoji_icon("support"), style="success"),
         InlineKeyboardButton(text="📹 Tutorial Video", callback_data="admin_set_video",
                              icon_custom_emoji_id=await get_emoji_icon("tutorial"), style="success")],
        [InlineKeyboardButton(text="🔗 All Files Link", callback_data="admin_set_all_files",
                              icon_custom_emoji_id=await get_emoji_icon("download"), style="success"),
         InlineKeyboardButton(text="🎨 Edit All Emojis", callback_data="admin_edit_emojis",
                              icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="success")],
        [InlineKeyboardButton(text="⚙️ Pay0 Setup", callback_data="admin_setup_pay0",
                              icon_custom_emoji_id=await get_emoji_icon("upi"), style="success"),
         InlineKeyboardButton(text="🪙 Binance Setup", callback_data="admin_setup_binance",
                              icon_custom_emoji_id=await get_emoji_icon("binance"), style="success")],
        [InlineKeyboardButton(text="🔗 External Key API", callback_data="admin_setup_external_api",
                              icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="success")],
        [InlineKeyboardButton(text="✏️ Edit UI Texts", callback_data="admin_edit_ui_menu",
                              icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="success"),
         InlineKeyboardButton(text="📝 Edit Reseller Price", callback_data="admin_edit_reseller_price",
                              icon_custom_emoji_id=await get_emoji_icon("money_icon"), style="success")],
        [InlineKeyboardButton(text="💰 Reseller Fee", callback_data="admin_set_reseller_fee",
                              icon_custom_emoji_id=await get_emoji_icon("money_icon"), style="success"),
         InlineKeyboardButton(text="💳 Min Balance", callback_data="admin_set_reseller_min",
                              icon_custom_emoji_id=await get_emoji_icon("money_icon"), style="success")],
        [InlineKeyboardButton(text="📞 Set Support Links", callback_data="admin_set_support_links",
                              icon_custom_emoji_id=await get_emoji_icon("support"), style="success"),
         InlineKeyboardButton(text="🎨 Set Category Emojis", callback_data="admin_set_category_emojis",
                              icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="success")],
        [InlineKeyboardButton(text="🖼 Set Panel Emojis", callback_data="admin_set_panel_emojis",
                              icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="success")],
        [InlineKeyboardButton(
            text=f"Bot Status: {status_val} {'🟢' if status_val == 'ON' else '🔴'}",
            callback_data="admin_toggle_bot",
            icon_custom_emoji_id=await get_emoji_icon("check_icon"),
            style="success" if status_val == 'ON' else "danger")],
        [InlineKeyboardButton(
            text=f"VIP System: {vip_val} {'🟢' if vip_val == 'ON' else '🔴'}",
            callback_data="admin_toggle_vip_sys",
            icon_custom_emoji_id=await get_emoji_icon("vip"),
            style="success" if vip_val == 'ON' else "danger")],
    ])
    return kb

async def admin_back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back",
                             icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")
    ]])

# ==============================================================================
# 8. NOTIFICATIONS
# ==============================================================================
async def send_advanced_notification(user_id: int, notif_type: str, amount: float,
                                     product: str = None, key: str = None,
                                     gateway: str = "Pay0") -> None:
    user_info = await db_query("SELECT first_name, phone, username, is_reseller, is_vip FROM users WHERE user_id=%s",
                               (user_id,), fetchone=True)
    name = user_info[0] if user_info else "Unknown"
    phone = user_info[1] if user_info and user_info[1] else "Not Provided"
    username = f"@{user_info[2]}" if user_info and user_info[2] else "None"

    tags = []
    if user_info and user_info[3]: tags.append("👑 Reseller")
    if user_info and user_info[4]: tags.append("🌟 VIP")
    tag_str = " | ".join(tags) if tags else "👤 Regular"

    time_now = datetime.now().strftime("%d-%m-%Y %I:%M %p")

    if notif_type == "ORDER":
        title = "🛒 <b>NEW ORDER PROCESSED!</b> 🛒"
        details = (f"📦 <b>Product:</b> {product}\n🔑 <b>Key:</b> <code>{key}</code>\n"
                   f"💰 <b>Amount Paid:</b> ₹{amount:.2f}\n📅 <b>Time:</b> {time_now}")
    else:
        title = "💰 <b>NEW WALLET DEPOSIT!</b> 💰"
        details = (f"💵 <b>Amount Added:</b> ₹{amount:.2f}\n🧾 <b>Gateway:</b> {gateway}\n"
                   f"🆔 <b>Reference:</b> <code>{product}</code>\n📅 <b>Time:</b> {time_now}")

    msg = (f"{title}\n━━━━━━━━━━━━━━━━━━\n👤 <b>Name:</b> {name}\n"
           f"🆔 <b>User ID:</b> <code>{user_id}</code>\n📱 <b>Phone:</b> {phone}\n"
           f"🔗 <b>Username:</b> {username}\n🏷 <b>Status:</b> {tag_str}\n"
           f"━━━━━━━━━━━━━━━━━━\n{details}")
    try:
        await bot.send_message(ADMIN_ID, msg, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Failed to send admin notification: {e}")

# ==============================================================================
# 9. PAYMENT VERIFIER (Pay0)
# ==============================================================================
async def run_payment_verification(user_id: int, order_id: str, reply_target: Any) -> None:
    txn = await db_query(
        "SELECT amount_inr, status, timestamp, purpose FROM transactions WHERE order_id=%s AND user_id=%s",
        (order_id, user_id), fetchone=True)
    if not txn:
        err = "❌ Invalid or fake Order ID detected in system!"
        if isinstance(reply_target, CallbackQuery):
            await reply_target.answer(err, show_alert=True)
        else:
            await reply_target.answer(err)
        return
    amount, status, ts, purpose = txn

    if status in ('completed', 'processing', 'paid') and purpose == 'product':
        if status == 'paid':
            await fulfill_product_transaction(order_id, user_id,
                reply_target.message if isinstance(reply_target, CallbackQuery) else reply_target)
        else:
            msg = "⏳ This product order is already being processed. Please wait for the key."
            if isinstance(reply_target, CallbackQuery):
                await reply_target.answer(msg, show_alert=True)
            else:
                await reply_target.answer(msg)
        return
    if status == 'paid':
        msg = "✅ This payment has already been securely credited to your wallet."
        if isinstance(reply_target, CallbackQuery):
            await reply_target.answer(msg, show_alert=True)
        else:
            await reply_target.answer(msg)
        return
    if status == 'expired':
        msg = "❌ This order has expired. Please create a new payment request."
        if isinstance(reply_target, CallbackQuery):
            await reply_target.answer(msg, show_alert=True)
        else:
            await reply_target.answer(msg)
        return

    if time.time() - ts > 900 and status == 'pending':
        await db_query("UPDATE transactions SET status='expired' WHERE order_id=%s AND status='pending'", (order_id,))
        if purpose == 'product':
            await release_purchase_lock(user_id)
        err_msg = "⏳ <b>Payment Timed Out!</b>\nThe 15-minute payment window has expired."
        if isinstance(reply_target, CallbackQuery):
            await reply_target.message.edit_text(err_msg, reply_markup=await back_kb("menu_shop" if purpose == 'product' else "menu_add_balance"), parse_mode='HTML')
        else:
            await reply_target.answer(err_msg, reply_markup=await back_kb("menu_shop" if purpose == 'product' else "menu_add_balance"))
        return

    api_key_check = await db_query("SELECT value FROM settings WHERE `key`='pay0_api'", fetchone=True)
    if not api_key_check or not api_key_check[0]:
        msg = "⚠️ Gateway API key missing. Administrator needs to configure it."
        if isinstance(reply_target, CallbackQuery):
            await reply_target.answer(msg, show_alert=True)
        else:
            await reply_target.answer(msg)
        return

    try:
        payload = {
            "user_token": api_key_check[0],
            "order_id": order_id,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post("https://pay0.shop/api/check-order-status",
                                    data=payload,
                                    headers={"Content-Type": "application/x-www-form-urlencoded"}) as resp:
                res_json = await resp.json(content_type=None) if resp.status == 200 else {}

        status_val = res_json.get("status")
        is_success = (status_val is True) or (str(status_val).lower() in ("true", "success", "1"))

        if not is_success:
            err = f"⚠️ Gateway Error: {res_json.get('message', 'Unknown Error')}"
            if isinstance(reply_target, CallbackQuery):
                await reply_target.answer(err, show_alert=True)
            else:
                await reply_target.answer(err)
            return

        real_status = str(res_json.get("result", {}).get("txnStatus", "PENDING")).upper()
        if real_status == "SUCCESS":
            claimed = await db_update_count(
                "UPDATE transactions SET status='paid' WHERE order_id=%s AND status='pending'", (order_id,))
            if not claimed:
                latest = await db_query("SELECT status,purpose FROM transactions WHERE order_id=%s", (order_id,), fetchone=True)
                if latest and latest[1] == 'product' and latest[0] in ('paid','processing','completed'):
                    if isinstance(reply_target, CallbackQuery):
                        await reply_target.answer("⏳ Payment already accepted; your key is being prepared.", show_alert=True)
                    return await fulfill_product_transaction(order_id, user_id, reply_target.message)
                if isinstance(reply_target, CallbackQuery):
                    await reply_target.answer("⏳ Payment was already processed.", show_alert=True)
                return

            if purpose == 'product':
                await fulfill_product_transaction(order_id, user_id,
                    reply_target.message if isinstance(reply_target, CallbackQuery) else reply_target)
            else:
                await db_query("UPDATE users SET balance = balance + %s WHERE user_id=%s", (amount, user_id))
                success_msg = f"🎉 <b>VERIFICATION SUCCESSFUL!</b>\n\n✅ {fmt_curr(amount)} has been added to your wallet securely."
                if isinstance(reply_target, CallbackQuery):
                    await reply_target.message.edit_text(success_msg, reply_markup=await back_kb(), parse_mode='HTML')
                else:
                    await reply_target.answer(success_msg, reply_markup=await back_kb())
                await send_advanced_notification(user_id, "DEPOSIT", amount, product=order_id, gateway="Pay0")
                await log_activity(user_id, "DEPOSIT_SUCCESS", f"Amount: {amount}, Gateway: Pay0, Order: {order_id}")
        elif real_status == "PENDING":
            fail_msg = "⏳ Payment is still Pending. Please wait a moment and verify again."
            if isinstance(reply_target, CallbackQuery):
                await reply_target.answer(fail_msg, show_alert=True)
            else:
                await reply_target.answer(fail_msg)
        else:
            fail_msg = f"❌ Payment Failed or Cancelled (Gateway Status: {real_status})."
            if isinstance(reply_target, CallbackQuery):
                await reply_target.answer(fail_msg, show_alert=True)
            else:
                await reply_target.answer(fail_msg)
    except Exception:
        logger.exception("Pay0 verification error")
        err = "⚠️ Unable to connect to payment gateway right now."
        if isinstance(reply_target, CallbackQuery):
            await reply_target.answer(err, show_alert=True)
        else:
            await reply_target.answer(err)


async def auto_verify_task() -> None:
    while True:
        await asyncio.sleep(15)
        api_key_check = await db_query("SELECT value FROM settings WHERE `key`='pay0_api'", fetchone=True)
        if not api_key_check or not api_key_check[0]:
            continue
        pending_txns = await db_query(
            "SELECT order_id, user_id, amount_inr, timestamp, purpose FROM transactions WHERE status='pending'",
            fetchall=True) or []
        for order_id, user_id, amount, ts, purpose in pending_txns:
            if time.time() - ts > 900:
                await db_query("UPDATE transactions SET status='expired' WHERE order_id=%s AND status='pending'", (order_id,))
                if purpose == 'product':
                    await release_purchase_lock(user_id)
                try:
                    await bot.send_message(user_id, f"⏳ <b>Order Expired!</b>\nYour payment window for order <code>{order_id}</code> has timed out.", parse_mode='HTML')
                except Exception:
                    pass
                continue
            try:
                payload = {
                    "user_token": api_key_check[0],
                    "order_id": order_id,
                }
                async with aiohttp.ClientSession() as session:
                    async with session.post("https://pay0.shop/api/check-order-status",
                                            data=payload,
                                            headers={"Content-Type": "application/x-www-form-urlencoded"}) as resp:
                        res_json = await resp.json(content_type=None) if resp.status == 200 else {}
                status_val = res_json.get("status")
                is_success = (status_val is True) or (str(status_val).lower() in ("true", "success", "1"))
                txn_status = str(res_json.get("result", {}).get("txnStatus", "")).upper()
                if is_success and txn_status == "SUCCESS":
                    claimed = await db_update_count(
                        "UPDATE transactions SET status='paid' WHERE order_id=%s AND status='pending'", (order_id,))
                    if not claimed:
                        continue
                    if purpose == 'product':
                        try:
                            msg = await bot.send_message(user_id, "✨ <b>AUTO-VERIFIED!</b>\n\nPayment received. Preparing your keys...", parse_mode='HTML')
                            await fulfill_product_transaction(order_id, user_id, msg)
                        except Exception:
                            logger.exception("Auto product fulfillment failed for %s", order_id)
                    else:
                        await db_query("UPDATE users SET balance = balance + %s WHERE user_id=%s", (amount, user_id))
                        try:
                            await bot.send_message(user_id, f"✨ <b>AUTO-VERIFIED!</b>\n\n✅ Your payment was detected successfully. {fmt_curr(amount)} has been added to your balance!", parse_mode='HTML')
                        except Exception:
                            pass
                        await send_advanced_notification(user_id, "DEPOSIT", amount, product=order_id, gateway="Pay0 Auto")
                        await log_activity(user_id, "DEPOSIT_AUTO_SUCCESS", f"Amount: {amount}, Gateway: Pay0 Auto, Order: {order_id}")
            except Exception as e:
                logger.debug(f"Auto-verify minor exception ignored: {e}")

# ==============================================================================
# 10. ONBOARDING & START
# ==============================================================================
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    try:
        await message.answer_sticker(WELCOME_STICKER_ID)
    except Exception:
        pass

    args = message.text.split()
    if len(args) > 1 and args[1].startswith("v_"):
        order_id = args[1].split("v_")[1]
        msg = await message.answer("🔄 <b>Verifying your payment securely...</b>\n<i>Connecting to gateway...</i>", parse_mode='HTML')
        await run_payment_verification(message.from_user.id, order_id, msg)
        return

    referred_by = None
    if len(args) > 1 and args[1].startswith("ref_"):
        try:
            referred_by = int(args[1].split("_")[1])
        except Exception:
            pass

    user = await db_query("SELECT phone FROM users WHERE user_id=%s", (message.from_user.id,), fetchone=True)
    current_username = message.from_user.username or ""
    await db_query("UPDATE users SET username=%s WHERE user_id=%s", (current_username, message.from_user.id))

    if not user or not user[0]:
        await db_query(
            "INSERT IGNORE INTO users (user_id, first_name, username, referred_by, joined_date) VALUES (%s, %s, %s, %s, %s)",
            (message.from_user.id, message.from_user.first_name, current_username, referred_by,
             datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        await log_activity(message.from_user.id, "ACCOUNT_CREATED")
        await message.answer(
            "<b>🛡 VERIFICATION REQUIRED</b>\n\nTo safeguard your orders and account, we need to verify you.\n👇 <b>Tap the button below:</b>",
            parse_mode='HTML', reply_markup=contact_kb())
    else:
        await log_activity(message.from_user.id, "CMD_START")
        await send_main_menu(message)

@dp.message(F.contact)
async def handle_contact(message: Message):
    if message.contact.user_id == message.from_user.id:
        await db_query("UPDATE users SET phone=%s WHERE user_id=%s", (message.contact.phone_number, message.from_user.id))
        referrer = await db_query("SELECT referred_by FROM users WHERE user_id=%s", (message.from_user.id,), fetchone=True)
        if referrer and referrer[0]:
            await db_query("UPDATE users SET referrals_count = referrals_count + 1 WHERE user_id=%s", (referrer[0],))
            try:
                await bot.send_message(referrer[0], f"🎉 <b>Referral Success!</b>\nUser <b>{message.from_user.first_name}</b> joined using your link!", parse_mode='HTML')
            except Exception:
                pass
        await log_activity(message.from_user.id, "CONTACT_VERIFIED")
        await message.answer("✅ Verification successful! Welcome to the system.", reply_markup=ReplyKeyboardRemove())
        await send_main_menu(message)
    else:
        await message.answer("❌ Security Alert: Please share your OWN contact using the provided button.")

async def send_main_menu(ctx: Any):
    text = await get_ui_text("start_menu")
    kb = await main_menu_kb(ctx.from_user.id)
    if isinstance(ctx, Message):
        await ctx.answer(text, reply_markup=kb, parse_mode='HTML')
    else:
        await ctx.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "back_main")
async def back_main(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await log_activity(call.from_user.id, "RETURN_MAIN_MENU")
    await send_main_menu(call)

# ==============================================================================
# 11. ADD BALANCE
# ==============================================================================
@dp.callback_query(F.data == "menu_add_balance")
async def select_gateway_menu(call: CallbackQuery):
    await log_activity(call.from_user.id, "VIEW_ADD_BALANCE")
    text = await get_ui_text("add_balance_menu")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="UPI PAY", callback_data="gateway_inr",
                              icon_custom_emoji_id=await get_emoji_icon("upi"), style="primary"),
         InlineKeyboardButton(text="BINANCE PAY", callback_data="gateway_crypto",
                              icon_custom_emoji_id=await get_emoji_icon("binance"), style="primary")],
        [InlineKeyboardButton(text="BACK", callback_data="back_main",
                              icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

# ==============================================================================
# 12. UPI (Pay0) & CRYPTO PAYMENT FLOWS
# ==============================================================================
@dp.callback_query(F.data == "gateway_inr")
async def add_balance_inr(call: CallbackQuery):
    text = f"💵 <b>— PAY0 UPI DEPOSIT —</b> 💵\n\nSelect amount to deposit:"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="₹50", callback_data="pay_50",
                              icon_custom_emoji_id=await get_emoji_icon("money_icon"), style="primary"),
         InlineKeyboardButton(text="₹100", callback_data="pay_100",
                              icon_custom_emoji_id=await get_emoji_icon("money_icon"), style="primary")],
        [InlineKeyboardButton(text="₹200", callback_data="pay_200",
                              icon_custom_emoji_id=await get_emoji_icon("money_icon"), style="primary"),
         InlineKeyboardButton(text="₹500", callback_data="pay_500",
                              icon_custom_emoji_id=await get_emoji_icon("money_icon"), style="primary")],
        [InlineKeyboardButton(text="✏️ Custom Amount", callback_data="custom_deposit_keypad",
                              icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="primary")],
        [InlineKeyboardButton(text="Back", callback_data="menu_add_balance",
                              icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "custom_deposit_keypad")
async def show_custom_keypad(call: CallbackQuery, state: FSMContext):
    await state.set_state(UserStates.custom_amount_input)
    await state.update_data(amount_str="0")
    await show_keypad(call.message)

async def show_keypad(message: Message, amount_str: str = "0"):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="      1      ", callback_data="kp_1", style="primary"),
         InlineKeyboardButton(text="      2      ", callback_data="kp_2", style="primary"),
         InlineKeyboardButton(text="      3      ", callback_data="kp_3", style="primary")],
        [InlineKeyboardButton(text="      4      ", callback_data="kp_4", style="primary"),
         InlineKeyboardButton(text="      5      ", callback_data="kp_5", style="primary"),
         InlineKeyboardButton(text="      6      ", callback_data="kp_6", style="primary")],
        [InlineKeyboardButton(text="      7      ", callback_data="kp_7", style="primary"),
         InlineKeyboardButton(text="      8      ", callback_data="kp_8", style="primary"),
         InlineKeyboardButton(text="      9      ", callback_data="kp_9", style="primary")],
        [InlineKeyboardButton(text="    ⌫    ", callback_data="kp_backspace", style="danger"),
         InlineKeyboardButton(text="      0      ", callback_data="kp_0", style="primary"),
         InlineKeyboardButton(text="    C    ", callback_data="kp_clear", style="danger")],
        [InlineKeyboardButton(text=f"✅ Confirm (₹{amount_str})", callback_data="kp_confirm",
                              icon_custom_emoji_id=await get_emoji_icon("check_icon"), style="success")],
        [InlineKeyboardButton(text="Cancel", callback_data="gateway_inr",
                              icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]
    ])
    text = f"💵 <b>Enter Amount (₹):</b>\n\nCurrent: ₹{amount_str}"
    await message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("kp_"), UserStates.custom_amount_input)
async def keypad_handler(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    amount_str = data.get("amount_str", "0")
    action = call.data.split("_")[1]
    if action == "confirm":
        if amount_str == "0":
            await call.answer("Amount cannot be zero.", show_alert=True)
            return
        try:
            amount = float(amount_str)
            if amount < 10:
                await call.answer("Minimum deposit is ₹10.", show_alert=True)
                return
            await state.clear()
            await call.message.edit_text("⏳ <b>Generating Secure Link...</b>", parse_mode='HTML')
            await generate_pay0_order(call.from_user.id, amount, call.message)
        except ValueError:
            await call.answer("Invalid amount.", show_alert=True)
        return
    if action == "backspace":
        amount_str = amount_str[:-1] if len(amount_str) > 1 else "0"
    elif action == "clear":
        amount_str = "0"
    else:
        amount_str = action if amount_str == "0" else amount_str + action
        if len(amount_str) > 6:
            amount_str = amount_str[:6]
    await state.update_data(amount_str=amount_str)
    await show_keypad(call.message, amount_str)
    await call.answer()

@dp.callback_query(F.data.startswith("pay_"))
async def process_pay0_payment_callback(call: CallbackQuery):
    inr_amount = float(call.data.split("_")[1])
    await call.message.edit_text("⏳ <b>Generating Secure Link via Pay0...</b>", parse_mode='HTML')
    await generate_pay0_order(call.from_user.id, inr_amount, call.message)

async def generate_pay0_order(user_id: int, inr_amount: float, message_obj: Message) -> None:
    api_key_check = await db_query("SELECT value FROM settings WHERE `key`='pay0_api'", fetchone=True)
    if not api_key_check or not api_key_check[0]:
        return await message_obj.edit_text("⚠️ Pay0 Gateway is currently offline. Admin needs to set API Token.",
                                           reply_markup=await back_kb("gateway_inr"), parse_mode='HTML')

    api_token = api_key_check[0]
    current_time = int(time.time())
    order_id = f"NXT{user_id}{current_time}"

    user_row = await db_query("SELECT phone, first_name FROM users WHERE user_id=%s", (user_id,), fetchone=True)
    mobile = user_row[0] if user_row and user_row[0] else "9999999999"
    customer_name = user_row[1] if user_row and user_row[1] else "Customer"

    bot_deep_link = f"https://t.me/{BOT_USERNAME}?start=v_{order_id}"

    await db_query(
        "INSERT INTO transactions (order_id, user_id, amount_inr, status, timestamp, purpose, quantity, payment_method) "
        "VALUES (%s, %s, %s, 'pending', %s, 'wallet', 1, 'upi')",
        (order_id, user_id, inr_amount, current_time))

    payment_url = ""
    async with aiohttp.ClientSession() as session:
        try:
            url = "https://pay0.shop/api/create-order"
            payload = {
                "customer_mobile": mobile,
                "customer_name": customer_name,
                "user_token": api_token,
                "amount": str(inr_amount),
                "order_id": order_id,
                "redirect_url": bot_deep_link,
                "remark1": "Wallet Topup",
                "remark2": "Bot Payment"
            }
            async with session.post(url, data=payload,
                                    headers={"Content-Type": "application/x-www-form-urlencoded"}) as resp:
                if resp.status == 200:
                    try:
                        res_data = await resp.json(content_type=None)
                    except Exception:
                        res_data = {}
                    status_val = res_data.get("status")
                    is_success = (status_val is True) or (str(status_val).lower() in ("true", "success", "1"))
                    if is_success:
                        result_obj = res_data.get("result") or {}
                        payment_url = (res_data.get("payment_url")
                                       or res_data.get("url")
                                       or res_data.get("redirect_url")
                                       or result_obj.get("payment_url")
                                       or result_obj.get("url")
                                       or result_obj.get("redirect_url")
                                       or result_obj.get("paymentUrl")
                                       or "")
                        if not payment_url:
                            return await message_obj.edit_text(
                                "❌ <b>Gateway Data Error:</b> Payment URL missing in API response.",
                                reply_markup=await back_kb("gateway_inr"), parse_mode='HTML')
                    else:
                        return await message_obj.edit_text(
                            f"❌ <b>Gateway Data Error:</b> {res_data.get('message', 'Unknown structure.')}",
                            reply_markup=await back_kb("gateway_inr"), parse_mode='HTML')
                else:
                    return await message_obj.edit_text(f"❌ <b>Gateway Server Error:</b> HTTP {resp.status}",
                                                       reply_markup=await back_kb("gateway_inr"), parse_mode='HTML')
        except Exception as e:
            return await message_obj.edit_text(f"❌ <b>API Connection Error:</b> {str(e)}",
                                               reply_markup=await back_kb("gateway_inr"), parse_mode='HTML')

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Pay Now (Auto Redirects)", url=payment_url, style="success")],
        [InlineKeyboardButton(text="🔄 Manual Verify", callback_data=f"verify_{order_id}",
                              icon_custom_emoji_id=await get_emoji_icon("check_icon"), style="primary")],
        [InlineKeyboardButton(text="Cancel Transaction", callback_data="menu_add_balance",
                              icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]
    ])
    text = (f"🧾 <b>SECURE INVOICE CREATED</b>\n\nAmount: <b>₹{inr_amount:.2f}</b>\n"
            f"Order ID: <code>{order_id}</code>\n⏳ <b>Timer:</b> 15:00 Minutes\n\n"
            "1️⃣ Click <b>Pay Now</b> to open UPI Gateway.\n2️⃣ Complete the payment in your app.\n"
            "3️⃣ <b>Auto-Verify:</b> Return to the bot after payment! ✨")
    await log_activity(user_id, "GENERATE_INVOICE", f"Amount: {inr_amount}, Order ID: {order_id}")
    await message_obj.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("verify_"))
async def manual_verify_callback(call: CallbackQuery):
    order_id = call.data.split("_", 1)[1]
    await run_payment_verification(call.from_user.id, order_id, call)

@dp.callback_query(F.data == "gateway_crypto")
async def add_balance_crypto(call: CallbackQuery, state: FSMContext):
    address_check = await db_query("SELECT value FROM settings WHERE `key`='binance_address'", fetchone=True)
    if not address_check or not address_check[0]:
        return await call.message.edit_text("⚠️ Binance Gateway is currently offline. Admin has not set a deposit address.",
                                            reply_markup=await back_kb("menu_add_balance"), parse_mode='HTML')
    deposit_address = address_check[0]
    msg = (f"🪙 <b>— BINANCE USDT DEPOSIT —</b> 🪙\n\n💵 <b>Exchange Rate:</b> 1 USDT = ₹{USDT_TO_INR}\n"
           f"⚠️ <b>Network:</b> Please send via <b>TRC20</b> or <b>BEP20</b>.\n\n"
           f"👇 <b>Send your USDT to this exact address:</b>\n<code>{deposit_address}</code>\n\n"
           f"━━━━━━━━━━━━━━━━━━\n✅ <b>After sending the USDT, reply to this message with your exact TxID "
           f"(Transaction Hash) to instantly claim your balance.</b>")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
        text="Cancel", callback_data="menu_add_balance",
        icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]])
    await call.message.edit_text(msg, reply_markup=kb, parse_mode='HTML')
    await state.set_state(UserStates.wait_for_crypto_txid)

@dp.message(UserStates.wait_for_crypto_txid)
async def process_crypto_txid(m: Message, state: FSMContext):
    txid = m.text.strip()
    user_id = m.from_user.id
    if len(txid) < 10:
        return await m.answer("❌ That doesn't look like a valid TxID. Please try again.")
    existing = await db_query("SELECT txid FROM crypto_txns WHERE txid=%s", (txid,), fetchone=True)
    if existing:
        return await m.answer("⚠️ This Transaction ID has already been claimed in the system!",
                              reply_markup=await back_kb("menu_add_balance"), parse_mode='HTML')

    api_key_check = await db_query("SELECT value FROM settings WHERE `key`='binance_api'", fetchone=True)
    secret_key_check = await db_query("SELECT value FROM settings WHERE `key`='binance_secret'", fetchone=True)
    if not api_key_check or not secret_key_check:
        return await m.answer("⚠️ Binance API is missing on the server. Contact Support.",
                              reply_markup=await back_kb("menu_add_balance"), parse_mode='HTML')

    await m.answer("🔄 <b>Verifying your TxID with Binance Blockchain...</b>\n<i>This may take up to 30 seconds...</i>", parse_mode='HTML')
    api_key = api_key_check[0]
    secret_key = secret_key_check[0]
    timestamp = int(time.time() * 1000)
    query_string = f"timestamp={timestamp}"
    signature = hmac.new(secret_key.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
    headers = {'X-MBX-APIKEY': api_key}
    url = f"https://api.binance.com/sapi/v1/capital/deposit/hisrec?{query_string}&signature={signature}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    try:
                        history = await resp.json(content_type=None)
                    except Exception:
                        history = []
                    found = False
                    for deposit in history:
                        if deposit.get("txId") == txid and deposit.get("status") == 1:
                            found = True
                            usdt_amount = float(deposit.get("amount"))
                            inr_amount = usdt_amount * USDT_TO_INR
                            await db_query("INSERT INTO crypto_txns (txid, user_id, amount_usdt, timestamp) VALUES (%s, %s, %s, %s)",
                                           (txid, user_id, usdt_amount, int(time.time())))
                            await db_query("UPDATE users SET balance = balance + %s WHERE user_id=%s", (inr_amount, user_id))
                            await m.answer(f"🎉 <b>CRYPTO DEPOSIT SUCCESSFUL!</b>\n\n✅ We safely received <b>{usdt_amount} USDT</b>.\n"
                                           f"💰 <b>{fmt_curr(inr_amount)}</b> has been added to your balance!",
                                           reply_markup=await main_menu_kb(m.from_user.id), parse_mode='HTML')
                            await send_advanced_notification(user_id, "DEPOSIT", inr_amount, product=txid, gateway="Binance Crypto")
                            await log_activity(user_id, "CRYPTO_DEPOSIT", f"TxID: {txid}, Amount: {inr_amount}")
                            await state.clear()
                            break
                    if not found:
                        await m.answer("❌ <b>TxID Not Found or Still Pending!</b>\nMake sure the transaction is fully confirmed. Try again in 5 mins.",
                                       reply_markup=await back_kb("menu_add_balance"), parse_mode='HTML')
                else:
                    await m.answer(f"⚠️ <b>Binance Server Error:</b> HTTP {resp.status}.",
                                   reply_markup=await back_kb("menu_add_balance"), parse_mode='HTML')
        except Exception as e:
            await m.answer(f"⚠️ <b>Connection Error:</b> {str(e)}",
                           reply_markup=await back_kb("menu_add_balance"), parse_mode='HTML')

# ==============================================================================
# 13. SHOP
# ==============================================================================
@dp.callback_query(F.data == "menu_shop")
async def view_shop_panels(call: CallbackQuery):
    await log_activity(call.from_user.id, "VIEW_SHOP")
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    text = (f"{await get_emoji('product_store')} <b><u>SELECT PRODUCT PANEL</u></b>\n"
            f"━━━━━━━━━━━━━━━━━━\n\n{await get_emoji('point_down')} <b>Choose a panel to view its packages:</b>")
    for cat in FIXED_CATEGORIES:
        emoji_id = await get_category_emoji(cat)
        kb.inline_keyboard.append([InlineKeyboardButton(
            text=cat, callback_data=f"cat_{cat[:30]}", icon_custom_emoji_id=emoji_id, style="danger")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="BACK", callback_data="back_main",
                                                    icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("cat_"))
async def view_panel_names(call: CallbackQuery):
    category = call.data.split("cat_", 1)[1]
    panel_names = await db_query(
        "SELECT DISTINCT panel_name FROM products WHERE category LIKE %s AND is_active=1 AND panel_name != ''",
        (category + '%',), fetchall=True)
    panel_names = sorted(panel_names or [], key=lambda row: natural_sort_key(row[0]))
    if not panel_names:
        prods = await db_query(
            "SELECT id, name, price_inr, stock, reseller_price, validity, device_limit, external_enabled "
            "FROM products WHERE category LIKE %s AND is_active=1",
            (category + '%',), fetchall=True)
        if not prods:
            return await call.answer("❌ No products available in this category yet.", show_alert=True)
        await show_products_for_panel(call, prods, category)
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    text = (f"{await get_emoji('product_store')} <b><u>{category.upper()} PANELS</u></b>\n"
            f"━━━━━━━━━━━━━━━━━━\n\n{await get_emoji('point_down')} <b>Choose a panel name:</b>")
    for pn in panel_names:
        panel = pn[0]
        emoji_id = (await get_panel_emoji(panel)) or (await get_emoji_icon("product_store"))
        kb.inline_keyboard.append([InlineKeyboardButton(
            text=panel, callback_data=f"pnl_{category[:30]}_{panel[:30]}", icon_custom_emoji_id=emoji_id, style="danger")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="BACK TO PANELS", callback_data="menu_shop",
                                                    icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("pnl_"))
async def view_products_for_panel(call: CallbackQuery):
    parts = call.data.split("pnl_", 1)[1].split("_", 1)
    if len(parts) != 2:
        return await call.answer("Invalid selection.", show_alert=True)
    category, panel_name = parts[0], parts[1]
    prods = await db_query(
        "SELECT id, name, price_inr, stock, reseller_price, validity, device_limit, external_enabled "
        "FROM products WHERE category LIKE %s AND panel_name LIKE %s AND is_active=1",
        (category + '%', panel_name + '%'), fetchall=True)
    if not prods:
        return await call.answer("No products found for this panel.", show_alert=True)
    await show_products_for_panel(call, prods, f"{category} - {panel_name}")

async def show_products_for_panel(call: CallbackQuery, prods: List[Tuple], header: str):
    user = await db_query("SELECT is_reseller, is_vip FROM users WHERE user_id=%s", (call.from_user.id,), fetchone=True)
    is_reseller = bool(user[0]) if user else False
    is_vip = bool(user[1]) if user else False
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    text = f"{await get_emoji('product_store')} <b><u>{header.upper()} PACKAGES</u></b>\n━━━━━━━━━━━━━━━━━━\n\n"
    prods = sorted(prods or [], key=lambda row: natural_sort_key(row[1]))
    for p in prods:
        prod_id, package_name, normal_price, stock, reseller_price, validity, device, external_enabled = p
        normal_price = float(normal_price) if normal_price is not None else 0.0
        reseller_price = float(reseller_price) if reseller_price is not None else 0.0
        base_price = reseller_price if is_reseller else normal_price
        display_price = base_price - (base_price * (VIP_DISCOUNT_PERCENTAGE / 100)) if is_vip else base_price
        stock_status = "♾️ API Available" if external_enabled else ("✅ In Stock" if stock > 0 else "❌ Out of Stock")
        text += f"{await get_emoji('product_store')} ⏱ <b>Validity: {package_name}</b>\n"
        if is_reseller or is_vip:
            text += f"💰 Regular Price: <s>{fmt_curr(normal_price)}</s>\n"
            if is_reseller and not is_vip:
                text += f"👑 <b>Reseller Price: {fmt_curr(display_price)}</b>\n"
            elif is_vip and not is_reseller:
                text += f"🌟 <b>VIP Price: {fmt_curr(display_price)}</b>\n"
            else:
                text += f"👑🌟 <b>Super Price: {fmt_curr(display_price)}</b>\n"
        else:
            text += f"💰 Price: {fmt_curr(normal_price)}\n"
        text += f"📱 Limit: {device} | 📦 {stock_status}\n\n"
        if external_enabled or stock > 0:
            kb.inline_keyboard.append([InlineKeyboardButton(
                text=f"Buy {package_name} - {fmt_curr(display_price)}",
                callback_data=f"buy_{prod_id}",
                icon_custom_emoji_id=await get_emoji_icon("product_store"), style="danger")])
        else:
            kb.inline_keyboard.append([InlineKeyboardButton(
                text=f"❌ {package_name} (Out of Stock)",
                callback_data="ignore_stock_click", style="danger")])
    text += f"{await get_emoji('point_down')} <b>Select package below to instantly purchase:</b>"
    kb.inline_keyboard.append([InlineKeyboardButton(text="BACK TO PANELS", callback_data="menu_shop",
                                                    icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "ignore_stock_click")
async def ignore_stock_click(call: CallbackQuery):
    await call.answer("⚠️ This duration is completely Out of Stock! Admins have been notified to refill.", show_alert=True)

@dp.callback_query(F.data.startswith("buy_"))
async def process_buy(call: CallbackQuery):
    try:
        prod_id = int(call.data.split("_", 1)[1])
    except (ValueError, IndexError):
        return await call.answer("❌ Invalid product.", show_alert=True)

    prod = await db_query("SELECT name, price_inr, stock, validity, external_enabled FROM products WHERE id=%s AND is_active=1",
                          (prod_id,), fetchone=True)
    if not prod:
        return await call.answer("❌ Product not found.", show_alert=True)

    user = await db_query("SELECT balance, is_reseller, is_vip FROM users WHERE user_id=%s", (call.from_user.id,), fetchone=True)
    if not user:
        return await call.answer("❌ User account not found!", show_alert=True)

    normal_price = float(prod[1] or 0)
    reseller_row = await db_query("SELECT reseller_price FROM products WHERE id=%s", (prod_id,), fetchone=True)
    reseller_price = float(reseller_row[0] or 0) if reseller_row else 0
    base_price = reseller_price if bool(user[1]) else normal_price
    unit_price = base_price - (base_price * VIP_DISCOUNT_PERCENTAGE / 100) if bool(user[2]) else base_price

    external = bool(prod[4])
    local_stock = int(prod[2] or 0)
    max_qty = 5 if external else min(5, local_stock)
    if max_qty < 1:
        return await call.answer("❌ This product is out of stock.", show_alert=True)

    kb = InlineKeyboardMarkup(inline_keyboard=[])
    row = []
    for qty in range(1, max_qty + 1):
        row.append(InlineKeyboardButton(
            text=f"{qty} Key{'s' if qty != 1 else ''} — {fmt_curr(unit_price * qty)}",
            callback_data=f"qty_{prod_id}_{qty}", style="primary"))
        if len(row) == 2:
            kb.inline_keyboard.append(row)
            row = []
    if row:
        kb.inline_keyboard.append(row)
    kb.inline_keyboard.append([InlineKeyboardButton(text="BACK", callback_data="menu_shop",
                                                    icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")])
    text = (f"🛒 <b>SELECT QUANTITY</b>\n━━━━━━━━━━━━━━━━━━\n\n"
            f"📦 <b>{html.escape(prod[0])}</b>\n"
            f"⏱ <b>Validity:</b> {html.escape(str(prod[3] or ''))}\n"
            f"💰 <b>Price per key:</b> {fmt_curr(unit_price)}\n\n"
            "👇 Choose how many keys you want:")
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')


async def get_purchase_pricing(user_id: int, prod_id: int):
    row = await db_query("""
        SELECT p.name, p.price_inr, p.reseller_price, p.stock, p.validity, p.device_limit,
               p.category, p.panel_name, p.external_enabled, p.external_product_id, p.external_duration, p.apk_link
        FROM products p WHERE p.id=%s AND p.is_active=1
    """, (prod_id,), fetchone=True)
    user = await db_query("SELECT balance, referred_by, is_reseller, total_saved, is_vip FROM users WHERE user_id=%s",
                          (user_id,), fetchone=True)
    if not row or not user:
        return None
    normal = float(row[1] or 0)
    reseller = float(row[2] or 0)
    base = reseller if bool(user[2]) else normal
    unit = base - (base * VIP_DISCOUNT_PERCENTAGE / 100) if bool(user[4]) else base
    return row, user, unit


async def acquire_purchase_lock(user_id: int, product_id: int, quantity: int, ttl: int = 1200) -> bool:
    now = int(time.time())
    await db_query("DELETE FROM purchase_locks WHERE expires_at < %s", (now,))
    existing = await db_query("SELECT user_id FROM purchase_locks WHERE user_id=%s", (user_id,), fetchone=True)
    if existing:
        return False
    try:
        await db_query(
            "INSERT INTO purchase_locks(user_id, product_id, quantity, created_at, expires_at) VALUES(%s,%s,%s,%s,%s)",
            (user_id, product_id, quantity, now, now + ttl))
        return True
    except Exception:
        logger.exception("Could not acquire purchase lock")
        return False


async def release_purchase_lock(user_id: int) -> None:
    await db_query("DELETE FROM purchase_locks WHERE user_id=%s", (user_id,))


async def _api_key_with_timer(product_id: str, duration: str, message: Message, index: int, quantity: int):
    task = asyncio.create_task(fetch_external_key(product_id, duration, ""))
    started = time.monotonic()
    while not task.done():
        elapsed = int(time.monotonic() - started)
        mm, ss = divmod(elapsed, 60)
        try:
            await message.edit_text(
                f"⏳ <b>WAIT FOR KEY</b>\n\nGenerating key <b>{index}/{quantity}</b>\n"
                f"🕐 Time: <b>{mm:02d}:{ss:02d}</b>\n"
                "🔒 Please do not click again — this order is locked.", parse_mode='HTML')
        except Exception:
            pass
        try:
            return await asyncio.wait_for(asyncio.shield(task), timeout=2.0)
        except asyncio.TimeoutError:
            continue
    return await task


async def generate_product_keys(prod_id: int, quantity: int, external_enabled: bool,
                                external_product_id: str, api_duration: str, update_message: Message):
    keys = []
    for index in range(1, quantity + 1):
        await update_message.edit_text(
            f"⏳ <b>WAIT FOR KEY</b>\n\nGenerating key <b>{index}/{quantity}</b>...\n"
            "🔒 Duplicate clicks are disabled until this process finishes.", parse_mode='HTML')
        if external_enabled:
            response = await _api_key_with_timer(external_product_id, api_duration, update_message, index, quantity)
            if response.get("status") != "success":
                return (keys if keys else None), response.get("msg", "Unknown API error")
            key = response.get("key")
            if isinstance(key, list):
                key = "\n".join(str(x) for x in key)
            if key is None or str(key).strip() in ("", "KEY_NOT_FOUND"):
                return None, "API returned no key"
            keys.append(str(key))
        else:
            key_data = await db_query(
                "SELECT id, key_text FROM product_keys WHERE product_id=%s AND is_used=0 LIMIT 1",
                (prod_id,), fetchone=True)
            if not key_data:
                return (keys if keys else None), "No manual key is available"
            keys.append(str(key_data[1]))
            await db_query("UPDATE product_keys SET is_used=1 WHERE id=%s AND is_used=0", (key_data[0],))
            await db_query("UPDATE products SET stock=CASE WHEN stock>0 THEN stock-1 ELSE 0 END WHERE id=%s", (prod_id,))
        if index < quantity:
            await asyncio.sleep(1)
    return keys, None


async def fulfill_product_transaction(order_id: str, user_id: int, reply_message: Message = None) -> bool:
    txn = await db_query(
        "SELECT amount_inr, status, timestamp, product_id, quantity, payment_method FROM transactions WHERE order_id=%s AND user_id=%s",
        (order_id, user_id), fetchone=True)
    if not txn:
        return False
    amount, status, ts, prod_id, quantity, payment_method = txn
    if status == 'completed':
        return True
    if status != 'paid':
        return False

    claimed = await db_update_count(
        "UPDATE transactions SET status='processing' WHERE order_id=%s AND user_id=%s AND status='paid'",
        (order_id, user_id))
    check = await db_query("SELECT status FROM transactions WHERE order_id=%s", (order_id,), fetchone=True)
    if not claimed and (not check or check[0] != 'processing'):
        return False
    if check and check[0] == 'processing' and not claimed:
        return False

    pricing = await get_purchase_pricing(user_id, int(prod_id))
    if not pricing:
        await db_query("UPDATE transactions SET status='failed' WHERE order_id=%s", (order_id,))
        await release_purchase_lock(user_id)
        return False
    prod, user, unit_price = pricing
    expected = unit_price * int(quantity or 1)
    if abs(float(amount) - expected) > 0.01:
        await db_query("UPDATE transactions SET status='failed' WHERE order_id=%s", (order_id,))
        await release_purchase_lock(user_id)
        return False

    external_enabled = bool(prod[8])
    external_product_id = str(prod[9] or '').strip()
    duration_candidates = []
    for candidate in (prod[10], prod[4], prod[0]):
        candidate = normalize_api_duration(str(candidate or ''))
        if candidate and candidate not in duration_candidates:
            duration_candidates.append(candidate)
    if external_enabled and not external_product_id:
        await db_query("UPDATE users SET balance=balance+%s WHERE user_id=%s", (float(amount), user_id))
        await db_query("UPDATE transactions SET status='failed' WHERE order_id=%s", (order_id,))
        await release_purchase_lock(user_id)
        return False

    if reply_message is None:
        reply_message = await bot.send_message(user_id, "⏳ <b>Preparing your keys...</b>", parse_mode='HTML')

    keys = []
    error = None
    if external_enabled:
        for candidate_duration in duration_candidates:
            one_keys, one_error = await generate_product_keys(
                int(prod_id), int(quantity), True, external_product_id, candidate_duration, reply_message)
            if one_keys:
                keys, error = one_keys, one_error
                break
            error = one_error
            if 'price not found' not in str(one_error).lower() and 'price_not_found' not in str(one_error).lower():
                break
    else:
        keys, error = await generate_product_keys(
            int(prod_id), int(quantity), False, external_product_id, '', reply_message)

    if not keys:
        await db_query("UPDATE users SET balance=balance+%s WHERE user_id=%s", (float(amount), user_id))
        await db_query("UPDATE transactions SET status='failed' WHERE order_id=%s", (order_id,))
        await release_purchase_lock(user_id)
        try:
            await reply_message.edit_text(
                f"❌ <b>Key generation failed:</b> {html.escape(error or 'Unknown error')}\n\n"
                "💰 The paid amount has been credited to your wallet.",
                reply_markup=await back_kb("menu_shop"), parse_mode='HTML')
        except Exception:
            pass
        return False

    generated_qty = len(keys)
    delivered_key = "\n".join(f"{i+1}. {k}" for i, k in enumerate(keys))
    actual_amount = unit_price * generated_qty
    remainder = max(0.0, float(amount) - actual_amount)
    if remainder > 0.01:
        await db_query("UPDATE users SET balance=balance+%s WHERE user_id=%s", (remainder, user_id))
    savings = float(prod[1] or 0) * generated_qty - actual_amount
    await db_query("UPDATE users SET spent=spent+%s, orders_count=orders_count+1, total_saved=total_saved+%s WHERE user_id=%s",
                   (actual_amount, savings, user_id))
    if user[1]:
        commission = actual_amount * 0.15
        await db_query("UPDATE users SET balance=balance+%s, referral_earned=referral_earned+%s WHERE user_id=%s",
                       (commission, commission, user[1]))
        try:
            await bot.send_message(user[1], f"🎁 <b>Referral Bonus Added!</b>\nYou earned {fmt_curr(commission)} from a successful purchase.", parse_mode='HTML')
        except Exception:
            pass

    product_full_name = f"{prod[6]} - {prod[7]} ({prod[0]}) x{quantity}"
    await db_query("INSERT INTO orders (user_id, product_name, price_paid, delivered_key, purchase_date) VALUES (%s, %s, %s, %s, %s)",
                   (user_id, product_full_name, actual_amount, delivered_key,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    await db_query("UPDATE transactions SET status='completed' WHERE order_id=%s AND status='processing'", (order_id,))
    await release_purchase_lock(user_id)
    await log_activity(user_id, "PURCHASE_SUCCESS",
                       f"Product: {product_full_name}, Paid: {actual_amount}, Keys: {generated_qty}, Payment: {payment_method}")
    await send_advanced_notification(user_id, "ORDER", actual_amount, product=product_full_name, key=delivered_key)

    msg = (f"✅ <b>PURCHASE SUCCESSFUL!</b>\n━━━━━━━━━━━━━━━━━━\n"
           f"📦 <b>Panel:</b> {html.escape(str(prod[6]))}\n📁 <b>Panel Name:</b> {html.escape(str(prod[7]))}\n"
           f"⏱ <b>Package:</b> {html.escape(str(prod[0]))}\n🔢 <b>Quantity:</b> {generated_qty}/{quantity}\n"
           f"💰 <b>Amount Used:</b> {fmt_curr(actual_amount)}\n📱 <b>Device Limit:</b> {html.escape(str(prod[5]))}\n"
           "━━━━━━━━━━━━━━━━━━\n")
    if prod[11] and str(prod[11]).startswith('http'):
        msg += f"📥 <b>APK Link:</b> <a href='{html.escape(str(prod[11]), quote=True)}'>Click Here to Download</a>\n\n"
    msg += f"🔑 <b>Your Keys:</b>\n<code>{html.escape(delivered_key)}</code>\n"
    if remainder > 0.01:
        msg += f"\n💰 <b>Unused amount credited to wallet:</b> {fmt_curr(remainder)}\n"
    try:
        await reply_message.edit_text(msg, reply_markup=await back_kb("menu_shop"),
                                      disable_web_page_preview=True, parse_mode='HTML')
    except Exception:
        await bot.send_message(user_id, msg, reply_markup=await back_kb("menu_shop"),
                               disable_web_page_preview=True, parse_mode='HTML')
    return True


@dp.callback_query(F.data.startswith("qty_"))
async def choose_quantity(call: CallbackQuery):
    try:
        _, prod_id_s, qty_s = call.data.split("_", 2)
        prod_id, quantity = int(prod_id_s), int(qty_s)
    except ValueError:
        return await call.answer("❌ Invalid quantity.", show_alert=True)
    if quantity < 1 or quantity > 5:
        return await call.answer("❌ Invalid quantity.", show_alert=True)

    pricing = await get_purchase_pricing(call.from_user.id, prod_id)
    if not pricing:
        return await call.answer("❌ Product or user not found.", show_alert=True)
    prod, user, unit_price = pricing
    if not bool(prod[8]) and int(prod[3] or 0) < quantity:
        return await call.answer("❌ Not enough keys in stock.", show_alert=True)
    total = unit_price * quantity
    balance = float(user[0] or 0)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💳 Wallet Balance — {fmt_curr(total)}",
                              callback_data=f"walletpay_{prod_id}_{quantity}", style="success")],
        [InlineKeyboardButton(text=f"🇮🇳 Pay Direct by UPI — {fmt_curr(total)}",
                              callback_data=f"upipay_{prod_id}_{quantity}", style="primary")],
        [InlineKeyboardButton(text=f"🪙 Pay Direct by Binance — {fmt_curr(total)}",
                              callback_data=f"binpay_{prod_id}_{quantity}", style="primary")],
        [InlineKeyboardButton(text="➕ Add Balance First", callback_data="menu_add_balance", style="primary")],
        [InlineKeyboardButton(text="BACK", callback_data=f"buy_{prod_id}",
                              icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]
    ])
    text = (f"🧾 <b>ORDER SUMMARY</b>\n━━━━━━━━━━━━━━━━━━\n\n"
            f"📦 <b>{html.escape(str(prod[0]))}</b>\n🔢 <b>Quantity:</b> {quantity}\n"
            f"💰 <b>Price per key:</b> {fmt_curr(unit_price)}\n💵 <b>Total:</b> {fmt_curr(total)}\n"
            f"👛 <b>Wallet Balance:</b> {fmt_curr(balance)}\n\n"
            "👇 Choose a payment method:")
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')


async def create_product_upi_order(user_id: int, prod_id: int, quantity: int, message_obj: Message):
    pricing = await get_purchase_pricing(user_id, prod_id)
    if not pricing:
        return await message_obj.edit_text("❌ Product not found.",
                                           reply_markup=await back_kb("menu_shop"), parse_mode='HTML')
    prod, user, unit_price = pricing
    total = unit_price * quantity
    if not await acquire_purchase_lock(user_id, prod_id, quantity):
        return await message_obj.edit_text("⏳ <b>An order is already processing.</b> Please finish or wait for the current order.",
                                           reply_markup=await back_kb("menu_shop"), parse_mode='HTML')

    api_key_check = await db_query("SELECT value FROM settings WHERE `key`='pay0_api'", fetchone=True)
    if not api_key_check or not api_key_check[0]:
        await release_purchase_lock(user_id)
        return await message_obj.edit_text("⚠️ UPI Gateway is currently offline.",
                                           reply_markup=await back_kb("menu_shop"), parse_mode='HTML')

    order_id = f"PRD{user_id}{int(time.time())}{random.randint(100,999)}"
    phone_row = await db_query("SELECT phone, first_name FROM users WHERE user_id=%s", (user_id,), fetchone=True)
    mobile = phone_row[0] if phone_row and phone_row[0] else "9999999999"
    customer_name = phone_row[1] if phone_row and phone_row[1] else "Customer"
    now = int(time.time())
    await db_query(
        "INSERT INTO transactions(order_id,user_id,amount_inr,status,timestamp,purpose,product_id,quantity,payment_method) "
        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (order_id, user_id, total, 'pending', now, 'product', prod_id, quantity, 'upi'))

    try:
        async with aiohttp.ClientSession() as session:
            payload = {
                "customer_mobile": mobile,
                "customer_name": customer_name,
                "user_token": api_key_check[0],
                "amount": str(total),
                "order_id": order_id,
                "redirect_url": f"https://t.me/{BOT_USERNAME}?start=v_{order_id}",
                "remark1": f"Product {prod_id} x{quantity}",
                "remark2": "Bot Purchase"
            }
            async with session.post("https://pay0.shop/api/create-order", data=payload,
                                    headers={"Content-Type": "application/x-www-form-urlencoded"}) as resp:
                data = await resp.json(content_type=None)
                status_val = data.get("status")
                is_success = (status_val is True) or (str(status_val).lower() in ("true", "success", "1"))
                if resp.status != 200 or not is_success:
                    raise RuntimeError(data.get('message', f'HTTP {resp.status}'))
                result_obj = data.get("result") or {}
                payment_url = (data.get("payment_url")
                               or data.get("url")
                               or data.get("redirect_url")
                               or result_obj.get("payment_url")
                               or result_obj.get("url")
                               or result_obj.get("redirect_url")
                               or result_obj.get("paymentUrl"))
                if not payment_url:
                    raise RuntimeError('Gateway did not return a payment URL')
    except Exception as exc:
        await db_query("UPDATE transactions SET status='failed' WHERE order_id=%s", (order_id,))
        await release_purchase_lock(user_id)
        return await message_obj.edit_text(f"❌ <b>UPI order failed:</b> {html.escape(str(exc))}",
                                           reply_markup=await back_kb("menu_shop"), parse_mode='HTML')

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 PAY NOW", url=payment_url, style="success")],
        [InlineKeyboardButton(text="🔄 VERIFY PAYMENT", callback_data=f"verify_{order_id}", style="primary")]
    ])
    await message_obj.edit_text(
        f"🧾 <b>PRODUCT PAYMENT</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        f"📦 {html.escape(str(prod[0]))}\n🔢 Quantity: <b>{quantity}</b>\n💰 Amount: <b>{fmt_curr(total)}</b>\n\n"
        "⏳ <b>Payment window: 15:00</b>\n"
        "1️⃣ Tap PAY NOW\n2️⃣ Complete payment\n3️⃣ Return here; payment will auto-verify\n\n"
        "🔒 Duplicate payment/key generation is blocked for this order.",
        reply_markup=kb, parse_mode='HTML')


@dp.callback_query(F.data.startswith("walletpay_"))
async def wallet_product_payment(call: CallbackQuery):
    try:
        _, prod_id_s, qty_s = call.data.split("_", 2)
        prod_id, quantity = int(prod_id_s), int(qty_s)
    except ValueError:
        return await call.answer("❌ Invalid order.", show_alert=True)
    pricing = await get_purchase_pricing(call.from_user.id, prod_id)
    if not pricing:
        return await call.answer("❌ Product not found.", show_alert=True)
    prod, user, unit_price = pricing
    total = unit_price * quantity
    if float(user[0] or 0) < total:
        return await call.answer(f"❌ Insufficient wallet balance. Need {fmt_curr(total)}.", show_alert=True)
    if not await acquire_purchase_lock(call.from_user.id, prod_id, quantity):
        return await call.answer("⏳ This order is already processing. Please wait for the key.", show_alert=True)

    order_id = f"WAL{call.from_user.id}{int(time.time())}{random.randint(100,999)}"
    deducted = await db_update_count(
        "UPDATE users SET balance=balance-%s WHERE user_id=%s AND balance>=%s",
        (total, call.from_user.id, total))
    if not deducted:
        await release_purchase_lock(call.from_user.id)
        return await call.answer("❌ Balance changed. Please try again.", show_alert=True)
    await db_query(
        "INSERT INTO transactions(order_id,user_id,amount_inr,status,timestamp,purpose,product_id,quantity,payment_method) "
        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (order_id, call.from_user.id, total, 'paid', int(time.time()), 'product', prod_id, quantity, 'wallet'))
    await call.answer("⏳ Payment received. Generating keys...", show_alert=False)
    await fulfill_product_transaction(order_id, call.from_user.id, call.message)


@dp.callback_query(F.data.startswith("upipay_"))
async def upi_product_payment(call: CallbackQuery):
    try:
        _, prod_id_s, qty_s = call.data.split("_", 2)
        prod_id, quantity = int(prod_id_s), int(qty_s)
    except ValueError:
        return await call.answer("❌ Invalid order.", show_alert=True)
    await call.answer("⏳ Creating UPI payment...", show_alert=False)
    await create_product_upi_order(call.from_user.id, prod_id, quantity, call.message)


@dp.callback_query(F.data.startswith("binpay_"))
async def binance_product_payment(call: CallbackQuery, state: FSMContext):
    try:
        _, prod_id_s, qty_s = call.data.split("_", 2)
        prod_id, quantity = int(prod_id_s), int(qty_s)
    except ValueError:
        return await call.answer("❌ Invalid order.", show_alert=True)
    pricing = await get_purchase_pricing(call.from_user.id, prod_id)
    if not pricing:
        return await call.answer("❌ Product not found.", show_alert=True)
    _, user, unit_price = pricing
    total = unit_price * quantity
    address = await get_setting('binance_address', '')
    if not address:
        return await call.answer("⚠️ Binance payment is not configured.", show_alert=True)
    if not await acquire_purchase_lock(call.from_user.id, prod_id, quantity):
        return await call.answer("⏳ This order is already processing.", show_alert=True)
    order_id = f"BPR{call.from_user.id}{int(time.time())}{random.randint(100,999)}"
    await db_query(
        "INSERT INTO transactions(order_id,user_id,amount_inr,status,timestamp,purpose,product_id,quantity,payment_method) "
        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (order_id, call.from_user.id, total, 'pending', int(time.time()), 'product', prod_id, quantity, 'binance'))
    await state.set_state(UserStates.wait_for_product_binance_txid)
    await state.update_data(product_order_id=order_id)
    await call.message.edit_text(
        f"🪙 <b>DIRECT BINANCE PAYMENT</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        f"Amount: <b>₹{total:.2f}</b>\nUSDT: <b>{total / USDT_TO_INR:.4f}</b>\n\n"
        f"Send USDT to:\n<code>{html.escape(address)}</code>\n\n"
        "After payment, send the exact TxID here. The bot will verify it before generating your keys.\n\n"
        "🔒 Only one active order is allowed at a time to prevent duplicate keys.", parse_mode='HTML')


@dp.message(UserStates.wait_for_product_binance_txid)
async def process_product_binance_txid(m: Message, state: FSMContext):
    data = await state.get_data()
    order_id = data.get('product_order_id')
    txid = (m.text or '').strip()
    if not order_id or len(txid) < 10:
        return await m.answer("❌ Please send a valid Binance TxID.")
    txn = await db_query(
        "SELECT amount_inr,status,user_id FROM transactions WHERE order_id=%s AND purpose='product'",
        (order_id,), fetchone=True)
    if not txn or txn[2] != m.from_user.id:
        return await m.answer("❌ Product payment order not found.")
    if txn[1] != 'pending':
        return await m.answer("⏳ This payment order is no longer pending.")
    existing = await db_query("SELECT txid FROM crypto_txns WHERE txid=%s", (txid,), fetchone=True)
    if existing:
        return await m.answer("⚠️ This TxID has already been used.")
    api_key_check = await db_query("SELECT value FROM settings WHERE `key`='binance_api'", fetchone=True)
    secret_key_check = await db_query("SELECT value FROM settings WHERE `key`='binance_secret'", fetchone=True)
    if not api_key_check or not secret_key_check:
        await release_purchase_lock(m.from_user.id)
        return await m.answer("⚠️ Binance verification is not configured.")
    await m.answer("🔄 <b>Verifying payment...</b> Please wait.", parse_mode='HTML')
    timestamp = int(time.time() * 1000)
    query_string = f"timestamp={timestamp}"
    signature = hmac.new(secret_key_check[0].encode(), query_string.encode(), hashlib.sha256).hexdigest()
    headers = {'X-MBX-APIKEY': api_key_check[0]}
    url = f"https://api.binance.com/sapi/v1/capital/deposit/hisrec?{query_string}&signature={signature}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                history = await resp.json(content_type=None) if resp.status == 200 else []
        expected_usdt = float(txn[0]) / USDT_TO_INR
        found = next((d for d in history
                      if d.get('txId') == txid and d.get('status') == 1
                      and abs(float(d.get('amount', 0)) - expected_usdt) < 0.0001), None)
        if not found:
            return await m.answer("❌ TxID not found, not confirmed, or amount does not match.")
        claimed = await db_update_count(
            "UPDATE transactions SET status='paid' WHERE order_id=%s AND status='pending'", (order_id,))
        if not claimed:
            return await m.answer("⏳ This order is already being processed.")
        await db_query("INSERT INTO crypto_txns(txid,user_id,amount_usdt,timestamp) VALUES(%s,%s,%s,%s)",
                       (txid, m.from_user.id, float(found.get('amount')), int(time.time())))
        await state.clear()
        await fulfill_product_transaction(order_id, m.from_user.id, m)
    except Exception as exc:
        await m.answer(f"⚠️ Binance verification error: {html.escape(str(exc))}")


@dp.callback_query(F.data.startswith("cancel_product_"))
async def cancel_product_order(call: CallbackQuery, state: FSMContext):
    order_id = call.data.split("_", 2)[2]
    txn = await db_query("SELECT status,user_id FROM transactions WHERE order_id=%s AND purpose='product'",
                         (order_id,), fetchone=True)
    if not txn or txn[1] != call.from_user.id:
        return await call.answer("❌ Order not found.", show_alert=True)
    if txn[0] == 'pending':
        await db_query("UPDATE transactions SET status='cancelled' WHERE order_id=%s AND status='pending'", (order_id,))
        await release_purchase_lock(call.from_user.id)
        return await call.message.edit_text("❌ <b>Product order cancelled.</b>",
                                            reply_markup=await back_kb("menu_shop"), parse_mode='HTML')
    return await call.answer("⏳ This order can no longer be cancelled.", show_alert=True)

# ==============================================================================
# 14. USER DASHBOARD, FILES, VIP, RESELLER, ORDERS, PROFILE, REFERRAL
# ==============================================================================
@dp.callback_query(F.data == "menu_all_files")
async def all_files_handler(call: CallbackQuery):
    link_q = await db_query("SELECT value FROM settings WHERE `key`='all_files_link'", fetchone=True)
    link = link_q[0] if link_q and link_q[0] != 'None' else None
    if link:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Access Download Channel ↗️", url=link,
                                  icon_custom_emoji_id=await get_emoji_icon("download"), style="success")],
            [InlineKeyboardButton(text="BACK", callback_data="back_main",
                                  icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]
        ])
        text = await get_ui_text("download_files")
        await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')
    else:
        await call.answer("⚠️ Admin has not configured the private download channel link yet.", show_alert=True)

@dp.callback_query(F.data == "menu_vip_dash")
async def vip_dashboard(call: CallbackQuery):
    u = await db_query("SELECT balance, is_vip, vip_since FROM users WHERE user_id=%s", (call.from_user.id,), fetchone=True)
    is_vip = bool(u[1])
    status_str = "🟢 Active (Lifetime)" if is_vip else "🔴 Not Subscribed"
    text = await get_ui_text("vip_menu", vip_status=status_str)
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    if is_vip:
        text += f"\n📅 <b>Member Since:</b> {u[2]}\n\nEnjoy your permanent 15% discount!"
    else:
        text += f"\n\n💳 <b>Your Current Balance:</b> {fmt_curr(u[0])}\n"
        if u[0] >= VIP_PRICE_INR:
            kb.inline_keyboard.append([InlineKeyboardButton(
                text=f"✅ Purchase VIP for {fmt_curr(VIP_PRICE_INR)}",
                callback_data="execute_vip_upgrade",
                icon_custom_emoji_id=await get_emoji_icon("vip"), style="success")])
        else:
            kb.inline_keyboard.append([InlineKeyboardButton(
                text=f"❌ Need {fmt_curr(VIP_PRICE_INR)} to Upgrade",
                callback_data="ignore_stock_click", style="danger")])
            kb.inline_keyboard.append([InlineKeyboardButton(
                text="💳 Add Balance Now", callback_data="menu_add_balance",
                icon_custom_emoji_id=await get_emoji_icon("add_balance"), style="success")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="BACK", callback_data="back_main",
                                                    icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "execute_vip_upgrade")
async def execute_vip_upgrade(call: CallbackQuery):
    u = await db_query("SELECT balance, is_vip FROM users WHERE user_id=%s", (call.from_user.id,), fetchone=True)
    if u[1]:
        return await call.answer("⚠️ You are already a VIP Member!", show_alert=True)
    if u[0] < VIP_PRICE_INR:
        return await call.answer(f"❌ Your balance dropped below {VIP_PRICE_INR}.", show_alert=True)
    new_balance = u[0] - VIP_PRICE_INR
    now_date = datetime.now().strftime("%Y-%m-%d")
    await db_query("UPDATE users SET balance=%s, is_vip=1, vip_since=%s WHERE user_id=%s",
                   (new_balance, now_date, call.from_user.id))
    await log_activity(call.from_user.id, "UPGRADED_VIP")
    try:
        await bot.send_message(ADMIN_ID, f"🌟 <b>NEW VIP UPGRADE</b>\n👤 User ID: <code>{call.from_user.id}</code>", parse_mode='HTML')
    except Exception:
        pass
    await call.answer("🎉 Upgrade Successful! You are now a VIP Member.", show_alert=True)
    await vip_dashboard(call)

@dp.callback_query(F.data == "menu_reseller_dash")
async def reseller_dashboard(call: CallbackQuery):
    u = await db_query("SELECT balance, is_reseller, reseller_since, total_saved FROM users WHERE user_id=%s",
                       (call.from_user.id,), fetchone=True)
    status_check = await db_query("SELECT value FROM settings WHERE `key`='reseller_system_status'", fetchone=True)
    system_status = status_check[0] if status_check else "ON"
    setup_fee = float(await get_setting("reseller_setup_fee", "200.0"))
    min_balance = float(await get_setting("reseller_min_balance", "500.0"))
    if u[1]:
        text = (f"{await get_emoji('shield_icon')} <b><u>— RESELLER DASHBOARD —</u></b> {await get_emoji('shield_icon')}\n\n"
                f"🟢 <b>Status:</b> Active\n📅 <b>Since:</b> {u[2]}\n"
                f"{await get_emoji('money_icon')} <b>Total Saved:</b> {fmt_curr(u[3])}\n\n"
                "🎉 You are enjoying exclusive wholesale prices on all products!")
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
            text="BACK", callback_data="back_main",
            icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]])
        await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')
        return
    if system_status == "OFF":
        return await call.answer("⚠️ Wholesale / Reseller registrations are currently closed by Admin.", show_alert=True)
    text = (f"⚡ <b><u>— BECOME A RESELLER —</u></b> ⚡\n\n"
            f"Upgrade your account to access wholesale <b>Reseller Prices</b>!\n\n"
            f"📋 <b>Requirements to Upgrade:</b>\n"
            f"1️⃣ Must have a minimum balance of <b>{fmt_curr(min_balance)}</b>.\n"
            f"2️⃣ A one-time setup fee of <b>{fmt_curr(setup_fee)}</b> will be deducted.\n\n"
            f"💳 <b>Your Current Balance:</b> {fmt_curr(u[0])}\n")
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    if u[0] >= min_balance:
        kb.inline_keyboard.append([InlineKeyboardButton(
            text=f"✅ Pay {fmt_curr(setup_fee)} & Become Reseller",
            callback_data="execute_reseller_upgrade",
            icon_custom_emoji_id=await get_emoji_icon("reseller"), style="success")])
    else:
        kb.inline_keyboard.append([InlineKeyboardButton(
            text=f"❌ Insufficient Balance (Need {fmt_curr(min_balance)})",
            callback_data="ignore_stock_click", style="danger")])
        kb.inline_keyboard.append([InlineKeyboardButton(
            text="💳 Add Balance", callback_data="menu_add_balance",
            icon_custom_emoji_id=await get_emoji_icon("add_balance"), style="success")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="BACK", callback_data="back_main",
                                                    icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "execute_reseller_upgrade")
async def execute_reseller_upgrade(call: CallbackQuery):
    setup_fee = float(await get_setting("reseller_setup_fee", "200.0"))
    min_balance = float(await get_setting("reseller_min_balance", "500.0"))
    u = await db_query("SELECT balance, is_reseller FROM users WHERE user_id=%s", (call.from_user.id,), fetchone=True)
    if u[1]:
        return await call.answer("⚠️ You are already a Reseller!", show_alert=True)
    if u[0] < min_balance:
        return await call.answer(f"❌ Your balance dropped below {fmt_curr(min_balance)}. Please top up.", show_alert=True)
    new_balance = u[0] - setup_fee
    await db_query("UPDATE users SET balance=%s, is_reseller=1, reseller_since=%s, account_type='Reseller' WHERE user_id=%s",
                   (new_balance, datetime.now().strftime("%Y-%m-%d"), call.from_user.id))
    await log_activity(call.from_user.id, "UPGRADED_RESELLER")
    try:
        await bot.send_message(ADMIN_ID, f"👑 <b>NEW RESELLER UPGRADE</b>\n👤 User ID: <code>{call.from_user.id}</code>", parse_mode='HTML')
    except Exception:
        pass
    await call.answer("🎉 Upgrade Successful! Welcome to the Reseller tier.", show_alert=True)
    await reseller_dashboard(call)

@dp.callback_query(F.data == "menu_orders")
async def my_orders(call: CallbackQuery):
    orders = await db_query(
        "SELECT product_name, delivered_key, purchase_date, price_paid FROM orders WHERE user_id=%s ORDER BY id DESC LIMIT 10",
        (call.from_user.id,), fetchall=True)
    if not orders:
        return await call.message.edit_text("🧾 You haven't made any purchases yet. Your vault is empty.",
                                            reply_markup=await back_kb(), parse_mode='HTML')
    text = "🧾 <b><u>— YOUR RECENT ORDERS (LAST 10) —</u></b> 🧾\n\n"
    for o in orders:
        text += f"📦 <b>{o[0]}</b> ({fmt_curr(o[3])})\n🔑 <code>{o[1]}</code>\n📅 <i>{o[2]}</i>\n━━━━━━━━━━━━━━━━\n"
    await call.message.edit_text(text, reply_markup=await back_kb(), parse_mode='HTML')

@dp.callback_query(F.data == "menu_profile")
async def show_profile(call: CallbackQuery):
    u = await db_query(
        "SELECT user_id, first_name, account_type, balance, orders_count, spent, referrals_count, "
        "joined_date, is_reseller, reseller_since, total_saved, is_vip FROM users WHERE user_id=%s",
        (call.from_user.id,), fetchone=True)
    acc_type_display = []
    if u[8]: acc_type_display.append(f"{await get_emoji('reseller')} Reseller")
    if u[11]: acc_type_display.append(f"{await get_emoji('vip')} VIP")
    type_str = " | ".join(acc_type_display) if acc_type_display else f"{await get_emoji('regular_user')} Regular User"
    text = (
        f"{await get_emoji('grid_id')} <b><u>— YOUR SECURE PROFILE —</u></b> {await get_emoji('grid_id')}\n\n"
        f"{await get_emoji('grid_id')} <b>Grid ID:</b> <code>{u[0]}</code>\n"
        f"{await get_emoji('name')} <b>Name:</b> {u[1]}\n"
        f"{await get_emoji('account_level')} <b>Account Level:</b> {type_str}\n\n"
        f"{await get_emoji('wallet_left')} <b>— Wallet —</b> {await get_emoji('wallet_right')}\n"
        f"{await get_emoji('wallet_left')} <b>Current Balance:</b> {fmt_curr(u[3])} {await get_emoji('wallet_right')}\n\n"
        f"{await get_emoji('global_stats')} <b>— Global Statistics —</b>\n"
        f"{await get_emoji('total_orders')} <b>Total Orders:</b> {u[4]}\n"
        f"{await get_emoji('total_spent')} <b>Total Spent:</b> {fmt_curr(u[5])}\n"
        f"{await get_emoji('total_referrals')} <b>Total Referrals:</b> {u[6]}\n\n"
    )
    if u[8]:
        text += (f"{await get_emoji('shield_icon')} <b>— RESELLER METRICS —</b> {await get_emoji('shield_icon')}\n"
                 f"{await get_emoji('money_icon')} <b>Total Saved via Reseller:</b> {fmt_curr(u[10])}\n\n")
    text += f"{await get_emoji('joined_grid')} <b>Joined Grid:</b> {u[7]}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Redeem Promo Code", callback_data="redeem_coupon",
                              icon_custom_emoji_id=await get_emoji_icon('redeem_icon'), style="success")],
        [InlineKeyboardButton(text="BACK", callback_data="back_main",
                              icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "redeem_coupon")
async def redeem_coupon_start(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("🎟 <b>Please enter your VIP / Promo redeem code below:</b>",
                                 reply_markup=await back_kb("menu_profile"), parse_mode='HTML')
    await state.set_state(UserStates.wait_for_redeem)

@dp.message(UserStates.wait_for_redeem)
async def process_redeem(m: Message, state: FSMContext):
    code = m.text.strip().upper()
    user_id = m.from_user.id
    already = await db_query("SELECT 1 FROM redeemed WHERE user_id=%s AND code=%s", (user_id, code), fetchone=True)
    if already:
        await m.answer("❌ Anti-Fraud Alert: You already redeemed this unique code!",
                       reply_markup=await main_menu_kb(m.from_user.id), parse_mode='HTML')
        await state.clear()
        return
    coupon = await db_query("SELECT amount, uses_left FROM coupons WHERE code=%s", (code,), fetchone=True)
    if not coupon:
        await m.answer("❌ Invalid or Expired Code!", reply_markup=await main_menu_kb(m.from_user.id), parse_mode='HTML')
    elif coupon[1] <= 0:
        await m.answer("❌ This code's usage limit has been fully claimed by other users.",
                       reply_markup=await main_menu_kb(m.from_user.id), parse_mode='HTML')
    else:
        await db_query("UPDATE users SET balance = balance + %s WHERE user_id=%s", (coupon[0], user_id))
        await db_query("UPDATE coupons SET uses_left = uses_left - 1 WHERE code=%s", (code,))
        await db_query("INSERT INTO redeemed (user_id, code) VALUES (%s, %s)", (user_id, code))
        await log_activity(user_id, "PROMO_REDEEMED", f"Code: {code}, Amount: {coupon[0]}")
        await m.answer(f"🎉 <b>Success!</b>\nSafely added {fmt_curr(coupon[0])} to your balance!",
                       reply_markup=await main_menu_kb(m.from_user.id), parse_mode='HTML')
        try:
            user_info = await db_query("SELECT first_name FROM users WHERE user_id=%s", (user_id,), fetchone=True)
            uname = user_info[0] if user_info else "Unknown User"
            await bot.send_message(ADMIN_ID,
                f"🎟 <b>PROMO CODE REDEEMED!</b>\n👤 User: {uname} (<code>{user_id}</code>)\n🔖 Code: <b>{code}</b>\n💵 Amount: {fmt_curr(coupon[0])}",
                parse_mode='HTML')
        except Exception:
            pass
    await state.clear()

@dp.callback_query(F.data == "menu_referral")
async def show_referral(call: CallbackQuery):
    u = await db_query("SELECT referrals_count, referral_earned FROM users WHERE user_id=%s", (call.from_user.id,), fetchone=True)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{call.from_user.id}"
    text = (f"{await get_emoji('referral')} <b><u>AFFILIATE PROGRAM</u></b> {await get_emoji('referral')}\n\n"
            f"✅ <b>Status:</b> ACTIVE\n💰 Earn <b>2% flat commission</b> on every successful purchase made by your referred friends!\n\n"
            f"📊 <b>YOUR STATS:</b>\n👥 Total Invited: {u[0]}\n💵 Life-time Earned: {fmt_curr(u[1])}\n\n"
            f"🔗 <b>Your Invite Link:</b>\n<code>{ref_link}</code>\n\n"
            "<i>Simply copy and share this link to start earning!</i>")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
        text="BACK", callback_data="back_main",
        icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

# ==============================================================================
# 15. LUDO / DICE SPIN
# ==============================================================================
@dp.callback_query(F.data == "menu_spin_landing")
async def lucky_spin_landing(call: CallbackQuery):
    status_check = await db_query("SELECT value FROM settings WHERE `key`='spin_status'", fetchone=True)
    spin_status = status_check[0] if status_check else "ON"
    if spin_status == "OFF":
        return await call.answer("⚠️ Lucky Ludo Spin is currently disabled by Admin.", show_alert=True)
    await call.message.edit_text(
        f"{await get_emoji('ludo_spin')} <b><u>— LUDO SPIN —</u></b> {await get_emoji('ludo_spin')}\n\n"
        "Test your luck! You can spin once every 24 hours.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎲 Spin Dice Now!", callback_data="execute_spin",
                                  icon_custom_emoji_id=await get_emoji_icon("ludo_spin"), style="success")],
            [InlineKeyboardButton(text="BACK", callback_data="back_main",
                                  icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]]),
        parse_mode='HTML')

@dp.callback_query(F.data == "execute_spin")
async def execute_spin(call: CallbackQuery):
    status_check = await db_query("SELECT value FROM settings WHERE `key`='spin_status'", fetchone=True)
    if status_check and status_check[0] == "OFF":
        return await call.answer("⚠️ Lucky Spin is disabled.", show_alert=True)
    u = await db_query("SELECT last_spin, balance, is_vip FROM users WHERE user_id=%s", (call.from_user.id,), fetchone=True)
    now = datetime.now()
    if u[0] and now < datetime.strptime(u[0], "%Y-%m-%d %H:%M:%S") + timedelta(hours=24):
        return await call.message.edit_text("❌ <b>Cooldown Active!</b>\nYou already played today. Come back tomorrow.",
                                            reply_markup=await back_kb(), parse_mode='HTML')
    await call.message.delete()
    dice_msg = await bot.send_dice(chat_id=call.message.chat.id, emoji="🎲")
    await asyncio.sleep(SPIN_DELAY_SECONDS)
    dice_val = dice_msg.dice.value
    limit_check = await db_query("SELECT value FROM settings WHERE `key`='daily_spin_limit'", fetchone=True)
    limit = float(limit_check[0]) if limit_check else 50.0
    rewards_db = await db_query("SELECT amount FROM spin_rewards WHERE amount <= %s", (limit,), fetchall=True)
    rewards_list = [r[0] for r in rewards_db] if rewards_db else [0.0]
    reward = random.choice(rewards_list)
    if bool(u[2]) and reward > 0:
        reward = reward * 2.0
    new_bal = u[1] + reward
    await db_query("UPDATE users SET balance=%s, last_spin=%s WHERE user_id=%s",
                   (new_bal, now.strftime("%Y-%m-%d %H:%M:%S"), call.from_user.id))
    await log_activity(call.from_user.id, "PLAYED_SPIN", f"Reward: {reward}, Dice: {dice_val}")
    msg = await get_ui_text("lucky_dice_result", dice_value=dice_val,
                            won_amount=fmt_curr(reward), new_balance=fmt_curr(new_bal))
    if bool(u[2]) and reward > 0:
        msg += "\n\n<i>🌟 VIP Bonus: 2x Multiplier Applied!</i>"
    await dice_msg.reply(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
        text="📚 BACK TO MENU", callback_data="back_main",
        icon_custom_emoji_id=await get_emoji_icon("back"), style="success")]]), parse_mode='HTML')

# ==============================================================================
# 16. TUTORIALS & SUPPORT
# ==============================================================================
@dp.callback_query(F.data == "menu_how_to")
async def tutorial_system(call: CallbackQuery):
    video_link_query = await db_query("SELECT value FROM settings WHERE `key`='how_to_video'", fetchone=True)
    video_link = video_link_query[0] if video_link_query and video_link_query[0] != 'None' else None
    text = (f"{await get_emoji('tutorial')} <b><u>— TUTORIALS & GUIDE —</u></b> {await get_emoji('tutorial')}\n\n"
            "1️⃣ Add funds via <b>Add Balance</b>\n2️⃣ Navigate to <b>Product Store</b>\n"
            "3️⃣ Choose your desired Panel and Package validity.\n"
            "4️⃣ The Key and Installation APK link will be instantly provided.")
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    if video_link:
        kb.inline_keyboard.append([InlineKeyboardButton(text="Watch Full Video Tutorial", url=video_link,
                                                        icon_custom_emoji_id=await get_emoji_icon("tutorial"), style="success")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="BACK", callback_data="back_main",
                                                    icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "menu_support")
async def support_center(call: CallbackQuery):
    telegram_link = await get_setting("support_telegram", "https://t.me/YOUR_SUPPORT")
    whatsapp_link = await get_setting("support_whatsapp", "https://wa.me/YOUR_NUMBER")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Contact on Telegram", url=telegram_link,
                              icon_custom_emoji_id=await get_emoji_icon("telegram"), style="primary")],
        [InlineKeyboardButton(text="Contact on WhatsApp", url=whatsapp_link,
                              icon_custom_emoji_id=await get_emoji_icon("whatsapp"), style="primary")],
        [InlineKeyboardButton(text="🎫 Open New Ticket", callback_data="open_ticket",
                              icon_custom_emoji_id=await get_emoji_icon("support"), style="success"),
         InlineKeyboardButton(text="📋 My Open Tickets", callback_data="my_tickets",
                              icon_custom_emoji_id=await get_emoji_icon("history"), style="success")],
        [InlineKeyboardButton(text="BACK", callback_data="back_main",
                              icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]
    ])
    await call.message.edit_text(
        f"{await get_emoji('telegram')}{await get_emoji('whatsapp')} <b><u>— PREMIUM SUPPORT CENTER —</u></b>\n\n"
        "Contact us via Telegram or WhatsApp for instant help, or open a support ticket for admin assistance.",
        reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "my_tickets")
async def view_my_tickets(call: CallbackQuery):
    tickets = await db_query("SELECT id, message, status, created_at FROM tickets WHERE user_id=%s ORDER BY id DESC LIMIT 5",
                             (call.from_user.id,), fetchall=True)
    if not tickets:
        return await call.message.edit_text("📋 You do not have any active or previous support tickets.",
                                            reply_markup=await back_kb("menu_support"), parse_mode='HTML')
    text = "📋 <b><u>— Your Recent Tickets —</u></b> 📋\n\n"
    for t in tickets:
        status_icon = "🟢" if t[2] == 'Open' else "🔴"
        text += f"🎫 <b>Ticket #{t[0]}</b> | Status: {status_icon} <b>{t[2]}</b>\n📅 <i>{t[3]}</i>\n📝 <i>{t[1][:80]}...</i>\n\n"
    await call.message.edit_text(text, reply_markup=await back_kb("menu_support"), parse_mode='HTML')

@dp.callback_query(F.data == "open_ticket")
async def open_ticket_start(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("📝 <b>Please type your issue/message below in detail:</b>",
                                 reply_markup=await back_kb("menu_support"), parse_mode='HTML')
    await state.set_state(UserStates.wait_for_ticket)

@dp.message(UserStates.wait_for_ticket)
async def process_ticket(m: Message, state: FSMContext):
    await db_query("INSERT INTO tickets (user_id, message, created_at) VALUES (%s, %s, %s)",
                   (m.from_user.id, m.text, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    await m.answer("✅ <b>Ticket Submitted Successfully!</b> Admins will reply soon.",
                   reply_markup=await main_menu_kb(m.from_user.id), parse_mode='HTML')
    try:
        await bot.send_message(ADMIN_ID, f"🚨 <b>NEW SUPPORT TICKET</b>\nFrom: <code>{m.from_user.id}</code>\nMsg: {m.text}", parse_mode='HTML')
    except Exception:
        pass
    await log_activity(m.from_user.id, "OPENED_TICKET")
    await state.clear()

# ==============================================================================
# 17. ADMIN PANEL
# ==============================================================================
@dp.message(Command("admin"))
async def admin_panel(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await message.answer("⚙️ <b>Advanced Admin Terminal</b>\n<i>Authorized Access Granted. Use the buttons below.</i>",
                         reply_markup=await admin_kb(), parse_mode='HTML')

@dp.callback_query(F.data == "admin_panel_back")
async def back_to_admin(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("⚙️ <b>Advanced Admin Terminal</b>\n<i>Authorized Access Granted. Use the buttons below.</i>",
                                 reply_markup=await admin_kb(), parse_mode='HTML')

@dp.callback_query(F.data == "admin_toggle_vip_sys")
async def toggle_vip_sys(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    res = await db_query("SELECT value FROM settings WHERE `key`='vip_status'", fetchone=True)
    current = res[0] if res else 'OFF'
    new_status = 'ON' if current == 'OFF' else 'OFF'
    await db_query("REPLACE INTO settings (`key`, value) VALUES ('vip_status', %s)", (new_status,))
    await call.message.edit_reply_markup(reply_markup=await admin_kb())

@dp.callback_query(F.data == "admin_user_control_start")
async def admin_user_control_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Download Full User List", callback_data="admin_download_userlist",
                              icon_custom_emoji_id=await get_emoji_icon("download"), style="success")],
        [InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back",
                              icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]
    ])
    await call.message.edit_text(
        "💻 <b>User Control Terminal</b>\n\n✏️ Enter the <b>User ID</b> or <b>@Username</b> you want to investigate or manage:\n\n"
        "👇 <b>OR</b> download the full user CSV format list:",
        reply_markup=kb, parse_mode='HTML')
    await state.set_state(AdminStates.manage_target_user)

@dp.callback_query(F.data == "admin_download_userlist")
async def admin_download_userlist(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    users = await db_query("SELECT username, user_id, phone, balance, orders_count, is_vip, is_reseller FROM users", fetchall=True)
    if not users:
        return await call.answer("❌ No users found in the database.", show_alert=True)
    file_content = "FULL DATABASE DUMP\n" + "=" * 100 + "\n"
    for u in users:
        uname = u[0] if u[0] else "No_Username"
        uid = u[1]
        phone = u[2] if u[2] else "No_Phone"
        bal = u[3]
        orders = u[4]
        vip_status = "YES" if u[5] else "NO"
        res_status = "YES" if u[6] else "NO"
        file_content += f"UID: {uid} | UNAME: {uname} | PHONE: {phone} | BAL: ₹{bal:.2f} | BUY: {orders} | VIP: {vip_status} | RES: {res_status}\n"
    doc = BufferedInputFile(file_content.encode('utf-8'), filename=f"DB_{datetime.now().strftime('%Y%m%d')}.txt")
    await call.message.answer_document(document=doc, caption="📋 <b>Database export complete.</b>", parse_mode='HTML')
    await call.answer()

@dp.message(AdminStates.manage_target_user)
async def process_user_lookup(m: Message, state: FSMContext):
    target = m.text.strip()
    if target.startswith('@'):
        target = target[1:]
    loader_msg = await hacker_loading(m, "Querying User Database")
    user_q = await db_query(
        "SELECT user_id, first_name, username, balance, is_reseller, orders_count, spent, joined_date, "
        "is_banned, warnings, is_vip FROM users WHERE user_id=%s OR username=%s",
        (target, target), fetchone=True)
    if not user_q:
        return await loader_msg.edit_text("❌ Target not found in the grid. Check ID/Username syntax.",
                                          reply_markup=await admin_back_kb(), parse_mode='HTML')
    u_id, u_name, u_user, bal, is_res, orders, spent, joined, is_banned, warnings, is_vip = user_q
    await state.update_data(target_u_id=u_id)
    status_emoji = "🔴 BANNED" if is_banned else "🟢 ACTIVE"
    tags = []
    if is_res: tags.append("👑 Reseller")
    if is_vip: tags.append("🌟 VIP")
    type_str = " | ".join(tags) if tags else "👤 Regular"
    text = (f"🛡 <b><u>USER CONTROL TERMINAL</u></b> 🛡\n━━━━━━━━━━━━━━━━━━\n"
            f"📛 <b>Name:</b> {u_name} (@{u_user})\n🆔 <b>ID:</b> <code>{u_id}</code>\n"
            f"📊 <b>Status:</b> {status_emoji}\n🔰 <b>Type:</b> {type_str}\n"
            f"⚠️ <b>Warnings Issued:</b> {warnings}\n━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>Wallet Balance:</b> {fmt_curr(bal)}\n"
            f"📦 <b>Orders:</b> {orders} | 💸 <b>Total Spent:</b> {fmt_curr(spent)}\n"
            f"📅 <b>Joined:</b> {joined}")
    ban_btn_text = "Unban ✅" if is_banned else "Ban 🚫"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Add Funds ➕", callback_data=f"usrctrl_add_{u_id}",
                              icon_custom_emoji_id=await get_emoji_icon("add_balance"), style="success"),
         InlineKeyboardButton(text="Minus Funds ➖", callback_data=f"usrctrl_min_{u_id}",
                              icon_custom_emoji_id=await get_emoji_icon("money_icon"), style="danger")],
        [InlineKeyboardButton(text=ban_btn_text, callback_data=f"usrctrl_ban_{u_id}",
                              icon_custom_emoji_id=await get_emoji_icon("shield_icon"), style="danger"),
         InlineKeyboardButton(text="Warn User ⚠️", callback_data=f"usrctrl_warn_{u_id}",
                              icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="danger")],
        [InlineKeyboardButton(text="Give VIP 🌟" if not is_vip else "Remove VIP 🚫",
                              callback_data=f"usrctrl_vip_{u_id}",
                              icon_custom_emoji_id=await get_emoji_icon("vip"), style="success")],
        [InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back",
                              icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]
    ])
    await loader_msg.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("usrctrl_"))
async def handle_user_actions(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    action = call.data.split("_")[1]
    u_id = int(call.data.split("_")[2])
    await state.update_data(target_u_id=u_id)
    if action == "ban":
        row = await db_query("SELECT is_banned FROM users WHERE user_id=%s", (u_id,), fetchone=True)
        current_status = row[0] if row else 0
        if current_status == 0:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Yes, Ban", callback_data=f"confirm_ban_{u_id}",
                                      icon_custom_emoji_id=await get_emoji_icon("check_icon"), style="danger"),
                 InlineKeyboardButton(text="❌ Cancel", callback_data="admin_user_control_start",
                                      icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]])
            await call.message.edit_text(f"⚠️ Are you sure you want to <b>BAN</b> user <code>{u_id}</code>?",
                                         reply_markup=kb, parse_mode='HTML')
            await state.set_state(AdminStates.confirm_ban)
        else:
            await db_query("UPDATE users SET is_banned=0 WHERE user_id=%s", (u_id,))
            await call.answer("✅ User unbanned successfully!", show_alert=True)
            m = call.message
            m.text = str(u_id)
            await process_user_lookup(m, state)
    elif action == "vip":
        row = await db_query("SELECT is_vip FROM users WHERE user_id=%s", (u_id,), fetchone=True)
        current_status = row[0] if row else 0
        if current_status == 1:
            await db_query("UPDATE users SET is_vip=0 WHERE user_id=%s", (u_id,))
            await call.answer("✅ VIP Removed!", show_alert=True)
        else:
            await db_query("UPDATE users SET is_vip=1, vip_since=%s WHERE user_id=%s",
                           (datetime.now().strftime("%Y-%m-%d"), u_id))
            await call.answer("✅ VIP Granted!", show_alert=True)
        m = call.message
        m.text = str(u_id)
        await process_user_lookup(m, state)
    elif action == "add":
        await call.message.edit_text("💰 Enter the amount to <b>ADD</b> to this user's wallet:",
                                     reply_markup=await admin_back_kb(), parse_mode='HTML')
        await state.set_state(AdminStates.wait_for_add_money)
    elif action == "min":
        await call.message.edit_text("💸 Enter the amount to <b>DEDUCT</b> from this user's wallet:",
                                     reply_markup=await admin_back_kb(), parse_mode='HTML')
        await state.set_state(AdminStates.wait_for_minus_money)
    elif action == "warn":
        await call.message.edit_text("⚠️ Type the strict warning message you want to send directly to this user:",
                                     reply_markup=await admin_back_kb(), parse_mode='HTML')
        await state.set_state(AdminStates.wait_for_warning)

@dp.callback_query(F.data.startswith("confirm_ban_"))
async def confirm_ban(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    u_id = int(call.data.split("_")[2])
    await db_query("UPDATE users SET is_banned=1 WHERE user_id=%s", (u_id,))
    await call.answer("🔴 User has been banned!", show_alert=True)
    await state.clear()
    m = call.message
    m.text = str(u_id)
    await process_user_lookup(m, state)

@dp.message(AdminStates.wait_for_add_money)
async def exec_add_money(m: Message, state: FSMContext):
    try:
        amt = float(m.text)
        data = await state.get_data()
        u_id = data['target_u_id']
        await db_query("UPDATE users SET balance = balance + %s WHERE user_id=%s", (amt, u_id))
        await m.answer(f"✅ Successfully added {fmt_curr(amt)} to target <code>{u_id}</code>.",
                       reply_markup=await admin_kb(), parse_mode='HTML')
        try:
            await bot.send_message(u_id, f"💰 <b>Wallet Top-up!</b>\nAdmin has manually added {fmt_curr(amt)} to your wallet.", parse_mode='HTML')
        except Exception:
            pass
        await state.clear()
    except ValueError:
        await m.answer("❌ Critical Error: Input must be a valid number.")

@dp.message(AdminStates.wait_for_minus_money)
async def exec_minus_money(m: Message, state: FSMContext):
    try:
        amt = float(m.text)
        data = await state.get_data()
        u_id = data['target_u_id']
        await db_query("UPDATE users SET balance = balance - %s WHERE user_id=%s", (amt, u_id))
        await m.answer(f"✅ Successfully deducted {fmt_curr(amt)} from target <code>{u_id}</code>.",
                       reply_markup=await admin_kb(), parse_mode='HTML')
        await state.clear()
    except ValueError:
        await m.answer("❌ Critical Error: Input must be a valid number.")

@dp.message(AdminStates.wait_for_warning)
async def exec_warn_user(m: Message, state: FSMContext):
    data = await state.get_data()
    u_id = data['target_u_id']
    warn_text = m.text
    await db_query("UPDATE users SET warnings = warnings + 1 WHERE user_id=%s", (u_id,))
    await m.answer(f"✅ Official warning dispatched to <code>{u_id}</code>.",
                   reply_markup=await admin_kb(), parse_mode='HTML')
    try:
        await bot.send_message(u_id, f"⚠️ <b>OFFICIAL WARNING FROM SYSTEM ADMIN:</b>\n\n{warn_text}\n\n"
                                     "<i>Subsequent infractions may lead to an automated grid ban.</i>", parse_mode='HTML')
    except Exception:
        pass
    await state.clear()

# ==============================================================================
# 18. ADMIN STATISTICS
# ==============================================================================
@dp.callback_query(F.data == "admin_view_stats")
async def admin_dashboard_stats(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    t_users = (await db_query("SELECT COUNT(*) FROM users", fetchone=True))[0]
    t_resellers = (await db_query("SELECT COUNT(*) FROM users WHERE is_reseller=1", fetchone=True))[0]
    t_vip = (await db_query("SELECT COUNT(*) FROM users WHERE is_vip=1", fetchone=True))[0]
    t_prods = (await db_query("SELECT COUNT(*) FROM products", fetchone=True))[0]
    t_keys = (await db_query("SELECT COUNT(*) FROM product_keys WHERE is_used=0", fetchone=True))[0]
    rev_row = await db_query("SELECT SUM(spent) FROM users", fetchone=True)
    t_rev = rev_row[0] if rev_row and rev_row[0] else 0.0
    today_str = datetime.now().strftime("%Y-%m-%d")
    t_spins = (await db_query("SELECT COUNT(*) FROM users WHERE last_spin LIKE %s", (f"{today_str}%",), fetchone=True))[0]
    msg = (f"📊 <b><u>GRID INTELLIGENCE DASHBOARD</u></b> 📊\n━━━━━━━━━━━━━━━━━━\n"
           f"👥 <b>Total Grid Users:</b> {t_users}\n👑 <b>Wholesale Resellers:</b> {t_resellers}\n"
           f"🌟 <b>Elite VIP Members:</b> {t_vip}\n━━━━━━━━━━━━━━━━━━\n"
           f"📦 <b>Active Products:</b> {t_prods}\n🔑 <b>Unused Keys in Vault:</b> {t_keys}\n"
           f"💰 <b>Total Gross Revenue:</b> {fmt_curr(t_rev)}\n🎰 <b>Ludo Spins Today:</b> {t_spins}\n"
           "━━━━━━━━━━━━━━━━━━")
    await call.message.edit_text(msg, reply_markup=await admin_back_kb(), parse_mode='HTML')

# ==============================================================================
# 19. ADMIN PRODUCT MANAGEMENT
# ==============================================================================
@dp.callback_query(F.data == "admin_add_prod")
async def add_prod_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for cat in FIXED_CATEGORIES:
        emoji_id = await get_category_emoji(cat)
        kb.inline_keyboard.append([InlineKeyboardButton(text=cat, callback_data=f"addprod_cat_{cat}",
                                                        icon_custom_emoji_id=emoji_id, style="danger")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="Cancel", callback_data="admin_panel_back",
                                                    icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")])
    await call.message.edit_text("<b>Step 1:</b> Choose the <b>Category</b> for this product:",
                                 reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("addprod_cat_"))
async def add_prod_category_selected(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    category = call.data.split("addprod_cat_", 1)[1]
    await state.update_data(cat=category)
    await call.message.edit_text(f"<b>Step 2:</b> Enter <b>PANEL NAME</b>\n(e.g., 'MST PANEL', 'DRIP PANEL'):",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.add_prod_panel_name)

@dp.message(AdminStates.add_prod_panel_name)
async def add_prod_panel_name(m: Message, state: FSMContext):
    await state.update_data(panel_name=m.text)
    await m.answer("<b>Step 3:</b> Enter <b>PACKAGE DURATION/DATE NAME</b>\n(e.g., '7 Days', '1 Month'):", parse_mode='HTML')
    await state.set_state(AdminStates.add_prod_name)

@dp.message(AdminStates.add_prod_name)
async def add_prod_name(m: Message, state: FSMContext):
    await state.update_data(name=m.text)
    await m.answer("⏳ Enter Time Validity String (e.g., '24 Hours'):", parse_mode='HTML')
    await state.set_state(AdminStates.add_prod_validity)

@dp.message(AdminStates.add_prod_validity)
async def add_prod_validity(m: Message, state: FSMContext):
    await state.update_data(validity=m.text)
    await m.answer("📱 Enter strict Device Enforcement Limit (e.g., '1 Device HWID'):", parse_mode='HTML')
    await state.set_state(AdminStates.add_prod_device_limit)

@dp.message(AdminStates.add_prod_device_limit)
async def add_prod_device_limit(m: Message, state: FSMContext):
    await state.update_data(device_limit=m.text)
    await m.answer("💰 Enter standard **User Price** in Rupees (₹) (e.g., 500):", parse_mode='HTML')
    await state.set_state(AdminStates.add_prod_price)

@dp.message(AdminStates.add_prod_price)
async def add_prod_price(m: Message, state: FSMContext):
    try:
        await state.update_data(price=float(m.text))
        await m.answer("👑 Enter wholesale **Reseller Price** in Rupees (₹) (e.g., 300):", parse_mode='HTML')
        await state.set_state(AdminStates.add_prod_reseller_price)
    except ValueError:
        await m.answer("❌ Invalid input datatype! Must be numerical.")

@dp.message(AdminStates.add_prod_reseller_price)
async def add_prod_reseller_price(m: Message, state: FSMContext):
    try:
        await state.update_data(reseller_price=float(m.text))
        await m.answer("🔗 Enter direct APK/Payload Download Link (or type 'none' to omit):", parse_mode='HTML')
        await state.set_state(AdminStates.add_prod_apk)
    except ValueError:
        await m.answer("❌ Invalid input datatype! Must be numerical.")

@dp.message(AdminStates.add_prod_apk)
async def add_prod_apk(m: Message, state: FSMContext):
    await state.update_data(apk="" if m.text.lower() == 'none' else m.text)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Yes — Generate Key via API", callback_data="addprod_ext_yes",
                              icon_custom_emoji_id=await get_emoji_icon("check_icon"), style="success")],
        [InlineKeyboardButton(text="❌ No — Use Manual Keys", callback_data="addprod_ext_no",
                              icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]])
    await m.answer("🔗 <b>Does this product use the external API for automatic key generation?</b>",
                   reply_markup=kb, parse_mode='HTML')
    await state.set_state(AdminStates.add_prod_external)

@dp.callback_query(F.data == "addprod_ext_yes", AdminStates.add_prod_external)
async def add_prod_external_yes(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    await state.update_data(external_enabled=1)
    await call.message.edit_text("🆔 Enter the <b>External Product ID (PID)</b> used by the API:",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.add_prod_external_product_id)

@dp.message(AdminStates.add_prod_external_product_id)
async def add_prod_external_pid(m: Message, state: FSMContext):
    pid = m.text.strip()
    if not pid:
        return await m.answer("❌ Product PID cannot be empty.")
    data = await state.get_data()
    await state.update_data(external_product_id=pid, requires_android_id=0)
    await m.answer(
        "⏱ <b>Enter the exact XYZ API duration/price tier.</b>\n\n"
        "Examples: <code>3 Hours</code>, <code>1 Day</code>, <code>7 Days</code>.\n"
        f"Your shop validity is: <code>{data.get('validity', '')}</code>\n\n"
        "If the API uses the same duration, send the same value.",
        reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.add_prod_external_duration)

@dp.message(AdminStates.add_prod_external_duration)
async def add_prod_external_duration(m: Message, state: FSMContext):
    api_duration = normalize_api_duration(m.text.strip())
    if not api_duration:
        return await m.answer("❌ API duration cannot be empty.")
    data = await state.get_data()
    await db_query("""
        INSERT INTO products
        (category, panel_name, name, price_inr, reseller_price, stock, apk_link, validity, device_limit,
         external_enabled, external_product_id, requires_android_id, external_duration)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (data['cat'], data['panel_name'], data['name'], data['price'], data['reseller_price'],
          1, data['apk'], data['validity'], data['device_limit'], 1,
          data['external_product_id'], 0, api_duration))
    last = await db_query("SELECT LAST_INSERT_ID()", fetchone=True)
    prod_id = last[0] if last else "?"
    await m.answer(
        f"✅ <b>API Product Created!</b>\n\nProduct ID: <code>{prod_id}</code>\n"
        f"External Product ID: <code>{data['external_product_id']}</code>\n"
        f"API Duration: <code>{api_duration}</code>\n\n"
        "The API will generate and deliver the key when the customer buys it.",
        reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "addprod_ext_no", AdminStates.add_prod_external)
async def add_prod_external_no(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    await state.update_data(external_enabled=0, external_product_id="", requires_android_id=0)
    await call.message.edit_text("📥 <b>Manual Key Product</b>\n\nNow send the keys, one key per line:",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.add_prod_keys)

@dp.message(AdminStates.add_prod_keys)
async def add_prod_keys(m: Message, state: FSMContext):
    keys = [k.strip() for k in m.text.strip().split('\n') if k.strip()]
    if not keys:
        return await m.answer("❌ No valid keys found. Send at least one key.")
    data = await state.get_data()
    stock = len(keys)
    await db_query("""
        INSERT INTO products
        (category, panel_name, name, price_inr, reseller_price, stock, apk_link, validity, device_limit,
         external_enabled, external_product_id, requires_android_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (data['cat'], data['panel_name'], data['name'], data['price'], data['reseller_price'], stock,
          data['apk'], data['validity'], data['device_limit'], 0, '', 0))
    last = await db_query("SELECT LAST_INSERT_ID()", fetchone=True)
    prod_id = last[0] if last else 0
    for k in keys:
        await db_query("INSERT INTO product_keys (product_id, key_text) VALUES (%s, %s)", (prod_id, k))
    await m.answer(f"✅ <b>Manual Product Created!</b>\n\n📦 Product ID: <code>{prod_id}</code>\n🔒 Stock: {stock} keys",
                   reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_manage_prods")
async def admin_manage_prods(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("⛔ Admin only.", show_alert=True)
        return
    try:
        prods = await db_query(
            """SELECT id, name, category, panel_name, COALESCE(stock, 0), COALESCE(is_active, 1)
               FROM products
               ORDER BY category, panel_name, name, id""",
            fetchall=True) or []
        if not prods:
            await call.answer("📦 No products found.", show_alert=True)
            await call.message.edit_text("📦 <b>Store Database</b>\n\nNo products are currently available.",
                                         reply_markup=await admin_back_kb(), parse_mode='HTML')
            return
        kb = InlineKeyboardMarkup(inline_keyboard=[])
        for p_id, name, category, panel_name, stock, is_active in prods:
            status_dot = "🟢" if int(is_active or 0) else "🔴"
            label = f"{status_dot} [{category or '-'}] {panel_name or '-'} - {name or '-'} (Stock: {int(stock or 0)})"
            if len(label) > 60:
                label = label[:57] + "..."
            kb.inline_keyboard.append([InlineKeyboardButton(text=label,
                                                            callback_data=f"admin_view_p_{int(p_id)}", style="primary")])
        kb.inline_keyboard.append([InlineKeyboardButton(text="🔄 Refresh Products",
                                                        callback_data="admin_manage_prods", style="success")])
        kb.inline_keyboard.append([InlineKeyboardButton(text="Back to Admin",
                                                        callback_data="admin_panel_back",
                                                        icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")])
        await call.answer()
        await call.message.edit_text(
            f"📦 <b>Manage Products</b>\n\nTotal products: <b>{len(prods)}</b>\n"
            "Select a product below to edit, stock-manage, hide/unhide, or delete.",
            reply_markup=kb, parse_mode='HTML')
    except Exception:
        logger.exception("Manage Products failed")
        await call.answer("❌ Manage Products error. Check Railway logs.", show_alert=True)

@dp.callback_query(F.data.startswith("admin_view_p_"))
async def admin_view_product(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("⛔ Admin only.", show_alert=True)
        return
    try:
        p_id = int(call.data.rsplit("_", 1)[1])
        prod = await db_query(
            """SELECT id, category, panel_name, name, price_inr, reseller_price,
                      COALESCE(stock, 0), apk_link, validity, device_limit,
                      COALESCE(is_active, 1), external_product_id,
                      requires_android_id, external_enabled, NULL, external_duration
               FROM products WHERE id=%s""",
            (p_id,), fetchone=True)
        if not prod:
            return await call.answer("❌ Product not found in database.", show_alert=True)

        def esc(v):
            return html.escape(str(v if v is not None else ""))

        price_inr = float(prod[4] or 0.0)
        reseller_price = float(prod[5] or 0.0)
        stock = int(prod[6] or 0)
        external_enabled = bool(prod[13])
        text = (
            "📦 <b><u>PRODUCT DETAILS</u></b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"<b>ID:</b> <code>{prod[0]}</code>\n"
            f"<b>Panel Group:</b> {esc(prod[1])}\n"
            f"<b>Panel Name:</b> {esc(prod[2])}\n"
            f"<b>Package:</b> {esc(prod[3])}\n"
            f"<b>Standard Price:</b> ₹{price_inr:.2f}\n"
            f"👑 <b>Wholesale Price:</b> ₹{reseller_price:.2f}\n"
            f"<b>Vault Stock:</b> {stock}\n"
            f"<b>API Stock Mode:</b> {'♾️ Unlimited (External API)' if external_enabled else '🔢 Manual Stock'}\n"
            f"<b>External Product ID:</b> {esc(prod[11]) if prod[11] else 'Not set'}\n"
            f"<b>API Duration:</b> {esc(prod[15]) if prod[15] else 'Not set'}\n"
            f"<b>Payload Link:</b> {esc(prod[7]) if prod[7] else 'None'}\n"
            f"<b>Time Config:</b> {esc(prod[8])}\n"
            f"<b>HWID Limit:</b> {esc(prod[9])}\n"
            f"<b>Visibility:</b> {'Active' if prod[10] else 'Hidden'}\n"
            "━━━━━━━━━━━━━━━━━━"
        )
        toggle_btn_text = "Hide Product 👁‍🗨" if prod[10] else "Unhide Product 👁"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Edit Panel Group 🏷️", callback_data=f"edit_p_{p_id}_cat",
                                  icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="primary"),
             InlineKeyboardButton(text="Edit Panel Name 🏷️", callback_data=f"edit_p_{p_id}_panel_name",
                                  icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="primary")],
            [InlineKeyboardButton(text="Edit Package Name ✏️", callback_data=f"edit_p_{p_id}_name",
                                  icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="primary")],
            [InlineKeyboardButton(text="Edit Price 💰", callback_data=f"edit_p_{p_id}_price",
                                  icon_custom_emoji_id=await get_emoji_icon("money_icon"), style="primary"),
             InlineKeyboardButton(text="Edit R-Price 👑", callback_data=f"edit_p_{p_id}_rprice",
                                  icon_custom_emoji_id=await get_emoji_icon("money_icon"), style="primary")],
            [InlineKeyboardButton(text="Edit Validity ⏳", callback_data=f"edit_p_{p_id}_validity",
                                  icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="primary"),
             InlineKeyboardButton(text="Edit Device 📱", callback_data=f"edit_p_{p_id}_device",
                                  icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="primary")],
            [InlineKeyboardButton(text="Edit API Duration ⏱️", callback_data=f"edit_p_{p_id}_api_duration",
                                  icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="primary")],
            [InlineKeyboardButton(text="Edit APK Link 🔗", callback_data=f"edit_p_{p_id}_apk",
                                  icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="primary"),
             InlineKeyboardButton(text="Add Keys ➕", callback_data=f"edit_p_{p_id}_keys",
                                  icon_custom_emoji_id=await get_emoji_icon("add_balance"), style="success")],
            [InlineKeyboardButton(text="Add to Stock 📦➕", callback_data=f"edit_p_{p_id}_stock_add",
                                  icon_custom_emoji_id=await get_emoji_icon("add_balance"), style="success")],
            [InlineKeyboardButton(text="Delete Key 🗑", callback_data=f"delkey_p_{p_id}",
                                  icon_custom_emoji_id=await get_emoji_icon("back"), style="danger"),
             InlineKeyboardButton(text=toggle_btn_text, callback_data=f"toggle_p_{p_id}",
                                  icon_custom_emoji_id=await get_emoji_icon("check_icon"), style="primary")],
            [InlineKeyboardButton(text="Delete Product 🗑", callback_data=f"delete_p_{p_id}",
                                  icon_custom_emoji_id=await get_emoji_icon("back"), style="danger"),
             InlineKeyboardButton(text="BACK", callback_data="admin_manage_prods",
                                  icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]
        ])
        await call.answer()
        await call.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True, parse_mode='HTML')
    except Exception:
        logger.exception("Error in admin_view_product")
        await call.answer("❌ Could not load this product. Check Railway logs.", show_alert=True)

@dp.callback_query(F.data.startswith("toggle_p_"))
async def admin_toggle_product(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    try:
        p_id = int(call.data.rsplit("_", 1)[1])
        row = await db_query("SELECT COALESCE(is_active,1) FROM products WHERE id=%s", (p_id,), fetchone=True)
        if not row:
            return await call.answer("❌ Product not found.", show_alert=True)
        new_val = 0 if int(row[0]) == 1 else 1
        await db_query("UPDATE products SET is_active=%s WHERE id=%s", (new_val, p_id))
        await call.answer("✅ Visibility updated!", show_alert=True)
        await admin_view_product(call)
    except Exception:
        logger.exception("Toggle product failed")
        await call.answer("❌ Could not update product visibility.", show_alert=True)

@dp.callback_query(F.data.startswith("edit_p_"))
async def start_edit_product(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    parts = call.data.split("_")
    p_id = int(parts[2])
    field = "_".join(parts[3:])
    await state.update_data(edit_p_id=p_id, edit_field=field)
    if field == 'keys':
        await call.message.edit_text("📥 <b>Vault Injection</b>\nPaste the <b>NEW KEYS</b> to append to the stock (1 key per line):",
                                     reply_markup=await admin_back_kb(), parse_mode='HTML')
        await state.set_state(AdminStates.wait_for_add_keys)
    elif field == 'stock_add':
        await call.message.edit_text("📦 <b>Add to Stock</b>\n\nEnter how many stock units to add.\nExample: <code>100</code> or <code>200</code>",
                                     reply_markup=await admin_back_kb(), parse_mode='HTML')
        await state.set_state(AdminStates.wait_for_new_value)
    else:
        field_name_map = {
            'cat': 'New Panel Group/Category Name', 'panel_name': 'New Panel Name',
            'name': 'New Package/Date Name', 'price': 'New Standard Price in ₹',
            'rprice': 'New Reseller Price in ₹', 'validity': 'New Time Validity String',
            'device': 'New HWID Limit String', 'apk': 'New Payload Link (or type "none")',
            'api_duration': 'Exact XYZ API Duration (e.g. 1 Day)',
            'stock_add': 'Stock units to add'
        }
        await call.message.edit_text(f"✏️ Input the required data for: <b>{field_name_map[field]}</b>",
                                     reply_markup=await admin_back_kb(), parse_mode='HTML')
        await state.set_state(AdminStates.wait_for_new_value)

@dp.message(AdminStates.wait_for_new_value)
async def process_edit_value(m: Message, state: FSMContext):
    data = await state.get_data()
    p_id = data['edit_p_id']
    field = data['edit_field']
    new_val = m.text.strip()

    if field in ['price', 'rprice']:
        try:
            new_val = float(new_val)
        except ValueError:
            return await m.answer("❌ Invalid number format. Please enter a valid price (e.g., 500).")
    elif field == 'apk':
        new_val = "" if new_val.lower() == 'none' else new_val
    elif field == 'stock_add':
        try:
            add_qty = int(new_val)
            if add_qty <= 0 or add_qty > 100000:
                raise ValueError
        except ValueError:
            return await m.answer("❌ Enter a positive whole number up to 100000, e.g. 100 or 200.")
        await db_query("UPDATE products SET stock=COALESCE(stock,0)+%s WHERE id=%s", (add_qty, p_id))
        row = await db_query("SELECT stock FROM products WHERE id=%s", (p_id,), fetchone=True)
        new_stock = row[0] if row else 0
        await m.answer(f"✅ <b>Stock Added!</b>\n\n➕ Added: <code>{add_qty}</code>\n📦 New Stock: <code>{new_stock}</code>",
                       reply_markup=await admin_kb(), parse_mode='HTML')
        await state.clear()
        return

    db_col_map = {'cat': 'category', 'panel_name': 'panel_name', 'name': 'name',
                  'price': 'price_inr', 'rprice': 'reseller_price',
                  'validity': 'validity', 'device': 'device_limit',
                  'apk': 'apk_link', 'api_duration': 'external_duration'}
    if field not in db_col_map:
        return await m.answer("❌ Unsupported product field.")
    await db_query(f"UPDATE products SET {db_col_map[field]}=%s WHERE id=%s", (new_val, p_id))
    await m.answer("✅ <b>Node updated gracefully!</b>", reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.message(AdminStates.wait_for_add_keys)
async def process_add_keys(m: Message, state: FSMContext):
    data = await state.get_data()
    p_id = data['edit_p_id']
    keys = [k.strip() for k in m.text.strip().split('\n') if k.strip()]
    if len(keys) == 0:
        return await m.answer("❌ Protocol breach: Zero valid keys found.", reply_markup=await admin_kb(), parse_mode='HTML')
    for k in keys:
        await db_query("INSERT INTO product_keys (product_id, key_text) VALUES (%s, %s)", (p_id, k))
    await db_query("UPDATE products SET stock = COALESCE(stock,0) + %s WHERE id=%s", (len(keys), p_id))
    await m.answer(f"✅ <b>Vault Secure!</b> {len(keys)} new keys appended and encrypted.",
                   reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data.startswith("delete_p_"))
async def admin_delete_product(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    try:
        p_id = int(call.data.rsplit("_", 1)[1])
        row = await db_query("SELECT id FROM products WHERE id=%s", (p_id,), fetchone=True)
        if not row:
            await call.answer("❌ Product not found.", show_alert=True)
            return
        await db_query("DELETE FROM product_keys WHERE product_id=%s", (p_id,))
        await db_query("DELETE FROM products WHERE id=%s", (p_id,))
        await call.answer("✅ Product and its unused key records deleted.", show_alert=True)
        await admin_manage_prods(call)
    except Exception:
        logger.exception("Delete product failed")
        await call.answer("❌ Could not delete product. Check Railway logs.", show_alert=True)

@dp.callback_query(F.data.startswith("delkey_p_"))
async def admin_delete_key_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    p_id = int(call.data.split("_")[2])
    await state.update_data(del_p_id=p_id)
    await call.message.edit_text("🗑 Send the <b>exact string match</b> of the key you wish to purge from the vault:",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_delete_key)

@dp.message(AdminStates.wait_for_delete_key)
async def process_delete_key(m: Message, state: FSMContext):
    data = await state.get_data()
    p_id = data['del_p_id']
    key_to_delete = m.text.strip()
    key_data = await db_query("SELECT id, is_used FROM product_keys WHERE product_id=%s AND key_text=%s",
                              (p_id, key_to_delete), fetchone=True)
    if not key_data:
        return await m.answer("❌ Key not found. Check logs and try again.",
                              reply_markup=await admin_back_kb(), parse_mode='HTML')
    if key_data[1] == 1:
        return await m.answer("⚠️ Action Blocked: This key has already been dispatched to a user.",
                              reply_markup=await admin_back_kb(), parse_mode='HTML')
    await db_query("DELETE FROM product_keys WHERE id=%s", (key_data[0],))
    await db_query("UPDATE products SET stock = stock - 1 WHERE id=%s", (p_id,))
    await m.answer(f"✅ Key <code>{key_to_delete}</code> securely purged from vault.\n📦 Database indices updated.",
                   reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

# ==============================================================================
# 20. ADMIN TICKETS, BROADCAST, COUPONS
# ==============================================================================
@dp.callback_query(F.data == "admin_view_tickets")
async def admin_view_tickets(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    tickets = await db_query("SELECT id, user_id, message, created_at FROM tickets WHERE status='Open' LIMIT 1", fetchall=True)
    if not tickets:
        return await call.answer("✅ Zero pending issues. Grid is clean!", show_alert=True)
    t = tickets[0]
    text = (f"🎫 <b><u>ACTIVE TICKET #{t[0]}</u></b>\n👤 <b>Origin UID:</b> <code>{t[1]}</code>\n"
            f"📅 <b>Timestamp:</b> {t[3]}\n\n📝 <b>Payload:</b>\n{t[2]}")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Formulate Reply", callback_data=f"reply_ticket_{t[0]}_{t[1]}",
                              icon_custom_emoji_id=await get_emoji_icon("telegram"), style="primary")],
        [InlineKeyboardButton(text="❌ Force Close Ticket", callback_data=f"close_ticket_{t[0]}",
                              icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")],
        [InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back",
                              icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("close_ticket_"))
async def close_ticket(call: CallbackQuery):
    ticket_id = call.data.split("_")[2]
    await db_query("UPDATE tickets SET status='Closed' WHERE id=%s", (ticket_id,))
    await call.answer("✅ Status set to Closed.", show_alert=True)
    await admin_view_tickets(call)

@dp.callback_query(F.data.startswith("reply_ticket_"))
async def reply_ticket_start(call: CallbackQuery, state: FSMContext):
    data = call.data.split("_")
    ticket_id, user_id = data[2], data[3]
    await state.update_data(ticket_id=ticket_id, user_id=user_id)
    await call.message.edit_text(f"💬 Formulating reply for node <code>{user_id}</code>.\n\nType your message payload:",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.ticket_reply_msg)

@dp.message(AdminStates.ticket_reply_msg)
async def send_ticket_reply(m: Message, state: FSMContext):
    data = await state.get_data()
    try:
        await bot.send_message(data['user_id'], f"📞 <b>Admin Reply (Ref #{data['ticket_id']}):</b>\n\n{m.text}", parse_mode='HTML')
        await db_query("UPDATE tickets SET status='Closed' WHERE id=%s", (data['ticket_id'],))
        await m.answer("✅ Payload delivered and connection closed successfully.",
                       reply_markup=await admin_kb(), parse_mode='HTML')
    except Exception as e:
        await m.answer(f"❌ Transmission Error: {e}", reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_broadcast_btn")
async def admin_broadcast_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    await call.message.edit_text("📢 <b>Mass Broadcast Protocol</b>\n\nSend the rich message payload you wish to transmit globally across the grid:",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.broadcast_msg)

@dp.message(AdminStates.broadcast_msg)
async def admin_broadcast_send(message: Message, state: FSMContext):
    users = await db_query("SELECT user_id FROM users", fetchall=True)
    sent, failed = 0, 0
    m = await message.answer("⏳ Broadcast protocol initiated... Do not interrupt.", parse_mode='HTML')
    for u in users:
        try:
            await message.send_copy(chat_id=u[0])
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.06)
    await m.edit_text(f"✅ <b>Global Broadcast Complete!</b>\n\n🟢 Nodes reached: {sent}\n🔴 Nodes failed/blocked: {failed}",
                      reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_create_coupon")
async def admin_create_coupon_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    await call.message.edit_text("🎟 Enter a highly secure alphanumeric sequence for the Promo Code:",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.add_coupon_code)

@dp.message(AdminStates.add_coupon_code)
async def admin_coupon_code(m: Message, state: FSMContext):
    await state.update_data(code=m.text.strip().upper())
    await m.answer("💰 Enter the monetary reward payload in <b>RUPEES (₹)</b>:", parse_mode='HTML')
    await state.set_state(AdminStates.add_coupon_amount)

@dp.message(AdminStates.add_coupon_amount)
async def admin_coupon_amount(m: Message, state: FSMContext):
    try:
        await state.update_data(amount=float(m.text))
        await m.answer("👥 Enter the exact maximum threshold uses for this code:", parse_mode='HTML')
        await state.set_state(AdminStates.add_coupon_uses)
    except ValueError:
        await m.answer("❌ Non-numerical data detected. Aborting.")

@dp.message(AdminStates.add_coupon_uses)
async def admin_coupon_uses(m: Message, state: FSMContext):
    try:
        uses = int(m.text)
        data = await state.get_data()
        await db_query("REPLACE INTO coupons (code, amount, uses_left) VALUES (%s, %s, %s)",
                       (data['code'], data['amount'], uses))
        await m.answer(f"✅ Protocol <b>{data['code']}</b> encoded!\nReward Vector: {fmt_curr(data['amount'])}\nThreshold Limit: {uses} executions.",
                       reply_markup=await admin_kb(), parse_mode='HTML')
        await state.clear()
    except ValueError:
        await m.answer("❌ Non-numerical data detected. Aborting.")

# ==============================================================================
# 21. ADMIN RESELLER & SPIN SETTINGS
# ==============================================================================
@dp.callback_query(F.data == "admin_reseller_menu")
async def admin_reseller_menu(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    status_check = await db_query("SELECT value FROM settings WHERE `key`='reseller_system_status'", fetchone=True)
    sys_status = status_check[0] if status_check else "ON"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Grant Reseller Rights", callback_data="reseller_make",
                              icon_custom_emoji_id=await get_emoji_icon("reseller"), style="success"),
         InlineKeyboardButton(text="➖ Revoke Reseller", callback_data="reseller_remove",
                              icon_custom_emoji_id=await get_emoji_icon("reseller"), style="danger")],
        [InlineKeyboardButton(text="📋 Audit Active Resellers", callback_data="reseller_view",
                              icon_custom_emoji_id=await get_emoji_icon("history"), style="primary")],
        [InlineKeyboardButton(text=f"{'🟢' if sys_status == 'ON' else '🔴'} Auto-Upgrade System: {sys_status}",
                              callback_data="admin_toggle_reseller_sys",
                              icon_custom_emoji_id=await get_emoji_icon("check_icon"),
                              style="success" if sys_status == 'ON' else "danger")],
        [InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back",
                              icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]])
    await call.message.edit_text("👑 <b>Wholesale Reseller Protocols</b>\nSelect administrative action:",
                                 reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "admin_toggle_reseller_sys")
async def toggle_reseller_sys(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    res = await db_query("SELECT value FROM settings WHERE `key`='reseller_system_status'", fetchone=True)
    current = res[0] if res else 'ON'
    new_status = 'OFF' if current == 'ON' else 'ON'
    await db_query("REPLACE INTO settings (`key`, value) VALUES ('reseller_system_status', %s)", (new_status,))
    await admin_reseller_menu(call)

@dp.callback_query(F.data.in_(["reseller_make", "reseller_remove"]))
async def reseller_prompt_id(call: CallbackQuery, state: FSMContext):
    action = call.data
    await state.update_data(reseller_action=action)
    await call.message.edit_text("👤 Identify target node. Input <b>User ID</b> or <b>@username</b>:",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.reseller_manage_id)

@dp.message(AdminStates.reseller_manage_id)
async def process_reseller_manage(m: Message, state: FSMContext):
    data = await state.get_data()
    target = m.text.strip()
    if target.startswith('@'):
        target = target[1:]
    user_q = await db_query("SELECT user_id, first_name FROM users WHERE user_id=%s OR username=%s",
                            (target, target), fetchone=True)
    if not user_q:
        return await m.answer("❌ Target completely ghosted. Not in database.",
                              reply_markup=await admin_back_kb(), parse_mode='HTML')
    u_id, u_name = user_q[0], user_q[1]
    if data['reseller_action'] == "reseller_make":
        await db_query("UPDATE users SET is_reseller=1, reseller_since=%s, account_type='Reseller' WHERE user_id=%s",
                       (datetime.now().strftime("%Y-%m-%d"), u_id))
        await m.answer(f"✅ Credentials upgraded. <b>{u_name}</b> (<code>{u_id}</code>) has reseller rights.",
                       reply_markup=await admin_kb(), parse_mode='HTML')
    else:
        await db_query("UPDATE users SET is_reseller=0, account_type='Regular' WHERE user_id=%s", (u_id,))
        await m.answer(f"✅ Credentials revoked. <b>{u_name}</b> (<code>{u_id}</code>) is back to regular user.",
                       reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "reseller_view")
async def reseller_view(call: CallbackQuery):
    resellers = await db_query("SELECT user_id, first_name, username FROM users WHERE is_reseller=1", fetchall=True)
    if not resellers:
        return await call.message.edit_text("📋 Zero active resellers found.",
                                            reply_markup=await admin_back_kb(), parse_mode='HTML')
    text = "👑 <b><u>ACTIVE RESELLER AUDIT LOG</u></b> 👑\n━━━━━━━━━━━━━━━━━━\n"
    for r in resellers:
        uname = f"(@{r[2]})" if r[2] else ""
        text += f"👤 {r[1]} {uname}\n🆔 <code>{r[0]}</code>\n\n"
    await call.message.edit_text(text, reply_markup=await admin_back_kb(), parse_mode='HTML')

@dp.callback_query(F.data == "admin_spin_menu")
async def admin_spin_menu(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    status = await db_query("SELECT value FROM settings WHERE `key`='spin_status'", fetchone=True)
    limit = await db_query("SELECT value FROM settings WHERE `key`='daily_spin_limit'", fetchone=True)
    status_val = status[0] if status else 'ON'
    limit_val = limit[0] if limit else '50.0'
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Append Reward Logic", callback_data="spin_add",
                              icon_custom_emoji_id=await get_emoji_icon("add_balance"), style="success"),
         InlineKeyboardButton(text="❌ Drop Reward Logic", callback_data="spin_del",
                              icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")],
        [InlineKeyboardButton(text="📋 Audit Configs", callback_data="spin_view",
                              icon_custom_emoji_id=await get_emoji_icon("history"), style="primary"),
         InlineKeyboardButton(text="⚙️ Throttle Limits", callback_data="spin_limit",
                              icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="primary")],
        [InlineKeyboardButton(text=f"{'🟢' if status_val == 'ON' else '🔴'} Master Toggle: {status_val}",
                              callback_data="spin_toggle",
                              icon_custom_emoji_id=await get_emoji_icon("check_icon"),
                              style="success" if status_val == 'ON' else "danger")],
        [InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back",
                              icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]])
    await call.message.edit_text(f"🎰 <b>Advanced Ludo/Spin Algorithms</b>\nCurrent Threshold: ₹{limit_val}",
                                 reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "spin_toggle")
async def spin_toggle(call: CallbackQuery):
    res = await db_query("SELECT value FROM settings WHERE `key`='spin_status'", fetchone=True)
    current = res[0] if res else 'ON'
    new_status = 'OFF' if current == 'ON' else 'ON'
    await db_query("REPLACE INTO settings (`key`, value) VALUES ('spin_status', %s)", (new_status,))
    await admin_spin_menu(call)

@dp.callback_query(F.data == "admin_toggle_bot")
async def toggle_bot(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    res = await db_query("SELECT value FROM settings WHERE `key`='bot_status'", fetchone=True)
    current = res[0] if res else 'ON'
    new_status = 'OFF' if current == 'ON' else 'ON'
    await db_query("REPLACE INTO settings (`key`, value) VALUES ('bot_status', %s)", (new_status,))
    await call.message.edit_reply_markup(reply_markup=await admin_kb())

@dp.callback_query(F.data == "spin_add")
async def spin_add_start(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("🎰 Inject new decimal logic limit (e.g. 15.50):",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.spin_add_reward)

@dp.message(AdminStates.spin_add_reward)
async def spin_add_exec(m: Message, state: FSMContext):
    try:
        amt = float(m.text)
        await db_query("INSERT INTO spin_rewards (amount) VALUES (%s)", (amt,))
        await m.answer(f"✅ Algorithm updated. New vector {fmt_curr(amt)} injected.",
                       reply_markup=await admin_kb(), parse_mode='HTML')
        await state.clear()
    except ValueError:
        await m.answer("❌ Math parsing error.")

@dp.callback_query(F.data == "spin_view")
async def spin_view(call: CallbackQuery):
    rewards = await db_query("SELECT amount FROM spin_rewards ORDER BY amount ASC", fetchall=True)
    text = "🎰 <b>Live Ludo Constants</b>\n\n"
    for r in rewards:
        text += f"🎁 {fmt_curr(r[0])}\n"
    await call.message.edit_text(text, reply_markup=await admin_back_kb(), parse_mode='HTML')

@dp.callback_query(F.data == "admin_set_video")
async def admin_set_video_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    await call.message.edit_text("📹 Input direct streaming / YouTube Link for Tutorial system:\n<i>(Or type 'None' to clear registry):</i>",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_howto_video)

@dp.message(AdminStates.wait_for_howto_video)
async def exec_set_video(m: Message, state: FSMContext):
    link = m.text.strip()
    await db_query("REPLACE INTO settings (`key`, value) VALUES ('how_to_video', %s)", (link,))
    await m.answer("✅ Routing complete. Video linked.", reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_set_all_files")
async def admin_set_all_files_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    await call.message.edit_text("🔗 Input the direct Channel / Cloud URL for 'Download Files' button:\n<i>(Or type 'None' to format data):</i>",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_all_files_link)

@dp.message(AdminStates.wait_for_all_files_link)
async def exec_set_all_files(m: Message, state: FSMContext):
    link = m.text.strip()
    await db_query("REPLACE INTO settings (`key`, value) VALUES ('all_files_link', %s)", (link,))
    await m.answer("✅ Global resource variable updated.", reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_edit_emojis")
async def admin_edit_emojis(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    rows = await db_query("SELECT `key`, value FROM settings WHERE `key` LIKE 'emoji_%%' ORDER BY `key`", fetchall=True)
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for row in rows:
        key = row[0]
        slot = key.replace("emoji_", "")
        current_id = row[1] if row[1] else "Not set"
        kb.inline_keyboard.append([InlineKeyboardButton(text=f"{slot} (ID: {current_id})",
                                                        callback_data=f"edit_emoji_{slot}", style="primary")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back",
                                                    icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")])
    await call.message.edit_text("🎨 <b>Edit All Emojis</b>\nChoose an emoji slot to change its ID:",
                                 reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("edit_emoji_"))
async def admin_edit_emoji_prompt(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    slot = call.data.split("edit_emoji_", 1)[1]
    await state.update_data(emoji_slot=slot)
    current = await get_setting(f"emoji_{slot}", "Not set")
    await call.message.edit_text(f"✏️ Enter new emoji ID for <b>{slot}</b>:\nCurrent: {current}\n(Leave empty to reset to default)",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_emoji_slot)

@dp.message(AdminStates.wait_for_emoji_slot)
async def save_emoji_slot(m: Message, state: FSMContext):
    data = await state.get_data()
    slot = data['emoji_slot']
    new_id = m.text.strip()
    if new_id == "":
        await db_query("DELETE FROM settings WHERE `key`=%s", (f"emoji_{slot}",))
        await m.answer(f"✅ Reset emoji for '{slot}' to default.", reply_markup=await admin_kb(), parse_mode='HTML')
    else:
        if not new_id.isdigit():
            await m.answer("❌ Invalid ID! Must be numeric.", reply_markup=await admin_kb(), parse_mode='HTML')
            return
        await set_setting(f"emoji_{slot}", new_id)
        await m.answer(f"✅ Emoji for '{slot}' updated to ID {new_id}.", reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_edit_ui_menu")
async def admin_edit_ui_menu(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Edit Start Menu Text", callback_data="edit_ui_start",
                              icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="primary")],
        [InlineKeyboardButton(text="Edit Download Files Text", callback_data="edit_ui_download",
                              icon_custom_emoji_id=await get_emoji_icon("download"), style="primary")],
        [InlineKeyboardButton(text="Edit VIP Menu Text", callback_data="edit_ui_vip",
                              icon_custom_emoji_id=await get_emoji_icon("vip"), style="primary")],
        [InlineKeyboardButton(text="Edit Lucky Dice Text", callback_data="edit_ui_dice",
                              icon_custom_emoji_id=await get_emoji_icon("ludo_spin"), style="primary")],
        [InlineKeyboardButton(text="Edit Add Balance Text", callback_data="edit_ui_add_balance",
                              icon_custom_emoji_id=await get_emoji_icon("add_balance"), style="primary")],
        [InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back",
                              icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]])
    await call.message.edit_text("✏️ <b>Edit User Interface Texts</b>\nSelect which text you want to modify:",
                                 reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("edit_ui_"))
async def admin_edit_ui_prompt(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    ui_key = call.data.split("_")[2]
    await state.update_data(ui_key=ui_key)
    current_text = await get_ui_text(ui_key)
    await call.message.edit_text(f"📝 Send the new text for <b>{ui_key.upper()}</b> menu.\n\nCurrent text:\n{current_text}",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.edit_ui_text)

@dp.message(AdminStates.edit_ui_text)
async def admin_save_ui_text(m: Message, state: FSMContext):
    data = await state.get_data()
    ui_key = data['ui_key']
    new_text = m.text
    await db_query("REPLACE INTO settings (`key`, value) VALUES (%s, %s)", (f"ui_{ui_key}", new_text))
    await m.answer(f"✅ UI text <b>{ui_key}</b> updated successfully!", reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_edit_reseller_price")
async def admin_edit_reseller_price_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    prods = await db_query("SELECT id, name, category, panel_name, reseller_price FROM products", fetchall=True)
    prods = sorted(prods or [], key=lambda row: (natural_sort_key(row[2]), natural_sort_key(row[3]), natural_sort_key(row[1])))
    if not prods:
        return await call.message.edit_text("No products to edit.", reply_markup=await admin_back_kb(), parse_mode='HTML')
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for p in prods:
        panel_name = p[3] if p[3] is not None else ""
        r_price = float(p[4]) if p[4] is not None else 0.0
        kb.inline_keyboard.append([InlineKeyboardButton(text=f"{p[2]} - {panel_name} - {p[1]} (₹{r_price:.2f})",
                                                        callback_data=f"edit_reseller_{p[0]}",
                                                        icon_custom_emoji_id=await get_emoji_icon("money_icon"), style="primary")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back",
                                                    icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")])
    await call.message.edit_text("👑 <b>Edit Reseller Price per Product</b>\nSelect a product to change its wholesale price:",
                                 reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("edit_reseller_"))
async def admin_edit_reseller_price_prompt(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    prod_id = int(call.data.split("_")[2])
    await state.update_data(edit_reseller_prod_id=prod_id)
    await call.message.edit_text("💰 Enter the new <b>Reseller Price</b> in Rupees (₹) for this product:",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.edit_reseller_price)

@dp.message(AdminStates.edit_reseller_price)
async def admin_save_reseller_price(m: Message, state: FSMContext):
    try:
        new_price = float(m.text)
        data = await state.get_data()
        prod_id = data['edit_reseller_prod_id']
        await db_query("UPDATE products SET reseller_price=%s WHERE id=%s", (new_price, prod_id))
        await m.answer(f"✅ Reseller price updated to {fmt_curr(new_price)} for product ID {prod_id}.",
                       reply_markup=await admin_kb(), parse_mode='HTML')
        await state.clear()
    except ValueError:
        await m.answer("❌ Invalid number. Please enter a valid price.")

@dp.callback_query(F.data == "admin_set_reseller_fee")
async def admin_set_reseller_fee(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    cur = await get_setting("reseller_setup_fee", "200.0")
    await call.message.edit_text(f"💰 Enter the new <b>Reseller Setup Fee</b> in Rupees (₹):\nCurrent: {cur}",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_reseller_setup_fee)

@dp.message(AdminStates.wait_for_reseller_setup_fee)
async def admin_save_reseller_fee(m: Message, state: FSMContext):
    try:
        fee = float(m.text)
        await set_setting("reseller_setup_fee", str(fee))
        await m.answer(f"✅ Reseller setup fee updated to {fmt_curr(fee)}.", reply_markup=await admin_kb(), parse_mode='HTML')
        await state.clear()
    except ValueError:
        await m.answer("❌ Invalid number. Please enter a valid amount.")

@dp.callback_query(F.data == "admin_set_reseller_min")
async def admin_set_reseller_min(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    cur = await get_setting("reseller_min_balance", "500.0")
    await call.message.edit_text(f"💳 Enter the new <b>Minimum Balance</b> required to become reseller (₹):\nCurrent: {cur}",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_reseller_min_balance)

@dp.message(AdminStates.wait_for_reseller_min_balance)
async def admin_save_reseller_min(m: Message, state: FSMContext):
    try:
        min_bal = float(m.text)
        await set_setting("reseller_min_balance", str(min_bal))
        await m.answer(f"✅ Minimum reseller balance updated to {fmt_curr(min_bal)}.",
                       reply_markup=await admin_kb(), parse_mode='HTML')
        await state.clear()
    except ValueError:
        await m.answer("❌ Invalid number. Please enter a valid amount.")

@dp.callback_query(F.data == "admin_set_support_links")
async def admin_set_support_links(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📞 Set Telegram Link", callback_data="admin_set_telegram",
                              icon_custom_emoji_id=await get_emoji_icon("telegram"), style="primary")],
        [InlineKeyboardButton(text="📱 Set WhatsApp Link", callback_data="admin_set_whatsapp",
                              icon_custom_emoji_id=await get_emoji_icon("whatsapp"), style="primary")],
        [InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back",
                              icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]])
    await call.message.edit_text("📌 <b>Support Contact Links</b>\nSet the URLs for Telegram and WhatsApp support:",
                                 reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "admin_set_telegram")
async def admin_set_telegram(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    await call.message.edit_text("✈️ Enter the Telegram contact URL (e.g., https://t.me/YOUR_SUPPORT):",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_support_telegram)

@dp.message(AdminStates.wait_for_support_telegram)
async def save_telegram_link(m: Message, state: FSMContext):
    link = m.text.strip()
    await set_setting("support_telegram", link)
    await m.answer("✅ Telegram support link updated!", reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_set_whatsapp")
async def admin_set_whatsapp(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    await call.message.edit_text("📱 Enter the WhatsApp contact URL (e.g., https://wa.me/1234567890):",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_support_whatsapp)

@dp.message(AdminStates.wait_for_support_whatsapp)
async def save_whatsapp_link(m: Message, state: FSMContext):
    link = m.text.strip()
    await set_setting("support_whatsapp", link)
    await m.answer("✅ WhatsApp support link updated!", reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_set_category_emojis")
async def admin_set_category_emojis(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for cat in FIXED_CATEGORIES:
        current = await get_setting(f"cat_emoji_{cat}", "Not set")
        kb.inline_keyboard.append([InlineKeyboardButton(text=f"{cat} (ID: {current})",
                                                        callback_data=f"set_cat_emoji_{cat}",
                                                        icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="primary")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back",
                                                    icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")])
    await call.message.edit_text("🎨 <b>Set Category Emojis</b>\nChoose a category to set its custom emoji ID:",
                                 reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("set_cat_emoji_"))
async def admin_set_category_emoji_prompt(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    category = call.data.split("set_cat_emoji_", 1)[1]
    await state.update_data(cat_emoji_category=category)
    await call.message.edit_text(f"🎨 Enter the emoji ID for <b>{category}</b>:\n(Leave empty to reset to default)",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_category_emoji)

@dp.message(AdminStates.wait_for_category_emoji)
async def save_category_emoji(m: Message, state: FSMContext):
    data = await state.get_data()
    category = data['cat_emoji_category']
    emoji_id = m.text.strip()
    if emoji_id == "":
        await db_query("DELETE FROM settings WHERE `key`=%s", (f"cat_emoji_{category}",))
        await m.answer(f"✅ Reset emoji for {category} to default.", reply_markup=await admin_kb(), parse_mode='HTML')
    else:
        if not emoji_id.isdigit():
            await m.answer("❌ Invalid ID! Must be numeric.", reply_markup=await admin_kb(), parse_mode='HTML')
            return
        await set_setting(f"cat_emoji_{category}", emoji_id)
        await m.answer(f"✅ Emoji set for {category} successfully!", reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_set_panel_emojis")
async def admin_set_panel_emojis(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    panels = await db_query("SELECT DISTINCT panel_name FROM products WHERE panel_name != '' ORDER BY panel_name", fetchall=True)
    if not panels:
        await call.message.edit_text("No panel names found in products.",
                                     reply_markup=await admin_back_kb(), parse_mode='HTML')
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for p in panels:
        panel = p[0]
        current = await get_setting(f"panel_emoji_{panel}", "Not set")
        kb.inline_keyboard.append([InlineKeyboardButton(text=f"{panel} (ID: {current})",
                                                        callback_data=f"set_panel_emoji_{panel}",
                                                        icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="primary")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back",
                                                    icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")])
    await call.message.edit_text("🖼 <b>Set Panel Emojis</b>\nChoose a panel name to set its custom emoji ID:",
                                 reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("set_panel_emoji_"))
async def admin_set_panel_emoji_prompt(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    panel_name = call.data.split("set_panel_emoji_", 1)[1]
    await state.update_data(panel_emoji_name=panel_name)
    await call.message.edit_text(f"🎨 Enter the emoji ID for panel <b>{panel_name}</b>:\n(Leave empty to reset to default)",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_panel_emoji_id)

@dp.message(AdminStates.wait_for_panel_emoji_id)
async def save_panel_emoji(m: Message, state: FSMContext):
    data = await state.get_data()
    panel_name = data['panel_emoji_name']
    emoji_id = m.text.strip()
    if emoji_id == "":
        await db_query("DELETE FROM settings WHERE `key`=%s", (f"panel_emoji_{panel_name}",))
        await m.answer(f"✅ Reset emoji for panel '{panel_name}'.", reply_markup=await admin_kb(), parse_mode='HTML')
    else:
        if not emoji_id.isdigit():
            await m.answer("❌ Invalid ID! Must be numeric.", reply_markup=await admin_kb(), parse_mode='HTML')
            return
        await set_setting(f"panel_emoji_{panel_name}", emoji_id)
        await m.answer(f"✅ Emoji set for panel '{panel_name}'!", reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_setup_pay0")
async def setup_pay0_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    cur = await get_setting("pay0_api", "")
    mask = (cur[:4] + "••••" + cur[-4:]) if len(cur) > 8 else ("Configured" if cur else "Not set")
    await call.message.edit_text(
        f"⚙️ <b>PAY0 SECURITY DEPLOYMENT</b>\n\nCurrent API Token: <code>{mask}</code>\n\n"
        "Input new master <b>API Token (user_token)</b>:\n<i>(Type /cancel to abort sequence)</i>",
        reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_pay0_api)

@dp.message(AdminStates.wait_for_pay0_api)
async def pay0_api(m: Message, state: FSMContext):
    if m.text == '/cancel':
        await state.clear()
        return await m.answer("Sequence killed.", reply_markup=await admin_kb(), parse_mode='HTML')
    await db_query("REPLACE INTO settings (`key`, value) VALUES ('pay0_api', %s)", (m.text.strip(),))
    await m.answer("✅ <b>Keys synchronized with Pay0 backbone.</b>", reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_setup_binance")
async def setup_binance_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    await call.message.edit_text("🪙 <b>CRYPTO NODE INIT: Step 1/3</b>\nInput Master <b>Binance API Key</b>:\n<i>(Type /cancel to halt protocol)</i>",
                                 reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_binance_api)

@dp.message(AdminStates.wait_for_binance_api)
async def setup_binance_api(m: Message, state: FSMContext):
    if m.text == '/cancel':
        await state.clear()
        return await m.answer("Sequence aborted.", reply_markup=await admin_kb(), parse_mode='HTML')
    await db_query("REPLACE INTO settings (`key`, value) VALUES ('binance_api', %s)", (m.text.strip(),))
    await m.answer("🪙 <b>CRYPTO NODE INIT: Step 2/3</b>\nNow inject the highly secure <b>Binance Secret Key</b>:", parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_binance_secret)

@dp.message(AdminStates.wait_for_binance_secret)
async def setup_binance_secret(m: Message, state: FSMContext):
    await db_query("REPLACE INTO settings (`key`, value) VALUES ('binance_secret', %s)", (m.text.strip(),))
    await m.answer("🪙 <b>CRYPTO NODE INIT: Step 3/3</b>\nFinal variable: Set the public <b>USDT Deposit Address (TRC20/BEP20)</b>\nUsers will broadcast to this ledger:",
                   parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_binance_address)

@dp.message(AdminStates.wait_for_binance_address)
async def setup_binance_address(m: Message, state: FSMContext):
    await db_query("REPLACE INTO settings (`key`, value) VALUES ('binance_address', %s)", (m.text.strip(),))
    await m.answer("✅ <b>Blockchain node synchronized.</b> Crypto gateway is fully armed.",
                   reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

# ==============================================================================
# 22. EXTERNAL KEY GENERATION API
# ==============================================================================
def normalize_api_duration(duration: str) -> str:
    value = str(duration or "").strip()
    if not value:
        return value
    value = re.sub(r"\s+", " ", value)

    duration_map = {
        "1 hour": "1 Hours", "1 hours": "1 Hours", "1hr": "1 Hours", "1h": "1 Hours",
        "2 hour": "2 Hours", "2 hours": "2 Hours", "2hr": "2 Hours",
        "3 hour": "3 Hours", "3 hours": "3 Hours", "3hr": "3 Hours",
        "6 hour": "6 Hours", "6 hours": "6 Hours", "6hr": "6 Hours",
        "12 hour": "12 Hours", "12 hours": "12 Hours", "12hr": "12 Hours",
        "1 day": "1 DaYs", "1 days": "1 DaYs", "1d": "1 DaYs",
        "2 day": "2 DaYs", "2 days": "2 DaYs", "2d": "2 DaYs",
        "3 day": "3 DaYs", "3 days": "3 DaYs", "3d": "3 DaYs",
        "5 day": "5 DaYs", "5 days": "5 DaYs", "5d": "5 DaYs",
        "7 day": "7 DaYs", "7 days": "7 DaYs", "7d": "7 DaYs",
    }
    lower_val = value.lower()
    for pattern, result in duration_map.items():
        if lower_val == pattern.lower():
            return result
    if value in ["1 Hours", "2 Hours", "3 Hours", "6 Hours", "12 Hours",
                 "1 DaYs", "2 DaYs", "3 DaYs", "5 DaYs", "7 DaYs"]:
        return value
    match = re.match(r"(\d+)\s*(hour|hours|hr|hrs|h|day|days|d)", value, re.IGNORECASE)
    if match:
        number = match.group(1)
        unit = match.group(2).lower()
        if unit in ["hour", "hours", "hr", "hrs", "h"]:
            return f"{number} Hours"
        if unit in ["day", "days", "d"]:
            return f"{number} DaYs"
    if value.isdigit():
        return f"{value} Hours"
    return value

async def fetch_external_key(product_id: str, duration: str, android_id: str = "") -> dict:
    url = (await get_setting("external_api_url", "https://adminpanels.shop/api/reseller_v1.php")).strip()
    api_key = (await get_setting("external_api_key", "")).strip()
    master_key = (await get_setting("external_master_key", "")).strip()
    if not url or not api_key:
        return {"status": "error", "msg": "External API configuration is incomplete"}
    product_id = str(product_id or "").strip()
    duration = str(duration or "").strip()
    if not product_id or not duration:
        return {"status": "error", "msg": "External API product/duration is empty"}
    data = {"api_key": api_key, "action": "buy", "product_id": product_id, "duration": duration}
    if android_id:
        data["android_id"] = str(android_id).strip()
    headers = {"Content-Type": "application/x-www-form-urlencoded",
               "Accept": "application/json, text/plain, */*"}
    if master_key:
        headers["x-master-key"] = master_key
    timeout = aiohttp.ClientTimeout(total=15, connect=5, sock_connect=5, sock_read=10)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, data=data, headers=headers, allow_redirects=True) as resp:
                raw = await resp.text()
                logger.info("External API BUY HTTP=%s body=%s", resp.status, raw[:1000])
                if resp.status != 200:
                    return {"status": "error", "msg": f"HTTP {resp.status}: {raw[:500]}"}
                try:
                    result = json.loads(raw)
                except json.JSONDecodeError:
                    return {"status": "error", "msg": f"API returned invalid JSON: {raw[:300]}"}
                return result if isinstance(result, dict) else {"status": "error", "msg": "API returned invalid response"}
    except asyncio.TimeoutError:
        return {"status": "error", "msg": "API timed out. No automatic retry was made to avoid duplicate key generation."}
    except aiohttp.ClientError as exc:
        return {"status": "error", "msg": f"API connection failed: {exc}"}
    except Exception as exc:
        logger.exception("API unexpected error")
        return {"status": "error", "msg": f"API unexpected error: {exc}"}

@dp.callback_query(F.data == "admin_setup_external_api")
async def admin_setup_external_api(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    url = await get_setting("external_api_url", "")
    key = await get_setting("external_api_key", "")
    master = await get_setting("external_master_key", "")
    mask = lambda x: (x[:4] + "••••" + x[-4:]) if len(x) > 8 else ("Configured" if x else "Not set")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Set API URL", callback_data="admin_set_ext_url",
                              icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="primary")],
        [InlineKeyboardButton(text="Set API Key", callback_data="admin_set_ext_key",
                              icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="primary")],
        [InlineKeyboardButton(text="Set Master Key", callback_data="admin_set_ext_master",
                              icon_custom_emoji_id=await get_emoji_icon("info_icon"), style="primary")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="admin_panel_back",
                              icon_custom_emoji_id=await get_emoji_icon("back"), style="danger")]])
    text = ("🔗 <b>External Key API Configuration</b>\n\n"
            f"URL: <code>{url or 'Not set'}</code>\n"
            f"API Key: <code>{mask(key)}</code>\n"
            f"Master Key: <code>{mask(master)}</code>\n\n"
            "Products can be switched to API generation from the Add Product flow.")
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "admin_set_ext_url")
async def set_ext_url(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    await call.message.edit_text("Enter External API URL:", reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_ext_url)

@dp.message(AdminStates.wait_for_ext_url)
async def save_ext_url(m: Message, state: FSMContext):
    value = m.text.strip()
    if not value.startswith(("http://", "https://")):
        return await m.answer("❌ URL must start with http:// or https://")
    await set_setting("external_api_url", value)
    await m.answer("✅ External API URL saved.", reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_set_ext_key")
async def set_ext_key(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    await call.message.edit_text("Enter External API Key:", reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_ext_key)

@dp.message(AdminStates.wait_for_ext_key)
async def save_ext_key(m: Message, state: FSMContext):
    await set_setting("external_api_key", m.text.strip())
    await m.answer("✅ External API Key saved.", reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_set_ext_master")
async def set_ext_master(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    await call.message.edit_text("Enter External API Master Key:", reply_markup=await admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_ext_master)

@dp.message(AdminStates.wait_for_ext_master)
async def save_ext_master(m: Message, state: FSMContext):
    await set_setting("external_master_key", m.text.strip())
    await m.answer("✅ External API Master Key saved.", reply_markup=await admin_kb(), parse_mode='HTML')
    await state.clear()

# ==============================================================================
# 23. BOOTSTRAPPING & MAIN
# ==============================================================================
async def main() -> None:
    logger.info("🔌 Connecting to MySQL database...")
    await init_mysql_pool()
    await init_db()
    logger.info("Initializing DB structure...")
    await migrate_categories()
    asyncio.create_task(auto_verify_task())
    logger.info("Pay0 Auto-Verifier Daemon Running in Background.")
    logger.info("🚀 CORE SYSTEM IS FULLY OPERATIONAL...")
    try:
        await dp.start_polling(bot)
    except Exception as err:
        logger.error(f"Critical System Failure in Polling: {err}")
    finally:
        await bot.session.close()
        await close_mysql_pool()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("System shutting down gracefully. Goodbye.")
