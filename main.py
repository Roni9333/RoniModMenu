import os
import asyncio
import sqlite3
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
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BOT_USERNAME = "@RoniModMenuBot"
ADMIN_ID = int(os.getenv("ADMIN_ID", "7191428925"))
ADMIN_CONTACT = "@RoniModMenu"

# Railway-safe SQLite path. Set DB_PATH=/data/yp_shop.db when using a Railway Volume.
DB_PATH = os.getenv("DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "yp_shop.db"))
DB_DIR = os.path.dirname(os.path.abspath(DB_PATH))
if DB_DIR:
    os.makedirs(DB_DIR, exist_ok=True)

USDT_TO_INR = 90.0
VIP_DISCOUNT_PERCENTAGE = 15.0
VIP_PRICE_INR = 299.0

WELCOME_STICKER_ID = "CAACAgIAAxkBAAEU-WZmH_..."  # Replace with your sticker ID
SPIN_DELAY_SECONDS = 2.5

FIXED_CATEGORIES = [
    "ANDROID NON ROOT PANEL",
    "ANDROID ROOT PANEL",
    "IPHONE PANEL",
    "PC PANEL",
    "GUILD CALORY CREDIT",
    "CARROM PANEL"
]

# ==============================================================================
# YOUR PREMIUM EMOJIS – all required emoji IDs
# ==============================================================================
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
    handlers=[
        logging.FileHandler("bot_activity.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

def fmt_curr(amount: float) -> str:
    return f"₹{amount:,.2f}"

def natural_sort_key(value: Any) -> List[Any]:
    """Sort names naturally: A, B, C... and 1, 2, 10 instead of 1, 10, 2."""
    text = str(value or "").strip()
    return [int(part) if part.isdigit() else part.casefold()
            for part in re.split(r"(\d+)", text)]

# ==============================================================================
# 2. DATABASE FUNCTIONS
# ==============================================================================
def db_query(query: str, params: tuple = (), fetchone: bool = False, fetchall: bool = False, commit: bool = True) -> Any:
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    try:
        c.execute(query, params)
        if fetchone:
            res = c.fetchone()
        elif fetchall:
            res = c.fetchall()
        else:
            res = None
        if commit: conn.commit()
        return res
    except Exception as e:
        logger.error(f"DB Error: {e} | Query: {query} | Params: {params}")
        if commit: conn.rollback()
        return None
    finally:
        conn.close()

def db_update_count(query: str, params: tuple = ()) -> int:
    """Execute an UPDATE/DELETE and return the affected row count for CAS/idempotency checks."""
    conn = sqlite3.connect(DB_PATH, timeout=20)
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()
        return cur.rowcount
    except Exception as e:
        logger.error(f"DB Update Error: {e} | Query: {query} | Params: {params}")
        conn.rollback()
        return 0
    finally:
        conn.close()

def get_setting(key: str, default: str = "") -> str:
    val = db_query("SELECT value FROM settings WHERE key=?", (key,), fetchone=True)
    return val[0] if val and val[0] else default

def set_setting(key: str, value: str) -> None:
    db_query("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))

def log_activity(user_id: int, action: str, details: str = "") -> None:
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        db_query(
            "INSERT INTO activity_logs (user_id, action, details, timestamp) VALUES (?, ?, ?, ?)",
            (user_id, action, details, timestamp)
        )
    except Exception as e:
        logger.error(f"Failed to log activity: {e}")

def get_emoji(slot: str, default_id: str = None) -> str:
    stored = get_setting(f"emoji_{slot}", "")
    emoji_id = stored if stored and stored.isdigit() else (default_id or DEFAULT_EMOJIS.get(slot, ""))
    if emoji_id:
        return f'<tg-emoji emoji-id="{emoji_id}">✨</tg-emoji>'
    return "✨"

def get_emoji_icon(slot: str, default_id: str = None) -> str:
    stored = get_setting(f"emoji_{slot}", "")
    emoji_id = stored if stored and stored.isdigit() else (default_id or DEFAULT_EMOJIS.get(slot, ""))
    return emoji_id

# ==============================================================================
# 3. STRING RESOURCES – using placeholders for premium emojis
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

def get_ui_text(key: str, **kwargs) -> str:
    val = db_query("SELECT value FROM settings WHERE key=?", (f"ui_{key}",), fetchone=True)
    template = val[0] if val and val[0] else UI_TEXTS.get(key, "")

    emoji_map = {
        '{product_store}': get_emoji('product_store'),
        '{profile}': get_emoji('profile'),
        '{add_balance}': get_emoji('add_balance'),
        '{history}': get_emoji('history'),
        '{referral}': get_emoji('referral'),
        '{tutorial}': get_emoji('tutorial'),
        '{support}': get_emoji('support'),
        '{ludo_spin}': get_emoji('ludo_spin'),
        '{download}': get_emoji('download'),
        '{telegram}': get_emoji('telegram'),
        '{whatsapp}': get_emoji('whatsapp'),
        '{upi}': get_emoji('upi'),
        '{binance}': get_emoji('binance'),
        '{info_icon}': get_emoji('info_icon'),
        '{check_icon}': get_emoji('check_icon'),
        '{checkbox_icon}': get_emoji('checkbox_icon'),
        '{shield_icon}': get_emoji('shield_icon'),
        '{money_icon}': get_emoji('money_icon'),
        '{redeem_icon}': get_emoji('redeem_icon'),
        '{wallet_left}': get_emoji('wallet_left'),
        '{wallet_right}': get_emoji('wallet_right'),
        '{point_down}': get_emoji('point_down'),
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
# 4. DATABASE INITIALISATION & MIGRATION
# ==============================================================================
def init_db() -> None:
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, 
            phone TEXT, 
            first_name TEXT, 
            username TEXT,
            balance REAL DEFAULT 0.0, 
            account_type TEXT DEFAULT 'Regular', 
            orders_count INTEGER DEFAULT 0, 
            spent REAL DEFAULT 0.0, 
            referrals_count INTEGER DEFAULT 0, 
            referral_earned REAL DEFAULT 0.0, 
            referred_by INTEGER, 
            last_spin TEXT, 
            joined_date TEXT,
            is_reseller INTEGER DEFAULT 0,
            reseller_since TEXT,
            total_saved REAL DEFAULT 0.0,
            is_banned INTEGER DEFAULT 0,
            warnings INTEGER DEFAULT 0,
            is_vip INTEGER DEFAULT 0,
            vip_since TEXT
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            category TEXT, 
            panel_name TEXT DEFAULT '',
            name TEXT, 
            price_inr REAL, 
            reseller_price REAL DEFAULT 0.0,
            stock INTEGER, 
            apk_link TEXT, 
            validity TEXT DEFAULT 'Lifetime', 
            device_limit TEXT DEFAULT '1 Device',
            is_active INTEGER DEFAULT 1
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS product_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            product_id INTEGER, 
            key_text TEXT, 
            is_used INTEGER DEFAULT 0
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            user_id INTEGER, 
            product_name TEXT, 
            price_paid REAL, 
            delivered_key TEXT, 
            purchase_date TEXT
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            user_id INTEGER, 
            message TEXT, 
            status TEXT DEFAULT 'Open',
            created_at TEXT
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY, 
            value TEXT
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS coupons (
            code TEXT PRIMARY KEY, 
            amount REAL, 
            uses_left INTEGER
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS redeemed (
            user_id INTEGER, 
            code TEXT
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            order_id TEXT PRIMARY KEY, 
            user_id INTEGER, 
            amount_inr REAL, 
            status TEXT, 
            timestamp INTEGER
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS purchase_locks (
            user_id INTEGER PRIMARY KEY,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS crypto_txns (
            txid TEXT PRIMARY KEY, 
            user_id INTEGER, 
            amount_usdt REAL, 
            timestamp INTEGER
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS spin_rewards (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            amount REAL
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS activity_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            action TEXT,
            details TEXT,
            timestamp TEXT
        )
    ''')

    migrations = [
        "ALTER TABLE users ADD COLUMN is_vip INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN vip_since TEXT",
        "ALTER TABLE products ADD COLUMN is_active INTEGER DEFAULT 1",
        "ALTER TABLE tickets ADD COLUMN created_at TEXT",
        "ALTER TABLE users ADD COLUMN is_banned INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN warnings INTEGER DEFAULT 0",
        "ALTER TABLE products ADD COLUMN panel_name TEXT DEFAULT ''",
        "ALTER TABLE products ADD COLUMN external_enabled INTEGER DEFAULT 0",
        "ALTER TABLE products ADD COLUMN external_product_id TEXT DEFAULT ''",
        "ALTER TABLE products ADD COLUMN requires_android_id INTEGER DEFAULT 0",
        "ALTER TABLE products ADD COLUMN external_duration TEXT DEFAULT ''",
        "ALTER TABLE transactions ADD COLUMN purpose TEXT DEFAULT 'wallet'",
        "ALTER TABLE transactions ADD COLUMN product_id INTEGER",
        "ALTER TABLE transactions ADD COLUMN quantity INTEGER DEFAULT 1",
        "ALTER TABLE transactions ADD COLUMN payment_method TEXT DEFAULT 'wallet'",
    ]
    for mig in migrations:
        try: c.execute(mig)
        except sqlite3.OperationalError: pass

    # Backfill the API duration for existing products. The purchase code still
    # has additional fallbacks, so old databases remain compatible.
    try:
        c.execute("UPDATE products SET external_duration = validity WHERE COALESCE(external_duration, '') = ''")
    except sqlite3.OperationalError:
        pass
    
    c.execute("SELECT COUNT(*) FROM spin_rewards")
    if c.fetchone()[0] == 0:
        c.executemany("INSERT INTO spin_rewards (amount) VALUES (?)", [(0.0,), (1.0,), (2.0,), (5.0,), (10.0,)])

    default_settings = [
        ('spin_status', 'ON'),
        ('daily_spin_limit', '50.0'),
        ('reseller_system_status', 'ON'),
        ('bot_status', 'ON'),
        ('how_to_video', 'None'),
        ('all_files_link', 'None'),
        ('zapupi_api', ''),
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
        c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, val))

    conn.commit()
    conn.close()

def migrate_categories() -> None:
    done = get_setting("migration_done", "0")
    
    # ALWAYS force update emojis and UI texts regardless of migration status
    logger.info("Forcing emoji and UI text updates...")
    
    # Update all emoji settings
    for slot, emoji_id in DEFAULT_EMOJIS.items():
        set_setting(f"emoji_{slot}", emoji_id)
    
    # Force update UI texts
    set_setting("ui_start_menu", UI_TEXTS['start_menu'])
    set_setting("ui_add_balance_menu", UI_TEXTS['add_balance_menu'])
    set_setting("ui_download_files", UI_TEXTS['download_files'])
    set_setting("ui_lucky_dice_result", UI_TEXTS['lucky_dice_result'])
    set_setting("ui_vip_menu", UI_TEXTS['vip_menu'])
    logger.info("UI texts and emojis updated with new placeholders and IDs.")
    
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
        db_query("UPDATE products SET category = ? WHERE LOWER(category) = ?", (new, old))
    
    db_query(
        "UPDATE products SET category = 'ANDROID NON ROOT PANEL' "
        "WHERE LOWER(category) NOT IN (?, ?, ?, ?, ?, ?)",
        (
            "android non root panel", "android root panel", "iphone panel",
            "pc panel", "guild calory credit", "carrom panel"
        )
    )
    
    set_setting("migration_done", "1")
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
            user_info = db_query("SELECT is_banned FROM users WHERE user_id=?", (user_id,), fetchone=True)
            if user_info and user_info[0] == 1:
                msg = "🚫 <b>ACCESS DENIED</b>\nYou have been banned from using this bot.\nContact support if you think this is a mistake."
                if isinstance(event, Message): await event.answer(msg)
                elif isinstance(event, CallbackQuery): await event.answer(msg, show_alert=True)
                return
                
            status_check = db_query("SELECT value FROM settings WHERE key='bot_status'", fetchone=True)
            status = status_check[0] if status_check else 'ON'
            if status == 'OFF':
                msg = "⚠️ <b>Store Maintenance</b>\n\nThe store is currently offline for updates. Please check back later!"
                if isinstance(event, Message): await event.answer(msg)
                elif isinstance(event, CallbackQuery): await event.answer("⚠️ Bot is currently OFF for Maintenance.", show_alert=True)
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
    
    wait_for_zapupi_api = State()
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
def get_category_emoji(category: str) -> str:
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
        return get_emoji_icon(slot, DEFAULT_EMOJIS.get(slot, ""))
    return ""

def get_panel_emoji(panel_name: str) -> str:
    stored = get_setting(f"panel_emoji_{panel_name}", "")
    if stored and stored.isdigit():
        return stored
    return get_emoji_icon("product_store")

def contact_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Verify Contact", request_contact=True)]], 
        resize_keyboard=True, 
        one_time_keyboard=True
    )

def main_menu_kb(user_id: Optional[int] = None) -> InlineKeyboardMarkup:
    status_check = db_query("SELECT value FROM settings WHERE key='reseller_system_status'", fetchone=True)
    sys_status = status_check[0] if status_check else 'ON'
    vip_sys_check = db_query("SELECT value FROM settings WHERE key='vip_status'", fetchone=True)
    vip_system = vip_sys_check[0] if vip_sys_check else 'OFF'
    
    is_reseller = False
    if user_id:
        user_check = db_query("SELECT is_reseller FROM users WHERE user_id=?", (user_id,), fetchone=True)
        if user_check:
            is_reseller = bool(user_check[0])

    kb = InlineKeyboardMarkup(inline_keyboard=[])
    
    kb.inline_keyboard.append([
        InlineKeyboardButton(
            text="Product Store", callback_data="menu_shop",
            icon_custom_emoji_id=get_emoji_icon("product_store"),
            style="primary"
        )
    ])
    kb.inline_keyboard.append([
        InlineKeyboardButton(
            text="My Profile", callback_data="menu_profile",
            icon_custom_emoji_id=get_emoji_icon("profile"),
            style="success"
        ),
        InlineKeyboardButton(
            text="Add Balance", callback_data="menu_add_balance",
            icon_custom_emoji_id=get_emoji_icon("add_balance"),
            style="success"
        )
    ])
    kb.inline_keyboard.append([
        InlineKeyboardButton(
            text="All History", callback_data="menu_orders",
            icon_custom_emoji_id=get_emoji_icon("history"),
            style="success"
        ),
        InlineKeyboardButton(
            text="Referral", callback_data="menu_referral",
            icon_custom_emoji_id=get_emoji_icon("referral"),
            style="success"
        )
    ])
    kb.inline_keyboard.append([
        InlineKeyboardButton(
            text="Tutorials", callback_data="menu_how_to",
            icon_custom_emoji_id=get_emoji_icon("tutorial"),
            style="success"
        ),
        InlineKeyboardButton(
            text="Support", callback_data="menu_support",
            icon_custom_emoji_id=get_emoji_icon("support"),
            style="success"
        )
    ])
    kb.inline_keyboard.append([
        InlineKeyboardButton(
            text="Ludo Spin", callback_data="menu_spin_landing",
            icon_custom_emoji_id=get_emoji_icon("ludo_spin"),
            style="success"
        ),
        InlineKeyboardButton(
            text="Download Files", callback_data="menu_all_files",
            icon_custom_emoji_id=get_emoji_icon("download"),
            style="success"
        )
    ])
    
    extras_row = []
    if sys_status == 'ON' or is_reseller:
        extras_row.append(InlineKeyboardButton(
            text="Reseller Panel", callback_data="menu_reseller_dash",
            icon_custom_emoji_id=get_emoji_icon("reseller"),
            style="success"
        ))
    if vip_system == 'ON':
        extras_row.append(InlineKeyboardButton(
            text="VIP Club", callback_data="menu_vip_dash",
            icon_custom_emoji_id=get_emoji_icon("vip"),
            style="success"
        ))
    if extras_row:
        kb.inline_keyboard.append(extras_row)
        
    return kb

def back_kb(callback: str = "back_main") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text="BACK", callback_data=callback,
                icon_custom_emoji_id=get_emoji_icon("back"),
                style="danger"
            )
        ]]
    )

def admin_kb() -> InlineKeyboardMarkup:
    status = db_query("SELECT value FROM settings WHERE key='bot_status'", fetchone=True)
    status_val = status[0] if status else 'ON'
    vip_status = db_query("SELECT value FROM settings WHERE key='vip_status'", fetchone=True)
    vip_val = vip_status[0] if vip_status else 'OFF'
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Bot Statistics", callback_data="admin_view_stats", icon_custom_emoji_id=get_emoji_icon("global_stats"), style="success")],
        [InlineKeyboardButton(text="👥 User Control Panel", callback_data="admin_user_control_start", icon_custom_emoji_id=get_emoji_icon("profile"), style="success")],
        [
            InlineKeyboardButton(text="➕ Add Product", callback_data="admin_add_prod", icon_custom_emoji_id=get_emoji_icon("product_store"), style="success"),
            InlineKeyboardButton(text="📦 Manage Products", callback_data="admin_manage_prods", icon_custom_emoji_id=get_emoji_icon("product_store"), style="success")
        ],
        [
            InlineKeyboardButton(text="👑 Reseller Mgmt", callback_data="admin_reseller_menu", icon_custom_emoji_id=get_emoji_icon("reseller"), style="success"),
            InlineKeyboardButton(text="🎰 Spin Settings", callback_data="admin_spin_menu", icon_custom_emoji_id=get_emoji_icon("ludo_spin"), style="success")
        ],
        [
            InlineKeyboardButton(text="🎟 Create Coupon", callback_data="admin_create_coupon", icon_custom_emoji_id=get_emoji_icon("redeem_icon"), style="success"),
            InlineKeyboardButton(text="📢 Broadcast", callback_data="admin_broadcast_btn", icon_custom_emoji_id=get_emoji_icon("telegram"), style="success")
        ],
        [
            InlineKeyboardButton(text="🎫 View Tickets", callback_data="admin_view_tickets", icon_custom_emoji_id=get_emoji_icon("support"), style="success"),
            InlineKeyboardButton(text="📹 Tutorial Video", callback_data="admin_set_video", icon_custom_emoji_id=get_emoji_icon("tutorial"), style="success")
        ],
        [
            InlineKeyboardButton(text="🔗 All Files Link", callback_data="admin_set_all_files", icon_custom_emoji_id=get_emoji_icon("download"), style="success"),
            InlineKeyboardButton(text="🎨 Edit All Emojis", callback_data="admin_edit_emojis", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="success")
        ],
        [
            InlineKeyboardButton(text="⚙️ ZapUPI Setup", callback_data="admin_setup_zapupi", icon_custom_emoji_id=get_emoji_icon("upi"), style="success"),
            InlineKeyboardButton(text="🪙 Binance Setup", callback_data="admin_setup_binance", icon_custom_emoji_id=get_emoji_icon("binance"), style="success")
        ],
        [
            InlineKeyboardButton(text="🔗 External Key API", callback_data="admin_setup_external_api", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="success")
        ],
        [
            InlineKeyboardButton(text="✏️ Edit UI Texts", callback_data="admin_edit_ui_menu", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="success"),
            InlineKeyboardButton(text="📝 Edit Reseller Price", callback_data="admin_edit_reseller_price", icon_custom_emoji_id=get_emoji_icon("money_icon"), style="success")
        ],
        [
            InlineKeyboardButton(text="💰 Reseller Fee", callback_data="admin_set_reseller_fee", icon_custom_emoji_id=get_emoji_icon("money_icon"), style="success"),
            InlineKeyboardButton(text="💳 Min Balance", callback_data="admin_set_reseller_min", icon_custom_emoji_id=get_emoji_icon("money_icon"), style="success")
        ],
        [
            InlineKeyboardButton(text="📞 Set Support Links", callback_data="admin_set_support_links", icon_custom_emoji_id=get_emoji_icon("support"), style="success"),
            InlineKeyboardButton(text="🎨 Set Category Emojis", callback_data="admin_set_category_emojis", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="success")
        ],
        [
            InlineKeyboardButton(text="🖼 Set Panel Emojis", callback_data="admin_set_panel_emojis", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="success")
        ],
        [
            InlineKeyboardButton(
                text=f"Bot Status: {status_val} {'🟢' if status_val == 'ON' else '🔴'}",
                callback_data="admin_toggle_bot",
                icon_custom_emoji_id=get_emoji_icon("check_icon"),
                style="success" if status_val == 'ON' else "danger"
            )
        ],
        [
            InlineKeyboardButton(
                text=f"VIP System: {vip_val} {'🟢' if vip_val == 'ON' else '🔴'}",
                callback_data="admin_toggle_vip_sys",
                icon_custom_emoji_id=get_emoji_icon("vip"),
                style="success" if vip_val == 'ON' else "danger"
            )
        ]
    ])
    return kb

def admin_back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="Back to Admin", callback_data="admin_panel_back",
            icon_custom_emoji_id=get_emoji_icon("back"),
            style="danger"
        )
    ]])

# ==============================================================================
# 8. NOTIFICATIONS
# ==============================================================================
async def send_advanced_notification(user_id: int, notif_type: str, amount: float, product: str = None, key: str = None, gateway: str = "ZapUPI") -> None:
    user_info = db_query("SELECT first_name, phone, username, is_reseller, is_vip FROM users WHERE user_id=?", (user_id,), fetchone=True)
    
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
        details = (f"📦 <b>Product:</b> {product}\n🔑 <b>Key:</b> <code>{key}</code>\n💰 <b>Amount Paid:</b> ₹{amount:.2f}\n📅 <b>Time:</b> {time_now}")
    else:
        title = "💰 <b>NEW WALLET DEPOSIT!</b> 💰"
        details = (f"💵 <b>Amount Added:</b> ₹{amount:.2f}\n🧾 <b>Gateway:</b> {gateway}\n🆔 <b>Reference:</b> <code>{product}</code>\n📅 <b>Time:</b> {time_now}")

    msg = f"{title}\n━━━━━━━━━━━━━━━━━━\n👤 <b>Name:</b> {name}\n🆔 <b>User ID:</b> <code>{user_id}</code>\n📱 <b>Phone:</b> {phone}\n🔗 <b>Username:</b> {username}\n🏷 <b>Status:</b> {tag_str}\n━━━━━━━━━━━━━━━━━━\n{details}"
    try: 
        await bot.send_message(ADMIN_ID, msg, parse_mode='HTML')
    except Exception as e: 
        logger.error(f"Failed to send admin notification: {e}")

# ==============================================================================
# 9. PAYMENT VERIFIER
# ==============================================================================
async def run_payment_verification(user_id: int, order_id: str, reply_target: Any) -> None:
    txn = db_query("SELECT amount_inr, status, timestamp, purpose FROM transactions WHERE order_id=? AND user_id=?", (order_id, user_id), fetchone=True)
    if not txn:
        err = "❌ Invalid or fake Order ID detected in system!"
        if isinstance(reply_target, CallbackQuery): await reply_target.answer(err, show_alert=True)
        else: await reply_target.answer(err)
        return
    amount, status, ts, purpose = txn
    if status in ('completed', 'processing', 'paid') and purpose == 'product':
        if status == 'paid':
            await fulfill_product_transaction(order_id, user_id, reply_target.message if isinstance(reply_target, CallbackQuery) else reply_target)
        else:
            msg = "⏳ This product order is already being processed. Please wait for the key."
            if isinstance(reply_target, CallbackQuery): await reply_target.answer(msg, show_alert=True)
            else: await reply_target.answer(msg)
        return
    if status == 'paid':
        msg = "✅ This payment has already been securely credited to your wallet."
        if isinstance(reply_target, CallbackQuery): await reply_target.answer(msg, show_alert=True)
        else: await reply_target.answer(msg)
        return
    if status == 'expired':
        msg = "❌ This order has expired. Please create a new payment request."
        if isinstance(reply_target, CallbackQuery): await reply_target.answer(msg, show_alert=True)
        else: await reply_target.answer(msg)
        return
    if time.time() - ts > 900 and status == 'pending':
        db_query("UPDATE transactions SET status='expired' WHERE order_id=? AND status='pending'", (order_id,))
        if purpose == 'product': release_purchase_lock(user_id)
        err_msg = "⏳ <b>Payment Timed Out!</b>\nThe 15-minute payment window has expired."
        if isinstance(reply_target, CallbackQuery): await reply_target.message.edit_text(err_msg, reply_markup=back_kb("menu_shop" if purpose == 'product' else "menu_add_balance"), parse_mode='HTML')
        else: await reply_target.answer(err_msg, reply_markup=back_kb("menu_shop" if purpose == 'product' else "menu_add_balance"))
        return

    api_key_check = db_query("SELECT value FROM settings WHERE key='zapupi_api'", fetchone=True)
    if not api_key_check or not api_key_check[0]:
        msg = "⚠️ Gateway API key missing. Administrator needs to configure it."
        if isinstance(reply_target, CallbackQuery): await reply_target.answer(msg, show_alert=True)
        else: await reply_target.answer(msg)
        return
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post("https://pay.zapupi.com/api/order-status", json={"zap_key": api_key_check[0], "order_id": order_id}) as resp:
                res_json = await resp.json(content_type=None) if resp.status == 200 else {}
        if res_json.get("status") != "success":
            err = f"⚠️ Gateway Error: {res_json.get('message', 'Unknown Error')}"
            if isinstance(reply_target, CallbackQuery): await reply_target.answer(err, show_alert=True)
            else: await reply_target.answer(err)
            return
        real_status = res_json.get("data", {}).get("status", "Pending")
        if real_status == "Success":
            claimed = db_update_count("UPDATE transactions SET status='paid' WHERE order_id=? AND status='pending'", (order_id,))
            if not claimed:
                latest = db_query("SELECT status,purpose FROM transactions WHERE order_id=?", (order_id,), fetchone=True)
                if latest and latest[1] == 'product' and latest[0] in ('paid','processing','completed'):
                    if isinstance(reply_target, CallbackQuery): await reply_target.answer("⏳ Payment already accepted; your key is being prepared.", show_alert=True)
                    return await fulfill_product_transaction(order_id, user_id, reply_target.message)
                return await reply_target.answer("⏳ Payment was already processed.", show_alert=True) if isinstance(reply_target, CallbackQuery) else None
            if purpose == 'product':
                await fulfill_product_transaction(order_id, user_id, reply_target.message if isinstance(reply_target, CallbackQuery) else reply_target)
            else:
                db_query("UPDATE users SET balance = balance + ? WHERE user_id=?", (amount, user_id))
                success_msg = f"🎉 <b>VERIFICATION SUCCESSFUL!</b>\n\n✅ {fmt_curr(amount)} has been added to your wallet securely."
                if isinstance(reply_target, CallbackQuery): await reply_target.message.edit_text(success_msg, reply_markup=back_kb(), parse_mode='HTML')
                else: await reply_target.answer(success_msg, reply_markup=back_kb())
                await send_advanced_notification(user_id, "DEPOSIT", amount, product=order_id, gateway="ZapUPI")
                log_activity(user_id, "DEPOSIT_SUCCESS", f"Amount: {amount}, Gateway: ZapUPI, Order: {order_id}")
        elif real_status == "Pending":
            fail_msg = "⏳ Payment is still Pending. Please wait a moment and verify again."
            if isinstance(reply_target, CallbackQuery): await reply_target.answer(fail_msg, show_alert=True)
            else: await reply_target.answer(fail_msg)
        else:
            fail_msg = f"❌ Payment Failed or Cancelled (Gateway Status: {real_status})."
            if isinstance(reply_target, CallbackQuery): await reply_target.answer(fail_msg, show_alert=True)
            else: await reply_target.answer(fail_msg)
    except Exception:
        logger.exception("ZapUPI verification error")
        err = "⚠️ Unable to connect to payment gateway right now."
        if isinstance(reply_target, CallbackQuery): await reply_target.answer(err, show_alert=True)
        else: await reply_target.answer(err)

async def auto_verify_task() -> None:
    while True:
        await asyncio.sleep(15)
        api_key_check = db_query("SELECT value FROM settings WHERE key='zapupi_api'", fetchone=True)
        if not api_key_check or not api_key_check[0]:
            continue
        pending_txns = db_query("SELECT order_id, user_id, amount_inr, timestamp, purpose FROM transactions WHERE status='pending'", fetchall=True) or []
        for order_id, user_id, amount, ts, purpose in pending_txns:
            if time.time() - ts > 900:
                db_query("UPDATE transactions SET status='expired' WHERE order_id=? AND status='pending'", (order_id,))
                if purpose == 'product': release_purchase_lock(user_id)
                try: await bot.send_message(user_id, f"⏳ <b>Order Expired!</b>\nYour payment window for order <code>{order_id}</code> has timed out.", parse_mode='HTML')
                except Exception: pass
                continue
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post("https://pay.zapupi.com/api/order-status", json={"zap_key": api_key_check[0], "order_id": order_id}) as resp:
                        res_json = await resp.json(content_type=None) if resp.status == 200 else {}
                if res_json.get("status") == "success" and res_json.get("data", {}).get("status") == "Success":
                    claimed = db_update_count("UPDATE transactions SET status='paid' WHERE order_id=? AND status='pending'", (order_id,))
                    if not claimed:
                        continue
                    if purpose == 'product':
                        try:
                            msg = await bot.send_message(user_id, "✨ <b>AUTO-VERIFIED!</b>\n\nPayment received. Preparing your keys...", parse_mode='HTML')
                            await fulfill_product_transaction(order_id, user_id, msg)
                        except Exception:
                            logger.exception("Auto product fulfillment failed for %s", order_id)
                    else:
                        db_query("UPDATE users SET balance = balance + ? WHERE user_id=?", (amount, user_id))
                        try: await bot.send_message(user_id, f"✨ <b>AUTO-VERIFIED!</b>\n\n✅ Your payment was detected successfully. {fmt_curr(amount)} has been added to your balance!", parse_mode='HTML')
                        except Exception: pass
                        await send_advanced_notification(user_id, "DEPOSIT", amount, product=order_id, gateway="ZapUPI Auto")
                        log_activity(user_id, "DEPOSIT_AUTO_SUCCESS", f"Amount: {amount}, Gateway: ZapUPI Auto, Order: {order_id}")
            except Exception as e:
                logger.debug(f"Auto-verify minor exception ignored: {e}")

# ==============================================================================
# 10. ONBOARDING & START
# ==============================================================================
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    try: await message.answer_sticker(WELCOME_STICKER_ID)
    except: pass 
    
    args = message.text.split()
    if len(args) > 1 and args[1].startswith("v_"):
        order_id = args[1].split("v_")[1]
        msg = await message.answer("🔄 <b>Verifying your payment securely...</b>\n<i>Connecting to gateway...</i>", parse_mode='HTML')
        await run_payment_verification(message.from_user.id, order_id, msg)
        return

    referred_by = None
    if len(args) > 1 and args[1].startswith("ref_"):
        try: referred_by = int(args[1].split("_")[1])
        except: pass

    user = db_query("SELECT phone FROM users WHERE user_id=?", (message.from_user.id,), fetchone=True)
    current_username = message.from_user.username or ""
    db_query("UPDATE users SET username=? WHERE user_id=?", (current_username, message.from_user.id))

    if not user or not user[0]:
        db_query("INSERT OR IGNORE INTO users (user_id, first_name, username, referred_by, joined_date) VALUES (?, ?, ?, ?, ?)", 
                 (message.from_user.id, message.from_user.first_name, current_username, referred_by, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        log_activity(message.from_user.id, "ACCOUNT_CREATED")
        await message.answer("<b>🛡 VERIFICATION REQUIRED</b>\n\nTo safeguard your orders and account, we need to verify you.\n👇 <b>Tap the button below:</b>", parse_mode='HTML', reply_markup=contact_kb())
    else:
        log_activity(message.from_user.id, "CMD_START")
        await send_main_menu(message)

@dp.message(F.contact)
async def handle_contact(message: Message):
    if message.contact.user_id == message.from_user.id:
        db_query("UPDATE users SET phone=? WHERE user_id=?", (message.contact.phone_number, message.from_user.id))
        referrer = db_query("SELECT referred_by FROM users WHERE user_id=?", (message.from_user.id,), fetchone=True)
        if referrer and referrer[0]:
            db_query("UPDATE users SET referrals_count = referrals_count + 1 WHERE user_id=?", (referrer[0],))
            try: await bot.send_message(referrer[0], f"🎉 <b>Referral Success!</b>\nUser <b>{message.from_user.first_name}</b> joined using your link!", parse_mode='HTML')
            except: pass
        log_activity(message.from_user.id, "CONTACT_VERIFIED")
        await message.answer("✅ Verification successful! Welcome to the system.", reply_markup=ReplyKeyboardRemove())
        await send_main_menu(message)
    else:
        await message.answer("❌ Security Alert: Please share your OWN contact using the provided button.")

async def send_main_menu(ctx: Any):
    text = get_ui_text("start_menu")
    kb = main_menu_kb(ctx.from_user.id)
    if isinstance(ctx, Message): 
        await ctx.answer(text, reply_markup=kb, parse_mode='HTML')
    else: 
        await ctx.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "back_main")
async def back_main(call: CallbackQuery, state: FSMContext):
    await state.clear()
    log_activity(call.from_user.id, "RETURN_MAIN_MENU")
    await send_main_menu(call)

# ==============================================================================
# 11. ADD BALANCE
# ==============================================================================
@dp.callback_query(F.data == "menu_add_balance")
async def select_gateway_menu(call: CallbackQuery):
    log_activity(call.from_user.id, "VIEW_ADD_BALANCE")
    text = get_ui_text("add_balance_menu")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="UPI PAY", callback_data="gateway_inr", icon_custom_emoji_id=get_emoji_icon("upi"), style="primary"),
            InlineKeyboardButton(text="BINANCE PAY", callback_data="gateway_crypto", icon_custom_emoji_id=get_emoji_icon("binance"), style="primary")
        ],
        [
            InlineKeyboardButton(text="BACK", callback_data="back_main", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")
        ]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

# ==============================================================================
# 12. UPI & CRYPTO PAYMENT FLOWS
# ==============================================================================
@dp.callback_query(F.data == "gateway_inr")
async def add_balance_inr(call: CallbackQuery):
    text = f"💵 <b>— ZAPUPI DEPOSIT —</b> 💵\n\nSelect amount to deposit:"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="₹50", callback_data="pay_50", icon_custom_emoji_id=get_emoji_icon("money_icon"), style="primary"), 
         InlineKeyboardButton(text="₹100", callback_data="pay_100", icon_custom_emoji_id=get_emoji_icon("money_icon"), style="primary")],
        [InlineKeyboardButton(text="₹200", callback_data="pay_200", icon_custom_emoji_id=get_emoji_icon("money_icon"), style="primary"), 
         InlineKeyboardButton(text="₹500", callback_data="pay_500", icon_custom_emoji_id=get_emoji_icon("money_icon"), style="primary")],
        [InlineKeyboardButton(text="✏️ Custom Amount", callback_data="custom_deposit_keypad", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="primary")],
        [InlineKeyboardButton(text="Back", callback_data="menu_add_balance", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "custom_deposit_keypad")
async def show_custom_keypad(call: CallbackQuery, state: FSMContext):
    await state.set_state(UserStates.custom_amount_input)
    await state.update_data(amount_str="0")
    await show_keypad(call.message)

async def show_keypad(message: Message, amount_str: str = "0"):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="      1      ", callback_data="kp_1", style="primary"), InlineKeyboardButton(text="      2      ", callback_data="kp_2", style="primary"), InlineKeyboardButton(text="      3      ", callback_data="kp_3", style="primary")],
        [InlineKeyboardButton(text="      4      ", callback_data="kp_4", style="primary"), InlineKeyboardButton(text="      5      ", callback_data="kp_5", style="primary"), InlineKeyboardButton(text="      6      ", callback_data="kp_6", style="primary")],
        [InlineKeyboardButton(text="      7      ", callback_data="kp_7", style="primary"), InlineKeyboardButton(text="      8      ", callback_data="kp_8", style="primary"), InlineKeyboardButton(text="      9      ", callback_data="kp_9", style="primary")],
        [InlineKeyboardButton(text="    ⌫    ", callback_data="kp_backspace", style="danger"), InlineKeyboardButton(text="      0      ", callback_data="kp_0", style="primary"), InlineKeyboardButton(text="    C    ", callback_data="kp_clear", style="danger")],
        [InlineKeyboardButton(text=f"✅ Confirm (₹{amount_str})", callback_data="kp_confirm", icon_custom_emoji_id=get_emoji_icon("check_icon"), style="success")],
        [InlineKeyboardButton(text="Cancel", callback_data="gateway_inr", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]
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
            await generate_zapupi_order(call.from_user.id, amount, call.message)
        except ValueError:
            await call.answer("Invalid amount.", show_alert=True)
        return
    if action == "backspace":
        if len(amount_str) > 1: amount_str = amount_str[:-1]
        else: amount_str = "0"
    elif action == "clear":
        amount_str = "0"
    else:
        if amount_str == "0": amount_str = action
        else: amount_str += action
        if len(amount_str) > 6: amount_str = amount_str[:6]
    await state.update_data(amount_str=amount_str)
    await show_keypad(call.message, amount_str)
    await call.answer()

@dp.callback_query(F.data.startswith("pay_"))
async def process_zapupi_payment_callback(call: CallbackQuery):
    inr_amount = float(call.data.split("_")[1])
    await call.message.edit_text("⏳ <b>Generating Secure Link via ZapUPI...</b>", parse_mode='HTML')
    await generate_zapupi_order(call.from_user.id, inr_amount, call.message)

async def generate_zapupi_order(user_id: int, inr_amount: float, message_obj: Message) -> None:
    api_key_check = db_query("SELECT value FROM settings WHERE key='zapupi_api'", fetchone=True)
    if not api_key_check or not api_key_check[0]:
        return await message_obj.edit_text("⚠️ ZapUPI Gateway is currently offline. Admin needs to set API Key.", reply_markup=back_kb("gateway_inr"), parse_mode='HTML')
        
    api_key = api_key_check[0]
    current_time = int(time.time())
    order_id = f"NXT{user_id}{current_time}"
    user_phone = db_query("SELECT phone FROM users WHERE user_id=?", (user_id,), fetchone=True)
    mobile = user_phone[0] if user_phone and user_phone[0] else "9999999999"
    bot_deep_link = f"https://t.me/{BOT_USERNAME}?start=v_{order_id}"
    
    db_query("INSERT INTO transactions (order_id, user_id, amount_inr, status, timestamp, purpose, quantity, payment_method) VALUES (?, ?, ?, 'pending', ?, 'wallet', 1, 'upi')", (order_id, user_id, inr_amount, current_time))
    payment_url = ""
    
    async with aiohttp.ClientSession() as session:
        try:
            url = "https://pay.zapupi.com/api/create-order"
            payload = {"zap_key": api_key, "order_id": order_id, "amount": str(inr_amount), "customer_mobile": mobile, "remark": "Wallet Topup", "success_url": bot_deep_link, "failed_url": f"https://t.me/{BOT_USERNAME}", "timeout_url": f"https://t.me/{BOT_USERNAME}", "webhook_url": f"https://t.me/{BOT_USERNAME}"}
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    try: res_data = await resp.json(content_type=None)
                    except: res_data = {}
                    if res_data.get("status") == "success": payment_url = res_data.get("payment_url")
                    else: return await message_obj.edit_text(f"❌ <b>Gateway Data Error:</b> {res_data.get('message', 'Unknown structure.')}", reply_markup=back_kb("gateway_inr"), parse_mode='HTML')
                else: return await message_obj.edit_text(f"❌ <b>Gateway Server Error:</b> HTTP {resp.status}", reply_markup=back_kb("gateway_inr"), parse_mode='HTML')
        except Exception as e:
            return await message_obj.edit_text(f"❌ <b>API Connection Error:</b> {str(e)}", reply_markup=back_kb("gateway_inr"), parse_mode='HTML')

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Pay Now (Auto Redirects)", url=payment_url, style="success")],
        [InlineKeyboardButton(text="🔄 Manual Verify", callback_data=f"verify_{order_id}", icon_custom_emoji_id=get_emoji_icon("check_icon"), style="primary")],
        [InlineKeyboardButton(text="Cancel Transaction", callback_data="menu_add_balance", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]
    ])
    text = (f"🧾 <b>SECURE INVOICE CREATED</b>\n\nAmount: <b>₹{inr_amount:.2f}</b>\nOrder ID: <code>{order_id}</code>\n⏳ <b>Timer:</b> 15:00 Minutes\n\n1️⃣ Click <b>Pay Now</b> to open UPI Gateway.\n2️⃣ Complete the payment in your app.\n3️⃣ <b>Auto-Verify:</b> Return to the bot after payment! ✨")
    log_activity(user_id, "GENERATE_INVOICE", f"Amount: {inr_amount}, Order ID: {order_id}")
    await message_obj.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("verify_"))
async def manual_verify_callback(call: CallbackQuery):
    order_id = call.data.split("_", 1)[1]
    await run_payment_verification(call.from_user.id, order_id, call)

@dp.callback_query(F.data == "gateway_crypto")
async def add_balance_crypto(call: CallbackQuery, state: FSMContext):
    address_check = db_query("SELECT value FROM settings WHERE key='binance_address'", fetchone=True)
    if not address_check or not address_check[0]:
        return await call.message.edit_text("⚠️ Binance Gateway is currently offline. Admin has not set a deposit address.", reply_markup=back_kb("menu_add_balance"), parse_mode='HTML')
    deposit_address = address_check[0]
    msg = (f"🪙 <b>— BINANCE USDT DEPOSIT —</b> 🪙\n\n💵 <b>Exchange Rate:</b> 1 USDT = ₹{USDT_TO_INR}\n⚠️ <b>Network:</b> Please send via <b>TRC20</b> or <b>BEP20</b>.\n\n👇 <b>Send your USDT to this exact address:</b>\n<code>{deposit_address}</code>\n\n━━━━━━━━━━━━━━━━━━\n✅ <b>After sending the USDT, reply to this message with your exact TxID (Transaction Hash) to instantly claim your balance.</b>")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Cancel", callback_data="menu_add_balance", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]])
    await call.message.edit_text(msg, reply_markup=kb, parse_mode='HTML')
    await state.set_state(UserStates.wait_for_crypto_txid)

@dp.message(UserStates.wait_for_crypto_txid)
async def process_crypto_txid(m: Message, state: FSMContext):
    txid = m.text.strip()
    user_id = m.from_user.id
    if len(txid) < 10: return await m.answer("❌ That doesn't look like a valid TxID. Please try again.")
    if db_query("SELECT txid FROM crypto_txns WHERE txid=?", (txid,), fetchone=True):
        return await m.answer("⚠️ This Transaction ID has already been claimed in the system!", reply_markup=back_kb("menu_add_balance"), parse_mode='HTML')
    api_key_check = db_query("SELECT value FROM settings WHERE key='binance_api'", fetchone=True)
    secret_key_check = db_query("SELECT value FROM settings WHERE key='binance_secret'", fetchone=True)
    if not api_key_check or not secret_key_check:
        return await m.answer("⚠️ Binance API is missing on the server. Contact Support.", reply_markup=back_kb("menu_add_balance"), parse_mode='HTML')
    await m.answer("🔄 <b>Verifying your TxID with Binance Blockchain...</b>\n<i>This may take up to 30 seconds...</i>", parse_mode='HTML')
    api_key = api_key_check[0]; secret_key = secret_key_check[0]
    timestamp = int(time.time() * 1000)
    query_string = f"timestamp={timestamp}"
    signature = hmac.new(secret_key.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
    headers = {'X-MBX-APIKEY': api_key}
    url = f"https://api.binance.com/sapi/v1/capital/deposit/hisrec?{query_string}&signature={signature}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    try: history = await resp.json(content_type=None)
                    except: history = []
                    found = False
                    for deposit in history:
                        if deposit.get("txId") == txid and deposit.get("status") == 1:
                            found = True
                            usdt_amount = float(deposit.get("amount"))
                            inr_amount = usdt_amount * USDT_TO_INR
                            db_query("INSERT INTO crypto_txns (txid, user_id, amount_usdt, timestamp) VALUES (?, ?, ?, ?)", (txid, user_id, usdt_amount, int(time.time())))
                            db_query("UPDATE users SET balance = balance + ? WHERE user_id=?", (inr_amount, user_id))
                            await m.answer(f"🎉 <b>CRYPTO DEPOSIT SUCCESSFUL!</b>\n\n✅ We safely received <b>{usdt_amount} USDT</b>.\n💰 <b>{fmt_curr(inr_amount)}</b> has been added to your balance!", reply_markup=main_menu_kb(m.from_user.id), parse_mode='HTML')
                            await send_advanced_notification(user_id, "DEPOSIT", inr_amount, product=txid, gateway="Binance Crypto")
                            log_activity(user_id, "CRYPTO_DEPOSIT", f"TxID: {txid}, Amount: {inr_amount}")
                            await state.clear()
                            break
                    if not found: await m.answer("❌ <b>TxID Not Found or Still Pending!</b>\nMake sure the transaction is fully confirmed. Try again in 5 mins.", reply_markup=back_kb("menu_add_balance"), parse_mode='HTML')
                else: await m.answer(f"⚠️ <b>Binance Server Error:</b> HTTP {resp.status}.", reply_markup=back_kb("menu_add_balance"), parse_mode='HTML')
        except Exception as e: await m.answer(f"⚠️ <b>Connection Error:</b> {str(e)}", reply_markup=back_kb("menu_add_balance"), parse_mode='HTML')

# ==============================================================================
# 13. SHOP – with uppercase categories and new point_down emoji
# ==============================================================================
@dp.callback_query(F.data == "menu_shop")
async def view_shop_panels(call: CallbackQuery):
    log_activity(call.from_user.id, "VIEW_SHOP")
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    text = f"{get_emoji('product_store')} <b><u>SELECT PRODUCT PANEL</u></b>\n━━━━━━━━━━━━━━━━━━\n\n{get_emoji('point_down')} <b>Choose a panel to view its packages:</b>"
    for cat in FIXED_CATEGORIES:
        count = db_query("SELECT COUNT(*) FROM products WHERE category LIKE ? AND is_active=1", (cat + '%',), fetchone=True)[0]
        emoji_id = get_category_emoji(cat)
        kb.inline_keyboard.append([InlineKeyboardButton(text=cat, callback_data=f"cat_{cat[:30]}", icon_custom_emoji_id=emoji_id, style="danger")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="BACK", callback_data="back_main", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("cat_"))
async def view_panel_names(call: CallbackQuery):
    category = call.data.split("cat_", 1)[1]
    panel_names = db_query("SELECT DISTINCT panel_name FROM products WHERE category LIKE ? AND is_active=1 AND panel_name != ''", (category + '%',), fetchall=True)
    panel_names = sorted(panel_names or [], key=lambda row: natural_sort_key(row[0]))
    if not panel_names:
        prods = db_query("SELECT id, name, price_inr, stock, reseller_price, validity, device_limit, external_enabled FROM products WHERE category LIKE ? AND is_active=1", (category + '%',), fetchall=True)
        if not prods: return await call.answer("❌ No products available in this category yet.", show_alert=True)
        await show_products_for_panel(call, prods, category)
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    text = f"{get_emoji('product_store')} <b><u>{category.upper()} PANELS</u></b>\n━━━━━━━━━━━━━━━━━━\n\n{get_emoji('point_down')} <b>Choose a panel name:</b>"
    for pn in panel_names:
        panel = pn[0]
        emoji_id = get_panel_emoji(panel) or get_emoji_icon("product_store")
        kb.inline_keyboard.append([InlineKeyboardButton(text=panel, callback_data=f"pnl_{category[:30]}_{panel[:30]}", icon_custom_emoji_id=emoji_id, style="danger")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="BACK TO PANELS", callback_data="menu_shop", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("pnl_"))
async def view_products_for_panel(call: CallbackQuery):
    parts = call.data.split("pnl_", 1)[1].split("_", 1)
    if len(parts) != 2: return await call.answer("Invalid selection.", show_alert=True)
    category, panel_name = parts[0], parts[1]
    prods = db_query("SELECT id, name, price_inr, stock, reseller_price, validity, device_limit, external_enabled FROM products WHERE category LIKE ? AND panel_name LIKE ? AND is_active=1", (category + '%', panel_name + '%'), fetchall=True)
    if not prods: return await call.answer("No products found for this panel.", show_alert=True)
    await show_products_for_panel(call, prods, f"{category} - {panel_name}")

async def show_products_for_panel(call: CallbackQuery, prods: List[Tuple], header: str):
    user = db_query("SELECT is_reseller, is_vip FROM users WHERE user_id=?", (call.from_user.id,), fetchone=True)
    is_reseller = bool(user[0]) if user else False
    is_vip = bool(user[1]) if user else False
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    text = f"{get_emoji('product_store')} <b><u>{header.upper()} PACKAGES</u></b>\n━━━━━━━━━━━━━━━━━━\n\n"
    # Always show packages in natural A-Z / 1-2-10 order, independent of add time.
    prods = sorted(prods or [], key=lambda row: natural_sort_key(row[1]))
    for p in prods:
        prod_id, package_name, normal_price, stock, reseller_price, validity, device, external_enabled = p
        normal_price = float(normal_price) if normal_price is not None else 0.0
        reseller_price = float(reseller_price) if reseller_price is not None else 0.0
        base_price = reseller_price if is_reseller else normal_price
        if is_vip: display_price = base_price - (base_price * (VIP_DISCOUNT_PERCENTAGE / 100))
        else: display_price = base_price
        stock_status = "♾️ API Available" if external_enabled else ("✅ In Stock" if stock > 0 else "❌ Out of Stock")
        text += f"{get_emoji('product_store')} ⏱ <b>Validity: {package_name}</b>\n"
        if is_reseller or is_vip:
            text += f"💰 Regular Price: <s>{fmt_curr(normal_price)}</s>\n"
            if is_reseller and not is_vip: text += f"👑 <b>Reseller Price: {fmt_curr(display_price)}</b>\n"
            elif is_vip and not is_reseller: text += f"🌟 <b>VIP Price: {fmt_curr(display_price)}</b>\n"
            else: text += f"👑🌟 <b>Super Price: {fmt_curr(display_price)}</b>\n"
        else: text += f"💰 Price: {fmt_curr(normal_price)}\n"
        text += f"📱 Limit: {device} | 📦 {stock_status}\n\n"
        if external_enabled or stock > 0:
            kb.inline_keyboard.append([InlineKeyboardButton(text=f"Buy {package_name} - {fmt_curr(display_price)}", callback_data=f"buy_{prod_id}", icon_custom_emoji_id=get_emoji_icon("product_store"), style="danger")])
        else:
            kb.inline_keyboard.append([InlineKeyboardButton(text=f"❌ {package_name} (Out of Stock)", callback_data="ignore_stock_click", style="danger")])
    text += f"{get_emoji('point_down')} <b>Select package below to instantly purchase:</b>"
    kb.inline_keyboard.append([InlineKeyboardButton(text="BACK TO PANELS", callback_data="menu_shop", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "ignore_stock_click")
async def ignore_stock_click(call: CallbackQuery):
    await call.answer("⚠️ This duration is completely Out of Stock! Admins have been notified to refill.", show_alert=True)

@dp.callback_query(F.data.startswith("buy_"))
async def process_buy(call: CallbackQuery):
    """Open quantity selection. No key is generated here."""
    try:
        prod_id = int(call.data.split("_", 1)[1])
    except (ValueError, IndexError):
        return await call.answer("❌ Invalid product.", show_alert=True)

    prod = db_query("SELECT name, price_inr, stock, validity, external_enabled FROM products WHERE id=? AND is_active=1", (prod_id,), fetchone=True)
    if not prod:
        return await call.answer("❌ Product not found.", show_alert=True)

    user = db_query("SELECT balance, is_reseller, is_vip FROM users WHERE user_id=?", (call.from_user.id,), fetchone=True)
    if not user:
        return await call.answer("❌ User account not found!", show_alert=True)

    normal_price = float(prod[1] or 0)
    reseller_row = db_query("SELECT reseller_price FROM products WHERE id=?", (prod_id,), fetchone=True)
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
            callback_data=f"qty_{prod_id}_{qty}", style="primary"
        ))
        if len(row) == 2:
            kb.inline_keyboard.append(row); row=[]
    if row:
        kb.inline_keyboard.append(row)
    kb.inline_keyboard.append([InlineKeyboardButton(text="BACK", callback_data="menu_shop", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")])

    text = (
        f"🛒 <b>SELECT QUANTITY</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        f"📦 <b>{html.escape(prod[0])}</b>\n"
        f"⏱ <b>Validity:</b> {html.escape(str(prod[3] or ''))}\n"
        f"💰 <b>Price per key:</b> {fmt_curr(unit_price)}\n\n"
        "👇 Choose how many keys you want:"
    )
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')


def get_purchase_pricing(user_id: int, prod_id: int):
    row = db_query("""
        SELECT p.name, p.price_inr, p.reseller_price, p.stock, p.validity, p.device_limit,
               p.category, p.panel_name, p.external_enabled, p.external_product_id, p.external_duration, p.apk_link
        FROM products p WHERE p.id=? AND p.is_active=1
    """, (prod_id,), fetchone=True)
    user = db_query("SELECT balance, referred_by, is_reseller, total_saved, is_vip FROM users WHERE user_id=?", (user_id,), fetchone=True)
    if not row or not user:
        return None
    normal = float(row[1] or 0)
    reseller = float(row[2] or 0)
    base = reseller if bool(user[2]) else normal
    unit = base - (base * VIP_DISCOUNT_PERCENTAGE / 100) if bool(user[4]) else base
    return row, user, unit


def acquire_purchase_lock(user_id: int, product_id: int, quantity: int, ttl: int = 1200) -> bool:
    now = int(time.time())
    conn = sqlite3.connect(DB_PATH, timeout=20)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM purchase_locks WHERE expires_at < ?", (now,))
        existing = conn.execute("SELECT user_id FROM purchase_locks WHERE user_id=?", (user_id,)).fetchone()
        if existing:
            conn.rollback()
            return False
        conn.execute(
            "INSERT INTO purchase_locks(user_id, product_id, quantity, created_at, expires_at) VALUES(?,?,?,?,?)",
            (user_id, product_id, quantity, now, now + ttl)
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        logger.exception("Could not acquire purchase lock")
        return False
    finally:
        conn.close()


def release_purchase_lock(user_id: int) -> None:
    db_query("DELETE FROM purchase_locks WHERE user_id=?", (user_id,))


async def _api_key_with_timer(product_id: str, duration: str, message: Message, index: int, quantity: int):
    """Run one non-idempotent API BUY while showing a live wait timer."""
    task = asyncio.create_task(fetch_external_key(product_id, duration, ""))
    started = time.monotonic()
    while not task.done():
        elapsed = int(time.monotonic() - started)
        mm, ss = divmod(elapsed, 60)
        try:
            await message.edit_text(
                f"⏳ <b>WAIT FOR KEY</b>\n\nGenerating key <b>{index}/{quantity}</b>\n"
                f"🕐 Time: <b>{mm:02d}:{ss:02d}</b>\n"
                "🔒 Please do not click again — this order is locked.", parse_mode='HTML'
            )
        except Exception:
            pass
        try:
            return await asyncio.wait_for(asyncio.shield(task), timeout=2.0)
        except asyncio.TimeoutError:
            continue
    return await task


async def generate_product_keys(prod_id: int, quantity: int, external_enabled: bool, external_product_id: str, api_duration: str, update_message: Message):
    """Generate exactly `quantity` keys. This function is only called after a purchase is locked."""
    keys = []
    for index in range(1, quantity + 1):
        await update_message.edit_text(
            f"⏳ <b>WAIT FOR KEY</b>\n\nGenerating key <b>{index}/{quantity}</b>...\n"
            "🔒 Duplicate clicks are disabled until this process finishes.", parse_mode='HTML'
        )
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
            key_data = db_query("SELECT id, key_text FROM product_keys WHERE product_id=? AND is_used=0 LIMIT 1", (prod_id,), fetchone=True)
            if not key_data:
                return (keys if keys else None), "No manual key is available"
            keys.append(str(key_data[1]))
            db_query("UPDATE product_keys SET is_used=1 WHERE id=? AND is_used=0", (key_data[0],))
            db_query("UPDATE products SET stock=CASE WHEN stock>0 THEN stock-1 ELSE 0 END WHERE id=?", (prod_id,))
        # Small UI pause so the user sees the per-key progress instead of a frozen screen.
        if index < quantity:
            await asyncio.sleep(1)
    return keys, None


async def fulfill_product_transaction(order_id: str, user_id: int, reply_message: Message = None) -> bool:
    """Atomically claim a paid product transaction so it can only deliver once."""
    txn = db_query("SELECT amount_inr, status, timestamp, product_id, quantity, payment_method FROM transactions WHERE order_id=? AND user_id=?", (order_id, user_id), fetchone=True)
    if not txn:
        return False
    amount, status, ts, prod_id, quantity, payment_method = txn
    if status == 'completed':
        return True
    if status != 'paid':
        return False
    claimed = db_update_count("UPDATE transactions SET status='processing' WHERE order_id=? AND user_id=? AND status='paid'", (order_id, user_id))
    check = db_query("SELECT status FROM transactions WHERE order_id=?", (order_id,), fetchone=True)
    if not claimed and (not check or check[0] != 'processing'):
        return False
    if check and check[0] == 'processing' and not claimed:
        return False

    pricing = get_purchase_pricing(user_id, int(prod_id))
    if not pricing:
        db_query("UPDATE transactions SET status='failed' WHERE order_id=?", (order_id,))
        release_purchase_lock(user_id)
        return False
    prod, user, unit_price = pricing
    expected = unit_price * int(quantity or 1)
    if abs(float(amount) - expected) > 0.01:
        db_query("UPDATE transactions SET status='failed' WHERE order_id=?", (order_id,))
        release_purchase_lock(user_id)
        return False

    external_enabled = bool(prod[8])
    external_product_id = str(prod[9] or '').strip()
    duration_candidates = []
    for candidate in (prod[10], prod[4], prod[0]):
        candidate = normalize_api_duration(str(candidate or ''))
        if candidate and candidate not in duration_candidates:
            duration_candidates.append(candidate)
    if external_enabled and not external_product_id:
        db_query("UPDATE users SET balance=balance+? WHERE user_id=?", (float(amount), user_id))
        db_query("UPDATE transactions SET status='failed' WHERE order_id=?", (order_id,))
        release_purchase_lock(user_id)
        return False

    if reply_message is None:
        reply_message = await bot.send_message(user_id, "⏳ <b>Preparing your keys...</b>", parse_mode='HTML')
    # For API products, a duration mismatch is safe to try with the next configured
    # candidate. Any other API error stops immediately because BUY is non-idempotent.
    keys = []
    error = None
    if external_enabled:
        for candidate_duration in duration_candidates:
            one_keys, one_error = await generate_product_keys(int(prod_id), int(quantity), True, external_product_id, candidate_duration, reply_message)
            if one_keys:
                keys, error = one_keys, one_error
                # Never retry another duration after any key was actually generated.
                # The BUY endpoint is non-idempotent and a retry could consume extra keys.
                break
            error = one_error
            if 'price not found' not in str(one_error).lower() and 'price_not_found' not in str(one_error).lower():
                break
    else:
        keys, error = await generate_product_keys(int(prod_id), int(quantity), False, external_product_id, '', reply_message)
    if not keys:
        # If a direct payment succeeded but key generation fails, preserve the money
        # by crediting it to the user's wallet rather than silently losing it.
        db_query("UPDATE users SET balance=balance+? WHERE user_id=?", (float(amount), user_id))
        db_query("UPDATE transactions SET status='failed' WHERE order_id=?", (order_id,))
        release_purchase_lock(user_id)
        try:
            await reply_message.edit_text(f"❌ <b>Key generation failed:</b> {html.escape(error or 'Unknown error')}\n\n💰 The paid amount has been credited to your wallet.", reply_markup=back_kb("menu_shop"), parse_mode='HTML')
        except Exception:
            pass
        return False

    generated_qty = len(keys)
    delivered_key = "\n".join(f"{i+1}. {k}" for i, k in enumerate(keys))
    actual_amount = unit_price * generated_qty
    remainder = max(0.0, float(amount) - actual_amount)
    if remainder > 0.01:
        db_query("UPDATE users SET balance=balance+? WHERE user_id=?", (remainder, user_id))
    savings = float(prod[1] or 0) * generated_qty - actual_amount
    db_query("UPDATE users SET spent=spent+?, orders_count=orders_count+1, total_saved=total_saved+? WHERE user_id=?", (actual_amount, savings, user_id))
    if user[1]:
        commission = actual_amount * 0.15
        db_query("UPDATE users SET balance=balance+?, referral_earned=referral_earned+? WHERE user_id=?", (commission, commission, user[1]))
        try:
            await bot.send_message(user[1], f"🎁 <b>Referral Bonus Added!</b>\nYou earned {fmt_curr(commission)} from a successful purchase.", parse_mode='HTML')
        except Exception:
            pass

    product_full_name = f"{prod[6]} - {prod[7]} ({prod[0]}) x{quantity}"
    db_query("INSERT INTO orders (user_id, product_name, price_paid, delivered_key, purchase_date) VALUES (?, ?, ?, ?, ?)",
             (user_id, product_full_name, actual_amount, delivered_key, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    db_query("UPDATE transactions SET status='completed' WHERE order_id=? AND status='processing'", (order_id,))
    release_purchase_lock(user_id)
    log_activity(user_id, "PURCHASE_SUCCESS", f"Product: {product_full_name}, Paid: {actual_amount}, Keys: {generated_qty}, Payment: {payment_method}")
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
    msg += f"\n<i>For any issues, tap Support or contact: {ADMIN_CONTACT}</i>"
    try:
        await reply_message.edit_text(msg, reply_markup=back_kb("menu_shop"), disable_web_page_preview=True, parse_mode='HTML')
    except Exception:
        await bot.send_message(user_id, msg, reply_markup=back_kb("menu_shop"), disable_web_page_preview=True, parse_mode='HTML')
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

    pricing = get_purchase_pricing(call.from_user.id, prod_id)
    if not pricing:
        return await call.answer("❌ Product or user not found.", show_alert=True)
    prod, user, unit_price = pricing
    if not bool(prod[8]) and int(prod[3] or 0) < quantity:
        return await call.answer("❌ Not enough keys in stock.", show_alert=True)
    total = unit_price * quantity
    balance = float(user[0] or 0)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💳 Wallet Balance — {fmt_curr(total)}", callback_data=f"walletpay_{prod_id}_{quantity}", style="success")],
        [InlineKeyboardButton(text=f"🇮🇳 Pay Direct by UPI — {fmt_curr(total)}", callback_data=f"upipay_{prod_id}_{quantity}", style="primary")],
        [InlineKeyboardButton(text=f"🪙 Pay Direct by Binance — {fmt_curr(total)}", callback_data=f"binpay_{prod_id}_{quantity}", style="primary")],
        [InlineKeyboardButton(text="➕ Add Balance First", callback_data="menu_add_balance", style="primary")],
        [InlineKeyboardButton(text="BACK", callback_data=f"buy_{prod_id}", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]
    ])
    text = (f"🧾 <b>ORDER SUMMARY</b>\n━━━━━━━━━━━━━━━━━━\n\n"
            f"📦 <b>{html.escape(str(prod[0]))}</b>\n🔢 <b>Quantity:</b> {quantity}\n"
            f"💰 <b>Price per key:</b> {fmt_curr(unit_price)}\n💵 <b>Total:</b> {fmt_curr(total)}\n"
            f"👛 <b>Wallet Balance:</b> {fmt_curr(balance)}\n\n"
            "👇 Choose a payment method:")
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')


async def create_product_upi_order(user_id: int, prod_id: int, quantity: int, message_obj: Message):
    pricing = get_purchase_pricing(user_id, prod_id)
    if not pricing:
        return await message_obj.edit_text("❌ Product not found.", reply_markup=back_kb("menu_shop"), parse_mode='HTML')
    prod, user, unit_price = pricing
    total = unit_price * quantity
    if not acquire_purchase_lock(user_id, prod_id, quantity):
        return await message_obj.edit_text("⏳ <b>An order is already processing.</b> Please finish or wait for the current order.", reply_markup=back_kb("menu_shop"), parse_mode='HTML')

    api_key_check = db_query("SELECT value FROM settings WHERE key='zapupi_api'", fetchone=True)
    if not api_key_check or not api_key_check[0]:
        release_purchase_lock(user_id)
        return await message_obj.edit_text("⚠️ UPI Gateway is currently offline.", reply_markup=back_kb("menu_shop"), parse_mode='HTML')

    order_id = f"PRD{user_id}{int(time.time())}{random.randint(100,999)}"
    phone_row = db_query("SELECT phone FROM users WHERE user_id=?", (user_id,), fetchone=True)
    mobile = phone_row[0] if phone_row and phone_row[0] else "9999999999"
    now = int(time.time())
    db_query("INSERT INTO transactions(order_id,user_id,amount_inr,status,timestamp,purpose,product_id,quantity,payment_method) VALUES(?,?,?,?,?,?,?,?,?)",
             (order_id,user_id,total,'pending',now,'product',prod_id,quantity,'upi'))
    try:
        async with aiohttp.ClientSession() as session:
            payload = {"zap_key": api_key_check[0], "order_id": order_id, "amount": str(total), "customer_mobile": mobile,
                       "remark": f"Product {prod_id} x{quantity}", "success_url": f"https://t.me/{BOT_USERNAME}?start=v_{order_id}",
                       "failed_url": f"https://t.me/{BOT_USERNAME}", "timeout_url": f"https://t.me/{BOT_USERNAME}", "webhook_url": f"https://t.me/{BOT_USERNAME}"}
            async with session.post("https://pay.zapupi.com/api/create-order", json=payload) as resp:
                data = await resp.json(content_type=None)
                if resp.status != 200 or data.get('status') != 'success':
                    raise RuntimeError(data.get('message', f'HTTP {resp.status}'))
                payment_url = data.get('payment_url')
                if not payment_url:
                    raise RuntimeError('Gateway did not return a payment URL')
    except Exception as exc:
        db_query("UPDATE transactions SET status='failed' WHERE order_id=?", (order_id,))
        release_purchase_lock(user_id)
        return await message_obj.edit_text(f"❌ <b>UPI order failed:</b> {html.escape(str(exc))}", reply_markup=back_kb("menu_shop"), parse_mode='HTML')

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 PAY NOW", url=payment_url, style="success")],
        [InlineKeyboardButton(text="🔄 VERIFY PAYMENT", callback_data=f"verify_{order_id}", style="primary")]
    ])
    await message_obj.edit_text(
        f"🧾 <b>PRODUCT PAYMENT</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        f"📦 {html.escape(str(prod[0]))}\n🔢 Quantity: <b>{quantity}</b>\n💰 Amount: <b>{fmt_curr(total)}</b>\n\n"
        "⏳ <b>Payment window: 15:00</b>\n"
        "1️⃣ Tap PAY NOW\n2️⃣ Complete payment\n3️⃣ Return here; payment will auto-verify\n\n"
        "🔒 Duplicate payment/key generation is blocked for this order.", reply_markup=kb, parse_mode='HTML')


@dp.callback_query(F.data.startswith("walletpay_"))
async def wallet_product_payment(call: CallbackQuery):
    try:
        _, prod_id_s, qty_s = call.data.split("_", 2)
        prod_id, quantity = int(prod_id_s), int(qty_s)
    except ValueError:
        return await call.answer("❌ Invalid order.", show_alert=True)
    pricing = get_purchase_pricing(call.from_user.id, prod_id)
    if not pricing:
        return await call.answer("❌ Product not found.", show_alert=True)
    prod, user, unit_price = pricing
    total = unit_price * quantity
    if float(user[0] or 0) < total:
        return await call.answer(f"❌ Insufficient wallet balance. Need {fmt_curr(total)}.", show_alert=True)
    if not acquire_purchase_lock(call.from_user.id, prod_id, quantity):
        return await call.answer("⏳ This order is already processing. Please wait for the key.", show_alert=True)

    order_id = f"WAL{call.from_user.id}{int(time.time())}{random.randint(100,999)}"
    deducted = db_update_count("UPDATE users SET balance=balance-? WHERE user_id=? AND balance>=?", (total, call.from_user.id, total))
    if not deducted:
        release_purchase_lock(call.from_user.id)
        return await call.answer("❌ Balance changed. Please try again.", show_alert=True)
    db_query("INSERT INTO transactions(order_id,user_id,amount_inr,status,timestamp,purpose,product_id,quantity,payment_method) VALUES(?,?,?,?,?,?,?,?,?)",
             (order_id,call.from_user.id,total,'paid',int(time.time()),'product',prod_id,quantity,'wallet'))
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
    pricing = get_purchase_pricing(call.from_user.id, prod_id)
    if not pricing:
        return await call.answer("❌ Product not found.", show_alert=True)
    _, user, unit_price = pricing
    total = unit_price * quantity
    address = get_setting('binance_address', '')
    if not address:
        return await call.answer("⚠️ Binance payment is not configured.", show_alert=True)
    if not acquire_purchase_lock(call.from_user.id, prod_id, quantity):
        return await call.answer("⏳ This order is already processing.", show_alert=True)
    order_id = f"BPR{call.from_user.id}{int(time.time())}{random.randint(100,999)}"
    db_query("INSERT INTO transactions(order_id,user_id,amount_inr,status,timestamp,purpose,product_id,quantity,payment_method) VALUES(?,?,?,?,?,?,?,?,?)",
             (order_id,call.from_user.id,total,'pending',int(time.time()),'product',prod_id,quantity,'binance'))
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
    txn = db_query("SELECT amount_inr,status,user_id FROM transactions WHERE order_id=? AND purpose='product'", (order_id,), fetchone=True)
    if not txn or txn[2] != m.from_user.id:
        return await m.answer("❌ Product payment order not found.")
    if txn[1] != 'pending':
        return await m.answer("⏳ This payment order is no longer pending.")
    if db_query("SELECT txid FROM crypto_txns WHERE txid=?", (txid,), fetchone=True):
        return await m.answer("⚠️ This TxID has already been used.")
    api_key_check = db_query("SELECT value FROM settings WHERE key='binance_api'", fetchone=True)
    secret_key_check = db_query("SELECT value FROM settings WHERE key='binance_secret'", fetchone=True)
    if not api_key_check or not secret_key_check:
        release_purchase_lock(m.from_user.id)
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
        found = next((d for d in history if d.get('txId') == txid and d.get('status') == 1 and abs(float(d.get('amount',0)) - expected_usdt) < 0.0001), None)
        if not found:
            return await m.answer("❌ TxID not found, not confirmed, or amount does not match.")
        claimed = db_update_count("UPDATE transactions SET status='paid' WHERE order_id=? AND status='pending'", (order_id,))
        if not claimed:
            return await m.answer("⏳ This order is already being processed.")
        db_query("INSERT INTO crypto_txns(txid,user_id,amount_usdt,timestamp) VALUES(?,?,?,?)", (txid,m.from_user.id,float(found.get('amount')),int(time.time())))
        await state.clear()
        await fulfill_product_transaction(order_id, m.from_user.id, m)
    except Exception as exc:
        await m.answer(f"⚠️ Binance verification error: {html.escape(str(exc))}")


@dp.callback_query(F.data.startswith("cancel_product_"))
async def cancel_product_order(call: CallbackQuery, state: FSMContext):
    order_id = call.data.split("_", 2)[2]
    txn = db_query("SELECT status,user_id FROM transactions WHERE order_id=? AND purpose='product'", (order_id,), fetchone=True)
    if not txn or txn[1] != call.from_user.id:
        return await call.answer("❌ Order not found.", show_alert=True)
    if txn[0] == 'pending':
        db_query("UPDATE transactions SET status='cancelled' WHERE order_id=? AND status='pending'", (order_id,))
        release_purchase_lock(call.from_user.id)
        return await call.message.edit_text("❌ <b>Product order cancelled.</b>", reply_markup=back_kb("menu_shop"), parse_mode='HTML')
    return await call.answer("⏳ This order can no longer be cancelled.", show_alert=True)

# ==============================================================================
# 14. USER DASHBOARD, FILES, VIP, RESELLER, ORDERS, PROFILE, REFERRAL
# ==============================================================================
@dp.callback_query(F.data == "menu_all_files")
async def all_files_handler(call: CallbackQuery):
    link_q = db_query("SELECT value FROM settings WHERE key='all_files_link'", fetchone=True)
    link = link_q[0] if link_q and link_q[0] != 'None' else None
    if link:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Access Download Channel ↗️", url=link, icon_custom_emoji_id=get_emoji_icon("download"), style="success")],
            [InlineKeyboardButton(text="BACK", callback_data="back_main", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]
        ])
        text = get_ui_text("download_files")
        await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')
    else:
        await call.answer("⚠️ Admin has not configured the private download channel link yet.", show_alert=True)

@dp.callback_query(F.data == "menu_vip_dash")
async def vip_dashboard(call: CallbackQuery):
    u = db_query("SELECT balance, is_vip, vip_since FROM users WHERE user_id=?", (call.from_user.id,), fetchone=True)
    is_vip = bool(u[1])
    status_str = "🟢 Active (Lifetime)" if is_vip else "🔴 Not Subscribed"
    text = get_ui_text("vip_menu", vip_status=status_str)
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    if is_vip:
        text += f"\n📅 <b>Member Since:</b> {u[2]}\n\nEnjoy your permanent 15% discount!"
    else:
        text += f"\n\n💳 <b>Your Current Balance:</b> {fmt_curr(u[0])}\n"
        if u[0] >= VIP_PRICE_INR: 
            kb.inline_keyboard.append([InlineKeyboardButton(text=f"✅ Purchase VIP for {fmt_curr(VIP_PRICE_INR)}", callback_data="execute_vip_upgrade", icon_custom_emoji_id=get_emoji_icon("vip"), style="success")])
        else:
            kb.inline_keyboard.append([InlineKeyboardButton(text=f"❌ Need {fmt_curr(VIP_PRICE_INR)} to Upgrade", callback_data="ignore_stock_click", style="danger")])
            kb.inline_keyboard.append([InlineKeyboardButton(text="💳 Add Balance Now", callback_data="menu_add_balance", icon_custom_emoji_id=get_emoji_icon("add_balance"), style="success")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="BACK", callback_data="back_main", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "execute_vip_upgrade")
async def execute_vip_upgrade(call: CallbackQuery):
    u = db_query("SELECT balance, is_vip FROM users WHERE user_id=?", (call.from_user.id,), fetchone=True)
    if u[1]: return await call.answer("⚠️ You are already a VIP Member!", show_alert=True)
    if u[0] < VIP_PRICE_INR: return await call.answer(f"❌ Your balance dropped below {VIP_PRICE_INR}.", show_alert=True)
    new_balance = u[0] - VIP_PRICE_INR
    now_date = datetime.now().strftime("%Y-%m-%d")
    db_query("UPDATE users SET balance=?, is_vip=1, vip_since=? WHERE user_id=?", (new_balance, now_date, call.from_user.id))
    log_activity(call.from_user.id, "UPGRADED_VIP")
    try: await bot.send_message(ADMIN_ID, f"🌟 <b>NEW VIP UPGRADE</b>\n👤 User ID: <code>{call.from_user.id}</code>", parse_mode='HTML')
    except: pass
    await call.answer("🎉 Upgrade Successful! You are now a VIP Member.", show_alert=True)
    await vip_dashboard(call)

@dp.callback_query(F.data == "menu_reseller_dash")
async def reseller_dashboard(call: CallbackQuery):
    u = db_query("SELECT balance, is_reseller, reseller_since, total_saved FROM users WHERE user_id=?", (call.from_user.id,), fetchone=True)
    status_check = db_query("SELECT value FROM settings WHERE key='reseller_system_status'", fetchone=True)
    system_status = status_check[0] if status_check else "ON"
    setup_fee = float(get_setting("reseller_setup_fee", "200.0"))
    min_balance = float(get_setting("reseller_min_balance", "500.0"))
    if u[1]: 
        text = (f"{get_emoji('shield_icon')} <b><u>— RESELLER DASHBOARD —</u></b> {get_emoji('shield_icon')}\n\n🟢 <b>Status:</b> Active\n📅 <b>Since:</b> {u[2]}\n{get_emoji('money_icon')} <b>Total Saved:</b> {fmt_curr(u[3])}\n\n🎉 You are enjoying exclusive wholesale prices on all products!")
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="BACK", callback_data="back_main", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]])
        await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')
        return
    if system_status == "OFF": return await call.answer("⚠️ Wholesale / Reseller registrations are currently closed by Admin.", show_alert=True)
    text = (f"⚡ <b><u>— BECOME A RESELLER —</u></b> ⚡\n\nUpgrade your account to access wholesale <b>Reseller Prices</b>!\n\n📋 <b>Requirements to Upgrade:</b>\n1️⃣ Must have a minimum balance of <b>{fmt_curr(min_balance)}</b>.\n2️⃣ A one-time setup fee of <b>{fmt_curr(setup_fee)}</b> will be deducted.\n\n💳 <b>Your Current Balance:</b> {fmt_curr(u[0])}\n")
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    if u[0] >= min_balance: 
        kb.inline_keyboard.append([InlineKeyboardButton(text=f"✅ Pay {fmt_curr(setup_fee)} & Become Reseller", callback_data="execute_reseller_upgrade", icon_custom_emoji_id=get_emoji_icon("reseller"), style="success")])
    else:
        kb.inline_keyboard.append([InlineKeyboardButton(text=f"❌ Insufficient Balance (Need {fmt_curr(min_balance)})", callback_data="ignore_stock_click", style="danger")])
        kb.inline_keyboard.append([InlineKeyboardButton(text="💳 Add Balance", callback_data="menu_add_balance", icon_custom_emoji_id=get_emoji_icon("add_balance"), style="success")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="BACK", callback_data="back_main", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "execute_reseller_upgrade")
async def execute_reseller_upgrade(call: CallbackQuery):
    setup_fee = float(get_setting("reseller_setup_fee", "200.0"))
    min_balance = float(get_setting("reseller_min_balance", "500.0"))
    u = db_query("SELECT balance, is_reseller FROM users WHERE user_id=?", (call.from_user.id,), fetchone=True)
    if u[1]: return await call.answer("⚠️ You are already a Reseller!", show_alert=True)
    if u[0] < min_balance: return await call.answer(f"❌ Your balance dropped below {fmt_curr(min_balance)}. Please top up.", show_alert=True)
    new_balance = u[0] - setup_fee
    db_query("UPDATE users SET balance=?, is_reseller=1, reseller_since=?, account_type='Reseller' WHERE user_id=?", (new_balance, datetime.now().strftime("%Y-%m-%d"), call.from_user.id))
    log_activity(call.from_user.id, "UPGRADED_RESELLER")
    try: await bot.send_message(ADMIN_ID, f"👑 <b>NEW RESELLER UPGRADE</b>\n👤 User ID: <code>{call.from_user.id}</code>", parse_mode='HTML')
    except: pass
    await call.answer("🎉 Upgrade Successful! Welcome to the Reseller tier.", show_alert=True)
    await reseller_dashboard(call)

@dp.callback_query(F.data == "menu_orders")
async def my_orders(call: CallbackQuery):
    orders = db_query("SELECT product_name, delivered_key, purchase_date, price_paid FROM orders WHERE user_id=? ORDER BY id DESC LIMIT 10", (call.from_user.id,), fetchall=True)
    if not orders: return await call.message.edit_text("🧾 You haven't made any purchases yet. Your vault is empty.", reply_markup=back_kb(), parse_mode='HTML')
    text = "🧾 <b><u>— YOUR RECENT ORDERS (LAST 10) —</u></b> 🧾\n\n"
    for o in orders: text += f"📦 <b>{o[0]}</b> ({fmt_curr(o[3])})\n🔑 <code>{o[1]}</code>\n📅 <i>{o[2]}</i>\n━━━━━━━━━━━━━━━━\n"
    await call.message.edit_text(text, reply_markup=back_kb(), parse_mode='HTML')

@dp.callback_query(F.data == "menu_profile")
async def show_profile(call: CallbackQuery):
    u = db_query("SELECT user_id, first_name, account_type, balance, orders_count, spent, referrals_count, joined_date, is_reseller, reseller_since, total_saved, is_vip FROM users WHERE user_id=?", (call.from_user.id,), fetchone=True)
    acc_type_display = []
    if u[8]: acc_type_display.append(f"{get_emoji('reseller')} Reseller")
    if u[11]: acc_type_display.append(f"{get_emoji('vip')} VIP")
    type_str = " | ".join(acc_type_display) if acc_type_display else f"{get_emoji('regular_user')} Regular User"
    text = (
        f"{get_emoji('grid_id')} <b><u>— YOUR SECURE PROFILE —</u></b> {get_emoji('grid_id')}\n\n"
        f"{get_emoji('grid_id')} <b>Grid ID:</b> <code>{u[0]}</code>\n"
        f"{get_emoji('name')} <b>Name:</b> {u[1]}\n"
        f"{get_emoji('account_level')} <b>Account Level:</b> {type_str}\n\n"
        f"{get_emoji('wallet_left')} <b>— Wallet —</b> {get_emoji('wallet_right')}\n"
        f"{get_emoji('wallet_left')} <b>Current Balance:</b> {fmt_curr(u[3])} {get_emoji('wallet_right')}\n\n"
        f"{get_emoji('global_stats')} <b>— Global Statistics —</b>\n"
        f"{get_emoji('total_orders')} <b>Total Orders:</b> {u[4]}\n"
        f"{get_emoji('total_spent')} <b>Total Spent:</b> {fmt_curr(u[5])}\n"
        f"{get_emoji('total_referrals')} <b>Total Referrals:</b> {u[6]}\n\n"
    )
    if u[8]:
        text += f"{get_emoji('shield_icon')} <b>— RESELLER METRICS —</b> {get_emoji('shield_icon')}\n{get_emoji('money_icon')} <b>Total Saved via Reseller:</b> {fmt_curr(u[10])}\n\n"
    text += f"{get_emoji('joined_grid')} <b>Joined Grid:</b> {u[7]}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Redeem Promo Code",
            callback_data="redeem_coupon",
            icon_custom_emoji_id=get_emoji_icon('redeem_icon'),
            style="success"
        )],
        [InlineKeyboardButton(text="BACK", callback_data="back_main", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "redeem_coupon")
async def redeem_coupon_start(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("🎟 <b>Please enter your VIP / Promo redeem code below:</b>", reply_markup=back_kb("menu_profile"), parse_mode='HTML')
    await state.set_state(UserStates.wait_for_redeem)

@dp.message(UserStates.wait_for_redeem)
async def process_redeem(m: Message, state: FSMContext):
    code = m.text.strip().upper()
    user_id = m.from_user.id
    if db_query("SELECT * FROM redeemed WHERE user_id=? AND code=?", (user_id, code), fetchone=True):
        await m.answer("❌ Anti-Fraud Alert: You already redeemed this unique code!", reply_markup=main_menu_kb(m.from_user.id), parse_mode='HTML')
        await state.clear()
        return
    coupon = db_query("SELECT amount, uses_left FROM coupons WHERE code=?", (code,), fetchone=True)
    if not coupon: await m.answer("❌ Invalid or Expired Code!", reply_markup=main_menu_kb(m.from_user.id), parse_mode='HTML')
    elif coupon[1] <= 0: await m.answer("❌ This code's usage limit has been fully claimed by other users.", reply_markup=main_menu_kb(m.from_user.id), parse_mode='HTML')
    else:
        db_query("UPDATE users SET balance = balance + ? WHERE user_id=?", (coupon[0], user_id))
        db_query("UPDATE coupons SET uses_left = uses_left - 1 WHERE code=?", (code,))
        db_query("INSERT INTO redeemed (user_id, code) VALUES (?, ?)", (user_id, code))
        log_activity(user_id, "PROMO_REDEEMED", f"Code: {code}, Amount: {coupon[0]}")
        await m.answer(f"🎉 <b>Success!</b>\nSafely added {fmt_curr(coupon[0])} to your balance!", reply_markup=main_menu_kb(m.from_user.id), parse_mode='HTML')
        try:
            user_info = db_query("SELECT first_name FROM users WHERE user_id=?", (user_id,), fetchone=True)
            uname = user_info[0] if user_info else "Unknown User"
            await bot.send_message(ADMIN_ID, f"🎟 <b>PROMO CODE REDEEMED!</b>\n👤 User: {uname} (<code>{user_id}</code>)\n🔖 Code: <b>{code}</b>\n💵 Amount: {fmt_curr(coupon[0])}", parse_mode='HTML')
        except Exception: pass
    await state.clear()

@dp.callback_query(F.data == "menu_referral")
async def show_referral(call: CallbackQuery):
    u = db_query("SELECT referrals_count, referral_earned FROM users WHERE user_id=?", (call.from_user.id,), fetchone=True)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{call.from_user.id}"
    text = (f"{get_emoji('referral')} <b><u>AFFILIATE PROGRAM</u></b> {get_emoji('referral')}\n\n✅ <b>Status:</b> ACTIVE\n💰 Earn <b>2% flat commission</b> on every successful purchase made by your referred friends!\n\n📊 <b>YOUR STATS:</b>\n👥 Total Invited: {u[0]}\n💵 Life-time Earned: {fmt_curr(u[1])}\n\n🔗 <b>Your Invite Link:</b>\n<code>{ref_link}</code>\n\n<i>Simply copy and share this link to start earning!</i>")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="BACK", callback_data="back_main", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

# ==============================================================================
# 15. LUDO / DICE SPIN
# ==============================================================================
@dp.callback_query(F.data == "menu_spin_landing")
async def lucky_spin_landing(call: CallbackQuery):
    status_check = db_query("SELECT value FROM settings WHERE key='spin_status'", fetchone=True)
    spin_status = status_check[0] if status_check else "ON"
    if spin_status == "OFF": return await call.answer("⚠️ Lucky Ludo Spin is currently disabled by Admin.", show_alert=True)
    await call.message.edit_text(f"{get_emoji('ludo_spin')} <b><u>— LUDO SPIN —</u></b> {get_emoji('ludo_spin')}\n\nTest your luck! You can spin once every 24 hours.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Spin Dice Now!", callback_data="execute_spin", icon_custom_emoji_id=get_emoji_icon("ludo_spin"), style="success")], 
        [InlineKeyboardButton(text="BACK", callback_data="back_main", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]
    ]), parse_mode='HTML')

@dp.callback_query(F.data == "execute_spin")
async def execute_spin(call: CallbackQuery):
    status_check = db_query("SELECT value FROM settings WHERE key='spin_status'", fetchone=True)
    if status_check and status_check[0] == "OFF": return await call.answer("⚠️ Lucky Spin is disabled.", show_alert=True)
    u = db_query("SELECT last_spin, balance, is_vip FROM users WHERE user_id=?", (call.from_user.id,), fetchone=True)
    now = datetime.now()
    if u[0] and now < datetime.strptime(u[0], "%Y-%m-%d %H:%M:%S") + timedelta(hours=24):
        return await call.message.edit_text("❌ <b>Cooldown Active!</b>\nYou already played today. Come back tomorrow.", reply_markup=back_kb(), parse_mode='HTML')
    await call.message.delete()
    dice_msg = await bot.send_dice(chat_id=call.message.chat.id, emoji="🎲")
    await asyncio.sleep(SPIN_DELAY_SECONDS) 
    dice_val = dice_msg.dice.value
    limit_check = db_query("SELECT value FROM settings WHERE key='daily_spin_limit'", fetchone=True)
    limit = float(limit_check[0]) if limit_check else 50.0
    rewards_db = db_query("SELECT amount FROM spin_rewards WHERE amount <= ?", (limit,), fetchall=True)
    rewards_list = [r[0] for r in rewards_db] if rewards_db else [0.0]
    reward = random.choice(rewards_list)
    if bool(u[2]) and reward > 0: reward = reward * 2.0
    new_bal = u[1] + reward
    db_query("UPDATE users SET balance=?, last_spin=? WHERE user_id=?", (new_bal, now.strftime("%Y-%m-%d %H:%M:%S"), call.from_user.id))
    log_activity(call.from_user.id, "PLAYED_SPIN", f"Reward: {reward}, Dice: {dice_val}")
    msg = get_ui_text("lucky_dice_result", dice_value=dice_val, won_amount=fmt_curr(reward), new_balance=fmt_curr(new_bal))
    if bool(u[2]) and reward > 0: msg += "\n\n<i>🌟 VIP Bonus: 2x Multiplier Applied!</i>"
    await dice_msg.reply(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📚 BACK TO MENU", callback_data="back_main", icon_custom_emoji_id=get_emoji_icon("back"), style="success")]]), parse_mode='HTML')

# ==============================================================================
# 16. TUTORIALS & SUPPORT
# ==============================================================================
@dp.callback_query(F.data == "menu_how_to")
async def tutorial_system(call: CallbackQuery):
    video_link_query = db_query("SELECT value FROM settings WHERE key='how_to_video'", fetchone=True)
    video_link = video_link_query[0] if video_link_query and video_link_query[0] != 'None' else None
    text = (f"{get_emoji('tutorial')} <b><u>— TUTORIALS & GUIDE —</u></b> {get_emoji('tutorial')}\n\n1️⃣ Add funds via <b>Add Balance</b>\n2️⃣ Navigate to <b>Product Store</b>\n3️⃣ Choose your desired Panel and Package validity.\n4️⃣ The Key and Installation APK link will be instantly provided.")
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    if video_link: kb.inline_keyboard.append([InlineKeyboardButton(text="Watch Full Video Tutorial", url=video_link, icon_custom_emoji_id=get_emoji_icon("tutorial"), style="success")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="BACK", callback_data="back_main", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "menu_support")
async def support_center(call: CallbackQuery):
    telegram_link = get_setting("support_telegram", "https://t.me/YOUR_SUPPORT")
    whatsapp_link = get_setting("support_whatsapp", "https://wa.me/YOUR_NUMBER")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Contact on Telegram", url=telegram_link, icon_custom_emoji_id=get_emoji_icon("telegram"), style="primary")],
        [InlineKeyboardButton(text="Contact on WhatsApp", url=whatsapp_link, icon_custom_emoji_id=get_emoji_icon("whatsapp"), style="primary")],
        [InlineKeyboardButton(text="🎫 Open New Ticket", callback_data="open_ticket", icon_custom_emoji_id=get_emoji_icon("support"), style="success"), 
         InlineKeyboardButton(text="📋 My Open Tickets", callback_data="my_tickets", icon_custom_emoji_id=get_emoji_icon("history"), style="success")], 
        [InlineKeyboardButton(text="BACK", callback_data="back_main", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]
    ])
    await call.message.edit_text(f"{get_emoji('telegram')}{get_emoji('whatsapp')} <b><u>— PREMIUM SUPPORT CENTER —</u></b>\n\nContact us via Telegram or WhatsApp for instant help, or open a support ticket for admin assistance.", reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "my_tickets")
async def view_my_tickets(call: CallbackQuery):
    tickets = db_query("SELECT id, message, status, created_at FROM tickets WHERE user_id=? ORDER BY id DESC LIMIT 5", (call.from_user.id,), fetchall=True)
    if not tickets: return await call.message.edit_text("📋 You do not have any active or previous support tickets.", reply_markup=back_kb("menu_support"), parse_mode='HTML')
    text = "📋 <b><u>— Your Recent Tickets —</u></b> 📋\n\n"
    for t in tickets:
        status_icon = "🟢" if t[2] == 'Open' else "🔴"
        text += f"🎫 <b>Ticket #{t[0]}</b> | Status: {status_icon} <b>{t[2]}</b>\n📅 <i>{t[3]}</i>\n📝 <i>{t[1][:80]}...</i>\n\n"
    await call.message.edit_text(text, reply_markup=back_kb("menu_support"), parse_mode='HTML')

@dp.callback_query(F.data == "open_ticket")
async def open_ticket_start(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("📝 <b>Please type your issue/message below in detail:</b>", reply_markup=back_kb("menu_support"), parse_mode='HTML')
    await state.set_state(UserStates.wait_for_ticket)

@dp.message(UserStates.wait_for_ticket)
async def process_ticket(m: Message, state: FSMContext):
    db_query("INSERT INTO tickets (user_id, message, created_at) VALUES (?, ?, ?)", (m.from_user.id, m.text, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    await m.answer("✅ <b>Ticket Submitted Successfully!</b> Admins will reply soon.", reply_markup=main_menu_kb(m.from_user.id), parse_mode='HTML')
    try: await bot.send_message(ADMIN_ID, f"🚨 <b>NEW SUPPORT TICKET</b>\nFrom: <code>{m.from_user.id}</code>\nMsg: {m.text}", parse_mode='HTML')
    except: pass
    log_activity(m.from_user.id, "OPENED_TICKET")
    await state.clear()

# ==============================================================================
# 17. ADMIN PANEL
# ==============================================================================
@dp.message(Command("admin"))
async def admin_panel(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    await state.clear()
    await message.answer("⚙️ <b>Advanced Admin Terminal</b>\n<i>Authorized Access Granted. Use the buttons below.</i>", reply_markup=admin_kb(), parse_mode='HTML')

@dp.callback_query(F.data == "admin_panel_back")
async def back_to_admin(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("⚙️ <b>Advanced Admin Terminal</b>\n<i>Authorized Access Granted. Use the buttons below.</i>", reply_markup=admin_kb(), parse_mode='HTML')

@dp.callback_query(F.data == "admin_toggle_vip_sys")
async def toggle_vip_sys(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    res = db_query("SELECT value FROM settings WHERE key='vip_status'", fetchone=True)
    current = res[0] if res else 'OFF'
    new_status = 'ON' if current == 'OFF' else 'OFF'
    db_query("INSERT OR REPLACE INTO settings (key, value) VALUES ('vip_status', ?)", (new_status,))
    await call.message.edit_reply_markup(reply_markup=admin_kb())

@dp.callback_query(F.data == "admin_user_control_start")
async def admin_user_control_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Download Full User List", callback_data="admin_download_userlist", icon_custom_emoji_id=get_emoji_icon("download"), style="success")],
        [InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]
    ])
    await call.message.edit_text("💻 <b>User Control Terminal</b>\n\n✏️ Enter the <b>User ID</b> or <b>@Username</b> you want to investigate or manage:\n\n👇 <b>OR</b> download the full user CSV format list:", reply_markup=kb, parse_mode='HTML')
    await state.set_state(AdminStates.manage_target_user)

@dp.callback_query(F.data == "admin_download_userlist")
async def admin_download_userlist(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    users = db_query("SELECT username, user_id, phone, balance, orders_count, is_vip, is_reseller FROM users", fetchall=True)
    if not users: return await call.answer("❌ No users found in the database.", show_alert=True)
    file_content = "FULL DATABASE DUMP\n" + "="*100 + "\n"
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
    if target.startswith('@'): target = target[1:]
    loader_msg = await hacker_loading(m, "Querying User Database")
    user_q = db_query("SELECT user_id, first_name, username, balance, is_reseller, orders_count, spent, joined_date, is_banned, warnings, is_vip FROM users WHERE user_id=? OR username=? COLLATE NOCASE", (target, target), fetchone=True)
    if not user_q: return await loader_msg.edit_text("❌ Target not found in the grid. Check ID/Username syntax.", reply_markup=admin_back_kb(), parse_mode='HTML')
    u_id, u_name, u_user, bal, is_res, orders, spent, joined, is_banned, warnings, is_vip = user_q
    await state.update_data(target_u_id=u_id)
    status_emoji = "🔴 BANNED" if is_banned else "🟢 ACTIVE"
    tags = []
    if is_res: tags.append("👑 Reseller")
    if is_vip: tags.append("🌟 VIP")
    type_str = " | ".join(tags) if tags else "👤 Regular"
    text = (f"🛡 <b><u>USER CONTROL TERMINAL</u></b> 🛡\n━━━━━━━━━━━━━━━━━━\n📛 <b>Name:</b> {u_name} (@{u_user})\n🆔 <b>ID:</b> <code>{u_id}</code>\n📊 <b>Status:</b> {status_emoji}\n🔰 <b>Type:</b> {type_str}\n⚠️ <b>Warnings Issued:</b> {warnings}\n━━━━━━━━━━━━━━━━━━\n💰 <b>Wallet Balance:</b> {fmt_curr(bal)}\n📦 <b>Orders:</b> {orders} | 💸 <b>Total Spent:</b> {fmt_curr(spent)}\n📅 <b>Joined:</b> {joined}")
    ban_btn_text = "Unban ✅" if is_banned else "Ban 🚫"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Add Funds ➕", callback_data=f"usrctrl_add_{u_id}", icon_custom_emoji_id=get_emoji_icon("add_balance"), style="success"), 
         InlineKeyboardButton(text="Minus Funds ➖", callback_data=f"usrctrl_min_{u_id}", icon_custom_emoji_id=get_emoji_icon("money_icon"), style="danger")],
        [InlineKeyboardButton(text=ban_btn_text, callback_data=f"usrctrl_ban_{u_id}", icon_custom_emoji_id=get_emoji_icon("shield_icon"), style="danger"), 
         InlineKeyboardButton(text="Warn User ⚠️", callback_data=f"usrctrl_warn_{u_id}", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="danger")],
        [InlineKeyboardButton(text="Give VIP 🌟" if not is_vip else "Remove VIP 🚫", callback_data=f"usrctrl_vip_{u_id}", icon_custom_emoji_id=get_emoji_icon("vip"), style="success")],
        [InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]
    ])
    await loader_msg.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("usrctrl_"))
async def handle_user_actions(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    action = call.data.split("_")[1]
    u_id = int(call.data.split("_")[2])
    await state.update_data(target_u_id=u_id)
    if action == "ban":
        current_status = db_query("SELECT is_banned FROM users WHERE user_id=?", (u_id,), fetchone=True)[0]
        if current_status == 0:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Yes, Ban", callback_data=f"confirm_ban_{u_id}", icon_custom_emoji_id=get_emoji_icon("check_icon"), style="danger"), 
                 InlineKeyboardButton(text="❌ Cancel", callback_data="admin_user_control_start", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]
            ])
            await call.message.edit_text(f"⚠️ Are you sure you want to <b>BAN</b> user <code>{u_id}</code>?", reply_markup=kb, parse_mode='HTML')
            await state.set_state(AdminStates.confirm_ban)
        else:
            db_query("UPDATE users SET is_banned=0 WHERE user_id=?", (u_id,))
            await call.answer("✅ User unbanned successfully!", show_alert=True)
            m = call.message; m.text = str(u_id); await process_user_lookup(m, state)
    elif action == "vip":
        current_status = db_query("SELECT is_vip FROM users WHERE user_id=?", (u_id,), fetchone=True)[0]
        if current_status == 1:
            db_query("UPDATE users SET is_vip=0 WHERE user_id=?", (u_id,))
            await call.answer("✅ VIP Removed!", show_alert=True)
        else:
            db_query("UPDATE users SET is_vip=1, vip_since=? WHERE user_id=?", (datetime.now().strftime("%Y-%m-%d"), u_id))
            await call.answer("✅ VIP Granted!", show_alert=True)
        m = call.message; m.text = str(u_id); await process_user_lookup(m, state)
    elif action == "add":
        await call.message.edit_text("💰 Enter the amount to <b>ADD</b> to this user's wallet:", reply_markup=admin_back_kb(), parse_mode='HTML')
        await state.set_state(AdminStates.wait_for_add_money)
    elif action == "min":
        await call.message.edit_text("💸 Enter the amount to <b>DEDUCT</b> from this user's wallet:", reply_markup=admin_back_kb(), parse_mode='HTML')
        await state.set_state(AdminStates.wait_for_minus_money)
    elif action == "warn":
        await call.message.edit_text("⚠️ Type the strict warning message you want to send directly to this user:", reply_markup=admin_back_kb(), parse_mode='HTML')
        await state.set_state(AdminStates.wait_for_warning)

@dp.callback_query(F.data.startswith("confirm_ban_"))
async def confirm_ban(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    u_id = int(call.data.split("_")[2])
    db_query("UPDATE users SET is_banned=1 WHERE user_id=?", (u_id,))
    await call.answer("🔴 User has been banned!", show_alert=True)
    await state.clear()
    m = call.message; m.text = str(u_id); await process_user_lookup(m, state)

@dp.message(AdminStates.wait_for_add_money)
async def exec_add_money(m: Message, state: FSMContext):
    try:
        amt = float(m.text)
        data = await state.get_data()
        u_id = data['target_u_id']
        db_query("UPDATE users SET balance = balance + ? WHERE user_id=?", (amt, u_id))
        await m.answer(f"✅ Successfully added {fmt_curr(amt)} to target <code>{u_id}</code>.", reply_markup=admin_kb(), parse_mode='HTML')
        try: await bot.send_message(u_id, f"💰 <b>Wallet Top-up!</b>\nAdmin has manually added {fmt_curr(amt)} to your wallet.", parse_mode='HTML')
        except: pass
        await state.clear()
    except ValueError: await m.answer("❌ Critical Error: Input must be a valid number.")

@dp.message(AdminStates.wait_for_minus_money)
async def exec_minus_money(m: Message, state: FSMContext):
    try:
        amt = float(m.text)
        data = await state.get_data()
        u_id = data['target_u_id']
        db_query("UPDATE users SET balance = balance - ? WHERE user_id=?", (amt, u_id))
        await m.answer(f"✅ Successfully deducted {fmt_curr(amt)} from target <code>{u_id}</code>.", reply_markup=admin_kb(), parse_mode='HTML')
        await state.clear()
    except ValueError: await m.answer("❌ Critical Error: Input must be a valid number.")

@dp.message(AdminStates.wait_for_warning)
async def exec_warn_user(m: Message, state: FSMContext):
    data = await state.get_data()
    u_id = data['target_u_id']
    warn_text = m.text
    db_query("UPDATE users SET warnings = warnings + 1 WHERE user_id=?", (u_id,))
    await m.answer(f"✅ Official warning dispatched to <code>{u_id}</code>.", reply_markup=admin_kb(), parse_mode='HTML')
    try: await bot.send_message(u_id, f"⚠️ <b>OFFICIAL WARNING FROM SYSTEM ADMIN:</b>\n\n{warn_text}\n\n<i>Subsequent infractions may lead to an automated grid ban.</i>", parse_mode='HTML')
    except: pass
    await state.clear()

# ==============================================================================
# 18. ADMIN STATISTICS
# ==============================================================================
@dp.callback_query(F.data == "admin_view_stats")
async def admin_dashboard_stats(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    t_users = db_query("SELECT COUNT(*) FROM users", fetchone=True)[0]
    t_resellers = db_query("SELECT COUNT(*) FROM users WHERE is_reseller=1", fetchone=True)[0]
    t_vip = db_query("SELECT COUNT(*) FROM users WHERE is_vip=1", fetchone=True)[0]
    t_prods = db_query("SELECT COUNT(*) FROM products", fetchone=True)[0]
    t_keys = db_query("SELECT COUNT(*) FROM product_keys WHERE is_used=0", fetchone=True)[0]
    t_rev = db_query("SELECT SUM(spent) FROM users", fetchone=True)[0] or 0.0
    today_str = datetime.now().strftime("%Y-%m-%d")
    t_spins = db_query("SELECT COUNT(*) FROM users WHERE last_spin LIKE ?", (f"{today_str}%",), fetchone=True)[0]
    msg = (f"📊 <b><u>GRID INTELLIGENCE DASHBOARD</u></b> 📊\n━━━━━━━━━━━━━━━━━━\n👥 <b>Total Grid Users:</b> {t_users}\n👑 <b>Wholesale Resellers:</b> {t_resellers}\n🌟 <b>Elite VIP Members:</b> {t_vip}\n━━━━━━━━━━━━━━━━━━\n📦 <b>Active Products:</b> {t_prods}\n🔑 <b>Unused Keys in Vault:</b> {t_keys}\n💰 <b>Total Gross Revenue:</b> {fmt_curr(t_rev)}\n🎰 <b>Ludo Spins Today:</b> {t_spins}\n━━━━━━━━━━━━━━━━━━")
    await call.message.edit_text(msg, reply_markup=admin_back_kb(), parse_mode='HTML')

# ==============================================================================
# 19. ADMIN PRODUCT MANAGEMENT
# ==============================================================================
@dp.callback_query(F.data == "admin_add_prod")
async def add_prod_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for cat in FIXED_CATEGORIES:
        emoji_id = get_category_emoji(cat)
        kb.inline_keyboard.append([InlineKeyboardButton(text=cat, callback_data=f"addprod_cat_{cat}", icon_custom_emoji_id=emoji_id, style="danger")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="Cancel", callback_data="admin_panel_back", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")])
    await call.message.edit_text("<b>Step 1:</b> Choose the <b>Category</b> for this product:", reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("addprod_cat_"))
async def add_prod_category_selected(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    category = call.data.split("addprod_cat_", 1)[1]
    await state.update_data(cat=category)
    await call.message.edit_text(f"<b>Step 2:</b> Enter <b>PANEL NAME</b>\n(e.g., 'MST PANEL', 'DRIP PANEL'):", reply_markup=admin_back_kb(), parse_mode='HTML')
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
    except ValueError: await m.answer("❌ Invalid input datatype! Must be numerical.")

@dp.message(AdminStates.add_prod_reseller_price)
async def add_prod_reseller_price(m: Message, state: FSMContext):
    try:
        await state.update_data(reseller_price=float(m.text))
        await m.answer("🔗 Enter direct APK/Payload Download Link (or type 'none' to omit):", parse_mode='HTML')
        await state.set_state(AdminStates.add_prod_apk)
    except ValueError: await m.answer("❌ Invalid input datatype! Must be numerical.")

@dp.message(AdminStates.add_prod_apk)
async def add_prod_apk(m: Message, state: FSMContext):
    await state.update_data(apk="" if m.text.lower() == 'none' else m.text)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Yes — Generate Key via API", callback_data="addprod_ext_yes", icon_custom_emoji_id=get_emoji_icon("check_icon"), style="success")],
        [InlineKeyboardButton(text="❌ No — Use Manual Keys", callback_data="addprod_ext_no", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]
    ])
    await m.answer("🔗 <b>Does this product use the external API for automatic key generation?</b>", reply_markup=kb, parse_mode='HTML')
    await state.set_state(AdminStates.add_prod_external)

@dp.callback_query(F.data == "addprod_ext_yes", AdminStates.add_prod_external)
async def add_prod_external_yes(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await state.update_data(external_enabled=1)
    await call.message.edit_text("🆔 Enter the <b>External Product ID (PID)</b> used by the API:", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.add_prod_external_product_id)

@dp.message(AdminStates.add_prod_external_product_id)
async def add_prod_external_pid(m: Message, state: FSMContext):
    pid = m.text.strip()
    if not pid:
        return await m.answer("❌ Product PID cannot be empty.")
    data = await state.get_data()
    # The API's duration is NOT necessarily the Telegram package name.
    # Ask for the exact duration/price tier configured on the API.
    await state.update_data(external_product_id=pid, requires_android_id=0)
    await m.answer(
        "⏱ <b>Enter the exact XYZ API duration/price tier.</b>\n\n"
        "Examples: <code>3 Hours</code>, <code>1 Day</code>, <code>7 Days</code>.\n"
        f"Your shop validity is: <code>{data.get('validity', '')}</code>\n\n"
        "If the API uses the same duration, send the same value.",
        reply_markup=admin_back_kb(), parse_mode='HTML'
    )
    await state.set_state(AdminStates.add_prod_external_duration)

@dp.message(AdminStates.add_prod_external_duration)
async def add_prod_external_duration(m: Message, state: FSMContext):
    api_duration = normalize_api_duration(m.text.strip())
    if not api_duration:
        return await m.answer("❌ API duration cannot be empty.")
    data = await state.get_data()
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    c.execute("""INSERT INTO products
        (category, panel_name, name, price_inr, reseller_price, stock, apk_link, validity, device_limit,
         external_enabled, external_product_id, requires_android_id, external_duration)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (data['cat'], data['panel_name'], data['name'], data['price'], data['reseller_price'], 1,
         data['apk'], data['validity'], data['device_limit'], 1, data['external_product_id'], 0, api_duration))
    prod_id = c.lastrowid
    conn.commit(); conn.close()
    await m.answer(
        f"✅ <b>API Product Created!</b>\n\nProduct ID: <code>{prod_id}</code>\n"
        f"External Product ID: <code>{data['external_product_id']}</code>\n"
        f"API Duration: <code>{api_duration}</code>\n\n"
        "The API will generate and deliver the key when the customer buys it.",
        reply_markup=admin_kb(), parse_mode='HTML'
    )
    await state.clear()

@dp.callback_query(F.data == "addprod_ext_no", AdminStates.add_prod_external)
async def add_prod_external_no(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await state.update_data(external_enabled=0, external_product_id="", requires_android_id=0)
    await call.message.edit_text("📥 <b>Manual Key Product</b>\n\nNow send the keys, one key per line:", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.add_prod_keys)

@dp.message(AdminStates.add_prod_keys)
async def add_prod_keys(m: Message, state: FSMContext):
    keys = [k.strip() for k in m.text.strip().split('\n') if k.strip()]
    if not keys:
        return await m.answer("❌ No valid keys found. Send at least one key.")
    data = await state.get_data()
    stock = len(keys)
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    c.execute("""INSERT INTO products
        (category, panel_name, name, price_inr, reseller_price, stock, apk_link, validity, device_limit,
         external_enabled, external_product_id, requires_android_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (data['cat'], data['panel_name'], data['name'], data['price'], data['reseller_price'], stock,
         data['apk'], data['validity'], data['device_limit'], 0, '', 0))
    prod_id = c.lastrowid
    for k in keys:
        c.execute("INSERT INTO product_keys (product_id, key_text) VALUES (?, ?)", (prod_id, k))
    conn.commit(); conn.close()
    await m.answer(f"✅ <b>Manual Product Created!</b>\n\n📦 Product ID: <code>{prod_id}</code>\n🔒 Stock: {stock} keys", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_manage_prods")
async def admin_manage_prods(call: CallbackQuery):
    """Open the product manager using explicit DB columns and safe labels.

    The old version relied on SELECT * and positional indexes. That is fragile
    because the products table has had columns added over time. This version
    uses explicit columns, COALESCE for NULL stock, and short button labels so
    Telegram accepts the keyboard reliably.
    """
    if call.from_user.id != ADMIN_ID:
        await call.answer("⛔ Admin only.", show_alert=True)
        return

    try:
        prods = db_query(
            """SELECT id, name, category, panel_name, COALESCE(stock, 0),
                      COALESCE(is_active, 1)
               FROM products
               ORDER BY category COLLATE NOCASE, panel_name COLLATE NOCASE, name COLLATE NOCASE, id""",
            fetchall=True,
            commit=False
        ) or []

        if not prods:
            await call.answer("📦 No products found.", show_alert=True)
            await call.message.edit_text(
                "📦 <b>Store Database</b>\n\nNo products are currently available.",
                reply_markup=admin_back_kb(),
                parse_mode='HTML'
            )
            return

        kb = InlineKeyboardMarkup(inline_keyboard=[])
        for p_id, name, category, panel_name, stock, is_active in prods:
            status_dot = "🟢" if int(is_active or 0) else "🔴"
            category = str(category or "-")
            panel_name = str(panel_name or "-")
            name = str(name or "-")
            # Keep the label comfortably below Telegram's button-text limits.
            label = f"{status_dot} [{category}] {panel_name} - {name} (Stock: {int(stock or 0)})"
            if len(label) > 60:
                label = label[:57] + "..."
            kb.inline_keyboard.append([
                InlineKeyboardButton(
                    text=label,
                    callback_data=f"admin_view_p_{int(p_id)}",
                    style="primary"
                )
            ])

        kb.inline_keyboard.append([
            InlineKeyboardButton(
                text="🔄 Refresh Products",
                callback_data="admin_manage_prods",
                style="success"
            )
        ])
        kb.inline_keyboard.append([
            InlineKeyboardButton(
                text="Back to Admin",
                callback_data="admin_panel_back",
                icon_custom_emoji_id=get_emoji_icon("back"),
                style="danger"
            )
        ])

        await call.answer()
        await call.message.edit_text(
            f"📦 <b>Manage Products</b>\n\nTotal products: <b>{len(prods)}</b>\nSelect a product below to edit, stock-manage, hide/unhide, or delete.",
            reply_markup=kb,
            parse_mode='HTML'
        )
    except Exception as e:
        logger.exception("Manage Products failed")
        await call.answer("❌ Manage Products error. Check Railway logs.", show_alert=True)

@dp.callback_query(F.data.startswith("admin_view_p_"))
async def admin_view_product(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("⛔ Admin only.", show_alert=True)
        return
    try:
        p_id = int(call.data.rsplit("_", 1)[1])
        prod = db_query(
            """SELECT id, category, panel_name, name, price_inr, reseller_price,
                      COALESCE(stock, 0), apk_link, validity, device_limit,
                      COALESCE(is_active, 1), external_product_id,
                      requires_android_id, external_enabled, api_product_id, external_duration
               FROM products WHERE id=?""",
            (p_id,), fetchone=True, commit=False
        )
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
            [InlineKeyboardButton(text="Edit Panel Group 🏷️", callback_data=f"edit_p_{p_id}_cat", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="primary"),
             InlineKeyboardButton(text="Edit Panel Name 🏷️", callback_data=f"edit_p_{p_id}_panel_name", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="primary")],
            [InlineKeyboardButton(text="Edit Package Name ✏️", callback_data=f"edit_p_{p_id}_name", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="primary")],
            [InlineKeyboardButton(text="Edit Price 💰", callback_data=f"edit_p_{p_id}_price", icon_custom_emoji_id=get_emoji_icon("money_icon"), style="primary"),
             InlineKeyboardButton(text="Edit R-Price 👑", callback_data=f"edit_p_{p_id}_rprice", icon_custom_emoji_id=get_emoji_icon("money_icon"), style="primary")],
            [InlineKeyboardButton(text="Edit Validity ⏳", callback_data=f"edit_p_{p_id}_validity", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="primary"),
             InlineKeyboardButton(text="Edit Device 📱", callback_data=f"edit_p_{p_id}_device", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="primary")],
            [InlineKeyboardButton(text="Edit API Duration ⏱️", callback_data=f"edit_p_{p_id}_api_duration", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="primary")],
            [InlineKeyboardButton(text="Edit APK Link 🔗", callback_data=f"edit_p_{p_id}_apk", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="primary"),
             InlineKeyboardButton(text="Add Keys ➕", callback_data=f"edit_p_{p_id}_keys", icon_custom_emoji_id=get_emoji_icon("add_balance"), style="success")],
            [InlineKeyboardButton(text="Add to Stock 📦➕", callback_data=f"edit_p_{p_id}_stock_add", icon_custom_emoji_id=get_emoji_icon("add_balance"), style="success")],
            [InlineKeyboardButton(text="Delete Key 🗑", callback_data=f"delkey_p_{p_id}", icon_custom_emoji_id=get_emoji_icon("back"), style="danger"),
             InlineKeyboardButton(text=toggle_btn_text, callback_data=f"toggle_p_{p_id}", icon_custom_emoji_id=get_emoji_icon("check_icon"), style="primary")],
            [InlineKeyboardButton(text="Delete Product 🗑", callback_data=f"delete_p_{p_id}", icon_custom_emoji_id=get_emoji_icon("back"), style="danger"),
             InlineKeyboardButton(text="BACK", callback_data="admin_manage_prods", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]
        ])
        await call.answer()
        await call.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True, parse_mode='HTML')
    except Exception as e:
        logger.exception("Error in admin_view_product")
        await call.answer("❌ Could not load this product. Check Railway logs.", show_alert=True)

@dp.callback_query(F.data.startswith("toggle_p_"))
async def admin_toggle_product(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    try:
        p_id = int(call.data.rsplit("_", 1)[1])
        row = db_query("SELECT COALESCE(is_active,1) FROM products WHERE id=?", (p_id,), fetchone=True)
        if not row:
            return await call.answer("❌ Product not found.", show_alert=True)
        new_val = 0 if int(row[0]) == 1 else 1
        db_query("UPDATE products SET is_active=? WHERE id=?", (new_val, p_id))
        await call.answer("✅ Visibility updated!", show_alert=True)
        await admin_view_product(call)
    except Exception:
        logger.exception("Toggle product failed")
        await call.answer("❌ Could not update product visibility.", show_alert=True)

# ==============================================================================
# FIX: Edit product field – correctly handle different data types
# ==============================================================================
@dp.callback_query(F.data.startswith("edit_p_"))
async def start_edit_product(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    parts = call.data.split("_")
    p_id = int(parts[2]); field = "_".join(parts[3:])
    await state.update_data(edit_p_id=p_id, edit_field=field)
    if field == 'keys':
        await call.message.edit_text("📥 <b>Vault Injection</b>\nPaste the <b>NEW KEYS</b> to append to the stock (1 key per line):", reply_markup=admin_back_kb(), parse_mode='HTML')
        await state.set_state(AdminStates.wait_for_add_keys)
    elif field == 'stock_add':
        await call.message.edit_text(
            "📦 <b>Add to Stock</b>\n\nEnter how many stock units to add.\nExample: <code>100</code> or <code>200</code>",
            reply_markup=admin_back_kb(), parse_mode='HTML'
        )
        await state.set_state(AdminStates.wait_for_new_value)
    else:
        field_name_map = {'cat': 'New Panel Group/Category Name', 'panel_name': 'New Panel Name', 'name': 'New Package/Date Name', 'price': 'New Standard Price in ₹', 'rprice': 'New Reseller Price in ₹', 'validity': 'New Time Validity String', 'device': 'New HWID Limit String', 'apk': 'New Payload Link (or type "none")', 'api_duration': 'Exact XYZ API Duration (e.g. 1 Day)', 'stock_add': 'Stock units to add'}
        await call.message.edit_text(f"✏️ Input the required data for: <b>{field_name_map[field]}</b>", reply_markup=admin_back_kb(), parse_mode='HTML')
        await state.set_state(AdminStates.wait_for_new_value)

@dp.message(AdminStates.wait_for_new_value)
async def process_edit_value(m: Message, state: FSMContext):
    data = await state.get_data()
    p_id = data['edit_p_id']; field = data['edit_field']; new_val = m.text.strip()
    
    # If field is price or reseller price, convert to float
    if field in ['price', 'rprice']:
        try:
            new_val = float(new_val)
        except ValueError:
            return await m.answer("❌ Invalid number format. Please enter a valid price (e.g., 500).")
    # If field is apk, store as string (don't convert to float!)
    elif field == 'apk':
        new_val = "" if new_val.lower() == 'none' else new_val
    elif field == 'stock_add':
        try:
            add_qty = int(new_val)
            if add_qty <= 0 or add_qty > 100000:
                raise ValueError
        except ValueError:
            return await m.answer("❌ Enter a positive whole number up to 100000, e.g. 100 or 200.")
        db_query("UPDATE products SET stock=COALESCE(stock,0)+? WHERE id=?", (add_qty, p_id))
        new_stock = db_query("SELECT stock FROM products WHERE id=?", (p_id,), fetchone=True)[0]
        await m.answer(f"✅ <b>Stock Added!</b>\n\n➕ Added: <code>{add_qty}</code>\n📦 New Stock: <code>{new_stock}</code>", reply_markup=admin_kb(), parse_mode='HTML')
        await state.clear()
        return
    # For all other fields (cat, panel_name, name, validity, device), keep as string
    
    db_col_map = {'cat': 'category', 'panel_name': 'panel_name', 'name': 'name', 'price': 'price_inr', 'rprice': 'reseller_price', 'validity': 'validity', 'device': 'device_limit', 'apk': 'apk_link', 'api_duration': 'external_duration'}
    if field not in db_col_map:
        return await m.answer("❌ Unsupported product field.")
    db_query(f"UPDATE products SET {db_col_map[field]}=? WHERE id=?", (new_val, p_id))
    await m.answer("✅ <b>Node updated gracefully!</b>", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.message(AdminStates.wait_for_add_keys)
async def process_add_keys(m: Message, state: FSMContext):
    data = await state.get_data()
    p_id = data['edit_p_id']
    keys = [k.strip() for k in m.text.strip().split('\n') if k.strip()]
    if len(keys) == 0: return await m.answer("❌ Protocol breach: Zero valid keys found.", reply_markup=admin_kb(), parse_mode='HTML')
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    for k in keys: c.execute("INSERT INTO product_keys (product_id, key_text) VALUES (?, ?)", (p_id, k))
    c.execute("UPDATE products SET stock = COALESCE(stock,0) + ? WHERE id=?", (len(keys), p_id))
    conn.commit(); conn.close()
    await m.answer(f"✅ <b>Vault Secure!</b> {len(keys)} new keys appended and encrypted.", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data.startswith("delete_p_"))
async def admin_delete_product(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    try:
        p_id = int(call.data.rsplit("_", 1)[1])
        conn = sqlite3.connect(DB_PATH, timeout=20)
        try:
            c = conn.cursor()
            c.execute("SELECT id FROM products WHERE id=?", (p_id,))
            if not c.fetchone():
                await call.answer("❌ Product not found.", show_alert=True)
                return
            c.execute("DELETE FROM product_keys WHERE product_id=?", (p_id,))
            c.execute("DELETE FROM products WHERE id=?", (p_id,))
            conn.commit()
        finally:
            conn.close()
        await call.answer("✅ Product and its unused key records deleted.", show_alert=True)
        await admin_manage_prods(call)
    except Exception:
        logger.exception("Delete product failed")
        await call.answer("❌ Could not delete product. Check Railway logs.", show_alert=True)

@dp.callback_query(F.data.startswith("delkey_p_"))
async def admin_delete_key_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    p_id = int(call.data.split("_")[2])
    await state.update_data(del_p_id=p_id)
    await call.message.edit_text("🗑 Send the <b>exact string match</b> of the key you wish to purge from the vault:", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_delete_key)

@dp.message(AdminStates.wait_for_delete_key)
async def process_delete_key(m: Message, state: FSMContext):
    data = await state.get_data()
    p_id = data['del_p_id']
    key_to_delete = m.text.strip()
    key_data = db_query("SELECT id, is_used FROM product_keys WHERE product_id=? AND key_text=?", (p_id, key_to_delete), fetchone=True)
    if not key_data: return await m.answer("❌ Key not found. Check logs and try again.", reply_markup=admin_back_kb(), parse_mode='HTML')
    if key_data[1] == 1: return await m.answer("⚠️ Action Blocked: This key has already been dispatched to a user.", reply_markup=admin_back_kb(), parse_mode='HTML')
    db_query("DELETE FROM product_keys WHERE id=?", (key_data[0],))
    db_query("UPDATE products SET stock = stock - 1 WHERE id=?", (p_id,))
    await m.answer(f"✅ Key <code>{key_to_delete}</code> securely purged from vault.\n📦 Database indices updated.", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

# ==============================================================================
# 20. ADMIN TICKETS, BROADCAST, COUPONS
# ==============================================================================
@dp.callback_query(F.data == "admin_view_tickets")
async def admin_view_tickets(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    tickets = db_query("SELECT id, user_id, message, created_at FROM tickets WHERE status='Open' LIMIT 1", fetchall=True)
    if not tickets: return await call.answer("✅ Zero pending issues. Grid is clean!", show_alert=True)
    t = tickets[0]
    text = (f"🎫 <b><u>ACTIVE TICKET #{t[0]}</u></b>\n👤 <b>Origin UID:</b> <code>{t[1]}</code>\n📅 <b>Timestamp:</b> {t[3]}\n\n📝 <b>Payload:</b>\n{t[2]}")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Formulate Reply", callback_data=f"reply_ticket_{t[0]}_{t[1]}", icon_custom_emoji_id=get_emoji_icon("telegram"), style="primary")],
        [InlineKeyboardButton(text="❌ Force Close Ticket", callback_data=f"close_ticket_{t[0]}", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")],
        [InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("close_ticket_"))
async def close_ticket(call: CallbackQuery):
    ticket_id = call.data.split("_")[2]
    db_query("UPDATE tickets SET status='Closed' WHERE id=?", (ticket_id,))
    await call.answer("✅ Status set to Closed.", show_alert=True)
    await admin_view_tickets(call) 

@dp.callback_query(F.data.startswith("reply_ticket_"))
async def reply_ticket_start(call: CallbackQuery, state: FSMContext):
    data = call.data.split("_")
    ticket_id, user_id = data[2], data[3]
    await state.update_data(ticket_id=ticket_id, user_id=user_id)
    await call.message.edit_text(f"💬 Formulating reply for node <code>{user_id}</code>.\n\nType your message payload:", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.ticket_reply_msg)

@dp.message(AdminStates.ticket_reply_msg)
async def send_ticket_reply(m: Message, state: FSMContext):
    data = await state.get_data()
    try:
        await bot.send_message(data['user_id'], f"📞 <b>Admin Reply (Ref #{data['ticket_id']}):</b>\n\n{m.text}", parse_mode='HTML')
        db_query("UPDATE tickets SET status='Closed' WHERE id=?", (data['ticket_id'],))
        await m.answer("✅ Payload delivered and connection closed successfully.", reply_markup=admin_kb(), parse_mode='HTML')
    except Exception as e: await m.answer(f"❌ Transmission Error: {e}", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_broadcast_btn")
async def admin_broadcast_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("📢 <b>Mass Broadcast Protocol</b>\n\nSend the rich message payload you wish to transmit globally across the grid:", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.broadcast_msg)

@dp.message(AdminStates.broadcast_msg)
async def admin_broadcast_send(message: Message, state: FSMContext):
    users = db_query("SELECT user_id FROM users", fetchall=True)
    sent, failed = 0, 0
    m = await message.answer("⏳ Broadcast protocol initiated... Do not interrupt.", parse_mode='HTML')
    for u in users:
        try:
            await message.send_copy(chat_id=u[0])
            sent += 1
        except Exception: failed += 1
        await asyncio.sleep(0.06) 
    await m.edit_text(f"✅ <b>Global Broadcast Complete!</b>\n\n🟢 Nodes reached: {sent}\n🔴 Nodes failed/blocked: {failed}", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_create_coupon")
async def admin_create_coupon_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("🎟 Enter a highly secure alphanumeric sequence for the Promo Code:", reply_markup=admin_back_kb(), parse_mode='HTML')
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
    except ValueError: await m.answer("❌ Non-numerical data detected. Aborting.")

@dp.message(AdminStates.add_coupon_uses)
async def admin_coupon_uses(m: Message, state: FSMContext):
    try:
        uses = int(m.text)
        data = await state.get_data()
        db_query("INSERT OR REPLACE INTO coupons (code, amount, uses_left) VALUES (?, ?, ?)", (data['code'], data['amount'], uses))
        await m.answer(f"✅ Protocol <b>{data['code']}</b> encoded!\nReward Vector: {fmt_curr(data['amount'])}\nThreshold Limit: {uses} executions.", reply_markup=admin_kb(), parse_mode='HTML')
        await state.clear()
    except ValueError: await m.answer("❌ Non-numerical data detected. Aborting.")

# ==============================================================================
# 21. ADMIN RESELLER & SPIN SETTINGS
# ==============================================================================
@dp.callback_query(F.data == "admin_reseller_menu")
async def admin_reseller_menu(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    status_check = db_query("SELECT value FROM settings WHERE key='reseller_system_status'", fetchone=True)
    sys_status = status_check[0] if status_check else "ON"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Grant Reseller Rights", callback_data="reseller_make", icon_custom_emoji_id=get_emoji_icon("reseller"), style="success"), 
         InlineKeyboardButton(text="➖ Revoke Reseller", callback_data="reseller_remove", icon_custom_emoji_id=get_emoji_icon("reseller"), style="danger")],
        [InlineKeyboardButton(text="📋 Audit Active Resellers", callback_data="reseller_view", icon_custom_emoji_id=get_emoji_icon("history"), style="primary")],
        [InlineKeyboardButton(text=f"{'🟢' if sys_status == 'ON' else '🔴'} Auto-Upgrade System: {sys_status}", callback_data="admin_toggle_reseller_sys", icon_custom_emoji_id=get_emoji_icon("check_icon"), style="success" if sys_status == 'ON' else "danger")], 
        [InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]
    ])
    await call.message.edit_text("👑 <b>Wholesale Reseller Protocols</b>\nSelect administrative action:", reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "admin_toggle_reseller_sys")
async def toggle_reseller_sys(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    res = db_query("SELECT value FROM settings WHERE key='reseller_system_status'", fetchone=True)
    current = res[0] if res else 'ON'
    new_status = 'OFF' if current == 'ON' else 'ON'
    db_query("INSERT OR REPLACE INTO settings (key, value) VALUES ('reseller_system_status', ?)", (new_status,))
    await admin_reseller_menu(call)

@dp.callback_query(F.data.in_(["reseller_make", "reseller_remove"]))
async def reseller_prompt_id(call: CallbackQuery, state: FSMContext):
    action = call.data
    await state.update_data(reseller_action=action)
    await call.message.edit_text("👤 Identify target node. Input <b>User ID</b> or <b>@username</b>:", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.reseller_manage_id)

@dp.message(AdminStates.reseller_manage_id)
async def process_reseller_manage(m: Message, state: FSMContext):
    data = await state.get_data()
    target = m.text.strip()
    if target.startswith('@'): target = target[1:]
    user_q = db_query("SELECT user_id, first_name FROM users WHERE user_id=? OR username=? COLLATE NOCASE", (target, target), fetchone=True)
    if not user_q: return await m.answer("❌ Target completely ghosted. Not in database.", reply_markup=admin_back_kb(), parse_mode='HTML')
    u_id, u_name = user_q[0], user_q[1]
    if data['reseller_action'] == "reseller_make":
        db_query("UPDATE users SET is_reseller=1, reseller_since=?, account_type='Reseller' WHERE user_id=?", (datetime.now().strftime("%Y-%m-%d"), u_id))
        await m.answer(f"✅ Credentials upgraded. <b>{u_name}</b> (<code>{u_id}</code>) has reseller rights.", reply_markup=admin_kb(), parse_mode='HTML')
    else:
        db_query("UPDATE users SET is_reseller=0, account_type='Regular' WHERE user_id=?", (u_id,))
        await m.answer(f"✅ Credentials revoked. <b>{u_name}</b> (<code>{u_id}</code>) is back to regular user.", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "reseller_view")
async def reseller_view(call: CallbackQuery):
    resellers = db_query("SELECT user_id, first_name, username FROM users WHERE is_reseller=1", fetchall=True)
    if not resellers: return await call.message.edit_text("📋 Zero active resellers found.", reply_markup=admin_back_kb(), parse_mode='HTML')
    text = "👑 <b><u>ACTIVE RESELLER AUDIT LOG</u></b> 👑\n━━━━━━━━━━━━━━━━━━\n"
    for r in resellers:
        uname = f"(@{r[2]})" if r[2] else ""
        text += f"👤 {r[1]} {uname}\n🆔 <code>{r[0]}</code>\n\n"
    await call.message.edit_text(text, reply_markup=admin_back_kb(), parse_mode='HTML')

@dp.callback_query(F.data == "admin_spin_menu")
async def admin_spin_menu(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    status = db_query("SELECT value FROM settings WHERE key='spin_status'", fetchone=True)
    limit = db_query("SELECT value FROM settings WHERE key='daily_spin_limit'", fetchone=True)
    status_val = status[0] if status else 'ON'
    limit_val = limit[0] if limit else '50.0'
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Append Reward Logic", callback_data="spin_add", icon_custom_emoji_id=get_emoji_icon("add_balance"), style="success"), 
         InlineKeyboardButton(text="❌ Drop Reward Logic", callback_data="spin_del", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")],
        [InlineKeyboardButton(text="📋 Audit Configs", callback_data="spin_view", icon_custom_emoji_id=get_emoji_icon("history"), style="primary"), 
         InlineKeyboardButton(text="⚙️ Throttle Limits", callback_data="spin_limit", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="primary")],
        [InlineKeyboardButton(text=f"{'🟢' if status_val == 'ON' else '🔴'} Master Toggle: {status_val}", callback_data="spin_toggle", icon_custom_emoji_id=get_emoji_icon("check_icon"), style="success" if status_val == 'ON' else "danger")],
        [InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]
    ])
    await call.message.edit_text(f"🎰 <b>Advanced Ludo/Spin Algorithms</b>\nCurrent Threshold: ₹{limit_val}", reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "spin_toggle")
async def spin_toggle(call: CallbackQuery):
    res = db_query("SELECT value FROM settings WHERE key='spin_status'", fetchone=True)
    current = res[0] if res else 'ON'
    new_status = 'OFF' if current == 'ON' else 'ON'
    db_query("INSERT OR REPLACE INTO settings (key, value) VALUES ('spin_status', ?)", (new_status,))
    await admin_spin_menu(call)

@dp.callback_query(F.data == "admin_toggle_bot")
async def toggle_bot(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    res = db_query("SELECT value FROM settings WHERE key='bot_status'", fetchone=True)
    current = res[0] if res else 'ON'
    new_status = 'OFF' if current == 'ON' else 'ON'
    db_query("INSERT OR REPLACE INTO settings (key, value) VALUES ('bot_status', ?)", (new_status,))
    await call.message.edit_reply_markup(reply_markup=admin_kb())

@dp.callback_query(F.data == "spin_add")
async def spin_add_start(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("🎰 Inject new decimal logic limit (e.g. 15.50):", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.spin_add_reward)

@dp.message(AdminStates.spin_add_reward)
async def spin_add_exec(m: Message, state: FSMContext):
    try:
        amt = float(m.text)
        db_query("INSERT INTO spin_rewards (amount) VALUES (?)", (amt,))
        await m.answer(f"✅ Algorithm updated. New vector {fmt_curr(amt)} injected.", reply_markup=admin_kb(), parse_mode='HTML')
        await state.clear()
    except ValueError: await m.answer("❌ Math parsing error.")

@dp.callback_query(F.data == "spin_view")
async def spin_view(call: CallbackQuery):
    rewards = db_query("SELECT amount FROM spin_rewards ORDER BY amount ASC", fetchall=True)
    text = "🎰 <b>Live Ludo Constants</b>\n\n"
    for r in rewards: text += f"🎁 {fmt_curr(r[0])}\n"
    await call.message.edit_text(text, reply_markup=admin_back_kb(), parse_mode='HTML')

@dp.callback_query(F.data == "admin_set_video")
async def admin_set_video_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("📹 Input direct streaming / YouTube Link for Tutorial system:\n<i>(Or type 'None' to clear registry):</i>", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_howto_video)

@dp.message(AdminStates.wait_for_howto_video)
async def exec_set_video(m: Message, state: FSMContext):
    link = m.text.strip()
    db_query("INSERT OR REPLACE INTO settings (key, value) VALUES ('how_to_video', ?)", (link,))
    await m.answer("✅ Routing complete. Video linked.", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_set_all_files")
async def admin_set_all_files_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("🔗 Input the direct Channel / Cloud URL for 'Download Files' button:\n<i>(Or type 'None' to format data):</i>", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_all_files_link)

@dp.message(AdminStates.wait_for_all_files_link)
async def exec_set_all_files(m: Message, state: FSMContext):
    link = m.text.strip()
    db_query("INSERT OR REPLACE INTO settings (key, value) VALUES ('all_files_link', ?)", (link,))
    await m.answer("✅ Global resource variable updated.", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_edit_emojis")
async def admin_edit_emojis(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    rows = db_query("SELECT key, value FROM settings WHERE key LIKE 'emoji_%' ORDER BY key", fetchall=True)
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for row in rows:
        key = row[0]
        slot = key.replace("emoji_", "")
        current_id = row[1] if row[1] else "Not set"
        kb.inline_keyboard.append([InlineKeyboardButton(text=f"{slot} (ID: {current_id})", callback_data=f"edit_emoji_{slot}", style="primary")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")])
    await call.message.edit_text("🎨 <b>Edit All Emojis</b>\nChoose an emoji slot to change its ID:", reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("edit_emoji_"))
async def admin_edit_emoji_prompt(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    slot = call.data.split("edit_emoji_", 1)[1]
    await state.update_data(emoji_slot=slot)
    current = get_setting(f"emoji_{slot}", "Not set")
    await call.message.edit_text(f"✏️ Enter new emoji ID for <b>{slot}</b>:\nCurrent: {current}\n(Leave empty to reset to default)", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_emoji_slot)

@dp.message(AdminStates.wait_for_emoji_slot)
async def save_emoji_slot(m: Message, state: FSMContext):
    data = await state.get_data()
    slot = data['emoji_slot']
    new_id = m.text.strip()
    if new_id == "":
        db_query("DELETE FROM settings WHERE key=?", (f"emoji_{slot}",))
        await m.answer(f"✅ Reset emoji for '{slot}' to default.", reply_markup=admin_kb(), parse_mode='HTML')
    else:
        if not new_id.isdigit():
            await m.answer("❌ Invalid ID! Must be numeric.", reply_markup=admin_kb(), parse_mode='HTML')
            return
        set_setting(f"emoji_{slot}", new_id)
        await m.answer(f"✅ Emoji for '{slot}' updated to ID {new_id}.", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_edit_ui_menu")
async def admin_edit_ui_menu(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Edit Start Menu Text", callback_data="edit_ui_start", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="primary")],
        [InlineKeyboardButton(text="Edit Download Files Text", callback_data="edit_ui_download", icon_custom_emoji_id=get_emoji_icon("download"), style="primary")],
        [InlineKeyboardButton(text="Edit VIP Menu Text", callback_data="edit_ui_vip", icon_custom_emoji_id=get_emoji_icon("vip"), style="primary")],
        [InlineKeyboardButton(text="Edit Lucky Dice Text", callback_data="edit_ui_dice", icon_custom_emoji_id=get_emoji_icon("ludo_spin"), style="primary")],
        [InlineKeyboardButton(text="Edit Add Balance Text", callback_data="edit_ui_add_balance", icon_custom_emoji_id=get_emoji_icon("add_balance"), style="primary")],
        [InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]
    ])
    await call.message.edit_text("✏️ <b>Edit User Interface Texts</b>\nSelect which text you want to modify:", reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("edit_ui_"))
async def admin_edit_ui_prompt(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    ui_key = call.data.split("_")[2]
    await state.update_data(ui_key=ui_key)
    current_text = get_ui_text(ui_key)
    await call.message.edit_text(f"📝 Send the new text for <b>{ui_key.upper()}</b> menu.\n\nCurrent text:\n{current_text}", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.edit_ui_text)

@dp.message(AdminStates.edit_ui_text)
async def admin_save_ui_text(m: Message, state: FSMContext):
    data = await state.get_data()
    ui_key = data['ui_key']
    new_text = m.text
    db_query("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (f"ui_{ui_key}", new_text))
    await m.answer(f"✅ UI text <b>{ui_key}</b> updated successfully!", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_edit_reseller_price")
async def admin_edit_reseller_price_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    prods = db_query("SELECT id, name, category, panel_name, reseller_price FROM products", fetchall=True)
    prods = sorted(prods or [], key=lambda row: (natural_sort_key(row[2]), natural_sort_key(row[3]), natural_sort_key(row[1])))
    if not prods: return await call.message.edit_text("No products to edit.", reply_markup=admin_back_kb(), parse_mode='HTML')
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for p in prods:
        panel_name = p[3] if p[3] is not None else ""
        r_price = float(p[4]) if p[4] is not None else 0.0
        kb.inline_keyboard.append([InlineKeyboardButton(text=f"{p[2]} - {panel_name} - {p[1]} (₹{r_price:.2f})", callback_data=f"edit_reseller_{p[0]}", icon_custom_emoji_id=get_emoji_icon("money_icon"), style="primary")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")])
    await call.message.edit_text("👑 <b>Edit Reseller Price per Product</b>\nSelect a product to change its wholesale price:", reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("edit_reseller_"))
async def admin_edit_reseller_price_prompt(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    prod_id = int(call.data.split("_")[2])
    await state.update_data(edit_reseller_prod_id=prod_id)
    await call.message.edit_text("💰 Enter the new <b>Reseller Price</b> in Rupees (₹) for this product:", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.edit_reseller_price)

@dp.message(AdminStates.edit_reseller_price)
async def admin_save_reseller_price(m: Message, state: FSMContext):
    try:
        new_price = float(m.text)
        data = await state.get_data()
        prod_id = data['edit_reseller_prod_id']
        db_query("UPDATE products SET reseller_price=? WHERE id=?", (new_price, prod_id))
        await m.answer(f"✅ Reseller price updated to {fmt_curr(new_price)} for product ID {prod_id}.", reply_markup=admin_kb(), parse_mode='HTML')
        await state.clear()
    except ValueError: await m.answer("❌ Invalid number. Please enter a valid price.")

@dp.callback_query(F.data == "admin_set_reseller_fee")
async def admin_set_reseller_fee(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("💰 Enter the new <b>Reseller Setup Fee</b> in Rupees (₹):\nCurrent: " + get_setting("reseller_setup_fee", "200.0"), reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_reseller_setup_fee)

@dp.message(AdminStates.wait_for_reseller_setup_fee)
async def admin_save_reseller_fee(m: Message, state: FSMContext):
    try:
        fee = float(m.text)
        set_setting("reseller_setup_fee", str(fee))
        await m.answer(f"✅ Reseller setup fee updated to {fmt_curr(fee)}.", reply_markup=admin_kb(), parse_mode='HTML')
        await state.clear()
    except ValueError: await m.answer("❌ Invalid number. Please enter a valid amount.")

@dp.callback_query(F.data == "admin_set_reseller_min")
async def admin_set_reseller_min(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("💳 Enter the new <b>Minimum Balance</b> required to become reseller (₹):\nCurrent: " + get_setting("reseller_min_balance", "500.0"), reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_reseller_min_balance)

@dp.message(AdminStates.wait_for_reseller_min_balance)
async def admin_save_reseller_min(m: Message, state: FSMContext):
    try:
        min_bal = float(m.text)
        set_setting("reseller_min_balance", str(min_bal))
        await m.answer(f"✅ Minimum reseller balance updated to {fmt_curr(min_bal)}.", reply_markup=admin_kb(), parse_mode='HTML')
        await state.clear()
    except ValueError: await m.answer("❌ Invalid number. Please enter a valid amount.")

@dp.callback_query(F.data == "admin_set_support_links")
async def admin_set_support_links(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📞 Set Telegram Link", callback_data="admin_set_telegram", icon_custom_emoji_id=get_emoji_icon("telegram"), style="primary")],
        [InlineKeyboardButton(text="📱 Set WhatsApp Link", callback_data="admin_set_whatsapp", icon_custom_emoji_id=get_emoji_icon("whatsapp"), style="primary")],
        [InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]
    ])
    await call.message.edit_text("📌 <b>Support Contact Links</b>\nSet the URLs for Telegram and WhatsApp support:", reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "admin_set_telegram")
async def admin_set_telegram(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("✈️ Enter the Telegram contact URL (e.g., https://t.me/YOUR_SUPPORT):", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_support_telegram)

@dp.message(AdminStates.wait_for_support_telegram)
async def save_telegram_link(m: Message, state: FSMContext):
    link = m.text.strip()
    set_setting("support_telegram", link)
    await m.answer("✅ Telegram support link updated!", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_set_whatsapp")
async def admin_set_whatsapp(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("📱 Enter the WhatsApp contact URL (e.g., https://wa.me/1234567890):", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_support_whatsapp)

@dp.message(AdminStates.wait_for_support_whatsapp)
async def save_whatsapp_link(m: Message, state: FSMContext):
    link = m.text.strip()
    set_setting("support_whatsapp", link)
    await m.answer("✅ WhatsApp support link updated!", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_set_category_emojis")
async def admin_set_category_emojis(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for cat in FIXED_CATEGORIES:
        current = get_setting(f"cat_emoji_{cat}", "Not set")
        kb.inline_keyboard.append([InlineKeyboardButton(text=f"{cat} (ID: {current})", callback_data=f"set_cat_emoji_{cat}", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="primary")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")])
    await call.message.edit_text("🎨 <b>Set Category Emojis</b>\nChoose a category to set its custom emoji ID:", reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("set_cat_emoji_"))
async def admin_set_category_emoji_prompt(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    category = call.data.split("set_cat_emoji_", 1)[1]
    await state.update_data(cat_emoji_category=category)
    await call.message.edit_text(f"🎨 Enter the emoji ID for <b>{category}</b>:\n(Leave empty to reset to default)", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_category_emoji)

@dp.message(AdminStates.wait_for_category_emoji)
async def save_category_emoji(m: Message, state: FSMContext):
    data = await state.get_data()
    category = data['cat_emoji_category']
    emoji_id = m.text.strip()
    if emoji_id == "":
        db_query("DELETE FROM settings WHERE key=?", (f"cat_emoji_{category}",))
        await m.answer(f"✅ Reset emoji for {category} to default.", reply_markup=admin_kb(), parse_mode='HTML')
    else:
        if not emoji_id.isdigit():
            await m.answer("❌ Invalid ID! Must be numeric.", reply_markup=admin_kb(), parse_mode='HTML')
            return
        set_setting(f"cat_emoji_{category}", emoji_id)
        await m.answer(f"✅ Emoji set for {category} successfully!", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_set_panel_emojis")
async def admin_set_panel_emojis(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    panels = db_query("SELECT DISTINCT panel_name FROM products WHERE panel_name != '' ORDER BY panel_name", fetchall=True)
    if not panels:
        await call.message.edit_text("No panel names found in products.", reply_markup=admin_back_kb(), parse_mode='HTML')
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for p in panels:
        panel = p[0]
        current = get_setting(f"panel_emoji_{panel}", "Not set")
        kb.inline_keyboard.append([InlineKeyboardButton(text=f"{panel} (ID: {current})", callback_data=f"set_panel_emoji_{panel}", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="primary")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="Back to Admin", callback_data="admin_panel_back", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")])
    await call.message.edit_text("🖼 <b>Set Panel Emojis</b>\nChoose a panel name to set its custom emoji ID:", reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data.startswith("set_panel_emoji_"))
async def admin_set_panel_emoji_prompt(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    panel_name = call.data.split("set_panel_emoji_", 1)[1]
    await state.update_data(panel_emoji_name=panel_name)
    await call.message.edit_text(f"🎨 Enter the emoji ID for panel <b>{panel_name}</b>:\n(Leave empty to reset to default)", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_panel_emoji_id)

@dp.message(AdminStates.wait_for_panel_emoji_id)
async def save_panel_emoji(m: Message, state: FSMContext):
    data = await state.get_data()
    panel_name = data['panel_emoji_name']
    emoji_id = m.text.strip()
    if emoji_id == "":
        db_query("DELETE FROM settings WHERE key=?", (f"panel_emoji_{panel_name}",))
        await m.answer(f"✅ Reset emoji for panel '{panel_name}'.", reply_markup=admin_kb(), parse_mode='HTML')
    else:
        if not emoji_id.isdigit():
            await m.answer("❌ Invalid ID! Must be numeric.", reply_markup=admin_kb(), parse_mode='HTML')
            return
        set_setting(f"panel_emoji_{panel_name}", emoji_id)
        await m.answer(f"✅ Emoji set for panel '{panel_name}'!", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_setup_zapupi")
async def setup_zapupi_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("⚙️ <b>ZAPUPI SECURITY DEPLOYMENT</b>\nInput master <b>API Key (zap_key)</b>:\n<i>(Type /cancel to abort sequence)</i>", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_zapupi_api)

@dp.message(AdminStates.wait_for_zapupi_api)
async def zapupi_api(m: Message, state: FSMContext):
    if m.text == '/cancel':
        await state.clear()
        return await m.answer("Sequence killed.", reply_markup=admin_kb(), parse_mode='HTML')
    db_query("INSERT OR REPLACE INTO settings (key, value) VALUES ('zapupi_api', ?)", (m.text.strip(),))
    await m.answer("✅ <b>Keys synchronized with ZapUPI backbone.</b>", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_setup_binance")
async def setup_binance_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("🪙 <b>CRYPTO NODE INIT: Step 1/3</b>\nInput Master <b>Binance API Key</b>:\n<i>(Type /cancel to halt protocol)</i>", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_binance_api)

@dp.message(AdminStates.wait_for_binance_api)
async def setup_binance_api(m: Message, state: FSMContext):
    if m.text == '/cancel':
        await state.clear()
        return await m.answer("Sequence aborted.", reply_markup=admin_kb(), parse_mode='HTML')
    db_query("INSERT OR REPLACE INTO settings (key, value) VALUES ('binance_api', ?)", (m.text.strip(),))
    await m.answer("🪙 <b>CRYPTO NODE INIT: Step 2/3</b>\nNow inject the highly secure <b>Binance Secret Key</b>:", parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_binance_secret)

@dp.message(AdminStates.wait_for_binance_secret)
async def setup_binance_secret(m: Message, state: FSMContext):
    db_query("INSERT OR REPLACE INTO settings (key, value) VALUES ('binance_secret', ?)", (m.text.strip(),))
    await m.answer("🪙 <b>CRYPTO NODE INIT: Step 3/3</b>\nFinal variable: Set the public <b>USDT Deposit Address (TRC20/BEP20)</b>\nUsers will broadcast to this ledger:", parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_binance_address)

@dp.message(AdminStates.wait_for_binance_address)
async def setup_binance_address(m: Message, state: FSMContext):
    db_query("INSERT OR REPLACE INTO settings (key, value) VALUES ('binance_address', ?)", (m.text.strip(),))
    await m.answer("✅ <b>Blockchain node synchronized.</b> Crypto gateway is fully armed.", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

# ==============================================================================
# 22. BOOTSTRAPPING & MAIN
# ==============================================================================
async def main() -> None:
    init_db()
    logger.info("Initializing DB structure...")
    migrate_categories()
    asyncio.create_task(auto_verify_task())
    logger.info("ZapUPI Auto-Verifier Daemon Running in Background.")
    logger.info("🚀 CORE SYSTEM IS FULLY OPERATIONAL...")
    try:
        await dp.start_polling(bot)
    except Exception as err:
        logger.error(f"Critical System Failure in Polling: {err}")
    finally:
        await bot.session.close()

# ==============================================================================
# EXTERNAL KEY GENERATION API
# ==============================================================================
def normalize_api_duration(duration: str) -> str:
    """Normalize the package name into the duration format expected by the API."""
    value = str(duration or "").strip()
    if not value:
        return value

    # Normalize whitespace
    value = re.sub(r"\s+", " ", value)
    
    # Define exact mapping for your API format
    # API expects: "X Hours" or "X DaYs" (with capital D and Y)
    duration_map = {
        # Hours - API expects "X Hours"
        "1 hour": "1 Hours",
        "1 hours": "1 Hours",
        "1hr": "1 Hours",
        "1h": "1 Hours",
        "2 hour": "2 Hours",
        "2 hours": "2 Hours",
        "2hr": "2 Hours",
        "3 hour": "3 Hours",
        "3 hours": "3 Hours",
        "3hr": "3 Hours",
        "6 hour": "6 Hours",
        "6 hours": "6 Hours",
        "6hr": "6 Hours",
        "12 hour": "12 Hours",
        "12 hours": "12 Hours",
        "12hr": "12 Hours",
        
        # Days - API expects "X DaYs" (capital D and Y)
        "1 day": "1 DaYs",
        "1 days": "1 DaYs",
        "1d": "1 DaYs",
        "2 day": "2 DaYs",
        "2 days": "2 DaYs",
        "2d": "2 DaYs",
        "3 day": "3 DaYs",
        "3 days": "3 DaYs",
        "3d": "3 DaYs",
        "5 day": "5 DaYs",
        "5 days": "5 DaYs",
        "5d": "5 DaYs",
        "7 day": "7 DaYs",
        "7 days": "7 DaYs",
        "7d": "7 DaYs",
    }
    
    # Check exact matches first (case insensitive)
    lower_val = value.lower()
    for pattern, result in duration_map.items():
        if lower_val == pattern.lower():
            return result
    
    # Check if it's already in correct format
    if value in ["1 Hours", "2 Hours", "3 Hours", "6 Hours", "12 Hours", 
                 "1 DaYs", "2 DaYs", "3 DaYs", "5 DaYs", "7 DaYs"]:
        return value
    
    # Try to extract number and determine unit
    match = re.match(r"(\d+)\s*(hour|hours|hr|hrs|h|day|days|d)", value, re.IGNORECASE)
    if match:
        number = match.group(1)
        unit = match.group(2).lower()
        if unit in ["hour", "hours", "hr", "hrs", "h"]:
            return f"{number} Hours"
        if unit in ["day", "days", "d"]:
            return f"{number} DaYs"
    
    # If it's just a number, treat as hours
    if value.isdigit():
        return f"{value} Hours"
    
    # Return as-is if nothing matches (fallback)
    return value

async def fetch_external_key(product_id: str, duration: str, android_id: str = "") -> dict:
    """Generate one key. This is a non-idempotent BUY endpoint, so never retry automatically."""
    url = get_setting("external_api_url", "https://adminpanels.shop/api/reseller_v1.php").strip()
    api_key = get_setting("external_api_key", "").strip()
    master_key = get_setting("external_master_key", "").strip()
    if not url or not api_key:
        return {"status": "error", "msg": "External API configuration is incomplete"}
    product_id = str(product_id or "").strip(); duration = str(duration or "").strip()
    if not product_id or not duration:
        return {"status": "error", "msg": "External API product/duration is empty"}
    data = {"api_key": api_key, "action": "buy", "product_id": product_id, "duration": duration}
    if android_id: data["android_id"] = str(android_id).strip()
    headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json, text/plain, */*"}
    if master_key: headers["x-master-key"] = master_key
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
    url = get_setting("external_api_url", "")
    key = get_setting("external_api_key", "")
    master = get_setting("external_master_key", "")
    mask = lambda x: (x[:4] + "••••" + x[-4:]) if len(x) > 8 else ("Configured" if x else "Not set")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Set API URL", callback_data="admin_set_ext_url", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="primary")],
        [InlineKeyboardButton(text="Set API Key", callback_data="admin_set_ext_key", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="primary")],
        [InlineKeyboardButton(text="Set Master Key", callback_data="admin_set_ext_master", icon_custom_emoji_id=get_emoji_icon("info_icon"), style="primary")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="admin_panel_back", icon_custom_emoji_id=get_emoji_icon("back"), style="danger")]
    ])
    text = ("🔗 <b>External Key API Configuration</b>\n\n"
            f"URL: <code>{url or 'Not set'}</code>\n"
            f"API Key: <code>{mask(key)}</code>\n"
            f"Master Key: <code>{mask(master)}</code>\n\n"
            "Products can be switched to API generation from the Add Product flow.")
    await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query(F.data == "admin_set_ext_url")
async def set_ext_url(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("Enter External API URL:", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_ext_url)

@dp.message(AdminStates.wait_for_ext_url)
async def save_ext_url(m: Message, state: FSMContext):
    value = m.text.strip()
    if not value.startswith(("http://", "https://")):
        return await m.answer("❌ URL must start with http:// or https://")
    set_setting("external_api_url", value)
    await m.answer("✅ External API URL saved.", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_set_ext_key")
async def set_ext_key(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("Enter External API Key:", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_ext_key)

@dp.message(AdminStates.wait_for_ext_key)
async def save_ext_key(m: Message, state: FSMContext):
    set_setting("external_api_key", m.text.strip())
    await m.answer("✅ External API Key saved.", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

@dp.callback_query(F.data == "admin_set_ext_master")
async def set_ext_master(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("Enter External API Master Key:", reply_markup=admin_back_kb(), parse_mode='HTML')
    await state.set_state(AdminStates.wait_for_ext_master)

@dp.message(AdminStates.wait_for_ext_master)
async def save_ext_master(m: Message, state: FSMContext):
    set_setting("external_master_key", m.text.strip())
    await m.answer("✅ External API Master Key saved.", reply_markup=admin_kb(), parse_mode='HTML')
    await state.clear()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("System shutting down gracefully. Goodbye.")