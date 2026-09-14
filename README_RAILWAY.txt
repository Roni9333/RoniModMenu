RAILWAY DEPLOYMENT — OLD DATABASE INCLUDED

1. Upload every file in this folder to a GitHub repository.
2. Railway -> New Project -> Deploy from GitHub Repo.
3. Select that repository.
4. Build command:
   pip install -r requirements.txt
5. Start command:
   python main.py

RAILWAY VARIABLES (IMPORTANT)
Add:
BOT_TOKEN = your NEW Telegram bot token
ADMIN_ID = 8620035649

The old database is included as:
yp_shop.db

It contains the old users/products/orders/settings/etc. and is used automatically
on first run.

OPTIONAL PERSISTENCE
Railway service storage is not a permanent database solution. For data that must
survive redeploy/recreate, add a Railway Volume mounted at /data and set:
DB_PATH=/data/yp_shop.db

Before doing that, copy the included yp_shop.db into the mounted volume on the
first initialization, or use a one-time initialization step.

SECURITY
The original source had a Telegram bot token hard-coded. This package removes it
from main.py and reads BOT_TOKEN from Railway Variables. Regenerate the old token
in BotFather before deployment.

DATABASE CHECK
The supplied old database was inspected and contains:
- users: 115
- products: 77
- orders: 58
- product_keys: 3
- transactions: 112
- settings: 82
- activity_logs: 3172
- spin_rewards: 7
- tickets: 1
- coupons: 1

The package preserves that database file unchanged.

MANAGE PRODUCTS FIX
The fixed package uses explicit product columns instead of SELECT * positional indexes,
escapes product text before Telegram HTML rendering, handles NULL stock safely, and uses
a Railway-safe DB_PATH. Manage Products also has a Refresh button and clearer error logs.
For persistence, create a Railway Volume mounted at /data and set DB_PATH=/data/yp_shop.db.

PRODUCT PURCHASE FLOW FIX
The shop purchase flow now works as:
1. Tap a package.
2. Select quantity (1–5 keys, limited by manual stock when applicable).
3. Choose payment: Wallet Balance, direct UPI, direct Binance, or Add Balance First.
4. For direct payments, the order is stored separately from wallet deposits.
5. After payment, the bot shows a live "WAIT FOR KEY" timer and per-key progress.
6. A database-backed purchase lock and atomic transaction status changes prevent
   repeated Telegram clicks/manual verification from generating duplicate API keys.
7. The external BUY API is intentionally NOT retried automatically because BUY is
   non-idempotent; retrying a timed-out POST could consume another key.
8. If a direct payment succeeds but key generation fails, the paid amount is credited
   to the user's wallet instead of being lost. If only some requested keys are generated,
   the unused amount is credited back to the wallet.
