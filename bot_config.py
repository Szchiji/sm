"""运行配置与 Redis key 定义。业务模块不应在这里放业务逻辑。"""
import os

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_IDS = os.getenv("ADMIN_USER_IDS", "")
WELCOME_TEXT = os.getenv("WELCOME_TEXT", "👋 欢迎！请选择要加入的群组：")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PORT = int(os.environ.get("PORT", 8080))
INVITE_EXPIRE_MINUTES = int(os.getenv("INVITE_EXPIRE_MINUTES", "1440"))
_legacy_cooldown_hours = int(os.getenv("INVITE_COOLDOWN_HOURS", "24"))
DEFAULT_COOLDOWN_MINUTES = int(os.getenv("INVITE_COOLDOWN_MINUTES", str(_legacy_cooldown_hours * 60)))
MIN_COOLDOWN_MINUTES = 1
MAX_COOLDOWN_MINUTES = 43200

RAILWAY_DOMAIN = (
    os.environ.get("RAILWAY_PUBLIC_DOMAIN")
    or os.environ.get("RAILWAY_URL")
    or os.environ.get("RAILWAY_STATIC_URL")
)
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")

GROUPS_KEY = "tg_bot:groups"
GROUPS_PREFIX = "tg_bot:groups:"
GROUP_OWNER_PREFIX = "tg_bot:group_owner:"
ADMINS_KEY = "tg_bot:admins"
INVITE_LOG_KEY = "tg_bot:invite_log"
USER_INVITE_PREFIX = "tg_bot:user_invite:"
PENDING_REQUEST_PREFIX = "tg_bot:pending:"
ADMIN_STATE_PREFIX = "tg_bot:admin_state:"
ADMIN_STATE_TTL = 300
ACTIVE_INVITES_KEY = "tg_bot:active_invites"


def groups_key(admin_id):
    return f"{GROUPS_PREFIX}{admin_id}"
