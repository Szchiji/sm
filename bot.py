import os
import logging
import redis
import json
import asyncio
from datetime import datetime, timedelta
from aiohttp import web
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, 
    ChatMemberHandler, ContextTypes
)

# 日志配置
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# 环境变量
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_IDS = os.getenv("ADMIN_USER_IDS", "")
WELCOME_TEXT = os.getenv("WELCOME_TEXT", "👋 欢迎！请选择要加入的群组：")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PORT = int(os.environ.get("PORT", 8080))
INVITE_EXPIRE_MINUTES = int(os.getenv("INVITE_EXPIRE_MINUTES", "5"))  # 邀请链接过期时间（分钟）
INVITE_COOLDOWN_HOURS = int(os.getenv("INVITE_COOLDOWN_HOURS", "24"))  # 邀请冷却时间（小时）

# Railway 域名检测
RAILWAY_DOMAIN = (
    os.environ.get("RAILWAY_PUBLIC_DOMAIN") or 
    os.environ.get("RAILWAY_URL") or 
    os.environ.get("RAILWAY_STATIC_URL")
)

# 手动设置 Webhook URL（优先级最高）
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")

# Redis Key 前缀
GROUPS_KEY = "tg_bot:groups"
ADMINS_KEY = "tg_bot:admins"
INVITE_LOG_KEY = "tg_bot:invite_log"  # 邀请记录
USER_INVITE_PREFIX = "tg_bot:user_invite:"  # 用户邀请限制前缀

# 连接 Redis
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping()
    logger.info("Redis connected successfully")
except Exception as e:
    logger.error(f"Redis connection failed: {e}")
    redis_client = None

def get_admin_ids_from_env():
    """从环境变量获取管理员ID列表"""
    if not ADMIN_USER_IDS:
        return []
    return [int(uid.strip()) for uid in ADMIN_USER_IDS.split(",") if uid.strip()]

def is_admin(user_id):
    """检查用户是否为管理员"""
    env_admins = get_admin_ids_from_env()
    if env_admins:
        return user_id in env_admins
    
    if not redis_client:
        return False
    admins = redis_client.smembers(ADMINS_KEY)
    return str(user_id) in admins

def init_admin_from_env():
    """将环境变量中的管理员同步到Redis"""
    if not redis_client:
        return
    env_admins = get_admin_ids_from_env()
    for admin_id in env_admins:
        redis_client.sadd(ADMINS_KEY, str(admin_id))

def get_groups():
    """从 Redis 获取所有群组"""
    if not redis_client:
        return {}
    try:
        data = redis_client.get(GROUPS_KEY)
        return json.loads(data) if data else {}
    except Exception as e:
        logger.error(f"Failed to get groups from Redis: {e}")
        return {}

def save_group(group_id, title, added_by):
    """保存群组到 Redis"""
    if not redis_client:
        logger.error("Redis not available")
        return False
    try:
        groups = get_groups()
        groups[str(group_id)] = {
            "title": title,
            "added_by": added_by,
            "invite_link": None
        }
        redis_client.set(GROUPS_KEY, json.dumps(groups))
        logger.info(f"Group saved to Redis: {title} ({group_id})")
        return True
    except Exception as e:
        logger.error(f"Failed to save group: {e}")
        return False

def remove_group(group_id):
    """从 Redis 删除群组"""
    if not redis_client:
        return False
    try:
        groups = get_groups()
        groups.pop(str(group_id), None)
        redis_client.set(GROUPS_KEY, json.dumps(groups))
        return True
    except Exception as e:
        logger.error(f"Failed to remove group: {e}")
        return False

def can_user_get_invite(user_id, group_id):
    """检查用户是否可以获取邀请（冷却时间限制）"""
    if not redis_client:
        return True  # Redis 不可用时允许
    
    key = f"{USER_INVITE_PREFIX}{user_id}:{group_id}"
    if redis_client.exists(key):
        # 获取剩余时间
        ttl = redis_client.ttl(key)
        return False, ttl
    return True, 0

def record_user_invite(user_id, group_id):
    """记录用户获取邀请的时间"""
    if not redis_client:
        return
    key = f"{USER_INVITE_PREFIX}{user_id}:{group_id}"
    # 设置过期时间（冷却时间）
    redis_client.setex(key, INVITE_COOLDOWN_HOURS * 3600, datetime.now().isoformat())

def log_invite(user_id, group_id, invite_link, group_title):
    """记录邀请日志"""
    if not redis_client:
        return
    log_entry = {
        "user_id": user_id,
        "group_id": group_id,
        "group_title": group_title,
        "invite_link": invite_link,
        "created_at": datetime.now().isoformat(),
        "expire_at": (datetime.now() + timedelta(minutes=INVITE_EXPIRE_MINUTES)).isoformat()
    }
    redis_client.lpush(INVITE_LOG_KEY, json.dumps(log_entry))
    # 只保留最近1000条记录
    redis_client.ltrim(INVITE_LOG_KEY, 0, 999)

def format_time_left(seconds):
    """格式化剩余时间"""
    if seconds < 60:
        return f"{seconds}秒"
    elif seconds < 3600:
        return f"{seconds // 60}分钟"
    else:
        return f"{seconds // 3600}小时{ (seconds % 3600) // 60 }分钟"

async def handle_join_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, user):
    """处理用户加入流程（支持多群组选择）"""
    groups = get_groups()
    logger.info(f"Current groups: {groups}")
    
    if not groups:
        await update.message.reply_text("机器人尚未配置群组，请联系管理员")
        return
    
    # 只有一个群组，直接处理
    if len(groups) == 1:
        group_id = list(groups.keys())[0]
        await send_invite_button(update, context, user, group_id, groups[group_id]['title'])
        return
    
    # 多个群组，显示选择菜单
    keyboard = []
    for gid, info in groups.items():
        # 检查用户是否已经获取过该群组的邀请
        can_get, ttl = can_user_get_invite(user.id, gid)
        if can_get:
            status = ""
        else:
            status = " (冷却中)"
        
        keyboard.append([InlineKeyboardButton(
            f"{info['title']}{status}", 
            callback_data=f"select_{gid}_{user.id}"
        )])
    
    await update.message.reply_text(
        WELCOME_TEXT + f"\n\n⏰ 邀请链接有效期：{INVITE_EXPIRE_MINUTES}分钟\n"
        f"🕐 每群组每{INVITE_COOLDOWN_HOURS}小时限领一次",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def send_invite_button(update: Update, context: ContextTypes.DEFAULT_TYPE, user, group_id, group_title):
    """发送邀请按钮"""
    # 检查冷却时间
    can_get, ttl = can_user_get_invite(user.id, group_id)
    if not can_get:
        time_left = format_time_left(ttl)
        await update.message.reply_text(
            f"⏳ 你已经在 {INVITE_COOLDOWN_HOURS} 小时内获取过该群组的邀请链接\n"
            f"请等待 {time_left} 后再试"
        )
        return
    
    keyboard = [[InlineKeyboardButton(
        f"🚀 加入 {group_title}", 
        callback_data=f"join_{group_id}_{user.id}"
    )]]
    
    await update.message.reply_text(
        f"👋 欢迎加入 {group_title}！\n\n"
        f"⏰ 邀请链接将在 {INVITE_EXPIRE_MINUTES} 分钟后过期\n"
        f"🎫 每人每 {INVITE_COOLDOWN_HOURS} 小时限领一次",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """用户点击链接开始"""
    user = update.effective_user
    logger.info(f"Start command from user: {user.id}, text: {update.message.text}")
    
    if not is_admin(user.id):
        if update.message.text and update.message.text.startswith("/start join"):
            await handle_join_flow(update, context, user)
        else:
            await update.message.reply_text("此机器人仅限授权管理员使用")
        return
    
    if update.message.text == "/start":
        groups = get_groups()
        admin_ids = get_admin_ids_from_env()
        
        text = (
            f"欢迎管理员！\n\n"
            f"当前状态：\n"
            f"已绑定群组: {len(groups)} 个\n"
            f"管理员ID: {', '.join(map(str, admin_ids))}\n"
            f"邀请有效期: {INVITE_EXPIRE_MINUTES}分钟\n"
            f"邀请冷却: {INVITE_COOLDOWN_HOURS}小时\n\n"
            f"使用说明：\n"
            f"1. 将机器人拉入私密群组并设为管理员\n"
            f"2. 分享链接给用户：\n"
            f"https://t.me/{context.bot.username}?start=join\n\n"
            f"管理员命令：\n"
            f"/addadmin [用户ID] - 添加其他管理员\n"
            f"/listgroups - 查看已绑定群组\n"
            f"/removegroup [群组ID] - 移除群组\n"
            f"/bindgroup [群组ID] [群组名称] - 手动绑定群组\n"
            f"/stats - 查看邀请统计\n"
            f"/test - 测试 Redis 连接"
        )
        await update.message.reply_text(text)
    elif update.message.text.startswith("/start join"):
        await handle_join_flow(update, context, user)

async def select_group_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理群组选择"""
    query = update.callback_query
    await query.answer()
    
    data = query.data.split("_")
    group_id = data[1]
    user_id = int(data[2])
    
    # 验证用户
    if query.from_user.id != user_id:
        await query.answer("这不是你的选择", show_alert=True)
        return
    
    groups = get_groups()
    if group_id not in groups:
        await query.edit_message_text("该群组已不可用")
        return
    
    # 检查冷却时间
    can_get, ttl = can_user_get_invite(user_id, group_id)
    if not can_get:
        time_left = format_time_left(ttl)
        await query.edit_message_text(
            f"⏳ 你已经在 {INVITE_COOLDOWN_HOURS} 小时内获取过该群组的邀请链接\n"
            f"请等待 {time_left} 后再试"
        )
        return
    
    group_title = groups[group_id]['title']
    
    keyboard = [[InlineKeyboardButton(
        f"🚀 加入 {group_title}", 
        callback_data=f"join_{group_id}_{user_id}"
    )]]
    
    await query.edit_message_text(
        f"👋 欢迎加入 {group_title}！\n\n"
        f"⏰ 邀请链接将在 {INVITE_EXPIRE_MINUTES} 分钟后过期\n"
        f"🎫 每人每 {INVITE_COOLDOWN_HOURS} 小时限领一次",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理加入按钮"""
    query = update.callback_query
    await query.answer()
    
    data = query.data.split("_")
    group_id = data[1]
    user_id = int(data[2])
    
    # 验证点击者身份
    if query.from_user.id != user_id:
        await query.answer("这不是你的链接", show_alert=True)
        return
    
    # 再次检查冷却时间（防止绕过）
    can_get, ttl = can_user_get_invite(user_id, group_id)
    if not can_get:
        time_left = format_time_left(ttl)
        await query.edit_message_text(
            f"⏳ 你已经在 {INVITE_COOLDOWN_HOURS} 小时内获取过该群组的邀请链接\n"
            f"请等待 {time_left} 后再试"
        )
        return
    
    try:
        # 计算过期时间
        expire_time = int((datetime.now() + timedelta(minutes=INVITE_EXPIRE_MINUTES)).timestamp())
        
        invite_link = await context.bot.create_chat_invite_link(
            chat_id=int(group_id),
            member_limit=1,
            expire_date=expire_time
        )
        
        # 记录邀请
        groups = get_groups()
        group_title = groups.get(group_id, {}).get('title', 'Unknown')
        log_invite(user_id, group_id, invite_link.invite_link, group_title)
        record_user_invite(user_id, group_id)
        
        keyboard = [[InlineKeyboardButton("👉 点击加入群组", url=invite_link.invite_link)]]
        await query.edit_message_text(
            f"✅ 验证通过！\n\n"
            f"⏰ 链接将在 {INVITE_EXPIRE_MINUTES} 分钟后过期\n"
            f"🔒 仅限你使用一次\n\n"
            f"点击下方链接加入：",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        logger.info(f"User {user_id} got invite link for group {group_id}, expires in {INVITE_EXPIRE_MINUTES}min")
        
    except Exception as e:
        logger.error(f"Failed to create invite link: {e}")
        await query.edit_message_text("生成邀请链接失败，请联系管理员")

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """查看邀请统计"""
    user = update.effective_user
    
    if not is_admin(user.id):
        await update.message.reply_text("你没有权限")
        return
    
    if not redis_client:
        await update.message.reply_text("Redis 不可用")
        return
    
    # 获取最近24小时的邀请记录
    logs = redis_client.lrange(INVITE_LOG_KEY, 0, 999)
    recent_invites = []
    for log in logs:
        try:
            entry = json.loads(log)
            created = datetime.fromisoformat(entry['created_at'])
            if datetime.now() - created < timedelta(days=1):
                recent_invites.append(entry)
        except:
            continue
    
    # 统计
    total_24h = len(recent_invites)
    unique_users = len(set(i['user_id'] for i in recent_invites))
    
    text = (
        f"📊 邀请统计（最近24小时）\n\n"
        f"总邀请数: {total_24h}\n"
        f"独立用户: {unique_users}\n\n"
        f"配置:\n"
        f"邀请有效期: {INVITE_EXPIRE_MINUTES}分钟\n"
        f"邀请冷却: {INVITE_COOLDOWN_HOURS}小时"
    )
    
    await update.message.reply_text(text)

async def test_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """测试命令，检查 Redis 和配置"""
    user = update.effective_user
    
    if not is_admin(user.id):
        await update.message.reply_text("你没有权限")
        return
    
    redis_status = "连接正常" if redis_client and redis_client.ping() else "连接失败"
    groups = get_groups()
    admin_ids = get_admin_ids_from_env()
    
    text = (
        f"🧪 测试报告\n\n"
        f"Redis 状态: {redis_status}\n"
        f"环境变量管理员: {admin_ids}\n"
        f"已绑定群组: {len(groups)} 个\n"
        f"邀请有效期: {INVITE_EXPIRE_MINUTES}分钟\n"
        f"邀请冷却: {INVITE_COOLDOWN_HOURS}小时\n"
        f"当前用户ID: {user.id}\n"
        f"是否为管理员: {is_admin(user.id)}"
    )
    
    await update.message.reply_text(text)

async def bot_added_to_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """机器人被添加到群组时触发"""
    logger.info(f"=== CHAT MEMBER UPDATE RECEIVED ===")
    
    chat_member_update = update.chat_member or update.my_chat_member
    
    if not chat_member_update:
        logger.warning("No chat_member or my_chat_member in update")
        return
    
    chat = update.effective_chat
    new_member = chat_member_update.new_chat_member
    old_member = chat_member_update.old_chat_member
    
    if not new_member or new_member.user.id != context.bot.id:
        return
    
    added_by = update.effective_user
    
    if not is_admin(added_by.id):
        logger.warning(f"Non-admin {added_by.id} tried to add bot")
        try:
            await context.bot.send_message(
                chat_id=chat.id,
                text="只有机器人管理员才能使用此功能"
            )
            await context.bot.leave_chat(chat.id)
        except Exception as e:
            logger.error(f"Failed to leave chat: {e}")
        return
    
    # 检查状态变化
    old_status = old_member.status if old_member else None
    new_status = new_member.status
    
    logger.info(f"Bot status changed from {old_status} to {new_status}")
    
    # 只处理新添加或提升为管理员的情况
    if new_status == 'administrator' and old_status != 'administrator':
        if save_group(chat.id, chat.title, added_by.id):
            try:
                await context.bot.send_message(
                    chat_id=added_by.id,
                    text=f"✅ 机器人已成功绑定到群组「{chat.title}」\n"
                         f"群组ID: `{chat.id}`\n"
                         f"分享链接：https://t.me/{context.bot.username}?start=join",
                    parse_mode="Markdown"
                )
                logger.info(f"Notification sent to admin {added_by.id}")
            except Exception as e:
                logger.error(f"Failed to notify admin: {e}")
    elif new_status == 'member':
        try:
            await context.bot.send_message(
                chat_id=chat.id,
                text=f"@{added_by.username} 请将机器人设为管理员，否则无法使用加群功能"
            )
        except Exception as e:
            logger.error(f"Failed to send message: {e}")

async def bot_removed_from_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """机器人被移除时清理"""
    chat_member_update = update.chat_member or update.my_chat_member
    
    if not chat_member_update:
        return
    
    chat = update.effective_chat
    new_member = chat_member_update.new_chat_member
    
    if not new_member or new_member.user.id != context.bot.id:
        return
    
    if new_member.status in ['left', 'kicked']:
        remove_group(chat.id)
        logger.info(f"Bot left group: {chat.title if chat else 'Unknown'}")

async def bind_group_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """手动绑定群组"""
    user = update.effective_user
    
    if not is_admin(user.id):
        await update.message.reply_text("你没有权限")
        return
    
    if len(context.args) < 2:
        await update.message.reply_text(
            "用法: /bindgroup [群组ID] [群组名称]\n"
            "示例: /bindgroup -1001234567890 我的私密群"
        )
        return
    
    group_id = context.args[0]
    group_title = " ".join(context.args[1:])
    
    if save_group(group_id, group_title, user.id):
        await update.message.reply_text(
            f"✅ 已手动绑定群组：{group_title}\n"
            f"分享链接：https://t.me/{context.bot.username}?start=join"
        )
    else:
        await update.message.reply_text("❌ 绑定失败")

async def add_admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """添加管理员命令"""
    user = update.effective_user
    
    if not is_admin(user.id):
        await update.message.reply_text("你没有权限")
        return
    
    if not context.args:
        await update.message.reply_text("用法: /addadmin [用户ID]")
        return
    
    new_admin_id = context.args[0]
    if not redis_client:
        await update.message.reply_text("系统错误")
        return
    
    redis_client.sadd(ADMINS_KEY, str(new_admin_id))
    await update.message.reply_text(f"已添加管理员: {new_admin_id}")

async def list_groups_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """查看已绑定群组"""
    user = update.effective_user
    
    if not is_admin(user.id):
        await update.message.reply_text("你没有权限")
        return
    
    groups = get_groups()
    
    if not groups:
        await update.message.reply_text("暂无绑定的群组")
        return
    
    text = "已绑定群组列表：\n\n"
    for gid, info in groups.items():
        text += f"• {info['title']}\n  ID: `{gid}`\n\n"
    
    await update.message.reply_text(text, parse_mode="Markdown")

async def remove_group_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """手动移除群组"""
    user = update.effective_user
    
    if not is_admin(user.id):
        await update.message.reply_text("你没有权限")
        return
    
    if not context.args:
        await update.message.reply_text("用法: /removegroup [群组ID]")
        return
    
    group_id = context.args[0]
    if remove_group(group_id):
        await update.message.reply_text(f"已移除群组: {group_id}")
    else:
        await update.message.reply_text("移除失败")

# HTTP 处理
async def health_check(request):
    """健康检查端点"""
    return web.Response(text="OK", status=200)

async def webhook_handler(request):
    """处理 Telegram Webhook 请求"""
    application = request.app['application']
    try:
        data = await request.json()
        update = Update.de_json(data, application.bot)
        await application.process_update(update)
        return web.Response(text="OK", status=200)
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return web.Response(text="Error", status=500)

async def main():
    """主函数"""
    if redis_client:
        init_admin_from_env()
        logger.info(f"Loaded admins: {get_admin_ids_from_env()}")
        logger.info(f"Invite expire: {INVITE_EXPIRE_MINUTES}min, Cooldown: {INVITE_COOLDOWN_HOURS}h")
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    # 命令处理器
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("test", test_cmd))
    application.add_handler(CommandHandler("stats", stats_cmd))
    application.add_handler(CommandHandler("addadmin", add_admin_cmd))
    application.add_handler(CommandHandler("listgroups", list_groups_cmd))
    application.add_handler(CommandHandler("removegroup", remove_group_cmd))
    application.add_handler(CommandHandler("bindgroup", bind_group_cmd))
    
    # 回调处理器
    application.add_handler(CallbackQueryHandler(select_group_callback, pattern="^select_"))
    application.add_handler(CallbackQueryHandler(button_handler, pattern="^join_"))
    
    # 群组变动处理器
    application.add_handler(ChatMemberHandler(bot_added_to_group, ChatMemberHandler.MY_CHAT_MEMBER))
    application.add_handler(ChatMemberHandler(bot_removed_from_group, ChatMemberHandler.MY_CHAT_MEMBER))
    
    # HTTP 服务器
    app = web.Application()
    app['application'] = application
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    app.router.add_post("/webhook", webhook_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"HTTP server started on port {PORT}")
    
    # 启动模式
    use_webhook = False
    webhook_url = None
    
    if WEBHOOK_URL:
        webhook_url = WEBHOOK_URL
        use_webhook = True
    elif RAILWAY_DOMAIN:
        webhook_url = f"https://{RAILWAY_DOMAIN}/webhook"
        use_webhook = True
    
    if use_webhook and webhook_url:
        await application.initialize()
        await application.bot.set_webhook(
            url=webhook_url,
            allowed_updates=["message", "callback_query", "chat_member", "my_chat_member"]
        )
        await application.start()
        logger.info(f"Bot started with webhook: {webhook_url}")
    else:
        await application.initialize()
        await application.start()
        await application.updater.start_polling(
            allowed_updates=["message", "callback_query", "chat_member", "my_chat_member"]
        )
        logger.info("Bot started with polling")
    
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
