import os
import logging
import redis
import json
import asyncio
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
WELCOME_TEXT = os.getenv("WELCOME_TEXT", "👋 欢迎！点击下方按钮加入群组：")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PORT = int(os.environ.get("PORT", 8080))

# Redis Key 前缀
GROUPS_KEY = "tg_bot:groups"
ADMINS_KEY = "tg_bot:admins"

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
    data = redis_client.get(GROUPS_KEY)
    return json.loads(data) if data else {}

def save_group(group_id, title, added_by):
    """保存群组到 Redis"""
    if not redis_client:
        return False
    groups = get_groups()
    groups[str(group_id)] = {
        "title": title,
        "added_by": added_by,
        "invite_link": None
    }
    redis_client.set(GROUPS_KEY, json.dumps(groups))
    return True

def remove_group(group_id):
    """从 Redis 删除群组"""
    if not redis_client:
        return False
    groups = get_groups()
    groups.pop(str(group_id), None)
    redis_client.set(GROUPS_KEY, json.dumps(groups))
    return True

async def handle_join_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, user):
    """处理用户加入流程"""
    groups = get_groups()
    if not groups:
        await update.message.reply_text("机器人尚未配置群组，请联系管理员")
        return
    
    group_id = list(groups.keys())[0]
    group_info = groups[group_id]
    
    keyboard = [[InlineKeyboardButton(
        f"加入 {group_info['title']}", 
        callback_data=f"join_{group_id}_{user.id}"
    )]]
    
    await update.message.reply_text(
        WELCOME_TEXT, 
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """用户点击链接开始"""
    user = update.effective_user
    
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
            f"管理员ID: {', '.join(map(str, admin_ids))}\n\n"
            f"使用说明：\n"
            f"1. 将机器人拉入私密群组并设为管理员\n"
            f"2. 分享链接给用户：\n"
            f"https://t.me/{context.bot.username}?start=join\n\n"
            f"管理员命令：\n"
            f"/addadmin [用户ID] - 添加其他管理员\n"
            f"/listgroups - 查看已绑定群组\n"
            f"/removegroup [群组ID] - 移除群组"
        )
        await update.message.reply_text(text)
    elif update.message.text.startswith("/start join"):
        await handle_join_flow(update, context, user)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理加入按钮"""
    query = update.callback_query
    await query.answer()
    
    data = query.data.split("_")
    group_id = data[1]
    user_id = int(data[2])
    
    if query.from_user.id != user_id:
        await query.answer("这不是你的链接", show_alert=True)
        return
    
    try:
        invite_link = await context.bot.create_chat_invite_link(
            chat_id=int(group_id),
            member_limit=1,
            expire_date=None
        )
        
        keyboard = [[InlineKeyboardButton("点击加入群组", url=invite_link.invite_link)]]
        await query.edit_message_text(
            "验证通过！点击下方链接加入（仅限你使用一次）：",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        logger.info(f"User {user_id} got invite link for group {group_id}")
        
    except Exception as e:
        logger.error(f"Failed to create invite link: {e}")
        await query.edit_message_text("生成邀请链接失败，请联系管理员")

async def bot_added_to_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """机器人被添加到群组时触发"""
    chat = update.effective_chat
    new_member = update.chat_member.new_chat_member
    
    if new_member.user.id != context.bot.id:
        return
    
    added_by = update.effective_user
    
    if not is_admin(added_by.id):
        logger.warning(f"Non-admin {added_by.id} tried to add bot to {chat.title}")
        try:
            await context.bot.send_message(
                chat_id=chat.id,
                text="只有机器人管理员才能使用此功能"
            )
            await context.bot.leave_chat(chat.id)
        except:
            pass
        return
    
    if new_member.status == 'administrator':
        if save_group(chat.id, chat.title, added_by.id):
            logger.info(f"Group saved: {chat.title} ({chat.id})")
            
            try:
                await context.bot.send_message(
                    chat_id=added_by.id,
                    text=f"机器人已成功绑定到群组 {chat.title}\n"
                         f"群组ID: {chat.id}\n"
                         f"分享链接：https://t.me/{context.bot.username}?start=join"
                )
            except:
                pass
    elif new_member.status == 'member':
        try:
            await context.bot.send_message(
                chat_id=chat.id,
                text=f"@{added_by.username} 请将机器人设为管理员，否则无法使用加群功能"
            )
        except:
            pass

async def bot_removed_from_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """机器人被移除时清理"""
    chat = update.effective_chat
    new_member = update.chat_member.new_member
    
    if new_member.user.id == context.bot.id and new_member.status in ['left', 'kicked']:
        remove_group(chat.id)
        logger.info(f"Bot left group: {chat.title}")

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
        text += f"• {info['title']}\n  ID: {gid}\n\n"
    
    await update.message.reply_text(text)

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

# HTTP 健康检查
async def health_check(request):
    """健康检查端点"""
    return web.Response(text="OK", status=200)

async def run_http_server():
    """运行 HTTP 服务器供健康检查"""
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"HTTP server started on port {PORT}")

async def main():
    """主函数"""
    if redis_client:
        init_admin_from_env()
        logger.info(f"Loaded admins: {get_admin_ids_from_env()}")
    
    # 创建应用
    application = Application.builder().token(BOT_TOKEN).build()
    
    # 添加处理器
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler, pattern="^join_"))
    application.add_handler(CommandHandler("addadmin", add_admin_cmd))
    application.add_handler(CommandHandler("listgroups", list_groups_cmd))
    application.add_handler(CommandHandler("removegroup", remove_group_cmd))
    application.add_handler(ChatMemberHandler(bot_added_to_group, ChatMemberHandler.MY_CHAT_MEMBER))
    
    # 启动 HTTP 服务器（用于健康检查）
    await run_http_server()
    
    # 启动机器人（使用 webhook 模式）
    RAILWAY_STATIC_URL = os.environ.get("RAILWAY_STATIC_URL")
    if RAILWAY_STATIC_URL:
        await application.initialize()
        await application.bot.set_webhook(
            url=f"{RAILWAY_STATIC_URL}/webhook",
            allowed_updates=Update.ALL_TYPES
        )
        await application.start()
        logger.info("Bot started with webhook")
        
        # 保持运行
        while True:
            await asyncio.sleep(3600)
    else:
        # 本地开发使用 polling
        await application.initialize()
        await application.start()
        await application.updater.start_polling()
        logger.info("Bot started with polling")
        
        # 保持运行
        while True:
            await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
