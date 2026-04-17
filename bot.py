import os
import logging
import redis
import json
import asyncio
import re
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
INVITE_EXPIRE_MINUTES = int(os.getenv("INVITE_EXPIRE_MINUTES", "5"))
INVITE_COOLDOWN_HOURS = int(os.getenv("INVITE_COOLDOWN_HOURS", "24"))

# Railway 域名检测
RAILWAY_DOMAIN = (
    os.environ.get("RAILWAY_PUBLIC_DOMAIN") or 
    os.environ.get("RAILWAY_URL") or 
    os.environ.get("RAILWAY_STATIC_URL")
)

WEBHOOK_URL = os.environ.get("WEBHOOK_URL")

# Redis Key 前缀
GROUPS_KEY = "tg_bot:groups"
ADMINS_KEY = "tg_bot:admins"
INVITE_LOG_KEY = "tg_bot:invite_log"
USER_INVITE_PREFIX = "tg_bot:user_invite:"

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
        return True, 0
    
    key = f"{USER_INVITE_PREFIX}{user_id}:{group_id}"
    if redis_client.exists(key):
        ttl = redis_client.ttl(key)
        return False, ttl
    return True, 0

def record_user_invite(user_id, group_id):
    """记录用户获取邀请的时间"""
    if not redis_client:
        return
    key = f"{USER_INVITE_PREFIX}{user_id}:{group_id}"
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
        "expire_at": (datetime.now() + timedelta(minutes=INVITE_EXPIRE_MINUTES)).isoformat(),
        "revoked": False
    }
    redis_client.lpush(INVITE_LOG_KEY, json.dumps(log_entry))
    redis_client.ltrim(INVITE_LOG_KEY, 0, 999)

def format_time_left(seconds):
    """格式化剩余时间"""
    if seconds < 60:
        return f"{seconds}秒"
    elif seconds < 3600:
        return f"{seconds // 60}分钟"
    else:
        hours = seconds // 3600
        mins = (seconds % 3600) // 60
        return f"{hours}小时{mins}分钟" if mins > 0 else f"{hours}小时"

# ============ 自动清理功能 ============

async def cleanup_expired_invites():
    """清理 Redis 中的过期邀请记录"""
    if not redis_client:
        return 0
    
    logs = redis_client.lrange(INVITE_LOG_KEY, 0, -1)
    removed = 0
    
    for log in logs:
        try:
            entry = json.loads(log)
            expire_at = datetime.fromisoformat(entry['expire_at'])
            
            # 过期超过1小时的记录删除
            if datetime.now() - expire_at > timedelta(hours=1):
                redis_client.lrem(INVITE_LOG_KEY, 0, log)
                removed += 1
        except:
            continue
    
    if removed > 0:
        logger.info(f"Cleanup: removed {removed} expired invite logs from Redis")
    return removed

async def revoke_expired_invites(application: Application):
    """撤销 Telegram 群组中已失效的邀请链接"""
    if not redis_client:
        return 0
    
    logs = redis_client.lrange(INVITE_LOG_KEY, 0, -1)
    revoked_count = 0
    failed_count = 0
    
    for log in logs:
        try:
            entry = json.loads(log)
            
            # 检查是否已撤销
            if entry.get('revoked', False):
                continue
            
            # 检查是否过期
            expire_at = datetime.fromisoformat(entry['expire_at'])
            if datetime.now() < expire_at:
                continue  # 未过期，跳过
            
            group_id = entry.get('group_id')
            invite_link = entry.get('invite_link', '')
            
            if not group_id or not invite_link:
                continue
            
            try:
                await application.bot.revoke_chat_invite_link(
                    chat_id=int(group_id),
                    invite_link=invite_link
                )
                
                # 标记为已撤销
                entry['revoked'] = True
                entry['revoked_at'] = datetime.now().isoformat()
                redis_client.lrem(INVITE_LOG_KEY, 0, log)
                redis_client.lpush(INVITE_LOG_KEY, json.dumps(entry))
                
                revoked_count += 1
                logger.info(f"Revoked expired invite: {invite_link}")
                
            except Exception as e:
                failed_count += 1
                logger.error(f"Failed to revoke invite {invite_link}: {e}")
                
                # 如果撤销失败（可能链接已失效或已被删除），标记为已处理
                if "INVITE_HASH_EXPIRED" in str(e) or "INVITE_HASH_INVALID" in str(e):
                    entry['revoked'] = True
                    entry['revoke_failed'] = True
                    entry['revoke_error'] = str(e)
                    redis_client.lrem(INVITE_LOG_KEY, 0, log)
                    redis_client.lpush(INVITE_LOG_KEY, json.dumps(entry))
                
        except Exception as e:
            logger.error(f"Error processing log entry: {e}")
            continue
    
    if revoked_count > 0 or failed_count > 0:
        logger.info(f"Revoke completed: {revoked_count} revoked, {failed_count} failed")
    return revoked_count

async def cleanup_expired_cooldowns():
    """检查即将过期的冷却"""
    if not redis_client:
        return 0
    
    pattern = f"{USER_INVITE_PREFIX}*"
    keys = redis_client.keys(pattern)
    
    expired_soon = 0
    for key in keys:
        ttl = redis_client.ttl(key)
        if 0 < ttl < 300:  # 5分钟内过期
            expired_soon += 1
    
    if expired_soon > 0:
        logger.info(f"Cleanup: {expired_soon} cooldowns expiring in 5min")
    return expired_soon

async def cleanup_expired_data(application: Application):
    """后台清理任务（包含撤销 Telegram 邀请链接）"""
    while True:
        try:
            # 1. 清理 Redis 中的过期记录
            removed = await cleanup_expired_invites()
            
            # 2. 撤销 Telegram 中的失效邀请链接
            revoked = await revoke_expired_invites(application)
            
            # 3. 检查即将过期的冷却
            await cleanup_expired_cooldowns()
            
            # 获取统计
            if redis_client:
                logs = redis_client.lrange(INVITE_LOG_KEY, 0, -1)
                active = 0
                expired = 0
                revoked_total = 0
                
                for log in logs:
                    try:
                        entry = json.loads(log)
                        if entry.get('revoked', False):
                            revoked_total += 1
                        elif datetime.fromisoformat(entry['expire_at']) > datetime.now():
                            active += 1
                        else:
                            expired += 1
                    except:
                        continue
                
                logger.info(f"Cleanup stats: {active} active, {expired} expired, {revoked_total} revoked, removed {removed} logs, revoked {revoked} invites this run")
            
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
        
        # 每小时执行
        await asyncio.sleep(3600)

# ============ 业务逻辑 ============

async def send_single_invite(update, context, user, group_id, group_title):
    """发送单个邀请"""
    can_get, ttl = can_user_get_invite(user.id, group_id)
    if not can_get:
        time_left = format_time_left(ttl)
        await update.message.reply_text(
            f"⏳ {group_title} 冷却中，请 {time_left} 后再试"
        )
        return False
    
    try:
        expire_time = int((datetime.now() + timedelta(minutes=INVITE_EXPIRE_MINUTES)).timestamp())
        invite_link = await context.bot.create_chat_invite_link(
            chat_id=int(group_id),
            member_limit=1,
            expire_date=expire_time
        )
        
        log_invite(user.id, group_id, invite_link.invite_link, group_title)
        record_user_invite(user.id, group_id)
        
        keyboard = [[InlineKeyboardButton(f"👉 加入 {group_title}", url=invite_link.invite_link)]]
        await update.message.reply_text(
            f"✅ {group_title}\n⏰ {INVITE_EXPIRE_MINUTES}分钟后过期",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return True
        
    except Exception as e:
        logger.error(f"Failed to create invite: {e}")
        await update.message.reply_text(f"❌ {group_title} 邀请生成失败")
        return False

async def handle_join_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, user):
    """处理用户加入流程（选择单个群组）"""
    groups = get_groups()
    logger.info(f"Current groups: {groups}")
    
    if not groups:
        await update.message.reply_text("机器人尚未配置群组，请联系管理员")
        return
    
    # 只有一个群组，直接处理
    if len(groups) == 1:
        group_id = list(groups.keys())[0]
        await send_single_invite(update, context, user, group_id, list(groups.values())[0]['title'])
        return
    
    # 多个群组，显示选择菜单
    keyboard = []
    for gid, info in groups.items():
        can_get, ttl = can_user_get_invite(user.id, gid)
        if can_get:
            status = ""
        else:
            status = " (冷却中)"
        
        keyboard.append([InlineKeyboardButton(
            f"{info['title']}{status}", 
            callback_data=f"select_{gid}_{user.id}"
        )])
    
    # 添加"加入全部"选项
    keyboard.append([InlineKeyboardButton(
        "🚀 一键加入所有群组", 
        callback_data=f"joinall_{user.id}"
    )])
    
    await update.message.reply_text(
        WELCOME_TEXT + f"\n\n⏰ 邀请链接有效期：{INVITE_EXPIRE_MINUTES}分钟\n"
        f"🕐 每群组每{INVITE_COOLDOWN_HOURS}小时限领一次",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_join_all(update: Update, context: ContextTypes.DEFAULT_TYPE, user):
    """处理批量加入所有群组"""
    groups = get_groups()
    
    if not groups:
        await update.message.reply_text("机器人尚未配置群组，请联系管理员")
        return
    
    # 只有一个群组，直接处理
    if len(groups) == 1:
        group_id = list(groups.keys())[0]
        await send_single_invite(update, context, user, group_id, list(groups.values())[0]['title'])
        return
    
    # 检查每个群组的冷却状态
    available_groups = []
    cooling_groups = []
    
    for gid, info in groups.items():
        can_get, ttl = can_user_get_invite(user.id, gid)
        if can_get:
            available_groups.append((gid, info['title']))
        else:
            cooling_groups.append((info['title'], ttl))
    
    if not available_groups:
        # 所有群组都在冷却中
        text = "⏳ 所有群组都在冷却中：\n\n"
        for title, ttl in cooling_groups:
            text += f"• {title}: {format_time_left(ttl)}\n"
        await update.message.reply_text(text)
        return
    
    if len(available_groups) == 1:
        # 只有一个可用，直接发送
        gid, title = available_groups[0]
        await send_single_invite(update, context, user, gid, title)
        return
    
    # 多个可用，直接生成所有邀请链接，无需确认
    processing_msg = await update.message.reply_text("⏳ 正在生成邀请链接，请稍候...")
    
    keyboard_buttons = []
    failed_groups = []
    success_count = 0
    
    for gid, title in available_groups:
        try:
            expire_time = int((datetime.now() + timedelta(minutes=INVITE_EXPIRE_MINUTES)).timestamp())
            invite_link = await context.bot.create_chat_invite_link(
                chat_id=int(gid),
                member_limit=1,
                expire_date=expire_time
            )
            log_invite(user.id, gid, invite_link.invite_link, title)
            record_user_invite(user.id, gid)
            keyboard_buttons.append([InlineKeyboardButton(f"👉 加入 {title}", url=invite_link.invite_link)])
            success_count += 1
        except Exception as e:
            logger.error(f"Failed to create invite for {gid}: {e}")
            failed_groups.append(title)
    
    text = f"✅ 已生成 {success_count} 个邀请链接\n"
    if cooling_groups:
        text += f"⏳ {len(cooling_groups)} 个群组冷却中\n"
    if failed_groups:
        text += f"❌ {len(failed_groups)} 个群组生成失败\n"
    text += f"\n⏰ 链接将在 {INVITE_EXPIRE_MINUTES} 分钟后过期\n"
    text += "🔒 每个链接仅限使用一次"
    
    await processing_msg.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard_buttons) if keyboard_buttons else None
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """用户点击链接开始"""
    user = update.effective_user
    logger.info(f"Start command from user: {user.id}, text: {update.message.text}")
    
    if not is_admin(user.id):
        if update.message.text == "/start join":
            await handle_join_flow(update, context, user)
        elif update.message.text == "/start joinall":
            await handle_join_all(update, context, user)
        else:
            await update.message.reply_text("此机器人仅限授权管理员使用")
        return
    
    # 管理员面板
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
            f"用户链接：\n"
            f"• 选择加入: https://t.me/{context.bot.username}?start=join\n"
            f"• 加入全部: https://t.me/{context.bot.username}?start=joinall\n\n"
            f"管理员命令：\n"
            f"/addadmin [用户ID]\n"
            f"/listgroups\n"
            f"/removegroup [群组ID]\n"
            f"/bindgroup [群组ID] [群组名称]\n"
            f"/stats - 查看统计\n"
            f"/cleanup - 清理过期数据\n"
            f"/revoke - 立即撤销失效链接\n"
            f"/test - 测试连接"
        )
        await update.message.reply_text(text)
    elif update.message.text == "/start join":
        await handle_join_flow(update, context, user)
    elif update.message.text == "/start joinall":
        await handle_join_all(update, context, user)

async def select_group_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理群组选择"""
    query = update.callback_query
    await query.answer()
    
    data = query.data.split("_")
    group_id = data[1]
    user_id = int(data[2])
    
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
    
    # 直接生成邀请链接，无需二次确认
    try:
        expire_time = int((datetime.now() + timedelta(minutes=INVITE_EXPIRE_MINUTES)).timestamp())
        invite_link = await context.bot.create_chat_invite_link(
            chat_id=int(group_id),
            member_limit=1,
            expire_date=expire_time
        )
        
        log_invite(user_id, group_id, invite_link.invite_link, group_title)
        record_user_invite(user_id, group_id)
        
        keyboard = [[InlineKeyboardButton(f"👉 点击加入 {group_title}", url=invite_link.invite_link)]]
        await query.edit_message_text(
            f"✅ {group_title}\n\n"
            f"⏰ 链接将在 {INVITE_EXPIRE_MINUTES} 分钟后过期\n"
            f"🔒 仅限你使用一次",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        logger.info(f"User {user_id} got invite link for group {group_id}")
        
    except Exception as e:
        logger.error(f"Failed to create invite: {e}")
        await query.edit_message_text(f"❌ {group_title} 邀请生成失败，请联系管理员")

async def join_all_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理一键加入所有群组的回调"""
    query = update.callback_query
    await query.answer()
    
    data = query.data.split("_")
    user_id = int(data[1])
    
    if query.from_user.id != user_id:
        await query.answer("这不是你的请求", show_alert=True)
        return
    
    groups = get_groups()
    results = []
    success_count = 0
    
    # 编辑消息显示处理中
    await query.edit_message_text("⏳ 正在生成邀请链接，请稍候...")
    
    for gid, info in groups.items():
        # 检查冷却
        can_get, ttl = can_user_get_invite(user_id, gid)
        if not can_get:
            results.append(f"❌ {info['title']} - 冷却中 ({format_time_left(ttl)})")
            continue
        
        try:
            # 生成邀请
            expire_time = int((datetime.now() + timedelta(minutes=INVITE_EXPIRE_MINUTES)).timestamp())
            invite_link = await context.bot.create_chat_invite_link(
                chat_id=int(gid),
                member_limit=1,
                expire_date=expire_time
            )
            
            # 记录
            log_invite(user_id, gid, invite_link.invite_link, info['title'])
            record_user_invite(user_id, gid)
            
            results.append(f"✅ [{info['title']}]({invite_link.invite_link})")
            success_count += 1
            
        except Exception as e:
            logger.error(f"Failed to create invite for {gid}: {e}")
            results.append(f"❌ {info['title']} - 生成失败")
    
    # 发送结果
    text = f"📋 邀请链接生成结果（成功 {success_count}/{len(groups)}）：\n\n"
    text += "\n".join(results)
    text += f"\n\n⏰ 所有链接 {INVITE_EXPIRE_MINUTES} 分钟后过期\n"
    text += "🔒 每个链接仅限使用一次"
    
    await query.edit_message_text(
        text,
        parse_mode="Markdown",
        disable_web_page_preview=True
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
    
    # 再次检查冷却时间
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

async def cleanup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """手动清理命令"""
    user = update.effective_user
    
    if not is_admin(user.id):
        await update.message.reply_text("⛔ 你没有权限")
        return
    
    await update.message.reply_text("🧹 开始清理过期数据...")
    
    removed = await cleanup_expired_invites()
    
    # 获取统计
    if redis_client:
        logs = redis_client.lrange(INVITE_LOG_KEY, 0, -1)
        valid = 0
        expired = 0
        revoked = 0
        
        for log in logs:
            try:
                entry = json.loads(log)
                if entry.get('revoked', False):
                    revoked += 1
                elif datetime.fromisoformat(entry['expire_at']) > datetime.now():
                    valid += 1
                else:
                    expired += 1
            except:
                expired += 1
        
        text = (
            f"✅ 清理完成\n\n"
            f"🗑️ 已删除记录: {removed}\n"
            f"✨ 有效邀请: {valid}\n"
            f"⏰ 待撤销: {expired}\n"
            f"🚫 已撤销: {revoked}\n"
            f"📊 总计: {len(logs)}"
        )
    else:
        text = "❌ Redis 不可用"
    
    await update.message.reply_text(text)

async def revoke_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """手动立即撤销失效链接"""
    user = update.effective_user
    
    if not is_admin(user.id):
        await update.message.reply_text("⛔ 你没有权限")
        return
    
    await update.message.reply_text("🚫 正在撤销失效的邀请链接...")
    
    # 获取 application 实例
    application = context.application
    
    revoked = await revoke_expired_invites(application)
    
    await update.message.reply_text(f"✅ 已撤销 {revoked} 个失效的邀请链接")

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
    revoked_count = 0
    
    for log in logs:
        try:
            entry = json.loads(log)
            if entry.get('revoked', False):
                revoked_count += 1
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
        f"独立用户: {unique_users}\n"
        f"已撤销链接: {revoked_count}\n\n"
        f"配置:\n"
        f"邀请有效期: {INVITE_EXPIRE_MINUTES}分钟\n"
        f"邀请冷却: {INVITE_COOLDOWN_HOURS}小时"
    )
    
    await update.message.reply_text(text)

async def test_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """测试命令"""
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
    
    old_status = old_member.status if old_member else None
    new_status = new_member.status
    
    logger.info(f"Bot status changed from {old_status} to {new_status}")
    
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
    application.add_handler(CommandHandler("cleanup", cleanup_cmd))
    application.add_handler(CommandHandler("revoke", revoke_cmd))
    application.add_handler(CommandHandler("addadmin", add_admin_cmd))
    application.add_handler(CommandHandler("listgroups", list_groups_cmd))
    application.add_handler(CommandHandler("removegroup", remove_group_cmd))
    application.add_handler(CommandHandler("bindgroup", bind_group_cmd))
    
    # 回调处理器
    application.add_handler(CallbackQueryHandler(select_group_callback, pattern="^select_"))
    application.add_handler(CallbackQueryHandler(join_all_callback, pattern="^joinall_"))
    application.add_handler(CallbackQueryHandler(button_handler, pattern="^join_"))
    
    # 群组变动处理器
    application.add_handler(ChatMemberHandler(bot_added_to_group, ChatMemberHandler.MY_CHAT_MEMBER))
    application.add_handler(ChatMemberHandler(bot_removed_from_group, ChatMemberHandler.MY_CHAT_MEMBER))
    
    # 启动后台清理任务（传递 application）
    cleanup_task = asyncio.create_task(cleanup_expired_data(application))
    logger.info("Background cleanup task started")
    
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
    
    # 保持运行
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        logger.info("Shutting down...")
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass
        raise

if __name__ == "__main__":
    asyncio.run(main())
