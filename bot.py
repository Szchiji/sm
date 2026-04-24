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
    ChatMemberHandler, ContextTypes, MessageHandler, filters
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
GROUPS_KEY = "tg_bot:groups"           # 旧全局 key（仅用于迁移）
GROUPS_PREFIX = "tg_bot:groups:"       # 新按管理员隔离的 key，后跟 admin_id
GROUP_OWNER_PREFIX = "tg_bot:group_owner:"  # 反向查找：group_id -> admin_id
ADMINS_KEY = "tg_bot:admins"
INVITE_LOG_KEY = "tg_bot:invite_log"
USER_INVITE_PREFIX = "tg_bot:user_invite:"
PENDING_REQUEST_PREFIX = "tg_bot:pending:"  # 待审批申请：key = pending:{user_id}_{group_id}
ADMIN_STATE_PREFIX = "tg_bot:admin_state:"  # 管理员输入状态：key = admin_state:{user_id}
ADMIN_STATE_TTL = 300  # 状态 5 分钟过期

def groups_key(admin_id):
    """返回指定管理员的群组 Redis key"""
    return f"{GROUPS_PREFIX}{admin_id}"

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
    if user_id in env_admins:
        return True
    
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

def get_groups(admin_id):
    """从 Redis 获取指定管理员的群组"""
    if not redis_client:
        return {}
    try:
        data = redis_client.get(groups_key(admin_id))
        return json.loads(data) if data else {}
    except Exception as e:
        logger.error(f"Failed to get groups from Redis: {e}")
        return {}

def save_group(group_id, title, added_by):
    """保存群组到 Redis（按管理员隔离）"""
    if not redis_client:
        logger.error("Redis not available")
        return False
    try:
        groups = get_groups(added_by)
        groups[str(group_id)] = {
            "title": title,
            "added_by": added_by,
            "invite_link": None
        }
        redis_client.set(groups_key(added_by), json.dumps(groups))
        # 记录反向查找：group_id -> admin_id
        redis_client.set(f"{GROUP_OWNER_PREFIX}{group_id}", str(added_by))
        logger.info(f"Group saved to Redis: {title} ({group_id}) for admin {added_by}")
        return True
    except Exception as e:
        logger.error(f"Failed to save group: {e}")
        return False

def remove_group(group_id):
    """从 Redis 删除群组（通过反向查找定位所属管理员）"""
    if not redis_client:
        return False
    try:
        owner_key = f"{GROUP_OWNER_PREFIX}{group_id}"
        admin_id = redis_client.get(owner_key)
        if not admin_id:
            logger.warning(f"No owner found for group {group_id}")
            return False
        groups = get_groups(admin_id)
        groups.pop(str(group_id), None)
        redis_client.set(groups_key(admin_id), json.dumps(groups))
        redis_client.delete(owner_key)
        return True
    except Exception as e:
        logger.error(f"Failed to remove group: {e}")
        return False

def set_group_approval(group_id, admin_id, required: bool):
    """设置群组是否需要管理员审批才能加入"""
    if not redis_client:
        return False
    try:
        groups = get_groups(admin_id)
        gid = str(group_id)
        if gid not in groups:
            return False
        groups[gid]['approval_required'] = required
        redis_client.set(groups_key(admin_id), json.dumps(groups))
        return True
    except Exception as e:
        logger.error(f"Failed to set approval for group {group_id}: {e}")
        return False

def save_pending_request(user_id, group_id, user_info, group_title, admin_id):
    """保存待审批的加群申请（TTL 与冷却时间相同）"""
    if not redis_client:
        return False
    key = f"{PENDING_REQUEST_PREFIX}{user_id}_{group_id}"
    data = {
        "user_id": user_id,
        "username": user_info.get("username"),
        "first_name": user_info.get("first_name", ""),
        "group_id": str(group_id),
        "group_title": group_title,
        "admin_id": admin_id,
        "created_at": datetime.now().isoformat()
    }
    redis_client.setex(key, INVITE_COOLDOWN_HOURS * 3600, json.dumps(data))
    return True

def get_pending_request(user_id, group_id):
    """获取待审批申请，不存在返回 None"""
    if not redis_client:
        return None
    key = f"{PENDING_REQUEST_PREFIX}{user_id}_{group_id}"
    data = redis_client.get(key)
    return json.loads(data) if data else None

def delete_pending_request(user_id, group_id):
    """删除待审批申请"""
    if not redis_client:
        return
    redis_client.delete(f"{PENDING_REQUEST_PREFIX}{user_id}_{group_id}")

# ============ 管理员输入状态管理 ============

def get_admin_state(user_id):
    """获取管理员当前输入状态"""
    if not redis_client:
        return None
    data = redis_client.get(f"{ADMIN_STATE_PREFIX}{user_id}")
    return json.loads(data) if data else None

def set_admin_state(user_id, state: dict):
    """设置管理员输入状态（5 分钟过期）"""
    if not redis_client:
        return
    redis_client.setex(f"{ADMIN_STATE_PREFIX}{user_id}", ADMIN_STATE_TTL, json.dumps(state))

def clear_admin_state(user_id):
    """清除管理员输入状态"""
    if not redis_client:
        return
    redis_client.delete(f"{ADMIN_STATE_PREFIX}{user_id}")

def migrate_global_groups():
    """将旧全局群组数据迁移到按管理员隔离的 key"""
    if not redis_client:
        return
    try:
        data = redis_client.get(GROUPS_KEY)
        if not data:
            return
        global_groups = json.loads(data)
        if not global_groups:
            return
        logger.info(f"Migrating {len(global_groups)} groups from global key to per-admin keys...")
        for gid, info in global_groups.items():
            admin_id = info.get("added_by")
            if not admin_id:
                logger.warning(f"Group {gid} has no added_by, skipping migration")
                continue
            save_group(gid, info["title"], admin_id)
        # 重命名旧 key 以防止重复迁移
        try:
            redis_client.rename(GROUPS_KEY, f"{GROUPS_KEY}:migrated")
        except Exception as rename_err:
            logger.warning(f"Could not rename old groups key: {rename_err}")
        logger.info("Migration complete")
    except Exception as e:
        logger.error(f"Migration failed: {e}")

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

def log_invite(user_id, group_id, invite_link, group_title, admin_id=None):
    """记录邀请日志"""
    if not redis_client:
        return
    log_entry = {
        "user_id": user_id,
        "group_id": group_id,
        "group_title": group_title,
        "invite_link": invite_link,
        "admin_id": admin_id,
        "created_at": datetime.now().isoformat(),
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

def format_user_info(user_id, first_name="", last_name="", username=None):
    """格式化用户信息（用于管理员审批通知），包含可点击的私聊链接"""
    full_name = f"{first_name} {last_name}".strip() or str(user_id)
    name_link = f'<a href="tg://user?id={user_id}">{full_name}</a>'
    username_part = f"@{username}" if username else "无用户名"
    return f"昵称：{name_link}\n用户名：{username_part}\nID：<code>{user_id}</code>"

# ============ 自动清理功能 ============

async def cleanup_expired_invites():
    """清理 Redis 中的过期邀请记录（删除7天前的日志）"""
    if not redis_client:
        return 0
    
    logs = redis_client.lrange(INVITE_LOG_KEY, 0, -1)
    removed = 0
    
    for log in logs:
        try:
            entry = json.loads(log)
            created_at = datetime.fromisoformat(entry['created_at'])
            if datetime.now() - created_at > timedelta(days=7):
                redis_client.lrem(INVITE_LOG_KEY, 0, log)
                removed += 1
        except:
            continue
    
    if removed > 0:
        logger.info(f"Cleanup: removed {removed} old invite logs from Redis")
    return removed

async def revoke_expired_invites(application: Application):
    """邀请链接仅限次数，无时效，无需自动撤销"""
    return 0

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
                        elif datetime.now() - datetime.fromisoformat(entry['created_at']) < timedelta(days=1):
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

async def request_join_approval(update, context, user, group_id, group_title, admin_id):
    """提交加群申请，通知管理员审批；返回 True 表示申请已提交"""
    existing = get_pending_request(user.id, group_id)
    if existing:
        await update.message.reply_text(
            f"⏳ 你已提交过「{group_title}」的申请，请等待管理员审核"
        )
        return True

    user_info = {"username": user.username, "first_name": user.first_name or ""}
    save_pending_request(user.id, group_id, user_info, group_title, admin_id)

    user_detail = format_user_info(user.id, user.first_name or "", user.last_name or "", user.username)
    keyboard = [[
        InlineKeyboardButton("✅ 同意", callback_data=f"approve_{user.id}_{group_id}"),
        InlineKeyboardButton("❌ 拒绝", callback_data=f"reject_{user.id}_{group_id}")
    ]]
    try:
        await context.bot.send_message(
            chat_id=admin_id,
            text=f"📋 加群申请\n\n{user_detail}\n\n申请加入：{group_title}",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Failed to notify admin {admin_id}: {e}")
        delete_pending_request(user.id, group_id)
        await update.message.reply_text(f"❌ {group_title} 申请提交失败，请联系管理员")
        return False

    await update.message.reply_text(f"📤 已提交加入「{group_title}」的申请，请等待管理员审核")
    return True

async def send_single_invite(update, context, user, group_id, group_title, admin_id=None):
    """发送单个邀请；若群组开启审批则转为申请流程"""
    if admin_id:
        groups = get_groups(admin_id)
        if groups.get(str(group_id), {}).get('approval_required', False):
            return await request_join_approval(update, context, user, group_id, group_title, admin_id)

    can_get, ttl = can_user_get_invite(user.id, group_id)
    if not can_get:
        time_left = format_time_left(ttl)
        await update.message.reply_text(
            f"⏳ {group_title} 冷却中，请 {time_left} 后再试"
        )
        return False
    
    try:
        invite_link = await context.bot.create_chat_invite_link(
            chat_id=int(group_id),
            member_limit=1
        )
        
        log_invite(user.id, group_id, invite_link.invite_link, group_title, admin_id)
        record_user_invite(user.id, group_id)
        
        keyboard = [[InlineKeyboardButton(f"👉 加入 {group_title}", url=invite_link.invite_link)]]
        await update.message.reply_text(
            f"✅ 已为你生成「{group_title}」的专属邀请链接\n🔒 仅限使用一次",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return True
        
    except Exception as e:
        logger.error(f"Failed to create invite: {e}")
        await update.message.reply_text(f"❌ {group_title} 邀请生成失败")
        return False

def build_group_selection_keyboard(user_id, admin_id, groups):
    """构建群组选择键盘和提示文本"""
    keyboard = []
    for gid, info in groups.items():
        can_get, ttl = can_user_get_invite(user_id, gid)
        if not can_get:
            label = f"✅ {info['title']} (已领取)"
        elif info.get('approval_required', False):
            label = f"🔒 {info['title']} (需审批)"
        else:
            label = f"👥 {info['title']}"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"select_{gid}_{user_id}_{admin_id}")])

    text = WELCOME_TEXT + f"\n\n🔒 每群组每{INVITE_COOLDOWN_HOURS}小时限领一次\n✅ = 已领取"
    return keyboard, text

async def handle_join_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, user, admin_id):
    """处理用户加入流程（选择单个群组）"""
    groups = get_groups(admin_id)
    logger.info(f"Current groups for admin {admin_id}: {groups}")
    
    if not groups:
        await update.message.reply_text("机器人尚未配置群组，请联系管理员")
        return
    
    # 只有一个群组，直接处理
    if len(groups) == 1:
        group_id = list(groups.keys())[0]
        await send_single_invite(update, context, user, group_id, list(groups.values())[0]['title'], admin_id)
        return
    
    # 多个群组，显示选择菜单
    keyboard, text = build_group_selection_keyboard(user.id, admin_id, groups)
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_join_all(update: Update, context: ContextTypes.DEFAULT_TYPE, user, admin_id):
    """处理批量加入所有群组"""
    groups = get_groups(admin_id)
    
    if not groups:
        await update.message.reply_text("机器人尚未配置群组，请联系管理员")
        return
    
    # 只有一个群组，直接处理
    if len(groups) == 1:
        group_id = list(groups.keys())[0]
        await send_single_invite(update, context, user, group_id, list(groups.values())[0]['title'], admin_id)
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
        await send_single_invite(update, context, user, gid, title, admin_id)
        return
    
    # 多个可用，直接生成所有邀请链接，无需确认
    processing_msg = await update.message.reply_text("⏳ 正在处理，请稍候...")
    
    keyboard_buttons = []
    failed_groups = []
    approval_submitted = []
    success_count = 0
    
    for gid, title in available_groups:
        group_info = groups[gid]
        if group_info.get('approval_required', False):
            # 该群组需要审批
            existing = get_pending_request(user.id, gid)
            if existing:
                approval_submitted.append(f"⏳ {title} (审核中)")
            else:
                user_info = {"username": user.username, "first_name": user.first_name or ""}
                save_pending_request(user.id, gid, user_info, title, admin_id)
                user_detail = format_user_info(user.id, user.first_name or "", user.last_name or "", user.username)
                notify_keyboard = [[
                    InlineKeyboardButton("✅ 同意", callback_data=f"approve_{user.id}_{gid}"),
                    InlineKeyboardButton("❌ 拒绝", callback_data=f"reject_{user.id}_{gid}")
                ]]
                try:
                    await context.bot.send_message(
                        chat_id=admin_id,
                        text=f"📋 加群申请\n\n{user_detail}\n\n申请加入：{title}",
                        reply_markup=InlineKeyboardMarkup(notify_keyboard),
                        parse_mode="HTML"
                    )
                    approval_submitted.append(f"📤 {title} (等待审核)")
                except Exception as e:
                    logger.error(f"Failed to notify admin for {gid}: {e}")
                    delete_pending_request(user.id, gid)
                    failed_groups.append(title)
        else:
            try:
                invite_link = await context.bot.create_chat_invite_link(
                    chat_id=int(gid),
                    member_limit=1
                )
                log_invite(user.id, gid, invite_link.invite_link, title, admin_id)
                record_user_invite(user.id, gid)
                keyboard_buttons.append([InlineKeyboardButton(f"👉 加入 {title}", url=invite_link.invite_link)])
                success_count += 1
            except Exception as e:
                logger.error(f"Failed to create invite for {gid}: {e}")
                failed_groups.append(title)
    
    text = ""
    if success_count:
        text += f"✅ 已生成 {success_count} 个邀请链接\n"
    if approval_submitted:
        text += "\n".join(approval_submitted) + "\n"
    if cooling_groups:
        text += f"⏳ {len(cooling_groups)} 个群组冷却中\n"
    if failed_groups:
        text += f"❌ {len(failed_groups)} 个群组处理失败\n"
    if success_count:
        text += "\n🔒 每个链接仅限使用一次"
    
    await processing_msg.edit_text(
        text or "处理完成",
        reply_markup=InlineKeyboardMarkup(keyboard_buttons) if keyboard_buttons else None
    )

# ============ 管理员面板（按钮菜单）============

def build_admin_main_keyboard():
    """构建管理员主菜单键盘"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📋 群组管理", callback_data="adm_groups"),
            InlineKeyboardButton("📊 统计数据", callback_data="adm_stats"),
        ],
        [
            InlineKeyboardButton("🔗 分享链接", callback_data="adm_links"),
            InlineKeyboardButton("🧪 测试连接", callback_data="adm_test"),
        ],
        [
            InlineKeyboardButton("🧹 清理数据", callback_data="adm_cleanup"),
            InlineKeyboardButton("🚫 撤销链接", callback_data="adm_revoke"),
        ],
        [
            InlineKeyboardButton("👥 添加管理员", callback_data="adm_addadmin"),
        ],
    ])

def build_admin_main_text(user_id):
    """构建管理员主菜单文本"""
    groups = get_groups(user_id)
    return (
        f"🤖 管理员面板\n\n"
        f"已绑定群组: {len(groups)} 个\n"
        f"邀请冷却: {INVITE_COOLDOWN_HOURS}小时/群组"
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.message.text or ""
    logger.info(f"Start command from user: {user.id}, text: {text}")
    
    # 解析 start 参数：/start join_{admin_id} 或 /start joinall_{admin_id}
    start_param = ""
    if " " in text:
        start_param = text.split(" ", 1)[1].strip()
    
    if not is_admin(user.id):
        if start_param.startswith("joinall_"):
            try:
                admin_id = int(start_param[len("joinall_"):])
                await handle_join_all(update, context, user, admin_id)
            except (ValueError, IndexError):
                await update.message.reply_text("链接无效，请联系管理员获取正确链接")
        elif start_param.startswith("join_"):
            try:
                admin_id = int(start_param[len("join_"):])
                await handle_join_flow(update, context, user, admin_id)
            except (ValueError, IndexError):
                await update.message.reply_text("链接无效，请联系管理员获取正确链接")
        else:
            await update.message.reply_text("此机器人仅限授权管理员使用")
        return
    
    # 管理员面板
    if not start_param:
        clear_admin_state(user.id)
        await update.message.reply_text(
            build_admin_main_text(user.id),
            reply_markup=build_admin_main_keyboard()
        )
    elif start_param.startswith("joinall_"):
        try:
            admin_id = int(start_param[len("joinall_"):])
            await handle_join_all(update, context, user, admin_id)
        except (ValueError, IndexError):
            await update.message.reply_text("链接无效")
    elif start_param.startswith("join_"):
        try:
            admin_id = int(start_param[len("join_"):])
            await handle_join_flow(update, context, user, admin_id)
        except (ValueError, IndexError):
            await update.message.reply_text("链接无效")

async def select_group_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理群组选择"""
    query = update.callback_query
    await query.answer()
    
    data = query.data.split("_")
    # format: select_{group_id}_{user_id}_{admin_id}
    if len(data) < 4:
        await query.edit_message_text("链接已失效，请重新获取")
        return
    group_id = data[1]
    user_id = int(data[2])
    admin_id = int(data[3])
    
    if query.from_user.id != user_id:
        await query.answer("这不是你的选择", show_alert=True)
        return
    
    groups = get_groups(admin_id)
    if group_id not in groups:
        await query.edit_message_text("该群组已不可用")
        return

    group_title = groups[group_id]['title']

    # 检查冷却时间 - 直接返回第一个界面，无需额外点击
    can_get, ttl = can_user_get_invite(user_id, group_id)
    if not can_get:
        keyboard, text = build_group_selection_keyboard(user_id, admin_id, groups)
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    # 检查是否需要审批
    if groups[group_id].get('approval_required', False):
        existing = get_pending_request(user_id, group_id)
        if existing:
            # 已提交过申请，直接返回第一个界面
            keyboard, text = build_group_selection_keyboard(user_id, admin_id, groups)
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        user_info = {"username": query.from_user.username, "first_name": query.from_user.first_name or ""}
        save_pending_request(user_id, group_id, user_info, group_title, admin_id)
        user_detail = format_user_info(user_id, query.from_user.first_name or "", query.from_user.last_name or "", query.from_user.username)
        notify_keyboard = [[
            InlineKeyboardButton("✅ 同意", callback_data=f"approve_{user_id}_{group_id}"),
            InlineKeyboardButton("❌ 拒绝", callback_data=f"reject_{user_id}_{group_id}")
        ]]
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=f"📋 加群申请\n\n{user_detail}\n\n申请加入：{group_title}",
                reply_markup=InlineKeyboardMarkup(notify_keyboard),
                parse_mode="HTML"
            )
            # 申请已提交，返回第一个界面并显示提交确认
            keyboard, text = build_group_selection_keyboard(user_id, admin_id, groups)
            await query.edit_message_text(
                f"📤 已提交加入「{group_title}」的申请，请等待管理员审核\n\n{text}",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"Failed to notify admin {admin_id}: {e}")
            delete_pending_request(user_id, group_id)
            await query.edit_message_text(f"❌ {group_title} 申请提交失败，请联系管理员")
        return

    # 直接生成邀请链接，无需二次确认
    try:
        invite_link = await context.bot.create_chat_invite_link(
            chat_id=int(group_id),
            member_limit=1
        )

        log_invite(user_id, group_id, invite_link.invite_link, group_title, admin_id)
        record_user_invite(user_id, group_id)

        # 生成成功后直接返回第一个界面（群组选择），邀请按钮置顶
        groups_updated = get_groups(admin_id)
        sel_keyboard, sel_text = build_group_selection_keyboard(user_id, admin_id, groups_updated)
        keyboard = [
            [InlineKeyboardButton(f"👉 点击加入 {group_title}", url=invite_link.invite_link)],
            *sel_keyboard
        ]
        await query.edit_message_text(
            sel_text,
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
    # format: joinall_{user_id}_{admin_id}
    if len(data) < 3:
        await query.edit_message_text("链接已失效，请重新获取")
        return
    user_id = int(data[1])
    admin_id = int(data[2])
    
    if query.from_user.id != user_id:
        await query.answer("这不是你的请求", show_alert=True)
        return
    
    groups = get_groups(admin_id)
    keyboard_buttons = []
    status_lines = []
    success_count = 0
    
    # 编辑消息显示处理中
    await query.edit_message_text("⏳ 正在处理，请稍候...")
    
    for gid, info in groups.items():
        # 检查冷却
        can_get, ttl = can_user_get_invite(user_id, gid)
        if not can_get:
            status_lines.append(f"✅ {info['title']} (已领取，冷却 {format_time_left(ttl)})")
            continue
        
        # 检查是否需要审批
        if info.get('approval_required', False):
            existing = get_pending_request(user_id, gid)
            if existing:
                status_lines.append(f"⏳ {info['title']} - 审核中")
                continue
            user_info = {"username": query.from_user.username, "first_name": query.from_user.first_name or ""}
            save_pending_request(user_id, gid, user_info, info['title'], admin_id)
            user_detail = format_user_info(user_id, query.from_user.first_name or "", query.from_user.last_name or "", query.from_user.username)
            notify_keyboard = [[
                InlineKeyboardButton("✅ 同意", callback_data=f"approve_{user_id}_{gid}"),
                InlineKeyboardButton("❌ 拒绝", callback_data=f"reject_{user_id}_{gid}")
            ]]
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=f"📋 加群申请\n\n{user_detail}\n\n申请加入：{info['title']}",
                    reply_markup=InlineKeyboardMarkup(notify_keyboard),
                    parse_mode="HTML"
                )
                status_lines.append(f"📤 {info['title']} - 等待审核")
            except Exception as e:
                logger.error(f"Failed to notify admin for {gid}: {e}")
                delete_pending_request(user_id, gid)
                status_lines.append(f"❌ {info['title']} - 申请提交失败")
            continue
        
        try:
            # 生成邀请
            invite_link = await context.bot.create_chat_invite_link(
                chat_id=int(gid),
                member_limit=1
            )
            
            # 记录
            log_invite(user_id, gid, invite_link.invite_link, info['title'], admin_id)
            record_user_invite(user_id, gid)
            
            keyboard_buttons.append([InlineKeyboardButton(f"👉 加入 {info['title']}", url=invite_link.invite_link)])
            success_count += 1
            
        except Exception as e:
            logger.error(f"Failed to create invite for {gid}: {e}")
            status_lines.append(f"❌ {info['title']} - 生成失败")
    
    # 组装文本
    text_parts = []
    if success_count:
        text_parts.append(f"✅ 已生成 {success_count} 个邀请链接，点击下方按钮加入各群组：")
    if status_lines:
        text_parts.append("\n".join(status_lines))
    if success_count:
        text_parts.append("🔒 每个链接仅限使用一次")
    
    await query.edit_message_text(
        "\n\n".join(text_parts) if text_parts else "处理完成",
        reply_markup=InlineKeyboardMarkup(keyboard_buttons) if keyboard_buttons else None
    )

async def backselect_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """返回群组选择菜单"""
    query = update.callback_query
    await query.answer()

    data = query.data.split("_")
    # format: backselect_{user_id}_{admin_id}
    if len(data) < 3:
        await query.edit_message_text("链接已失效，请重新获取")
        return

    user_id = int(data[1])
    admin_id = int(data[2])

    if query.from_user.id != user_id:
        await query.answer("这不是你的操作", show_alert=True)
        return

    groups = get_groups(admin_id)
    if not groups:
        await query.edit_message_text("机器人尚未配置群组，请联系管理员")
        return

    keyboard, text = build_group_selection_keyboard(user_id, admin_id, groups)
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

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
            f"✅ 你已领取过该群组的邀请链接\n冷却剩余：{time_left}"
        )
        return
    
    try:
        invite_link = await context.bot.create_chat_invite_link(
            chat_id=int(group_id),
            member_limit=1
        )
        
        # 记录邀请
        admin_id_str = redis_client.get(f"{GROUP_OWNER_PREFIX}{group_id}") if redis_client else None
        admin_id = int(admin_id_str) if admin_id_str else None
        groups = get_groups(admin_id) if admin_id else {}
        group_title = groups.get(group_id, {}).get('title', 'Unknown')
        log_invite(user_id, group_id, invite_link.invite_link, group_title, admin_id)
        record_user_invite(user_id, group_id)
        
        keyboard = [[InlineKeyboardButton("👉 点击加入群组", url=invite_link.invite_link)]]
        await query.edit_message_text(
            f"✅ 已为你生成专属邀请链接\n🔒 仅限使用一次",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        logger.info(f"User {user_id} got invite link for group {group_id}")
        
    except Exception as e:
        logger.error(f"Failed to create invite link: {e}")
        await query.edit_message_text("生成邀请链接失败，请联系管理员")

async def approve_request_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """管理员同意加群申请"""
    query = update.callback_query
    await query.answer()

    # format: approve_{user_id}_{group_id}
    parts = query.data.split("_", 2)
    if len(parts) < 3:
        await query.edit_message_text("数据格式错误")
        return

    user_id = int(parts[1])
    group_id = parts[2]
    admin_id = query.from_user.id

    req = get_pending_request(user_id, group_id)
    if not req:
        await query.edit_message_text("❌ 申请已过期或不存在")
        return

    group_title = req['group_title']

    try:
        invite_link = await context.bot.create_chat_invite_link(
            chat_id=int(group_id),
            member_limit=1
        )
        log_invite(user_id, group_id, invite_link.invite_link, group_title, admin_id)
        record_user_invite(user_id, group_id)
        delete_pending_request(user_id, group_id)

        try:
            keyboard = [[InlineKeyboardButton(f"👉 加入 {group_title}", url=invite_link.invite_link)]]
            await context.bot.send_message(
                chat_id=user_id,
                text=f"✅ 你的加入「{group_title}」申请已通过！\n"
                     f"🔒 仅限一次使用",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"Failed to notify user {user_id}: {e}")

        await query.edit_message_text(f"✅ 已同意用户 {user_id} 加入「{group_title}」")
        logger.info(f"Admin {admin_id} approved join request: user {user_id} -> group {group_id}")

    except Exception as e:
        logger.error(f"Failed to create invite for approved request: {e}")
        await query.edit_message_text(f"❌ 生成邀请链接失败: {e}")

async def reject_request_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """管理员拒绝加群申请"""
    query = update.callback_query
    await query.answer()

    # format: reject_{user_id}_{group_id}
    parts = query.data.split("_", 2)
    if len(parts) < 3:
        await query.edit_message_text("数据格式错误")
        return

    user_id = int(parts[1])
    group_id = parts[2]

    req = get_pending_request(user_id, group_id)
    group_title = req['group_title'] if req else "未知群组"
    delete_pending_request(user_id, group_id)

    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=f"❌ 你的加入「{group_title}」申请未通过"
        )
    except Exception as e:
        logger.error(f"Failed to notify user {user_id}: {e}")

    await query.edit_message_text(f"❌ 已拒绝用户 {user_id} 加入「{group_title}」")
    logger.info(f"Admin {query.from_user.id} rejected join request: user {user_id} -> group {group_id}")

async def admin_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理管理员面板的所有 adm_* 回调按钮"""
    query = update.callback_query
    user = query.from_user

    if not is_admin(user.id):
        await query.answer("⛔ 你没有权限", show_alert=True)
        return

    # 任何按钮点击都清除输入状态
    clear_admin_state(user.id)

    data = query.data
    back_btn = [[InlineKeyboardButton("⬅️ 返回", callback_data="adm_back")]]

    # ── 主菜单 / 返回 ──────────────────────────────────────────────────────────
    if data == "adm_back":
        await query.answer()
        await query.edit_message_text(
            build_admin_main_text(user.id),
            reply_markup=build_admin_main_keyboard()
        )
        return

    # ── 群组列表 ───────────────────────────────────────────────────────────────
    if data == "adm_groups":
        await query.answer()
        groups = get_groups(user.id)
        keyboard = []
        if groups:
            for gid, info in groups.items():
                icon = "🔒" if info.get('approval_required', False) else "🔓"
                keyboard.append([InlineKeyboardButton(
                    f"{icon} {info['title']}",
                    callback_data=f"adm_grp_info_{gid}"
                )])
        keyboard.append([InlineKeyboardButton("➕ 手动绑定群组", callback_data="adm_bindgroup")])
        keyboard.append([InlineKeyboardButton("⬅️ 返回", callback_data="adm_back")])
        msg = (f"📋 已绑定群组（{len(groups)} 个）\n🔓=直接加入  🔒=需审批"
               if groups else "暂无绑定的群组\n\n将机器人设为群管理员后即可自动绑定")
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    # ── 群组详情 ───────────────────────────────────────────────────────────────
    if data.startswith("adm_grp_info_"):
        await query.answer()
        gid = data[len("adm_grp_info_"):]
        groups = get_groups(user.id)
        if gid not in groups:
            await query.edit_message_text(
                "该群组不存在",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回", callback_data="adm_groups")]])
            )
            return
        info = groups[gid]
        approval = "🔒 需审批" if info.get('approval_required', False) else "🔓 直接加入"
        toggle_label = "🔓 切换为直接加入" if info.get('approval_required', False) else "🔒 切换为需审批"
        keyboard = [
            [InlineKeyboardButton(toggle_label, callback_data=f"adm_grp_tog_{gid}")],
            [InlineKeyboardButton("❌ 移除群组", callback_data=f"adm_grp_del_{gid}")],
            [InlineKeyboardButton("⬅️ 返回群组列表", callback_data="adm_groups")],
        ]
        await query.edit_message_text(
            f"群组：{info['title']}\nID: `{gid}`\n审批模式: {approval}",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        return

    # ── 切换审批模式 ───────────────────────────────────────────────────────────
    if data.startswith("adm_grp_tog_"):
        gid = data[len("adm_grp_tog_"):]
        groups = get_groups(user.id)
        if gid not in groups:
            await query.answer("群组不存在", show_alert=True)
            return
        current = groups[gid].get('approval_required', False)
        set_group_approval(gid, user.id, not current)
        status = "开启 🔒" if not current else "关闭 🔓"
        await query.answer(f"审批模式已{status}")
        # 刷新群组详情
        groups = get_groups(user.id)
        info = groups[gid]
        approval = "🔒 需审批" if info.get('approval_required', False) else "🔓 直接加入"
        toggle_label = "🔓 切换为直接加入" if info.get('approval_required', False) else "🔒 切换为需审批"
        keyboard = [
            [InlineKeyboardButton(toggle_label, callback_data=f"adm_grp_tog_{gid}")],
            [InlineKeyboardButton("❌ 移除群组", callback_data=f"adm_grp_del_{gid}")],
            [InlineKeyboardButton("⬅️ 返回群组列表", callback_data="adm_groups")],
        ]
        await query.edit_message_text(
            f"群组：{info['title']}\nID: `{gid}`\n审批模式: {approval}",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        return

    # ── 确认移除群组 ───────────────────────────────────────────────────────────
    if data.startswith("adm_grp_delok_"):
        await query.answer()
        gid = data[len("adm_grp_delok_"):]
        groups = get_groups(user.id)
        title = groups.get(gid, {}).get('title', gid)
        result = remove_group(gid)
        msg = f"✅ 已移除群组「{title}」" if result else "❌ 移除失败"
        await query.edit_message_text(
            msg,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回群组列表", callback_data="adm_groups")]])
        )
        return

    # ── 提示确认移除 ──────────────────────────────────────────────────────────
    if data.startswith("adm_grp_del_"):
        await query.answer()
        gid = data[len("adm_grp_del_"):]
        groups = get_groups(user.id)
        if gid not in groups:
            await query.answer("群组不存在", show_alert=True)
            return
        info = groups[gid]
        keyboard = [[
            InlineKeyboardButton("✅ 确认移除", callback_data=f"adm_grp_delok_{gid}"),
            InlineKeyboardButton("取消", callback_data=f"adm_grp_info_{gid}"),
        ]]
        await query.edit_message_text(
            f"⚠️ 确认移除群组「{info['title']}」？",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    # ── 统计数据 ───────────────────────────────────────────────────────────────
    if data == "adm_stats":
        await query.answer()
        if not redis_client:
            await query.edit_message_text("Redis 不可用", reply_markup=InlineKeyboardMarkup(back_btn))
            return
        logs = redis_client.lrange(INVITE_LOG_KEY, 0, 999)
        recent_invites = []
        revoked_count = 0
        for log in logs:
            try:
                entry = json.loads(log)
                if str(entry.get('admin_id')) != str(user.id):
                    continue
                if entry.get('revoked', False):
                    revoked_count += 1
                created = datetime.fromisoformat(entry['created_at'])
                if datetime.now() - created < timedelta(days=1):
                    recent_invites.append(entry)
            except:
                continue
        total_24h = len(recent_invites)
        unique_users = len(set(i['user_id'] for i in recent_invites))
        text = (
            f"📊 邀请统计（最近24小时）\n\n"
            f"总邀请数: {total_24h}\n"
            f"独立用户: {unique_users}\n"
            f"已撤销链接: {revoked_count}\n\n"
            f"配置:\n"
            f"邀请冷却: {INVITE_COOLDOWN_HOURS}小时/群组"
        )
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(back_btn))
        return

    # ── 分享链接 ───────────────────────────────────────────────────────────────
    if data == "adm_links":
        await query.answer()
        bot_username = context.bot.username
        text = (
            f"🔗 分享链接（仅包含你的群组）：\n\n"
            f"• 选择加入：\nhttps://t.me/{bot_username}?start=join_{user.id}\n\n"
            f"• 一键加入全部：\nhttps://t.me/{bot_username}?start=joinall_{user.id}"
        )
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(back_btn))
        return

    # ── 测试连接 ───────────────────────────────────────────────────────────────
    if data == "adm_test":
        await query.answer()
        redis_status = "连接正常" if redis_client and redis_client.ping() else "连接失败"
        groups = get_groups(user.id)
        text = (
            f"🧪 测试报告\n\n"
            f"Redis 状态: {redis_status}\n"
            f"已绑定群组: {len(groups)} 个\n"
            f"邀请冷却: {INVITE_COOLDOWN_HOURS}小时/群组\n"
            f"当前用户ID: {user.id}\n"
            f"是否为管理员: 是"
        )
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(back_btn))
        return

    # ── 清理数据 ───────────────────────────────────────────────────────────────
    if data == "adm_cleanup":
        await query.answer()
        await query.edit_message_text("🧹 正在清理过期数据...")
        removed = await cleanup_expired_invites()
        if redis_client:
            logs = redis_client.lrange(INVITE_LOG_KEY, 0, -1)
            valid = expired = revoked = 0
            for log in logs:
                try:
                    entry = json.loads(log)
                    if entry.get('revoked', False):
                        revoked += 1
                    elif datetime.now() - datetime.fromisoformat(entry['created_at']) < timedelta(days=1):
                        valid += 1
                    else:
                        expired += 1
                except:
                    expired += 1
            text = (
                f"✅ 清理完成\n\n"
                f"🗑️ 已删除记录: {removed}\n"
                f"✨ 近24小时邀请: {valid}\n"
                f"📅 超24小时记录: {expired}\n"
                f"🚫 已撤销: {revoked}\n"
                f"📊 总计: {len(logs)}"
            )
        else:
            text = "❌ Redis 不可用"
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(back_btn))
        return

    # ── 撤销链接 ───────────────────────────────────────────────────────────────
    if data == "adm_revoke":
        await query.answer()
        await query.edit_message_text("🚫 正在撤销失效的邀请链接...")
        revoked = await revoke_expired_invites(context.application)
        await query.edit_message_text(
            f"✅ 已撤销 {revoked} 个失效的邀请链接",
            reply_markup=InlineKeyboardMarkup(back_btn)
        )
        return

    # ── 添加管理员（等待输入）─────────────────────────────────────────────────
    if data == "adm_addadmin":
        await query.answer()
        set_admin_state(user.id, {"action": "add_admin"})
        await query.edit_message_text(
            "👥 请发送要添加的管理员用户 ID：\n（点击「取消」或发送 /cancel 可退出）",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("取消", callback_data="adm_back")]])
        )
        return

    # ── 手动绑定群组（等待输入）───────────────────────────────────────────────
    if data == "adm_bindgroup":
        await query.answer()
        set_admin_state(user.id, {"action": "bind_group_id"})
        await query.edit_message_text(
            "➕ 请发送要绑定的群组 ID：\n（例如：-1001234567890）\n（点击「取消」或发送 /cancel 可退出）",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("取消", callback_data="adm_groups")]])
        )
        return

async def admin_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理管理员在输入状态下发送的文本消息"""
    user = update.effective_user
    if not is_admin(user.id):
        return

    state = get_admin_state(user.id)
    if not state:
        return

    text = update.message.text.strip()
    action = state.get("action")

    if action == "add_admin":
        try:
            new_admin_id = int(text)
        except ValueError:
            await update.message.reply_text("❌ 无效的用户 ID，请发送纯数字")
            return
        if not redis_client:
            await update.message.reply_text("❌ 系统错误：Redis 不可用")
            clear_admin_state(user.id)
            return
        redis_client.sadd(ADMINS_KEY, str(new_admin_id))
        clear_admin_state(user.id)
        await update.message.reply_text(
            f"✅ 已添加管理员: {new_admin_id}",
            reply_markup=build_admin_main_keyboard()
        )
        return

    if action == "bind_group_id":
        try:
            group_id = int(text)
        except ValueError:
            await update.message.reply_text("❌ 无效的群组 ID，请发送数字（如 -1001234567890）")
            return
        set_admin_state(user.id, {"action": "bind_group_name", "group_id": str(group_id)})
        await update.message.reply_text(f"✅ 群组 ID: {group_id}\n\n请继续发送群组名称：")
        return

    if action == "bind_group_name":
        group_id = state.get("group_id")
        group_title = text
        clear_admin_state(user.id)
        if save_group(group_id, group_title, user.id):
            bot_username = context.bot.username
            await update.message.reply_text(
                f"✅ 已绑定群组：{group_title}\n"
                f"分享链接：https://t.me/{bot_username}?start=join_{user.id}",
                reply_markup=build_admin_main_keyboard()
            )
        else:
            await update.message.reply_text("❌ 绑定失败", reply_markup=build_admin_main_keyboard())
        return

async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """取消当前输入状态，返回管理员主菜单"""
    user = update.effective_user
    if not is_admin(user.id):
        return
    clear_admin_state(user.id)
    await update.message.reply_text(
        build_admin_main_text(user.id),
        reply_markup=build_admin_main_keyboard()
    )

async def set_approval_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """切换群组的审批模式（开/关）"""
    user = update.effective_user

    if not is_admin(user.id):
        await update.message.reply_text("你没有权限")
        return

    if not context.args:
        await update.message.reply_text(
            "用法: /setapproval [群组ID]\n"
            "每次执行会切换该群组的审批模式（开/关）\n"
            "开启后，用户需等待管理员同意才能获得邀请链接"
        )
        return

    group_id = context.args[0]
    groups = get_groups(user.id)

    if str(group_id) not in groups:
        await update.message.reply_text("未找到该群组，或你无权修改它")
        return

    current = groups[str(group_id)].get('approval_required', False)
    new_value = not current

    if set_group_approval(group_id, user.id, new_value):
        status = "开启 🔒" if new_value else "关闭 🔓"
        await update.message.reply_text(
            f"✅ 群组「{groups[str(group_id)]['title']}」审批模式已{status}"
        )
    else:
        await update.message.reply_text("❌ 设置失败")

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
                elif datetime.now() - datetime.fromisoformat(entry['created_at']) < timedelta(days=1):
                    valid += 1
                else:
                    expired += 1
            except:
                expired += 1
        
        text = (
            f"✅ 清理完成\n\n"
            f"🗑️ 已删除记录: {removed}\n"
            f"✨ 近24小时邀请: {valid}\n"
            f"📅 超24小时记录: {expired}\n"
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
    
    # 获取最近24小时属于该管理员的邀请记录
    logs = redis_client.lrange(INVITE_LOG_KEY, 0, 999)
    recent_invites = []
    revoked_count = 0
    
    for log in logs:
        try:
            entry = json.loads(log)
            if str(entry.get('admin_id')) != str(user.id):
                continue
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
        f"邀请冷却: {INVITE_COOLDOWN_HOURS}小时/群组"
    )
    
    await update.message.reply_text(text)

async def test_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """测试命令"""
    user = update.effective_user
    
    if not is_admin(user.id):
        await update.message.reply_text("你没有权限")
        return
    
    redis_status = "连接正常" if redis_client and redis_client.ping() else "连接失败"
    groups = get_groups(user.id)
    
    text = (
        f"🧪 测试报告\n\n"
        f"Redis 状态: {redis_status}\n"
        f"已绑定群组: {len(groups)} 个\n"
        f"邀请冷却: {INVITE_COOLDOWN_HOURS}小时/群组\n"
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
    
    old_status = old_member.status if old_member else None
    new_status = new_member.status
    
    logger.info(f"Bot status changed from {old_status} to {new_status}")
    
    # Bot is leaving or was kicked — handled by bot_removed_from_group
    if new_status in ('left', 'kicked'):
        return
    
    added_by = update.effective_user
    
    if not added_by or added_by.id == context.bot.id:
        logger.warning("Could not determine who added the bot")
        return
    
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
    
    if new_status == 'administrator' and old_status != 'administrator':
        if save_group(chat.id, chat.title, added_by.id):
            try:
                await context.bot.send_message(
                    chat_id=added_by.id,
                    text=f"✅ 机器人已成功绑定到群组「{chat.title}」\n"
                         f"群组ID: `{chat.id}`\n"
                         f"分享链接（仅包含你的群组）：\n"
                         f"• 选择加入: https://t.me/{context.bot.username}?start=join_{added_by.id}\n"
                         f"• 加入全部: https://t.me/{context.bot.username}?start=joinall_{added_by.id}",
                    parse_mode="Markdown"
                )
                logger.info(f"Notification sent to admin {added_by.id}")
            except Exception as e:
                logger.error(f"Failed to notify admin: {e}")
    elif new_status == 'member':
        mention = f"@{added_by.username}" if added_by.username else added_by.full_name
        try:
            await context.bot.send_message(
                chat_id=chat.id,
                text=f"{mention} 请将机器人设为管理员，否则无法使用加群功能"
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
            f"分享链接：https://t.me/{context.bot.username}?start=join_{user.id}"
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
    
    groups = get_groups(user.id)
    
    if not groups:
        await update.message.reply_text("暂无绑定的群组")
        return
    
    text = "已绑定群组列表：\n\n"
    for gid, info in groups.items():
        approval = "🔒 需审批" if info.get('approval_required', False) else "🔓 直接加入"
        text += f"• {info['title']}\n  ID: `{gid}`\n  审批模式: {approval}\n\n"
    
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
    # 验证该群组确实属于调用管理员
    groups = get_groups(user.id)
    if str(group_id) not in groups:
        await update.message.reply_text("未找到该群组，或你无权移除它")
        return
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
        migrate_global_groups()
        logger.info(f"Loaded admins: {get_admin_ids_from_env()}")
        logger.info(f"Cooldown: {INVITE_COOLDOWN_HOURS}h, invite links: count-limited only")
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    # 命令处理器
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("cancel", cancel_cmd))
    application.add_handler(CommandHandler("test", test_cmd))
    application.add_handler(CommandHandler("stats", stats_cmd))
    application.add_handler(CommandHandler("cleanup", cleanup_cmd))
    application.add_handler(CommandHandler("revoke", revoke_cmd))
    application.add_handler(CommandHandler("addadmin", add_admin_cmd))
    application.add_handler(CommandHandler("listgroups", list_groups_cmd))
    application.add_handler(CommandHandler("removegroup", remove_group_cmd))
    application.add_handler(CommandHandler("bindgroup", bind_group_cmd))
    application.add_handler(CommandHandler("setapproval", set_approval_cmd))

    # 管理员文本输入处理器（状态机）
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, admin_message_handler))

    # 回调处理器
    application.add_handler(CallbackQueryHandler(admin_callback_handler, pattern="^adm_"))
    application.add_handler(CallbackQueryHandler(approve_request_callback, pattern="^approve_"))
    application.add_handler(CallbackQueryHandler(reject_request_callback, pattern="^reject_"))
    application.add_handler(CallbackQueryHandler(select_group_callback, pattern="^select_"))
    application.add_handler(CallbackQueryHandler(backselect_callback, pattern="^backselect_"))
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
