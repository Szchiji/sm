import os
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# 配置日志
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# 从环境变量读取配置
BOT_TOKEN = os.getenv("BOT_TOKEN")
PRIVATE_GROUP_ID = os.getenv("PRIVATE_GROUP_ID")  # 例如: -1001234567890
WELCOME_TEXT = os.getenv("WELCOME_TEXT", "👋 欢迎！点击下方按钮加入群组：")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """用户点击 t.me/YourBot?start=xxx 时触发"""
    user = update.effective_user
    
    # 创建加入按钮
    keyboard = [
        [InlineKeyboardButton("🚀 立即加入群组", callback_data=f"join_{user.id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        WELCOME_TEXT,
        reply_markup=reply_markup
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理按钮点击"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    callback_data = query.data
    
    if callback_data.startswith("join_"):
        try:
            # 生成一次性邀请链接（可选，或直接用 approveChatJoinRequest）
            # 方案1: 如果群组开启了"加入请求"模式，直接批准
            # 方案2: 生成限时邀请链接
            
            # 这里使用方案2: 生成一次性邀请链接
            invite_link = await context.bot.create_chat_invite_link(
                chat_id=PRIVATE_GROUP_ID,
                member_limit=1,  # 只能使用一次
                expire_date=None  # 永不过期，或设置 300（5分钟后过期）
            )
            
            # 编辑消息，显示邀请链接
            keyboard = [[InlineKeyboardButton("👉 点击加入", url=invite_link.invite_link)]]
            await query.edit_message_text(
                "✅ 验证通过！点击下方链接加入群组（仅限你使用一次）：",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
            logger.info(f"用户 {user_id} 获取了邀请链接")
            
        except Exception as e:
            logger.error(f"生成邀请链接失败: {e}")
            await query.edit_message_text("❌ 出错了，请联系管理员")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """错误处理"""
    logger.error(f"Update {update} caused error {context.error}")

def main():
    # 创建应用
    application = Application.builder().token(BOT_TOKEN).build()
    
    # 添加处理器
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_error_handler(error_handler)
    
    # 启动机器人（使用 webhook 模式，适合 Railway）
    PORT = int(os.environ.get("PORT", 8080))
    RAILWAY_STATIC_URL = os.environ.get("RAILWAY_STATIC_URL")
    
    if RAILWAY_STATIC_URL:
        # Railway 环境：使用 Webhook
        application.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            webhook_url=f"{RAILWAY_STATIC_URL}/webhook",
            secret_token=BOT_TOKEN.split(":")[1]  # 使用 token 后半部分作为 secret
        )
    else:
        # 本地开发：使用 Polling
        application.run_polling()

if __name__ == "__main__":
    main()
