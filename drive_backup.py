"""将 Redis 关键数据备份到私有 Google Drive 文件夹。"""
import base64
import io
import json
import os
from datetime import datetime, timedelta, timezone

BACKUP_FOLDER_ID = os.getenv("GOOGLE_DRIVE_BACKUP_FOLDER_ID", "")
SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64", "")
RETENTION_DAYS = int(os.getenv("BACKUP_RETENTION_DAYS", "30"))
BACKUP_LAST_KEY = "tg_bot:backup:last"


def _credentials():
    if not BACKUP_FOLDER_ID or not SERVICE_ACCOUNT_B64:
        return None
    from google.oauth2.service_account import Credentials
    raw = base64.b64decode(SERVICE_ACCOUNT_B64).decode("utf-8")
    info = json.loads(raw)
    return Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/drive.file"],
    )


def build_snapshot(redis_client):
    """导出机器人需要恢复的 Redis 数据，不包含无关缓存。"""
    snapshot = {
        "schema_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "admins": sorted(redis_client.smembers("tg_bot:admins")),
        "groups": {},
        "group_owner": {},
        "invite_log": [],
        "pending": {},
        "active_invites": redis_client.hgetall("tg_bot:active_invites"),
    }
    for key in redis_client.scan_iter(match="tg_bot:groups:*"):
        snapshot["groups"][key] = redis_client.get(key)
    for key in redis_client.scan_iter(match="tg_bot:group_owner:*"):
        snapshot["group_owner"][key] = redis_client.get(key)
    for raw in redis_client.lrange("tg_bot:invite_log", 0, -1):
        try:
            snapshot["invite_log"].append(json.loads(raw))
        except (TypeError, ValueError):
            continue
    for key in redis_client.scan_iter(match="tg_bot:pending:*"):
        snapshot["pending"][key] = redis_client.get(key)
    return snapshot


def upload_snapshot(redis_client):
    """上传一个带 UTC 日期的 JSON 备份，并将旧备份移入回收站。"""
    credentials = _credentials()
    if credentials is None:
        return {"status": "disabled", "reason": "Google Drive credentials or folder ID missing"}

    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseUpload

    snapshot = build_snapshot(redis_client)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    filename = f"telegram-invite-bot-{stamp}.json"
    service = build("drive", "v3", credentials=credentials, cache_discovery=False)
    metadata = {"name": filename, "parents": [BACKUP_FOLDER_ID], "mimeType": "application/json"}
    media = MediaIoBaseUpload(
        io.BytesIO(json.dumps(snapshot, ensure_ascii=False, indent=2).encode("utf-8")),
        mimetype="application/json",
        resumable=False,
    )
    created = service.files().create(body=metadata, media_body=media, fields="id,name,createdTime").execute()

    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    query = (
        f"'{BACKUP_FOLDER_ID}' in parents and trashed = false "
        "and name contains 'telegram-invite-bot-'"
    )
    response = service.files().list(q=query, fields="files(id,name,createdTime)", pageSize=100).execute()
    trashed = 0
    for old in response.get("files", []):
        created_time = old.get("createdTime")
        if not created_time:
            continue
        created_at = datetime.fromisoformat(created_time.replace("Z", "+00:00"))
        if created_at < cutoff and old["id"] != created["id"]:
            # 永久删除被禁止；移入 Drive 回收站可恢复。
            service.files().update(fileId=old["id"], body={"trashed": True}).execute()
            trashed += 1
    return {"status": "uploaded", "file": created, "trashed": trashed}
