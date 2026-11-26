from drive_backup import build_snapshot, upload_snapshot


class FakeRedis:
    def __init__(self):
        self.values = {
            "tg_bot:groups:100": '{"-100": {"title": "Test"}}',
            "tg_bot:group_owner:-100": "100",
            "tg_bot:pending:7_-100": '{"group_title": "Test"}',
        }
        self.lists = {"tg_bot:invite_log": ['{"user_id": 7, "group_id": -100}']}
        self.hashes = {"tg_bot:active_invites": {"https://example.invalid/invite": '{"group_id": -100}'}}
        self.sets = {"tg_bot:admins": {"100"}}

    def smembers(self, key):
        return self.sets.get(key, set())

    def scan_iter(self, match):
        prefix = match[:-1]
        return (key for key in self.values if key.startswith(prefix))

    def get(self, key):
        return self.values.get(key)

    def lrange(self, key, start, end):
        return self.lists.get(key, [])[start:] if end == -1 else self.lists.get(key, [])[start:end + 1]

    def hgetall(self, key):
        return self.hashes.get(key, {})


def test_build_snapshot_contains_recoverable_data():
    snapshot = build_snapshot(FakeRedis())

    assert snapshot["schema_version"] == 1
    assert snapshot["admins"] == ["100"]
    assert snapshot["groups"]["tg_bot:groups:100"]
    assert snapshot["group_owner"]["tg_bot:group_owner:-100"] == "100"
    assert snapshot["invite_log"] == [{"user_id": 7, "group_id": -100}]
    assert "tg_bot:active_invites" not in snapshot["groups"]


def test_upload_is_disabled_without_drive_configuration(monkeypatch):
    monkeypatch.setattr("drive_backup.BACKUP_FOLDER_ID", "")
    monkeypatch.setattr("drive_backup.SERVICE_ACCOUNT_B64", "")

    result = upload_snapshot(FakeRedis())

    assert result["status"] == "disabled"
