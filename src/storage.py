"""SQLite 持久层。

所有状态(原始记录、事件片段、复核历史、投诉撤回、审计日志)全部落盘,
服务重启后时间段合并结果不丢失。原始记录表仅插入、不更新、不删除,
保证证据不可覆盖。
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
    device_id     TEXT PRIMARY KEY,
    trust_level   TEXT NOT NULL,
    registered_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS complaints (
    complaint_ref     TEXT PRIMARY KEY,
    withdrawn_at      TEXT,
    withdraw_reason   TEXT,
    withdraw_operator TEXT
);

-- 原始记录:insert-only,重复上传被拒绝而非覆盖。
-- UNIQUE(device_id, sampled_at) 是幂等去重键,含被拒绝的留痕记录。
CREATE TABLE IF NOT EXISTS records (
    record_id     TEXT PRIMARY KEY,
    device_id     TEXT NOT NULL,
    db_reading    REAL NOT NULL,
    sampled_at    TEXT NOT NULL,
    received_at   TEXT NOT NULL,
    grid_id       TEXT NOT NULL,
    complaint_ref TEXT NOT NULL,
    status        TEXT NOT NULL,
    reject_reason TEXT,
    created_at    TEXT NOT NULL,
    UNIQUE (device_id, sampled_at)
);

CREATE TABLE IF NOT EXISTS segments (
    segment_id       TEXT PRIMARY KEY,
    grid_id          TEXT NOT NULL,
    complaint_ref    TEXT NOT NULL,
    start_time       TEXT NOT NULL,
    end_time         TEXT NOT NULL,
    record_count     INTEGER NOT NULL,
    max_db           REAL NOT NULL,
    trusted_count    INTEGER NOT NULL,
    clue_count       INTEGER NOT NULL,
    penalty_eligible INTEGER NOT NULL,
    status           TEXT NOT NULL,
    merged_into      TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS segment_records (
    segment_id TEXT NOT NULL REFERENCES segments(segment_id),
    record_id  TEXT NOT NULL REFERENCES records(record_id),
    PRIMARY KEY (segment_id, record_id)
);

-- 复核历史:append-only,每次复核写新行,永不覆盖。
CREATE TABLE IF NOT EXISTS reviews (
    review_id   TEXT PRIMARY KEY,
    segment_id  TEXT NOT NULL REFERENCES segments(segment_id),
    conclusion  TEXT NOT NULL,
    basis       TEXT NOT NULL,
    operator    TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    action     TEXT NOT NULL,
    detail     TEXT NOT NULL,
    operator   TEXT,
    created_at TEXT NOT NULL
);
"""


class Storage:
    """对 SQLite 的薄封装:只负责读写,业务规则在 service 层。"""

    def __init__(self, db_path: str | Path):
        self._conn = sqlite3.connect(str(db_path))
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # ---- 设备 ----

    def upsert_device(self, device_id: str, trust_level: str, now: str) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT INTO devices(device_id, trust_level, registered_at) VALUES (?,?,?) "
                "ON CONFLICT(device_id) DO UPDATE SET trust_level=excluded.trust_level",
                (device_id, trust_level, now),
            )

    def get_device(self, device_id: str) -> Optional[dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM devices WHERE device_id=?", (device_id,)
        ).fetchone()
        return dict(row) if row else None

    # ---- 投诉 ----

    def get_complaint(self, complaint_ref: str) -> Optional[dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM complaints WHERE complaint_ref=?", (complaint_ref,)
        ).fetchone()
        return dict(row) if row else None

    def mark_complaint_withdrawn(
        self, complaint_ref: str, now: str, reason: str, operator: str
    ) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT INTO complaints(complaint_ref, withdrawn_at, withdraw_reason, withdraw_operator) "
                "VALUES (?,?,?,?)",
                (complaint_ref, now, reason, operator),
            )

    # ---- 原始记录(insert-only) ----

    def insert_record(self, rec: dict[str, Any]) -> bool:
        """插入原始记录;同设备同采样时刻已存在时返回 False,不覆盖原记录。"""
        with self._conn:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO records("
                "record_id, device_id, db_reading, sampled_at, received_at,"
                " grid_id, complaint_ref, status, reject_reason, created_at"
                ") VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    rec["record_id"], rec["device_id"], rec["db_reading"],
                    rec["sampled_at"], rec["received_at"], rec["grid_id"],
                    rec["complaint_ref"], rec["status"], rec["reject_reason"],
                    rec["created_at"],
                ),
            )
            return cur.rowcount == 1

    def get_record(self, record_id: str) -> Optional[dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM records WHERE record_id=?", (record_id,)
        ).fetchone()
        return dict(row) if row else None

    def find_record_by_key(
        self, device_id: str, sampled_at: str
    ) -> Optional[dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM records WHERE device_id=? AND sampled_at=?",
            (device_id, sampled_at),
        ).fetchone()
        return dict(row) if row else None

    def count_records(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]

    # ---- 事件片段 ----

    def insert_segment(self, seg: dict[str, Any]) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT INTO segments("
                "segment_id, grid_id, complaint_ref, start_time, end_time,"
                " record_count, max_db, trusted_count, clue_count,"
                " penalty_eligible, status, merged_into, created_at, updated_at"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    seg["segment_id"], seg["grid_id"], seg["complaint_ref"],
                    seg["start_time"], seg["end_time"], seg["record_count"],
                    seg["max_db"], seg["trusted_count"], seg["clue_count"],
                    seg["penalty_eligible"], seg["status"], seg["merged_into"],
                    seg["created_at"], seg["updated_at"],
                ),
            )

    def update_segment(self, seg: dict[str, Any]) -> None:
        with self._conn:
            self._conn.execute(
                "UPDATE segments SET start_time=?, end_time=?, record_count=?,"
                " max_db=?, trusted_count=?, clue_count=?, penalty_eligible=?,"
                " status=?, merged_into=?, updated_at=? WHERE segment_id=?",
                (
                    seg["start_time"], seg["end_time"], seg["record_count"],
                    seg["max_db"], seg["trusted_count"], seg["clue_count"],
                    seg["penalty_eligible"], seg["status"], seg["merged_into"],
                    seg["updated_at"], seg["segment_id"],
                ),
            )

    def get_segment(self, segment_id: str) -> Optional[dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM segments WHERE segment_id=?", (segment_id,)
        ).fetchone()
        return dict(row) if row else None

    def list_segments(
        self,
        grid_id: Optional[str] = None,
        complaint_ref: Optional[str] = None,
        status: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        sql, params = "SELECT * FROM segments WHERE 1=1", []
        if grid_id is not None:
            sql += " AND grid_id=?"
            params.append(grid_id)
        if complaint_ref is not None:
            sql += " AND complaint_ref=?"
            params.append(complaint_ref)
        if status is not None:
            sql += " AND status=?"
            params.append(status)
        sql += " ORDER BY start_time"
        rows = self._conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def link_segment_record(self, segment_id: str, record_id: str) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO segment_records(segment_id, record_id) VALUES (?,?)",
                (segment_id, record_id),
            )

    def move_segment_records(self, from_id: str, to_id: str) -> None:
        with self._conn:
            self._conn.execute(
                "UPDATE OR IGNORE segment_records SET segment_id=? WHERE segment_id=?",
                (to_id, from_id),
            )
            self._conn.execute(
                "DELETE FROM segment_records WHERE segment_id=?", (from_id,)
            )

    def segment_record_ids(self, segment_id: str) -> list[str]:
        rows = self._conn.execute(
            "SELECT record_id FROM segment_records WHERE segment_id=?",
            (segment_id,),
        ).fetchall()
        return [r[0] for r in rows]

    # ---- 复核历史(append-only) ----

    def insert_review(self, review: dict[str, Any]) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT INTO reviews(review_id, segment_id, conclusion, basis, operator, created_at)"
                " VALUES (?,?,?,?,?,?)",
                (
                    review["review_id"], review["segment_id"],
                    review["conclusion"], review["basis"],
                    review["operator"], review["created_at"],
                ),
            )

    def list_reviews(self, segment_id: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM reviews WHERE segment_id=? ORDER BY created_at, rowid",
            (segment_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ---- 审计日志(append-only) ----

    def audit(self, action: str, detail: str, operator: Optional[str], now: str) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT INTO audit_log(action, detail, operator, created_at) VALUES (?,?,?,?)",
                (action, detail, operator, now),
            )

    def list_audit(self, limit: int = 200) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM audit_log ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]
