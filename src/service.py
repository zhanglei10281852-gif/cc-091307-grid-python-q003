"""社区噪声证据核验服务。

领域模型与规则：

* 原始读数（records）只追加、永不更新或删除（数据库触发器兜底），
  被拒绝的上传同样留档，便于审计。
* 入库校验：必填字段、时间格式、分贝物理量程、设备时钟漂移、
  异常峰值（相对同设备历史中位数的稳健跳变）、重复上传。
* 设备有可信等级（high/low）。低可信设备（或被标记异常峰值）
  的读数只能作为线索（lead），不能单独支撑处罚建议。
* 同一位置网格 + 同一投诉关联号下，采样间隔不超过间隔阈值的
  连续读数合并为一个事件片段（segment）。片段结论：
  enforceable / lead_only / insufficient / below_threshold。
* 投诉撤回后，仅停止后续读数与该投诉的关联；历史读数、片段、
  复核意见全部保留。
* 片段为派生数据，使用确定性编号（网格+投诉+首条记录号），
  每次入库及服务启动时做幂等对账，重启后合并结果不丢失；
  被并入更大片段的旧片段以 superseded_by 留痕。
"""

from __future__ import annotations

import json
import os
import re
import statistics
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

# ---------------------------------------------------------------------------
# 常量与工具
# ---------------------------------------------------------------------------

TRUST_HIGH = "high"
TRUST_LOW = "low"
VALID_TRUST = {TRUST_HIGH, TRUST_LOW}

# 入库状态
ST_ACCEPTED = "accepted"            # 接受且参与片段合并
ST_UNLINKED = "accepted_unlinked"   # 接受但投诉已撤回，停止关联
ST_REJECTED = "rejected"            # 校验拒绝（仍留档）

# 片段证据状态
EV_ENFORCEABLE = "enforceable"      # 可提出处罚建议
EV_LEAD_ONLY = "lead_only"          # 仅有低可信线索
EV_INSUFFICIENT = "insufficient"    # 有高可信数据但时长/条数不足
EV_BELOW = "below_threshold"        # 未超过噪声限值


class ServiceError(Exception):
    """业务错误基类。"""

    def __init__(self, code: str, message: str, http_status: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status


class NotFound(ServiceError):
    def __init__(self, code: str, message: str, active_id: str | None = None):
        super().__init__(code, message, 404)
        self.active_id = active_id


class Conflict(ServiceError):
    def __init__(self, code: str, message: str, active_id: str | None = None):
        super().__init__(code, message, 409)
        self.active_id = active_id


def parse_ts(value: Any) -> datetime:
    """解析 ISO8601 时间；朴素时间按 UTC 处理，统一返回带时区 UTC。"""
    if isinstance(value, datetime):
        dt = value
    else:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("empty timestamp")
        text = value.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)  # 可能抛 ValueError
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def ts_str(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _slug(text: str | None) -> str:
    if not text:
        return "NA"
    return re.sub(r"[^A-Za-z0-9]+", "-", text).strip("-")[:24] or "NA"


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------


@dataclass
class Config:
    db_path: str = "evidence.db"
    # 夜间娱乐噪声限值（分贝），超过即视为疑似扰民
    noise_limit_db: float = 55.0
    # 触发处罚建议所需的高可信读数条数与最短持续时长
    min_high_trust_samples: int = 3
    min_event_span_seconds: float = 120.0
    # 片段合并：相邻读数最大允许间隔
    gap_tolerance_seconds: float = 600.0
    # 设备时钟与服务端时间最大偏差
    drift_tolerance_seconds: float = 900.0
    # 异常峰值：相对同设备近期中位数的向上跳变量
    spike_delta_db: float = 20.0
    spike_window: int = 20
    # 分贝物理量程，超出直接拒绝
    db_min: float = 0.0
    db_max: float = 140.0


# ---------------------------------------------------------------------------
# 核心服务
# ---------------------------------------------------------------------------


class EvidenceService:
    def __init__(self, config: Config | None = None):
        self.config = config or Config()
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.config.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()
        # 启动即对账：保证重启后片段合并结果一致、不丢失
        self._reconcile_segments()

    # ---- 建表 -------------------------------------------------------------

    def _init_schema(self) -> None:
        c = self._conn
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS devices (
                device_id     TEXT PRIMARY KEY,
                trust_level   TEXT NOT NULL DEFAULT 'low',
                note          TEXT NOT NULL DEFAULT '',
                registered_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS complaints (
                complaint_ref TEXT PRIMARY KEY,
                status        TEXT NOT NULL DEFAULT 'active',
                created_at    TEXT NOT NULL,
                withdrawn_at  TEXT,
                withdraw_note TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS records (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id        TEXT,
                db_reading       REAL,
                sampled_at       TEXT,
                received_at      TEXT,
                grid             TEXT,
                complaint_ref    TEXT,
                raw_json         TEXT NOT NULL,
                ingest_status    TEXT NOT NULL,
                linked           INTEGER NOT NULL DEFAULT 1,
                flags            TEXT NOT NULL DEFAULT '[]',
                rejection_reasons TEXT NOT NULL DEFAULT '[]',
                trust_level      TEXT,
                evidence_status  TEXT,
                created_at       TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_records_group
                ON records(grid, complaint_ref, sampled_at);
            CREATE INDEX IF NOT EXISTS idx_records_device
                ON records(device_id, sampled_at);

            CREATE TABLE IF NOT EXISTS segments (
                segment_id          TEXT PRIMARY KEY,
                grid                TEXT NOT NULL,
                complaint_ref       TEXT,
                start_at            TEXT NOT NULL,
                end_at              TEXT NOT NULL,
                sample_count        INTEGER NOT NULL,
                device_count        INTEGER NOT NULL,
                high_trust_count    INTEGER NOT NULL,
                peak_db             REAL NOT NULL,
                median_db           REAL NOT NULL,
                evidence_status     TEXT NOT NULL,
                penalty_recommendation INTEGER NOT NULL DEFAULT 0,
                superseded_by       TEXT,
                updated_at          TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS segment_records (
                segment_id TEXT NOT NULL,
                record_id  INTEGER NOT NULL,
                seq        INTEGER NOT NULL,
                PRIMARY KEY (record_id)
            );
            CREATE INDEX IF NOT EXISTS idx_segrec_seg ON segment_records(segment_id);

            CREATE TABLE IF NOT EXISTS reviews (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                segment_id  TEXT NOT NULL,
                conclusion  TEXT NOT NULL,
                basis       TEXT NOT NULL,
                operator    TEXT NOT NULL,
                created_at  TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_reviews_seg ON reviews(segment_id);
            """
        )
        # 原始记录与复核意见只追加，触发器层面禁止覆盖/删除
        c.executescript(
            """
            CREATE TRIGGER IF NOT EXISTS trg_records_no_update
            BEFORE UPDATE ON records
            BEGIN
                SELECT RAISE(ABORT, 'records are append-only and immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_records_no_delete
            BEFORE DELETE ON records
            BEGIN
                SELECT RAISE(ABORT, 'records are append-only and immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_reviews_no_update
            BEFORE UPDATE ON reviews
            BEGIN
                SELECT RAISE(ABORT, 'reviews are append-only and immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_reviews_no_delete
            BEFORE DELETE ON reviews
            BEGIN
                SELECT RAISE(ABORT, 'reviews are append-only and immutable');
            END;
            """
        )
        c.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> "EvidenceService":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ---- 设备与投诉 -------------------------------------------------------

    def register_device(
        self, device_id: str, trust_level: str = TRUST_LOW, note: str = ""
    ) -> dict:
        if trust_level not in VALID_TRUST:
            raise ServiceError("bad_trust", f"trust_level 必须为 {VALID_TRUST}")
        if not isinstance(device_id, str) or not device_id.strip():
            raise ServiceError("bad_device", "device_id 不能为空")
        device_id = device_id.strip()
        with self._lock:
            self._conn.execute(
                """INSERT INTO devices(device_id, trust_level, note, registered_at)
                   VALUES(?,?,?,?)
                   ON CONFLICT(device_id) DO UPDATE SET
                     trust_level=excluded.trust_level, note=excluded.note""",
                (device_id, trust_level, note, ts_str(now_utc())),
            )
            self._conn.commit()
        return {"device_id": device_id, "trust_level": trust_level}

    def _device_trust(self, device_id: str) -> str:
        row = self._conn.execute(
            "SELECT trust_level FROM devices WHERE device_id=?", (device_id,)
        ).fetchone()
        return row["trust_level"] if row else TRUST_LOW

    def withdraw_complaint(
        self, complaint_ref: str, note: str = "", when: datetime | None = None
    ) -> dict:
        """撤回投诉：停止后续关联，历史证据与审计全部保留。"""
        if not isinstance(complaint_ref, str) or not complaint_ref.strip():
            raise ServiceError("bad_complaint", "complaint_ref 不能为空")
        complaint_ref = complaint_ref.strip()
        ts = ts_str(when or now_utc())
        with self._lock:
            self._conn.execute(
                """INSERT INTO complaints(complaint_ref, status, created_at,
                                          withdrawn_at, withdraw_note)
                   VALUES(?, 'active', ?, NULL, '')
                   ON CONFLICT(complaint_ref) DO NOTHING""",
                (complaint_ref, ts),
            )
            row = self._conn.execute(
                "SELECT status FROM complaints WHERE complaint_ref=?",
                (complaint_ref,),
            ).fetchone()
            already = row["status"] == "withdrawn"
            if not already:
                self._conn.execute(
                    """UPDATE complaints
                       SET status='withdrawn', withdrawn_at=?, withdraw_note=?
                       WHERE complaint_ref=?""",
                    (ts, note, complaint_ref),
                )
            self._conn.commit()
        return {
            "complaint_ref": complaint_ref,
            "status": "withdrawn",
            "withdrawn_at": ts,
            "idempotent": already,
        }

    def _complaint_is_withdrawn(self, complaint_ref: str | None) -> bool:
        if not complaint_ref:
            return False
        row = self._conn.execute(
            "SELECT status FROM complaints WHERE complaint_ref=?", (complaint_ref,)
        ).fetchone()
        return bool(row and row["status"] == "withdrawn")

    # ---- 批量导入 ---------------------------------------------------------

    def ingest_batch(
        self, rows: Iterable[dict], received_at: datetime | None = None
    ) -> dict:
        """批量导入读数。逐条返回状态、拒绝原因与证据状态。

        接受状态：accepted / accepted_unlinked / rejected。
        """
        if not isinstance(rows, list):
            try:
                rows = list(rows)
            except TypeError:
                raise ServiceError("bad_payload", "records 必须为数组")
        server_now = received_at or now_utc()
        results: list[dict] = []
        with self._lock:
            try:
                seen_in_batch: set[tuple[str, str]] = set()
                for index, raw in enumerate(rows):
                    if not isinstance(raw, dict):
                        results.append(
                            {
                                "index": index,
                                "status": ST_REJECTED,
                                "reasons": ["not_an_object"],
                                "flags": [],
                            }
                        )
                        continue
                    results.append(
                        self._ingest_one(raw, server_now, seen_in_batch)
                    )
                self._ensure_complaints(rows)
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
            # 派生片段对账（同锁内，内部独立事务）
            self._reconcile_segments()

            # 回填片段编号（锁内读取，避免并发入库造成归属漂移）
            for r in results:
                if r.get("record_id"):
                    seg = self._conn.execute(
                        "SELECT segment_id FROM segment_records WHERE record_id=?",
                        (r["record_id"],),
                    ).fetchone()
                    if seg:
                        r["segment_id"] = seg["segment_id"]

        counts = {ST_ACCEPTED: 0, ST_UNLINKED: 0, ST_REJECTED: 0}
        for r in results:
            counts[r["status"]] = counts.get(r["status"], 0) + 1
        return {
            "received_at": ts_str(server_now),
            "total": len(results),
            "accepted": counts[ST_ACCEPTED],
            "accepted_unlinked": counts[ST_UNLINKED],
            "rejected": counts[ST_REJECTED],
            "results": results,
        }

    def _ensure_complaints(self, rows: list[dict]) -> None:
        refs = {
            str(r.get("complaint_ref")).strip()
            for r in rows
            if isinstance(r, dict)
            and isinstance(r.get("complaint_ref"), str)
            and r.get("complaint_ref", "").strip()
        }
        for ref in refs:
            self._conn.execute(
                """INSERT INTO complaints(complaint_ref, status, created_at)
                   VALUES(?, 'active', ?)
                   ON CONFLICT(complaint_ref) DO NOTHING""",
                (ref, ts_str(now_utc())),
            )

    def _ingest_one(
        self, raw: dict, server_now: datetime, seen_in_batch: set[tuple[str, str]]
    ) -> dict:
        cfg = self.config
        reasons: list[str] = []
        flags: list[str] = []

        device_id = raw.get("device_id")
        grid = raw.get("grid")
        complaint_ref = raw.get("complaint_ref")
        if not isinstance(device_id, str) or not device_id.strip():
            reasons.append("missing_device_id")
            device_id = None
        else:
            device_id = device_id.strip()
        if not isinstance(grid, str) or not grid.strip():
            reasons.append("missing_grid")
            grid = None
        else:
            grid = grid.strip()
        if complaint_ref is not None and (
            not isinstance(complaint_ref, str) or not complaint_ref.strip()
        ):
            reasons.append("bad_complaint_ref")
            complaint_ref = None
        elif isinstance(complaint_ref, str):
            complaint_ref = complaint_ref.strip()

        # 采样时间
        sampled_at: datetime | None = None
        sampled_text = raw.get("sampled_at")
        if sampled_text is None or sampled_text == "":
            reasons.append("missing_sampled_at")
        else:
            try:
                sampled_at = parse_ts(sampled_text)
            except (ValueError, TypeError):
                reasons.append("bad_timestamp")

        # 分贝读数
        db_value = raw.get("db")
        if db_value is None:
            reasons.append("missing_db")
            db_num = None
        else:
            try:
                db_num = float(db_value)
            except (TypeError, ValueError):
                reasons.append("bad_db")
                db_num = None
            else:
                if db_num != db_num or db_num < cfg.db_min or db_num > cfg.db_max:
                    reasons.append("db_out_of_range")

        # 时钟漂移
        if sampled_at is not None:
            drift = abs((sampled_at - server_now).total_seconds())
            if drift > cfg.drift_tolerance_seconds:
                reasons.append("clock_drift")

        # 重复上传（同设备同采样时刻，库内或批次内）
        duplicate = False
        if device_id and sampled_at is not None:
            key = (device_id, ts_str(sampled_at))
            if key in seen_in_batch:
                duplicate = True
                reasons.append("duplicate_upload")
            else:
                hit = self._conn.execute(
                    """SELECT id FROM records
                       WHERE device_id=? AND sampled_at=?
                         AND ingest_status IN (?, ?)
                       LIMIT 1""",
                    (device_id, key[1], ST_ACCEPTED, ST_UNLINKED),
                ).fetchone()
                if hit:
                    duplicate = True
                    reasons.append("duplicate_upload")
                seen_in_batch.add(key)

        # 硬校验失败：原样留档为 rejected，不参与合并
        if reasons:
            rid = self._insert_record(
                device_id, db_num, sampled_at, server_now, grid, complaint_ref,
                raw, ST_REJECTED, linked=0, flags=flags, reasons=reasons,
                trust=None, evidence=None,
            )
            return {
                "status": ST_REJECTED,
                "record_id": rid,
                "reasons": reasons,
                "flags": flags,
                "evidence_status": None,
            }

        # 投诉已撤回：读数接受、留档，但停止后续关联
        linked = 1
        status = ST_ACCEPTED
        if self._complaint_is_withdrawn(complaint_ref):
            linked = 0
            status = ST_UNLINKED
            flags.append("complaint_withdrawn_unlinked")

        trust = self._device_trust(device_id)
        if trust == TRUST_LOW:
            flags.append("low_trust_device")

        # 异常峰值：相对同设备近期已接受读数中位数的稳健跳变
        if self._is_spike(device_id, db_num):
            flags.append("anomalous_peak")

        # 高可信设备的异常读数降级为线索
        evidence = "evidence" if trust == TRUST_HIGH and "anomalous_peak" not in flags else "lead"

        rid = self._insert_record(
            device_id, db_num, sampled_at, server_now, grid, complaint_ref,
            raw, status, linked=linked, flags=flags, reasons=[],
            trust=trust, evidence=evidence,
        )
        return {
            "status": status,
            "record_id": rid,
            "reasons": [],
            "flags": flags,
            "evidence_status": evidence,
        }

    def _is_spike(self, device_id: str, db_num: float) -> bool:
        cfg = self.config
        rows = self._conn.execute(
            """SELECT db_reading FROM records
               WHERE device_id=? AND ingest_status=?
               ORDER BY sampled_at DESC, id DESC LIMIT ?""",
            (device_id, ST_ACCEPTED, cfg.spike_window),
        ).fetchall()
        if len(rows) < 3:
            return False
        baseline = statistics.median(r["db_reading"] for r in rows)
        return db_num - baseline > cfg.spike_delta_db

    def _insert_record(
        self, device_id, db_num, sampled_at, server_now, grid, complaint_ref,
        raw, status, linked, flags, reasons, trust, evidence,
    ) -> int:
        cur = self._conn.execute(
            """INSERT INTO records(device_id, db_reading, sampled_at, received_at,
                  grid, complaint_ref, raw_json, ingest_status, linked, flags,
                  rejection_reasons, trust_level, evidence_status, created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                device_id,
                db_num,
                ts_str(sampled_at) if sampled_at else None,
                ts_str(server_now),
                grid,
                complaint_ref,
                json.dumps(raw, ensure_ascii=False, sort_keys=True),
                status,
                1 if linked else 0,
                json.dumps(flags, ensure_ascii=False),
                json.dumps(reasons, ensure_ascii=False),
                trust,
                evidence,
                ts_str(now_utc()),
            ),
        )
        return int(cur.lastrowid)

    # ---- 片段合并（确定性、幂等、可重启恢复）------------------------------

    def _reconcile_segments(self) -> None:
        cfg = self.config
        with self._lock:
            try:
                # 1. 收集既有归属（用于 superseded 留痕）
                old_membership: dict[str, set[int]] = {}
                for row in self._conn.execute(
                    "SELECT segment_id, record_id FROM segment_records"
                ):
                    old_membership.setdefault(row["segment_id"], set()).add(
                        row["record_id"]
                    )

                # 2. 按规则重新分组（records 不可变，结果可完整复现）
                groups: dict[str, list[sqlite3.Row]] = {}
                rows = self._conn.execute(
                    """SELECT id, device_id, db_reading, sampled_at, grid,
                              complaint_ref, trust_level, evidence_status
                       FROM records
                       WHERE ingest_status=? AND linked=1
                       ORDER BY grid, complaint_ref, sampled_at, id""",
                    (ST_ACCEPTED,),
                ).fetchall()

                current: list[sqlite3.Row] = []
                last_key: tuple | None = None
                last_ts: datetime | None = None

                def flush(buf: list[sqlite3.Row]) -> None:
                    if not buf:
                        return
                    first = min(buf, key=lambda r: (r["sampled_at"], r["id"]))
                    seg_id = "SEG-{}-{}-R{:08d}".format(
                        _slug(first["grid"]),
                        _slug(first["complaint_ref"]),
                        first["id"],
                    )
                    groups[seg_id] = buf

                for row in rows:
                    key = (row["grid"], row["complaint_ref"])
                    ts = parse_ts(row["sampled_at"])
                    if (
                        last_key is not None
                        and key == last_key
                        and last_ts is not None
                        and (ts - last_ts).total_seconds()
                        <= cfg.gap_tolerance_seconds
                    ):
                        current.append(row)
                    else:
                        flush(current)
                        current = [row]
                    last_key = key
                    last_ts = ts
                flush(current)

                # 3. 计算片段属性并 upsert
                record_to_seg: dict[int, str] = {}
                stamp = ts_str(now_utc())
                for seg_id, buf in groups.items():
                    for r in buf:
                        record_to_seg[r["id"]] = seg_id
                    dbs = [r["db_reading"] for r in buf]
                    devices = {r["device_id"] for r in buf}
                    high = sum(
                        1 for r in buf if r["evidence_status"] == "evidence"
                    )
                    start = min(parse_ts(r["sampled_at"]) for r in buf)
                    end = max(parse_ts(r["sampled_at"]) for r in buf)
                    span = (end - start).total_seconds()
                    median_db = statistics.median(dbs)
                    peak = max(dbs)
                    exceeded = median_db >= cfg.noise_limit_db
                    if (
                        exceeded
                        and high >= cfg.min_high_trust_samples
                        and span >= cfg.min_event_span_seconds
                    ):
                        status = EV_ENFORCEABLE
                    elif exceeded and high == 0:
                        status = EV_LEAD_ONLY
                    elif exceeded:
                        status = EV_INSUFFICIENT
                    else:
                        status = EV_BELOW
                    self._conn.execute(
                        """INSERT INTO segments(segment_id, grid, complaint_ref,
                              start_at, end_at, sample_count, device_count,
                              high_trust_count, peak_db, median_db,
                              evidence_status, penalty_recommendation,
                              superseded_by, updated_at)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,NULL,?)
                           ON CONFLICT(segment_id) DO UPDATE SET
                              start_at=excluded.start_at,
                              end_at=excluded.end_at,
                              sample_count=excluded.sample_count,
                              device_count=excluded.device_count,
                              high_trust_count=excluded.high_trust_count,
                              peak_db=excluded.peak_db,
                              median_db=excluded.median_db,
                              evidence_status=excluded.evidence_status,
                              penalty_recommendation=excluded.penalty_recommendation,
                              superseded_by=NULL,
                              updated_at=excluded.updated_at""",
                        (
                            seg_id,
                            buf[0]["grid"],
                            buf[0]["complaint_ref"],
                            ts_str(start),
                            ts_str(end),
                            len(buf),
                            len(devices),
                            high,
                            peak,
                            median_db,
                            status,
                            1 if status == EV_ENFORCEABLE else 0,
                            stamp,
                        ),
                    )

                # 4. 已消失的旧片段（被桥接连入更大片段）→ superseded 留痕
                for old_id, members in old_membership.items():
                    if old_id in groups:
                        continue
                    target = next(
                        (record_to_seg[m] for m in members if m in record_to_seg),
                        None,
                    )
                    self._conn.execute(
                        "UPDATE segments SET superseded_by=?, updated_at=? "
                        "WHERE segment_id=?",
                        (target, stamp, old_id),
                    )

                # 5. 重建派生归属
                self._conn.execute("DELETE FROM segment_records")
                for seg_id, buf in groups.items():
                    ordered = sorted(buf, key=lambda r: (parse_ts(r["sampled_at"]), r["id"]))
                    for seq, r in enumerate(ordered):
                        self._conn.execute(
                            "INSERT INTO segment_records(segment_id, record_id, seq)"
                            " VALUES(?,?,?)",
                            (seg_id, r["id"], seq),
                        )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    # ---- 查询 -------------------------------------------------------------

    @staticmethod
    def _segment_dict(row: sqlite3.Row) -> dict:
        return {
            "segment_id": row["segment_id"],
            "grid": row["grid"],
            "complaint_ref": row["complaint_ref"],
            "start_at": row["start_at"],
            "end_at": row["end_at"],
            "sample_count": row["sample_count"],
            "device_count": row["device_count"],
            "high_trust_count": row["high_trust_count"],
            "peak_db": row["peak_db"],
            "median_db": row["median_db"],
            "evidence_status": row["evidence_status"],
            "penalty_recommendation": bool(row["penalty_recommendation"]),
            "active": row["superseded_by"] is None,
            "superseded_by": row["superseded_by"],
            "updated_at": row["updated_at"],
        }

    def list_segments(
        self,
        grid: str | None = None,
        complaint_ref: str | None = None,
        status: str | None = None,
        start: datetime | str | None = None,
        end: datetime | str | None = None,
        include_superseded: bool = False,
    ) -> dict:
        where = []
        params: list[Any] = []
        if not include_superseded:
            where.append("superseded_by IS NULL")
        if grid:
            where.append("grid=?")
            params.append(grid.strip())
        if complaint_ref:
            where.append("complaint_ref=?")
            params.append(complaint_ref.strip())
        if status:
            where.append("evidence_status=?")
            params.append(status)
        if start:
            where.append("end_at >= ?")
            params.append(ts_str(parse_ts(start)))
        if end:
            where.append("start_at <= ?")
            params.append(ts_str(parse_ts(end)))
        sql = "SELECT * FROM segments"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY start_at"
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return {"segments": [self._segment_dict(r) for r in rows]}

    def _active_chain(self, segment_id: str) -> list[str]:
        """返回该片段（可能已被合并）到当前有效片段的 supersede 链。"""
        chain = [segment_id]
        seen = {segment_id}
        current = segment_id
        with self._lock:
            while True:
                row = self._conn.execute(
                    "SELECT superseded_by FROM segments WHERE segment_id=?",
                    (current,),
                ).fetchone()
                if not row or not row["superseded_by"]:
                    break
                current = row["superseded_by"]
                if current in seen:
                    break
                chain.append(current)
                seen.add(current)
        return chain

    def _review_scope(self, segment_id: str) -> list[str]:
        """汇总复核的范围：片段本身 + 所有（传递地）并入它的旧片段。"""
        with self._lock:
            edges = self._conn.execute(
                "SELECT segment_id, superseded_by FROM segments "
                "WHERE superseded_by IS NOT NULL"
            ).fetchall()
        parents: dict[str, list[str]] = {}
        for e in edges:
            parents.setdefault(e["superseded_by"], []).append(e["segment_id"])
        scope = [segment_id]
        stack = [segment_id]
        seen = {segment_id}
        while stack:
            for child in parents.get(stack.pop(), []):
                if child not in seen:
                    seen.add(child)
                    scope.append(child)
                    stack.append(child)
        return scope

    def get_segment(self, segment_id: str) -> dict:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM segments WHERE segment_id=?", (segment_id,)
            ).fetchone()
            if not row:
                raise NotFound("segment_not_found", f"片段 {segment_id} 不存在")
            seg = self._segment_dict(row)
            records = self._conn.execute(
                """SELECT r.id, r.device_id, r.db_reading, r.sampled_at,
                          r.grid, r.complaint_ref, r.trust_level,
                          r.evidence_status, r.flags, sr.seq
                   FROM segment_records sr JOIN records r ON r.id=sr.record_id
                   WHERE sr.segment_id=? ORDER BY sr.seq""",
                (segment_id,),
            ).fetchall()
            # 复核意见沿合并链（含被并入的旧片段）汇总：审计不丢
            chain = self._active_chain(segment_id)
            root = chain[-1]
            scope = self._review_scope(root)
            reviews = self._conn.execute(
                f"SELECT * FROM reviews WHERE segment_id IN "
                f"({','.join('?' for _ in scope)}) ORDER BY created_at, id",
                scope,
            ).fetchall()
        seg["records"] = [
            {
                "record_id": r["id"],
                "device_id": r["device_id"],
                "db": r["db_reading"],
                "sampled_at": r["sampled_at"],
                "grid": r["grid"],
                "complaint_ref": r["complaint_ref"],
                "trust_level": r["trust_level"],
                "evidence_status": r["evidence_status"],
                "flags": json.loads(r["flags"]),
                "seq": r["seq"],
            }
            for r in records
        ]
        seg["reviews"] = [self._review_dict(r) for r in reviews]
        seg["merge_chain"] = chain
        return seg

    def get_record(self, record_id: int) -> dict:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM records WHERE id=?", (record_id,)
            ).fetchone()
        if not row:
            raise NotFound("record_not_found", f"记录 {record_id} 不存在")
        return {
            "record_id": row["id"],
            "device_id": row["device_id"],
            "db": row["db_reading"],
            "sampled_at": row["sampled_at"],
            "received_at": row["received_at"],
            "grid": row["grid"],
            "complaint_ref": row["complaint_ref"],
            "ingest_status": row["ingest_status"],
            "linked": bool(row["linked"]),
            "flags": json.loads(row["flags"]),
            "rejection_reasons": json.loads(row["rejection_reasons"]),
            "trust_level": row["trust_level"],
            "evidence_status": row["evidence_status"],
            "raw": json.loads(row["raw_json"]),
            "created_at": row["created_at"],
        }

    # ---- 人工复核 ---------------------------------------------------------

    @staticmethod
    def _review_dict(row: sqlite3.Row) -> dict:
        return {
            "review_id": row["id"],
            "segment_id": row["segment_id"],
            "conclusion": row["conclusion"],
            "basis": row["basis"],
            "operator": row["operator"],
            "created_at": row["created_at"],
        }

    def add_review(
        self, segment_id: str, conclusion: str, basis: str, operator: str
    ) -> dict:
        conclusion = (conclusion or "").strip() if isinstance(conclusion, str) else ""
        basis = (basis or "").strip() if isinstance(basis, str) else ""
        operator = (operator or "").strip() if isinstance(operator, str) else ""
        missing = [
            name
            for name, val in (
                ("conclusion", conclusion),
                ("basis", basis),
                ("operator", operator),
            )
            if not val
        ]
        if missing:
            raise ServiceError(
                "missing_review_fields",
                "复核必须写明结论、依据和操作者，缺失：" + ",".join(missing),
            )
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM segments WHERE segment_id=?", (segment_id,)
            ).fetchone()
            if not row:
                raise NotFound(
                    "segment_not_found", f"片段 {segment_id} 不存在"
                )
            if row["superseded_by"]:
                raise Conflict(
                    "segment_superseded",
                    f"片段 {segment_id} 已并入 {row['superseded_by']}，"
                    "请对当前有效片段复核",
                    active_id=row["superseded_by"],
                )
            stamp = ts_str(now_utc())
            cur = self._conn.execute(
                """INSERT INTO reviews(segment_id, conclusion, basis, operator,
                                       created_at)
                   VALUES(?,?,?,?,?)""",
                (segment_id, conclusion, basis, operator, stamp),
            )
            self._conn.commit()
            review_id = int(cur.lastrowid)
            full = self._conn.execute(
                "SELECT * FROM reviews WHERE id=?", (review_id,)
            ).fetchone()
            return self._review_dict(full)

    def list_reviews(self, segment_id: str | None = None) -> dict:
        with self._lock:
            if segment_id:
                chain = self._active_chain(segment_id)
                scope = self._review_scope(chain[-1])
                rows = self._conn.execute(
                    f"SELECT * FROM reviews WHERE segment_id IN "
                    f"({','.join('?' for _ in scope)}) ORDER BY created_at, id",
                    scope,
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM reviews ORDER BY created_at, id"
                ).fetchall()
        return {"reviews": [self._review_dict(r) for r in rows]}


# ---------------------------------------------------------------------------
# 可选：HTTP 入口（标准库，无第三方依赖）
# ---------------------------------------------------------------------------


def create_app(config: Config | None = None):  # pragma: no cover - 薄封装
    from .httpapi import build_server

    return build_server(config or Config())


def main(argv: list[str] | None = None) -> int:  # pragma: no cover
    import argparse
    from http.server import ThreadingHTTPServer
    from .httpapi import make_handler

    parser = argparse.ArgumentParser(description="社区噪声证据核验服务")
    parser.add_argument("--db", default=os.environ.get("EVIDENCE_DB", "evidence.db"))
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args(argv)

    service = EvidenceService(Config(db_path=args.db))
    httpd = ThreadingHTTPServer((args.host, args.port), make_handler(service))
    print(f"证据核验服务监听 http://{args.host}:{args.port} (db={args.db})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        service.close()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
