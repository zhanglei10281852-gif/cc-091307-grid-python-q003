"""社区噪声证据核验服务。

职责:
- 接收网格员上传的噪声记录(设备编号、分贝读数、采样时间、位置网格、投诉关联号);
- 校验时钟漂移、异常峰值、重复上传,逐条返回拒绝原因与证据状态;
- 按连续时段规则把通过校验的记录合并为可复核的事件片段,全部落库,
  服务重启后合并结果不丢失;
- 低可信设备数据仅作线索,只含线索的片段不能单独触发处罚建议;
- 人工复核必须写明结论、依据、操作者,复核历史只增不改;
- 原始记录不可覆盖;投诉撤回后仅停止后续关联,历史数据与审计保留。
"""
from __future__ import annotations

import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Optional

from .models import (
    DeviceTrust,
    RecordStatus,
    RejectReason,
    ReviewConclusion,
    SegmentStatus,
    iso,
    parse_instant,
    to_utc,
)
from .storage import Storage

_REQUIRED_FIELDS = ("device_id", "db_reading", "sampled_at", "grid_id", "complaint_ref")


@dataclass
class ImportItemResult:
    """批量导入中单条记录的处理结果。"""

    index: int
    accepted: bool
    record_id: Optional[str] = None
    status: Optional[RecordStatus] = None
    reject_reason: Optional[RejectReason] = None
    segment_id: Optional[str] = None
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "accepted": self.accepted,
            "record_id": self.record_id,
            "status": self.status.value if self.status else None,
            "reject_reason": self.reject_reason.value if self.reject_reason else None,
            "segment_id": self.segment_id,
            "detail": self.detail,
        }


@dataclass
class ReviewResult:
    """人工复核提交结果。"""

    accepted: bool
    review_id: Optional[str] = None
    segment_id: Optional[str] = None
    reject_reason: Optional[str] = None
    detail: str = ""


@dataclass
class WithdrawResult:
    """投诉撤回结果。"""

    accepted: bool
    complaint_ref: str = ""
    already_withdrawn: bool = False
    detail: str = ""


class EvidenceService:
    """证据核验服务入口:批量导入、片段查询、人工复核、投诉撤回。"""

    def __init__(
        self,
        db_path: str,
        *,
        clock: Optional[Callable[[], datetime]] = None,
        max_clock_skew: timedelta = timedelta(minutes=10),
        merge_gap: timedelta = timedelta(minutes=5),
        db_floor: float = 20.0,
        db_ceiling: float = 130.0,
    ):
        """
        :param db_path: SQLite 数据库文件路径,所有状态持久化于此。
        :param clock: 当前时间来源,默认系统 UTC 时间;测试可注入假时钟。
        :param max_clock_skew: 采样时间与接收时间允许的最大偏差,超出判为时钟漂移。
        :param merge_gap: 同一(网格, 投诉)下相邻记录合并为同一片段的最大时间间隔。
        :param db_floor/db_ceiling: 分贝读数的物理合理范围,超出判为异常峰值。
        """
        self._store = Storage(db_path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._max_clock_skew = max_clock_skew
        self._merge_gap = merge_gap
        self._db_floor = db_floor
        self._db_ceiling = db_ceiling

    def close(self) -> None:
        self._store.close()

    # ------------------------------------------------------------------
    # 设备登记
    # ------------------------------------------------------------------

    def register_device(
        self, device_id: str, trust: DeviceTrust | str, operator: Optional[str] = None
    ) -> dict[str, Any]:
        """登记或更新设备可信度。未登记设备在导入时自动按低可信处理。"""
        trust = DeviceTrust(trust)
        now = iso(self._now())
        self._store.upsert_device(device_id, trust.value, now)
        self._store.audit("DEVICE_REGISTERED", f"device={device_id} trust={trust.value}", operator, now)
        return {"device_id": device_id, "trust_level": trust.value}

    # ------------------------------------------------------------------
    # 批量导入
    # ------------------------------------------------------------------

    def import_batch(
        self, records: Iterable[dict[str, Any]], operator: Optional[str] = None
    ) -> list[ImportItemResult]:
        """批量导入噪声记录,逐条校验并返回证据状态或拒绝原因。

        单条失败不影响同批其他记录;通过校验的记录立即参与连续时段合并。
        """
        results: list[ImportItemResult] = []
        for index, raw in enumerate(records):
            results.append(self._import_one(index, raw, operator))
        return results

    def _import_one(
        self, index: int, raw: dict[str, Any], operator: Optional[str]
    ) -> ImportItemResult:
        now = self._now()
        now_iso = iso(now)

        # 1) 载荷完整性:字段缺失或格式非法时无法构造原始记录,仅审计留痕。
        fields, error = self._validate_payload(raw)
        if error is not None:
            self._store.audit("RECORD_REJECTED", f"reason=INVALID_PAYLOAD detail={error}", operator, now_iso)
            return ImportItemResult(
                index=index, accepted=False, status=RecordStatus.REJECTED,
                reject_reason=RejectReason.INVALID_PAYLOAD, detail=error,
            )

        device_id = fields["device_id"]
        sampled_at = fields["sampled_at"]
        sampled_iso = iso(sampled_at)

        # 2) 重复上传:同设备同采样时刻只允许一条留痕,原记录不可覆盖。
        existing = self._store.find_record_by_key(device_id, sampled_iso)
        if existing is not None:
            self._store.audit(
                "RECORD_REJECTED",
                f"reason=DUPLICATE device={device_id} sampled_at={sampled_iso} kept={existing['record_id']}",
                operator, now_iso,
            )
            return ImportItemResult(
                index=index, accepted=False, record_id=existing["record_id"],
                status=RecordStatus.REJECTED, reject_reason=RejectReason.DUPLICATE,
                detail="同设备同采样时刻的记录已存在,原始记录不可覆盖",
            )

        # 3) 时钟漂移:采样时间与接收时间偏差超过阈值(含未来时间)。
        drift = abs(now - sampled_at)
        if drift > self._max_clock_skew:
            return self._reject_and_trace(
                index, fields, now, RejectReason.CLOCK_DRIFT,
                f"采样时间与接收时间偏差 {drift} 超过阈值 {self._max_clock_skew}", operator,
            )

        # 4) 异常峰值:分贝读数超出物理合理范围。
        db_reading = fields["db_reading"]
        if not (self._db_floor <= db_reading <= self._db_ceiling):
            return self._reject_and_trace(
                index, fields, now, RejectReason.ABNORMAL_PEAK,
                f"分贝读数 {db_reading} 超出合理范围 [{self._db_floor}, {self._db_ceiling}]", operator,
            )

        # 5) 投诉已撤回:停止后续关联,记录留痕但不进入片段。
        complaint = self._store.get_complaint(fields["complaint_ref"])
        if complaint is not None and complaint["withdrawn_at"] is not None:
            return self._reject_and_trace(
                index, fields, now, RejectReason.COMPLAINT_WITHDRAWN,
                f"投诉 {fields['complaint_ref']} 已于 {complaint['withdrawn_at']} 撤回,停止关联", operator,
            )

        # 6) 设备可信度:低可信设备数据仅作线索。
        device = self._store.get_device(device_id)
        if device is None:
            self._store.upsert_device(device_id, DeviceTrust.LOW.value, now_iso)
            self._store.audit("DEVICE_REGISTERED", f"device={device_id} trust=LOW (auto)", operator, now_iso)
            trust = DeviceTrust.LOW
        else:
            trust = DeviceTrust(device["trust_level"])
        status = RecordStatus.ACCEPTED if trust is DeviceTrust.TRUSTED else RecordStatus.CLUE

        # 7) 落库并参与连续时段合并。
        record_id = self._new_id("rec")
        self._store.insert_record({
            "record_id": record_id, "device_id": device_id, "db_reading": db_reading,
            "sampled_at": sampled_iso, "received_at": now_iso,
            "grid_id": fields["grid_id"], "complaint_ref": fields["complaint_ref"],
            "status": status.value, "reject_reason": None, "created_at": now_iso,
        })
        segment_id = self._attach_to_segment(
            record_id, fields["grid_id"], fields["complaint_ref"],
            sampled_at, db_reading, status, now_iso, operator,
        )
        self._store.audit(
            "RECORD_IMPORTED",
            f"record={record_id} status={status.value} segment={segment_id}", operator, now_iso,
        )
        return ImportItemResult(
            index=index, accepted=True, record_id=record_id, status=status,
            segment_id=segment_id,
            detail="已接收" if status is RecordStatus.ACCEPTED else "低可信设备数据,仅作线索",
        )

    def _reject_and_trace(
        self, index: int, fields: dict[str, Any], now: datetime,
        reason: RejectReason, detail: str, operator: Optional[str],
    ) -> ImportItemResult:
        """校验失败的记录同样落库留痕(状态 REJECTED),但不参与片段合并。"""
        now_iso = iso(now)
        record_id = self._new_id("rec")
        self._store.insert_record({
            "record_id": record_id, "device_id": fields["device_id"],
            "db_reading": fields["db_reading"], "sampled_at": iso(fields["sampled_at"]),
            "received_at": now_iso, "grid_id": fields["grid_id"],
            "complaint_ref": fields["complaint_ref"], "status": RecordStatus.REJECTED.value,
            "reject_reason": reason.value, "created_at": now_iso,
        })
        self._store.audit(
            "RECORD_REJECTED", f"record={record_id} reason={reason.value}", operator, now_iso,
        )
        return ImportItemResult(
            index=index, accepted=False, record_id=record_id,
            status=RecordStatus.REJECTED, reject_reason=reason, detail=detail,
        )

    def _validate_payload(
        self, raw: dict[str, Any]
    ) -> tuple[dict[str, Any], Optional[str]]:
        if not isinstance(raw, dict):
            return {}, "记录必须是字段字典"
        missing = [k for k in _REQUIRED_FIELDS if raw.get(k) is None]
        if missing:
            return {}, f"缺少必填字段: {', '.join(missing)}"
        fields: dict[str, Any] = {}
        for key in ("device_id", "grid_id", "complaint_ref"):
            value = str(raw[key]).strip()
            if not value:
                return {}, f"字段 {key} 不能为空"
            fields[key] = value
        try:
            db_reading = float(raw["db_reading"])
        except (TypeError, ValueError):
            return {}, "db_reading 必须是数值"
        if not math.isfinite(db_reading):
            return {}, "db_reading 必须是有限数值"
        fields["db_reading"] = db_reading
        try:
            fields["sampled_at"] = parse_instant(raw["sampled_at"])
        except (TypeError, ValueError):
            return {}, "sampled_at 必须是 ISO8601 时间或 datetime"
        return fields, None

    # ------------------------------------------------------------------
    # 连续时段合并
    # ------------------------------------------------------------------

    def _attach_to_segment(
        self, record_id: str, grid_id: str, complaint_ref: str,
        sampled_at: datetime, db_reading: float, status: RecordStatus,
        now_iso: str, operator: Optional[str],
    ) -> str:
        """把记录并入同(网格, 投诉)下时间间隔不超过阈值的 OPEN 片段。

        候选片段从数据库现查,不依赖内存状态,因此重启后合并结果延续;
        新记录若同时桥接多个片段,则把它们合并为一个连续时段。
        """
        candidates = [
            seg for seg in self._store.list_segments(
                grid_id=grid_id, complaint_ref=complaint_ref, status=SegmentStatus.OPEN.value
            )
            if self._distance_to_segment(seg, sampled_at) <= self._merge_gap
        ]
        is_clue = status is RecordStatus.CLUE
        if not candidates:
            segment_id = self._new_id("seg")
            self._store.insert_segment({
                "segment_id": segment_id, "grid_id": grid_id, "complaint_ref": complaint_ref,
                "start_time": iso(sampled_at), "end_time": iso(sampled_at),
                "record_count": 1, "max_db": db_reading,
                "trusted_count": 0 if is_clue else 1, "clue_count": 1 if is_clue else 0,
                "penalty_eligible": 0 if is_clue else 1,
                "status": SegmentStatus.OPEN.value, "merged_into": None,
                "created_at": now_iso, "updated_at": now_iso,
            })
            self._store.link_segment_record(segment_id, record_id)
            self._store.audit("SEGMENT_CREATED", f"segment={segment_id} grid={grid_id}", operator, now_iso)
            return segment_id

        # 主片段取结束时间最新者,其余候选被新记录桥接,合并进来。
        candidates.sort(key=lambda seg: seg["end_time"])
        primary = candidates[-1]
        start = min(parse_instant(primary["start_time"]), sampled_at)
        end = max(parse_instant(primary["end_time"]), sampled_at)
        max_db = max(primary["max_db"], db_reading)
        record_count = primary["record_count"] + 1
        trusted_count = primary["trusted_count"] + (0 if is_clue else 1)
        clue_count = primary["clue_count"] + (1 if is_clue else 0)

        for other in candidates[:-1]:
            self._store.move_segment_records(other["segment_id"], primary["segment_id"])
            start = min(start, parse_instant(other["start_time"]))
            end = max(end, parse_instant(other["end_time"]))
            max_db = max(max_db, other["max_db"])
            record_count += other["record_count"]
            trusted_count += other["trusted_count"]
            clue_count += other["clue_count"]
            other["status"] = SegmentStatus.MERGED.value
            other["merged_into"] = primary["segment_id"]
            other["updated_at"] = now_iso
            self._store.update_segment(other)
            self._store.audit(
                "SEGMENT_MERGED",
                f"segment={other['segment_id']} into={primary['segment_id']}", operator, now_iso,
            )

        primary.update({
            "start_time": iso(start), "end_time": iso(end),
            "record_count": record_count, "max_db": max_db,
            "trusted_count": trusted_count, "clue_count": clue_count,
            "penalty_eligible": 1 if trusted_count > 0 else 0,
            "updated_at": now_iso,
        })
        self._store.update_segment(primary)
        self._store.link_segment_record(primary["segment_id"], record_id)
        return primary["segment_id"]

    def _distance_to_segment(self, segment: dict[str, Any], instant: datetime) -> timedelta:
        """采样时刻到片段时间区间的距离;落在区间内为 0。"""
        start = parse_instant(segment["start_time"])
        end = parse_instant(segment["end_time"])
        return max(start - instant, instant - end, timedelta(0))

    # ------------------------------------------------------------------
    # 片段查询
    # ------------------------------------------------------------------

    def query_segments(
        self,
        grid_id: Optional[str] = None,
        complaint_ref: Optional[str] = None,
        status: Optional[SegmentStatus | str] = None,
    ) -> list[dict[str, Any]]:
        """按网格/投诉/状态查询事件片段,返回可复核的连续时段视图。"""
        status_value = SegmentStatus(status).value if status is not None else None
        return [self._segment_view(seg) for seg in self._store.list_segments(
            grid_id=grid_id, complaint_ref=complaint_ref, status=status_value
        )]

    def get_segment(self, segment_id: str) -> Optional[dict[str, Any]]:
        """片段详情:含组成记录清单与全部复核历史,供人工复核使用。"""
        seg = self._store.get_segment(segment_id)
        if seg is None:
            return None
        view = self._segment_view(seg)
        view["record_ids"] = self._store.segment_record_ids(segment_id)
        view["reviews"] = self._store.list_reviews(segment_id)
        return view

    def get_record(self, record_id: str) -> Optional[dict[str, Any]]:
        """原始记录查询,返回证据状态与拒绝原因。"""
        return self._store.get_record(record_id)

    def list_audit(self, limit: int = 200) -> list[dict[str, Any]]:
        """审计日志查询(最新的在前)。"""
        return self._store.list_audit(limit)

    def _segment_view(self, seg: dict[str, Any]) -> dict[str, Any]:
        return {
            "segment_id": seg["segment_id"],
            "grid_id": seg["grid_id"],
            "complaint_ref": seg["complaint_ref"],
            "start_time": seg["start_time"],
            "end_time": seg["end_time"],
            "record_count": seg["record_count"],
            "max_db": seg["max_db"],
            "trusted_count": seg["trusted_count"],
            "clue_count": seg["clue_count"],
            # 只含低可信设备线索的片段不具备处罚资格,不能单独触发处罚建议。
            "penalty_eligible": bool(seg["penalty_eligible"]),
            "status": seg["status"],
            "merged_into": seg["merged_into"],
        }

    # ------------------------------------------------------------------
    # 人工复核
    # ------------------------------------------------------------------

    def review_segment(
        self,
        segment_id: str,
        conclusion: ReviewConclusion | str,
        basis: str,
        operator: str,
    ) -> ReviewResult:
        """提交人工复核:结论、依据、操作者缺一不可,复核历史只增不改。"""
        now_iso = iso(self._now())
        segment = self._store.get_segment(segment_id)
        if segment is None or segment["status"] == SegmentStatus.MERGED.value:
            return ReviewResult(False, segment_id=segment_id,
                                reject_reason="SEGMENT_NOT_FOUND", detail="片段不存在或已被合并")
        try:
            conclusion = ReviewConclusion(conclusion)
        except ValueError:
            return ReviewResult(False, segment_id=segment_id,
                                reject_reason="INVALID_CONCLUSION", detail="复核结论非法")
        if not basis or not str(basis).strip():
            return ReviewResult(False, segment_id=segment_id,
                                reject_reason="MISSING_BASIS", detail="复核必须写明依据")
        if not operator or not str(operator).strip():
            return ReviewResult(False, segment_id=segment_id,
                                reject_reason="MISSING_OPERATOR", detail="复核必须写明操作者")
        if conclusion is ReviewConclusion.PENALTY_SUGGESTED and not segment["penalty_eligible"]:
            return ReviewResult(
                False, segment_id=segment_id, reject_reason="NOT_PENALTY_ELIGIBLE",
                detail="片段仅含低可信设备线索,不能单独触发处罚建议",
            )

        review_id = self._new_id("rev")
        self._store.insert_review({
            "review_id": review_id, "segment_id": segment_id,
            "conclusion": conclusion.value, "basis": str(basis).strip(),
            "operator": str(operator).strip(), "created_at": now_iso,
        })
        segment["status"] = SegmentStatus.REVIEWED.value
        segment["updated_at"] = now_iso
        self._store.update_segment(segment)
        self._store.audit(
            "REVIEW_SUBMITTED",
            f"review={review_id} segment={segment_id} conclusion={conclusion.value}",
            operator, now_iso,
        )
        return ReviewResult(True, review_id=review_id, segment_id=segment_id, detail="复核已记录")

    # ------------------------------------------------------------------
    # 投诉撤回
    # ------------------------------------------------------------------

    def withdraw_complaint(
        self, complaint_ref: str, operator: str, reason: str = ""
    ) -> WithdrawResult:
        """撤回投诉:仅停止后续关联,历史记录、片段与审计全部保留。"""
        now_iso = iso(self._now())
        existing = self._store.get_complaint(complaint_ref)
        if existing is not None and existing["withdrawn_at"] is not None:
            return WithdrawResult(
                False, complaint_ref=complaint_ref, already_withdrawn=True,
                detail=f"投诉已于 {existing['withdrawn_at']} 撤回,不重复处理",
            )
        self._store.mark_complaint_withdrawn(complaint_ref, now_iso, reason, operator)
        self._store.audit(
            "COMPLAINT_WITHDRAWN", f"complaint={complaint_ref} reason={reason}", operator, now_iso,
        )
        return WithdrawResult(True, complaint_ref=complaint_ref,
                              detail="投诉已撤回,后续记录停止关联,历史数据保留")

    # ------------------------------------------------------------------

    def _now(self) -> datetime:
        return to_utc(self._clock())

    @staticmethod
    def _new_id(prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:16]}"
