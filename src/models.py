"""社区噪声证据核验的领域模型与枚举定义。"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum


class DeviceTrust(str, Enum):
    """采集设备可信度。"""

    TRUSTED = "TRUSTED"  # 已登记校准的可信设备,数据可作为证据
    LOW = "LOW"          # 低可信设备,数据仅作线索,不能单独触发处罚建议


class RecordStatus(str, Enum):
    """原始记录的证据状态。"""

    ACCEPTED = "ACCEPTED"  # 通过校验,可作为证据参与片段合并
    CLUE = "CLUE"          # 低可信设备数据,仅作线索参与片段合并
    REJECTED = "REJECTED"  # 未通过校验,留痕但不参与片段合并


class RejectReason(str, Enum):
    """记录被拒绝的原因。"""

    CLOCK_DRIFT = "CLOCK_DRIFT"                  # 采样时间与接收时间偏差超过阈值
    ABNORMAL_PEAK = "ABNORMAL_PEAK"              # 分贝读数超出物理合理范围
    DUPLICATE = "DUPLICATE"                      # 同设备同采样时刻重复上传
    COMPLAINT_WITHDRAWN = "COMPLAINT_WITHDRAWN"  # 投诉已撤回,停止后续关联
    INVALID_PAYLOAD = "INVALID_PAYLOAD"          # 字段缺失或格式非法


class ReviewConclusion(str, Enum):
    """人工复核结论。"""

    VALID = "VALID"                          # 证据有效
    INSUFFICIENT = "INSUFFICIENT"            # 证据不足
    PENALTY_SUGGESTED = "PENALTY_SUGGESTED"  # 建议触发处罚


class SegmentStatus(str, Enum):
    """事件片段状态。"""

    OPEN = "OPEN"        # 未复核,仍可按连续时段规则并入新记录
    REVIEWED = "REVIEWED"  # 已复核,冻结不再并入新记录
    MERGED = "MERGED"    # 已被桥接合并进其他片段,仅保留审计


def to_utc(dt: datetime) -> datetime:
    """把 datetime 规范为 UTC aware;naive 时间按 UTC 解释。"""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def iso(dt: datetime) -> str:
    """统一存储格式:UTC、毫秒精度,保证字符串序与时间序一致。"""
    return to_utc(dt).isoformat(timespec="milliseconds")


def parse_instant(value: object) -> datetime:
    """解析采样时间,接受 datetime 或 ISO8601 字符串(支持 'Z')。"""
    if isinstance(value, datetime):
        return to_utc(value)
    if isinstance(value, str):
        return to_utc(datetime.fromisoformat(value))
    raise ValueError(f"无法解析时间: {value!r}")
