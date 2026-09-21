"""证据核验服务的端到端测试,覆盖需求中的全部核验与留痕规则。"""
from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src import (
    DeviceTrust,
    EvidenceService,
    RecordStatus,
    RejectReason,
    ReviewConclusion,
    SegmentStatus,
)

BASE = datetime(2026, 9, 21, 22, 0, tzinfo=timezone.utc)  # 夜间投诉高发时段


class FakeClock:
    """可手动推进的时钟,用于模拟采样与接收的时间关系。"""

    def __init__(self, now: datetime = BASE):
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs) -> None:
        self.now += timedelta(**kwargs)


def make_record(device="DEV-01", db=68.0, sampled_at=BASE, grid="GRID-A", ref="C-1001"):
    return {
        "device_id": device,
        "db_reading": db,
        "sampled_at": sampled_at,
        "grid_id": grid,
        "complaint_ref": ref,
    }


class ServiceTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self._tmp.name) / "evidence.db")
        self.clock = FakeClock()
        self.service = EvidenceService(self.db_path, clock=self.clock)
        self.service.register_device("DEV-01", DeviceTrust.TRUSTED, operator="admin")
        self.service.register_device("DEV-LOW", DeviceTrust.LOW, operator="admin")

    def tearDown(self):
        self.service.close()
        self._tmp.cleanup()

    def import_one(self, **kwargs):
        results = self.service.import_batch([make_record(**kwargs)], operator="gridder-7")
        self.assertEqual(len(results), 1)
        return results[0]


class TestImportValidation(ServiceTestCase):
    def test_trusted_record_accepted_and_segment_created(self):
        result = self.import_one()
        self.assertTrue(result.accepted)
        self.assertEqual(result.status, RecordStatus.ACCEPTED)
        self.assertIsNotNone(result.segment_id)
        segment = self.service.get_segment(result.segment_id)
        self.assertEqual(segment["record_count"], 1)
        self.assertTrue(segment["penalty_eligible"])

    def test_clock_drift_rejected_for_past_and_future(self):
        past = self.import_one(sampled_at=BASE - timedelta(minutes=30))
        self.assertFalse(past.accepted)
        self.assertEqual(past.reject_reason, RejectReason.CLOCK_DRIFT)

        future = self.import_one(sampled_at=BASE + timedelta(minutes=30))
        self.assertFalse(future.accepted)
        self.assertEqual(future.reject_reason, RejectReason.CLOCK_DRIFT)

        # 阈值内的轻微漂移可以接收
        ok = self.import_one(sampled_at=BASE - timedelta(minutes=3))
        self.assertTrue(ok.accepted)

    def test_abnormal_peak_rejected(self):
        too_high = self.import_one(db=180.0)
        self.assertEqual(too_high.reject_reason, RejectReason.ABNORMAL_PEAK)
        too_low = self.import_one(db=5.0, sampled_at=BASE + timedelta(seconds=30))
        self.assertEqual(too_low.reject_reason, RejectReason.ABNORMAL_PEAK)
        nan = self.import_one(db=float("nan"), sampled_at=BASE + timedelta(seconds=60))
        self.assertEqual(nan.reject_reason, RejectReason.INVALID_PAYLOAD)

    def test_duplicate_upload_rejected_and_original_kept(self):
        first = self.import_one(db=68.0)
        self.assertTrue(first.accepted)
        # 同设备同采样时刻但读数不同:拒绝,且原记录不被覆盖
        dup = self.import_one(db=99.0)
        self.assertFalse(dup.accepted)
        self.assertEqual(dup.reject_reason, RejectReason.DUPLICATE)
        self.assertEqual(dup.record_id, first.record_id)
        original = self.service.get_record(first.record_id)
        self.assertEqual(original["db_reading"], 68.0)
        self.assertEqual(original["status"], RecordStatus.ACCEPTED.value)

    def test_invalid_payload_rejected(self):
        results = self.service.import_batch([{"device_id": "DEV-01"}], operator="op")
        self.assertFalse(results[0].accepted)
        self.assertEqual(results[0].reject_reason, RejectReason.INVALID_PAYLOAD)

    def test_batch_partial_acceptance(self):
        self.clock.advance(minutes=1)
        results = self.service.import_batch([
            make_record(sampled_at=self.clock.now),                       # 通过
            make_record(db=500.0, sampled_at=self.clock.now + timedelta(seconds=5)),   # 异常峰值
            make_record(sampled_at=self.clock.now + timedelta(seconds=10)),            # 通过
        ], operator="gridder-7")
        self.assertEqual([r.accepted for r in results], [True, False, True])
        self.assertEqual(results[1].reject_reason, RejectReason.ABNORMAL_PEAK)


class TestDeviceTrust(ServiceTestCase):
    def test_low_trust_device_is_clue_only(self):
        result = self.import_one(device="DEV-LOW")
        self.assertTrue(result.accepted)
        self.assertEqual(result.status, RecordStatus.CLUE)
        segment = self.service.get_segment(result.segment_id)
        self.assertFalse(segment["penalty_eligible"])
        self.assertEqual(segment["clue_count"], 1)
        self.assertEqual(segment["trusted_count"], 0)

    def test_unknown_device_defaults_to_low_trust(self):
        result = self.import_one(device="DEV-UNKNOWN")
        self.assertEqual(result.status, RecordStatus.CLUE)

    def test_clue_only_segment_cannot_trigger_penalty(self):
        clue = self.import_one(device="DEV-LOW")
        review = self.service.review_segment(
            clue.segment_id, ReviewConclusion.PENALTY_SUGGESTED,
            basis="夜间分贝超标", operator="reviewer-1",
        )
        self.assertFalse(review.accepted)
        self.assertEqual(review.reject_reason, "NOT_PENALTY_ELIGIBLE")
        # 线索片段可以得出证据不足等结论
        ok = self.service.review_segment(
            clue.segment_id, ReviewConclusion.INSUFFICIENT,
            basis="仅低可信设备数据,需可信设备补采", operator="reviewer-1",
        )
        self.assertTrue(ok.accepted)

    def test_mixed_segment_penalty_eligible(self):
        self.import_one(device="DEV-LOW", sampled_at=BASE)
        trusted = self.import_one(sampled_at=BASE + timedelta(minutes=2))
        segment = self.service.get_segment(trusted.segment_id)
        self.assertEqual(segment["record_count"], 2)
        self.assertTrue(segment["penalty_eligible"])


class TestSegmentMerging(ServiceTestCase):
    def test_continuous_records_merge_into_one_segment(self):
        ids = []
        for minute in (0, 2, 4):
            ids.append(self.import_one(sampled_at=BASE + timedelta(minutes=minute)).segment_id)
        self.assertEqual(len(set(ids)), 1)
        segment = self.service.get_segment(ids[0])
        self.assertEqual(segment["record_count"], 3)
        self.assertEqual(segment["start_time"], "2026-09-21T22:00:00.000+00:00")
        self.assertEqual(segment["end_time"], "2026-09-21T22:04:00.000+00:00")

    def test_gap_beyond_threshold_starts_new_segment(self):
        first = self.import_one(sampled_at=BASE)
        second = self.import_one(sampled_at=BASE + timedelta(minutes=6))
        self.assertNotEqual(first.segment_id, second.segment_id)

    def test_different_grid_or_complaint_not_merged(self):
        a = self.import_one(device="DEV-01", grid="GRID-A", ref="C-1")
        b = self.import_one(device="DEV-02", grid="GRID-B", ref="C-1")
        c = self.import_one(device="DEV-03", grid="GRID-A", ref="C-2")
        self.assertEqual(len({a.segment_id, b.segment_id, c.segment_id}), 3)

    def test_out_of_order_record_bridges_segments(self):
        first = self.import_one(sampled_at=BASE)
        third = self.import_one(sampled_at=BASE + timedelta(minutes=8))
        self.assertNotEqual(first.segment_id, third.segment_id)
        # 乱序到达的中间记录桥接两个片段
        bridge = self.import_one(sampled_at=BASE + timedelta(minutes=4))
        merged = self.service.get_segment(bridge.segment_id)
        self.assertEqual(merged["record_count"], 3)
        # 被并片段保留审计痕迹
        old = self.service.get_segment(first.segment_id)
        self.assertEqual(old["status"], SegmentStatus.MERGED.value)
        self.assertEqual(old["merged_into"], merged["segment_id"])

    def test_merge_survives_restart(self):
        self.import_one(sampled_at=BASE)
        self.import_one(sampled_at=BASE + timedelta(minutes=2))
        self.service.close()
        # 模拟服务重启:同一数据库文件重新打开
        self.service = EvidenceService(self.db_path, clock=self.clock)
        segments = self.service.query_segments(grid_id="GRID-A")
        self.assertEqual(len(segments), 1)
        self.assertEqual(segments[0]["record_count"], 2)
        # 重启后新记录继续并入既有片段,合并结果不丢失
        self.import_one(sampled_at=BASE + timedelta(minutes=4))
        segments = self.service.query_segments(grid_id="GRID-A")
        self.assertEqual(len(segments), 1)
        self.assertEqual(segments[0]["record_count"], 3)

    def test_reviewed_segment_frozen(self):
        first = self.import_one(sampled_at=BASE)
        self.service.review_segment(
            first.segment_id, ReviewConclusion.VALID,
            basis="连续超标且设备可信", operator="reviewer-1",
        )
        later = self.import_one(sampled_at=BASE + timedelta(minutes=2))
        self.assertNotEqual(later.segment_id, first.segment_id)


class TestReview(ServiceTestCase):
    def test_review_requires_conclusion_basis_operator(self):
        seg_id = self.import_one().segment_id
        missing_basis = self.service.review_segment(seg_id, ReviewConclusion.VALID, basis="", operator="r1")
        self.assertFalse(missing_basis.accepted)
        self.assertEqual(missing_basis.reject_reason, "MISSING_BASIS")
        missing_operator = self.service.review_segment(seg_id, ReviewConclusion.VALID, basis="依据", operator=" ")
        self.assertFalse(missing_operator.accepted)
        self.assertEqual(missing_operator.reject_reason, "MISSING_OPERATOR")
        bad_segment = self.service.review_segment("seg_none", ReviewConclusion.VALID, basis="依据", operator="r1")
        self.assertEqual(bad_segment.reject_reason, "SEGMENT_NOT_FOUND")

    def test_review_history_is_append_only(self):
        seg_id = self.import_one().segment_id
        r1 = self.service.review_segment(seg_id, ReviewConclusion.INSUFFICIENT, basis="单次峰值", operator="r1")
        r2 = self.service.review_segment(seg_id, ReviewConclusion.VALID, basis="补充比对后确认", operator="r2")
        self.assertTrue(r1.accepted and r2.accepted)
        reviews = self.service.get_segment(seg_id)["reviews"]
        self.assertEqual(len(reviews), 2)
        self.assertEqual(reviews[0]["conclusion"], ReviewConclusion.INSUFFICIENT.value)
        self.assertEqual(reviews[1]["operator"], "r2")
        self.assertEqual(reviews[1]["basis"], "补充比对后确认")

    def test_penalty_suggested_on_trusted_segment(self):
        seg_id = self.import_one().segment_id
        result = self.service.review_segment(
            seg_id, ReviewConclusion.PENALTY_SUGGESTED,
            basis="夜间连续超标,可信设备采集", operator="reviewer-1",
        )
        self.assertTrue(result.accepted)
        segment = self.service.get_segment(seg_id)
        self.assertEqual(segment["status"], SegmentStatus.REVIEWED.value)


class TestWithdrawal(ServiceTestCase):
    def test_withdraw_stops_future_association_but_keeps_audit(self):
        first = self.import_one(sampled_at=BASE)
        self.import_one(sampled_at=BASE + timedelta(minutes=2))
        withdrawn = self.service.withdraw_complaint("C-1001", operator="officer-3", reason="投诉人撤销")
        self.assertTrue(withdrawn.accepted)
        # 撤回后新记录拒绝关联,但留痕可查
        self.clock.advance(minutes=1)
        later = self.import_one(sampled_at=BASE + timedelta(minutes=4))
        self.assertFalse(later.accepted)
        self.assertEqual(later.reject_reason, RejectReason.COMPLAINT_WITHDRAWN)
        traced = self.service.get_record(later.record_id)
        self.assertEqual(traced["status"], RecordStatus.REJECTED.value)
        # 历史片段与记录完整保留
        segment = self.service.get_segment(first.segment_id)
        self.assertEqual(segment["record_count"], 2)
        # 重复撤回幂等拒绝,不覆盖首次撤回信息
        again = self.service.withdraw_complaint("C-1001", operator="officer-3")
        self.assertFalse(again.accepted)
        self.assertTrue(again.already_withdrawn)
        # 审计日志包含撤回与拒绝关联两类事件
        actions = {row["action"] for row in self.service.list_audit()}
        self.assertIn("COMPLAINT_WITHDRAWN", actions)
        self.assertIn("RECORD_REJECTED", actions)

    def test_records_never_deleted(self):
        self.import_one(sampled_at=BASE)
        self.import_one(db=500.0, sampled_at=BASE + timedelta(minutes=1))  # 被拒也留痕
        self.service.withdraw_complaint("C-1001", operator="officer-3")
        self.import_one(sampled_at=BASE + timedelta(minutes=2))            # 撤回后留痕
        self.assertEqual(self.service._store.count_records(), 3)


if __name__ == "__main__":
    unittest.main()
