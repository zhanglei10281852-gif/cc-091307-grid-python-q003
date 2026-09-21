"""证据核验服务集成测试（仅标准库 unittest）。"""

import json
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path

from src.httpapi import make_handler
from src.service import (
    Config,
    EvidenceService,
    ServiceError,
    EV_ENFORCEABLE,
    EV_LEAD_ONLY,
    EV_INSUFFICIENT,
    EV_BELOW,
)

UTC = timezone.utc
NOW = datetime(2026, 9, 21, 22, 0, 0, tzinfo=UTC)


def iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat()


def rec(device, db, dt, grid="G01", complaint="C001"):
    return {
        "device_id": device,
        "db": db,
        "sampled_at": iso(dt),
        "grid": grid,
        "complaint_ref": complaint,
    }


class ServiceCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "test.db")
        self.svc = EvidenceService(Config(db_path=self.db))

    def tearDown(self):
        self.svc.close()
        self.tmp.cleanup()

    def ingest(self, rows):
        return self.svc.ingest_batch(rows, received_at=NOW)

    # -- 校验 ---------------------------------------------------------------

    def test_field_validation_and_rejection_reasons(self):
        out = self.ingest(
            [
                {"device_id": "", "db": 60, "sampled_at": iso(NOW), "grid": "G"},
                {"device_id": "D1", "db": "x", "sampled_at": iso(NOW), "grid": "G"},
                {"device_id": "D1", "db": 60, "sampled_at": "not-a-time", "grid": "G"},
                {"device_id": "D1", "db": 200, "sampled_at": iso(NOW), "grid": "G"},
                {"device_id": "D1", "db": 60,
                 "sampled_at": iso(NOW + timedelta(hours=2)), "grid": "G"},
            ]
        )
        self.assertEqual(out["rejected"], 5)
        self.assertIn("missing_device_id", out["results"][0]["reasons"])
        self.assertIn("bad_db", out["results"][1]["reasons"])
        self.assertIn("bad_timestamp", out["results"][2]["reasons"])
        self.assertIn("db_out_of_range", out["results"][3]["reasons"])
        self.assertIn("clock_drift", out["results"][4]["reasons"])
        # 被拒绝的原始报文仍可审计
        stored = self.svc.get_record(out["results"][0]["record_id"])
        self.assertEqual(stored["ingest_status"], "rejected")
        self.assertEqual(stored["raw"]["db"], 60)

    def test_duplicate_upload_within_and_across_batches(self):
        rows = [rec("D1", 60, NOW), rec("D1", 60, NOW)]
        out1 = self.ingest(rows)
        self.assertEqual(out1["accepted"], 1)
        self.assertEqual(out1["rejected"], 1)
        self.assertIn("duplicate_upload", out1["results"][1]["reasons"])
        out2 = self.ingest([rec("D1", 99, NOW)])
        self.assertEqual(out2["rejected"], 1)
        self.assertIn("duplicate_upload", out2["results"][0]["reasons"])

    def test_anomalous_peak_downgrades_high_trust_to_lead(self):
        self.svc.register_device("DH", "high")
        base = [rec("DH", v, NOW + timedelta(seconds=t), "G2", "C2")
                for t, v in [(-30, 40), (-20, 41), (-10, 42)]]
        self.ingest(base)
        out = self.ingest([rec("DH", 88, NOW + timedelta(seconds=5), "G2", "C2")])
        r = out["results"][0]
        self.assertEqual(r["status"], "accepted")
        self.assertIn("anomalous_peak", r["flags"])
        self.assertEqual(r["evidence_status"], "lead")

    # -- 片段规则 -----------------------------------------------------------

    def test_enforceable_segment_requires_high_trust_duration_count(self):
        self.svc.register_device("DH", "high")
        out = self.ingest([
            rec("DH", 60, NOW - timedelta(seconds=60), "G3", "C3"),
            rec("DH", 62, NOW, "G3", "C3"),
            rec("DH", 61, NOW + timedelta(seconds=60), "G3", "C3"),
        ])
        seg_id = out["results"][0]["segment_id"]
        seg = self.svc.get_segment(seg_id)
        self.assertEqual(seg["evidence_status"], EV_ENFORCEABLE)
        self.assertTrue(seg["penalty_recommendation"])
        self.assertEqual(seg["sample_count"], 3)
        self.assertEqual(seg["high_trust_count"], 3)

    def test_low_trust_only_segment_is_lead_only(self):
        self.svc.register_device("L1", "low")
        self.svc.register_device("L2", "low")
        out = self.ingest([
            rec("L1", 70, NOW - timedelta(seconds=60), "G4", "C4"),
            rec("L2", 72, NOW, "G4", "C4"),
            rec("L1", 71, NOW + timedelta(seconds=60), "G4", "C4"),
        ])
        seg = self.svc.get_segment(out["results"][0]["segment_id"])
        self.assertEqual(seg["evidence_status"], EV_LEAD_ONLY)
        self.assertFalse(seg["penalty_recommendation"])
        for r in seg["records"]:
            self.assertIn("low_trust_device", r["flags"])

    def test_insufficient_and_below_status(self):
        self.svc.register_device("DH", "high")
        out = self.ingest([  # 只有 1 条高可信
            rec("DH", 60, NOW - timedelta(seconds=60), "G5", "C5"),
            rec("LX", 62, NOW + timedelta(seconds=60), "G5", "C5"),
        ])
        seg = self.svc.get_segment(out["results"][0]["segment_id"])
        self.assertEqual(seg["evidence_status"], EV_INSUFFICIENT)

        out2 = self.ingest([  # 中位数低于限值（时间错开避免同设备同时刻去重）
            rec("DH", 40, NOW + timedelta(seconds=300), "G6", "C6"),
            rec("DH", 42, NOW + timedelta(seconds=360), "G6", "C6"),
            rec("DH", 41, NOW + timedelta(seconds=420), "G6", "C6"),
        ])
        seg2 = self.svc.get_segment(out2["results"][0]["segment_id"])
        self.assertEqual(seg2["evidence_status"], EV_BELOW)

    def test_gap_splits_segments_and_bridge_merges_with_supersede_chain(self):
        # 漂移容差 900s，两簇分别在 ±800s 附近，间隔 1400s > 600s 间隔阈值
        out1 = self.ingest([
            rec("DH", 60, NOW - timedelta(seconds=800), "G7", "C7"),
            rec("DH", 61, NOW - timedelta(seconds=700), "G7", "C7"),
            rec("DH", 63, NOW + timedelta(seconds=700), "G7", "C7"),
            rec("DH", 62, NOW + timedelta(seconds=800), "G7", "C7"),
        ])
        seg_ids = {r["segment_id"] for r in out1["results"]}
        self.assertEqual(len(seg_ids), 2)
        early, late = sorted(seg_ids)

        # 在合并前对将被吞并的片段做复核（审计须保留）
        review = self.svc.add_review(
            late, "初步成立", "后半夜连续超限读数2条", "张三"
        )

        # 补入桥接读数，间隔均 <= 600s
        self.ingest([
            rec("DH", 60, NOW - timedelta(seconds=200), "G7", "C7"),
            rec("DH", 60, NOW + timedelta(seconds=200), "G7", "C7"),
            rec("DH", 60, NOW + timedelta(seconds=400), "G7", "C7"),
        ])
        active = self.svc.list_segments(grid="G7")["segments"]
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0]["segment_id"], early)
        self.assertEqual(active[0]["sample_count"], 7)

        # 旧片段标记 superseded 而非删除
        old = self.svc.get_segment(late)
        self.assertFalse(old["active"])
        self.assertEqual(old["superseded_by"], early)

        # 对已失效片段复核 → 冲突并指向当前片段
        with self.assertRaises(ServiceError) as cm:
            self.svc.add_review(late, "x", "y", "z")
        self.assertEqual(cm.exception.code, "segment_superseded")
        self.assertEqual(cm.exception.active_id, early)

        # 旧复核沿合并链仍可查
        merged = self.svc.get_segment(early)
        operators = [rv["operator"] for rv in merged["reviews"]]
        self.assertIn("张三", operators)

    def test_segments_survive_restart(self):
        self.svc.register_device("DH", "high")
        self.ingest([
            rec("DH", 60, NOW - timedelta(seconds=60), "G8", "C8"),
            rec("DH", 62, NOW, "G8", "C8"),
            rec("DH", 61, NOW + timedelta(seconds=60), "G8", "C8"),
        ])
        before = self.svc.list_segments()["segments"]
        self.svc.close()

        svc2 = EvidenceService(Config(db_path=self.db))  # __init__ 自动对账
        try:
            after = svc2.list_segments()["segments"]
            self.assertEqual(
                [(s["segment_id"], s["sample_count"], s["evidence_status"])
                 for s in before],
                [(s["segment_id"], s["sample_count"], s["evidence_status"])
                 for s in after],
            )
            seg = svc2.get_segment(before[0]["segment_id"])
            self.assertEqual(len(seg["records"]), 3)
        finally:
            svc2.close()

    # -- 投诉撤回 -----------------------------------------------------------

    def test_withdraw_stops_future_linking_but_keeps_audit(self):
        out1 = self.ingest([rec("DH", 61, NOW, "G9", "C9")])
        seg_id = out1["results"][0]["segment_id"]
        self.svc.add_review(seg_id, "噪声属实", "夜间连续超限", "李四")

        w = self.svc.withdraw_complaint("C9", "投诉人主动撤诉")
        self.assertEqual(w["status"], "withdrawn")
        again = self.svc.withdraw_complaint("C9")  # 幂等
        self.assertTrue(again["idempotent"])

        out2 = self.ingest([rec("DH", 90, NOW + timedelta(seconds=30), "G9", "C9")])
        r = out2["results"][0]
        self.assertEqual(r["status"], "accepted_unlinked")
        self.assertNotIn("segment_id", r)
        stored = self.svc.get_record(r["record_id"])
        self.assertFalse(stored["linked"])

        # 历史片段、读数、复核全部保留
        seg = self.svc.get_segment(seg_id)
        self.assertEqual(seg["sample_count"], 1)
        self.assertEqual(len(seg["reviews"]), 1)
        self.assertEqual(len(self.svc.list_segments(complaint_ref="C9")["segments"]), 1)

    # -- 复核留痕 -----------------------------------------------------------

    def test_review_requires_conclusion_basis_operator(self):
        out = self.ingest([rec("DH", 60, NOW, "G10", "C10")])
        seg_id = out["results"][0]["segment_id"]
        with self.assertRaises(ServiceError) as cm:
            self.svc.add_review(seg_id, "  ", "依据", "王五")
        self.assertEqual(cm.exception.code, "missing_review_fields")

    def test_reviews_and_records_are_immutable(self):
        out = self.ingest([rec("DH", 60, NOW, "G11", "C11")])
        rid = out["results"][0]["record_id"]
        seg_id = out["results"][0]["segment_id"]
        self.svc.add_review(seg_id, "成立", "读数连续超限", "赵六")

        raw = sqlite3.connect(self.db)
        try:
            # RAISE(ABORT) 触发器以 DatabaseError(IntegrityError) 中止写操作
            with self.assertRaises(sqlite3.DatabaseError):
                raw.execute("UPDATE records SET db_reading=10 WHERE id=?", (rid,))
            with self.assertRaises(sqlite3.DatabaseError):
                raw.execute("DELETE FROM records WHERE id=?", (rid,))
            with self.assertRaises(sqlite3.DatabaseError):
                raw.execute("UPDATE reviews SET conclusion='x' WHERE id=1")
        finally:
            raw.close()

    def test_segment_query_filters(self):
        self.svc.register_device("DH", "high")
        self.ingest([
            rec("DH", 60, NOW - timedelta(seconds=60), "G12", "C12"),
            rec("DH", 62, NOW, "G12", "C12"),
            rec("DH", 61, NOW + timedelta(seconds=60), "G12", "C12"),
        ])
        self.assertEqual(
            len(self.svc.list_segments(status=EV_ENFORCEABLE)["segments"]), 1
        )
        self.assertEqual(
            self.svc.list_segments(status=EV_BELOW)["segments"], []
        )
        self.assertEqual(
            self.svc.list_segments(complaint_ref="NOPE")["segments"], []
        )

    def test_unknown_segment_404(self):
        with self.assertRaises(ServiceError) as cm:
            self.svc.get_segment("SEG-nope")
        self.assertEqual(cm.exception.code, "segment_not_found")


class HttpCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db = str(Path(cls.tmp.name) / "http.db")
        cls.svc = EvidenceService(Config(db_path=cls.db))
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(cls.svc))
        cls.port = cls.httpd.server_address[1]
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.thread.join(timeout=5)
        cls.svc.close()
        cls.tmp.cleanup()

    def _req(self, method, path, body=None):
        conn = HTTPConnection("127.0.0.1", self.port, timeout=5)
        payload = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json"} if payload else {}
        conn.request(method, path, body=payload, headers=headers)
        resp = conn.getresponse()
        data = json.loads(resp.read().decode())
        conn.close()
        return resp.status, data

    def test_full_flow_over_http(self):
        status, _ = self._req("POST", "/v1/devices",
                              {"device_id": "DH", "trust_level": "high"})
        self.assertEqual(status, 200)

        records = [
            {"device_id": "DH", "db": 60,
             "sampled_at": iso(NOW - timedelta(seconds=60)),
             "grid": "GW", "complaint_ref": "CW"},
            {"device_id": "DH", "db": 62, "sampled_at": iso(NOW),
             "grid": "GW", "complaint_ref": "CW"},
            {"device_id": "DH", "db": 61,
             "sampled_at": iso(NOW + timedelta(seconds=60)),
             "grid": "GW", "complaint_ref": "CW"},
            {"device_id": "DH", "db": 61,
             "sampled_at": iso(NOW + timedelta(seconds=60)),
             "grid": "GW", "complaint_ref": "CW"},
        ]
        status, out = self._req("POST", "/v1/records:batch",
                                {"records": records, "received_at": iso(NOW)})
        self.assertEqual(status, 200)
        self.assertEqual(out["accepted"], 3)
        self.assertEqual(out["rejected"], 1)
        self.assertIn("duplicate_upload", out["results"][3]["reasons"])
        seg_id = out["results"][0]["segment_id"]

        status, seg = self._req("GET", f"/v1/segments?grid=GW&status={EV_ENFORCEABLE}")
        self.assertEqual(status, 200)
        self.assertEqual(len(seg["segments"]), 1)

        # 复核缺字段 → 400 + 明确原因
        status, err = self._req(
            "POST", f"/v1/segments/{seg_id}/reviews",
            {"conclusion": "", "basis": "b", "operator": "o"},
        )
        self.assertEqual(status, 400)
        self.assertEqual(err["error"]["code"], "missing_review_fields")

        status, review = self._req(
            "POST", f"/v1/segments/{seg_id}/reviews",
            {"conclusion": "处罚建议成立",
             "basis": "高可信设备3条读数，跨度120秒，中位数≥55dB",
             "operator": "执法员-陈七"},
        )
        self.assertEqual(status, 201)
        self.assertEqual(review["operator"], "执法员-陈七")

        status, detail = self._req("GET", f"/v1/segments/{seg_id}")
        self.assertEqual(status, 200)
        self.assertEqual(len(detail["reviews"]), 1)

        status, err = self._req("GET", "/v1/segments/SEG-missing")
        self.assertEqual(status, 404)
        self.assertEqual(err["error"]["code"], "segment_not_found")


if __name__ == "__main__":
    unittest.main()
