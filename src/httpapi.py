"""证据核验服务的 HTTP 接口（仅依赖标准库）。

端点：
  POST /v1/devices                 注册/更新设备 {device_id, trust_level, note}
  POST /v1/complaints/withdraw     撤回投诉 {complaint_ref, note}
  POST /v1/records:batch           批量导入读数 {records:[...]}
  GET  /v1/segments                片段查询（grid/complaint_ref/status/start/end）
  GET  /v1/segments/{id}           片段详情（含原始读数与复核链）
  GET  /v1/records/{id}            原始记录详情（含拒绝原因与原始报文）
  POST /v1/segments/{id}/reviews   人工复核 {conclusion, basis, operator}
  GET  /v1/segments/{id}/reviews   复核列表（沿合并链汇总）

错误响应统一为 {"error": {"code", "message", ...}}。
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

from .service import Conflict, EvidenceService, NotFound, ServiceError, parse_ts


def _json_response(handler: BaseHTTPRequestHandler, status: int, body: dict) -> None:
    payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(payload)))
    handler.end_headers()
    handler.wfile.write(payload)


def _error(handler: BaseHTTPRequestHandler, exc: ServiceError) -> None:
    error: dict = {"code": exc.code, "message": exc.message}
    if isinstance(exc, (NotFound, Conflict)) and exc.active_id:
        error["active_segment_id"] = exc.active_id
    _json_response(handler, exc.http_status, {"error": error})


def make_handler(service: EvidenceService):
    class Handler(BaseHTTPRequestHandler):
        server_version = "EvidenceService/1.0"

        def log_message(self, fmt: str, *args: object) -> None:  # 静默
            return

        # -- 工具 -----------------------------------------------------------

        def _read_json(self) -> dict:
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            try:
                body = json.loads(raw.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                raise ServiceError("bad_json", "请求体不是合法 JSON")
            if not isinstance(body, dict):
                raise ServiceError("bad_payload", "请求体必须是 JSON 对象")
            return body

        def _send(self, status: int, body: dict) -> None:
            _json_response(self, status, body)

        # -- 路由 -----------------------------------------------------------

        def do_GET(self) -> None:  # noqa: N802
            try:
                parsed = urlparse(self.path)
                path = parsed.path.rstrip("/") or "/"
                qs = parse_qs(parsed.query)

                def one(name: str):
                    return qs[name][0] if name in qs else None

                if path == "/v1/segments":
                    kwargs = {
                        "grid": one("grid"),
                        "complaint_ref": one("complaint_ref"),
                        "status": one("status"),
                        "include_superseded": one("include_superseded") in (
                            "1", "true", "yes",
                        ),
                    }
                    if one("start"):
                        kwargs["start"] = parse_ts(one("start"))
                    if one("end"):
                        kwargs["end"] = parse_ts(one("end"))
                    self._send(200, service.list_segments(**kwargs))
                    return

                if path.startswith("/v1/segments/"):
                    rest = path[len("/v1/segments/"):]
                    parts = rest.split("/")
                    if len(parts) == 1:
                        self._send(200, service.get_segment(parts[0]))
                        return
                    if len(parts) == 2 and parts[1] == "reviews":
                        self._send(200, service.list_reviews(parts[0]))
                        return

                if path.startswith("/v1/records/"):
                    rec_id = path[len("/v1/records/"):]
                    if rec_id.isdigit():
                        self._send(200, service.get_record(int(rec_id)))
                        return

                if path in ("/health", "/v1/health"):
                    self._send(200, {"status": "ok"})
                    return

                raise ServiceError("not_found", f"未知路径：{path}", 404)
            except ServiceError as exc:
                _error(self, exc)
            except Exception:  # noqa: BLE001
                _error(self, ServiceError("internal", "服务内部错误", 500))

        def do_POST(self) -> None:  # noqa: N802
            try:
                parsed = urlparse(self.path)
                path = parsed.path.rstrip("/") or "/"
                body = self._read_json()

                if path == "/v1/devices":
                    result = service.register_device(
                        str(body.get("device_id", "")),
                        str(body.get("trust_level", "low")),
                        str(body.get("note", "")),
                    )
                    self._send(200, result)
                    return

                if path == "/v1/complaints/withdraw":
                    result = service.withdraw_complaint(
                        str(body.get("complaint_ref", "")),
                        str(body.get("note", "")),
                    )
                    self._send(200, result)
                    return

                if path == "/v1/records:batch":
                    records = body.get("records")
                    if not isinstance(records, list):
                        raise ServiceError(
                            "bad_payload", "records 必须为数组"
                        )
                    received_at = None
                    if body.get("received_at"):
                        received_at = parse_ts(body["received_at"])
                    self._send(200, service.ingest_batch(records, received_at))
                    return

                if path.startswith("/v1/segments/"):
                    rest = path[len("/v1/segments/"):]
                    parts = rest.split("/")
                    if len(parts) == 2 and parts[1] == "reviews":
                        result = service.add_review(
                            parts[0],
                            str(body.get("conclusion", "")),
                            str(body.get("basis", "")),
                            str(body.get("operator", "")),
                        )
                        self._send(201, result)
                        return

                raise ServiceError("not_found", f"未知路径：{path}", 404)
            except ServiceError as exc:
                _error(self, exc)
            except Exception:  # noqa: BLE001
                _error(self, ServiceError("internal", "服务内部错误", 500))

    return Handler


def build_server(config, host: str = "0.0.0.0", port: int = 8080):
    from http.server import ThreadingHTTPServer

    service = EvidenceService(config)
    httpd = ThreadingHTTPServer((host, port), make_handler(service))
    return httpd, service
