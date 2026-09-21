# 社区噪声证据核验

面向网格员 / 执法人员的噪声扰民证据核验服务：接收设备噪声读数，完成
时钟漂移、异常峰值、重复上传校验，按连续时段规则归并为可复核的事件
片段，并对人工复核全流程留痕。仅依赖 Python 3.11 标准库，持久化使用
SQLite（默认 WAL 无关的单文件库），无第三方依赖。

## 业务规则

- **入库记录**（含被拒绝的）只追加、永不更新或删除，原始报文完整保留；
  数据库触发器在底层兜底，任何 UPDATE/DELETE 均被中止。
- **校验项**：必填字段（设备编号 / 分贝 / 采样时间 / 网格）、时间格式、
  分贝物理量程、设备时钟与服务端时钟漂移、相对同设备近期中位数的异常
  峰值、同设备同时刻的重复上传（批次内与跨批次均拦截）。
- **设备信任等级**：`high` / `low`（未注册设备默认 low）。低可信设备数据
  标记为 `lead`（线索）；高可信设备被识别为异常峰值的读数同样降级为
  线索。线索**不能单独触发处罚建议**。
- **片段合并**：同一位置网格 + 同一投诉关联号下，相邻读数间隔不超过
  `gap_tolerance_seconds`（默认 600s）归入同一片段。证据状态：
  - `enforceable`：中位数 ≥ 限值（默认 55dB）、高可信读数 ≥ 3 条、
    跨度 ≥ 120s，`penalty_recommendation=true`；
  - `lead_only`：超限但无高可信读数（仅线索）；
  - `insufficient`：超限且有高可信读数，但条数 / 时长不足；
  - `below_threshold`：未超限。
- **片段持久化与重启恢复**：片段编号由「网格 + 投诉号 + 首条记录号」
  确定性生成；每次导入及服务启动时基于不可变原始记录做幂等对账。
  新读数桥接两个旧片段时，旧片段不删除，以 `superseded_by` 指向合并
  后的片段，旧片段上的复核意见沿合并族继续可查。
- **投诉撤回**：后续读数仍接收留档（`accepted_unlinked`，`linked=false`），
  但不再关联任何片段；历史读数、片段、复核意见全部保留，撤回操作幂等。
- **人工复核**：必须同时写明 `conclusion`（结论）、`basis`（依据）、
  `operator`（操作者），缺一返回 `missing_review_fields`；复核意见只追加。
  对已被合并的片段复核返回 409，并给出当前有效片段编号。

## 运行

```bash
python3 -m src.service --db evidence.db --host 0.0.0.0 --port 8080
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/devices` | 注册 / 更新设备 `{device_id, trust_level, note}` |
| POST | `/v1/complaints/withdraw` | 撤回投诉 `{complaint_ref, note}` |
| POST | `/v1/records:batch` | 批量导入 `{received_at?, records:[...]}` |
| GET | `/v1/segments` | 查询：`grid`、`complaint_ref`、`status`、`start`、`end`、`include_superseded` |
| GET | `/v1/segments/{id}` | 片段详情（原始读数、证据状态、合并链、全部复核意见） |
| GET | `/v1/records/{id}` | 原始记录（拒绝原因、标记位、原始报文） |
| POST | `/v1/segments/{id}/reviews` | 人工复核 `{conclusion, basis, operator}` |
| GET | `/v1/segments/{id}/reviews` | 复核列表（沿合并族汇总） |

读数字段：`device_id`、`db`、`sampled_at`（ISO8601，朴素时间按 UTC）、
`grid`、`complaint_ref`。

批量导入逐条返回：

- `status`：`accepted` / `accepted_unlinked` / `rejected`
- `reasons`：拒绝原因，如 `missing_db`、`bad_timestamp`、`db_out_of_range`、
  `clock_drift`、`duplicate_upload`
- `flags`：`low_trust_device`、`anomalous_peak`、`complaint_withdrawn_unlinked`
- `evidence_status`：`evidence` / `lead`
- `segment_id`：归属片段（拒绝或撤回脱钩时不返回）

错误响应统一为 `{"error": {"code", "message", ...}}`，409 冲突附带
`active_segment_id`。

## 直接以库方式使用

```python
from src.service import Config, EvidenceService

svc = EvidenceService(Config(db_path="evidence.db"))
svc.register_device("GW-01", "high")
svc.ingest_batch([{"device_id": "GW-01", "db": 62.4,
                   "sampled_at": "2026-09-21T22:00:00Z",
                   "grid": "G07", "complaint_ref": "TS-03"}])
svc.add_review("SEG-...", "噪声属实", "连续3点超限、跨度120秒", "陈七")
```

## 测试

```bash
python3 -m unittest discover -s tests -v
```

阈值（噪声限值、漂移容差、合并间隔、最短时长、峰值跳变等）均在
`src/service.py` 的 `Config` 中，可按部署环境调整。
