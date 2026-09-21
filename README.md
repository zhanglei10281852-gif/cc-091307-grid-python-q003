# 社区噪声证据核验

面向网格员与社区管理的噪声证据核验服务:接收带设备编号、分贝读数、采样时间、
位置网格和投诉关联号的记录,完成时钟漂移、异常峰值、重复上传校验,按连续时段
规则形成可复核的事件片段,并对人工复核、投诉撤回全程留痕。

运行环境:Python 3.11,仅依赖标准库(持久化使用 SQLite)。

## 核验规则

| 规则 | 判定 | 处理 |
| --- | --- | --- |
| 时钟漂移 | 采样时间与接收时间偏差超过阈值(默认 10 分钟,含未来时间) | 拒绝并留痕,`CLOCK_DRIFT` |
| 异常峰值 | 分贝读数超出物理合理范围(默认 20–130 dB) | 拒绝并留痕,`ABNORMAL_PEAK` |
| 重复上传 | 同设备同采样时刻已存在记录 | 拒绝并保留原记录,`DUPLICATE` |
| 投诉已撤回 | 投诉关联号已撤回 | 停止关联、留痕,`COMPLAINT_WITHDRAWN` |

- 原始记录 insert-only,任何校验失败都只追加留痕,不覆盖、不删除。
- 同一(位置网格, 投诉关联号)下,相邻采样间隔不超过阈值(默认 5 分钟)的
  记录合并为一个事件片段;乱序到达的记录可桥接合并既有片段。片段全部落库,
  服务重启后合并结果不丢失,新记录继续并入既有片段。
- 低可信设备(含未登记设备)的数据状态为 `CLUE`,仅作线索;只含线索的片段
  `penalty_eligible=False`,复核时不能得出"建议处罚"结论。
- 人工复核必须写明结论、依据、操作者;复核历史 append-only,已复核片段冻结,
  不再并入新记录。
- 投诉撤回仅停止后续关联,历史记录、片段与审计日志全部保留。

## 接口用法

```python
from datetime import datetime, timezone
from src import EvidenceService, DeviceTrust, ReviewConclusion

service = EvidenceService("evidence.db")  # 状态持久化于 SQLite,重启不丢
service.register_device("DEV-01", DeviceTrust.TRUSTED, operator="admin")

# 批量导入:逐条返回证据状态或拒绝原因
results = service.import_batch([{
    "device_id": "DEV-01",
    "db_reading": 68.5,
    "sampled_at": datetime.now(timezone.utc),
    "grid_id": "GRID-A",
    "complaint_ref": "C-1001",
}], operator="gridder-7")

# 片段查询:可复核的连续时段视图
segments = service.query_segments(grid_id="GRID-A", complaint_ref="C-1001")
detail = service.get_segment(segments[0]["segment_id"])  # 含记录清单与复核历史

# 人工复核:结论、依据、操作者必填
service.review_segment(
    segments[0]["segment_id"],
    ReviewConclusion.PENALTY_SUGGESTED,
    basis="夜间连续超标,可信设备采集",
    operator="reviewer-1",
)

# 投诉撤回:停止后续关联,历史数据保留
service.withdraw_complaint("C-1001", operator="officer-3", reason="投诉人撤销")
```

时钟漂移阈值、合并间隔、分贝量程均可在 `EvidenceService` 构造时按部署环境调整。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
