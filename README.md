# 古法蓝晒底片整理室 · 可恢复工序账

把底片、药液批次、冲洗工位、入盒交付接成一条可恢复工序账。

## 机制

| 需求 | 实现 |
| --- | --- |
| 同一工位同时只接一张底片，后到请求留冲突 | 工位条件锁（`lockedBy`），并发请求后到者返回 `409 station_conflict` |
| 交接完成前锁定工位、预占药液 | 两阶段交接：先 `reserved`（药液 `stock→held`、工位锁定），再 `confirmed`（`held→consumed`、释放工位） |
| 写入失败后从最后确认步骤恢复，重试不重复扣减 | 每次交接收幂等键（`Idempotency-Key` 头或 `idempotencyKey` 字段）→ 唯一 `opId`；崩溃后 `recover` 把 `reserved` 操作继续推进，重试回放不重复扣减 |
| 药液浓度/配比一变，未完成结论与入盒许可立即失效重算 | 批次带 `version`，变更即 `version++`；结论记录 `basedOnBatchVersion`，读时自动重算，入盒许可失效则拒绝入盒 |
| 旧数据没有批次号时升级补来源，历史履历仍能查到 | 迁移补 `chemicalBatch=B-LEGACY` 与 `batchSource=legacy-backfill`，补来源写入日志，历史 logs/steps 保留可查 |

## 运行

```bash
npm start
```

访问 `http://localhost:3040`。数据保存在 `data/cyanotype-negative-room.json`。

服务启动时自动执行：迁移补来源 → 崩溃恢复（把未完成的 `reserved` 操作推进到 `confirmed`）。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/items` | 底片列表（结论随批次版本自动重算） |
| POST | `/api/items` | 新增底片（批次留空则补来源） |
| PATCH | `/api/items/:id` | 更新底片 |
| POST | `/api/items/:id/logs` | 追加日志 |
| POST | `/api/items/:id/handoff` | 交接（锁定工位、预占药液、幂等恢复） |
| POST | `/api/items/:id/recompute` | 强制按当前批次重算结论 |
| GET | `/api/batches` | 药液批次列表 |
| PATCH | `/api/batches/:code` | 更新浓度/配比（`version++` 触发失效重算） |
| GET | `/api/stations` | 工位列表（含锁定状态） |
| GET | `/api/operations` | 工序账（操作记录） |
| POST | `/api/recover` | 手动触发崩溃恢复 |
| GET | `/api/stats` | 统计 |

交接请求带 `Idempotency-Key` 头（或 body 里的 `idempotencyKey`），同一键重试不重复扣减。

## 测试

```bash
node demo.js        # 工序账核心逻辑：48 项断言（补来源、工位锁、预占扣减、幂等、崩溃恢复、版本失效）
node smoke-http.js  # HTTP 层冒烟：13 项断言（并发冲突、幂等扣减、批次失效、入盒拒绝、恢复端点）
```

## 文件

- `ledger.js` — 可恢复工序账核心逻辑（无 I/O，纯函数操作 db）
- `server.js` — HTTP 服务 + 内嵌界面（启动时加载一次 db，并发请求共享内存状态）
- `demo.js` — 核心逻辑验证
- `smoke-http.js` — HTTP 层冒烟测试
