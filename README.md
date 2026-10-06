# 古法蓝晒底片整理室 · 可恢复工序账

运行：

```bash
npm start
```

访问 `http://localhost:3040`。

## 数据模型

底片、药液批次、冲洗工位、入盒交付接成**一条只追加的工序账**
`data/cyanotype-ledger.jsonl`（每行一条事件，append + fsync 落盘）。
内存状态由账本重放得到，不再有“两个终端互相覆盖”的整文件改写。

- 工序顺序：涂布 → 晾干 → 曝光 → 冲洗 →（复晒，可回环）→ 入盒 → 交付
- 工位：T-01 涂布晾干 / X-01 曝光 / W-01、W-02 冲洗 / B-01 入盒
- 药液：存量 `stockMl`、预占 `reservedMl`、已用 `usedMl`

## 并发与恢复规则

1. **同一工位同时只接一张底片**：先到的请求建立占用
   （`STATION_CLAIMED`，锁定工位并预占药液），后到请求收到
   `409 station_busy`，同时写一条 `CONFLICT` 冲突事件留痕，不会盖掉先到步骤。
   同一张底片同时也只能占一个工位（`negative_already_claimed`）。
2. **交接完成前锁定工位和药液预占**：占用时只冻结药量不扣减；
   确认完成（`STEP_COMMITTED`）时才实际扣减，并释放工位。
3. **写入失败/断电后从最后确认步骤恢复**：重启时重放账本，
   “已占未确认”的占用（工位锁、预占药量）原样恢复，
   在 `GET /api/recovery` 的 `openClaims` 中列出，凭 `claimId` 续作。
4. **重试不重复扣减**：每个写请求可带 `idemKey`（幂等键）。
   同一把钥匙重放返回首次结果（`replayed: true`），扣药只发生一次；
   无占用的裸确认返回 `409 no_open_claim`。
5. **药液浓度/配比变更**（`POST /api/batches/formula`）使批次
   `formulaVersion` 递增；所有未交付底片的曝光结论、冲洗结论、入盒许可
   只要锚定的版本落后即**立即失效**（底片履历记一笔 invalidate），
   必须用新配方重做曝光/冲洗，许可重新有效后才能入盒、交付。已交付底片封档。
6. **旧数据升级**：首次启动自动迁移旧版
   `cyanotype-negative-room.json`：
   - 旧步骤按时间还原为带 `legacy: true` 的履历，历史仍可逐张查询；
   - 没有批次号的底片补登来源批次 `LEGACY-0001`（“旧数据缺少批次号”）；
   - 按旧状态语义补登缺失的曝光/冲洗/入盒/交付节点；
   - 原文件归档为 `cyanotype-negative-room.legacy.json`。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/negatives` / `/api/negatives/:code` | 底片工序账（含结论有效性、入盒许可、失效履历） |
| POST | `/api/negatives` | 建档 `{plateSize, chemicalBatch, waterSource, idemKey?}` |
| POST | `/api/claims` | 占用工位 `{code, step, station, batchNo?, idemKey?}` |
| POST | `/api/steps/confirm` | 确认完成并扣药 `{code, claimId?, usedMl?, developStatus?, defect?, repair?, note?, idemKey?}` |
| POST | `/api/claims/abort` | 中止占用，释放工位与预占 `{code, claimId?, reason?}` |
| POST | `/api/box` | 入盒（许可有效才放行）`{code, box, idemKey?}` |
| POST | `/api/deliver` | 交付（封档）`{code, idemKey?}` |
| POST | `/api/batches` | 登记批次 `{batchNo, concentration, ratio, sourceNote, stockMl, idemKey?}` |
| POST | `/api/batches/formula` | 浓度/配比变更，旧结论立即失效 `{batchNo, concentration?, ratio?, idemKey?}` |
| GET | `/api/batches` / `/api/stations` / `/api/conflicts` / `/api/recovery` | 批次/工位/冲突台账/恢复视图 |
| GET | `/api/ledger` | 原始工序账（NDJSON） |

所有冲突拒绝均返回 4xx 与结构化错误码（`station_busy`、
`negative_already_claimed`、`step_out_of_order`、`box_permit_invalid`、
`chemical_insufficient`、`claim_mismatch` 等），冲突详情可在
`GET /api/conflicts` 追溯。
