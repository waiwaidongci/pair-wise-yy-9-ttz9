// demo.js — 验证可恢复工序账的五条机制
// 运行：node demo.js
import {
  migrate,
  handoff,
  confirmHandoff,
  recover,
  updateBatch,
  recomputeConclusion,
  ensureFreshConclusion,
  currentBatch,
  opIdForKey,
  newId,
} from "./ledger.js";

let passed = 0;
let failed = 0;
function check(name, cond, extra = "") {
  if (cond) {
    passed += 1;
    console.log(`  PASS  ${name}`);
  } else {
    failed += 1;
    console.log(`  FAIL  ${name} ${extra}`);
  }
}

// 构造一个干净的内存 db
function makeDb() {
  return {
    schemaVersion: 1,
    batches: [
      { batch: "B-0620", concentration: 0.1, ratio: "1:2", version: 1, stock: 10, held: 0, consumed: 0 },
    ],
    stations: [
      { code: "WS-01", lockedBy: null },
      { code: "WS-02", lockedBy: null },
    ],
    items: [
      {
        id: "CN-001",
        code: "CN-001",
        plateSize: "18x24cm",
        chemicalBatch: "B-0620",
        exposure: "8分钟",
        waterSource: "井水过滤",
        box: "蓝盒A-03",
        status: "待曝光",
        logs: [],
      },
      {
        id: "CN-002",
        code: "CN-002",
        plateSize: "13x18cm",
        exposure: "6分钟",
        waterSource: "井水过滤",
        status: "待曝光",
        logs: [],
        // 故意没有 chemicalBatch，用于验证升级补来源
      },
    ],
    operations: [],
    idempotency: {},
  };
}

console.log("\n== 1. 升级补来源（旧数据无批次号） ==");
{
  const db = makeDb();
  const { events } = migrate(db);
  const legacy = db.items.find((i) => i.id === "CN-002");
  check("无批次号的底片被补 B-LEGACY", legacy.chemicalBatch === "B-LEGACY");
  check("补来源标记 batchSource=legacy-backfill", legacy.batchSource === "legacy-backfill");
  check("补来源日志写入", legacy.logs.some((l) => l.step === "补来源"));
  check("schemaVersion 升到 2", db.schemaVersion === 2);
  check("迁移事件包含 backfill", events.some((e) => e.type === "backfill" && e.negativeId === "CN-002"));
  check("历史履历 logs 仍可查（未被清空）", legacy.logs.length >= 1);
}

console.log("\n== 2. 工位锁定：同一工位同时只接一张底片 ==");
{
  const db = makeDb();
  migrate(db);
  const r1 = handoff(db, { negativeId: "CN-001", station: "WS-01", stage: "冲洗中", idempotencyKey: "k1" });
  check("第一张底片交接 reserved", r1.ok && r1.phase === "reserved");
  const r2 = handoff(db, { negativeId: "CN-002", station: "WS-01", stage: "冲洗中", idempotencyKey: "k2" });
  check("第二张底片同工位返回 409 station_conflict", !r2.ok && r2.status === 409 && r2.error === "station_conflict");
  check("冲突时工位仍被 CN-001 的 op 锁定", db.stations.find((s) => s.code === "WS-01").lockedBy === r1.op.opId);
  // 完成第一张后释放
  confirmHandoff(db, r1.op);
  const r3 = handoff(db, { negativeId: "CN-002", station: "WS-01", stage: "冲洗中", idempotencyKey: "k3" });
  check("第一张完成后工位释放，第二张可接入", r3.ok);
}

console.log("\n== 3. 药液预占与扣减：交接完成前锁定、完成后扣减 ==");
{
  const db = makeDb();
  migrate(db);
  const before = currentBatch(db, "B-0620");
  const stock0 = before.stock;
  const r = handoff(db, { negativeId: "CN-001", station: "WS-01", stage: "冲洗中", idempotencyKey: "k4", amount: 2 });
  check("reserved 后 stock 减少、held 增加", before.stock === stock0 - 2 && before.held === 2);
  check("工位被锁定", db.stations.find((s) => s.code === "WS-01").lockedBy === r.op.opId);
  confirmHandoff(db, r.op);
  check("confirmed 后 held 清零、consumed 增加、stock 不再变", before.held === 0 && before.consumed === 2 && before.stock === stock0 - 2);
  check("confirmed 后工位释放", db.stations.find((s) => s.code === "WS-01").lockedBy === null);
}

console.log("\n== 4. 幂等：同一 idempotencyKey 重试不重复扣减 ==");
{
  const db = makeDb();
  migrate(db);
  const r1 = handoff(db, { negativeId: "CN-001", station: "WS-01", stage: "冲洗中", idempotencyKey: "dup-1", amount: 3 });
  confirmHandoff(db, r1.op);
  const stockAfterFirst = currentBatch(db, "B-0620").stock;
  const consumedAfterFirst = currentBatch(db, "B-0620").consumed;
  // 用同一 key 重试（模拟断电后续作）
  const r2 = handoff(db, { negativeId: "CN-001", station: "WS-01", stage: "冲洗中", idempotencyKey: "dup-1", amount: 3 });
  check("重试回放 confirmed", r2.ok && r2.replay === true && r2.phase === "confirmed");
  check("重试不重复扣减 stock", currentBatch(db, "B-0620").stock === stockAfterFirst);
  check("重试不重复增加 consumed", currentBatch(db, "B-0620").consumed === consumedAfterFirst);
  check("opId 稳定（同一 key 映射同一 opId）", r2.op.opId === opIdForKey("dup-1"));
}

console.log("\n== 5. 断电恢复：reserved 操作从最后确认步骤恢复 ==");
{
  const db = makeDb();
  migrate(db);
  // 模拟：只持久化了 reserve（stock→held、工位锁定），进程崩溃，未 confirm
  const r = handoff(db, { negativeId: "CN-001", station: "WS-01", stage: "冲洗中", idempotencyKey: "crash-1", amount: 2 });
  check("崩溃前 op 为 reserved", r.op.state === "reserved");
  const stockAtCrash = currentBatch(db, "B-0620").stock;
  const heldAtCrash = currentBatch(db, "B-0620").held;
  const statusAtCrash = db.items.find((i) => i.id === "CN-001").status;
  check("崩溃时底片状态未推进", statusAtCrash === "待曝光");
  // 重启恢复
  const resumed = recover(db);
  check("recover 恢复了 1 个 reserved 操作", resumed.length === 1 && resumed[0].result === "confirmed");
  check("恢复后底片状态推进到 冲洗中", db.items.find(i => i.id === "CN-001").status === "冲洗中");
  check("恢复后 held→consumed（不重复扣减）", currentBatch(db, "B-0620").held === 0 && currentBatch(db, "B-0620").consumed === 2);
  check("恢复后 stock 与崩溃时一致（只扣一次）", currentBatch(db, "B-0620").stock === stockAtCrash);
  check("恢复后工位释放", db.stations.find(s => s.code === "WS-01").lockedBy === null);
  // 再次恢复：无 reserved 操作，不重复扣减
  const resumed2 = recover(db);
  check("再次恢复无操作、不重复扣减", resumed2.length === 0 && currentBatch(db, "B-0620").consumed === 2);
}

console.log("\n== 6. 版本失效：浓度/配比一变，结论与入盒许可立即失效重算 ==");
{
  const db = makeDb();
  migrate(db);
  const item = db.items.find((i) => i.id === "CN-001");
  const batch = currentBatch(db, "B-0620");
  // 初始结论（浓度 0.1、配比 1:2）应合格
  item.conclusion = recomputeConclusion(item, batch);
  check("初始曝光结论充足", item.conclusion.exposure === "充足");
  check("初始冲洗结论合格", item.conclusion.wash === "合格");
  check("初始入盒许可发放", item.conclusion.boxinPermit === true);
  check("结论基于版本 1", item.conclusion.basedOnBatchVersion === 1);

  // 改配比到 1:3 → 冲洗结论应不合格、入盒许可失效
  const r = updateBatch(db, "B-0620", { ratio: "1:3" });
  check("配比变更后 version++", r.batch.version === 2);
  check("失效操作计数 invalidated=1", r.invalidated === 1);
  check("结论被标记失效", item.conclusion.valid === false && item.conclusion.staleReason === "batch_changed");
  // 读时自动重算
  ensureFreshConclusion(item, db);
  check("重算后冲洗结论不合格", item.conclusion.wash === "不合格");
  check("重算后入盒许可失效", item.conclusion.boxinPermit === false);
  check("重算后基于版本 2", item.conclusion.basedOnBatchVersion === 2);

  // 尝试入盒 → 被拒（不预占）
  const stockBefore = batch.stock;
  const h = handoff(db, { negativeId: "CN-001", station: "WS-01", stage: "待入盒", idempotencyKey: "permit-1" });
  check("许可失效时入盒返回 permit_invalid", !h.ok && h.status === 409 && h.error === "permit_invalid");
  check("入盒被拒不预占药液", batch.stock === stockBefore && batch.held === 0);

  // 改回配比 1:2 → 许可恢复
  updateBatch(db, "B-0620", { ratio: "1:2" });
  ensureFreshConclusion(item, db);
  check("配比恢复后入盒许可重新发放", item.conclusion.boxinPermit === true);
}

console.log("\n== 7. 浓度变更同样触发失效 ==");
{
  const db = makeDb();
  migrate(db);
  const item = db.items.find((i) => i.id === "CN-001");
  const batch = currentBatch(db, "B-0620");
  item.conclusion = recomputeConclusion(item, batch);
  const r = updateBatch(db, "B-0620", { concentration: 0.01 });
  check("浓度变更后 version++", r.batch.version === 2);
  ensureFreshConclusion(item, db);
  check("浓度降低后曝光结论不足", item.conclusion.exposure === "不足");
  check("浓度降低后入盒许可失效", item.conclusion.boxinPermit === false);
}

console.log("\n== 8. 工位不存在 / 底片不存在 ==");
{
  const db = makeDb();
  migrate(db);
  const r1 = handoff(db, { negativeId: "CN-001", station: "WS-99", stage: "冲洗中", idempotencyKey: "e1" });
  check("工位不存在 404", !r1.ok && r1.status === 404 && r1.error === "station_not_found");
  const r2 = handoff(db, { negativeId: "CN-999", station: "WS-01", stage: "冲洗中", idempotencyKey: "e2" });
  check("底片不存在 404", !r2.ok && r2.status === 404 && r2.error === "negative_not_found");
  const r3 = handoff(db, { negativeId: "CN-001", station: "WS-01", stage: "冲洗中" });
  check("缺少幂等键 400", !r3.ok && r3.status === 400 && r3.error === "idempotency_key_required");
}

console.log("\n== 9. 库存不足：拒绝预占并释放工位 ==");
{
  const db = makeDb();
  migrate(db);
  const batch = currentBatch(db, "B-0620");
  batch.stock = 1;
  const r = handoff(db, { negativeId: "CN-001", station: "WS-01", stage: "冲洗中", idempotencyKey: "low-1", amount: 5 });
  check("库存不足 409 insufficient_chemicals", !r.ok && r.status === 409 && r.error === "insufficient_chemicals");
  check("库存不足时工位未锁定", db.stations.find(s => s.code === "WS-01").lockedBy === null);
  check("库存不足时 held 未增加", batch.held === 0);
}

console.log(`\n结果：${passed} 通过，${failed} 失败`);
process.exit(failed ? 1 : 0);
