// ledger.js — 可恢复工序账核心逻辑（无 I/O，纯函数操作 db 对象）
//
// 对应需求的五条机制：
//  1. 工位锁定：同一工位同时只接一张底片，后到请求返回冲突(409)。
//  2. 药液预占：交接完成前先预占药液(stock→held)并锁定工位；完成后扣减(held→consumed)。
//  3. 幂等与恢复：每次交接有唯一 idempotencyKey → 唯一 opId；
//     写入失败后从最后确认步骤(reserved 操作)恢复，重试不重复扣减。
//  4. 版本失效：药液批次有 version，浓度/配比一变 version++，
//     基于旧版本的曝光/冲洗结论与入盒许可立即失效并重算。
//  5. 升级补来源：旧数据无批次号时迁移补 batch 与 batchSource，历史履历保留可查。

export const STAGES = ["待曝光", "冲洗中", "待入盒", "已交付"];

export function newId(prefix) {
  return prefix + "-" + Date.now().toString(36) + "-" + Math.random().toString(36).slice(2, 8);
}

// 简单稳定哈希：把幂等键映射成确定的 opId，保证重试落到同一操作上
function hash(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) {
    h = (h << 5) - h + str.charCodeAt(i);
    h |= 0;
  }
  return (h >>> 0).toString(36);
}

export function opIdForKey(key) {
  return "op_" + hash(key);
}

export function now() {
  return new Date().toISOString();
}

// ---------- 5) 迁移：升级补来源 ----------
export function migrate(db) {
  const events = [];
  db.schemaVersion = db.schemaVersion ?? 1;
  db.batches ||= [];
  db.stations ||= [];
  db.operations ||= [];
  db.idempotency ||= {};

  if (db.schemaVersion < 2) {
    db.schemaVersion = 2;
    events.push({ type: "schema_upgrade", from: 1, to: 2 });
  }

  // 保证补来源用的占位批次存在
  let legacy = db.batches.find((b) => b.batch === "B-LEGACY");
  if (!legacy) {
    legacy = {
      batch: "B-LEGACY",
      concentration: 0.1,
      ratio: "1:1",
      version: 1,
      stock: 10,
      held: 0,
      consumed: 0,
      legacy: true,
      note: "历史数据补来源占位批次",
    };
    db.batches.push(legacy);
  }

  // 升级补来源：任何缺批次号的底片都补（与 schemaVersion 无关，兜底历史数据）
  for (const item of db.items || []) {
    if (!item.chemicalBatch) {
      item.chemicalBatch = "B-LEGACY";
      item.batchSource = "legacy-backfill";
      item.logs ||= [];
      item.logs.push({
        at: now(),
        step: "补来源",
        note: "历史数据无批次号，升级补来源 B-LEGACY（batchSource=legacy-backfill）",
      });
      events.push({ type: "backfill", negativeId: item.id, code: item.code, batch: "B-LEGACY" });
    }
  }
  return { db, events };
}

// ---------- 药液批次 ----------
export function currentBatch(db, code) {
  return db.batches.find((b) => b.batch === code) || null;
}

// 依据当前批次浓度/配比重算结论（曝光、冲洗、入盒许可）
export function recomputeConclusion(item, batch) {
  const expMin = parseInt(item.exposure, 10) || 0;
  const conc = batch?.concentration ?? 0;
  const ratio = batch?.ratio ?? "1:1";
  const [rNum, rDen] = ratio.split(":").map(Number);
  const ratioVal = rNum / (rDen || 1);
  // 曝光结论：曝光分钟数 × 浓度 达到阈值才算充足
  const exposureOk = expMin > 0 && conc > 0 && expMin * conc >= 0.5;
  // 冲洗结论：配比落在可用区间且浓度有效
  const washOk = conc > 0 && ratioVal >= 0.5 && ratioVal <= 2;
  // 入盒许可：曝光与冲洗结论都合格才发放
  const boxinPermit = exposureOk && washOk;
  return {
    exposure: exposureOk ? "充足" : "不足",
    wash: washOk ? "合格" : "不合格",
    boxinPermit,
    basedOnBatchVersion: batch?.version ?? 0,
    computedAt: now(),
    valid: true,
  };
}

// 读时保证结论新鲜：批次版本一变就重算
export function ensureFreshConclusion(item, db) {
  const batch = currentBatch(db, item.chemicalBatch);
  const curVer = batch?.version ?? 0;
  if (!item.conclusion || !item.conclusion.valid || item.conclusion.basedOnBatchVersion !== curVer) {
    item.conclusion = recomputeConclusion(item, batch);
  }
  return item;
}

// 批次浓度/配比变更后，立即把引用该批次的未完成结论置为失效
export function invalidateForBatch(db, batchCode) {
  let n = 0;
  for (const item of db.items || []) {
    if (item.chemicalBatch === batchCode && item.conclusion && item.status !== "已交付") {
      item.conclusion.valid = false;
      item.conclusion.staleReason = "batch_changed";
      item.conclusion.invalidatedAt = now();
      n += 1;
    }
  }
  return n;
}

// 更新批次浓度/配比（version++ 触发失效重算）
export function updateBatch(db, code, patch) {
  const batch = currentBatch(db, code);
  if (!batch) return { ok: false, status: 404, error: "batch_not_found" };
  let changed = false;
  if (patch.concentration !== undefined && patch.concentration !== batch.concentration) {
    batch.concentration = patch.concentration;
    changed = true;
  }
  if (patch.ratio !== undefined && patch.ratio !== batch.ratio) {
    batch.ratio = patch.ratio;
    changed = true;
  }
  if (changed) {
    batch.version += 1;
    batch.updatedAt = now();
    if (patch.note) batch.note = patch.note;
    const invalidated = invalidateForBatch(db, batch.batch);
    return { ok: true, status: 200, batch, invalidated };
  }
  return { ok: true, status: 200, batch, invalidated: 0 };
}

// ---------- 工位 ----------
export function findStation(db, code) {
  return db.stations.find((s) => s.code === code) || null;
}

// 条件锁：工位空闲或已被本 op 锁定时才放行
export function acquireStation(db, stationCode, opId) {
  const station = findStation(db, stationCode);
  if (!station) return { ok: false, status: 404, error: "station_not_found" };
  if (station.lockedBy && station.lockedBy !== opId) {
    return { ok: false, status: 409, error: "station_conflict", heldBy: station.lockedBy };
  }
  station.lockedBy = opId;
  station.lockedAt = now();
  return { ok: true, station };
}

export function releaseStation(db, stationCode, opId) {
  const station = findStation(db, stationCode);
  if (station && station.lockedBy === opId) {
    station.lockedBy = null;
    station.releasedAt = now();
  }
}

// ---------- 工序账 / 交接 ----------
// 交接：锁定工位并预占药液（reserved），由调用方先持久化一次再确认。
// 幂等：同一 idempotencyKey 重试不会重复预占/扣减。
export function handoff(db, input) {
  const idempotencyKey = input.idempotencyKey || input.opKey || null;
  if (!idempotencyKey) return { ok: false, status: 400, error: "idempotency_key_required" };

  const opId = opIdForKey(idempotencyKey);

  // 1) 幂等回放：该键已处理过 → 不重复执行
  if (db.idempotency[idempotencyKey]) {
    const op = db.operations.find((o) => o.opId === opId);
    if (op) {
      if (op.state === "confirmed") {
        return { ok: true, status: 200, replay: true, phase: "confirmed", op, item: op._item || null };
      }
      if (op.state === "reserved") {
        // 崩溃恢复：reserved 操作继续确认
        const c = confirmHandoff(db, op, input);
        return { ...c, replay: true, phase: "recovered" };
      }
      if (op.state === "aborted") {
        return { ok: false, status: 409, replay: true, op, error: op.error || "op_aborted" };
      }
    }
  }

  const item = db.items.find((i) => i.id === input.negativeId || i.code === input.negativeId);
  if (!item) return { ok: false, status: 404, error: "negative_not_found" };

  const stationCode = input.station;
  const stage = input.stage;
  if (!stage || !STAGES.includes(stage)) return { ok: false, status: 400, error: "invalid_stage" };
  if (!stationCode) return { ok: false, status: 400, error: "station_required" };

  const batch = currentBatch(db, item.chemicalBatch);
  if (!batch) return { ok: false, status: 409, error: "batch_missing" };

  // 入盒许可：入盒前先按当前批次版本重算，许可不合格则拒绝（不预占）
  if (stage === "待入盒") {
    ensureFreshConclusion(item, db);
    if (!item.conclusion.boxinPermit) {
      return {
        ok: false,
        status: 409,
        error: "permit_invalid",
        reason: item.conclusion,
        item,
      };
    }
  }

  // 2) 锁定工位（条件锁）
  const lock = acquireStation(db, stationCode, opId);
  if (!lock.ok) return { ...lock, item };

  // 3) 预占药液：库存不足则放弃并释放工位
  const amount = Number(input.amount) || 1;
  if (batch.stock < amount) {
    releaseStation(db, stationCode, opId);
    return {
      ok: false,
      status: 409,
      error: "insufficient_chemicals",
      batch: batch.batch,
      stock: batch.stock,
      need: amount,
    };
  }

  // 4) 写操作记录(reserved)并预占 —— 第一次持久化的内容
  const op = {
    opId,
    idempotencyKey,
    negativeId: item.id,
    station: stationCode,
    batch: batch.batch,
    stage,
    amount,
    state: "reserved",
    reservation: { batch: batch.batch, amount, status: "held" },
    createdAt: now(),
  };
  batch.stock -= amount;
  batch.held += amount;
  db.operations.push(op);
  db.idempotency[idempotencyKey] = opId;

  return { ok: true, status: 201, phase: "reserved", op, item };
}

// 确认交接：推进底片阶段、重算结论、药液 held→consumed、释放工位
export function confirmHandoff(db, op, input = {}) {
  if (op.state === "confirmed") {
    return { ok: true, status: 200, replay: true, op, item: op._item || null };
  }
  if (op.state !== "reserved") return { ok: false, status: 409, op, error: "op_not_reserved" };

  const item = db.items.find((i) => i.id === op.negativeId);
  if (!item) return abortOp(db, op, "negative_missing");

  // 幂等推进：底片已到该阶段则不重复推进
  if (item.status !== op.stage) item.status = op.stage;
  if (input.defect) item.defect = input.defect;
  if (input.note) item.lastNote = input.note;

  // 重算结论（基于当前批次版本）
  const batch = currentBatch(db, op.batch);
  item.conclusion = recomputeConclusion(item, batch);

  item.logs ||= [];
  item.logs.push({ at: now(), step: op.stage, note: input.note || `交接至${op.stage}` });
  item.steps ||= [];
  item.steps.push({ at: now(), station: op.station, stage: op.stage, opId: op.opId, note: input.note || "" });

  // 药液 held→consumed（只在 reserved→confirmed 时发生一次）
  if (op.reservation.status === "held" && batch) {
    batch.held -= op.amount;
    batch.consumed += op.amount;
    op.reservation.status = "consumed";
  }

  // 释放工位
  releaseStation(db, op.station, op.opId);

  op.state = "confirmed";
  op.confirmedAt = now();
  op._item = item;
  return { ok: true, status: 200, op, item };
}

export function abortOp(db, op, error) {
  if (op.state === "confirmed") return { ok: true, status: 200, replay: true, op };
  const batch = currentBatch(db, op.batch);
  if (op.reservation.status === "held" && batch) {
    batch.held -= op.amount;
    batch.stock += op.amount;
    op.reservation.status = "released";
  }
  releaseStation(db, op.station, op.opId);
  op.state = "aborted";
  op.error = error;
  op.abortedAt = now();
  return { ok: false, status: 409, op, error };
}

// 崩溃恢复：把所有 reserved 操作继续推进到 confirmed
export function recover(db) {
  const resumed = [];
  for (const op of db.operations || []) {
    if (op.state === "reserved") {
      const r = confirmHandoff(db, op);
      resumed.push({ opId: op.opId, result: r.ok ? "confirmed" : r.error });
    }
  }
  return resumed;
}

// ---------- 查询 ----------
export function listItems(db) {
  for (const item of db.items || []) ensureFreshConclusion(item, db);
  return db.items;
}

export function getItem(db, idOrCode) {
  const item = (db.items || []).find((i) => i.id === idOrCode || i.code === idOrCode);
  if (item) ensureFreshConclusion(item, db);
  return item;
}

export function listOperations(db) {
  return db.operations || [];
}
