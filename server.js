import http from "node:http";
import { mkdir, readFile, rename, appendFile, open } from "node:fs/promises";
import { existsSync, createReadStream } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";

const __dirname = dirname(fileURLToPath(import.meta.url));
const dataDir = join(__dirname, "data");
const dbPath = join(dataDir, "cyanotype-negative-room.json");
const legacyPath = join(dataDir, "cyanotype-negative-room.legacy.json");
const ledgerPath = join(dataDir, "cyanotype-ledger.jsonl");
const port = Number(process.env.PORT || 3040);

// 工序顺序：涂布 → 晾干 → 曝光 → 冲洗 →（复晒，可回环）→ 入盒 → 交付
const STEPS = ["涂布", "晾干", "曝光", "冲洗", "复晒", "入盒", "交付"];
const CHEMICAL_STEPS = ["涂布", "曝光", "冲洗"]; // 这些工序确认时要扣药液
const STATION_STEPS = ["涂布", "晾干", "曝光", "冲洗", "复晒", "入盒"]; // 交付不占工位
const stages = ["待曝光", "冲洗中", "待入盒", "已交付"];

const now = () => new Date().toISOString();
function newCode(seq) {
  const y = new Date().getFullYear();
  return `CN-${y}-${String(seq).padStart(4, "0")}`;
}

/* ---------------- 互斥：同一进程内所有写操作串行 ---------------- */
let chain = Promise.resolve();
function withLock(task) {
  const run = chain.then(() => task());
  chain = run.catch(() => {});
  return run;
}

/* ---------------- 仅追加工序账（WAL） ---------------- */
async function appendEvent(event) {
  await mkdir(dataDir, { recursive: true });
  const line = JSON.stringify(event) + "\n";
  // O_APPEND 保证多条记录不会交错；sync 落盘后才向调用方报成功
  const fh = await open(ledgerPath, "a");
  try {
    await fh.appendFile(line);
    await fh.sync();
  } finally {
    await fh.close();
  }
  return event;
}

/* ---------------- 事件 → 状态折叠 ---------------- */
function emptyState() {
  return {
    seq: 0,
    negatives: new Map(), // code -> negative
    batches: new Map(),   // batchNo -> batch
    stations: new Map(),  // station -> station record
    idem: new Map()       // idemKey -> 首次结果
  };
}

function applyEvent(state, e) {
  state.seq = e.seq;
  switch (e.type) {
    case "BATCH_REGISTERED": {
      state.batches.set(e.batchNo, {
        batchNo: e.batchNo,
        formulaVersion: e.formulaVersion,
        concentration: e.concentration,
        ratio: e.ratio,
        sourceNote: e.sourceNote,
        stockMl: e.stockMl,
        reservedMl: 0,
        usedMl: 0,
        createdAt: e.at
      });
      break;
    }
    case "NEGATIVE_CREATED": {
      state.negatives.set(e.code, {
        code: e.code,
        plateSize: e.plateSize || "",
        waterSource: e.waterSource || "",
        box: "",
        defaultBatch: e.chemicalBatch || "",
        createdAt: e.at,
        steps: [],          // 已确认工序（含 legacy）
        openClaim: null,    // 进行中未确认的占用
        history: []         // 底片级事件摘要（交付/入盒/失效等）
      });
      break;
    }
    case "STATION_CLAIMED": {
      const neg = state.negatives.get(e.code);
      const st = state.stations.get(e.station);
      const batch = state.batches.get(e.batchNo);
      neg.openClaim = {
        claimId: e.claimId,
        station: e.station,
        step: e.step,
        batchNo: e.batchNo,
        formulaVersion: e.formulaVersion,
        reserveMl: e.reserveMl,
        at: e.at
      };
      st.lockedBy = e.code;
      st.lockedSince = e.at;
      st.lockedStep = e.step;
      st.claimId = e.claimId;
      if (e.reserveMl > 0) batch.reservedMl += e.reserveMl;
      break;
    }
    case "STEP_COMMITTED": {
      const neg = state.negatives.get(e.code);
      const st = state.stations.get(neg.openClaim.station);
      const batch = state.batches.get(neg.openClaim.batchNo);
      if (neg.openClaim.reserveMl > 0) {
        batch.reservedMl -= neg.openClaim.reserveMl;
        batch.usedMl += e.usedMl;
      }
      st.lockedBy = null;
      st.lockedSince = null;
      st.lockedStep = null;
      st.claimId = null;
      neg.steps.push({
        step: neg.openClaim.step,
        station: neg.openClaim.station,
        batchNo: neg.openClaim.batchNo,
        formulaVersion: neg.openClaim.formulaVersion,
        usedMl: e.usedMl,
        developStatus: e.developStatus || "",
        defect: e.defect || "",
        repair: e.repair || "",
        note: e.note || "",
        at: e.at,
        legacy: false
      });
      neg.openClaim = null;
      break;
    }
    case "STEP_ABORTED": {
      const neg = state.negatives.get(e.code);
      if (neg.openClaim) {
        const st = state.stations.get(neg.openClaim.station);
        const batch = state.batches.get(neg.openClaim.batchNo);
        if (neg.openClaim.reserveMl > 0) batch.reservedMl -= neg.openClaim.reserveMl;
        st.lockedBy = null;
        st.lockedSince = null;
        st.lockedStep = null;
        st.claimId = null;
        neg.history.push({ at: e.at, kind: "abort", step: neg.openClaim.step, note: e.reason || "中止占用" });
        neg.openClaim = null;
      }
      break;
    }
    case "BOXED": {
      const neg = state.negatives.get(e.code);
      neg.box = e.box;
      neg.steps.push({
        step: "入盒",
        station: e.station || "",
        batchNo: "",
        formulaVersion: 0,
        usedMl: 0,
        developStatus: "",
        defect: "",
        repair: "",
        note: e.note || "",
        at: e.at,
        legacy: false
      });
      break;
    }
    case "DELIVERED": {
      const neg = state.negatives.get(e.code);
      neg.steps.push({
        step: "交付",
        station: "",
        batchNo: "",
        formulaVersion: 0,
        usedMl: 0,
        developStatus: "",
        defect: "",
        repair: "",
        note: e.note || "入盒交付完成",
        at: e.at,
        legacy: false
      });
      neg.history.push({ at: e.at, kind: "delivered", box: neg.box });
      break;
    }
    case "FORMULA_CHANGED": {
      const batch = state.batches.get(e.batchNo);
      batch.formulaVersion = e.newVersion;
      batch.concentration = e.concentration;
      batch.ratio = e.ratio;
      for (const code of e.affected) {
        const neg = state.negatives.get(code);
        if (neg) neg.history.push({
          at: e.at,
          kind: "invalidate",
          batchNo: e.batchNo,
          fromVersion: e.oldVersion,
          toVersion: e.newVersion,
          note: "药液浓度/配比变更，曝光、冲洗结论与入盒许可失效，需重算"
        });
      }
      break;
    }
    case "CONFLICT":
      // 冲突只入账，不改变状态
      break;
    case "LEGACY_MIGRATION": {
      for (const neg of e.negatives) {
        state.negatives.set(neg.code, {
          ...neg,
          openClaim: null,
          history: [{ at: e.at, kind: "legacy", note: e.note }]
        });
      }
      break;
    }
  }
  if (e.idemKey) {
    state.idem.set(e.idemKey, { type: e.type, result: e.result });
  }
}

/* ---------------- 启动迁移：旧版单文件数据升级 ---------------- */
async function migrateLegacy(state) {
  if (!existsSync(dbPath)) return { migrated: 0 };
  const raw = JSON.parse(await readFile(dbPath, "utf8"));
  const items = Array.isArray(raw.items) ? raw.items : [];
  let seq = 0;
  const next = (type, payload) => ({ seq: ++seq, at: now(), type, ...payload });
  const events = [];

  // 旧数据没有批次号 → 升级补来源批次
  const legacyBatchNo = "LEGACY-0001";
  const usedBatches = new Set();
  for (const it of items) {
    const b = (it.chemicalBatch || "").trim();
    if (b && b !== "无" && b !== "-") usedBatches.add(b);
  }
  const register = (batchNo, concentration, ratio, sourceNote, stockMl) =>
    events.push(next("BATCH_REGISTERED", {
      batchNo, formulaVersion: 1, concentration, ratio, sourceNote, stockMl
    }));
  register("B-0620", "20%", "柠檬酸铁铵:铁氰化钾=1:1", "初始建档批次", 2000);
  for (const b of usedBatches) {
    if (!state.batches.has(b) && b !== "B-0620") {
      register(b, "旧档未记录", "旧档未记录", "历史底片沿用批次，浓度配比待补录", 0);
    }
  }
  let needLegacyBatch = false;
  for (const it of items) {
    const b = (it.chemicalBatch || "").trim();
    if (!b || b === "无" || b === "-") { needLegacyBatch = true; break; }
  }
  if (needLegacyBatch) {
    register(legacyBatchNo, "旧档未记录", "旧档未记录", "旧数据缺少批次号，迁移时补登的来源批次", 0);
  }

  const negatives = [];
  for (const it of items) {
    const code = it.code || it.id || `CN-OLD-${seq}`;
    const batchNo = (it.chemicalBatch || "").trim() && it.chemicalBatch !== "无" && it.chemicalBatch !== "-"
      ? it.chemicalBatch
      : legacyBatchNo;
    events.push(next("NEGATIVE_CREATED", {
      code,
      plateSize: it.plateSize || "",
      chemicalBatch: batchNo,
      waterSource: it.waterSource || "",
      legacy: true,
      result: { code }
    }));

    const steps = [];
    const seen = new Set();
    const pushStep = (s) => {
      const key = s.step + "|" + s.at;
      if (seen.has(key)) return;
      seen.add(key);
      steps.push(s);
    };
    // 步骤明细优先，其次用 logs 补齐
    for (const s of it.steps || []) {
      const step = STEPS.includes(s.step) ? s.step : null;
      if (step) pushStep({
        step, station: s.station || "旧档未记录", batchNo,
        formulaVersion: 1, usedMl: 0,
        developStatus: s.developStatus || "", defect: s.defect || "",
        repair: s.repair || "", note: s.note || "", at: s.at, legacy: true
      });
    }
    for (const l of it.logs || []) {
      const step = STEPS.includes(l.step) ? l.step : null;
      if (step) pushStep({
        step, station: "旧档未记录", batchNo,
        formulaVersion: 1, usedMl: 0,
        developStatus: "", defect: step === "曝光" ? (it.defect || "") : "",
        repair: "", note: l.note || "", at: l.at, legacy: true
      });
    }
    // 旧状态语义补齐，保证历史履历完整
    const has = (name) => steps.some(s => s.step === name);
    if (!has("曝光") && (it.status === "冲洗中" || it.status === "待入盒" || it.status === "已交付")) {
      steps.push({ step: "曝光", station: "旧档未记录", batchNo, formulaVersion: 1, usedMl: 0, developStatus: "", defect: it.defect || "", repair: "", note: "按旧状态补登", at: "2026-06-20", legacy: true });
    }
    if (!has("冲洗") && (it.status === "待入盒" || it.status === "已交付")) {
      steps.push({ step: "冲洗", station: "旧档未记录", batchNo, formulaVersion: 1, usedMl: 0, developStatus: "", defect: it.defect || "", repair: "", note: "按旧状态补登", at: "2026-06-21", legacy: true });
    }
    steps.sort((a, b) => String(a.at).localeCompare(String(b.at)));

    let box = it.box || "";
    if (it.status === "待入盒" || it.status === "已交付") {
      if (!has("入盒")) steps.push({ step: "入盒", station: "旧档未记录", batchNo: "", formulaVersion: 0, usedMl: 0, developStatus: "", defect: "", repair: "", note: "按旧状态补登", at: "2026-06-21", legacy: true });
      box = it.box || "旧档盒位";
    }
    if (it.status === "已交付" && !has("交付")) {
      steps.push({ step: "交付", station: "", batchNo: "", formulaVersion: 0, usedMl: 0, developStatus: "", defect: "", repair: "", note: "按旧状态补登", at: "2026-06-22", legacy: true });
    }

    negatives.push({
      code, plateSize: it.plateSize || "", waterSource: it.waterSource || "",
      box, defaultBatch: batchNo, createdAt: events[0]?.at || now(), steps, history: []
    });
  }

  events.push(next("LEGACY_MIGRATION", {
    negatives,
    note: `旧版记录迁移：共 ${negatives.length} 张底片；缺批次号的补登 ${legacyBatchNo}，原文件归档为 cyanotype-negative-room.legacy.json`
  }));

  for (const e of events) {
    await appendEvent(e);
    applyEvent(state, e);
  }
  await rename(dbPath, legacyPath);
  return { migrated: negatives.length };
}

/* ---------------- 首启动种子：工位、批次 ---------------- */
async function seedIfEmpty(state) {
  if (existsSync(dbPath)) return; // 交给迁移
  if (existsSync(ledgerPath)) return;
  const stations = [
    { station: "T-01", kind: "涂布晾干工位" },
    { station: "X-01", kind: "曝光工位" },
    { station: "W-01", kind: "冲洗工位" },
    { station: "W-02", kind: "冲洗工位" },
    { station: "B-01", kind: "入盒工位" }
  ];
  let seq = 0;
  const ev = (type, payload) => ({ seq: ++seq, at: now(), type, ...payload });
  const events = [
    ev("BATCH_REGISTERED", { batchNo: "B-0620", formulaVersion: 1, concentration: "20%", ratio: "柠檬酸铁铵:铁氰化钾=1:1", sourceNote: "初始建档批次", stockMl: 2000 }),
    ...stations.flatMap(() => []) // 工位在 state 里预置，不占事件
  ];
  for (const e of events) {
    await appendEvent(e);
    applyEvent(state, e);
  }
}

/* ---------------- 状态装载与崩溃恢复 ---------------- */
async function loadState() {
  await mkdir(dataDir, { recursive: true });
  const state = emptyState();
  for (const s of ["T-01", "X-01", "W-01", "W-02", "B-01"]) {
    state.stations.set(s, {
      station: s,
      kind: { "T-01": "涂布晾干工位", "X-01": "曝光工位", "W-01": "冲洗工位", "W-02": "冲洗工位", "B-01": "入盒工位" }[s],
      lockedBy: null, lockedSince: null, lockedStep: null, claimId: null
    });
  }

  if (existsSync(ledgerPath)) {
    const raw = await readFile(ledgerPath, "utf8");
    for (const line of raw.split("\n")) {
      const t = line.trim();
      if (t) applyEvent(state, JSON.parse(t));
    }
  }

  let migration = null;
  if (existsSync(dbPath)) migration = await migrateLegacy(state);
  if (!existsSync(ledgerPath) && !migration) await seedIfEmpty(state);

  // 崩溃恢复扫描：重放后仍挂着的 openClaim 就是“已占未确认”
  const openClaims = [];
  for (const neg of state.negatives.values()) {
    if (neg.openClaim) {
      openClaims.push({
        code: neg.code,
        claimId: neg.openClaim.claimId,
        station: neg.openClaim.station,
        step: neg.openClaim.step,
        batchNo: neg.openClaim.batchNo,
        reserveMl: neg.openClaim.reserveMl,
        since: neg.openClaim.at
      });
    }
  }
  return { state, recovery: { openClaims, migrated: migration?.migrated ?? null } };
}

/* ---------------- 派生结论：配方版本一变立即失效重算 ---------------- */
function deriveNegative(neg, state) {
  const committed = neg.steps.filter(s => !s.step.startsWith("__"));
  const delivered = committed.some(s => s.step === "交付");
  const boxed = committed.some(s => s.step === "入盒");

  // 各结论锚定的配方版本：该工序确认时所用批次的版本
  const findStep = (name) => {
    const all = committed.filter(s => s.step === name && s.batchNo);
    return all[all.length - 1] || null;
  };
  const exposureStep = findStep("曝光");
  const washStep = findStep("冲洗");
  const batchVer = (no) => state.batches.get(no)?.formulaVersion ?? 1;

  const exposure = !exposureStep ? null : {
    valid: batchVer(exposureStep.batchNo) === exposureStep.formulaVersion,
    at: exposureStep.at,
    batchNo: exposureStep.batchNo,
    formulaVersion: exposureStep.formulaVersion,
    currentVersion: batchVer(exposureStep.batchNo),
    note: exposureStep.note,
    defect: exposureStep.defect
  };
  const wash = !washStep ? null : {
    valid: batchVer(washStep.batchNo) === washStep.formulaVersion,
    at: washStep.at,
    batchNo: washStep.batchNo,
    formulaVersion: washStep.formulaVersion,
    currentVersion: batchVer(washStep.batchNo),
    developStatus: washStep.developStatus,
    defect: washStep.defect
  };

  // 入盒许可：已入盒/已交付且上游仍有效才放行；未交付时上游失效，许可立即失效
  let boxPermit = null;
  if (boxed) {
    const valid = !!exposure?.valid && !!wash?.valid;
    boxPermit = {
      valid: valid || delivered, // 已交付的封档，不再翻旧账
      box: neg.box,
      reason: valid ? null : "药液配方已变更，曝光或冲洗结论失效，入盒许可暂停，需重算后重新入盒"
    };
  } else if (exposure?.valid && wash?.valid) {
    boxPermit = { valid: true, box: null, reason: null };
  } else if (exposure || wash) {
    boxPermit = { valid: false, box: null, reason: "曝光或冲洗结论已失效，重算完成前不得入盒" };
  }

  let status;
  if (delivered) status = "已交付";
  else if (boxed) status = "待入盒";
  else if (committed.some(s => s.step === "冲洗")) status = "待入盒";
  else if (committed.some(s => ["曝光", "晾干", "涂布"].includes(s.step))) status = "冲洗中";
  else status = "待曝光";

  const lastConfirmedStep = committed.length ? committed[committed.length - 1].step : null;
  const invalidations = neg.history.filter(h => h.kind === "invalidate");

  return {
    code: neg.code,
    plateSize: neg.plateSize,
    waterSource: neg.waterSource,
    box: neg.box,
    defaultBatch: neg.defaultBatch,
    status,
    lastConfirmedStep,
    openClaim: neg.openClaim,
    exposure,
    wash,
    boxPermit,
    invalidations,
    steps: committed,
    history: neg.history
  };
}

/* ---------------- HTTP 小工具 ---------------- */
async function body(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  return chunks.length ? JSON.parse(Buffer.concat(chunks).toString("utf8")) : {};
}
function send(res, status, data) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(data, null, 2));
}
function html(res, text) {
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(text);
}
class ApiError extends Error {
  constructor(status, code, detail) {
    super(code);
    this.status = status;
    this.code = code;
    this.detail = detail;
  }
}

/* ---------------- 主流程：占用 / 确认 / 中止 ---------------- */
async function commit(state, type, payload) {
  const event = { seq: state.seq + 1, at: now(), type, ...payload };
  await appendEvent(event);
  applyEvent(state, event);
  return event;
}

async function claimStation(state, input) {
  const { code, step, station, batchNo, idemKey } = input;
  if (idempotentHit(state, idemKey, "STATION_CLAIMED")) return state.idem.get(idemKey).result;
  const neg = state.negatives.get(code);
  if (!neg) throw new ApiError(404, "negative_not_found", code);
  if (!STEPS.includes(step)) throw new ApiError(400, "bad_step", step);
  if (!STATION_STEPS.includes(step)) throw new ApiError(400, "step_needs_no_station", step);
  if (neg.steps.some(s => s.step === "交付")) throw new ApiError(409, "already_delivered", code);

  // 该底片同时只能占一个工位
  if (neg.openClaim) {
    await recordConflict(state, {
      code, step, station: station || "",
      reason: "negative_already_claimed",
      detail: { openClaimId: neg.openClaim.claimId, openStation: neg.openClaim.station, openStep: neg.openClaim.step },
      at: now()
    });
    throw new ApiError(409, "negative_already_claimed", neg.openClaim);
  }

  // 工序顺序：复晒允许回环；入盒只许可有效时进入
  const order = ["涂布", "晾干", "曝光", "冲洗", "复晒", "入盒"];
  if (step !== "复晒") {
    const derived = deriveNegative(neg, state);
    if (derived.exposure && !derived.exposure.valid && step !== "曝光" && step !== "冲洗" && step !== "复晒") {
      throw new ApiError(409, "conclusion_invalid", "曝光结论已失效，必须先重算曝光/冲洗");
    }
    if (step === "入盒" && derived.boxPermit && !derived.boxPermit.valid) {
      throw new ApiError(409, "box_permit_invalid", derived.boxPermit.reason);
    }
    const have = new Set(neg.steps.map(s => s.step));
    const idx = order.indexOf(step);
    const prereq = order.slice(0, idx).filter(s => s !== "复晒");
    const missing = prereq.filter(s => !have.has(s));
    if (missing.length) throw new ApiError(409, "step_out_of_order", { missing });
  }

  const st = state.stations.get(station);
  if (!st) throw new ApiError(404, "station_not_found", station);
  // 同一工位同时只接一张底片，后到留冲突
  if (st.lockedBy) {
    const conflict = await recordConflict(state, {
      code, step, station,
      reason: "station_busy",
      detail: { lockedBy: st.lockedBy, lockedStep: st.lockedStep, since: st.lockedSince },
      at: now()
    });
    throw new ApiError(409, "station_busy", {
      station,
      lockedBy: st.lockedBy,
      lockedStep: st.lockedStep,
      conflictSeq: conflict.seq
    });
  }

  const useBatch = CHEMICAL_STEPS.includes(step);
  const bNo = useBatch ? (batchNo || neg.defaultBatch) : "";
  const batch = bNo ? state.batches.get(bNo) : null;
  if (useBatch && !batch) throw new ApiError(404, "batch_not_found", bNo);

  // 预占药量（先冻结，不扣减；确认时才扣）
  const reserveMl = useBatch
    ? { "涂布": 8, "曝光": 0, "冲洗": 15 }[step]
    : 0;
  if (batch && reserveMl > 0 && batch.stockMl - batch.reservedMl - batch.usedMl < reserveMl) {
    throw new ApiError(409, "chemical_insufficient", {
      batchNo: bNo, availableMl: batch.stockMl - batch.reservedMl - batch.usedMl, needMl: reserveMl
    });
  }

  const claimId = randomUUID();
  const result = {
    code, claimId, station, step,
    batchNo: bNo,
    formulaVersion: batch?.formulaVersion ?? 0,
    reserveMl,
    resume: `占用已建立，请确认“${step}”；写盘若失败，凭 claimId 从本步恢复，不会重复扣药`
  };
  await commit(state, "STATION_CLAIMED", {
    code, claimId, station, step,
    batchNo: bNo,
    formulaVersion: batch?.formulaVersion ?? 0,
    reserveMl,
    idemKey,
    result
  });
  return result;
}

async function commitStep(state, input) {
  const { code, claimId, idemKey, usedMl, developStatus, defect, repair, note } = input;
  if (idempotentHit(state, idemKey, "STEP_COMMITTED")) {
    return { ...state.idem.get(idemKey).result, replayed: true };
  }
  const neg = state.negatives.get(code);
  if (!neg) throw new ApiError(404, "negative_not_found", code);
  if (!neg.openClaim) throw new ApiError(409, "no_open_claim", { code, lastConfirmedStep: neg.steps.at(-1)?.step || null });
  if (claimId && claimId !== neg.openClaim.claimId) {
    throw new ApiError(409, "claim_mismatch", { currentClaimId: neg.openClaim.claimId });
  }

  // 扣减只在此处发生一次；预占量是上限，实扣可小于预占
  const actualUsed = usedMl == null ? neg.openClaim.reserveMl : Math.max(0, Number(usedMl) || 0);
  const batch = state.batches.get(neg.openClaim.batchNo);
  if (batch && actualUsed > neg.openClaim.reserveMl) {
    throw new ApiError(400, "used_exceeds_reservation", { reserveMl: neg.openClaim.reserveMl, usedMl: actualUsed });
  }

  const result = {
    code,
    step: neg.openClaim.step,
    station: neg.openClaim.station,
    batchNo: neg.openClaim.batchNo,
    deductedMl: actualUsed,
    confirmedAt: now()
  };
  await commit(state, "STEP_COMMITTED", {
    code,
    usedMl: actualUsed,
    developStatus: developStatus || "",
    defect: defect || "",
    repair: repair || "",
    note: note || "",
    idemKey,
    result
  });
  return result;
}

async function abortClaim(state, input) {
  const { code, claimId, reason } = input;
  const neg = state.negatives.get(code);
  if (!neg) throw new ApiError(404, "negative_not_found", code);
  if (!neg.openClaim) throw new ApiError(409, "no_open_claim", code);
  if (claimId && claimId !== neg.openClaim.claimId) {
    throw new ApiError(409, "claim_mismatch", { currentClaimId: neg.openClaim.claimId });
  }
  const result = { code, releasedStation: neg.openClaim.station, releasedReserveMl: neg.openClaim.reserveMl };
  await commit(state, "STEP_ABORTED", { code, reason: reason || "人工中止" });
  return result;
}

async function boxNegative(state, input) {
  const { code, box, idemKey, note } = input;
  if (idempotentHit(state, idemKey, "BOXED")) return state.idem.get(idemKey).result;
  const neg = state.negatives.get(code);
  if (!neg) throw new ApiError(404, "negative_not_found", code);
  const derived = deriveNegative(neg, state);
  if (derived.status === "已交付") throw new ApiError(409, "already_delivered", code);
  if (!derived.boxPermit || !derived.boxPermit.valid) {
    throw new ApiError(409, "box_permit_invalid", derived.boxPermit?.reason || "曝光/冲洗未完成，不得入盒");
  }
  if (neg.openClaim) throw new ApiError(409, "negative_already_claimed", neg.openClaim);
  if (!box) throw new ApiError(400, "box_required");
  const result = { code, box, at: now() };
  await commit(state, "BOXED", { code, box, station: "B-01", note: note || "", idemKey, result });
  return result;
}

async function deliverNegative(state, input) {
  const { code, idemKey, note } = input;
  if (idempotentHit(state, idemKey, "DELIVERED")) return state.idem.get(idemKey).result;
  const neg = state.negatives.get(code);
  if (!neg) throw new ApiError(404, "negative_not_found", code);
  const derived = deriveNegative(neg, state);
  if (derived.status === "已交付") return { code, delivered: true, replayed: true, at: neg.steps.at(-1)?.at };
  if (!derived.boxPermit?.valid || !derived.boxPermit.box) {
    throw new ApiError(409, "box_permit_invalid", "入盒许可无效或尚未入盒，不能交付");
  }
  const result = { code, delivered: true, box: neg.box, at: now() };
  await commit(state, "DELIVERED", { code, note: note || "", idemKey, result });
  return result;
}

async function registerBatch(state, input) {
  const { batchNo, concentration, ratio, sourceNote, stockMl, idemKey } = input;
  if (idempotentHit(state, idemKey, "BATCH_REGISTERED")) return state.idem.get(idemKey).result;
  if (!batchNo) throw new ApiError(400, "batch_no_required");
  if (state.batches.has(batchNo)) throw new ApiError(409, "batch_exists", batchNo);
  const result = { batchNo, formulaVersion: 1 };
  await commit(state, "BATCH_REGISTERED", {
    batchNo, formulaVersion: 1,
    concentration: concentration || "",
    ratio: ratio || "",
    sourceNote: sourceNote || "",
    stockMl: Number(stockMl) || 0,
    idemKey, result
  });
  return result;
}

async function changeFormula(state, input) {
  const { batchNo, concentration, ratio, idemKey } = input;
  if (idempotentHit(state, idemKey, "FORMULA_CHANGED")) return state.idem.get(idemKey).result;
  const batch = state.batches.get(batchNo);
  if (!batch) throw new ApiError(404, "batch_not_found", batchNo);
  const oldVersion = batch.formulaVersion;

  // 未完成（未交付）且存在锚定版本落后于新版本的结论，全部立即失效
  // （即使底片连上次变更都还没重算，本次变更仍要再留一笔失效）
  const newVersion = oldVersion + 1;
  const affected = [];
  for (const neg of state.negatives.values()) {
    if (neg.steps.some(s => s.step === "交付")) continue;
    const stale = neg.steps.some(s =>
      s.batchNo === batchNo &&
      CHEMICAL_STEPS.includes(s.step) &&
      s.formulaVersion < newVersion
    );
    if (stale) affected.push(neg.code);
  }
  const result = { batchNo, oldVersion, newVersion, affected, at: now() };
  await commit(state, "FORMULA_CHANGED", {
    batchNo, oldVersion, newVersion,
    concentration: concentration ?? batch.concentration,
    ratio: ratio ?? batch.ratio,
    affected,
    idemKey, result
  });
  return result;
}

async function createNegative(state, input) {
  const { plateSize, chemicalBatch, waterSource, idemKey } = input;
  if (idempotentHit(state, idemKey, "NEGATIVE_CREATED")) return state.idem.get(idemKey).result;
  if (chemicalBatch && !state.batches.has(chemicalBatch)) {
    throw new ApiError(404, "batch_not_found", chemicalBatch);
  }
  const code = newCode(state.seq + 1);
  const result = { code };
  await commit(state, "NEGATIVE_CREATED", {
    code,
    plateSize: plateSize || "",
    chemicalBatch: chemicalBatch || "",
    waterSource: waterSource || "",
    idemKey, result
  });
  return result;
}

async function recordConflict(state, payload) {
  const conflictId = "CF-" + randomUUID().slice(0, 8);
  return await commit(state, "CONFLICT", { conflictId, ...payload });
}
function idempotentHit(state, key, type) {
  if (!key) return false;
  const hit = state.idem.get(key);
  return !!hit && hit.type === type;
}

/* ---------------- 页面 ---------------- */
function page() {
  return `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>古法蓝晒·可恢复工序账</title>
<style>
  :root { --bg:#eef1ea; --panel:#fff; --ink:#20241f; --muted:#687066; --line:#d2dccb; --accent:#46663a; --warn:#9b4937; --hold:#9a6a1f; }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--ink); font-family:Arial,"PingFang SC",sans-serif; }
  header { padding:20px 26px; background:#fff; border-bottom:1px solid var(--line); display:flex; justify-content:space-between; align-items:center; gap:12px; }
  h1 { margin:0; font-size:23px; } h2 { margin:0 0 10px; font-size:16px; } h3 { margin:0; font-size:17px; }
  main { display:grid; grid-template-columns:360px 1fr; gap:18px; padding:18px 26px; }
  form,.panel,.card { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:14px; }
  label { display:block; margin:8px 0 4px; color:var(--muted); font-size:12px; }
  input,select,textarea { width:100%; border:1px solid var(--line); border-radius:6px; padding:8px; font:inherit; background:#fff; }
  button { border:0; border-radius:6px; background:var(--accent); color:#fff; padding:9px 12px; font-weight:700; cursor:pointer; margin-top:10px; }
  button.secondary { background:#69736a; } button.danger { background:var(--warn); } button.hold { background:var(--hold); }
  .stats { display:grid; grid-template-columns:repeat(4,1fr); gap:10px; margin-bottom:14px; }
  .stat { background:#fff; border:1px solid var(--line); border-radius:8px; padding:12px; }
  .stat strong { display:block; font-size:22px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(330px,1fr)); gap:12px; }
  .card { display:grid; gap:7px; }
  .meta { color:var(--muted); font-size:12px; }
  .pill { display:inline-block; border:1px solid var(--line); border-radius:999px; padding:2px 9px; font-size:12px; }
  .pill.ok { background:#e7f0df; border-color:#b7cfa2; }
  .pill.bad { background:#f6e2dc; border-color:#d8a99a; color:var(--warn); }
  .pill.hold { background:#f6ecd8; border-color:#d8c192; color:var(--hold); }
  .row { display:flex; gap:8px; flex-wrap:wrap; align-items:center; }
  .banner { border-radius:8px; padding:10px 12px; margin-bottom:12px; font-size:13px; }
  .banner.warn { background:#f6e2dc; border:1px solid #d8a99a; color:var(--warn); }
  .banner.info { background:#e6ece0; border:1px solid #b7cfa2; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th,td { text-align:left; border-bottom:1px solid var(--line); padding:6px 8px; }
  .logs { border-top:1px dashed var(--line); padding-top:6px; max-height:120px; overflow:auto; font-size:12px; color:var(--muted); }
  .logs b { color:var(--ink); }
  .invalid { color:var(--warn); font-weight:700; }
  details summary { cursor:pointer; font-size:13px; color:var(--accent); }
  @media (max-width:920px){ main{grid-template-columns:1fr;} header{display:block;} }
</style>
</head>
<body>
<header>
  <div><h1>古法蓝晒 · 可恢复工序账</h1>
  <div class="meta">底片 → 药液批次 → 冲洗工位 → 入盒交付，一条工序账；占工位先预占、确认才扣药、失败从最后确认步恢复</div></div>
  <button id="reload">刷新</button>
</header>
<main>
<section>
  <form id="negForm"><h2>新增底片</h2>
    <label>玻璃板尺寸</label><input name="plateSize" placeholder="如 18x24cm">
    <label>药液批次（可空，从批次表选）</label><select name="chemicalBatch" id="batchSelect"></select>
    <label>冲洗水源</label><input name="waterSource" placeholder="如 井水过滤">
    <label>幂等键（重试不重复，可空）</label><input name="idemKey" placeholder="终端生成的唯一请求号">
    <button>建档</button>
  </form>
  <form id="batchForm" style="margin-top:14px"><h2>登记 / 调整药液批次</h2>
    <label>批次号（登记用）</label><input name="batchNo" placeholder="如 B-0701">
    <label>浓度</label><input name="concentration" placeholder="如 22%">
    <label>配比</label><input name="ratio" placeholder="如 柠檬酸铁铵:铁氰化钾=1:1">
    <label>来源说明</label><input name="sourceNote">
    <label>存量（毫升，登记用）</label><input name="stockMl" type="number" placeholder="2000">
    <div class="row"><button>登记批次</button><button type="button" class="hold" id="formulaBtn">浓度/配比变更（旧结论立即失效）</button></div>
  </form>
  <form id="claimForm" style="margin-top:14px"><h2>占用工位（先占后做）</h2>
    <label>底片</label><select name="code" id="claimNeg"></select>
    <label>工序</label><select name="step" id="claimStep"></select>
    <label>工位</label><select name="station" id="claimStation"></select>
    <label>药液批次（涂布/冲洗时）</label><select name="batchNo" id="claimBatch"></select>
    <label>幂等键</label><input name="idemKey">
    <button>占用并预占药液</button>
  </form>
  <form id="confirmForm" style="margin-top:14px"><h2>确认工序完成（此刻才扣药）</h2>
    <label>底片</label><select name="code" id="confirmNeg"></select>
    <label>恢复令牌 claimId（崩溃续作时填写）</label><input name="claimId" placeholder="留空使用当前占用">
    <label>实际耗药（毫升，可空=按预占）</label><input name="usedMl" type="number">
    <label>显影状态</label><input name="developStatus">
    <label>缺陷类型</label><input name="defect">
    <label>修补记录</label><input name="repair">
    <label>备注</label><input name="note">
    <label>幂等键（断电重试同一把钥匙，绝不重扣）</label><input name="idemKey">
    <div class="row"><button>确认完成</button><button type="button" class="danger" id="abortBtn">中止占用并释放</button></div>
  </form>
  <form id="boxForm" style="margin-top:14px"><h2>入盒 / 交付</h2>
    <label>底片</label><select name="code" id="boxNeg"></select>
    <label>盒位</label><input name="box" placeholder="如 蓝盒A-03">
    <label>幂等键</label><input name="idemKey">
    <div class="row"><button>入盒</button><button type="button" class="secondary" id="deliverBtn">确认交付</button></div>
  </form>
</section>
<section>
  <div id="recovery"></div>
  <div class="stats" id="stats"></div>
  <div class="panel" style="margin-bottom:14px"><h2>药液批次（浓度/配比版本）</h2><div id="batches"></div></div>
  <div class="panel" style="margin-bottom:14px"><h2>冲洗工位（同时只接一张）</h2><div id="stations"></div></div>
  <div class="panel" style="margin-bottom:14px"><h2>冲突留痕（后到请求不覆盖先到）</h2><div id="conflicts"></div></div>
  <div class="panel"><h2>底片工序账</h2><div class="grid" id="cards"></div></div>
</section>
</main>
<script>
const STEPS = ["涂布","晾干","曝光","冲洗","复晒","入盒"];
const STATIONS = ["T-01","X-01","W-01","W-02","B-01"];
let S = { negatives: [], batches: [], stations: [], conflicts: [], recovery: null };
async function api(path, options) {
  const opt = options && options.body ? { ...options, headers: { 'Content-Type':'application/json' } } : options;
  const res = await fetch(path, opt);
  const data = await res.json();
  if (!res.ok) throw Object.assign(new Error(data.error || '请求失败'), { detail: data.detail });
  return data;
}
function formObject(f){ return Object.fromEntries(new FormData(f).entries()); }
function pill(ok, text){ return '<span class="pill ' + (ok===true?'ok':ok===false?'bad':'hold') + '">' + text + '</span>'; }
function esc(s){ return String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }
function uuid(){ return 'IDM-' + crypto.randomUUID(); }

async function load() {
  S.negatives = await api('/api/negatives');
  S.batches = await api('/api/batches');
  S.stations = await api('/api/stations');
  S.conflicts = await api('/api/conflicts');
  S.recovery = await api('/api/recovery');
  render();
}
function batchOptions(sel){ return S.batches.map(b => '<option value="'+b.batchNo+'"'+(b.batchNo===sel?' selected':'')+'>'+b.batchNo+' v'+b.formulaVersion+' · 余 '+(b.stockMl-b.reservedMl-b.usedMl)+'ml</option>').join(''); }
function negOptions(){ return S.negatives.map(n => '<option value="'+n.code+'">'+n.code+' · '+n.status+'</option>').join(''); }

function render() {
  document.querySelector('#batchSelect').innerHTML = '<option value="">（不指定）</option>' + batchOptions('');
  document.querySelector('#claimBatch').innerHTML = '<option value="">（用底片默认批次）</option>' + batchOptions('');
  document.querySelector('#claimNeg').innerHTML = negOptions();
  document.querySelector('#confirmNeg').innerHTML = negOptions();
  document.querySelector('#boxNeg').innerHTML = negOptions();
  if (!document.querySelector('#claimStep').dataset.init) {
    document.querySelector('#claimStep').innerHTML = STEPS.map(s => '<option>'+s+'</option>').join('');
    document.querySelector('#claimStation').innerHTML = STATIONS.map(s => '<option>'+s+'</option>').join('');
    document.querySelector('#claimStep').dataset.init = '1';
  }

  const rec = S.recovery;
  document.querySelector('#recovery').innerHTML = rec && rec.openClaims.length
    ? '<div class="banner warn"><b>断电恢复：</b>发现 ' + rec.openClaims.length + ' 笔“已占未确认”，工位仍锁定、药液仍预占。请凭 claimId 在“确认工序完成”处重试，确认只扣一次药。<br>'
      + rec.openClaims.map(c => esc(c.code) + ' / ' + esc(c.step) + ' / ' + esc(c.station) + ' / claimId=' + esc(c.claimId)).join('<br>') + '</div>'
    : '<div class="banner info">工序账一致，无悬挂占用。</div>';

  const labels = ["待曝光","冲洗中","待入盒","已交付"];
  document.querySelector('#stats').innerHTML = labels.map(l =>
    '<div class="stat"><span>'+l+'</span><strong>'+S.negatives.filter(n => n.status===l).length+'</strong></div>').join('');

  document.querySelector('#batches').innerHTML = '<table><tr><th>批次</th><th>版本</th><th>浓度</th><th>配比</th><th>存量/预占/已用(ml)</th><th>来源</th></tr>'
    + S.batches.map(b => '<tr><td>'+esc(b.batchNo)+'</td><td>v'+b.formulaVersion+'</td><td>'+esc(b.concentration)+'</td><td>'+esc(b.ratio)+'</td><td>'+b.stockMl+' / '+b.reservedMl+' / '+b.usedMl+'</td><td>'+esc(b.sourceNote)+'</td></tr>').join('') + '</table>';

  document.querySelector('#stations').innerHTML = S.stations.map(st => st.lockedBy
    ? pill(false, st.station + ' ' + st.kind + '：被 ' + esc(st.lockedBy) + ' 占用（' + esc(st.lockedStep) + '），自 ' + new Date(st.lockedSince).toLocaleString())
    : pill(true, st.station + ' ' + st.kind + '：空闲')).join(' &nbsp; ');

  document.querySelector('#conflicts').innerHTML = S.conflicts.length
    ? '<table><tr><th>时间</th><th>后到底片</th><th>工序</th><th>工位</th><th>原因</th><th>先到占用</th></tr>'
      + S.conflicts.slice(-8).reverse().map(c => '<tr><td>'+new Date(c.at).toLocaleString()+'</td><td>'+esc(c.code)+'</td><td>'+esc(c.step||'')+'</td><td>'+esc(c.station||'')+'</td><td>'+esc(c.reason)+'</td><td class="meta">'+esc(JSON.stringify(c.detail||{}))+'</td></tr>').join('') + '</table>'
    : '<div class="meta">暂无冲突</div>';

  document.querySelector('#cards').innerHTML = S.negatives.map(cardHtml).join('');
}
function cardHtml(n) {
  const concl = (label, c) => !c ? '<div class="meta">'+label+'：—</div>'
    : '<div>'+label+'：'+(c.valid ? pill(true,'有效 v'+c.formulaVersion) : pill(false,'失效 需重算（旧v'+c.formulaVersion+'→现v'+c.currentVersion+'）'))+'</div>';
  const permit = !n.boxPermit ? '<div class="meta">入盒许可：—</div>'
    : '<div>入盒许可：'+(n.boxPermit.valid ? pill(true, n.boxPermit.box ? '已入盒 '+esc(n.boxPermit.box) : '可入盒') : pill(false, n.boxPermit.reason))+'</div>';
  const claim = n.openClaim ? '<div class="pill hold">占用中：'+esc(n.openClaim.step)+' @ '+esc(n.openClaim.station)+'，预占 '+n.openClaim.reserveMl+'ml，claimId='+esc(n.openClaim.claimId.slice(0,8))+'…</div>' : '';
  const logs = n.steps.slice(-6).map(s => '<div><b>'+esc(s.step)+'</b> @ '+esc(s.station)+' · '+esc(s.batchNo||'')+(s.legacy?'（旧档）':'')+' · '+(s.usedMl?('扣'+s.usedMl+'ml'):'')+' · '+new Date(s.at).toLocaleString()+(s.defect?(' · 缺陷:'+esc(s.defect)):'')+'</div>').join('') || '暂无已确认步骤';
  return '<article class="card"><div class="row"><h3>'+esc(n.code)+'</h3>'+pill(true, n.status)+'</div>'
    + '<div class="meta">'+esc(n.plateSize)+' · 水源 '+esc(n.waterSource||'—')+' · 默认批次 '+esc(n.defaultBatch||'—')+' · 最后确认：'+esc(n.lastConfirmedStep||'无')+'</div>'
    + claim + concl('曝光结论', n.exposure) + concl('冲洗结论', n.wash) + permit
    + (n.invalidations.length ? '<div class="invalid">配方变更 '+n.invalidations.length+' 次，相关结论已重算</div>' : '')
    + '<details><summary>历史履历（'+n.steps.length+' 步，含旧档，永不覆盖）</summary><div class="logs">'+logs+'</div></details></article>';
}

function bind(formId, url, extra) {
  const f = document.querySelector('#' + formId);
  f.addEventListener('submit', async ev => {
    ev.preventDefault();
    try {
      const o = formObject(f);
      if (!o.idemKey) o.idemKey = uuid();
      const payload = extra ? extra(o) : o;
      const r = await api(url, { method: 'POST', body: JSON.stringify(payload) });
      alert('已受理：' + JSON.stringify(r));
      await load();
    } catch (e) { alert('被拒/冲突：' + e.message + (e.detail ? ' ' + JSON.stringify(e.detail) : '')); }
  });
}
bind('negForm', '/api/negatives');
bind('claimForm', '/api/claims');
bind('confirmForm', '/api/steps/confirm');
bind('boxForm', '/api/box');
bind('batchForm', '/api/batches');

document.querySelector('#abortBtn').onclick = async () => {
  const f = document.querySelector('#confirmForm');
  const o = formObject(f);
  try { alert(JSON.stringify(await api('/api/claims/abort', { method:'POST', body: JSON.stringify({ code:o.code, claimId:o.claimId, reason:'人工中止' }) }))); await load(); }
  catch(e){ alert(e.message); }
};
document.querySelector('#formulaBtn').onclick = async () => {
  const f = document.querySelector('#batchForm');
  const o = formObject(f);
  if (!o.batchNo) return alert('填要调整的批次号');
  try { const r = await api('/api/batches/formula', { method:'POST', body: JSON.stringify({ batchNo:o.batchNo, concentration:o.concentration, ratio:o.ratio, idemKey:uuid() }) });
    alert('配方已升到 v' + r.newVersion + '，立即失效底片：' + (r.affected.join(', ') || '无')); await load(); }
  catch(e){ alert(e.message); }
};
document.querySelector('#deliverBtn').onclick = async () => {
  const f = document.querySelector('#boxForm');
  const o = formObject(f);
  try { alert(JSON.stringify(await api('/api/deliver', { method:'POST', body: JSON.stringify({ code:o.code, idemKey:o.idemKey||uuid() }) }))); await load(); }
  catch(e){ alert('交付被拒：' + e.message); }
};
document.querySelector('#reload').onclick = load;
load();
</script>
</body>
</html>`;
}

/* ---------------- 路由 ---------------- */
const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const { state, recovery } = await loadState();

    if (req.method === "GET" && url.pathname === "/") return html(res, page());
    if (req.method === "GET" && url.pathname === "/api/negatives") {
      return send(res, 200, [...state.negatives.values()].map(n => deriveNegative(n, state)));
    }
    if (req.method === "GET" && url.pathname === "/api/batches") {
      return send(res, 200, [...state.batches.values()].map(b => ({
        ...b, availableMl: b.stockMl - b.reservedMl - b.usedMl
      })));
    }
    if (req.method === "GET" && url.pathname === "/api/stations") {
      return send(res, 200, [...state.stations.values()]);
    }
    if (req.method === "GET" && url.pathname === "/api/conflicts") {
      // 从工序账直接翻冲突事件（历史履历可查）
      const conflicts = [];
      if (existsSync(ledgerPath)) {
        const raw = await readFile(ledgerPath, "utf8");
        for (const line of raw.split("\n")) {
          const t = line.trim();
          if (!t) continue;
          const e = JSON.parse(t);
          if (e.type === "CONFLICT") conflicts.push(e);
        }
      }
      return send(res, 200, conflicts);
    }
    if (req.method === "GET" && url.pathname === "/api/recovery") {
      return send(res, 200, { lastSeq: state.seq, ...recovery });
    }
    if (req.method === "GET" && url.pathname === "/api/ledger") {
      // 原始工序账下载/查阅
      res.writeHead(200, { "Content-Type": "application/x-ndjson; charset=utf-8" });
      if (existsSync(ledgerPath)) return createReadStream(ledgerPath).pipe(res);
      return res.end("");
    }
    const negOne = url.pathname.match(/^\/api\/negatives\/([^/]+)$/);
    if (negOne && req.method === "GET") {
      const neg = state.negatives.get(decodeURIComponent(negOne[1]));
      if (!neg) return send(res, 404, { error: "negative_not_found" });
      return send(res, 200, deriveNegative(neg, state));
    }

    const mut = async (handler) => withLock(async () => {
      // 锁内重新装载，保证串行写基于最新账本
      const fresh = await loadState();
      return handler(fresh.state, fresh.recovery);
    });

    const route = (method, pathRegex, handler) => {
      if (req.method !== method) return false;
      const m = url.pathname.match(pathRegex);
      if (!m) return false;
      mut(async (st) => {
        const result = await handler(st, await body(req), m);
        if (!res.headersSent) send(res, 200, result);
      }).catch(err => {
        if (err instanceof ApiError) return send(res, err.status, { error: err.code, detail: err.detail });
        console.error(err);
        send(res, 500, { error: "internal", detail: err.message });
      });
      return true;
    };

    if (route("POST", /^\/api\/negatives$/, (st, input) => createNegative(st, input))) return;
    if (route("POST", /^\/api\/claims$/, (st, input) => claimStation(st, input))) return;
    if (route("POST", /^\/api\/claims\/abort$/, (st, input) => abortClaim(st, input))) return;
    if (route("POST", /^\/api\/steps\/confirm$/, (st, input) => commitStep(st, input))) return;
    if (route("POST", /^\/api\/box$/, (st, input) => boxNegative(st, input))) return;
    if (route("POST", /^\/api\/deliver$/, (st, input) => deliverNegative(st, input))) return;
    if (route("POST", /^\/api\/batches$/, (st, input) => registerBatch(st, input))) return;
    if (route("POST", /^\/api\/batches\/formula$/, (st, input) => changeFormula(st, input))) return;

    return send(res, 404, { error: "not_found" });
  } catch (error) {
    console.error(error);
    send(res, 500, { error: error.message });
  }
});

server.listen(port, () => console.log("古法蓝晒可恢复工序账 listening on http://localhost:" + port));
