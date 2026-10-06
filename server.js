import http from "node:http";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  migrate,
  handoff,
  confirmHandoff,
  recover,
  updateBatch,
  listItems,
  getItem,
  listOperations,
  ensureFreshConclusion,
  currentBatch,
  recomputeConclusion,
  STAGES,
  newId,
  now,
} from "./ledger.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const dbPath = join(__dirname, "data", "cyanotype-negative-room.json");
const port = Number(process.env.PORT || 3040);

const seed = {
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
      defect: "边角显影不均",
      logs: [{ at: "2026-06-20", step: "曝光", note: "阴天补时2分钟" }],
    },
    {
      id: "CN-002",
      code: "CN-002",
      plateSize: "13x18cm",
      exposure: "6分钟",
      waterSource: "井水过滤",
      status: "待曝光",
      logs: [],
      // 故意没有 chemicalBatch，首次加载时迁移补来源
    },
  ],
  operations: [],
  idempotency: {},
};

async function loadDb() {
  if (!existsSync(dbPath)) {
    await mkdir(dirname(dbPath), { recursive: true });
    await writeFile(dbPath, JSON.stringify(seed, null, 2));
  }
  const db = JSON.parse(await readFile(dbPath, "utf8"));
  const { events } = migrate(db);
  if (events.length) await saveDb(db);
  return db;
}
async function saveDb(db) {
  await writeFile(dbPath, JSON.stringify(db, null, 2));
}

// 模块级内存 db：启动时加载一次，所有请求共享同一份内存状态。
// 这样工位锁对并发请求立即可见（handoff 是同步的，Node 单线程内不会交错），
// 避免每次请求各自 loadDb 导致的丢更新问题。
let db;
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
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function summarize(item) {
  const logCount = (item.logs || []).length + (item.steps || []).length;
  return { ...item, logCount };
}

function page() {
  return `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>古法蓝晒底片整理室 · 工序账</title>
  <style>
    :root { --bg:#f1f3ef; --panel:#fff; --ink:#20241f; --muted:#687066; --line:#d4ddd0; --accent:#526f43; --warn:#9b4937; --ok:#3f6b4a; --held:#8a6d3b; }
    * { box-sizing:border-box; } body { margin:0; background:var(--bg); color:var(--ink); font-family:Arial,"PingFang SC",sans-serif; }
    header { padding:22px 28px; background:#fff; border-bottom:1px solid var(--line); display:flex; justify-content:space-between; gap:16px; align-items:center; }
    h1 { margin:0; font-size:26px; } h2 { margin:0 0 12px; font-size:18px; } h3 { margin:0 0 6px; font-size:15px; }
    main { display:grid; grid-template-columns:400px 1fr; gap:22px; padding:22px 28px; }
    form,.panel,.card,.stat { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:16px; }
    label { display:block; margin:10px 0 5px; color:var(--muted); font-size:13px; } input,select,textarea { width:100%; border:1px solid var(--line); border-radius:6px; padding:9px; font:inherit; background:#fff; } textarea { min-height:60px; }
    button { border:0; border-radius:6px; background:var(--accent); color:#fff; padding:10px 13px; font-weight:700; cursor:pointer; } button.secondary { background:#69736a; } button.danger { background:var(--warn); }
    .stats { display:grid; grid-template-columns:repeat(auto-fit,minmax(110px,1fr)); gap:10px; margin-bottom:14px; } .stat strong { display:block; font-size:22px; } .stat span { color:var(--muted); font-size:12px; }
    .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(280px,1fr)); gap:12px; } .card { display:grid; gap:6px; }
    .meta { color:var(--muted); font-size:12px; } .pill { display:inline-block; border:1px solid var(--line); border-radius:999px; padding:2px 8px; font-size:12px; }
    .pill.ok { color:var(--ok); border-color:var(--ok); } .pill.warn { color:var(--warn); border-color:var(--warn); } .pill.held { color:var(--held); border-color:var(--held); }
    .logs { border-top:1px solid var(--line); padding-top:8px; max-height:110px; overflow:auto; font-size:12px; }
    .row { display:flex; gap:8px; flex-wrap:wrap; align-items:center; } .row > * { flex:1; }
    table { width:100%; border-collapse:collapse; font-size:13px; } th,td { text-align:left; padding:6px 8px; border-bottom:1px solid var(--line); } th { color:var(--muted); font-weight:600; }
    .stale { color:var(--warn); font-weight:700; }
    @media (max-width:900px){ header{display:block;padding:18px 16px;} main{grid-template-columns:1fr;padding:16px;} }
  </style>
</head>
<body>
  <header><div><h1>古法蓝晒底片整理室</h1><div class="meta">底片 · 药液批次 · 冲洗工位 · 入盒交付 — 可恢复工序账</div></div><div class="row"><button id="recover" class="secondary">崩溃恢复</button><button id="reload">刷新</button></div></header>
  <main>
    <section>
      <form id="createForm"><h2>新增底片</h2>
        <label>底片编号</label><input name="code" required>
        <label>玻璃板尺寸</label><input name="plateSize">
        <label>药液批次（留空则升级补来源）</label><input name="chemicalBatch" placeholder="如 B-0620">
        <label>曝光时间</label><input name="exposure" placeholder="如 8分钟">
        <label>冲洗水源</label><input name="waterSource">
        <label>存放盒位</label><input name="box">
        <button>保存底片</button>
      </form>
      <form id="handoffForm" style="margin-top:14px"><h2>交接工序（锁定工位 · 预占药液）</h2>
        <label>底片</label><select name="negativeId" id="hItem"></select>
        <label>工位</label><select name="station" id="hStation"></select>
        <label>阶段</label><select name="stage">${STAGES.map((s) => `<option value="${s}">${s}</option>`).join("")}</select>
        <label>药液用量</label><input name="amount" type="number" min="1" value="1">
        <label>备注 / 缺陷</label><input name="note" placeholder="如 显影稳定">
        <button>提交交接</button>
        <div class="meta" id="handoffMsg"></div>
      </form>
      <form id="batchForm" style="margin-top:14px"><h2>药液批次（浓度/配比变更即失效重算）</h2>
        <label>批次</label><select name="code" id="bCode"></select>
        <label>浓度</label><input name="concentration" type="number" step="0.01">
        <label>配比（如 1:2）</label><input name="ratio">
        <button class="secondary">更新批次</button>
        <div class="meta" id="batchMsg"></div>
      </form>
    </section>
    <section>
      <div class="stats" id="stats"></div>
      <div class="panel" style="margin-bottom:14px"><h2>工位</h2><div id="stations"></div></div>
      <div class="panel" style="margin-bottom:14px"><h2>药液批次</h2><div id="batches"></div></div>
      <div class="panel" style="margin-bottom:14px"><h2>底片（结论随批次版本自动重算）</h2><div class="grid" id="cards"></div></div>
      <div class="panel"><h2>工序账（最近操作）</h2><div id="ops"></div></div>
    </section>
  </main>
  <script>
    const STAGES = ${JSON.stringify(STAGES)};
    const createForm = document.querySelector('#createForm');
    const handoffForm = document.querySelector('#handoffForm');
    const batchForm = document.querySelector('#batchForm');
    const cards = document.querySelector('#cards');
    const statsEl = document.querySelector('#stats');
    let items = [], stations = [], batches = [], operations = [];

    async function api(path, options) {
      const res = await fetch(path, { ...options, headers: { 'Content-Type': 'application/json', ...(options && options.headers) } });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || '请求失败' + (data.heldBy ? '（工位被 ' + data.heldBy + ' 占用）' : ''));
      return data;
    }
    function pillClass(c) { return c === '已交付' ? 'ok' : (c === '冲洗中' ? 'held' : 'warn'); }
    function conclusionHtml(it) {
      const c = it.conclusion;
      if (!c) return '<span class="meta">暂无结论</span>';
      const stale = c.valid === false;
      return '<div class="meta">曝光：<span class="' + (c.exposure === '充足' ? 'ok' : 'warn') + '">' + c.exposure + '</span> · 冲洗：<span class="' + (c.wash === '合格' ? 'ok' : 'warn') + '">' + c.wash + '</span> · 入盒许可：<span class="' + (c.boxinPermit ? 'ok' : 'warn') + '">' + (c.boxinPermit ? '发放' : '失效') + '</span> · v' + c.basedOnBatchVersion + (stale ? ' <span class="stale">已失效</span>' : '') + '</div>';
    }
    function render() {
      statsEl.innerHTML = STAGES.map(s => '<div class="stat"><span>' + s + '</span><strong>' + items.filter(i => i.status === s).length + '</strong></div>').join('');
      document.querySelector('#stations').innerHTML = '<table><tr><th>工位</th><th>状态</th><th>锁定操作</th></tr>' + stations.map(s => '<tr><td>' + s.code + '</td><td>' + (s.lockedBy ? '<span class="pill held">占用中</span>' : '<span class="pill ok">空闲</span>') + '</td><td class="meta">' + (s.lockedBy || '—') + '</td></tr>').join('') + '</table>';
      document.querySelector('#batches').innerHTML = '<table><tr><th>批次</th><th>浓度</th><th>配比</th><th>版本</th><th>库存</th><th>预占</th><th>已扣</th></tr>' + batches.map(b => '<tr><td>' + b.batch + (b.legacy ? ' <span class="meta">(补来源)</span>' : '') + '</td><td>' + b.concentration + '</td><td>' + b.ratio + '</td><td>v' + b.version + '</td><td>' + b.stock + '</td><td>' + b.held + '</td><td>' + b.consumed + '</td></tr>').join('') + '</table>';
      document.querySelector('#bCode').innerHTML = batches.map(b => '<option value="' + b.batch + '">' + b.batch + '</option>').join('');
      document.querySelector('#hItem').innerHTML = items.map(i => '<option value="' + i.id + '">' + i.code + ' · ' + i.status + '</option>').join('');
      document.querySelector('#hStation').innerHTML = stations.map(s => '<option value="' + s.code + '">' + s.code + '</option>').join('');
      cards.innerHTML = items.map(it => {
        const logs = (it.logs || []).slice(-3).map(l => '<div>' + l.step + '：' + (l.note || '') + '</div>').join('');
        return '<article class="card"><h3>' + it.code + '</h3><div><span class="pill ' + pillClass(it.status) + '">' + it.status + '</span> ' + (it.batchSource ? '<span class="meta">批次来源：' + it.batchSource + '</span>' : '') + '</div>' +
          '<div class="meta">尺寸 ' + (it.plateSize || '—') + ' · 批次 ' + (it.chemicalBatch || '—') + ' · 曝光 ' + (it.exposure || '—') + ' · 水源 ' + (it.waterSource || '—') + ' · 盒位 ' + (it.box || '—') + '</div>' +
          conclusionHtml(it) +
          (it.defect ? '<div class="meta">缺陷：' + it.defect + '</div>' : '') +
          '<div class="logs">' + (logs || '暂无记录') + '</div></article>';
      }).join('');
      document.querySelector('#ops').innerHTML = '<table><tr><th>操作</th><th>底片</th><th>工位</th><th>阶段</th><th>状态</th><th>药液</th></tr>' + operations.slice(-12).reverse().map(o => '<tr><td class="meta">' + o.opId + '</td><td>' + o.negativeId + '</td><td>' + o.station + '</td><td>' + o.stage + '</td><td><span class="pill ' + (o.state === 'confirmed' ? 'ok' : (o.state === 'reserved' ? 'held' : 'warn')) + '">' + o.state + '</span></td><td class="meta">' + o.amount + ' ' + o.batch + ' (' + o.reservation.status + ')</td></tr>').join('') + '</table>';
    }
    async function load() {
      items = await api('/api/items');
      stations = await api('/api/stations');
      batches = await api('/api/batches');
      operations = await api('/api/operations');
      render();
    }
    createForm.onsubmit = async e => {
      e.preventDefault();
      const body = Object.fromEntries(new FormData(createForm).entries());
      await api('/api/items', { method: 'POST', body: JSON.stringify(body) });
      createForm.reset();
      await load();
    };
    handoffForm.onsubmit = async e => {
      e.preventDefault();
      const msg = document.querySelector('#handoffMsg');
      const fd = new FormData(handoffForm);
      const body = Object.fromEntries(fd.entries());
      body.amount = Number(body.amount) || 1;
      body.idempotencyKey = 'h-' + Date.now() + '-' + Math.random().toString(36).slice(2, 8);
      try {
        const r = await api('/api/items/' + body.negativeId + '/handoff', { method: 'POST', body: JSON.stringify(body) });
        msg.textContent = '已交接：' + r.opId + ' → ' + (r.item ? r.item.status : '');
        msg.className = 'meta ok';
      } catch (err) {
        msg.textContent = '冲突：' + err.message;
        msg.className = 'meta stale';
      }
      await load();
    };
    batchForm.onsubmit = async e => {
      e.preventDefault();
      const msg = document.querySelector('#batchMsg');
      const fd = new FormData(batchForm);
      const code = fd.get('code');
      const patch = {};
      if (fd.get('concentration')) patch.concentration = Number(fd.get('concentration'));
      if (fd.get('ratio')) patch.ratio = fd.get('ratio');
      try {
        const r = await api('/api/batches/' + code, { method: 'PATCH', body: JSON.stringify(patch) });
        msg.textContent = '已更新 v' + r.batch.version + '，失效 ' + r.invalidated + ' 条结论';
        msg.className = 'meta ok';
      } catch (err) {
        msg.textContent = '失败：' + err.message;
        msg.className = 'meta stale';
      }
      await load();
    };
    document.querySelector('#reload').onclick = load;
    document.querySelector('#recover').onclick = async () => {
      const r = await api('/api/recover', { method: 'POST' });
      alert('恢复完成，处理 ' + r.resumed.length + ' 个未完成操作');
      await load();
    };
    load();
  </script>
</body>
</html>`;
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    // 使用模块级 db（启动时加载一次，并发请求共享）

    if (req.method === "GET" && url.pathname === "/") return html(res, page());

    if (req.method === "GET" && url.pathname === "/api/items") {
      return send(res, 200, listItems(db).map(summarize));
    }

    if (req.method === "POST" && url.pathname === "/api/items") {
      const input = await body(req);
      const item = {
        id: newId("CN"),
        code: input.code || "",
        plateSize: input.plateSize || "",
        chemicalBatch: input.chemicalBatch || "",
        exposure: input.exposure || "",
        waterSource: input.waterSource || "",
        box: input.box || "",
        status: input.status || "待曝光",
        logs: [{ at: now(), step: "建档", note: "创建底片" }],
      };
      // 填了批次号但批次不存在 → 建一个占位批次；没填则迁移时补来源
      if (item.chemicalBatch && !currentBatch(db, item.chemicalBatch)) {
        db.batches.push({ batch: item.chemicalBatch, concentration: 0.1, ratio: "1:1", version: 1, stock: 0, held: 0, consumed: 0, note: "建档时占位批次" });
      }
      db.items.unshift(item);
      const { events } = migrate(db);
      await saveDb(db);
      return send(res, 201, item);
    }

    const patch = url.pathname.match(/^\/api\/items\/([^/]+)$/);
    if (patch && req.method === "PATCH") {
      const item = db.items.find((x) => x.id === patch[1] || x.code === patch[1]);
      if (!item) return send(res, 404, { error: "item_not_found" });
      Object.assign(item, await body(req));
      item.logs ||= [];
      item.logs.push({ at: now(), step: "状态", note: "更新为" + item.status });
      ensureFreshConclusion(item, db);
      await saveDb(db);
      return send(res, 200, item);
    }

    const log = url.pathname.match(/^\/api\/items\/([^/]+)\/logs$/);
    if (log && req.method === "POST") {
      const item = db.items.find((x) => x.id === log[1] || x.code === log[1]);
      if (!item) return send(res, 404, { error: "item_not_found" });
      const input = await body(req);
      item.logs ||= [];
      item.logs.push({ at: now(), step: input.step || "记录", note: input.note || "" });
      await saveDb(db);
      return send(res, 201, item);
    }

    // 交接：锁定工位、预占药液、幂等恢复
    const handoffMatch = url.pathname.match(/^\/api\/items\/([^/]+)\/handoff$/);
    if (handoffMatch && req.method === "POST") {
      const input = await body(req);
      input.negativeId = handoffMatch[1];
      input.idempotencyKey = input.idempotencyKey || req.headers["idempotency-key"] || null;
      const r = handoff(db, input);
      if (!r.ok) {
        if (r.op) await saveDb(db); // aborted op 也要落盘
        return send(res, r.status, r);
      }
      if (r.phase === "reserved") {
        await saveDb(db); // 第一次持久化：reserve（stock→held、工位锁定）
        await sleep(20); // 留出交接处理窗口，使工位锁对并发请求可见
        const c = confirmHandoff(db, r.op, input);
        await saveDb(db); // 第二次持久化：confirm（推进、held→consumed、释放工位）
        return send(res, 200, c);
      }
      // replay confirmed / recovered
      await saveDb(db);
      return send(res, 200, r);
    }

    // 强制重算结论
    const recomputeMatch = url.pathname.match(/^\/api\/items\/([^/]+)\/recompute$/);
    if (recomputeMatch && req.method === "POST") {
      const item = db.items.find((x) => x.id === recomputeMatch[1] || x.code === recomputeMatch[1]);
      if (!item) return send(res, 404, { error: "item_not_found" });
      const batch = currentBatch(db, item.chemicalBatch);
      item.conclusion = recomputeConclusion(item, batch);
      await saveDb(db);
      return send(res, 200, item.conclusion);
    }

    if (req.method === "GET" && url.pathname === "/api/batches") {
      return send(res, 200, db.batches);
    }
    if (req.method === "PATCH" && url.pathname.match(/^\/api\/batches\/[^/]+$/)) {
      const code = url.pathname.split("/").pop();
      const r = updateBatch(db, code, await body(req));
      if (!r.ok) return send(res, r.status, r);
      await saveDb(db);
      return send(res, 200, r);
    }

    if (req.method === "GET" && url.pathname === "/api/stations") {
      return send(res, 200, db.stations);
    }

    if (req.method === "GET" && url.pathname === "/api/operations") {
      return send(res, 200, listOperations(db));
    }

    if (req.method === "POST" && url.pathname === "/api/recover") {
      const resumed = recover(db);
      await saveDb(db);
      return send(res, 200, { resumed });
    }

    if (req.method === "GET" && url.pathname === "/api/stats") {
      const stats = Object.fromEntries(STAGES.map((s) => [s, 0]));
      for (const it of listItems(db)) stats[it.status] = (stats[it.status] || 0) + 1;
      return send(res, 200, stats);
    }

    send(res, 404, { error: "not_found" });
  } catch (error) {
    send(res, 500, { error: error.message });
  }
});

// 启动时先加载 db、迁移补来源、恢复未完成的 reserved 操作
server.listen(port, async () => {
  db = await loadDb();
  const resumed = recover(db);
  if (resumed.length) {
    await saveDb(db);
    console.log("崩溃恢复：处理 " + resumed.length + " 个未完成操作");
  }
  console.log("古法蓝晒底片整理室（工序账）listening on http://localhost:" + port);
});
