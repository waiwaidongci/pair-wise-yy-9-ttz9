// smoke-http.js — HTTP 层冒烟测试：并发冲突、幂等、批次失效、崩溃恢复
const base = "http://localhost:3040";

async function api(path, options) {
  const res = await fetch(base + path, {
    ...options,
    headers: { "Content-Type": "application/json", ...(options && options.headers) },
  });
  const data = await res.json().catch(() => ({}));
  return { http: res.status, data };
}

let pass = 0, fail = 0;
function check(name, cond, extra = "") {
  if (cond) { pass++; console.log("  PASS  " + name); }
  else { fail++; console.log("  FAIL  " + name + " " + extra); }
}

(async () => {
  console.log("\n== 补来源 ==");
  {
    const { data } = await api("/api/items");
    const cn2 = data.find((i) => i.code === "CN-002");
    check("CN-002 补 B-LEGACY", cn2.chemicalBatch === "B-LEGACY");
    check("CN-002 batchSource=legacy-backfill", cn2.batchSource === "legacy-backfill");
    check("CN-002 补来源日志", cn2.logs.some((l) => l.step === "补来源"));
  }

  console.log("\n== 并发冲突：同一工位同时只接一张底片 ==");
  {
    const [a, b] = await Promise.all([
      api("/api/items/CN-001/handoff", { method: "POST", headers: { "Idempotency-Key": "conc-a" }, body: JSON.stringify({ station: "WS-01", stage: "冲洗中", amount: 1 }) }),
      api("/api/items/CN-002/handoff", { method: "POST", headers: { "Idempotency-Key": "conc-b" }, body: JSON.stringify({ station: "WS-01", stage: "冲洗中", amount: 1 }) }),
    ]);
    const results = [a, b];
    const ok = results.filter((r) => r.http === 200);
    const conflicts = results.filter((r) => r.http === 409 && r.data.error === "station_conflict");
    check("一个成功一个冲突", ok.length === 1 && conflicts.length === 1, JSON.stringify(results.map((r) => r.http + "/" + (r.data.error || "ok"))));
    check("冲突方 heldBy 指向占用操作", conflicts[0] && conflicts[0].data.heldBy != null);
  }

  console.log("\n== 幂等：同一 key 重试不重复扣减 ==");
  {
    const before = await api("/api/batches");
    const b0 = before.data.find((b) => b.batch === "B-0620");
    for (let i = 0; i < 2; i++) {
      await api("/api/items/CN-001/handoff", { method: "POST", headers: { "Idempotency-Key": "idem-x" }, body: JSON.stringify({ station: "WS-02", stage: "冲洗中", amount: 1 }) });
    }
    const after = await api("/api/batches");
    const b1 = after.data.find((b) => b.batch === "B-0620");
    check("consumed 只 +1", b1.consumed === b0.consumed + 1, "从 " + b0.consumed + " 到 " + b1.consumed);
    check("stock 只 -1", b1.stock === b0.stock - 1);
  }

  console.log("\n== 版本失效：配比变更 → 结论失效 → 入盒被拒 ==");
  {
    const r = await api("/api/batches/B-0620", { method: "PATCH", body: JSON.stringify({ ratio: "1:3" }) });
    check("版本升到 2", r.data.batch.version === 2);
    check("invalidated=1", r.data.invalidated === 1);
    const items = await api("/api/items");
    const cn1 = items.data.find((i) => i.code === "CN-001");
    // 读时已重算：配比 1:3 → 冲洗不合格、入盒许可失效、版本为 2
    check("CN-001 按新配比重算（冲洗不合格、许可失效、v2）", cn1.conclusion.wash === "不合格" && cn1.conclusion.boxinPermit === false && cn1.conclusion.basedOnBatchVersion === 2);
    // 尝试入盒 → 409 permit_invalid
    const h = await api("/api/items/CN-001/handoff", { method: "POST", headers: { "Idempotency-Key": "permit-x" }, body: JSON.stringify({ station: "WS-02", stage: "待入盒", amount: 1 }) });
    check("入盒被拒 permit_invalid", h.http === 409 && h.data.error === "permit_invalid");
    // 恢复配比
    await api("/api/batches/B-0620", { method: "PATCH", body: JSON.stringify({ ratio: "1:2" }) });
    const items2 = await api("/api/items");
    const cn1b = items2.data.find((i) => i.code === "CN-001");
    check("配比恢复后入盒许可重新发放", cn1b.conclusion.boxinPermit === true);
  }

  console.log("\n== 崩溃恢复：recover 端点 ==");
  {
    const r = await api("/api/recover", { method: "POST" });
    check("recover 返回 resumed 数组", Array.isArray(r.data.resumed));
  }

  console.log("\n结果：" + pass + " 通过，" + fail + " 失败");
  process.exit(fail ? 1 : 0);
})();
