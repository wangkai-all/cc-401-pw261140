"use strict";

const $ = (sel) => document.querySelector(sel);
const fmtBytes = (n) => {
  if (n === null || n === undefined) return "–";
  const units = ["B", "KiB", "MiB", "GiB"];
  let v = Number(n);
  let i = 0;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${i === 0 ? v : v.toFixed(1)} ${units[i]}`;
};
const short = (d) => (d ? d.slice(0, 12) : "–");
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

let state = { release: { datasets: [] }, consumers: [], scenario: [], dataset: "catalog" };

function toast(message, kind = "") {
  let el = $(".toast");
  if (!el) {
    el = document.createElement("div");
    el.className = "toast";
    document.body.appendChild(el);
  }
  el.textContent = message;
  el.className = `toast show ${kind}`;
  clearTimeout(el._t);
  el._t = setTimeout(() => { el.className = "toast"; }, 4200);
}

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const text = await res.text();
  let body;
  try { body = text ? JSON.parse(text) : {}; } catch { body = { error: text }; }
  if (!res.ok) throw new Error(body.error || `${res.status} ${res.statusText}`);
  return body;
}

function setStatus(ok, text) {
  const el = $("#server-status");
  el.className = `status ${ok ? "ok" : "err"}`;
  $("#status-text").textContent = text;
}

async function refresh() {
  try {
    const data = await api("/console/api/state");
    state = data;
    setStatus(true, "已连接 · 真实文件系统");
    renderRelease();
    renderConsumers();
    renderSelectors();
    renderEvents();
  } catch (err) {
    setStatus(false, `连接失败：${err.message}`);
  }
}

// ---------------------------------------------------------------- release
function renderRelease() {
  const ds = state.release.datasets[0];
  const list = $("#release-list");
  $("#release-path").textContent = state.release.datasets.length ? "" : "（空）";
  if (!ds || !ds.versions.length) {
    list.innerHTML = `<p class="hint">发布目录为空。点击“发布新版本”生成 v1。</p>`;
    return;
  }
  list.innerHTML = ds.versions
    .slice()
    .reverse()
    .map((v) => {
      const diffs = Object.entries(v.diffs || {})
        .map(([base, meta]) => {
          if (base === "none") {
            return `<span class="chip full" title="整包，${fmtBytes(meta.download_bytes)}">full · ${fmtBytes(meta.download_bytes)}</span>`;
          }
          return `<span class="chip diff" title="自 ${base} 的差分：${meta.download_segments} 段 / ${fmtBytes(meta.download_bytes)}">${base} → ${fmtBytes(meta.download_bytes)} · ${meta.download_segments}段</span>`;
        })
        .join("");
      return `
      <div class="version-card">
        <div class="tag">${esc(v.version)}</div>
        <div>
          <div class="meta">${v.segments} 段 · ${v.total_records} 记录 · ${fmtBytes(v.total_bytes)} · 内容 ${short(v.content_digest)}</div>
          <div class="meta">清单 ${short(v.manifest_digest)} · ${esc(v.created_at)}</div>
        </div>
        <div class="diffs">${diffs}</div>
      </div>`;
    })
    .join("");
}

// -------------------------------------------------------------- consumers
function renderConsumers() {
  const grid = $("#consumer-list");
  if (!state.consumers.length) {
    grid.innerHTML = `<p class="hint">没有消费端。</p>`;
    return;
  }
  grid.innerHTML = state.consumers
    .map((c) => {
      const installs = c.installed.length
        ? c.installed
            .map(
              (i) => `
          <div class="install">
            <div class="row"><span class="k">版本</span><b class="mono">${esc(i.version)}</b></div>
            <div class="row"><span class="k">来源</span><span class="mono">${esc(i.source)}</span></div>
            <div class="row"><span class="k">内容摘要</span><span class="mono">${short(i.content_digest)}</span></div>
            <div class="row"><span class="k">完整性</span><span class="${i.healthy ? "state-ok" : "state-bad"}">${i.healthy ? "校验通过" : "损坏：" + esc([...(i.missing || []), ...(i.corrupt || [])].join(", "))}</span></div>
          </div>`
            )
            .join("")
        : `<p class="hint">尚未安装任何版本。</p>`;
      return `<div class="consumer-card"><h3><span>${esc(c.name)}</span><span class="chip">${c.installed.length} 个版本</span></h3>${installs}</div>`;
    })
    .join("");
}

// -------------------------------------------------------------- selectors
function renderSelectors() {
  const ds = state.release.datasets[0];
  const versions = ds ? ds.versions.map((v) => v.version) : [];
  const consumerNames = state.consumers.map((c) => c.name);

  fillSelect($("#op-consumer"), consumerNames, $("#op-consumer").value);
  fillSelect($("#q-consumer"), consumerNames, $("#q-consumer").value);
  fillSelect($("#op-version"), versions.slice().reverse(), $("#op-version").value);
  fillSelect($("#q-version"), [["", "最新"], ...versions.slice().reverse().map((v) => [v, v])], $("#q-version").value, true);
}

function fillSelect(sel, values, current, pairs = false) {
  const opts = pairs ? values : values.map((v) => [v, v]);
  const keep = current;
  sel.innerHTML = opts.map(([val, label]) => `<option value="${esc(val)}">${esc(label)}</option>`).join("");
  if (opts.some(([val]) => val === keep)) sel.value = keep;
}

// ------------------------------------------------------------------ events
function renderEvents() {
  const log = $("#event-log");
  const events = (state.scenario || []).slice().reverse().slice(0, 60);
  if (!events.length) { log.innerHTML = `<li><span class="t">–</span><span class="e">empty</span><span class="d">还没有事件</span></li>`; return; }
  log.innerHTML = events
    .map((e) => {
      const t = (e.at || "").slice(11, 19);
      const details = { ...e };
      delete details.at; delete details.event;
      return `<li><span class="t">${esc(t)}</span><span class="e">${esc(e.event)}</span><span class="d">${esc(JSON.stringify(details))}</span></li>`;
    })
    .join("");
}

// ------------------------------------------------------------------ output
function show(selector, html, isError = false) {
  const el = $(selector);
  el.classList.remove("hidden");
  el.innerHTML = html;
  if (isError) el.style.borderColor = "#5a2a2a"; else el.style.borderColor = "";
  el.scrollIntoView({ behavior: "smooth", block: "nearest" });
}

function planSummary(plan) {
  if (!plan || !plan.selected) return "";
  const s = plan.selected;
  const options = (plan.options || [])
    .map((o) => {
      const active = o === s || o.kind === s.kind && o.base_version === s.base_version;
      const label = o.kind === "full" ? "整包" : o.kind === "chain" ? "链式差分" : o.kind === "refetch-base" ? "补取旧段+差分" : "差分";
      const steps = o.steps && o.steps.length ? ` (${o.steps.map((x) => `${x.base}→${x.target}`).join(" → ")})` : "";
      return `<span class="chip ${active ? "full" : ""}">${label} · ${fmtBytes(o.download_bytes)}${steps}</span>`;
    })
    .join(" ");
  const reasons = (s.reasons || []).map((r) => `<li>${esc(r)}</li>`).join("");
  const warns = (s.warnings || []).map((w) => `<li class="warnings">⚠ ${esc(w)}</li>`).join("");
  const missing = (s.missing_base_segments || []).length ? `<li class="warnings">缺失旧段：${esc(s.missing_base_segments.join(", "))}</li>` : "";
  const corrupt = (s.corrupt_base_segments || []).length ? `<li class="warnings">损坏旧段：${esc(s.corrupt_base_segments.join(", "))}</li>` : "";
  return `
    <div class="plan-head">
      <span class="strategy-line">选中策略：<b>${esc(s.kind)}</b>${s.base_version ? `，基线 ${esc(s.base_version)}` : ""}</span>
      <span class="chip">下载 ${fmtBytes(s.download_bytes)}</span>
      <span class="chip">${s.download_segments} 段</span>
      <span class="chip">整包 ${fmtBytes(plan.target_bytes)}</span>
    </div>
    <div class="plan-head">可选策略：${options}</div>
    <ul class="reasons">${reasons}${warns}${missing}${corrupt}</ul>`;
}

function resultSummary(result) {
  const rows = [];
  rows.push(`<div class="plan-head"><span class="chip ${result.fell_back ? "warn" : "full"}">${esc(result.strategy_used)}</span>
    <span class="chip">下载 ${fmtBytes(result.downloaded_bytes)}</span>
    <span class="chip">${result.downloaded_segments ?? "–"} 段</span>
    <span class="chip ${result.verified ? "full" : "bad"}">${result.verified ? "最终内容校验通过" : "校验失败"}</span></div>`);
  if (result.fell_back) rows.push(`<ul class="reasons"><li class="warnings">已回退整包：${esc(result.fallback_reason)}</li></ul>`);
  if (result.damage) rows.push(`<ul class="reasons"><li class="warnings">升级前故障：${esc(result.damage.action)} ${esc(result.damage.segment)} @ ${esc(result.damage.version)}</li></ul>`);
  return rows.join("");
}

// ----------------------------------------------------------------- actions
async function action(button, fn) {
  button.disabled = true;
  try { await fn(); }
  catch (err) { toast(err.message, "bad"); }
  finally { button.disabled = false; }
}

$("#btn-reset").addEventListener("click", (e) => action(e.target, async () => {
  if (!confirm("清空发布目录、源文件和所有消费端？")) return;
  await api("/console/api/reset", { method: "POST", body: "{}" });
  $("#plan-output").classList.add("hidden");
  $("#query-output").classList.add("hidden");
  $("#gc-output").classList.add("hidden");
  toast("已重置为空目录", "good");
  await refresh();
}));

$("#btn-publish").addEventListener("click", (e) => action(e.target, async () => {
  const body = {
    count: Number($("#pub-count").value),
    edits: Number($("#pub-edits").value),
    skip_version: $("#pub-skip").checked,
    diff_mode: $("#pub-diffmode").value,
  };
  const res = await api("/console/api/publish", { method: "POST", body: JSON.stringify(body) });
  toast(`已发布 ${res.published.map((p) => p.version).join(", ")}`, "good");
  await refresh();
}));

$("#btn-plan").addEventListener("click", (e) => action(e.target, async () => {
  const body = {
    consumer: $("#op-consumer").value,
    version: $("#op-version").value,
    prefer: $("#op-prefer").value,
  };
  if (!body.version) throw new Error("发布目录还没有版本");
  const plan = await api("/console/api/plan", { method: "POST", body: JSON.stringify(body) });
  show("#plan-output", planSummary(plan));
}));

$("#btn-upgrade").addEventListener("click", (e) => action(e.target, async () => {
  const interrupt = $("#op-interrupt").value;
  const body = {
    consumer: $("#op-consumer").value,
    version: $("#op-version").value,
    prefer: $("#op-prefer").value,
    damage: $("#op-damage").value || null,
    max_blobs: interrupt ? Number(interrupt) : null,
  };
  if (!body.version) throw new Error("发布目录还没有版本");
  try {
    const result = await api("/console/api/upgrade", { method: "POST", body: JSON.stringify(body) });
    show("#plan-output", planSummary(result.plan) + resultSummary(result) +
      `<pre class="output" style="margin-top:10px">${esc(JSON.stringify(result, null, 2))}</pre>`);
    toast(`升级完成：${result.version}（${result.strategy_used}，${fmtBytes(result.downloaded_bytes)}）`, "good");
  } catch (err) {
    // An interrupted transfer is an expected, resumable outcome.
    show("#plan-output", `<ul class="reasons"><li class="warnings">传输中断：${esc(err.message)}</li>
      <li>已下载并校验的段保留在暂存区；再次执行会从断点继续，已安装版本未被改动。</li></ul>`, true);
    toast("传输中断（可续传）", "bad");
  }
  await refresh();
}));

$("#btn-query").addEventListener("click", (e) => action(e.target, async () => {
  const body = {
    consumer: $("#q-consumer").value,
    version: $("#q-version").value || null,
    category: $("#q-category").value || null,
    min_score: Number($("#q-minscore").value) || 0,
    text: $("#q-text").value || null,
    limit: Number($("#q-limit").value) || 8,
  };
  const res = await api("/console/api/query", { method: "POST", body: JSON.stringify(body) });
  const rows = res.records
    .map(
      (r) => `<tr><td class="mono">${esc(r.id)}</td><td>${esc(r.title)}</td><td>${esc(r.category)}</td><td class="mono">${Number(r.score).toFixed(2)}</td><td>${esc(r.body).slice(0, 80)}…</td></tr>`
    )
    .join("");
  show(
    "#query-output",
    `<div class="plan-head"><span class="chip">${res.count} 条</span><span class="chip">来自已安装版本的段文件</span></div>
     <table class="records"><thead><tr><th>id</th><th>标题</th><th>分类</th><th>分数</th><th>正文</th></tr></thead><tbody>${rows}</tbody></table>`
  );
}));

$("#btn-gc").addEventListener("click", (e) => action(e.target, async () => {
  const body = { keep_last: Number($("#gc-keep").value), dry_run: $("#gc-dry").checked };
  const res = await api("/console/api/gc", { method: "POST", body: JSON.stringify(body) });
  show(
    "#gc-output",
    `<div class="plan-head">
       <span class="chip">${res.dry_run ? "预演" : "已执行"}</span>
       <span class="chip">保留 ${esc((res.kept || []).join(", "))}</span>
       <span class="chip bad">移除版本 ${esc((res.removed_versions || []).join(", ") || "无")}</span>
       <span class="chip">释放 ${fmtBytes(res.freed_bytes)}</span>
       <span class="chip ${res.release_verify && res.release_verify.ok ? "full" : "bad"}">发布目录校验 ${res.release_verify && res.release_verify.ok ? "通过" : "失败"}</span>
     </div>
     <pre class="output" style="margin-top:10px">${esc(JSON.stringify(res, null, 2))}</pre>`
  );
  toast(res.dry_run ? "预演完成" : "清理完成，校验通过", "good");
  await refresh();
}));

// ------------------------------------------------------------------- boot
refresh();
setInterval(async () => {
  try {
    const data = await api("/console/api/state");
    state = data;
    renderConsumers();
    renderSelectors();
    renderEvents();
    renderRelease();
  } catch { /* status shown by manual refresh */ }
}, 4000);
