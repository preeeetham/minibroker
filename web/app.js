// minibroker live demo: talks to mb-web over one WebSocket.
"use strict";

const $ = (id) => document.getElementById(id);
const SVG = "http://www.w3.org/2000/svg";
const HISTORY = 60;

const state = {
  ws: null,
  retry: 0,
  pub: [], del: [], p99: [], conn: [],
  filters: new Set(),
  paused: false,
  feedCount: 0,
  fleetCount: 0, fleetRate: 0,
  stress: null,
};

/* ---------- formatting ---------- */
const nf = new Intl.NumberFormat("en-US");
function compact(n) {
  if (n >= 1e9) return (n / 1e9).toFixed(1) + "B";
  if (n >= 1e6) return (n / 1e6).toFixed(n >= 1e7 ? 0 : 1) + "M";
  if (n >= 1e4) return (n / 1e3).toFixed(0) + "k";
  return nf.format(Math.round(n));
}
function latency(us) {
  if (!us) return "—";
  return us >= 1000 ? (us / 1000).toFixed(us >= 10000 ? 0 : 1) + '<span class="unit">ms</span>'
                    : Math.round(us) + '<span class="unit">µs</span>';
}
function latencyText(us) {
  if (!us) return "—";
  return us >= 1000 ? (us / 1000).toFixed(1) + " ms" : Math.round(us) + " µs";
}
function duration(s) {
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = s % 60;
  return h ? `${h}h ${m}m` : m ? `${m}m ${sec}s` : `${sec}s`;
}
function clock(d = new Date()) {
  return d.toTimeString().slice(0, 8) + "." + String(d.getMilliseconds()).padStart(3, "0").slice(0, 2);
}

/* ---------- MQTT-style matching (mirrors src/protocol/topic.rs) ---------- */
function matches(filter, topic) {
  const f = filter.split("/"), t = topic.split("/");
  for (let i = 0; ; i++) {
    if (f[i] === "#") return true;
    if (i >= f.length || i >= t.length) return f.length === t.length && i === f.length;
    if (f[i] !== "+" && f[i] !== t[i]) return false;
  }
}

/* ---------- WebSocket ---------- */
function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  state.ws = ws;
  setStatus("connecting");
  ws.onopen = () => {
    state.retry = 0;
    setStatus("live");
    if (!state.filters.size && !state.autoSubscribed) {
      state.autoSubscribed = true;
      state.filters.add($("filter").value.trim());
    }
    for (const f of state.filters) send({ op: "sub", filter: f });
  };
  ws.onmessage = (e) => {
    let m;
    try { m = JSON.parse(e.data); } catch { return; }
    switch (m.t) {
      case "hello": $("session").textContent = "#" + m.session; break;
      case "history": onHistory(m); break;
      case "stats": onStats(m); break;
      case "msgs": onMessages(m); break;
      case "ack": onAck(m); break;
      case "error": toast(m.msg, "err"); break;
      case "stress": onStress(m); break;
    }
  };
  ws.onclose = () => {
    setStatus("reconnecting");
    const delay = Math.min(1000 * 2 ** state.retry++, 15000);
    setTimeout(connect, delay);
  };
}
function send(obj) {
  if (state.ws && state.ws.readyState === WebSocket.OPEN) { state.ws.send(JSON.stringify(obj)); return true; }
  toast("Not connected yet. Retrying…", "err");
  return false;
}
function setStatus(s) {
  const pill = $("status-pill");
  pill.classList.toggle("live", s === "live");
  $("status-text").textContent = s === "live" ? "Live" : s === "connecting" ? "Connecting" : "Reconnecting";
}

/* ---------- stats → KPIs, chart, sparklines ---------- */
function push(arr, v) { arr.push(v); if (arr.length > HISTORY) arr.shift(); }

function onHistory(h) {
  state.pub = h.pub.slice(-HISTORY);
  state.del = h.del.slice(-HISTORY);
  state.p99 = h.p99.slice(-HISTORY);
  state.conn = h.conn.slice(-HISTORY);
  spark("spark-pub", state.pub, "var(--series-1)");
  spark("spark-del", state.del, "var(--series-2)");
  spark("spark-lat", state.p99, "var(--text-2)");
  spark("spark-conn", state.conn, "var(--text-2)");
  drawChart();
}

function onStats(s) {
  push(state.pub, s.published_ps);
  push(state.del, s.delivered_ps);
  push(state.p99, s.p99_us);
  push(state.conn, s.connections);

  $("kpi-pub").textContent = compact(s.published_ps);
  $("kpi-pub-total").textContent = compact(s.publishes_total);
  $("kpi-del").textContent = compact(s.delivered_ps);
  $("kpi-del-total").textContent = compact(s.deliveries_total);
  $("kpi-fanout").textContent = s.published_ps ? (s.delivered_ps / s.published_ps).toFixed(1) + "×" : "—";
  $("kpi-p99").innerHTML = latency(s.p99_us);
  $("kpi-p50").textContent = latencyText(s.p50_us);
  $("kpi-conn").textContent = nf.format(s.connections);
  $("kpi-subs").textContent = nf.format(s.subscriptions);
  $("kpi-kicks").textContent = nf.format(s.slow_kicks);
  $("uptime").textContent = duration(s.uptime_s);

  spark("spark-pub", state.pub, "var(--series-1)");
  spark("spark-del", state.del, "var(--series-2)");
  spark("spark-lat", state.p99, "var(--text-2)");
  spark("spark-conn", state.conn, "var(--text-2)");
  drawChart();

  // once-a-second client-side rates
  $("feed-rate").textContent = `${compact(state.feedCount)} msg/s`;
  state.fleetRate = state.fleetCount;
  $("fleet-rate").textContent = `${compact(state.fleetCount)} msg/s`;
  $("term-rate").textContent = compact(state.fleetCount);
  if (state.stress) state.stress.minFleet = Math.min(state.stress.minFleet, state.fleetCount);
  state.feedCount = 0;
  state.fleetCount = 0;
}

function spark(id, data, color) {
  const svg = $(id);
  if (data.length < 2) return;
  const pts = data.slice(-30), max = Math.max(...pts, 1), min = Math.min(...pts, 0);
  const x = (i) => (i / (pts.length - 1)) * 120;
  const y = (v) => 30 - ((v - min) / (max - min || 1)) * 26;
  const line = pts.map((v, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join("");
  svg.innerHTML =
    `<defs><linearGradient id="${id}-g" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stop-color="${color}" stop-opacity=".22"/><stop offset="1" stop-color="${color}" stop-opacity="0"/></linearGradient></defs>` +
    `<path d="${line}L120,32L0,32Z" fill="url(#${id}-g)"/>` +
    `<path d="${line}" fill="none" stroke="${color}" stroke-width="1.6" stroke-linejoin="round" stroke-linecap="round" vector-effect="non-scaling-stroke"/>`;
}

/* ---------- main chart ---------- */
const chart = { w: 0, h: 0, m: { l: 44, r: 58, t: 14, b: 26 }, max: 1 };

function niceMax(v) {
  if (v <= 0) return 10;
  const p = 10 ** Math.floor(Math.log10(v)), n = v / p;
  return (n <= 1 ? 1 : n <= 2 ? 2 : n <= 2.5 ? 2.5 : n <= 5 ? 5 : 10) * p;
}

function drawChart() {
  const svg = $("chart-svg"), box = $("chart");
  const w = box.clientWidth, h = box.clientHeight, { m } = chart;
  chart.w = w; chart.h = h;
  svg.setAttribute("viewBox", `0 0 ${w} ${h}`);
  const n = state.pub.length;
  if (n < 2) {
    svg.innerHTML = `<text class="skeleton" x="${w / 2}" y="${h / 2}" text-anchor="middle">Collecting live data…</text>`;
    return;
  }
  const max = niceMax(Math.max(...state.pub, ...state.del) * 1.1);
  chart.max = max;
  const iw = w - m.l - m.r, ih = h - m.t - m.b;
  const x = (i) => m.l + (i + (HISTORY - n)) / (HISTORY - 1) * iw;
  const y = (v) => m.t + ih - (v / max) * ih;

  let out = '<g class="grid">';
  for (let k = 0; k <= 4; k++) {
    const v = (max / 4) * k, yy = y(v).toFixed(1);
    out += `<line x1="${m.l}" x2="${w - m.r}" y1="${yy}" y2="${yy}"/>`;
  }
  out += '</g><g class="axis">';
  for (let k = 0; k <= 4; k++) {
    const v = (max / 4) * k;
    out += `<text x="${m.l - 10}" y="${(y(v) + 4).toFixed(1)}" text-anchor="end">${compact(v)}</text>`;
  }
  for (const s of [60, 45, 30, 15, 0]) {
    const xx = m.l + ((HISTORY - 1 - s) / (HISTORY - 1)) * iw;
    out += `<text x="${xx.toFixed(1)}" y="${h - 6}" text-anchor="middle">${s ? "-" + s + "s" : "now"}</text>`;
  }
  out += "</g>";

  const series = [
    { data: state.del, color: "var(--series-2)", id: "del" },
    { data: state.pub, color: "var(--series-1)", id: "pub" },
  ];
  out += "<defs>" + series.map((s) =>
    `<linearGradient id="g-${s.id}" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stop-color="${s.color}" stop-opacity=".22"/><stop offset="1" stop-color="${s.color}" stop-opacity="0"/></linearGradient>`
  ).join("") + "</defs>";

  const labels = [];
  for (const s of series) {
    const line = s.data.map((v, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join("");
    out += `<path d="${line}L${x(n - 1).toFixed(1)},${y(0)}L${x(0).toFixed(1)},${y(0)}Z" fill="url(#g-${s.id})"/>`;
    out += `<path d="${line}" fill="none" stroke="${s.color}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>`;
    const last = s.data[n - 1];
    out += `<circle cx="${x(n - 1)}" cy="${y(last)}" r="4" fill="${s.color}" stroke="var(--surface)" stroke-width="2"/>`;
    labels.push({ y: y(last), text: compact(last) });
  }
  // direct end labels, nudged apart if they collide
  labels.sort((a, b) => a.y - b.y);
  if (labels.length === 2 && labels[1].y - labels[0].y < 14) {
    const mid = (labels[0].y + labels[1].y) / 2;
    labels[0].y = mid - 7; labels[1].y = mid + 7;
  }
  for (const l of labels) out += `<text class="end-label" x="${x(n - 1) + 10}" y="${(l.y + 4).toFixed(1)}">${l.text}</text>`;

  out += `<g id="hover" style="display:none"><line class="cross" id="cross" y1="${m.t}" y2="${m.t + ih}"/>` +
         `<circle class="hover-dot" id="hd-pub" r="4.5" fill="var(--series-1)"/>` +
         `<circle class="hover-dot" id="hd-del" r="4.5" fill="var(--series-2)"/></g>`;
  out += `<rect id="hit" x="${m.l}" y="${m.t}" width="${iw}" height="${ih}" fill="transparent"/>`;
  svg.innerHTML = out;

  const hit = $("hit");
  hit.onmousemove = (e) => hover(e, x, y, iw);
  hit.onmouseleave = () => { $("hover").style.display = "none"; $("chart-tip").hidden = true; };
}

function hover(e, x, y, iw) {
  const n = state.pub.length, rect = $("chart-svg").getBoundingClientRect();
  const px = e.clientX - rect.left;
  const slot = Math.round(((px - chart.m.l) / iw) * (HISTORY - 1)) - (HISTORY - n);
  const i = Math.max(0, Math.min(n - 1, slot));
  const cx = x(i);
  $("hover").style.display = "";
  $("cross").setAttribute("x1", cx); $("cross").setAttribute("x2", cx);
  $("hd-pub").setAttribute("cx", cx); $("hd-pub").setAttribute("cy", y(state.pub[i]));
  $("hd-del").setAttribute("cx", cx); $("hd-del").setAttribute("cy", y(state.del[i]));
  const tip = $("chart-tip");
  const ago = n - 1 - i;
  tip.innerHTML =
    `<div class="tt-time">${ago ? ago + " s ago" : "now"}</div>` +
    `<div class="tt-row"><span><i class="swatch" style="--c:var(--series-1)"></i>Published</span><b>${nf.format(state.pub[i])}/s</b></div>` +
    `<div class="tt-row"><span><i class="swatch" style="--c:var(--series-2)"></i>Delivered</span><b>${nf.format(state.del[i])}/s</b></div>`;
  tip.style.left = Math.max(80, Math.min(chart.w - 80, cx)) + "px";
  tip.style.top = Math.min(y(state.pub[i]), y(state.del[i])) + "px";
  tip.hidden = false;
}

/* ---------- fleet tiles ---------- */
const tiles = new Map();
function buildFleet(n = 24) {
  const root = $("fleet");
  for (let i = 1; i <= n; i++) {
    const id = "ev-" + String(i).padStart(2, "0");
    const el = document.createElement("div");
    el.className = "ev";
    el.innerHTML = `<div class="ev-top"><span class="ev-id">${id}</span><span class="ev-batt-pct">—</span></div>` +
                   `<div class="ev-speed">—<small>km/h</small></div><div class="ev-bar"><i style="width:0%"></i></div>`;
    root.appendChild(el);
    tiles.set(id, { el, speed: el.querySelector(".ev-speed"), pct: el.querySelector(".ev-batt-pct"), bar: el.querySelector(".ev-bar i"), lastFlash: 0 });
  }
}
function updateTile(topic, payload) {
  const [, id, kind] = topic.split("/");
  const t = tiles.get(id);
  if (!t) return;
  if (kind === "speed") {
    t.speed.firstChild.nodeValue = Math.round(parseFloat(payload));
    const now = performance.now();
    if (now - t.lastFlash > 900) {
      t.lastFlash = now;
      t.el.classList.add("flash");
      requestAnimationFrame(() => requestAnimationFrame(() => t.el.classList.remove("flash")));
    }
  } else if (kind === "battery") {
    const b = parseFloat(payload);
    t.pct.textContent = (b < 20 ? "⚠ " : "") + Math.round(b) + "%";
    t.bar.style.width = b + "%";
    t.el.classList.toggle("low", b < 40 && b >= 20);
    t.el.classList.toggle("crit", b < 20);
  }
}

/* ---------- hero terminal: a readable sample of the live stream ---------- */
const TERM_LINES = 15;
let termLast = 0;
function termSample(batch) {
  const now = performance.now();
  if (now - termLast < 160 || !batch.length) return; // ~6 lines/s: readable, still alive
  const fleet = batch.filter(([t]) => t.startsWith("fleet/"));
  if (!fleet.length) return;
  termLast = now;
  const [topic, payload] = fleet[Math.floor(Math.random() * fleet.length)];
  const [, id, kind] = topic.split("/");
  const line = document.createElement("div");
  line.className = "term-line";
  const t = document.createElement("span"); t.className = "t"; t.textContent = clock().slice(0, 8);
  const tp = document.createElement("span"); tp.className = "tp";
  const idEl = document.createElement("span"); idEl.className = "id"; idEl.textContent = id;
  const k = document.createElement("span"); k.className = "k"; k.textContent = kind;
  tp.append("fleet/", idEl, "/", k);
  const pl = document.createElement("span"); pl.className = "pl";
  pl.textContent = kind === "speed" ? `${payload} km/h` : kind === "battery" ? `${payload} %` : payload;
  line.append(t, tp, pl);
  const term = $("term");
  term.prepend(line);
  while (term.children.length > TERM_LINES) term.lastElementChild.remove();
}

/* ---------- message feed ---------- */
const MAX_ROWS = 200;
function onMessages(m) {
  const rows = $("feed-rows");
  const frag = document.createDocumentFragment();
  const now = clock();
  let added = 0;
  termSample(m.m);
  for (const [topic, payload] of m.m) {
    if (topic.startsWith("fleet/")) { state.fleetCount++; updateTile(topic, payload); }
    if (state.paused || !state.filters.size) continue;
    let hit = false;
    for (const f of state.filters) if (matches(f, topic)) { hit = true; break; }
    if (!hit) continue;
    state.feedCount++;
    if (added >= 60) continue; // render cap per batch; the rate chip still counts all
    const row = document.createElement("div");
    row.className = "feed-row new";
    const t = document.createElement("span"); t.className = "t"; t.textContent = now;
    const tp = document.createElement("span"); tp.className = "tp"; tp.textContent = topic; tp.title = topic;
    const pl = document.createElement("span"); pl.className = "pl"; pl.textContent = payload; pl.title = payload;
    row.append(t, tp, pl);
    frag.prepend(row);
    added++;
  }
  if (added) {
    $("feed-empty").hidden = true;
    rows.prepend(frag);
    // Trim only rows (never the placeholder, which lives outside #feed-rows).
    while (rows.childElementCount > MAX_ROWS) rows.lastElementChild.remove();
  }
}

function renderSubs() {
  const root = $("active-subs");
  root.replaceChildren();
  for (const f of state.filters) {
    const tag = document.createElement("span");
    tag.className = "sub-tag";
    const label = document.createElement("span"); label.textContent = f;
    const x = document.createElement("button");
    x.type = "button"; x.setAttribute("aria-label", `Unsubscribe from ${f}`);
    x.innerHTML = '<svg viewBox="0 0 12 12" width="10" height="10"><path d="M2 2l8 8M10 2l-8 8" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/></svg>';
    x.onclick = () => send({ op: "unsub", filter: f });
    tag.append(label, x);
    root.appendChild(tag);
  }
  if (!state.filters.size) {
    $("feed-empty").hidden = false;
    $("feed-empty").textContent = "Subscribe to a filter and messages will stream in here.";
  }
}

function onAck(m) {
  if (m.op === "sub") { state.filters.add(m.filter); renderSubs(); }
  if (m.op === "unsub") { state.filters.delete(m.filter); renderSubs(); }
  if (m.op === "pub") toast(`Published to ${m.topic}`, "ok");
}

let toastTimer;
function toast(text, kind) {
  const el = $("toast");
  el.className = "toast " + kind;
  el.textContent = (kind === "ok" ? "✓ " : "⚠ ") + text;
  el.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => (el.hidden = true), 3500);
}

/* ---------- backpressure demo ---------- */
function setStep(n, cls, val) {
  const el = document.querySelector(`.step[data-step="${n}"]`);
  el.classList.remove("active", "done", "kick");
  if (cls) el.classList.add(cls);
  if (val !== undefined) $("v" + n).textContent = val;
}
function onStress(m) {
  const btn = $("stress-btn");
  if (m.phase === "busy") {
    $("stress-note").textContent = "Another visitor is running it right now. Try again in a few seconds.";
    btn.disabled = false;
    return;
  }
  if (m.phase === "start") {
    state.stress = { minFleet: Infinity };
    $("s1").textContent = `1 KiB messages to ${m.topic}`;
    $("stress-result").hidden = true;
    [1, 2, 3, 4].forEach((i) => setStep(i, null, "—"));
    setStep(1, "active", "0");
  }
  if (m.phase === "flood") {
    setStep(1, "done", compact(m.sent) + " sent");
    if (m.dropped === 0) setStep(2, "active", "filling…");
    else { setStep(2, "done", "full"); setStep(3, m.kicked ? "done" : "active", compact(m.dropped) + " dropped"); }
    if (m.kicked) setStep(4, "kick", (m.ms / 1000).toFixed(1) + " s");
  }
  if (m.phase === "done") {
    btn.disabled = false;
    if (m.kicked) {
      setStep(3, "done", compact(m.dropped) + " dropped");
      setStep(4, "kick", (m.kick_ms / 1000).toFixed(1) + " s");
    }
    const fleet = state.stress && isFinite(state.stress.minFleet) ? state.stress.minFleet : state.fleetRate;
    const res = $("stress-result");
    res.innerHTML = m.kicked
      ? `Sent <b>${nf.format(m.sent)}</b> messages. The stalled subscriber had received <b>${nf.format(m.received)}</b> when the broker started dropping. It dropped <b>${nf.format(m.dropped)}</b> for it, then disconnected it after <b>${(m.kick_ms / 1000).toFixed(1)} s</b> (${m.reason === "SlowConsumer" ? "<code>ERROR SlowConsumer</code>" : "socket closed mid-write, so no error frame could be sent"}). Your fleet stream never dropped below <b>${nf.format(fleet)} msg/s</b> while this ran.`
      : `Sent <b>${nf.format(m.sent)}</b> messages; the subscriber was not disconnected within the time limit (${m.reason}).`;
    res.hidden = false;
    state.stress = null;
  }
  if (m.phase === "error") {
    btn.disabled = false;
    toast("Demo failed: " + m.msg, "err");
  }
}

/* ---------- wiring ---------- */
function init() {
  buildFleet();
  drawChart();
  $("sub-form").addEventListener("submit", (e) => {
    e.preventDefault();
    const f = $("filter").value.trim();
    if (f) send({ op: "sub", filter: f });
  });
  document.querySelectorAll(".preset").forEach((b) =>
    b.addEventListener("click", () => { $("filter").value = b.dataset.filter; send({ op: "sub", filter: b.dataset.filter }); }));
  $("pub-form").addEventListener("submit", (e) => {
    e.preventDefault();
    send({ op: "pub", topic: $("pub-topic").value.trim(), payload: $("pub-payload").value });
  });
  $("pause").addEventListener("click", (e) => {
    state.paused = !state.paused;
    e.currentTarget.setAttribute("aria-pressed", state.paused);
    e.currentTarget.textContent = state.paused ? "Resume" : "Pause";
  });
  $("clear").addEventListener("click", () => {
    $("feed-rows").replaceChildren();
  });
  $("stress-btn").addEventListener("click", (e) => {
    if (send({ op: "stress" })) {
      e.currentTarget.disabled = true;
      $("stress-note").textContent = "Running… watch the fleet and chart above keep moving.";
    }
  });
  let raf;
  window.addEventListener("resize", () => { cancelAnimationFrame(raf); raf = requestAnimationFrame(drawChart); });
  connect();
}
document.addEventListener("DOMContentLoaded", init);
