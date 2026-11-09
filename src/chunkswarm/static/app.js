// ChunkSwarm dashboard. Every name that came from another peer is inserted
// with textContent (never parsed as HTML), so it can never run as code.
"use strict";

const TOKEN = document.querySelector('meta[name="cs-token"]').content;
const $ = (id) => document.getElementById(id);
const palette = new Map();          // "ip:port" -> colour class c0..c5

function el(tag, text, cls) {
  const e = document.createElement(tag);
  if (text !== undefined && text !== null) e.textContent = text;
  if (cls) e.className = cls;
  return e;
}

function size(n) {
  const units = ["B", "KB", "MB", "GB"];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return i === 0 ? `${n} B` : `${n.toFixed(1)} ${units[i]}`;
}

function colourOf(label) {
  if (!palette.has(label)) palette.set(label, `c${palette.size % 6}`);
  return palette.get(label);
}

async function api(path, options = {}) {
  const opts = { ...options, headers: { ...(options.headers || {}) } };
  if (opts.method === "POST") {
    opts.headers["X-ChunkSwarm-Token"] = TOKEN;
    opts.headers["Content-Type"] = "application/json";
  }
  const res = await fetch(path, opts);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
}

// ---------------------------------------------------------------- status
async function refreshStatus() {
  try {
    const s = await api("/api/status");
    $("peer-name").textContent = `Peer ${s.name}`;
    document.title = `Peer ${s.name}: ChunkSwarm`;
    $("peer-sub").textContent = `Sending files on port ${s.port}. Tracker at ${s.tracker}.`;
    const facts = $("facts");
    facts.replaceChildren();
    const t = el("li", s.tracker_ok ? "Tracker online" : "Tracker unreachable", s.tracker_ok ? "" : "off");
    t.prepend(el("span", null, "dot"));
    facts.append(t, el("li", `Sharing ${s.files_shared} file${s.files_shared === 1 ? "" : "s"}`),
                 el("li", `Uploaded ${size(s.bytes_served)}`));
  } catch (e) {
    $("peer-sub").textContent = `Lost contact with this peer: ${e.message}`;
  }
}

async function refreshFiles() {
  const { files } = await api("/api/files");
  const list = $("files");
  list.replaceChildren(...files.map((f) => {
    const li = el("li");
    li.append(el("span", f.name, "name"), el("span", `${size(f.size)}, ${f.chunks} chunk${f.chunks === 1 ? "" : "s"}`, "meta"));
    return li;
  }));
  $("files-empty").hidden = files.length > 0;
}

// ---------------------------------------------------------------- search
$("search-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const q = $("q").value.trim();
  const msg = $("search-msg");
  msg.textContent = "Searching…";
  try {
    const { results } = await api(`/api/search?q=${encodeURIComponent(q)}`);
    $("results").replaceChildren(...results.map(resultRow));
    msg.textContent = results.length ? "" : (q ? `Nobody is sharing a file matching “${q}” right now.`
                                               : "No other peer is sharing anything right now.");
  } catch (e) {
    msg.textContent = `Search failed: ${e.message}`;
  }
});

function resultRow(r) {
  const li = el("li");
  const btn = el("button", "Download");
  btn.type = "button";
  btn.addEventListener("click", async () => {
    btn.disabled = true;
    try {
      await api("/api/downloads", { method: "POST", body: JSON.stringify({ file_id: r.file_id }) });
      btn.textContent = "Started";
      setTimeout(() => { btn.textContent = "Download"; btn.disabled = false; }, 2000);
      pollSoon();
    } catch (e) {
      btn.disabled = false;
      $("search-msg").textContent = `Could not start the download: ${e.message}`;
    }
  });
  li.append(el("span", r.name, "name"), el("span", size(r.size), "meta size"),
            el("span", `${r.sources} peer${r.sources === 1 ? "" : "s"}`, "meta peers"), btn);
  return li;
}

// ---------------------------------------------------------------- downloads
let timer = null;
let shownJob = null;

function pollSoon() { clearTimeout(timer); timer = setTimeout(refreshDownloads, 100); }

async function refreshDownloads() {
  let running = false;
  try {
    const { jobs, history } = await api("/api/downloads");
    if (jobs.length) {
      drawJob(jobs[0]);
      running = jobs.some((j) => j.status === "running");
    }
    drawHistory(history);
  } catch (e) {
    $("cur-msg").textContent = `Could not read downloads: ${e.message}`;
  }
  clearTimeout(timer);
  timer = setTimeout(refreshDownloads, running ? 500 : 3000);
}

function drawJob(j) {
  const map = $("map");
  if (shownJob !== j.job_id) {                 // new job: build one tile per chunk
    shownJob = j.job_id;
    map.replaceChildren(...j.owners.map(() => document.createElement("i")));
    map.classList.toggle("dense", j.total > 400);
  }
  j.owners.forEach((owner, i) => {
    const cls = owner === null ? "" : owner === "resumed" ? "resumed" : colourOf(owner);
    if (map.children[i].className !== cls) map.children[i].className = cls;
  });
  const pct = j.total ? Math.floor((j.done * 100) / j.total) : 100;
  $("cur-name").textContent = j.name;
  $("cur-pct").textContent = `${pct}%`;
  map.setAttribute("aria-label", `${j.done} of ${j.total} chunks received`);
  $("current-h").textContent = j.status === "running" ? "Downloading" : j.status === "done" ? "Downloaded" : "Download failed";
  document.querySelector(".current").classList.toggle("failed", j.status === "failed");

  const msg = $("cur-msg");
  msg.classList.remove("bad");
  if (j.status === "running") {
    $("cur-speed").textContent = `${j.speed.toFixed(1)} MB/s from ${j.sources.length} peer${j.sources.length === 1 ? "" : "s"}`;
    msg.textContent = j.resumed ? `${j.resumed} chunks were already on disk from an earlier attempt.` : "";
  } else if (j.status === "done") {
    $("cur-speed").textContent = `${j.mb_per_s} MB/s average`;
    msg.textContent = `Saved to ${j.path} in ${j.seconds} s. Every chunk and the whole file passed the SHA-256 check.`;
  } else {
    $("cur-speed").textContent = "";
    msg.textContent = `${j.error}. Chunks already received are kept: download again to resume.`;
    msg.classList.add("bad");
  }

  const counts = new Map();
  j.owners.forEach((o) => { if (o) counts.set(o, (counts.get(o) || 0) + 1); });
  const legend = $("legend");
  legend.replaceChildren(...[...counts].map(([label, n]) => {
    const li = el("li");
    const swatch = el("i", null, label === "resumed" ? "resumed" : colourOf(label));
    li.append(swatch, document.createTextNode(
      label === "resumed" ? `Already on disk: ${n}` : `${label}: ${n} chunk${n === 1 ? "" : "s"}`));
    return li;
  }));
  Object.keys(j.failed_sources).forEach((label) => {
    legend.append(el("li", `${label} dropped: ${j.failed_sources[label]}`, "gone"));
  });
}

function drawHistory(rows) {
  $("history").replaceChildren(...rows.map((r) => {
    const tr = el("tr");
    tr.append(el("td", r.name, "name"),
              el("td", r.status === "done" ? "Verified" : "Failed", r.status === "done" ? "" : "failed"),
              el("td", r.seconds === null ? "" : `${r.seconds} s`, "num"),
              el("td", r.mb_per_s === null ? "" : `${r.mb_per_s} MB/s`, "num"),
              el("td", String(r.sources ?? ""), "num"));
    if (r.error) tr.title = r.error;
    return tr;
  }));
  $("hist-empty").hidden = rows.length > 0;
}

$("rescan").addEventListener("click", async () => {
  try {
    await api("/api/rescan", { method: "POST", body: "{}" });
    await refreshFiles();
    refreshStatus();
  } catch (e) {
    $("files-empty").hidden = false;
    $("files-empty").textContent = `Rescan failed: ${e.message}`;
  }
});

refreshStatus();
refreshFiles();
refreshDownloads();
setInterval(refreshStatus, 3000);
