"""
thomasnet.com Scraper — Flask Web App
Run: python app.py
Then open http://localhost:5000
"""

import json
import os
import queue
import threading
import time
import uuid
from pathlib import Path

from flask import Flask, Response, jsonify, render_template_string, request, stream_with_context

from scraper import build_url, enrich_contact, get_driver, wait_for_results, parse_card
from selenium.webdriver.common.by import By

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Server-side batch state — persists while the server is running.
# Results are also written to batch_results.json so they survive a page reload.
# ---------------------------------------------------------------------------
RESULTS_FILE = Path(__file__).parent / "batch_results.json"

_batch_lock = threading.Lock()
_batch_state = {
    "running": False,
    "keywords": [],        # full list for this run
    "current_idx": -1,     # which keyword is active (-1 = idle)
    "phase": "",           # "scraping" | "enriching" | ""
    "results": [],         # every row from every keyword
    "keyword_log": [],     # [{keyword, status, count}]
    "seen_companies": set(),
    "enrichment_cache": {},  # website_key → {"emails": [...], "phones": [...]}
}

# SSE subscribers — each /batch/stream request gets its own queue
_subscribers: list[queue.Queue] = []
_subscribers_lock = threading.Lock()


def _load_saved_results():
    """Load any persisted results from a previous run."""
    if RESULTS_FILE.exists():
        try:
            data = json.loads(RESULTS_FILE.read_text(encoding="utf-8"))
            _batch_state["results"] = data.get("results", [])
            _batch_state["keyword_log"] = data.get("keyword_log", [])
            _batch_state["enrichment_cache"] = data.get("enrichment_cache", {})
        except Exception:
            pass


def _save_results():
    """Persist current results to disk so the user can reload the page."""
    try:
        RESULTS_FILE.write_text(
            json.dumps({
                "results": _batch_state["results"],
                "keyword_log": _batch_state["keyword_log"],
                "enrichment_cache": _batch_state["enrichment_cache"],
            }, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        pass


def _broadcast(event_type: str, payload):
    """Push an SSE event to every connected subscriber."""
    with _subscribers_lock:
        dead = []
        for q in _subscribers:
            try:
                q.put_nowait((event_type, payload))
            except queue.Full:
                dead.append(q)
        for q in dead:
            _subscribers.remove(q)


# ---------------------------------------------------------------------------
# Background worker
# ---------------------------------------------------------------------------

def _run_batch(keywords: list[str], sponsored_only: bool):
    with _batch_lock:
        _batch_state["running"] = True
        _batch_state["keywords"] = keywords
        _batch_state["current_idx"] = -1
        _batch_state["phase"] = ""
        _batch_state["results"] = []
        _batch_state["keyword_log"] = []
        _batch_state["seen_companies"] = set()
        # NOTE: enrichment_cache is intentionally NOT reset — it persists across runs
        # so companies already enriched in a previous run are never re-fetched.

    _broadcast("batch_start", {"total": len(keywords)})

    for idx, keyword in enumerate(keywords):
        with _batch_lock:
            _batch_state["current_idx"] = idx
            _batch_state["phase"] = "scraping"
            _batch_state["keyword_log"].append({"keyword": keyword, "status": "scraping", "count": 0})

        _broadcast("keyword_start", {"idx": idx, "keyword": keyword, "total": len(keywords)})

        keyword_results = []
        try:
            # --- Scrape thomasnet ---
            _broadcast("status", f"[{idx+1}/{len(keywords)}] Scraping: {keyword}")
            driver = get_driver(headless=False)
            try:
                driver.get(build_url(keyword))
                time.sleep(5)
                if not wait_for_results(driver):
                    raise RuntimeError("Results list did not load — may be rate-limited.")
                time.sleep(3)
                cards = driver.find_elements(By.CSS_SELECTOR, "li[data-sentry-component='SearchResultSupplier']")
                for card in cards:
                    data = parse_card(card)
                    if sponsored_only and not data["sponsored"]:
                        continue

                    # Dedup by normalised website URL, fallback to company name
                    dedup_key = (data.get("website") or data.get("company_name", "")).lower().rstrip("/")
                    with _batch_lock:
                        if dedup_key in _batch_state["seen_companies"]:
                            continue
                        _batch_state["seen_companies"].add(dedup_key)

                    data["_keyword"] = keyword
                    data["_emails"] = []
                    data["_phones"] = []
                    # UUID for reliable DOM targeting — never confused with array indices
                    data["_row_id"] = str(uuid.uuid4())

                    # Compute index and append atomically inside the lock
                    with _batch_lock:
                        result_idx = len(_batch_state["results"])
                        data["_result_idx"] = result_idx
                        _batch_state["results"].append(data)

                    keyword_results.append(data)
                    _broadcast("result", {"row": data, "result_idx": result_idx})
            finally:
                driver.quit()

            # Update keyword log with scraped count
            with _batch_lock:
                _batch_state["keyword_log"][idx]["count"] = len(keyword_results)
                _batch_state["keyword_log"][idx]["status"] = "enriching"
                _batch_state["phase"] = "enriching"

            _broadcast("keyword_enriching", {"idx": idx, "count": len(keyword_results)})

            # --- Enrich each result ---
            for i, row in enumerate(keyword_results):
                website = row.get("website", "")
                cache_key = website.lower().rstrip("/") if website else ""

                _broadcast("status", f"[{idx+1}/{len(keywords)}] Enriching {i+1}/{len(keyword_results)}: {row['company_name']}")

                # Check enrichment cache first — skip HTTP request if already done
                with _batch_lock:
                    cached = _batch_state["enrichment_cache"].get(cache_key) if cache_key else None

                if cached:
                    contact = cached
                else:
                    contact = enrich_contact(website)
                    if cache_key:
                        with _batch_lock:
                            _batch_state["enrichment_cache"][cache_key] = contact
                    time.sleep(0.5)  # polite delay only when actually fetching

                row["_emails"] = contact["emails"]
                row["_phones"] = contact["phones"]

                result_idx = row["_result_idx"]
                with _batch_lock:
                    _batch_state["results"][result_idx]["_emails"] = contact["emails"]
                    _batch_state["results"][result_idx]["_phones"] = contact["phones"]

                # Use row_id (UUID) for DOM targeting — no index confusion possible
                _broadcast("enriched", {
                    "row_id": row["_row_id"],
                    "result_idx": result_idx,
                    "emails": contact["emails"],
                    "phones": contact["phones"],
                })

            with _batch_lock:
                _batch_state["keyword_log"][idx]["status"] = "done"

            _broadcast("keyword_done", {"idx": idx, "keyword": keyword, "count": len(keyword_results)})

        except Exception as ex:
            with _batch_lock:
                if idx < len(_batch_state["keyword_log"]):
                    _batch_state["keyword_log"][idx]["status"] = "error"
                else:
                    _batch_state["keyword_log"].append({"keyword": keyword, "status": "error", "count": 0})
            _broadcast("keyword_error", {"idx": idx, "keyword": keyword, "error": str(ex)})

        _save_results()

        # Polite gap between keywords
        if idx < len(keywords) - 1:
            time.sleep(5)

    with _batch_lock:
        _batch_state["running"] = False
        _batch_state["current_idx"] = -1
        _batch_state["phase"] = ""

    _broadcast("batch_done", {"total_results": len(_batch_state["results"])})


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------

HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>ThomasNet Scraper</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #f4f6f9; color: #1a1a2e; min-height: 100vh; }

    header { background: #1a1a2e; color: white; padding: 20px 32px; display: flex; align-items: center; gap: 14px; }
    header h1 { font-size: 1.3rem; font-weight: 600; }

    /* Tabs */
    .tabs { background: #1a1a2e; padding: 0 32px; display: flex; gap: 0; border-top: 1px solid rgba(255,255,255,.1); }
    .tab-btn { color: rgba(255,255,255,.5); background: none; border: none; padding: 12px 20px; font-size: 0.9rem; font-weight: 500; cursor: pointer; border-bottom: 2px solid transparent; transition: all .2s; }
    .tab-btn.active { color: white; border-bottom-color: #818cf8; }
    .tab-btn:hover { color: rgba(255,255,255,.85); }

    .container { max-width: 1400px; margin: 0 auto; padding: 28px 24px; }
    .tab-panel { display: none; }
    .tab-panel.active { display: block; }

    /* Cards */
    .card { background: white; border-radius: 10px; padding: 24px; box-shadow: 0 1px 4px rgba(0,0,0,.08); margin-bottom: 22px; }

    /* Form elements */
    .field { display: flex; flex-direction: column; gap: 6px; }
    .field label { font-size: 0.78rem; font-weight: 700; text-transform: uppercase; letter-spacing: .05em; color: #666; }
    input[type="text"] { border: 1.5px solid #ddd; border-radius: 7px; padding: 10px 14px; font-size: 1rem; outline: none; transition: border-color .2s; }
    input[type="text"]:focus { border-color: #4f46e5; }
    textarea { border: 1.5px solid #ddd; border-radius: 7px; padding: 12px 14px; font-size: 0.9rem; font-family: monospace; width: 100%; resize: vertical; min-height: 160px; outline: none; transition: border-color .2s; line-height: 1.6; }
    textarea:focus { border-color: #4f46e5; }

    .row { display: flex; gap: 12px; align-items: flex-end; flex-wrap: wrap; }
    .toggle-label { display: flex; align-items: center; gap: 8px; cursor: pointer; font-size: 0.9rem; color: #444; padding: 10px 0; }
    input[type="checkbox"] { width: 16px; height: 16px; accent-color: #4f46e5; }

    .btn { border: none; border-radius: 7px; padding: 10px 22px; font-size: 0.95rem; font-weight: 600; cursor: pointer; transition: background .2s; white-space: nowrap; }
    .btn-primary { background: #4f46e5; color: white; }
    .btn-primary:hover { background: #4338ca; }
    .btn-primary:disabled { background: #a5b4fc; cursor: not-allowed; }
    .btn-green { background: #10b981; color: white; text-decoration: none; display: inline-block; }
    .btn-green:hover { background: #059669; }
    .btn-danger { background: #ef4444; color: white; }
    .btn-danger:hover { background: #dc2626; }
    .btn-sm { padding: 6px 14px; font-size: 0.82rem; }

    /* Status bar */
    .status-bar { background: white; border-radius: 10px; padding: 16px 22px; box-shadow: 0 1px 4px rgba(0,0,0,.08); margin-bottom: 22px; display: none; align-items: center; gap: 12px; }
    .status-bar.visible { display: flex; }
    .spinner { width: 18px; height: 18px; border: 2.5px solid #e0e0e0; border-top-color: #4f46e5; border-radius: 50%; animation: spin .7s linear infinite; flex-shrink: 0; }
    @keyframes spin { to { transform: rotate(360deg); } }
    .status-text { font-size: 0.9rem; color: #555; }

    /* Batch layout */
    .batch-layout { display: grid; grid-template-columns: 260px 1fr; gap: 22px; align-items: start; }
    @media (max-width: 800px) { .batch-layout { grid-template-columns: 1fr; } }

    /* Keyword queue sidebar */
    .queue-list { display: flex; flex-direction: column; gap: 4px; max-height: 500px; overflow-y: auto; }
    .queue-item { display: flex; align-items: center; gap: 8px; padding: 8px 10px; border-radius: 6px; font-size: 0.83rem; background: #f8f8fb; }
    .queue-item.status-pending   { color: #888; }
    .queue-item.status-scraping  { background: #ede9fe; color: #4f46e5; font-weight: 600; }
    .queue-item.status-enriching { background: #fef3c7; color: #92400e; font-weight: 600; }
    .queue-item.status-done      { background: #d1fae5; color: #065f46; }
    .queue-item.status-error     { background: #fef2f2; color: #b91c1c; }
    .queue-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; background: currentColor; opacity: .6; }
    .queue-kw { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .queue-count { font-size: 0.75rem; opacity: .7; }

    /* Progress bar */
    .progress-section { margin-bottom: 16px; }
    .progress-label { display: flex; justify-content: space-between; font-size: 0.82rem; color: #666; margin-bottom: 6px; }
    .progress-track { height: 6px; background: #e9e9f0; border-radius: 99px; overflow: hidden; }
    .progress-fill { height: 100%; background: #4f46e5; border-radius: 99px; width: 0%; transition: width .4s ease; }
    .progress-fill.green { background: #10b981; }

    /* Results section */
    .results-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 14px; flex-wrap: wrap; gap: 10px; }
    .results-header h2 { font-size: 1rem; font-weight: 700; }
    .badge { background: #ede9fe; color: #4f46e5; padding: 3px 10px; border-radius: 20px; font-size: 0.78rem; font-weight: 700; }
    .badge-green { background: #d1fae5; color: #065f46; }

    .table-wrap { overflow-x: auto; border-radius: 10px; box-shadow: 0 1px 4px rgba(0,0,0,.08); }
    table { width: 100%; border-collapse: collapse; background: white; font-size: 0.85rem; }
    thead tr { background: #1a1a2e; color: white; }
    thead th { padding: 11px 13px; text-align: left; font-weight: 600; white-space: nowrap; font-size: 0.75rem; text-transform: uppercase; letter-spacing: .04em; }
    tbody tr { border-bottom: 1px solid #f0f0f0; transition: background .15s; }
    tbody tr:last-child { border-bottom: none; }
    tbody tr:hover { background: #fafafa; }
    td { padding: 11px 13px; vertical-align: top; }

    .pill { display: inline-block; padding: 2px 8px; border-radius: 20px; font-size: 0.72rem; font-weight: 700; white-space: nowrap; }
    .pill-sponsored { background: #fef3c7; color: #92400e; }
    .pill-organic   { background: #d1fae5; color: #065f46; }

    .company-name { font-weight: 600; }
    .website-link { color: #4f46e5; text-decoration: none; font-size: 0.8rem; }
    .website-link:hover { text-decoration: underline; }

    .tag-list { display: flex; flex-wrap: wrap; gap: 3px; }
    .tag { background: #ede9fe; color: #4f46e5; padding: 1px 6px; border-radius: 4px; font-size: 0.72rem; }

    .desc { color: #555; max-width: 220px; line-height: 1.4; font-size: 0.82rem; }

    .contact-cell { min-width: 150px; }
    .contact-loading { display: inline-flex; align-items: center; gap: 5px; color: #bbb; font-size: 0.76rem; }
    .mini-spinner { width: 9px; height: 9px; border: 1.5px solid #ddd; border-top-color: #10b981; border-radius: 50%; animation: spin .7s linear infinite; }
    .contact-list { display: flex; flex-direction: column; gap: 3px; }
    .contact-item { display: flex; align-items: center; gap: 4px; font-size: 0.8rem; }
    .contact-icon { opacity: .45; font-size: 0.72rem; }
    .contact-val { word-break: break-all; }
    .contact-email { color: #4f46e5; }
    .contact-phone { color: #059669; }
    .contact-none  { color: #ccc; font-size: 0.76rem; font-style: italic; }

    .kw-tag { display: inline-block; background: #f0f0f8; color: #666; padding: 1px 7px; border-radius: 4px; font-size: 0.72rem; margin-bottom: 2px; }

    .empty-state { text-align: center; padding: 40px 20px; color: #aaa; font-size: 0.9rem; }

    #error-box { display: none; background: #fef2f2; border: 1px solid #fecaca; border-radius: 10px; padding: 16px 20px; color: #b91c1c; margin-bottom: 20px; font-size: 0.9rem; }
    #error-box.visible { display: block; }

    .hint { font-size: 0.8rem; color: #888; margin-top: 8px; }
  </style>
</head>
<body>

<header>
  <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
    <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
  </svg>
  <h1>ThomasNet Scraper</h1>
</header>

<nav class="tabs">
  <button class="tab-btn active" onclick="switchTab('single')">Single Search</button>
  <button class="tab-btn" onclick="switchTab('batch')">Batch Run</button>
</nav>

<div class="container">
  <div id="error-box"></div>

  <!-- ── SINGLE SEARCH ── -->
  <div class="tab-panel active" id="panel-single">
    <div class="card">
      <form onsubmit="startSingle(event)">
        <div class="row">
          <div class="field">
            <label for="single-term">Search Term</label>
            <input type="text" id="single-term" placeholder="e.g. CNC MACHINE" required style="width:300px">
          </div>
          <label class="toggle-label">
            <input type="checkbox" id="single-all"> Include organic results
          </label>
          <button class="btn btn-primary" id="single-btn" type="submit">Scrape</button>
        </div>
      </form>
    </div>

    <div class="status-bar" id="single-status">
      <div class="spinner"></div>
      <span class="status-text" id="single-status-text">Starting…</span>
    </div>

    <div id="single-enrich-section" style="display:none" class="card" style="padding:16px">
      <div class="progress-section">
        <div class="progress-label">
          <span>Enriching contact info…</span>
          <span id="single-enrich-counter">0 / 0</span>
        </div>
        <div class="progress-track"><div class="progress-fill green" id="single-enrich-fill"></div></div>
      </div>
    </div>

    <div id="single-results" style="display:none">
      <div class="results-header">
        <div style="display:flex;align-items:center;gap:8px">
          <h2>Results</h2>
          <span class="badge" id="single-count">0</span>
        </div>
        <button class="btn btn-green btn-sm" onclick="downloadCSV('single')">Download CSV</button>
      </div>
      <div class="table-wrap"><table>
        <thead><tr>
          <th>Type</th><th>Company</th><th>Location</th><th>Employees</th>
          <th>Revenue</th><th>Year</th><th>Industry</th><th>Description</th>
          <th>Tags</th><th>Email</th><th>Phone</th>
        </tr></thead>
        <tbody id="single-body"></tbody>
      </table></div>
    </div>
  </div>

  <!-- ── BATCH RUN ── -->
  <div class="tab-panel" id="panel-batch">
    <div class="card" id="batch-setup-card">
      <div class="field" style="margin-bottom:14px">
        <label>Keywords — one per line</label>
        <textarea id="batch-keywords" placeholder="CNC MACHINE&#10;injection molding&#10;hydraulic pumps&#10;sheet metal fabrication&#10;gear manufacturing"></textarea>
        <p class="hint">Each keyword runs as a separate search. The scraper will process them one at a time overnight.</p>
      </div>
      <div class="row">
        <label class="toggle-label">
          <input type="checkbox" id="batch-all"> Include organic results
        </label>
        <button class="btn btn-primary" id="batch-start-btn" onclick="startBatch()">Start Batch Run</button>
        <button class="btn btn-danger" id="batch-stop-btn" onclick="stopBatch()" style="display:none">Stop</button>
      </div>
    </div>

    <div class="status-bar" id="batch-status">
      <div class="spinner"></div>
      <span class="status-text" id="batch-status-text">Starting…</span>
    </div>

    <div class="batch-layout" id="batch-main" style="display:none">
      <!-- Sidebar: keyword queue -->
      <div>
        <div class="card" style="padding:18px">
          <div style="font-size:0.8rem;font-weight:700;text-transform:uppercase;letter-spacing:.05em;color:#888;margin-bottom:10px">Keyword Queue</div>
          <div class="progress-section">
            <div class="progress-label">
              <span id="batch-kw-progress-label">0 / 0 complete</span>
            </div>
            <div class="progress-track"><div class="progress-fill" id="batch-kw-fill"></div></div>
          </div>
          <div class="queue-list" id="batch-queue"></div>
        </div>
      </div>

      <!-- Main: results table -->
      <div>
        <div class="results-header">
          <div style="display:flex;align-items:center;gap:8px">
            <h2>All Results</h2>
            <span class="badge" id="batch-count">0</span>
          </div>
          <button class="btn btn-green btn-sm" onclick="downloadCSV('batch')">Download CSV</button>
        </div>
        <div class="table-wrap"><table>
          <thead><tr>
            <th>Keyword</th><th>Type</th><th>Company</th><th>Email</th><th>Phone</th>
          </tr></thead>
          <tbody id="batch-body"></tbody>
        </table></div>
      </div>
    </div>
  </div>
</div>

<script>
// ── Tab switching ──────────────────────────────────────────────────────────
function switchTab(name) {
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('panel-' + name).classList.add('active');
  document.querySelectorAll('.tab-btn').forEach(b => {
    if (b.textContent.toLowerCase().includes(name === 'single' ? 'single' : 'batch')) b.classList.add('active');
  });
}

// ── Shared helpers ─────────────────────────────────────────────────────────
function esc(s) {
  return String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
function trunc(s, n) { return s && s.length > n ? s.slice(0, n) + '…' : (s || ''); }

function contactLoadingCell() {
  return `<span class="contact-loading"><span class="mini-spinner"></span>checking…</span>`;
}
function renderEmails(emails) {
  if (!emails || !emails.length) return '<span class="contact-none">—</span>';
  return `<div class="contact-list">${emails.map(e =>
    `<div class="contact-item"><span class="contact-icon">✉</span><span class="contact-val contact-email">${esc(e)}</span></div>`
  ).join('')}</div>`;
}
function renderPhones(phones) {
  if (!phones || !phones.length) return '<span class="contact-none">—</span>';
  return `<div class="contact-list">${phones.map(p =>
    `<div class="contact-item"><span class="contact-icon">☎</span><span class="contact-val contact-phone">${esc(p)}</span></div>`
  ).join('')}</div>`;
}
function renderTags(tags) {
  if (!tags) return '';
  return `<div class="tag-list">${tags.split('; ').filter(Boolean).map(t => `<span class="tag">${esc(t)}</span>`).join('')}</div>`;
}
function pill(sponsored) {
  return sponsored
    ? '<span class="pill pill-sponsored">Sponsored</span>'
    : '<span class="pill pill-organic">Organic</span>';
}

function showError(msg) {
  const el = document.getElementById('error-box');
  el.textContent = msg;
  el.classList.add('visible');
}

// ── Download CSV ───────────────────────────────────────────────────────────
let singleResults = [];
let batchResults  = [];

function downloadCSV(mode) {
  const rows = mode === 'single' ? singleResults : batchResults;
  if (!rows.length) return;
  const base = ['sponsored','company_name','website','location','employees','revenue',
                 'year_established','industry_type','description','tags'];
  const batchCols = ['keyword','sponsored','company_name','website','emails','phones'];
  const headers = mode === 'batch' ? batchCols : [...base, 'emails', 'phones'];
  const lines = [headers.join(',')];
  for (const r of rows) {
    const vals = headers.map(k => {
      if (k === 'emails') return `"${(r._emails||[]).join('; ').replace(/"/g,'""')}"`;
      if (k === 'phones') return `"${(r._phones||[]).join('; ').replace(/"/g,'""')}"`;
      if (k === 'keyword') return `"${esc(r._keyword||'').replace(/"/g,'""')}"`;
      return `"${String(r[k]??'').replace(/"/g,'""')}"`;
    });
    lines.push(vals.join(','));
  }
  const blob = new Blob([lines.join('\\n')], {type:'text/csv'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `thomasnet_${mode}_results.csv`;
  a.click();
}

// ── SINGLE SEARCH ──────────────────────────────────────────────────────────
let singleEnrichTotal = 0, singleEnrichDone = 0;

function startSingle(e) {
  e.preventDefault();
  const term = document.getElementById('single-term').value.trim();
  const all  = document.getElementById('single-all').checked;
  if (!term) return;

  singleResults = [];
  singleEnrichTotal = 0;
  singleEnrichDone  = 0;

  document.getElementById('single-body').innerHTML = '';
  document.getElementById('single-results').style.display = 'none';
  document.getElementById('single-enrich-section').style.display = 'none';
  document.getElementById('error-box').classList.remove('visible');
  document.getElementById('single-status').classList.add('visible');
  document.getElementById('single-status-text').textContent = 'Launching browser…';
  document.getElementById('single-btn').disabled = true;
  document.getElementById('single-count').textContent = '0';

  const src = new EventSource(`/scrape?term=${encodeURIComponent(term)}&include_all=${all?'1':'0'}`);

  src.addEventListener('status', e => {
    document.getElementById('single-status-text').textContent = e.data;
  });
  src.addEventListener('result', e => {
    const row = JSON.parse(e.data);
    row._emails = []; row._phones = [];
    const idx = singleResults.length;
    singleResults.push(row);
    _appendSingleRow(row, idx);
    document.getElementById('single-count').textContent = singleResults.length;
    document.getElementById('single-results').style.display = 'block';
  });
  src.addEventListener('enriching', e => {
    document.getElementById('single-status').classList.remove('visible');
    singleEnrichTotal = parseInt(e.data, 10);
    document.getElementById('single-enrich-counter').textContent = `0 / ${singleEnrichTotal}`;
    document.getElementById('single-enrich-section').style.display = 'block';
  });
  src.addEventListener('enriched', e => {
    const d = JSON.parse(e.data);
    singleResults[d.index]._emails = d.emails;
    singleResults[d.index]._phones = d.phones;
    _updateSingleContact(d.index, d.emails, d.phones);
    singleEnrichDone++;
    document.getElementById('single-enrich-counter').textContent = `${singleEnrichDone} / ${singleEnrichTotal}`;
    document.getElementById('single-enrich-fill').style.width = `${singleEnrichDone/singleEnrichTotal*100}%`;
  });
  src.addEventListener('done', () => {
    src.close();
    document.getElementById('single-status').classList.remove('visible');
    document.getElementById('single-enrich-section').style.display = 'none';
    document.getElementById('single-btn').disabled = false;
    if (!singleResults.length) showError('No results found — try again in a moment.');
  });
  src.addEventListener('error_msg', e => {
    src.close();
    document.getElementById('single-status').classList.remove('visible');
    document.getElementById('single-enrich-section').style.display = 'none';
    document.getElementById('single-btn').disabled = false;
    showError(e.data);
  });
}

function _appendSingleRow(r, idx) {
  const tr = document.createElement('tr');
  tr.dataset.idx = idx;
  tr.innerHTML = `
    <td>${pill(r.sponsored)}</td>
    <td><div class="company-name">${esc(r.company_name)}</div>
        ${r.website ? `<a class="website-link" href="${esc(r.website)}" target="_blank">${esc(trunc(r.website,32))}</a>` : ''}</td>
    <td>${esc(r.location)}</td>
    <td>${esc(r.employees)}</td>
    <td>${esc(r.revenue)}</td>
    <td>${esc(r.year_established)}</td>
    <td>${esc(r.industry_type)}</td>
    <td><div class="desc">${esc(trunc(r.description,110))}</div></td>
    <td>${renderTags(r.tags)}</td>
    <td class="contact-cell" id="se-${idx}">${contactLoadingCell()}</td>
    <td class="contact-cell" id="sp-${idx}">${contactLoadingCell()}</td>`;
  document.getElementById('single-body').appendChild(tr);
}
function _updateSingleContact(idx, emails, phones) {
  const e = document.getElementById(`se-${idx}`);
  const p = document.getElementById(`sp-${idx}`);
  if (e) e.innerHTML = renderEmails(emails);
  if (p) p.innerHTML = renderPhones(phones);
}

// ── BATCH RUN ──────────────────────────────────────────────────────────────
let batchSrc = null;
let batchKeywordStatuses = [];
let batchKwTotal = 0, batchKwDone = 0;

function startBatch() {
  const raw = document.getElementById('batch-keywords').value.trim();
  const keywords = raw.split('\\n').map(k => k.trim()).filter(Boolean);
  if (!keywords.length) { showError('Please enter at least one keyword.'); return; }

  const all = document.getElementById('batch-all').checked;

  batchResults = [];
  batchKeywordStatuses = keywords.map(k => ({ keyword: k, status: 'pending', count: 0 }));
  batchKwTotal = keywords.length;
  batchKwDone  = 0;

  document.getElementById('batch-body').innerHTML = '';
  document.getElementById('batch-count').textContent = '0';
  document.getElementById('error-box').classList.remove('visible');
  document.getElementById('batch-status').classList.add('visible');
  document.getElementById('batch-status-text').textContent = 'Starting…';
  document.getElementById('batch-start-btn').disabled = true;
  document.getElementById('batch-stop-btn').style.display = 'inline-block';
  document.getElementById('batch-main').style.display = 'grid';
  document.getElementById('batch-setup-card').style.display = 'none';

  _renderQueue();

  const params = new URLSearchParams({ keywords: keywords.join('\\n'), include_all: all ? '1' : '0' });
  batchSrc = new EventSource(`/batch/stream?${params}`);

  batchSrc.addEventListener('status', e => {
    document.getElementById('batch-status-text').textContent = e.data;
  });
  batchSrc.addEventListener('keyword_start', e => {
    const d = JSON.parse(e.data);
    batchKeywordStatuses[d.idx].status = 'scraping';
    _renderQueue();
  });
  batchSrc.addEventListener('keyword_enriching', e => {
    const d = JSON.parse(e.data);
    batchKeywordStatuses[d.idx].status = 'enriching';
    _renderQueue();
  });
  batchSrc.addEventListener('result', e => {
    const d = JSON.parse(e.data);
    d.row._emails = [];
    d.row._phones = [];
    batchResults.push(d.row);
    _appendBatchRow(d.row);
    document.getElementById('batch-count').textContent = batchResults.length;
  });
  batchSrc.addEventListener('enriched', e => {
    const d = JSON.parse(e.data);
    // Find by UUID — never confused by index shifts or page reloads
    const row = batchResults.find(r => r._row_id === d.row_id);
    if (row) {
      row._emails = d.emails;
      row._phones = d.phones;
    }
    _updateBatchContact(d.row_id, d.emails, d.phones);
  });
  batchSrc.addEventListener('keyword_done', e => {
    const d = JSON.parse(e.data);
    batchKeywordStatuses[d.idx].status = 'done';
    batchKeywordStatuses[d.idx].count  = d.count;
    batchKwDone++;
    _renderQueue();
    _updateKwProgress();
  });
  batchSrc.addEventListener('keyword_error', e => {
    const d = JSON.parse(e.data);
    batchKeywordStatuses[d.idx].status = 'error';
    batchKwDone++;
    _renderQueue();
    _updateKwProgress();
  });
  batchSrc.addEventListener('batch_done', () => {
    batchSrc.close();
    batchSrc = null;
    document.getElementById('batch-status').classList.remove('visible');
    document.getElementById('batch-start-btn').disabled = false;
    document.getElementById('batch-stop-btn').style.display = 'none';
    document.getElementById('batch-setup-card').style.display = 'block';
  });
  batchSrc.addEventListener('error_msg', e => {
    batchSrc.close();
    batchSrc = null;
    document.getElementById('batch-status').classList.remove('visible');
    document.getElementById('batch-start-btn').disabled = false;
    document.getElementById('batch-stop-btn').style.display = 'none';
    showError(e.data);
  });
}

function stopBatch() {
  if (batchSrc) { batchSrc.close(); batchSrc = null; }
  fetch('/batch/stop', {method:'POST'});
  document.getElementById('batch-status').classList.remove('visible');
  document.getElementById('batch-start-btn').disabled = false;
  document.getElementById('batch-stop-btn').style.display = 'none';
  document.getElementById('batch-setup-card').style.display = 'block';
}

function _renderQueue() {
  const el = document.getElementById('batch-queue');
  el.innerHTML = batchKeywordStatuses.map(item => {
    const label = {
      pending:'Pending', scraping:'Scraping…', enriching:'Enriching…',
      done:`Done (${item.count})`, error:'Error'
    }[item.status] || item.status;
    return `<div class="queue-item status-${item.status}">
      <div class="queue-dot"></div>
      <div class="queue-kw" title="${esc(item.keyword)}">${esc(item.keyword)}</div>
      <div class="queue-count">${label}</div>
    </div>`;
  }).join('');
}

function _updateKwProgress() {
  const pct = batchKwTotal ? (batchKwDone / batchKwTotal * 100) : 0;
  document.getElementById('batch-kw-fill').style.width = pct + '%';
  document.getElementById('batch-kw-progress-label').textContent = `${batchKwDone} / ${batchKwTotal} complete`;
}

// Use _row_id (UUID) as the DOM cell identifier — immune to index confusion
function _appendBatchRow(r) {
  const rowId = r._row_id || ('fallback-' + batchResults.length);
  const tr = document.createElement('tr');
  tr.innerHTML = `
    <td><span class="kw-tag">${esc(r._keyword||'')}</span></td>
    <td>${pill(r.sponsored)}</td>
    <td><div class="company-name">${esc(r.company_name)}</div>
        ${r.website ? `<a class="website-link" href="${esc(r.website)}" target="_blank">${esc(trunc(r.website,28))}</a>` : ''}</td>
    <td class="contact-cell" id="be-${rowId}">${contactLoadingCell()}</td>
    <td class="contact-cell" id="bp-${rowId}">${contactLoadingCell()}</td>`;
  document.getElementById('batch-body').appendChild(tr);
}

// rowId is a UUID string, not a numeric index
function _updateBatchContact(rowId, emails, phones) {
  const e = document.getElementById(`be-${rowId}`);
  const p = document.getElementById(`bp-${rowId}`);
  if (e) e.innerHTML = renderEmails(emails);
  if (p) p.innerHTML = renderPhones(phones);
}

// ── On load: restore any previously saved batch results ───────────────────
window.addEventListener('load', async () => {
  try {
    const res = await fetch('/batch/state');
    const data = await res.json();
    if (data.results && data.results.length) {
      batchResults = data.results;
      document.getElementById('batch-main').style.display = 'grid';
      document.getElementById('batch-count').textContent = batchResults.length;
      batchResults.forEach((r, idx) => {
        // Assign a fallback row_id for old saved results that predate this feature
        if (!r._row_id) r._row_id = 'legacy-' + idx;
        _appendBatchRow(r);
        _updateBatchContact(r._row_id, r._emails || [], r._phones || []);
      });
      if (data.keyword_log) {
        batchKeywordStatuses = data.keyword_log;
        batchKwTotal = batchKeywordStatuses.length;
        batchKwDone  = batchKeywordStatuses.filter(k => k.status === 'done' || k.status === 'error').length;
        _renderQueue();
        _updateKwProgress();
      }
    }
  } catch(e) {}
});
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    _load_saved_results()
    return render_template_string(HTML)


# Single-search SSE (unchanged from before)
@app.route("/scrape")
def scrape_single():
    term = request.args.get("term", "").strip()
    include_all = request.args.get("include_all", "0") == "1"
    sponsored_only = not include_all
    if not term:
        return Response("data: bad\n\n", mimetype="text/event-stream")

    q: queue.Queue = queue.Queue()

    def worker():
        results = []
        found = 0
        try:
            q.put(("status", "Navigating to thomasnet.com…"))
            driver = get_driver(headless=False)
            try:
                driver.get(build_url(term))
                q.put(("status", "Waiting for page to load…"))
                time.sleep(5)
                if not wait_for_results(driver):
                    q.put(("error_msg", "Results list did not load."))
                    return
                time.sleep(3)
                cards = driver.find_elements(By.CSS_SELECTOR, "li[data-sentry-component='SearchResultSupplier']")
                q.put(("status", f"Found {len(cards)} cards, extracting…"))
                for card in cards:
                    data = parse_card(card)
                    if sponsored_only and not data["sponsored"]:
                        continue
                    found += 1
                    results.append(data)
                    q.put(("result", data))
                    q.put(("status", f"Extracted {found} result(s)…"))
            finally:
                driver.quit()
        except Exception as ex:
            q.put(("error_msg", str(ex)))
            return

        if not results:
            q.put(("done", "0"))
            return

        q.put(("enriching", str(len(results))))
        for idx, row in enumerate(results):
            contact = enrich_contact(row.get("website", ""))
            q.put(("enriched", {"index": idx, "emails": contact["emails"], "phones": contact["phones"]}))
            time.sleep(0.5)
        q.put(("done", str(len(results))))

    threading.Thread(target=worker, daemon=True).start()

    def generate():
        while True:
            try:
                etype, payload = q.get(timeout=180)
            except queue.Empty:
                yield "event: error_msg\ndata: Timed out.\n\n"
                break
            if etype in ("result", "enriched"):
                yield f"event: {etype}\ndata: {json.dumps(payload)}\n\n"
            elif etype in ("status", "enriching", "done", "error_msg"):
                yield f"event: {etype}\ndata: {payload}\n\n"
                if etype in ("done", "error_msg"):
                    break

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# Batch SSE stream
@app.route("/batch/stream")
def batch_stream():
    if _batch_state["running"]:
        return Response("event: error_msg\ndata: A batch is already running.\n\n",
                        mimetype="text/event-stream")

    raw = request.args.get("keywords", "")
    keywords = [k.strip() for k in raw.split("\n") if k.strip()]
    include_all = request.args.get("include_all", "0") == "1"

    if not keywords:
        return Response("event: error_msg\ndata: No keywords provided.\n\n",
                        mimetype="text/event-stream")

    # Register this client's queue
    client_q: queue.Queue = queue.Queue(maxsize=500)
    with _subscribers_lock:
        _subscribers.append(client_q)

    threading.Thread(
        target=_run_batch,
        args=(keywords, not include_all),
        daemon=True,
    ).start()

    def generate():
        while True:
            try:
                etype, payload = client_q.get(timeout=300)
            except queue.Empty:
                yield "event: error_msg\ndata: Timed out.\n\n"
                break
            if isinstance(payload, (dict, list)):
                yield f"event: {etype}\ndata: {json.dumps(payload)}\n\n"
            else:
                yield f"event: {etype}\ndata: {payload}\n\n"
            if etype in ("batch_done", "error_msg"):
                break

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/batch/stop", methods=["POST"])
def batch_stop():
    with _batch_lock:
        _batch_state["running"] = False
    _broadcast("batch_done", {"total_results": len(_batch_state["results"])})
    return jsonify({"ok": True})


@app.route("/batch/state")
def batch_state():
    return jsonify({
        "running": _batch_state["running"],
        "results": _batch_state["results"],
        "keyword_log": _batch_state["keyword_log"],
    })


if __name__ == "__main__":
    _load_saved_results()
    print("Starting ThomasNet Scraper…")
    print("Open http://localhost:5000")
    app.run(debug=False, threaded=True, port=5000)
