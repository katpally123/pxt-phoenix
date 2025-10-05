// PXT Phoenix – Pyodide glue (no backend, all local)

let pyodidePromise = null;
async function ensurePyodide() {
  if (pyodidePromise) return pyodidePromise;

  pyodidePromise = (async () => {
    const py = await loadPyodide({
      indexURL: "https://cdn.jsdelivr.net/pyodide/v0.25.1/full/",
    });
    // Load Python packages into the in-browser runtime
    await py.loadPackage(["pandas", "numpy", "python-dateutil"]);
    return py;
  })();

  return pyodidePromise;
}

async function loadEngine(py) {
  const code = await fetch("engine_pyodide.py").then(r => r.text());
  await py.runPythonAsync(code);
  return py.globals.get("build_all");
}

async function readSettings() {
  return fetch("settings.json").then(r => r.json());
}

function toBytes(buf) {
  return new Uint8Array(buf);
}

async function buildData(files) {
  const statusEl = document.getElementById("status");
  statusEl.textContent = "Loading Pyodide…";
  const py = await ensurePyodide();

  statusEl.textContent = "Preparing engine…";
  const buildAll = await loadEngine(py);

  statusEl.textContent = "Reading settings…";
  const settings = await readSettings();

  // JS File objects → { name: bytes }
  const fileMap = {};
  for (const f of files) fileMap[f.name] = toBytes(await f.arrayBuffer());

  // Read target date (YYYY-MM-DD) or empty
  const targetDate = document.getElementById("targetDate").value || "";

  // Inject into Python globals
  py.globals.set("JS_FILE_MAP", fileMap);
  py.globals.set("JS_SETTINGS", settings);
  py.globals.set("JS_TARGET_DATE", targetDate);

  // IMPORTANT: do NOT import from 'js'; we already set globals.
  const pyCode = `
res = build_all(JS_FILE_MAP.to_py(), JS_SETTINGS.to_py(), JS_TARGET_DATE)
import json
json.dumps(res)
  `;

  statusEl.textContent = "Processing in browser…";
  const jsonStr = await py.runPythonAsync(pyCode);
  statusEl.textContent = "Done.";
  const data = JSON.parse(jsonStr);
  document.getElementById("badge").textContent =
    "Data as of: " + (data.generated_at || new Date().toISOString());
  return data;
}

// --- Minimal renderers (swap in your existing ones later) ---
function renderSummaryBlock(summary) {
  const wrap = document.getElementById("summary");
  const rows = Object.entries(summary.by_department || {}).map(([dept, m]) => `
    <tr>
      <td>${dept}</td>
      <td>${(m.regular_expected_AMZN||0)+(m.regular_expected_TEMP||0)}</td>
      <td>${(m.regular_present_AMZN||0)+(m.regular_present_TEMP||0)}</td>
      <td>${m.swap_out||0}</td>
      <td>${m.swap_in_expected||0}</td>
      <td>${m.swap_in_present||0}</td>
      <td>${m.vet_accept||0}</td>
      <td>${m.vet_present||0}</td>
      <td>${m.vto_accept||0}</td>
    </tr>
  `).join("");
  wrap.innerHTML = `
    <h2>Department Summary</h2>
    <table class="table">
      <thead><tr>
        <th>Dept</th><th>Regular Exp</th><th>Regular Present</th>
        <th>Swap OUT</th><th>Swap IN Exp</th><th>Swap IN Pres</th>
        <th>VET Acc</th><th>VET Pres</th><th>VTO Acc</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

function renderVetBlock(vet) {
  const wrap = document.getElementById("vet");
  const rows = (vet.records||[]).map(r=>`
    <tr><td>${r.work_date||""}</td><td>${r.type||""}</td><td>${r.eid||""}</td><td>${r.dept_id||""}</td><td>${r.management_area_id||""}</td><td>${r.employment_type||""}</td><td>${r.present?"✔":""}</td></tr>
  `).join("");
  wrap.innerHTML = `
    <h2>VET / VTO</h2>
    <table class="table">
      <thead><tr><th>Date</th><th>Type</th><th>EID</th><th>DeptID</th><th>MA</th><th>Type</th><th>Present</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

function renderSwapsBlock(sw) {
  const wrap = document.getElementById("swaps");
  function block(title, arr) {
    return `
    <h3>${title} (${arr.length})</h3>
    <table class="table">
      <thead><tr><th>Skip</th><th>Work</th><th>EID</th><th>DeptID</th><th>MA</th><th>Type</th><th>Present</th></tr></thead>
      <tbody>${
        arr.map(r=>`<tr>
          <td>${r.skip_date||""}</td><td>${r.work_date||""}</td>
          <td>${r.eid||""}</td><td>${r.dept_id||""}</td><td>${r.management_area_id||""}</td>
          <td>${r.employment_type||""}</td><td>${r.present?"✔":""}</td>
        </tr>`).join("")
      }</tbody>
    </table>`;
  }
  wrap.innerHTML = `<h2>Swaps</h2>${block("Swap OUT", sw.swap_out||[])}${block("Swap IN (expected)", sw.swap_in_expected||[])}${block("Swap IN (present)", sw.swap_in_present||[])}`;
}

function renderAuditBlock(){ document.getElementById("audit").innerHTML = ""; }

// Wire UI
document.getElementById("runBtn").addEventListener("click", async () => {
  const files = document.getElementById("fileInput").files;
  if (!files || files.length === 0) { alert("Please select your daily CSVs first."); return; }
  try {
    const data = await buildData(files);
    renderSummaryBlock(data.dept_summary || {});
    renderVetBlock(data.vet_vto || { records: [] });
    renderSwapsBlock(data.swaps || { swap_out: [], swap_in_expected: [], swap_in_present: [] });
    renderAuditBlock();
  } catch (e) {
    console.error(e);
    document.getElementById("status").textContent = "Error: " + e;
  }
});

document.getElementById("resetBtn").addEventListener("click", () => {
  document.getElementById("fileInput").value = "";
  document.getElementById("summary").innerHTML = "";
  document.getElementById("vet").innerHTML = "";
  document.getElementById("swaps").innerHTML = "";
  document.getElementById("audit").innerHTML = "";
  document.getElementById("status").textContent = "Cleared. (Ephemeral mode)";
  document.getElementById("badge").textContent = "Data as of: —";
});
