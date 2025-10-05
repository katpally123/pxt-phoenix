// app.js — v2025-10-05 (Pyodide + engine reload safe call)

const SETTINGS_URL = new URL("settings.json", document.baseURI).href;
let SETTINGS = null;
let pyodide = null;

// ===== UI Elements =====
const fileInput = document.getElementById("csvFiles");
const buildBtn = document.getElementById("buildBtn");
const resetBtn = document.getElementById("resetBtn");
const dateEl   = document.getElementById("targetDate");
const logEl    = document.getElementById("log");

// ===== Boot =====
(async function boot() {
  log("Loading Pyodide…");
  pyodide = await loadPyodide({ indexURL: "https://cdn.jsdelivr.net/pyodide/v0.26.4/full/" });
  log("Pyodide ready");

  try {
    const r = await fetch(SETTINGS_URL, { cache: "no-store" });
    if (r.ok) {
      SETTINGS = await r.json();
      log("Loaded settings.json");
    } else {
      log("settings.json not found — continuing with null");
    }
  } catch (e) {
    log("settings.json fetch failed — continuing");
  }

  // create a Python module file in FS if you're bundling engine_pyodide.py locally
  // If you serve engine_pyodide.py as a <script type=py>, skip this and rely on import.
  await pyodide.FS.writeFile("/engine_pyodide.py", new TextEncoder().encode(`REPLACE_WITH_ENGINE_CODE_IF_EMBEDDING`));
})();

// ===== Helpers =====
function log(msg) {
  if (!logEl) return;
  const time = new Date().toLocaleTimeString();
  logEl.textContent += `[${time}] ${msg}\n`;
  logEl.scrollTop = logEl.scrollHeight;
}

async function filesToPyTuples(fileList) {
  const tuples = [];
  for (const f of fileList) {
    const buf = new Uint8Array(await f.arrayBuffer());
    // toPy converts TypedArray -> Python memoryview/bytes seamlessly
    const pyBytes = pyodide.toPy(buf);
    tuples.push([f.name, pyBytes]);
  }
  return pyodide.toPy(tuples); // becomes a Python list of (str, bytes)
}

function getTargetDateISO() {
  // Expecting DD/MM/YYYY (e.g., 04/10/2025) from your UI sample
  const raw = (dateEl?.value || "").trim();
  const m = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!m) return null;
  const [_, dd, mm, yyyy] = m;
  return `${yyyy}-${mm}-${dd}`; // ISO
}

// ===== Actions =====
buildBtn?.addEventListener("click", async () => {
  try {
    const files = fileInput?.files || [];
    if (!files.length) {
      alert("Select your 6 CSVs first.");
      return;
    }
    const files_py = await filesToPyTuples(files);
    const target_date_py = getTargetDateISO() || "";
    const settings_py = SETTINGS ? pyodide.toPy(SETTINGS) : pyodide.toPy(null);

    // inject variables
    pyodide.globals.set("files_py", files_py);
    pyodide.globals.set("target_date_py", target_date_py);
    pyodide.globals.set("settings_py", settings_py);

    const pyCode = `
import sys, importlib, json
# ensure module reload so we don't hold a stale build_all signature
modname = "engine_pyodide"
if modname in sys.modules:
    importlib.reload(sys.modules[modname])
else:
    import engine_pyodide  # ensure first import

import engine_pyodide as eng

# Call is variadic-compatible: build_all(files, target_date, settings) works.
_result = eng.build_all(files_py, target_date_py, settings_py)
_result["engine_loaded"] = True
_result["engine_version"] = _result.get("engine_version", "<unknown>")
json.dumps(_result)
    `;
    log("Building…");
    const out = await pyodide.runPythonAsync(pyCode);
    const result = JSON.parse(out);
    log(`Built OK — Engine ${result.engine_version}`);
    // TODO: send result.tables + result.diagnostics into your renderers
    console.log(result);
  } catch (e) {
    console.error(e);
    log(`ERROR: ${e?.message || e}`);
    alert(`Build failed: ${e?.message || e}`);
  }
});

resetBtn?.addEventListener("click", () => {
  fileInput.value = "";
  if (dateEl) dateEl.value = "";
  log("Reset.");
});
