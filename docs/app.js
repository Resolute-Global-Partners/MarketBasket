/**
 * Market Basket — client-side data app.
 */

import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";

// ─── constants ─────────────────────────────────────────────────────────────

const PREM_BIN_SIZE = 500;
const PREM_BIN_CAP  = 5000;
const YEAR_LABELS   = ["pre-2010", "2010-2014", "2015-2019", "2020+"];

// Canonical letter-grade order for the Credit Code filter. Skips N/O/R/T/X/Y.
// Matches CREDIT_CODE_ORDER in src/marketbasket/config.py.
const CREDIT_CODE_ORDER = [
  "A", "B", "C", "D", "E", "F", "G", "H", "I", "J",
  "K", "L", "M", "P", "Q", "S", "U", "V", "W", "Z",
];

// Coverage IDs match Sum<Id>Premium columns in the parquet, and disc-<Id> input IDs.
const COVERAGES = [
  "LiabBI", "LiabPD", "Comp", "Coll", "MedPay",
  "UIMBI", "UIMPD", "UninsBI", "UninsPD",
];

// Rate filter ids — these are REQUIRED (no "Any"). Defaults set in index.html.
const RATE_FILTER_IDS = ["payplan-type", "has-phys-dmg", "has-um-uim", "has-medpay"];

// Default liab order if a state's index entry doesn't specify. Most states use
// 25/50 as the floor; TX uses 30/60 (the index will override).
const LIAB_ORDER_DEFAULT = ["25/50", "50/100", "100/300"];

// ─── state ─────────────────────────────────────────────────────────────────

const app = {
  db: null, conn: null, index: null,
  currentState: null, grid: null,
  lastRows: null, lastTotalRow: null,
  // Session cache so re-selecting a state never re-downloads its parquet.
  buffers: {},            // stateCode -> Uint8Array
  registered: new Set(),  // parquet filenames already registered with DuckDB
  // True when the current parquet has Sum<C>Bridging columns. Older parquets
  // don't, and we fall back to a scaling approximation.
  hasExactBridging: false,
};

init().catch(err => {
  setStatus(`Startup failed: ${err.message}`, true);
  console.error(err);
});

// ─── entry ─────────────────────────────────────────────────────────────────

async function init() {
  setStatus("Loading DuckDB…");
  app.db = await bootDuckDB();
  app.conn = await app.db.connect();

  setStatus("Loading state index…");
  // Cache-bust so we always see the latest after a refresh has been run.
  app.index = await fetch(`data/index.json?t=${Date.now()}`, { cache: "no-store" }).then(r => {
    if (!r.ok) throw new Error(`data/index.json missing (HTTP ${r.status})`);
    return r.json();
  });

  document.getElementById("generated-at").textContent =
    `Data last refreshed: ${formatTimestamp(app.index.generated_at)}`;

  populateStateDropdown();
  wireControls();

  const states = activeStates();
  const initial = states.includes("IL") ? "IL" : states[0];
  if (initial) {
    document.getElementById("state").value = initial;
    await loadState(initial);
  } else {
    setStatus("No states available.", true);
  }
}

async function bootDuckDB() {
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);
  const worker = await duckdb.createWorker(bundle.mainWorker);
  const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  return db;
}

// ─── state load ────────────────────────────────────────────────────────────

async function loadState(stateCode) {
  const entry = app.index.states[stateCode];
  if (!entry) { setStatus(`Unknown state: ${stateCode}`, true); return; }

  setStatus(`Loading ${stateCode}…`);
  const fname = `${stateCode}.parquet`;

  // Download each state's parquet at most once per session. The cache-bust
  // token is the data's publish timestamp (not Date.now()), so the browser
  // disk cache can serve it instantly on repeat visits while a real data
  // refresh — which changes generated_at — still busts it.
  if (!app.buffers[stateCode]) {
    const ver = encodeURIComponent(app.index.generated_at || "0");
    const url = `data/${stateCode}.parquet?v=${ver}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`${url} missing (HTTP ${res.status})`);
    app.buffers[stateCode] = new Uint8Array(await res.arrayBuffer());
  }
  if (!app.registered.has(fname)) {
    await app.db.registerFileBuffer(fname, app.buffers[stateCode]);
    app.registered.add(fname);
  }

  await app.conn.query(`DROP VIEW IF EXISTS mb`);
  await app.conn.query(`CREATE VIEW mb AS SELECT * FROM read_parquet('${fname}')`);

  app.currentState = stateCode;
  // Probe the parquet schema once so refreshAll knows which path to take.
  app.hasExactBridging = await detectExactBridging();
  await populateFiltersFromData();
  buildGrid(entry);
  await refreshAll();

  setStatus(`${stateCode} ready`);
}

/**
 * Check whether the currently-loaded `mb` view has the per-coverage bridging
 * columns (SumLiabBIBridging etc.). When present, we report exact values;
 * when absent (older parquet), fall back to scaling SumCPremium by the
 * SumBridgingPremium/SumPremium ratio.
 */
async function detectExactBridging() {
  try {
    const rows = await sqlRows(`PRAGMA table_info('mb')`);
    const names = new Set(rows.map(r => r.name));
    return COVERAGES.every(c => names.has(`Sum${c}Bridging`));
  } catch (e) {
    console.warn("schema probe failed", e);
    return false;
  }
}

// ─── dropdowns ─────────────────────────────────────────────────────────────

// States that have OUR_COMPANIES defined (i.e. listed in the index with our_companies).
// Acts as the gate for what shows in the State dropdown.
function activeStates() {
  return Object.keys(app.index.states)
    .filter(s => Array.isArray(app.index.states[s].our_companies)
                 && app.index.states[s].our_companies.length > 0)
    .sort();
}

function populateStateDropdown() {
  const sel = document.getElementById("state");
  sel.innerHTML = "";
  for (const s of activeStates()) {
    const opt = document.createElement("option");
    opt.value = s; opt.textContent = s;
    sel.appendChild(opt);
  }
}

function setOptions(selId, values, defaultValue, displayFn) {
  const sel = document.getElementById(selId);
  sel.innerHTML = "";
  for (const v of values) {
    const opt = document.createElement("option");
    opt.value = String(v);
    opt.textContent = displayFn ? displayFn(v) : String(v);
    sel.appendChild(opt);
  }
  if (defaultValue !== undefined) sel.value = String(defaultValue);
}

// "202405" -> "05/2024"  (keeps .value unchanged; only display label changes)
function displayYYYYMM(v) {
  const s = String(v);
  if (!/^\d{6}$/.test(s)) return s;
  return `${s.slice(4, 6)}/${s.slice(0, 4)}`;
}

async function populateFiltersFromData() {
  const entry = app.index.states[app.currentState];

  const months = entry.months.map(String);
  setOptions("date-from", months, months[0],                        displayYYYYMM);
  setOptions("date-to",   months, months[months.length - 1],        displayYYYYMM);

  const prem = [];
  for (let v = 0; v <= PREM_BIN_CAP + PREM_BIN_SIZE; v += PREM_BIN_SIZE) prem.push(v);
  setOptions("prem-min", prem, 0);
  setOptions("prem-max", prem, PREM_BIN_CAP + PREM_BIN_SIZE);

  // Liab Limits: state-specific floor (TX uses 30/60, others 25/50).
  const liabOrder = (Array.isArray(entry.liab_limits) && entry.liab_limits.length)
    ? entry.liab_limits
    : LIAB_ORDER_DEFAULT;
  setOptions("liab", ["Any", ...liabOrder], "Any");

  setOptions("term", ["6", "12"], "6");
  setOptions("non-owner", ["Any", "No", "Yes"], "Any");
  setOptions("num-drivers",  ["Any", "1", "2", "3", "4+"], "Any");
  setOptions("num-vehicles", ["Any", "1", "2", "3", "4", "5+"], "Any");
  setOptions("prior-insurance", ["Any", "No", "Yes"], "Any");
  setOptions("year-bin", ["Any", ...YEAR_LABELS], "Any");

  // Credit Code: letter-grade pickers (A..Z with skips). Order is canonical;
  // we only show codes actually present in the state's data. Hide both rows
  // entirely when the state has no credit formula.
  const codes = Array.isArray(entry.credit_codes) ? entry.credit_codes : [];
  const ordered = CREDIT_CODE_ORDER.filter(c => codes.includes(c));
  const creditRow = document.getElementById("credit-from")?.closest(".filter-row");
  if (ordered.length === 0) {
    if (creditRow) creditRow.style.display = "none";
  } else {
    if (creditRow) creditRow.style.display = "";
    setOptions("credit-from", ordered, ordered[0]);
    setOptions("credit-to",   ordered, ordered[ordered.length - 1]);
  }

  // County: Any + actual counties present (from index entry, falls back to query).
  let counties = entry.counties || [];
  if (!counties.length) {
    try {
      const rows = await sqlRows(
        `SELECT DISTINCT County FROM mb WHERE County IS NOT NULL ORDER BY County`
      );
      counties = rows.map(r => r.County);
    } catch (e) { console.warn("county query failed", e); }
  }
  // Show "Other" last for readability.
  counties = counties.slice().sort((a, b) => {
    if (a === "Other") return 1;
    if (b === "Other") return -1;
    return a.localeCompare(b);
  });
  setOptions("county", ["Any", ...counties], "Any");

  document.getElementById("market-provider").value = "ITC";

  await setDefaultRateFilters();
}

/**
 * Default the coverage-signature rate filters to the most common real
 * combination in THIS state's data, rather than a hardcoded "liability-only".
 *
 * Liability-only (No/No/No) is modal where UM/MedPay are optional (AZ/TN/IN),
 * but in compulsory-UM states like Illinois ~100% of every carrier's quotes
 * carry UM — so a hardcoded UM=No selected ~1% of the state and produced a
 * wildly unrepresentative comparison (a handful of stray quotes per carrier).
 * Picking the modal signature keeps the default view representative everywhere.
 * PayPlanType stays "Various" (the common pay type; PIF has identical premium).
 */
async function setDefaultRateFilters() {
  document.getElementById("payplan-type").value = "Various";
  try {
    const rows = await sqlRows(`
      SELECT HasPhysDmg, HasUM_UIM, HasMedPay
      FROM mb WHERE PayPlanType = 'Various'
      GROUP BY 1, 2, 3
      ORDER BY SUM(Quotes) DESC
      LIMIT 1
    `);
    if (rows.length) {
      const r = rows[0];
      document.getElementById("has-phys-dmg").value = String(Number(r.HasPhysDmg));
      document.getElementById("has-um-uim").value   = String(Number(r.HasUM_UIM));
      document.getElementById("has-medpay").value   = String(Number(r.HasMedPay));
    }
  } catch (e) {
    console.warn("default rate-filter query failed; using No/No/No", e);
    document.getElementById("has-phys-dmg").value = "0";
    document.getElementById("has-um-uim").value   = "0";
    document.getElementById("has-medpay").value   = "0";
  }
}

// ─── SQL ───────────────────────────────────────────────────────────────────

function currentFilters() {
  const v = id => document.getElementById(id).value;
  return {
    dateFrom: parseInt(v("date-from"), 10),
    dateTo:   parseInt(v("date-to"),   10),
    premMin:  parseInt(v("prem-min"),  10),
    premMax:  parseInt(v("prem-max"),  10),
    creditFrom: v("credit-from"),
    creditTo:   v("credit-to"),
    county:   v("county"),
    liab:     v("liab"),
    term:     parseInt(v("term"), 10),
    nonOwner: v("non-owner"),
    numDrv:   v("num-drivers"),
    numVeh:   v("num-vehicles"),
    prior:    v("prior-insurance"),
    yearBin:  v("year-bin"),
    marketProvider: v("market-provider"),
    // Rate filters (required, no "Any")
    payplanType: v("payplan-type"),
    hasPhysDmg:  v("has-phys-dmg"),
    hasUmUim:    v("has-um-uim"),
    hasMedpay:   v("has-medpay"),
  };
}

// Read all discount inputs (Total + 9 per-coverage) and clamp to (-Inf, 100].
// Returns { total: <number>, coverages: {LiabBI: <number>, ...} }.
function currentDiscounts() {
  const readClamp = id => {
    const el = document.getElementById(id);
    let n = el ? Number(el.value) : 0;
    if (!Number.isFinite(n)) n = 0;
    if (n > 100) n = 100;
    return n;
  };
  const coverages = {};
  for (const c of COVERAGES) coverages[c] = readClamp(`disc-${c}`);
  return { total: readClamp("disc-Total"), coverages };
}

// Has any non-zero discount? Avoids the CASE expression when nothing is set.
function anyDiscountActive(disc) {
  if (disc.total !== 0) return true;
  return Object.values(disc.coverages).some(v => v !== 0);
}

function whereClause(f) {
  const conds = [];
  conds.push(`YYYYMM >= ${f.dateFrom}`);
  conds.push(`YYYYMM <= ${f.dateTo}`);
  conds.push(`PremBin >= ${f.premMin}`);
  conds.push(`PremBin <  ${f.premMax}`);
  conds.push(`Term = ${f.term}`);
  // Credit range: expand "From X To Y" into the slice of CREDIT_CODE_ORDER
  // between them (inclusive). When the slice covers every code present in
  // this state, skip the WHERE clause so NULL CreditCode rows pass through.
  const entry = app.index.states[app.currentState];
  const presentCodes = (entry && Array.isArray(entry.credit_codes)) ? entry.credit_codes : [];
  if (presentCodes.length && f.creditFrom && f.creditTo) {
    const ordered = CREDIT_CODE_ORDER.filter(c => presentCodes.includes(c));
    const lo = ordered.indexOf(f.creditFrom);
    const hi = ordered.indexOf(f.creditTo);
    if (lo >= 0 && hi >= 0) {
      const [a, b] = lo <= hi ? [lo, hi] : [hi, lo];
      const slice = ordered.slice(a, b + 1);
      if (slice.length < ordered.length) {
        const inList = slice.map(c => `'${c}'`).join(",");
        conds.push(`CreditCode IN (${inList})`);
      }
    }
  }
  if (f.county !== "Any") conds.push(`County = '${f.county.replace(/'/g, "''")}'`);
  if (f.liab    !== "Any") conds.push(`LiabLimits = '${f.liab}'`);
  if (f.numDrv  !== "Any") conds.push(`NumDrivers = '${f.numDrv}'`);
  if (f.numVeh  !== "Any") conds.push(`NumVehicles = '${f.numVeh}'`);
  if (f.yearBin !== "Any") conds.push(`YearBin = '${f.yearBin}'`);
  if (f.nonOwner !== "Any") conds.push(`NonOwner = ${f.nonOwner === "Yes" ? 1 : 0}`);
  if (f.prior    !== "Any") conds.push(`PriorInsurance = ${f.prior === "Yes" ? 1 : 0}`);

  // Rate filters — required, always applied (no "Any")
  conds.push(`PayPlanType = '${f.payplanType}'`);
  conds.push(`HasPhysDmg  = ${parseInt(f.hasPhysDmg, 10)}`);
  conds.push(`HasUM_UIM   = ${parseInt(f.hasUmUim,   10)}`);
  conds.push(`HasMedPay   = ${parseInt(f.hasMedpay,  10)}`);

  // Market Provider: all currently ingested data is from ITC. EZ Lynx data
  // hasn't been pulled yet, so filter it out. When Rate_Source is added to
  // the aggregation pipeline, this block will be replaced with a real column
  // filter.
  if (f.marketProvider === "EZ Lynx") {
    conds.push("1 = 0");   // no EZ Lynx data yet
  }
  // "ITC" and "Any" pass through (current data is all ITC)

  return "WHERE " + conds.join(" AND ");
}

/**
 * Build a SQL expression that returns the per-row "adjusted SumPremium" —
 * applies discounts only to companies in `ourCompanies`. When no discounts
 * are active, falls back to plain SumPremium.
 *
 * Stacking is additive:
 *   adjusted = SumPremium - (total% * SumPremium) - Σ(cov% * Sum<C>Premium)
 *   adjusted = GREATEST(adjusted, 0)
 *
 * So Total=10% + LiabBI=20% means LiabBI dollars effectively get 30% off,
 * other dollars get 10% off. Total=100% with everything else 0 -> 0.
 * Positive = discount, negative = surcharge.
 */
function adjustedSumPremiumSQL(disc, ourCompanies) {
  if (!anyDiscountActive(disc) || !ourCompanies || ourCompanies.length === 0) {
    return "SumPremium";
  }
  const inList = ourCompanies.map(c => `'${c.replace(/'/g, "''")}'`).join(",");
  const parts = [];
  if (disc.total !== 0) parts.push(`${disc.total / 100} * SumPremium`);
  for (const c of COVERAGES) {
    const v = disc.coverages[c];
    if (v !== 0) parts.push(`${v / 100} * Sum${c}Premium`);
  }
  const subtract = parts.join(" + ");
  return `CASE WHEN CompanyName IN (${inList}) THEN GREATEST(SumPremium - (${subtract}), 0) ELSE SumPremium END`;
}

/**
 * Per-row per-coverage BRIDGING premium.
 *
 * If the parquet has Sum<C>Bridging columns (newer pipeline), those are exact
 * — sums of per-coverage premium across rows where PurchasedFinal=1. Older
 * parquets don't have them; fall back to scaling Sum<C>Premium by the in-cell
 * bridge ratio (SumBridgingPremium / SumPremium). Within a tight groupby cell
 * the coverage mix is roughly uniform, so the approximation tracks closely.
 *
 * Discounts (Total + per-coverage) are applied on top, only for our
 * companies. Floored at 0.
 */
function bridgingCoverageSQL(coverage, disc, ourCompanies, exactAvailable) {
  const baseBridge = exactAvailable
    ? `Sum${coverage}Bridging`
    : `(Sum${coverage}Premium * (CASE WHEN SumPremium > 0 THEN SumBridgingPremium / SumPremium ELSE 0 END))`;

  const isOurs = ourCompanies && ourCompanies.length > 0
    ? `CompanyName IN (${ourCompanies.map(c => `'${c.replace(/'/g, "''")}'`).join(",")})`
    : "1=0";

  if (!anyDiscountActive(disc)) return baseBridge;

  const totalFactor = 1 - (disc.total / 100);
  const covFactor = 1 - (disc.coverages[coverage] / 100);
  // Additive stacking: subtract total% AND coverage% of the bridging-coverage value.
  const adjusted = `GREATEST(${baseBridge} * (${totalFactor} + ${covFactor} - 1), 0)`;
  return `CASE WHEN ${isOurs} THEN ${adjusted} ELSE ${baseBridge} END`;
}

// ─── grid ──────────────────────────────────────────────────────────────────

function buildGrid(entry) {
  const comparisonCo = entry.comparison_company;
  const showSICvs = entry.companies.includes("SIC");

  const fmtInt      = p => p.value == null ? "" : Number(p.value).toLocaleString(undefined, {maximumFractionDigits:0});
  const fmtPct1     = p => p.value == null ? "" : (Number(p.value) * 100).toFixed(1) + "%";
  const fmtPctSign  = p => p.value == null ? "" : (Number(p.value) >= 0 ? "+" : "") + (Number(p.value) * 100).toFixed(1) + "%";
  const fmtDollar   = p => p.value == null ? "" : "$" + Number(p.value).toLocaleString(undefined, {maximumFractionDigits:0});
  const fmtSize     = p => p.value == null ? "" : Number(p.value).toFixed(1) + "%";
  const fmtRankDiff = p => {
    if (p.value == null) return "";
    const n = Number(p.value);
    return (n > 0 ? "+" : "") + n.toString();
  };
  const diffClass  = p => p.value == null ? "" : (Number(p.value) >= 0 ? "cell-pos" : "cell-neg");

  // All data columns share the same flex + minWidth so they're equal width and
  // no header text gets truncated with "…". Program (text) is fixed width,
  // not sortable (sorting alphabetically is the default display order anyway).
  const COL = { flex: 1, minWidth: 120, type: "numericColumn" };

  const cols = [
    { field: "CompanyName", headerName: "Program", pinned: "left", width: 170, minWidth: 150,
      sortable: false, lockPosition: "left" },
    { ...COL, field: "Quotes",             headerName: "Quotes",               valueFormatter: fmtInt },
    { ...COL, field: "SizePct",            headerName: "Size (%)",             valueFormatter: fmtSize },
    { ...COL, field: "AvgPremium",         headerName: "Avg Written\nPremium", valueFormatter: fmtDollar },
    { ...COL, field: "WrittenRank",        headerName: "Written\nRank",        valueFormatter: fmtInt },
    { ...COL, field: "BridgingCount",      headerName: "Bridging\nCount",      valueFormatter: fmtInt },
    { ...COL, field: "BridgeRate",         headerName: "Bridge\nRate",         valueFormatter: fmtPct1 },
    { ...COL, field: "AvgBridgingPremium", headerName: "Avg Bridging\nPremium",valueFormatter: fmtDollar },
    { ...COL, field: "BridgeRank",         headerName: "Bridge\nRank",         valueFormatter: fmtInt },
    { ...COL, field: "AvgPremiumDiff",     headerName: "Avg Premium\nDiff",    valueFormatter: fmtPctSign, cellClass: diffClass },
    { ...COL, field: "RankDiff",           headerName: "Rank\nDiff",           valueFormatter: fmtRankDiff, cellClass: diffClass },
  ];
  if (comparisonCo) {
    cols.push({ ...COL, field: "VsCompareCo", headerName: `vs ${comparisonCo}`,
      valueFormatter: fmtPctSign, cellClass: diffClass });
  }
  if (showSICvs) {
    cols.push({ ...COL, field: "VsSIC", headerName: "vs SIC",
      valueFormatter: fmtPctSign, cellClass: diffClass });
  }

  // Per-coverage avg breakdown — 9 columns at the far right.
  // Header label is friendlier than the raw column id.
  // Values are AVG-PER-BRIDGED-POLICY (Sum<C>Bridging / BridgingCount), with
  // discounts applied for our companies.
  const COV_LABELS = {
    LiabBI: "Liab BI", LiabPD: "Liab PD", Comp: "Comp", Coll: "Coll",
    MedPay: "MedPay", UIMBI: "UIM BI", UIMPD: "UIM PD",
    UninsBI: "Unins BI", UninsPD: "Unins PD",
  };
  for (const c of COVERAGES) {
    cols.push({ ...COL, field: `Avg${c}`, headerName: `Avg Bridging\n${COV_LABELS[c]}`,
      valueFormatter: fmtDollar, cellClass: "cell-coverage" });
  }

  const gridDiv = document.getElementById("grid");
  gridDiv.innerHTML = "";
  app.grid = agGrid.createGrid(gridDiv, {
    columnDefs: cols,
    defaultColDef: {
      sortable: true, resizable: true, filter: true,
      wrapHeaderText: true, autoHeaderHeight: true,
    },
    rowData: [],
    animateRows: false,
    domLayout: "autoHeight",
    rowClassRules: {
      "row-reference": p => isReferenceRow(p.data, comparisonCo),
      "row-total":     p => p.node && p.node.rowPinned === "bottom",
    },
  });
}

function isReferenceRow(row, comparisonCo) {
  if (!row || row._isTotal) return false;
  return row.CompanyName === comparisonCo || row.CompanyName === "SIC";
}

async function refreshAll() {
  if (!app.currentState) return;

  const f = currentFilters();
  const where = whereClause(f);
  const disc = currentDiscounts();
  const entry = app.index.states[app.currentState];
  const ourCompanies = entry.our_companies || [];

  // Per-row adjusted SumPremium (applies coverage discounts to our companies).
  const adjPrem = adjustedSumPremiumSQL(disc, ourCompanies);
  // Per-row scale factor for bridging — same proportional discount as the
  // written premium. Avoids divide-by-zero on rows with SumPremium=0.
  const scale = `CASE WHEN SumPremium > 0 THEN (${adjPrem}) / SumPremium ELSE 1 END`;
  const adjBridge = `${scale} * SumBridgingPremium`;

  // Cast sums to DOUBLE so JS gets regular numbers, not BigInt.
  // SUM on INT returns BIGINT, which comes through as BigInt in JS and
  // breaks arithmetic (e.g. bigint / bigint = integer division → 0).
  //
  // Per-coverage breakdown (SumCBridging) is computed AFTER bridging — each
  // coverage is scaled by the in-cell bridge ratio, then discounted (Total +
  // per-coverage stack) for our companies. Yields avg-per-bridged-policy in
  // computeDerived.
  const coverageSelects = COVERAGES
    .map(c => `CAST(SUM(${bridgingCoverageSQL(c, disc, ourCompanies, app.hasExactBridging)}) AS DOUBLE) AS Sum${c}Bridging`)
    .join(",\n            ");
  const aggRows = await sqlRows(`
    SELECT  CompanyName,
            CAST(SUM(Quotes)             AS DOUBLE) AS Quotes,
            CAST(SUM(${adjPrem})         AS DOUBLE) AS SumPremium,
            CAST(SUM(BridgingCount)      AS DOUBLE) AS BridgingCount,
            CAST(SUM(${adjBridge})       AS DOUBLE) AS SumBridgingPremium,
            ${coverageSelects}
    FROM mb ${where}
    GROUP BY CompanyName
  `);

  // Always show every company that exists in this state. If the current
  // filter returns zero rows for a company, show it with zeros instead of
  // hiding it — matching the original Excel's behaviour.
  const seen = new Set(aggRows.map(r => r.CompanyName));
  for (const c of entry.companies) {
    if (!seen.has(c)) {
      const filler = {
        CompanyName: c,
        Quotes: 0, SumPremium: 0, BridgingCount: 0, SumBridgingPremium: 0,
      };
      for (const cov of COVERAGES) filler[`Sum${cov}Bridging`] = 0;
      aggRows.push(filler);
    }
  }

  const { rows, totalRow, total } = computeDerived(aggRows, entry);

  // Put company rows + TOTAL as regular rows (no pinning) so there's no gap.
  // Companies (incl. Other) are sortable rowData; TOTAL pinned at bottom
  // so user-driven column sort doesn't move it.
  app.grid.setGridOption("rowData", rows);
  app.grid.setGridOption("pinnedBottomRowData", totalRow ? [totalRow] : []);

  app.lastRows = rows;
  app.lastTotalRow = totalRow;
  updateTopStats(entry, total, rows.length);
}

function computeDerived(rows, entry) {
  const comparisonCo = entry.comparison_company;

  const total = rows.reduce((a, r) => {
    a.Quotes             += Number(r.Quotes || 0);
    a.SumPremium         += Number(r.SumPremium || 0);
    a.BridgingCount      += Number(r.BridgingCount || 0);
    a.SumBridgingPremium += Number(r.SumBridgingPremium || 0);
    for (const c of COVERAGES) a[`Sum${c}Bridging`] += Number(r[`Sum${c}Bridging`] || 0);
    return a;
  }, (() => {
    const init = { Quotes: 0, SumPremium: 0, BridgingCount: 0, SumBridgingPremium: 0 };
    for (const c of COVERAGES) init[`Sum${c}Bridging`] = 0;
    return init;
  })());

  const refCmpAvg = avgBridging(rows.find(r => r.CompanyName === comparisonCo));
  const refSICAvg = avgBridging(rows.find(r => r.CompanyName === "SIC"));

  const derived = rows.map(r => {
    const q  = Number(r.Quotes || 0);
    const bc = Number(r.BridgingCount || 0);
    const sp = Number(r.SumPremium || 0);
    const sb = Number(r.SumBridgingPremium || 0);
    const avgBr = bc > 0 ? sb / bc : null;
    const avgPrem = q > 0 ? sp / q : null;
    const out = {
      CompanyName: r.CompanyName,
      Quotes: q,
      SizePct: total.Quotes > 0 ? (q / total.Quotes) * 100 : null,
      BridgingCount: bc,
      BridgeRate: q > 0 ? bc / q : null,
      AvgPremium: avgPrem,
      AvgBridgingPremium: avgBr,
      // (Written - Bridging) / Written: positive means bridging is cheaper
      // than the average written quote (good — green); negative means the
      // people who bridged paid more than the average quoter (bad — red).
      AvgPremiumDiff: (avgPrem != null && avgPrem > 0 && avgBr != null)
        ? (avgPrem - avgBr) / avgPrem
        : null,
      VsCompareCo: (avgBr != null && refCmpAvg != null && refCmpAvg > 0) ? (avgBr / refCmpAvg - 1) : null,
      VsSIC:       (avgBr != null && refSICAvg != null && refSICAvg > 0) ? (avgBr / refSICAvg - 1) : null,
    };
    // Avg<C> is per-BRIDGED-policy (sum of bridging coverage / bridging count).
    // sc is already discount-adjusted in SQL for our companies.
    for (const c of COVERAGES) {
      const sc = Number(r[`Sum${c}Bridging`] || 0);
      out[`Avg${c}`] = bc > 0 ? sc / bc : null;
    }
    return out;
  });

  // Rankings by avg premium (ascending → rank 1 = cheapest).
  // Ties share the same rank. Rows without a valid AvgPremium / AvgBridgingPremium
  // (e.g. 0 quotes after filtering) get null rank.
  assignRank(derived, "AvgPremium",         "WrittenRank");
  assignRank(derived, "AvgBridgingPremium", "BridgeRank");

  // Rank difference: written_rank − bridge_rank.
  // Positive: company is chosen more than its price rank would suggest (overperforming).
  // Negative: company is chosen less than its price rank would suggest (underperforming).
  derived.forEach(r => {
    r.RankDiff = (r.WrittenRank != null && r.BridgeRank != null)
      ? r.WrittenRank - r.BridgeRank
      : null;
  });

  // Default display order: alphabetical, "Other …" at the bottom.
  // AG-Grid's own sort (user clicking a column header) overrides this.
  derived.sort((a, b) => {
    const aOther = /^Other/.test(a.CompanyName), bOther = /^Other/.test(b.CompanyName);
    if (aOther !== bOther) return aOther ? 1 : -1;
    return a.CompanyName.localeCompare(b.CompanyName);
  });

  const tq = total.Quotes, tbc = total.BridgingCount;
  const tsp = total.SumPremium, tsb = total.SumBridgingPremium;
  const totAvgBr = tbc > 0 ? tsb / tbc : null;

  const exclAvg = (co) => {
    const f = rows.filter(r => r.CompanyName !== co);
    const bc = f.reduce((s, r) => s + Number(r.BridgingCount || 0), 0);
    const sb = f.reduce((s, r) => s + Number(r.SumBridgingPremium || 0), 0);
    return bc > 0 ? sb / bc : null;
  };

  const totAvgPrem = tq > 0 ? tsp / tq : null;
  const totalRow = tq > 0 ? {
    _isTotal: true,
    CompanyName: "TOTAL",
    Quotes: tq,
    SumPremium: tsp,
    SizePct: 100,
    BridgingCount: tbc,
    BridgeRate: tq > 0 ? tbc / tq : null,
    AvgPremium: totAvgPrem,
    AvgBridgingPremium: totAvgBr,
    AvgPremiumDiff: (totAvgPrem != null && totAvgPrem > 0 && totAvgBr != null)
      ? (totAvgPrem - totAvgBr) / totAvgPrem
      : null,
    VsCompareCo: (comparisonCo && refCmpAvg != null && refCmpAvg > 0) ? (exclAvg(comparisonCo) / refCmpAvg - 1) : null,
    VsSIC:       (refSICAvg != null && refSICAvg > 0) ? (exclAvg("SIC") / refSICAvg - 1) : null,
  } : null;
  if (totalRow) {
    for (const c of COVERAGES) {
      totalRow[`Avg${c}`] = tbc > 0 ? Number(total[`Sum${c}Bridging`] || 0) / tbc : null;
    }
  }

  return { rows: derived, totalRow, total };
}

function avgBridging(row) {
  if (!row) return null;
  const bc = Number(row.BridgingCount || 0);
  const sb = Number(row.SumBridgingPremium || 0);
  return bc > 0 ? sb / bc : null;
}

/**
 * Assign ranks (1 = lowest value) to `rows[].<rankField>` based on `rows[].<valueField>`.
 * Null/undefined values don't get ranked (rankField stays null).
 * Ties share the same rank (min-rank method: 1, 2, 2, 4, ...).
 */
function assignRank(rows, valueField, rankField) {
  const ranked = rows
    .map((r, idx) => ({ idx, value: r[valueField] }))
    .filter(x => x.value != null);
  ranked.sort((a, b) => a.value - b.value);

  let lastValue = null, lastRank = 0;
  ranked.forEach((x, i) => {
    if (x.value !== lastValue) {
      lastRank = i + 1;
      lastValue = x.value;
    }
    rows[x.idx][rankField] = lastRank;
  });
  // Ensure rankField is explicitly null for un-ranked rows
  rows.forEach(r => { if (!(rankField in r)) r[rankField] = null; });
}

// ─── top stats (title bar) ────────────────────────────────────────────────

function updateTopStats(entry, total, nCompaniesShown) {
  document.getElementById("ts-state").textContent = entry.state;
  document.getElementById("ts-quotes").textContent =
    total ? Number(total.Quotes).toLocaleString() : "—";

  const ms = entry.months.map(String);
  const spread = ms.length
    ? `${displayYYYYMM(ms[0])} → ${displayYYYYMM(ms[ms.length-1])} (${ms.length} mo)`
    : "—";
  document.getElementById("ts-months").textContent = spread;

  // "X named" — count of named companies (excludes Other and numeric CompanyIds).
  const knownCos = entry.companies.filter(
    c => !/^Other/.test(c) && !/^\d+$/.test(c)
  ).length;
  document.getElementById("ts-cos").textContent = `${knownCos} named`;
}


// ─── wiring ────────────────────────────────────────────────────────────────

function wireControls() {
  document.getElementById("state").addEventListener("change", async e => {
    await loadState(e.target.value);
  });

  const filterIds = [
    "date-from", "date-to", "prem-min", "prem-max",
    "liab", "term", "market-provider",
    "credit-from", "credit-to", "county",
    "non-owner", "num-drivers", "num-vehicles",
    "prior-insurance", "year-bin",
    ...RATE_FILTER_IDS,
  ];
  for (const id of filterIds) {
    document.getElementById(id).addEventListener("change", refreshAll);
  }

  // Discount Simulator: Total + per-coverage inputs trigger a refresh on change.
  // Clamp >100 on blur (the +100% cap).
  const discIds = ["Total", ...COVERAGES];
  for (const id of discIds) {
    const el = document.getElementById(`disc-${id}`);
    if (!el) continue;
    el.addEventListener("change", () => {
      if (Number(el.value) > 100) el.value = "100";
      refreshAll();
    });
  }
  document.getElementById("disc-reset").addEventListener("click", () => {
    for (const id of discIds) {
      const el = document.getElementById(`disc-${id}`);
      if (el) el.value = "0";
    }
    refreshAll();
  });

  document.getElementById("reset").addEventListener("click", async () => {
    // populateFiltersFromData() already resets the rate filters to the state's
    // modal coverage signature via setDefaultRateFilters().
    await populateFiltersFromData();
    // Discount inputs.
    for (const id of discIds) {
      const el = document.getElementById(`disc-${id}`);
      if (el) el.value = "0";
    }
    await refreshAll();
  });

}

// ─── helpers ───────────────────────────────────────────────────────────────

async function sqlRows(sql) {
  const res = await app.conn.query(sql);
  return res.toArray().map(r => r.toJSON());
}

function setStatus(msg, isError = false) {
  const el = document.getElementById("status");
  if (!el) return;
  el.textContent = msg;
  el.style.color = isError ? "#c0392b" : "";
}

function formatTimestamp(iso) {
  try {
    // Force London time regardless of viewer's local timezone.
    const formatted = new Date(iso).toLocaleString("en-GB", {
      timeZone: "Europe/London",
      year: "numeric", month: "short", day: "2-digit",
      hour: "2-digit", minute: "2-digit", hour12: false,
    });
    return `${formatted} (London time)`;
  } catch {
    return iso;
  }
}
