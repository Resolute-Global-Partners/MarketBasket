/**
 * Market Basket — client-side data app.
 */

import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";

// ─── constants ─────────────────────────────────────────────────────────────

const PREM_BIN_SIZE = 500;
const PREM_BIN_CAP  = 5000;
const CREDIT_BIN_SIZE = 50;
const CREDIT_BIN_MIN = 600;
const CREDIT_BIN_MAX = 1750;
const YEAR_LABELS   = ["pre-2010", "2010-2014", "2015-2019", "2020+"];

// Coverage IDs match Sum<Id>Premium columns in the parquet, and disc-<Id> input IDs.
const COVERAGES = [
  "LiabBI", "LiabPD", "Comp", "Coll", "MedPay",
  "UIMBI", "UIMPD", "UninsBI", "UninsPD",
];

const PAYPLAN_ORDER = [
  "8% down, 12 payments", "10% down, 12 payments", "17% down, 6 payments",
  "20% down, 5 payments", "20% down, 6 payments", "22% down, 6 payments",
  "25% down, 4 payments", "25% down, 5 payments", "25% down, 6 payments",
  "30% down, 5 payments", "40% down, 3 payments", "42% down, 5 payments",
  "50% down, 2 payments", "Full pay",
];

const LIAB_ORDER = ["25/50", "50/100", "100/300"];

// ─── state ─────────────────────────────────────────────────────────────────

const app = {
  db: null, conn: null, index: null,
  currentState: null, grid: null,
  lastRows: null, lastTotalRow: null,
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
  const url = `data/${stateCode}.parquet?t=${Date.now()}`;
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`${url} missing (HTTP ${res.status})`);
  const buf = new Uint8Array(await res.arrayBuffer());

  const fname = `${stateCode}.parquet`;
  await app.db.registerFileBuffer(fname, buf);
  await app.conn.query(`DROP VIEW IF EXISTS mb`);
  await app.conn.query(`CREATE VIEW mb AS SELECT * FROM read_parquet('${fname}')`);

  app.currentState = stateCode;
  await populateFiltersFromData();
  buildGrid(entry);
  await refreshAll();

  setStatus(`${stateCode} ready`);
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

  setOptions("liab", ["Any", ...LIAB_ORDER], "Any");

  let ppPresent = new Set();
  try {
    const ppRows = await sqlRows(
      `SELECT DISTINCT PayPlan FROM mb WHERE PayPlan IS NOT NULL ORDER BY PayPlan`
    );
    ppPresent = new Set(ppRows.map(r => r.PayPlan));
  } catch (e) { console.warn("payplan query failed", e); }
  setOptions("payplan", ["Any", ...PAYPLAN_ORDER.filter(p => ppPresent.has(p))], "Any");

  setOptions("term", ["6", "12"], "6");
  setOptions("non-owner", ["Any", "No", "Yes"], "Any");
  setOptions("num-drivers",  ["Any", "1", "2", "3", "4+"], "Any");
  setOptions("num-vehicles", ["Any", "1", "2", "3", "4", "5+"], "Any");
  setOptions("prior-insurance", ["Any", "No", "Yes"], "Any");
  setOptions("year-bin", ["Any", ...YEAR_LABELS], "Any");

  // Credit min/max: 50-pt buckets from CREDIT_BIN_MIN to CREDIT_BIN_MAX.
  const creditBins = [];
  for (let v = CREDIT_BIN_MIN; v <= CREDIT_BIN_MAX; v += CREDIT_BIN_SIZE) creditBins.push(v);
  setOptions("credit-min", creditBins, CREDIT_BIN_MIN);
  setOptions("credit-max", creditBins, CREDIT_BIN_MAX);

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
}

// ─── SQL ───────────────────────────────────────────────────────────────────

function currentFilters() {
  const v = id => document.getElementById(id).value;
  return {
    dateFrom: parseInt(v("date-from"), 10),
    dateTo:   parseInt(v("date-to"),   10),
    premMin:  parseInt(v("prem-min"),  10),
    premMax:  parseInt(v("prem-max"),  10),
    creditMin: parseInt(v("credit-min"), 10),
    creditMax: parseInt(v("credit-max"), 10),
    county:   v("county"),
    liab:     v("liab"),
    payplan:  v("payplan"),
    term:     parseInt(v("term"), 10),
    nonOwner: v("non-owner"),
    numDrv:   v("num-drivers"),
    numVeh:   v("num-vehicles"),
    prior:    v("prior-insurance"),
    yearBin:  v("year-bin"),
    marketProvider: v("market-provider"),
  };
}

// Read all 9 discount inputs and clamp to (-Inf, 100]. Returns object keyed by COVERAGES name.
function currentDiscounts() {
  const out = {};
  for (const c of COVERAGES) {
    const el = document.getElementById(`disc-${c}`);
    let n = el ? Number(el.value) : 0;
    if (!Number.isFinite(n)) n = 0;
    if (n > 100) n = 100;
    out[c] = n;
  }
  return out;
}

// Has any non-zero discount? Avoids the CASE expression when nothing is set.
function anyDiscountActive(disc) {
  return Object.values(disc).some(v => v !== 0);
}

function whereClause(f) {
  const conds = [];
  conds.push(`YYYYMM >= ${f.dateFrom}`);
  conds.push(`YYYYMM <= ${f.dateTo}`);
  conds.push(`PremBin >= ${f.premMin}`);
  conds.push(`PremBin <  ${f.premMax}`);
  conds.push(`Term = ${f.term}`);
  // Credit range: filters out NULL CreditBin rows when credit min/max moved off
  // their defaults. When at default extremes, allow NULL through.
  const creditAtDefault = (f.creditMin === CREDIT_BIN_MIN && f.creditMax === CREDIT_BIN_MAX);
  if (!creditAtDefault) {
    conds.push(`CreditBin IS NOT NULL AND CreditBin >= ${f.creditMin} AND CreditBin <= ${f.creditMax}`);
  }
  if (f.county !== "Any") conds.push(`County = '${f.county.replace(/'/g, "''")}'`);
  if (f.liab    !== "Any") conds.push(`LiabLimits = '${f.liab}'`);
  if (f.payplan !== "Any") conds.push(`PayPlan = '${f.payplan.replace(/'/g, "''")}'`);
  if (f.numDrv  !== "Any") conds.push(`NumDrivers = '${f.numDrv}'`);
  if (f.numVeh  !== "Any") conds.push(`NumVehicles = '${f.numVeh}'`);
  if (f.yearBin !== "Any") conds.push(`YearBin = '${f.yearBin}'`);
  if (f.nonOwner !== "Any") conds.push(`NonOwner = ${f.nonOwner === "Yes" ? 1 : 0}`);
  if (f.prior    !== "Any") conds.push(`PriorInsurance = ${f.prior === "Yes" ? 1 : 0}`);

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
 * applies coverage discounts only to companies in `ourCompanies`. When no
 * discounts are active, falls back to plain SumPremium.
 *
 * Discount semantics: positive = discount (reduce premium), negative = surcharge.
 * Floored at 0 implicitly via the +100% cap on inputs (sum of coverage
 * subtractions can't exceed sum of coverages).
 */
function adjustedSumPremiumSQL(disc, ourCompanies) {
  if (!anyDiscountActive(disc) || !ourCompanies || ourCompanies.length === 0) {
    return "SumPremium";
  }
  const inList = ourCompanies.map(c => `'${c.replace(/'/g, "''")}'`).join(",");
  const subtract = COVERAGES
    .map(c => `${(disc[c] / 100)} * Sum${c}Premium`)
    .join(" + ");
  return `CASE WHEN CompanyName IN (${inList}) THEN SumPremium - (${subtract}) ELSE SumPremium END`;
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
  const aggRows = await sqlRows(`
    SELECT  CompanyName,
            CAST(SUM(Quotes)             AS DOUBLE) AS Quotes,
            CAST(SUM(${adjPrem})         AS DOUBLE) AS SumPremium,
            CAST(SUM(BridgingCount)      AS DOUBLE) AS BridgingCount,
            CAST(SUM(${adjBridge})       AS DOUBLE) AS SumBridgingPremium
    FROM mb ${where}
    GROUP BY CompanyName
  `);

  // Always show every company that exists in this state. If the current
  // filter returns zero rows for a company, show it with zeros instead of
  // hiding it — matching the original Excel's behaviour.
  const seen = new Set(aggRows.map(r => r.CompanyName));
  for (const c of entry.companies) {
    if (!seen.has(c)) {
      aggRows.push({
        CompanyName: c,
        Quotes: 0, SumPremium: 0, BridgingCount: 0, SumBridgingPremium: 0,
      });
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

  const total = rows.reduce((a, r) => ({
    Quotes:             a.Quotes + Number(r.Quotes || 0),
    SumPremium:         a.SumPremium + Number(r.SumPremium || 0),
    BridgingCount:      a.BridgingCount + Number(r.BridgingCount || 0),
    SumBridgingPremium: a.SumBridgingPremium + Number(r.SumBridgingPremium || 0),
  }), { Quotes: 0, SumPremium: 0, BridgingCount: 0, SumBridgingPremium: 0 });

  const refCmpAvg = avgBridging(rows.find(r => r.CompanyName === comparisonCo));
  const refSICAvg = avgBridging(rows.find(r => r.CompanyName === "SIC"));

  const derived = rows.map(r => {
    const q  = Number(r.Quotes || 0);
    const bc = Number(r.BridgingCount || 0);
    const sp = Number(r.SumPremium || 0);
    const sb = Number(r.SumBridgingPremium || 0);
    const avgBr = bc > 0 ? sb / bc : null;
    return {
      CompanyName: r.CompanyName,
      Quotes: q,
      SizePct: total.Quotes > 0 ? (q / total.Quotes) * 100 : null,
      BridgingCount: bc,
      BridgeRate: q > 0 ? bc / q : null,
      AvgPremium: q > 0 ? sp / q : null,
      AvgBridgingPremium: avgBr,
      VsCompareCo: (avgBr != null && refCmpAvg != null && refCmpAvg > 0) ? (avgBr / refCmpAvg - 1) : null,
      VsSIC:       (avgBr != null && refSICAvg != null && refSICAvg > 0) ? (avgBr / refSICAvg - 1) : null,
    };
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

  const totalRow = tq > 0 ? {
    _isTotal: true,
    CompanyName: "TOTAL",
    Quotes: tq,
    SumPremium: tsp,
    SizePct: 100,
    BridgingCount: tbc,
    BridgeRate: tq > 0 ? tbc / tq : null,
    AvgPremium: tq > 0 ? tsp / tq : null,
    AvgBridgingPremium: totAvgBr,
    VsCompareCo: (comparisonCo && refCmpAvg != null && refCmpAvg > 0) ? (exclAvg(comparisonCo) / refCmpAvg - 1) : null,
    VsSIC:       (refSICAvg != null && refSICAvg > 0) ? (exclAvg("SIC") / refSICAvg - 1) : null,
  } : null;

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
    "liab", "payplan", "term", "market-provider",
    "credit-min", "credit-max", "county",
    "non-owner", "num-drivers", "num-vehicles",
    "prior-insurance", "year-bin",
  ];
  for (const id of filterIds) {
    document.getElementById(id).addEventListener("change", refreshAll);
  }

  // Discount Simulator: per-coverage inputs trigger a refresh on change/input.
  // Clamp >100 on blur (the +100% cap).
  for (const c of COVERAGES) {
    const el = document.getElementById(`disc-${c}`);
    if (!el) continue;
    el.addEventListener("change", () => {
      if (Number(el.value) > 100) el.value = "100";
      refreshAll();
    });
  }
  document.getElementById("disc-reset").addEventListener("click", () => {
    for (const c of COVERAGES) {
      const el = document.getElementById(`disc-${c}`);
      if (el) el.value = "0";
    }
    refreshAll();
  });

  document.getElementById("reset").addEventListener("click", async () => {
    await populateFiltersFromData();
    // Also reset discount inputs.
    for (const c of COVERAGES) {
      const el = document.getElementById(`disc-${c}`);
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
