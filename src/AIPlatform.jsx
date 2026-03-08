/**
 * ================================================================
 * AI DATA SCIENTIST PLATFORM
 * Module 1: Dataset Understanding Engine
 * Module 2: AI Data Cleaning Engine
 * ================================================================
 * Senior Developer Build — Real engines, zero mock data
 * Architecture: Single-file, shared state, sequential pipeline
 * ================================================================
 */

import { useState, useCallback, useRef, useEffect } from "react";
import Papa from "papaparse";
import * as XLSX from "xlsx";

// ════════════════════════════════════════════════════════════════
// SECTION 1 — SHARED ENGINE: PROFILING
// ════════════════════════════════════════════════════════════════

function detectType(values) {
  const nonNull = values.filter(v => v !== null && v !== undefined && v !== "");
  if (nonNull.length === 0) return "empty";
  const numericCount = nonNull.filter(v => !isNaN(Number(v)) && String(v).trim() !== "").length;
  if (numericCount / nonNull.length > 0.85) return "numeric";
  const datePatterns = [
    /^\d{4}-\d{2}-\d{2}/, /^\d{2}\/\d{2}\/\d{4}/,
    /^\d{2}-\d{2}-\d{4}/, /^\w+ \d{1,2}, \d{4}/, /^\d{1,2} \w+ \d{4}/
  ];
  const dateCount = nonNull.filter(v => datePatterns.some(p => p.test(String(v)))).length;
  if (dateCount / nonNull.length > 0.7) return "datetime";
  const boolVals = new Set(["true","false","yes","no","0","1","t","f","y","n"]);
  const boolCount = nonNull.filter(v => boolVals.has(String(v).toLowerCase())).length;
  if (boolCount / nonNull.length > 0.9) return "boolean";
  const avgLen = nonNull.reduce((a, v) => a + String(v).length, 0) / nonNull.length;
  if (avgLen > 40) return "text";
  return "categorical";
}

function computeStats(values, type) {
  const nonNull = values.filter(v => v !== null && v !== undefined && v !== "");
  const missing = values.length - nonNull.length;
  const missingPct = values.length > 0 ? (missing / values.length) * 100 : 0;
  const unique = new Set(nonNull.map(String)).size;
  const cardinality = nonNull.length > 0 ? (unique / nonNull.length) * 100 : 0;
  let stats = { missing, missingPct, unique, cardinality };

  if (type === "numeric") {
    const nums = nonNull.map(Number).filter(n => !isNaN(n)).sort((a, b) => a - b);
    if (!nums.length) return stats;
    const mean = nums.reduce((a, b) => a + b, 0) / nums.length;
    const sorted = [...nums];
    const median = sorted.length % 2 === 0
      ? (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2
      : sorted[Math.floor(sorted.length / 2)];
    const std = Math.sqrt(nums.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / nums.length);
    const q1 = sorted[Math.floor(sorted.length * 0.25)];
    const q3 = sorted[Math.floor(sorted.length * 0.75)];
    const iqr = q3 - q1;
    const outlierCount = sorted.filter(v => v < q1 - 1.5*iqr || v > q3 + 1.5*iqr).length;
    const skewness = std > 0 ? (3 * (mean - median)) / std : 0;
    const skewLabel = Math.abs(skewness) < 0.5 ? "Normal"
      : skewness > 0 ? "Right-skewed" : "Left-skewed";
    const min = sorted[0], max = sorted[sorted.length - 1];
    const bucketSize = (max - min) / 10 || 1;
    const buckets = Array(10).fill(0);
    sorted.forEach(v => { buckets[Math.min(Math.floor((v - min) / bucketSize), 9)]++; });
    stats = { ...stats, mean, median, std, min, max, q1, q3, iqr,
      outlierCount, skewness, skewLabel, buckets, count: nums.length };
  }
  if (type === "categorical") {
    const freq = {};
    nonNull.forEach(v => { freq[String(v)] = (freq[String(v)] || 0) + 1; });
    const sorted = Object.entries(freq).sort((a, b) => b[1] - a[1]);
    stats = { ...stats, topValues: sorted.slice(0, 5), totalCategorical: nonNull.length };
  }
  return stats;
}

function getRecommendation(col, type, stats) {
  const recs = [];
  if (stats.missingPct > 40)
    recs.push({ level:"danger",  text:`Drop column — ${stats.missingPct.toFixed(1)}% missing` });
  else if (stats.missingPct > 30)
    recs.push({ level:"warning", text:"MICE / Iterative imputation (30–40% missing)" });
  else if (stats.missingPct > 5) {
    if (type === "numeric")
      recs.push({ level:"warning", text: stats.skewLabel !== "Normal"
        ? "Median imputation (skewed dist.)" : "Mean imputation recommended" });
    else
      recs.push({ level:"warning", text:"Mode imputation or 'Unknown' category" });
  }
  if (type === "numeric" && stats.outlierCount > 0) {
    const pct = ((stats.outlierCount / stats.count) * 100).toFixed(1);
    recs.push({ level: pct > 5 ? "danger":"warning",
      text:`${stats.outlierCount} outliers (${pct}%) — IQR detected` });
  }
  if (type === "categorical" && stats.cardinality > 90)
    recs.push({ level:"info", text:"High cardinality — consider Target Encoding" });
  if (type === "numeric" && stats.skewLabel !== "Normal")
    recs.push({ level:"info", text:`${stats.skewLabel} — consider log/sqrt transform` });
  if (!recs.length) recs.push({ level:"good", text:"Column looks clean ✓" });
  return recs;
}

function computeQualityScore(columns) {
  if (!columns.length) return 0;
  let score = 100;
  const avgMissing = columns.reduce((a, c) => a + c.stats.missingPct, 0) / columns.length;
  score -= Math.min(40, avgMissing * 1.5);
  score -= Math.min(20, columns.filter(c => c.type === "numeric" && c.stats.outlierCount > 0).length * 4);
  return Math.max(0, Math.round(score));
}

function detectDuplicates(rows) {
  const seen = new Set(); let dupes = 0;
  rows.forEach(row => {
    const k = JSON.stringify(Object.values(row));
    seen.has(k) ? dupes++ : seen.add(k);
  });
  return dupes;
}

function profileDataset(rows) {
  if (!rows.length) return null;
  const cols = Object.keys(rows[0]);
  const columns = cols.map(col => {
    const values = rows.map(r => {
      const v = r[col]; return (v === "" || v === null || v === undefined) ? null : v;
    });
    const type = detectType(values);
    const stats = computeStats(values, type);
    return { name: col, type, stats, recommendations: getRecommendation(col, type, stats) };
  });
  const duplicates = detectDuplicates(rows);
  return { rowCount: rows.length, colCount: cols.length, columns, duplicates,
    duplicatePct: (duplicates / rows.length) * 100, qualityScore: computeQualityScore(columns) };
}

// ════════════════════════════════════════════════════════════════
// SECTION 2 — MODULE 2 ENGINE: AI DATA CLEANING
// ════════════════════════════════════════════════════════════════

/**
 * CleaningEngine — deterministic, step-by-step pipeline
 * Each step returns { rows, log entries }
 * Fully auditable — every change recorded
 */
class CleaningEngine {
  constructor(rows, profile) {
    this.rows = rows.map(r => ({ ...r })); // deep clone
    this.profile = profile;
    this.log = [];
    this.stats = {
      rowsDropped: 0, colsDropped: 0, cellsImputed: 0,
      outliersWinsorized: 0, typesFixed: 0, duplicatesRemoved: 0,
    };
  }

  _addLog(step, column, action, detail, affected = 0, severity = "info") {
    this.log.push({ id: this.log.length, step, column, action, detail, affected, severity,
      timestamp: Date.now() });
  }

  // ── Step 1: Drop columns with > 40% missing ─────────────────
  step1_dropHighMissingCols() {
    const toDrop = this.profile.columns
      .filter(c => c.stats.missingPct > 40)
      .map(c => c.name);

    toDrop.forEach(col => {
      this.rows = this.rows.map(r => { const nr = { ...r }; delete nr[col]; return nr; });
      this._addLog(1, col, "DROP COLUMN",
        `${this.profile.columns.find(c=>c.name===col)?.stats.missingPct.toFixed(1)}% missing — exceeds 40% threshold`,
        this.rows.length, "danger");
      this.stats.colsDropped++;
    });

    if (!toDrop.length)
      this._addLog(1, "—", "SKIP", "No columns exceed 40% missing threshold", 0, "good");
    return this;
  }

  // ── Step 2: Remove exact duplicate rows ─────────────────────
  step2_removeDuplicates() {
    const before = this.rows.length;
    const seen = new Set();
    this.rows = this.rows.filter(row => {
      const k = JSON.stringify(Object.values(row));
      if (seen.has(k)) return false;
      seen.add(k); return true;
    });
    const removed = before - this.rows.length;
    this.stats.duplicatesRemoved = removed;
    this._addLog(2, "ALL", removed > 0 ? "REMOVE DUPLICATES" : "SKIP",
      removed > 0 ? `${removed} exact duplicate rows removed` : "No duplicate rows found",
      removed, removed > 0 ? "warning" : "good");
    return this;
  }

  // ── Step 3: Impute missing values ────────────────────────────
  step3_imputeMissing() {
    const currentCols = Object.keys(this.rows[0] || {});
    const colMeta = this.profile.columns.filter(c => currentCols.includes(c.name));

    colMeta.forEach(col => {
      const { name, type, stats } = col;
      if (stats.missingPct === 0) return;

      // < 5% missing → drop those rows
      if (stats.missingPct < 5) {
        const before = this.rows.length;
        this.rows = this.rows.filter(r => r[name] !== null && r[name] !== undefined && r[name] !== "");
        const dropped = before - this.rows.length;
        this.stats.rowsDropped += dropped;
        this._addLog(3, name, "DROP ROWS",
          `${stats.missingPct.toFixed(1)}% missing (<5%) — dropped ${dropped} rows`, dropped, "info");
        return;
      }

      // 5–40%: impute
      if (type === "numeric") {
        const nums = this.rows
          .map(r => r[name])
          .filter(v => v !== null && v !== undefined && v !== "" && !isNaN(Number(v)))
          .map(Number);
        if (!nums.length) return;

        let fillValue, method;
        if (stats.skewLabel !== "Normal") {
          const sorted = [...nums].sort((a,b)=>a-b);
          fillValue = sorted.length % 2 === 0
            ? (sorted[sorted.length/2-1] + sorted[sorted.length/2]) / 2
            : sorted[Math.floor(sorted.length/2)];
          method = "MEDIAN";
        } else {
          fillValue = nums.reduce((a,b) => a+b, 0) / nums.length;
          method = "MEAN";
        }

        let count = 0;
        this.rows = this.rows.map(r => {
          if (r[name] === null || r[name] === undefined || r[name] === "") {
            count++; return { ...r, [name]: +fillValue.toFixed(4) };
          }
          return r;
        });
        this.stats.cellsImputed += count;
        this._addLog(3, name, `IMPUTE (${method})`,
          `${count} cells filled with ${method.toLowerCase()} = ${fillValue.toFixed(2)} (${stats.skewLabel})`,
          count, "warning");
      } else {
        // Categorical / other — mode
        const freq = {};
        this.rows.forEach(r => {
          const v = r[name];
          if (v !== null && v !== undefined && v !== "") freq[v] = (freq[v]||0) + 1;
        });
        const mode = Object.entries(freq).sort((a,b)=>b[1]-a[1])[0]?.[0] ?? "Unknown";
        let count = 0;
        this.rows = this.rows.map(r => {
          if (r[name] === null || r[name] === undefined || r[name] === "") {
            count++; return { ...r, [name]: mode };
          }
          return r;
        });
        this.stats.cellsImputed += count;
        this._addLog(3, name, "IMPUTE (MODE)",
          `${count} cells filled with mode = "${mode}"`, count, "warning");
      }
    });
    return this;
  }

  // ── Step 4: Winsorize outliers (IQR method) ──────────────────
  step4_treatOutliers() {
    const currentCols = Object.keys(this.rows[0] || {});
    const numericCols = this.profile.columns.filter(c =>
      c.type === "numeric" && currentCols.includes(c.name) && c.stats.outlierCount > 0);

    if (!numericCols.length) {
      this._addLog(4, "—", "SKIP", "No outliers detected in any numeric column", 0, "good");
      return this;
    }

    numericCols.forEach(col => {
      const { name, stats } = col;
      const nums = this.rows.map(r => Number(r[name])).filter(n => !isNaN(n)).sort((a,b)=>a-b);
      const q1 = nums[Math.floor(nums.length * 0.25)];
      const q3 = nums[Math.floor(nums.length * 0.75)];
      const iqr = q3 - q1;
      const lower = q1 - 1.5 * iqr;
      const upper = q3 + 1.5 * iqr;
      let count = 0;
      this.rows = this.rows.map(r => {
        const v = Number(r[name]);
        if (isNaN(v)) return r;
        if (v < lower) { count++; return { ...r, [name]: +lower.toFixed(4) }; }
        if (v > upper) { count++; return { ...r, [name]: +upper.toFixed(4) }; }
        return r;
      });
      this.stats.outliersWinsorized += count;
      this._addLog(4, name, "WINSORIZE",
        `${count} outliers capped to [${lower.toFixed(2)}, ${upper.toFixed(2)}] (IQR bounds)`,
        count, "warning");
    });
    return this;
  }

  // ── Step 5: Data type correction ─────────────────────────────
  step5_fixTypes() {
    const currentCols = Object.keys(this.rows[0] || {});
    const colMeta = this.profile.columns.filter(c => currentCols.includes(c.name));

    colMeta.forEach(col => {
      const { name, type } = col;
      if (type === "numeric") {
        let count = 0;
        this.rows = this.rows.map(r => {
          const v = r[name];
          if (v !== null && v !== undefined && v !== "" && typeof v === "string" && !isNaN(Number(v))) {
            count++; return { ...r, [name]: Number(v) };
          }
          return r;
        });
        if (count > 0) {
          this.stats.typesFixed += count;
          this._addLog(5, name, "TYPE CAST", `${count} string values cast to Number`, count, "info");
        }
      }
      if (type === "boolean") {
        const trueVals = new Set(["true","yes","1","t","y"]);
        let count = 0;
        this.rows = this.rows.map(r => {
          const v = String(r[name]).toLowerCase();
          if (typeof r[name] === "boolean") return r;
          if (r[name] !== null && r[name] !== undefined && r[name] !== "") {
            count++; return { ...r, [name]: trueVals.has(v) };
          }
          return r;
        });
        if (count > 0) {
          this.stats.typesFixed += count;
          this._addLog(5, name, "TYPE CAST", `${count} values standardised to Boolean`, count, "info");
        }
      }
    });

    if (!this.log.filter(l => l.step === 5).length)
      this._addLog(5, "—", "SKIP", "All column types already correct", 0, "good");
    return this;
  }

  // ── Step 6: Categorical standardisation ─────────────────────
  step6_standardizeCategorical() {
    const currentCols = Object.keys(this.rows[0] || {});
    const catCols = this.profile.columns.filter(c =>
      c.type === "categorical" && currentCols.includes(c.name));

    catCols.forEach(col => {
      let trimmed = 0, normalized = 0;
      // Build a normalization map (e.g. "male","Male","MALE" → "Male")
      const seen = {};
      this.rows.forEach(r => {
        const v = r[col.name];
        if (v === null || v === undefined || v === "") return;
        const clean = String(v).trim();
        const key = clean.toLowerCase();
        if (!seen[key]) seen[key] = clean[0].toUpperCase() + clean.slice(1).toLowerCase();
      });
      this.rows = this.rows.map(r => {
        const v = r[col.name];
        if (v === null || v === undefined || v === "") return r;
        const clean = String(v).trim();
        const norm = seen[clean.toLowerCase()] || clean;
        if (clean !== String(v)) trimmed++;
        if (norm !== clean) normalized++;
        return { ...r, [col.name]: norm };
      });
      if (trimmed + normalized > 0) {
        this._addLog(6, col.name, "STANDARDISE",
          `Trimmed ${trimmed} whitespace, normalized ${normalized} inconsistent labels`,
          trimmed + normalized, "info");
        this.stats.typesFixed += trimmed + normalized;
      }
    });
    if (!this.log.filter(l => l.step === 6).length)
      this._addLog(6, "—", "SKIP", "All categorical values already standardised", 0, "good");
    return this;
  }

  run() {
    return this
      .step1_dropHighMissingCols()
      .step2_removeDuplicates()
      .step3_imputeMissing()
      .step4_treatOutliers()
      .step5_fixTypes()
      .step6_standardizeCategorical();
  }

  getResult() {
    const afterProfile = profileDataset(this.rows);
    return { cleanedRows: this.rows, log: this.log, stats: this.stats, afterProfile };
  }
}

// Sample data generator (shared)
function generateSampleData(n = 300) {
  const cities = ["Mumbai","Delhi","Bangalore","Pune","Chennai","Hyderabad"];
  const products = ["Laptop","Phone","Tablet","Watch","Headphones"];
  const rows = [];
  for (let i = 0; i < n; i++) {
    rows.push({
      customer_id: `CUST-${String(i+1).padStart(4,"0")}`,
      age:      Math.random() > 0.07  ? Math.round(18 + Math.random()*60)                             : null,
      salary:   Math.random() > 0.12  ? Math.round(20000 + Math.random()*200000 + (Math.random()>0.9?800000:0)) : null,
      city:     Math.random() > 0.03  ? cities[Math.floor(Math.random()*cities.length)]              : null,
      product:  products[Math.floor(Math.random()*products.length)],
      purchase_amount: Math.round(500 + Math.random()*50000),
      is_premium: Math.random() > 0.7 ? "Yes" : "No",
      join_date: `2022-0${1+Math.floor(Math.random()*9)}-${10+Math.floor(Math.random()*18)}`,
      rating:   Math.random() > 0.05  ? +(1 + Math.random()*4).toFixed(1)                           : null,
      postal_code: Math.random() > 0.45 ? null : `4${Math.floor(10000+Math.random()*90000)}`,
    });
  }
  for (let i = 0; i < 8; i++) rows.push({ ...rows[Math.floor(Math.random()*20)] });
  return rows;
}

// ════════════════════════════════════════════════════════════════
// SECTION 3 — DESIGN TOKENS & SHARED UI PRIMITIVES
// ════════════════════════════════════════════════════════════════

const C = {
  bg:       "#03060F",
  surface:  "rgba(255,255,255,0.025)",
  border:   "rgba(255,255,255,0.07)",
  text:     "#E2E8F0",
  muted:    "#64748B",
  subtle:   "#94A3B8",
  green:    "#6EE7B7",
  blue:     "#93C5FD",
  purple:   "#C4B5FD",
  yellow:   "#FCD34D",
  red:      "#FCA5A5",
  orange:   "#FDBA74",
  accentG:  "rgba(110,231,183,0.08)",
  accentB:  "rgba(147,197,253,0.08)",
};

const TYPE_META = {
  numeric:     { icon:"#",  color:C.green,  bg:"rgba(110,231,183,0.1)", label:"Numeric" },
  categorical: { icon:"≡",  color:C.blue,   bg:"rgba(147,197,253,0.1)", label:"Categorical" },
  datetime:    { icon:"◷",  color:C.yellow, bg:"rgba(252,211,77,0.1)",  label:"Datetime" },
  boolean:     { icon:"⊡",  color:"#F9A8D4",bg:"rgba(249,168,212,0.1)",label:"Boolean" },
  text:        { icon:"¶",  color:C.purple, bg:"rgba(196,181,253,0.1)", label:"Text" },
  empty:       { icon:"∅",  color:C.muted,  bg:"rgba(148,163,184,0.1)", label:"Empty" },
};

const REC_META = {
  good:    { color:C.green,  icon:"✓", label:"Clean" },
  info:    { color:C.blue,   icon:"ℹ", label:"INFO" },
  warning: { color:C.yellow, icon:"⚠", label:"WARN" },
  danger:  { color:C.red,    icon:"✕", label:"CRIT" },
};

const STEP_META = {
  1: { label:"Drop Columns",    icon:"⊟", color:C.red    },
  2: { label:"Remove Dupes",    icon:"⟳", color:C.yellow },
  3: { label:"Impute Missing",  icon:"◈", color:C.blue   },
  4: { label:"Treat Outliers",  icon:"⋈", color:C.orange },
  5: { label:"Fix Types",       icon:"⊞", color:C.purple },
  6: { label:"Standardise",     icon:"⌸", color:C.green  },
};

const fmt = (n, d=2) => typeof n === "number" ? n.toFixed(d) : "—";
const fmtK = n => n >= 1000 ? `${(n/1000).toFixed(1)}k` : String(n);

function downloadCSV(rows, filename="cleaned_dataset.csv") {
  const csv = Papa.unparse(rows);
  const blob = new Blob([csv], { type:"text/csv" });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement("a");
  a.href = url; a.download = filename; a.click();
  URL.revokeObjectURL(url);
}

// ── Shared UI atoms ──────────────────────────────────────────

function MissingBar({ pct }) {
  const color = pct > 40 ? C.red : pct > 5 ? C.yellow : C.green;
  return (
    <div style={{ display:"flex", alignItems:"center", gap:8 }}>
      <div style={{ flex:1, height:5, borderRadius:99, background:"rgba(255,255,255,0.06)" }}>
        <div style={{ height:"100%", borderRadius:99, background:color,
          width:`${Math.min(pct,100)}%`, transition:"width 0.8s ease" }}/>
      </div>
      <span style={{ fontSize:11, color, fontFamily:"'DM Mono',monospace",
        minWidth:42, textAlign:"right" }}>{pct.toFixed(1)}%</span>
    </div>
  );
}

function MiniSparkline({ buckets }) {
  if (!buckets) return null;
  const max = Math.max(...buckets, 1);
  return (
    <div style={{ display:"flex", alignItems:"flex-end", gap:2, height:32 }}>
      {buckets.map((v, i) => (
        <div key={i} style={{ width:8, borderRadius:2,
          height:`${Math.max(4,(v/max)*32)}px`,
          background:`rgba(99,179,237,${0.3+(v/max)*0.7})`,
          transition:"height 0.4s ease" }}/>
      ))}
    </div>
  );
}

function QualityBadge({ score }) {
  const color = score >= 80 ? C.green : score >= 60 ? C.yellow : C.red;
  const label = score >= 80 ? "Good" : score >= 60 ? "Fair" : "Poor";
  const r = 40, circ = 2*Math.PI*r, dash = (score/100)*circ;
  return (
    <div style={{ position:"relative", width:100, height:100 }}>
      <svg width={100} height={100} style={{ transform:"rotate(-90deg)" }}>
        <circle cx={50} cy={50} r={r} fill="none" stroke="rgba(255,255,255,0.05)" strokeWidth={8}/>
        <circle cx={50} cy={50} r={r} fill="none" stroke={color} strokeWidth={8}
          strokeDasharray={`${dash} ${circ}`} strokeLinecap="round"
          style={{ transition:"stroke-dasharray 1.2s ease" }}/>
      </svg>
      <div style={{ position:"absolute", inset:0, display:"flex", flexDirection:"column",
        alignItems:"center", justifyContent:"center" }}>
        <span style={{ fontSize:22, fontWeight:800, color, fontFamily:"'DM Mono',monospace" }}>{score}</span>
        <span style={{ fontSize:10, color:C.muted }}>{label}</span>
      </div>
    </div>
  );
}

function StatPill({ label, value, color=C.blue }) {
  return (
    <div style={{ display:"flex", flexDirection:"column", gap:2 }}>
      <span style={{ fontSize:10, color:C.muted, textTransform:"uppercase",
        letterSpacing:"0.08em" }}>{label}</span>
      <span style={{ fontSize:15, fontWeight:700, color, fontFamily:"'DM Mono',monospace" }}>{value}</span>
    </div>
  );
}

function Tag({ children, color=C.blue }) {
  return (
    <span style={{ fontSize:10, color, background:`${color}18`,
      borderRadius:99, padding:"1px 7px", whiteSpace:"nowrap" }}>
      {children}
    </span>
  );
}

function SectionHeader({ children }) {
  return (
    <p style={{ fontSize:11, color:C.muted, textTransform:"uppercase",
      letterSpacing:"0.1em", marginBottom:12 }}>{children}</p>
  );
}

// ════════════════════════════════════════════════════════════════
// SECTION 4 — MODULE 1 COMPONENTS
// ════════════════════════════════════════════════════════════════

function ColumnCard({ col }) {
  const [open, setOpen] = useState(false);
  const meta = TYPE_META[col.type] || TYPE_META.categorical;
  return (
    <div onClick={() => setOpen(o => !o)} style={{
      background: open ? "rgba(255,255,255,0.04)" : C.surface,
      border:`1px solid ${open?"rgba(255,255,255,0.09)":C.border}`,
      borderRadius:12, padding:"13px 17px",
      cursor:"pointer", transition:"all 0.2s",
    }}>
      <div style={{ display:"flex", alignItems:"center", gap:11 }}>
        <div style={{ width:32, height:32, borderRadius:8, background:meta.bg,
          border:`1px solid ${meta.color}33`, display:"flex",
          alignItems:"center", justifyContent:"center",
          fontSize:15, color:meta.color, fontFamily:"'DM Mono',monospace", flexShrink:0 }}>
          {meta.icon}
        </div>
        <div style={{ flex:1, minWidth:0 }}>
          <div style={{ display:"flex", alignItems:"center", gap:7, marginBottom:4 }}>
            <span style={{ fontSize:13, fontWeight:600, color:C.text,
              overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>{col.name}</span>
            <Tag color={meta.color}>{meta.label}</Tag>
          </div>
          <MissingBar pct={col.stats.missingPct}/>
        </div>
        {col.type === "numeric" && <MiniSparkline buckets={col.stats.buckets}/>}
        {col.recommendations.slice(0,1).map((r,i) => {
          const rm = REC_META[r.level];
          return <Tag key={i} color={rm.color}>{rm.icon} {rm.label}</Tag>;
        })}
        <span style={{ color:C.muted, fontSize:13, flexShrink:0,
          transform:open?"rotate(180deg)":"none", transition:"transform 0.2s" }}>▾</span>
      </div>

      {open && (
        <div style={{ marginTop:15, paddingTop:15,
          borderTop:`1px solid rgba(255,255,255,0.06)`, animation:"fadeIn 0.2s ease" }}>
          {col.type === "numeric" && (
            <div style={{ display:"grid", gridTemplateColumns:"repeat(4,1fr)", gap:16, marginBottom:16 }}>
              <StatPill label="Mean"    value={fmt(col.stats.mean)}   color={C.green}/>
              <StatPill label="Median"  value={fmt(col.stats.median)} color={C.blue}/>
              <StatPill label="Std Dev" value={fmt(col.stats.std)}    color={C.purple}/>
              <StatPill label="Skew"    value={col.stats.skewLabel}   color={col.stats.skewLabel==="Normal"?C.green:C.yellow}/>
              <StatPill label="Min"     value={fmt(col.stats.min,0)}  color={C.muted}/>
              <StatPill label="Max"     value={fmt(col.stats.max,0)}  color={C.muted}/>
              <StatPill label="Outliers" value={col.stats.outlierCount} color={col.stats.outlierCount>0?C.red:C.green}/>
              <StatPill label="Unique"  value={col.stats.unique}     color={C.yellow}/>
            </div>
          )}
          {col.type === "categorical" && col.stats.topValues && (
            <div style={{ marginBottom:16 }}>
              <SectionHeader>Top Values</SectionHeader>
              {col.stats.topValues.map(([val,cnt],i) => {
                const pct = (cnt/col.stats.totalCategorical)*100;
                return (
                  <div key={i} style={{ display:"flex", alignItems:"center", gap:10, marginBottom:5 }}>
                    <span style={{ fontSize:12, color:C.text, width:120, overflow:"hidden",
                      textOverflow:"ellipsis", whiteSpace:"nowrap",
                      fontFamily:"'DM Mono',monospace" }}>{val}</span>
                    <div style={{ flex:1, height:4, borderRadius:99, background:"rgba(255,255,255,0.06)" }}>
                      <div style={{ height:"100%", borderRadius:99,
                        background:"rgba(147,197,253,0.5)", width:`${pct}%` }}/>
                    </div>
                    <span style={{ fontSize:11, color:C.subtle, minWidth:55, textAlign:"right",
                      fontFamily:"'DM Mono',monospace" }}>{cnt} ({pct.toFixed(0)}%)</span>
                  </div>
                );
              })}
            </div>
          )}
          <SectionHeader>AI Recommendations</SectionHeader>
          {col.recommendations.map((r,i) => {
            const rm = REC_META[r.level];
            return (
              <div key={i} style={{ display:"flex", alignItems:"center", gap:8, marginBottom:5 }}>
                <span style={{ color:rm.color, fontSize:13 }}>{rm.icon}</span>
                <span style={{ fontSize:12, color:C.subtle }}>{r.text}</span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

function UploadZone({ onData }) {
  const [drag, setDrag]       = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError]     = useState(null);
  const [progress, setProgress] = useState(0);
  const inputRef = useRef();

  const parseFile = useCallback((file) => {
    setError(null); setLoading(true); setProgress(0);
    const ext = file.name.split(".").pop().toLowerCase();
    const ticker = setInterval(() => setProgress(p => Math.min(p+8, 85)), 80);

    const finish = (data) => {
      clearInterval(ticker); setProgress(100);
      const profile = profileDataset(data);
      setTimeout(() => { setLoading(false); onData({ rows:data, profile, fileName:file.name, fileSize:file.size }); }, 300);
    };
    const fail = (msg) => { clearInterval(ticker); setError(msg); setLoading(false); };

    if (ext === "csv") {
      Papa.parse(file, { header:true, skipEmptyLines:true, dynamicTyping:false,
        complete: ({data, errors}) => {
          if (errors.length && !data.length) return fail("Failed to parse CSV.");
          finish(data);
        }, error: e => fail(e.message) });
    } else if (["xlsx","xls"].includes(ext)) {
      const r = new FileReader();
      r.onload = e => {
        try {
          const wb = XLSX.read(e.target.result, {type:"array"});
          finish(XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]], {defval:""}));
        } catch { fail("Failed to parse Excel file."); }
      };
      r.readAsArrayBuffer(file);
    } else if (ext === "json") {
      const r = new FileReader();
      r.onload = e => {
        try {
          let data = JSON.parse(e.target.result);
          if (!Array.isArray(data)) data = [data];
          finish(data);
        } catch { fail("Invalid JSON. Expected array of objects."); }
      };
      r.readAsText(file);
    } else { fail(`Unsupported: .${ext}. Use CSV, Excel, or JSON.`); }
  }, [onData]);

  return (
    <div onDragOver={e=>{e.preventDefault();setDrag(true);}}
         onDragLeave={()=>setDrag(false)} onDrop={e=>{e.preventDefault();setDrag(false);parseFile(e.dataTransfer.files[0]);}}
         onClick={()=>!loading&&inputRef.current?.click()}
         style={{ border:`2px dashed ${drag?C.green:"rgba(255,255,255,0.09)"}`,
           borderRadius:20, padding:"60px 40px", textAlign:"center",
           cursor:loading?"default":"pointer",
           background:drag?C.accentG:C.surface, transition:"all 0.25s" }}>
      <input ref={inputRef} type="file" accept=".csv,.xlsx,.xls,.json" onChange={e=>parseFile(e.target.files[0])} style={{display:"none"}}/>
      {loading ? (
        <div>
          <div style={{fontSize:40,marginBottom:16}}>⚙️</div>
          <p style={{color:C.text,fontSize:16,marginBottom:16}}>Profiling dataset…</p>
          <div style={{width:300,margin:"0 auto",height:4,borderRadius:99,background:"rgba(255,255,255,0.08)"}}>
            <div style={{height:"100%",borderRadius:99,
              background:"linear-gradient(90deg,#6EE7B7,#93C5FD)",
              width:`${progress}%`,transition:"width 0.2s"}}/>
          </div>
          <p style={{color:C.muted,fontSize:12,marginTop:8,fontFamily:"'DM Mono',monospace"}}>{progress}%</p>
        </div>
      ) : (
        <div>
          <div style={{fontSize:52,marginBottom:20,filter:"drop-shadow(0 0 20px rgba(110,231,183,0.3))"}}>📊</div>
          <p style={{color:C.text,fontSize:18,fontWeight:600,marginBottom:8}}>Drop your dataset here</p>
          <p style={{color:C.muted,fontSize:14,marginBottom:24}}>CSV, Excel (.xlsx), or JSON</p>
          <div style={{display:"flex",gap:10,justifyContent:"center",flexWrap:"wrap"}}>
            {[".csv",".xlsx",".xls",".json"].map(ext=>(
              <span key={ext} style={{padding:"5px 14px",borderRadius:99,
                background:"rgba(255,255,255,0.04)",border:`1px solid ${C.border}`,
                color:C.subtle,fontSize:12,fontFamily:"'DM Mono',monospace"}}>{ext}</span>
            ))}
          </div>
          {error && (
            <div style={{marginTop:20,padding:"10px 16px",background:"rgba(252,165,165,0.08)",
              border:`1px solid rgba(252,165,165,0.2)`,borderRadius:8,color:C.red,fontSize:13}}>
              ⚠ {error}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function Module1({ dataset, onData, onReset }) {
  const [filterType, setFilterType] = useState("all");
  const [sortBy, setSortBy]         = useState("index");
  const [searchQ, setSearchQ]       = useState("");
  const [activeTab, setActiveTab]   = useState("overview");
  const p = dataset?.profile;

  const filteredCols = p?.columns
    ?.filter(c => filterType === "all" || c.type === filterType)
    ?.filter(c => !searchQ || c.name.toLowerCase().includes(searchQ.toLowerCase()))
    ?.sort((a,b) => sortBy==="missing" ? b.stats.missingPct-a.stats.missingPct
                  : sortBy==="name"    ? a.name.localeCompare(b.name) : 0) ?? [];

  const typeCounts = p?.columns?.reduce((acc,c)=>{ acc[c.type]=(acc[c.type]||0)+1; return acc; }, {}) ?? {};

  if (!dataset) return (
    <div>
      <UploadZone onData={onData}/>
      <div style={{textAlign:"center",marginTop:20}}>
        <button onClick={()=>{
          const rows = generateSampleData(300);
          onData({ rows, profile:profileDataset(rows), fileName:"sample_customers.csv", fileSize:42800 });
        }} style={{padding:"10px 24px",borderRadius:8,background:"rgba(99,179,237,0.08)",
          border:`1px solid rgba(99,179,237,0.2)`,color:C.blue,fontSize:14,cursor:"pointer"}}>
          ✦ Load Sample Dataset (300 rows)
        </button>
      </div>
    </div>
  );

  return (
    <div style={{animation:"fadeIn 0.4s ease"}}>
      {/* File bar */}
      <div style={{display:"flex",alignItems:"center",gap:16,padding:"11px 18px",
        borderRadius:12,background:C.accentG,border:`1px solid rgba(110,231,183,0.12)`,
        marginBottom:24,flexWrap:"wrap"}}>
        <span style={{fontSize:18}}>📁</span>
        <span style={{fontWeight:600,color:C.text,fontSize:14}}>{dataset.fileName}</span>
        <span style={{color:C.muted,fontSize:12,fontFamily:"'DM Mono',monospace"}}>
          {(dataset.fileSize/1024).toFixed(1)} KB</span>
        <div style={{height:16,width:1,background:C.border}}/>
        <span style={{color:C.subtle,fontSize:12}}>
          {fmtK(p.rowCount)} rows × {p.colCount} cols</span>
      </div>

      {/* Tabs */}
      <div style={{display:"flex",gap:4,marginBottom:24,
        borderBottom:`1px solid ${C.border}`,paddingBottom:0}}>
        {[["overview","Overview"],["columns","Columns"],["quality","Quality"]].map(([id,label])=>(
          <button key={id} onClick={()=>setActiveTab(id)} style={{
            padding:"9px 17px",borderRadius:"8px 8px 0 0",cursor:"pointer",
            background:activeTab===id?"rgba(255,255,255,0.05)":"transparent",
            border:activeTab===id?`1px solid ${C.border}`:"1px solid transparent",
            borderBottom:activeTab===id?`1px solid ${C.bg}`:"none",
            color:activeTab===id?C.text:C.muted,fontSize:13,
            fontWeight:activeTab===id?600:400,marginBottom:activeTab===id?-1:0}}>
            {label}
          </button>
        ))}
      </div>

      {/* Overview tab */}
      {activeTab === "overview" && (
        <div style={{animation:"fadeIn 0.3s ease"}}>
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(190px,1fr))",gap:14,marginBottom:24}}>
            {[
              {label:"Total Rows",    value:fmtK(p.rowCount),   icon:"▦",  color:C.blue},
              {label:"Total Columns", value:p.colCount,          icon:"▥",  color:C.green},
              {label:"Duplicates",    value:`${p.duplicates}`,   icon:"⟳",  color:p.duplicates>0?C.yellow:C.green},
              {label:"Quality Score", value:`${p.qualityScore}`, icon:"★",  color:p.qualityScore>=80?C.green:p.qualityScore>=60?C.yellow:C.red},
            ].map(({label,value,icon,color})=>(
              <div key={label} style={{padding:"18px",borderRadius:12,
                background:C.surface,border:`1px solid ${C.border}`}}>
                <div style={{fontSize:22,marginBottom:8}}>{icon}</div>
                <div style={{fontSize:26,fontWeight:800,color,
                  fontFamily:"'DM Mono',monospace",marginBottom:4}}>{value}</div>
                <div style={{fontSize:11,color:C.muted,textTransform:"uppercase",
                  letterSpacing:"0.08em"}}>{label}</div>
              </div>
            ))}
          </div>
          <div style={{display:"grid",gridTemplateColumns:"1fr 180px",gap:18,marginBottom:24}}>
            <div style={{padding:"18px",borderRadius:12,background:C.surface,border:`1px solid ${C.border}`}}>
              <SectionHeader>Column Type Breakdown</SectionHeader>
              {Object.entries(typeCounts).map(([type,count])=>{
                const m = TYPE_META[type]||TYPE_META.categorical;
                const pct = (count/p.colCount)*100;
                return (
                  <div key={type} style={{display:"flex",alignItems:"center",gap:12,marginBottom:10}}>
                    <span style={{fontSize:12,color:m.color,background:m.bg,borderRadius:5,
                      padding:"2px 8px",minWidth:88,textAlign:"center"}}>{m.label}</span>
                    <div style={{flex:1,height:6,borderRadius:99,background:"rgba(255,255,255,0.05)"}}>
                      <div style={{height:"100%",borderRadius:99,
                        background:m.color,width:`${pct}%`,opacity:0.65}}/>
                    </div>
                    <span style={{fontSize:12,color:C.subtle,fontFamily:"'DM Mono',monospace",minWidth:20}}>{count}</span>
                  </div>
                );
              })}
            </div>
            <div style={{padding:"18px",borderRadius:12,background:C.surface,
              border:`1px solid ${C.border}`,display:"flex",flexDirection:"column",
              alignItems:"center",justifyContent:"center",gap:8}}>
              <SectionHeader>Data Quality</SectionHeader>
              <QualityBadge score={p.qualityScore}/>
            </div>
          </div>
          <div style={{padding:"18px",borderRadius:12,background:C.surface,border:`1px solid ${C.border}`}}>
            <SectionHeader>Missing Values by Column</SectionHeader>
            <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(200px,1fr))",gap:12}}>
              {[...p.columns].sort((a,b)=>b.stats.missingPct-a.stats.missingPct).map(col=>(
                <div key={col.name}>
                  <span style={{fontSize:11,color:C.subtle,fontFamily:"'DM Mono',monospace",
                    display:"block",marginBottom:4,overflow:"hidden",
                    textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{col.name}</span>
                  <MissingBar pct={col.stats.missingPct}/>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Columns tab */}
      {activeTab === "columns" && (
        <div style={{animation:"fadeIn 0.3s ease"}}>
          <div style={{display:"flex",gap:10,marginBottom:18,flexWrap:"wrap"}}>
            <input value={searchQ} onChange={e=>setSearchQ(e.target.value)}
              placeholder="Search columns…"
              style={{flex:1,minWidth:170,padding:"8px 13px",borderRadius:8,
                background:"rgba(255,255,255,0.04)",border:`1px solid ${C.border}`,
                color:C.text,fontSize:13,fontFamily:"'DM Mono',monospace",outline:"none"}}/>
            <select value={filterType} onChange={e=>setFilterType(e.target.value)}
              style={{padding:"8px 13px",borderRadius:8,background:"rgba(255,255,255,0.04)",
                border:`1px solid ${C.border}`,color:C.subtle,fontSize:13,cursor:"pointer",outline:"none"}}>
              <option value="all">All Types</option>
              <option value="numeric">Numeric</option>
              <option value="categorical">Categorical</option>
              <option value="datetime">Datetime</option>
              <option value="boolean">Boolean</option>
            </select>
            <select value={sortBy} onChange={e=>setSortBy(e.target.value)}
              style={{padding:"8px 13px",borderRadius:8,background:"rgba(255,255,255,0.04)",
                border:`1px solid ${C.border}`,color:C.subtle,fontSize:13,cursor:"pointer",outline:"none"}}>
              <option value="index">Original Order</option>
              <option value="missing">Most Missing</option>
              <option value="name">A → Z</option>
            </select>
          </div>
          <p style={{fontSize:12,color:C.muted,marginBottom:10}}>
            {filteredCols.length} column{filteredCols.length!==1?"s":""} — click to expand</p>
          <div style={{display:"flex",flexDirection:"column",gap:8}}>
            {filteredCols.map(col=><ColumnCard key={col.name} col={col}/>)}
          </div>
        </div>
      )}

      {/* Quality tab */}
      {activeTab === "quality" && (
        <div style={{animation:"fadeIn 0.3s ease"}}>
          {["danger","warning","info","good"].map(level=>{
            const cols = p.columns.filter(c=>c.recommendations.some(r=>r.level===level));
            if (!cols.length) return null;
            const rm = REC_META[level];
            const labels = {danger:"Critical Issues",warning:"Warnings",info:"Suggestions",good:"Clean Columns"};
            return (
              <div key={level} style={{marginBottom:22}}>
                <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:10}}>
                  <span style={{color:rm.color,fontSize:16}}>{rm.icon}</span>
                  <h3 style={{fontSize:14,fontWeight:600,color:rm.color}}>{labels[level]}</h3>
                  <Tag color={rm.color}>{cols.length}</Tag>
                </div>
                <div style={{display:"flex",flexDirection:"column",gap:5}}>
                  {cols.map(col=>(
                    <div key={col.name} style={{display:"flex",gap:14,padding:"11px 15px",
                      borderRadius:8,background:`${rm.color}08`,border:`1px solid ${rm.color}18`}}>
                      <span style={{fontSize:12,color:C.text,fontWeight:600,
                        fontFamily:"'DM Mono',monospace",minWidth:130}}>{col.name}</span>
                      <div style={{display:"flex",flexDirection:"column",gap:3}}>
                        {col.recommendations.filter(r=>r.level===level).map((r,i)=>(
                          <span key={i} style={{fontSize:12,color:C.subtle}}>{r.text}</span>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            );
          })}
          <div style={{padding:"18px",borderRadius:12,marginTop:6,
            background:C.accentG,border:`1px solid rgba(110,231,183,0.12)`}}>
            <p style={{fontSize:13,color:C.green,fontWeight:600,marginBottom:6}}>
              ✦ Ready for Module 2: AI Data Cleaning Engine
            </p>
            <p style={{fontSize:12,color:C.muted,lineHeight:1.6}}>
              Profile complete. Click "Run Cleaning →" in the nav to automatically apply
              all recommended fixes — imputation, outlier treatment, type correction, and deduplication.
            </p>
          </div>
        </div>
      )}
    </div>
  );
}

// ════════════════════════════════════════════════════════════════
// SECTION 5 — MODULE 2 COMPONENTS
// ════════════════════════════════════════════════════════════════

function StepTimeline({ log, runningStep }) {
  const steps = [1,2,3,4,5,6];
  return (
    <div style={{display:"flex",flexDirection:"column",gap:0}}>
      {steps.map((s,i) => {
        const meta = STEP_META[s];
        const stepLogs = log.filter(l => l.step === s);
        const done = stepLogs.length > 0;
        const running = runningStep === s;
        return (
          <div key={s} style={{display:"flex",gap:16,paddingBottom:i<5?20:0}}>
            {/* Spine */}
            <div style={{display:"flex",flexDirection:"column",alignItems:"center"}}>
              <div style={{
                width:36,height:36,borderRadius:"50%",flexShrink:0,
                display:"flex",alignItems:"center",justifyContent:"center",
                background: done ? `${meta.color}22` : running ? `${meta.color}14` : "rgba(255,255,255,0.04)",
                border:`2px solid ${done||running ? meta.color : "rgba(255,255,255,0.08)"}`,
                fontSize:16,transition:"all 0.3s",
                boxShadow: running ? `0 0 12px ${meta.color}55` : "none",
              }}>{done?"✓":running?"⟳":meta.icon}</div>
              {i < 5 && (
                <div style={{width:2,flex:1,marginTop:4,
                  background:done?`${meta.color}40`:"rgba(255,255,255,0.05)"}}/>
              )}
            </div>
            {/* Content */}
            <div style={{flex:1,paddingTop:4}}>
              <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:6}}>
                <span style={{fontSize:13,fontWeight:600,
                  color:done||running?meta.color:C.muted}}>{meta.label}</span>
                {running && (
                  <span style={{fontSize:11,color:meta.color,animation:"pulse 1s infinite"}}>
                    running…
                  </span>
                )}
              </div>
              {stepLogs.map((l,i) => (
                <div key={i} style={{display:"flex",alignItems:"flex-start",gap:10,
                  padding:"8px 12px",borderRadius:8,marginBottom:5,
                  background:`${REC_META[l.severity]?.color||C.blue}08`,
                  border:`1px solid ${REC_META[l.severity]?.color||C.blue}18`}}>
                  <div style={{display:"flex",flexDirection:"column",flex:1,gap:2}}>
                    <div style={{display:"flex",alignItems:"center",gap:8}}>
                      <span style={{fontSize:11,fontWeight:700,color:REC_META[l.severity]?.color||C.blue,
                        fontFamily:"'DM Mono',monospace"}}>{l.action}</span>
                      {l.column !== "—" && l.column !== "ALL" && (
                        <Tag color={C.subtle}>{l.column}</Tag>
                      )}
                    </div>
                    <span style={{fontSize:12,color:C.subtle}}>{l.detail}</span>
                  </div>
                  {l.affected > 0 && (
                    <span style={{fontSize:12,fontWeight:700,
                      color:C.text,fontFamily:"'DM Mono',monospace",
                      flexShrink:0}}>{l.affected}</span>
                  )}
                </div>
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function DiffCard({ label, before, after, unit="", lowerBetter=true, icon }) {
  const improved = lowerBetter ? after < before : after > before;
  const delta = after - before;
  const pctChange = before !== 0 ? ((delta/before)*100).toFixed(1) : "—";
  const color = delta === 0 ? C.subtle : improved ? C.green : C.red;
  return (
    <div style={{padding:"16px",borderRadius:12,background:C.surface,
      border:`1px solid ${C.border}`,display:"flex",flexDirection:"column",gap:10}}>
      <div style={{display:"flex",alignItems:"center",gap:8}}>
        <span style={{fontSize:18}}>{icon}</span>
        <span style={{fontSize:12,color:C.muted,textTransform:"uppercase",
          letterSpacing:"0.08em"}}>{label}</span>
      </div>
      <div style={{display:"flex",alignItems:"flex-end",gap:12}}>
        <div>
          <div style={{fontSize:10,color:C.muted,marginBottom:2}}>BEFORE</div>
          <div style={{fontSize:20,fontWeight:700,color:C.subtle,
            fontFamily:"'DM Mono',monospace"}}>{before}{unit}</div>
        </div>
        <div style={{fontSize:18,color:C.muted,paddingBottom:2}}>→</div>
        <div>
          <div style={{fontSize:10,color:C.muted,marginBottom:2}}>AFTER</div>
          <div style={{fontSize:20,fontWeight:700,color,
            fontFamily:"'DM Mono',monospace"}}>{after}{unit}</div>
        </div>
      </div>
      {delta !== 0 && (
        <div style={{display:"flex",alignItems:"center",gap:6}}>
          <span style={{fontSize:11,color,fontFamily:"'DM Mono',monospace"}}>
            {delta > 0 ? "+" : ""}{typeof delta==="number"?delta.toFixed(delta%1?1:0):delta}{unit}
          </span>
          <span style={{fontSize:11,color:C.muted}}>({pctChange}%)</span>
        </div>
      )}
    </div>
  );
}

function Module2({ dataset, onCleaningDone }) {
  const [phase, setPhase]           = useState("ready"); // ready | running | done
  const [log, setLog]               = useState([]);
  const [runningStep, setRunningStep] = useState(null);
  const [cleanResult, setCleanResult] = useState(null);
  const [activeTab, setActiveTab]   = useState("log");

  const p = dataset?.profile;

  const runCleaning = useCallback(async () => {
    setPhase("running");
    setLog([]);
    setCleanResult(null);

    const engine = new CleaningEngine(dataset.rows, dataset.profile);

    // Animate step-by-step
    for (let step = 1; step <= 6; step++) {
      setRunningStep(step);
      await new Promise(r => setTimeout(r, 600));
      const method = [
        null,
        "step1_dropHighMissingCols",
        "step2_removeDuplicates",
        "step3_imputeMissing",
        "step4_treatOutliers",
        "step5_fixTypes",
        "step6_standardizeCategorical",
      ][step];
      engine[method]();
      setLog([...engine.log]);
      await new Promise(r => setTimeout(r, 400));
    }

    setRunningStep(null);
    const result = engine.getResult();
    setCleanResult(result);
    setPhase("done");
    onCleaningDone(result);
  }, [dataset, onCleaningDone]);

  if (!p) return <div style={{color:C.muted,textAlign:"center",padding:60}}>
    Complete Module 1 first.</div>;

  return (
    <div style={{animation:"fadeIn 0.4s ease"}}>
      {/* Header card */}
      <div style={{padding:"20px 24px",borderRadius:14,
        background:"linear-gradient(135deg,rgba(99,179,237,0.08),rgba(110,231,183,0.05))",
        border:`1px solid rgba(99,179,237,0.15)`,marginBottom:24}}>
        <div style={{display:"flex",alignItems:"flex-start",justifyContent:"space-between",
          flexWrap:"wrap",gap:16}}>
          <div>
            <h2 style={{fontSize:17,fontWeight:700,color:C.text,marginBottom:5}}>
              AI Data Cleaning Engine
            </h2>
            <p style={{fontSize:13,color:C.muted,lineHeight:1.6}}>
              Automatically applies 6 deterministic cleaning steps based on the profile from Module 1.
              Every transformation is logged for full auditability.
            </p>
          </div>
          <div style={{display:"flex",alignItems:"center",gap:12,flexWrap:"wrap"}}>
            {phase === "done" && (
              <button onClick={()=>downloadCSV(cleanResult.cleanedRows, "cleaned_"+dataset.fileName)}
                style={{padding:"9px 18px",borderRadius:8,background:"rgba(110,231,183,0.1)",
                  border:`1px solid rgba(110,231,183,0.25)`,color:C.green,fontSize:13,cursor:"pointer"}}>
                ↓ Download Cleaned CSV
              </button>
            )}
            <button
              onClick={phase==="ready"||phase==="done" ? runCleaning : undefined}
              disabled={phase==="running"}
              style={{
                padding:"10px 22px",borderRadius:8,fontSize:13,fontWeight:600,cursor:"pointer",
                background: phase==="running" ? "rgba(255,255,255,0.04)"
                  : "linear-gradient(135deg,#6EE7B7,#3B82F6)",
                color: phase==="running" ? C.muted : "#0F172A",
                border:"none", opacity:phase==="running"?0.7:1,
                transition:"all 0.2s",
              }}>
              {phase==="ready"  ? "▶ Run Cleaning Pipeline"
              :phase==="running" ? "⟳ Running…"
              :                   "↺ Re-run Pipeline"}
            </button>
          </div>
        </div>

        {/* Pipeline preview chips */}
        <div style={{display:"flex",gap:6,marginTop:16,flexWrap:"wrap"}}>
          {Object.entries(STEP_META).map(([s,m]) => {
            const done = log.some(l=>l.step===Number(s));
            const running = runningStep===Number(s);
            return (
              <div key={s} style={{display:"flex",alignItems:"center",gap:5,
                padding:"5px 11px",borderRadius:99,transition:"all 0.3s",
                background:done?`${m.color}18`:running?`${m.color}10`:"rgba(255,255,255,0.04)",
                border:`1px solid ${done||running?m.color:C.border}`,
                boxShadow:running?`0 0 8px ${m.color}44`:"none",
              }}>
                <span style={{fontSize:11,color:done||running?m.color:C.muted}}>{m.icon}</span>
                <span style={{fontSize:11,color:done||running?m.color:C.muted}}>{m.label}</span>
              </div>
            );
          })}
        </div>
      </div>

      {/* Tabs */}
      <div style={{display:"flex",gap:4,marginBottom:24,
        borderBottom:`1px solid ${C.border}`}}>
        {[
          ["log","Execution Log"],
          ["diff","Before vs After"],
          ...(phase==="done"?[["preview","Data Preview"]]:[]),
        ].map(([id,label])=>(
          <button key={id} onClick={()=>setActiveTab(id)} style={{
            padding:"9px 17px",borderRadius:"8px 8px 0 0",cursor:"pointer",
            background:activeTab===id?"rgba(255,255,255,0.05)":"transparent",
            border:activeTab===id?`1px solid ${C.border}`:"1px solid transparent",
            borderBottom:activeTab===id?`1px solid ${C.bg}`:"none",
            color:activeTab===id?C.text:C.muted,fontSize:13,
            fontWeight:activeTab===id?600:400,marginBottom:activeTab===id?-1:0}}>
            {label}
          </button>
        ))}
      </div>

      {/* LOG TAB */}
      {activeTab==="log" && (
        <div style={{animation:"fadeIn 0.3s ease"}}>
          {phase === "ready" && (
            <div style={{textAlign:"center",padding:"60px 0",color:C.muted}}>
              <div style={{fontSize:48,marginBottom:16,opacity:0.4}}>⚙️</div>
              <p style={{fontSize:14}}>Click <strong style={{color:C.text}}>Run Cleaning Pipeline</strong> to begin</p>
              <p style={{fontSize:12,marginTop:8,color:"#374151"}}>
                6 steps will execute automatically based on your dataset profile
              </p>
            </div>
          )}
          {phase !== "ready" && (
            <div style={{padding:"4px 0"}}>
              <StepTimeline log={log} runningStep={runningStep}/>
              {phase==="done" && (
                <div style={{marginTop:24,padding:"16px 20px",borderRadius:12,
                  background:C.accentG,border:`1px solid rgba(110,231,183,0.15)`}}>
                  <p style={{fontSize:13,color:C.green,fontWeight:600,marginBottom:4}}>
                    ✓ Cleaning pipeline complete
                  </p>
                  <p style={{fontSize:12,color:C.muted}}>
                    {cleanResult?.stats.cellsImputed} cells imputed · {" "}
                    {cleanResult?.stats.duplicatesRemoved} duplicates removed · {" "}
                    {cleanResult?.stats.outliersWinsorized} outliers winsorized · {" "}
                    {cleanResult?.stats.colsDropped} columns dropped
                  </p>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* DIFF TAB */}
      {activeTab==="diff" && (
        <div style={{animation:"fadeIn 0.3s ease"}}>
          {phase !== "done" ? (
            <div style={{textAlign:"center",padding:"60px 0",color:C.muted}}>
              <p>Run the pipeline first to see before/after comparison.</p>
            </div>
          ) : (
            <>
              <div style={{display:"grid",
                gridTemplateColumns:"repeat(auto-fill,minmax(200px,1fr))",gap:14,marginBottom:28}}>
                <DiffCard icon="▦" label="Row Count"
                  before={p.rowCount} after={cleanResult.afterProfile.rowCount}
                  lowerBetter={false}/>
                <DiffCard icon="★" label="Quality Score"
                  before={p.qualityScore} after={cleanResult.afterProfile.qualityScore}
                  lowerBetter={false}/>
                <DiffCard icon="⟳" label="Duplicates"
                  before={p.duplicates} after={cleanResult.afterProfile.duplicates}/>
                <DiffCard icon="◈" label="Cells Imputed"
                  before={0} after={cleanResult.stats.cellsImputed} lowerBetter={false}/>
                <DiffCard icon="⋈" label="Outliers Winsorized"
                  before={p.columns.reduce((a,c)=>a+(c.stats.outlierCount||0),0)}
                  after={cleanResult.afterProfile.columns.reduce((a,c)=>a+(c.stats.outlierCount||0),0)}/>
                <DiffCard icon="⊟" label="Columns Dropped"
                  before={p.colCount} after={cleanResult.afterProfile.colCount}/>
              </div>

              {/* Per-column missing comparison */}
              <div style={{padding:"18px",borderRadius:12,
                background:C.surface,border:`1px solid ${C.border}`}}>
                <SectionHeader>Missing Values — Before vs After</SectionHeader>
                <div style={{display:"flex",flexDirection:"column",gap:10}}>
                  {p.columns
                    .filter(c => c.stats.missingPct > 0)
                    .sort((a,b)=>b.stats.missingPct-a.stats.missingPct)
                    .map(col => {
                      const afterCol = cleanResult.afterProfile.columns.find(c=>c.name===col.name);
                      const afterMissing = afterCol?.stats.missingPct ?? null;
                      return (
                        <div key={col.name} style={{display:"grid",
                          gridTemplateColumns:"150px 1fr 1fr",alignItems:"center",gap:16}}>
                          <span style={{fontSize:12,color:C.text,fontFamily:"'DM Mono',monospace",
                            overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>
                            {col.name}</span>
                          <div>
                            <div style={{fontSize:10,color:C.muted,marginBottom:3}}>BEFORE</div>
                            <MissingBar pct={col.stats.missingPct}/>
                          </div>
                          <div>
                            <div style={{fontSize:10,color:C.muted,marginBottom:3}}>AFTER</div>
                            {afterMissing !== null
                              ? <MissingBar pct={afterMissing}/>
                              : <span style={{fontSize:11,color:C.red}}>Column dropped</span>
                            }
                          </div>
                        </div>
                      );
                    })}
                </div>
              </div>
            </>
          )}
        </div>
      )}

      {/* PREVIEW TAB */}
      {activeTab==="preview" && cleanResult && (
        <div style={{animation:"fadeIn 0.3s ease"}}>
          <div style={{overflowX:"auto",borderRadius:12,
            border:`1px solid ${C.border}`}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12,
              fontFamily:"'DM Mono',monospace"}}>
              <thead>
                <tr style={{background:"rgba(255,255,255,0.04)"}}>
                  {Object.keys(cleanResult.cleanedRows[0]||{}).map(col=>(
                    <th key={col} style={{padding:"10px 14px",textAlign:"left",
                      color:C.subtle,fontWeight:600,fontSize:11,
                      textTransform:"uppercase",letterSpacing:"0.06em",
                      borderBottom:`1px solid ${C.border}`,whiteSpace:"nowrap"}}>
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {cleanResult.cleanedRows.slice(0,20).map((row,i)=>(
                  <tr key={i} style={{borderBottom:`1px solid rgba(255,255,255,0.03)`,
                    background:i%2===0?"transparent":"rgba(255,255,255,0.015)"}}>
                    {Object.values(row).map((val,j)=>(
                      <td key={j} style={{padding:"8px 14px",color:C.subtle,
                        whiteSpace:"nowrap",maxWidth:160,overflow:"hidden",
                        textOverflow:"ellipsis"}}>
                        {val===null||val===undefined||val===""
                          ? <span style={{color:C.red,opacity:0.5}}>null</span>
                          : String(val)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p style={{fontSize:11,color:C.muted,marginTop:10}}>
            Showing first 20 of {fmtK(cleanResult.cleanedRows.length)} cleaned rows
          </p>
        </div>
      )}
    </div>
  );
}

// ════════════════════════════════════════════════════════════════
// SECTION 6 — MODULE 3 ENGINE: AUTOMATED EDA
// ════════════════════════════════════════════════════════════════

/**
 * EDA Engine — pure JS, zero external chart libs
 * All charts rendered as SVG with real computed coordinates
 */

// ── Pearson correlation for two numeric arrays ────────────────
function pearsonR(xs, ys) {
  const n = Math.min(xs.length, ys.length);
  if (n < 3) return 0;
  const meanX = xs.slice(0,n).reduce((a,b)=>a+b,0)/n;
  const meanY = ys.slice(0,n).reduce((a,b)=>a+b,0)/n;
  let num=0, dx2=0, dy2=0;
  for (let i=0;i<n;i++) {
    const dx=xs[i]-meanX, dy=ys[i]-meanY;
    num+=dx*dy; dx2+=dx*dx; dy2+=dy*dy;
  }
  const denom = Math.sqrt(dx2*dy2);
  return denom===0 ? 0 : +(num/denom).toFixed(4);
}

// ── Build full correlation matrix for numeric cols ────────────
function buildCorrelationMatrix(rows, profile) {
  const numCols = profile.columns.filter(c=>c.type==="numeric").map(c=>c.name);
  const vectors = {};
  numCols.forEach(col => {
    vectors[col] = rows.map(r=>Number(r[col])).filter(n=>!isNaN(n));
  });
  const matrix = {};
  numCols.forEach(a => {
    matrix[a] = {};
    numCols.forEach(b => {
      matrix[a][b] = a===b ? 1 : pearsonR(vectors[a], vectors[b]);
    });
  });
  return { matrix, cols: numCols };
}

// ── Histogram buckets ─────────────────────────────────────────
function buildHistogram(values, bins=20) {
  const nums = values.map(Number).filter(n=>!isNaN(n));
  if (!nums.length) return [];
  const min=Math.min(...nums), max=Math.max(...nums);
  const size=(max-min)/bins||1;
  const buckets=Array(bins).fill(0).map((_,i)=>({ x0:min+i*size, x1:min+(i+1)*size, count:0 }));
  nums.forEach(v=>{ const i=Math.min(Math.floor((v-min)/size),bins-1); buckets[i].count++; });
  return buckets;
}

// ── KDE curve for histogram overlay ──────────────────────────
function buildKDE(values, bandwidth, xPoints) {
  const nums = values.map(Number).filter(n=>!isNaN(n));
  const n = nums.length;
  if (!n) return [];
  const bw = bandwidth || (1.06*Math.sqrt(nums.reduce((a,b)=>a+Math.pow(b-nums.reduce((x,y)=>x+y,0)/n,2),0)/n)*Math.pow(n,-0.2));
  return xPoints.map(x => ({
    x,
    y: nums.reduce((sum,xi)=>sum+Math.exp(-0.5*Math.pow((x-xi)/bw,2)),0)/(n*bw*Math.sqrt(2*Math.PI))
  }));
}

// ── Auto-generate EDA insights ────────────────────────────────
function generateEDAInsights(rows, profile, corrMatrix) {
  const insights = [];
  const numCols = profile.columns.filter(c=>c.type==="numeric");
  const catCols = profile.columns.filter(c=>c.type==="categorical");

  // Top correlations
  if (corrMatrix) {
    const pairs=[];
    corrMatrix.cols.forEach((a,i)=>corrMatrix.cols.forEach((b,j)=>{
      if(j>i) pairs.push({a,b,r:corrMatrix.matrix[a][b]});
    }));
    pairs.sort((x,y)=>Math.abs(y.r)-Math.abs(x.r));
    pairs.slice(0,3).forEach(({a,b,r})=>{
      const strength=Math.abs(r)>0.8?"strongly":Math.abs(r)>0.5?"moderately":"weakly";
      const dir=r>0?"positively":"negatively";
      insights.push({
        icon:"⟷", color:Math.abs(r)>0.7?C.green:C.blue,
        title:"Correlation Detected",
        text:`${a} and ${b} are ${strength} ${dir} correlated (r = ${r.toFixed(2)}).`,
        type:"correlation"
      });
    });
  }

  // Most skewed column
  const skewed = numCols.filter(c=>c.stats.skewLabel!=="Normal")
    .sort((a,b)=>Math.abs(b.stats.skewness)-Math.abs(a.stats.skewness));
  if (skewed.length)
    insights.push({
      icon:"⟿", color:C.yellow, title:"Distribution Skew",
      text:`${skewed[0].name} is ${skewed[0].stats.skewLabel} (skewness = ${skewed[0].stats.skewness.toFixed(2)}). Consider log transformation before modeling.`,
      type:"distribution"
    });

  // Most outliers
  const outlierCol = [...numCols].sort((a,b)=>(b.stats.outlierCount||0)-(a.stats.outlierCount||0))[0];
  if (outlierCol?.stats.outlierCount > 0)
    insights.push({
      icon:"◉", color:C.orange, title:"Outlier Concentration",
      text:`${outlierCol.name} has the most outliers (${outlierCol.stats.outlierCount} values beyond IQR bounds). These were winsorized in Module 2.`,
      type:"outlier"
    });

  // Dominant category
  catCols.slice(0,2).forEach(col=>{
    if (!col.stats.topValues?.length) return;
    const [topVal, topCnt] = col.stats.topValues[0];
    const pct = ((topCnt/col.stats.totalCategorical)*100).toFixed(0);
    insights.push({
      icon:"▦", color:C.purple, title:"Category Distribution",
      text:`In ${col.name}, "${topVal}" is dominant at ${pct}% of all values.`,
      type:"category"
    });
  });

  // High/low value column
  if (numCols.length >= 2) {
    const sorted=[...numCols].sort((a,b)=>(b.stats.mean||0)-(a.stats.mean||0));
    insights.push({
      icon:"↑", color:C.green, title:"Value Range",
      text:`${sorted[0].name} has the highest mean (${sorted[0].stats.mean?.toFixed(2)}), while ${sorted[sorted.length-1].name} is the lowest (${sorted[sorted.length-1].stats.mean?.toFixed(2)}).`,
      type:"range"
    });
  }

  return insights;
}

// ════════════════════════════════════════════════════════════════
// SECTION 6b — MODULE 3 SVG CHART COMPONENTS
// ════════════════════════════════════════════════════════════════

const CHART_W = 560, CHART_H = 320;
const PAD = { top:24, right:24, bottom:52, left:60 };
const INNER_W = CHART_W - PAD.left - PAD.right;
const INNER_H = CHART_H - PAD.top - PAD.bottom;

function AxisLabel({ x, y, text, anchor="middle", fontSize=10, color="#64748B" }) {
  return <text x={x} y={y} textAnchor={anchor} fontSize={fontSize}
    fill={color} fontFamily="'DM Mono',monospace">{text}</text>;
}

function GridLines({ xTicks=5, yTicks=5 }) {
  return (
    <g opacity={0.12}>
      {Array(yTicks).fill(0).map((_,i)=>{
        const y = (i/(yTicks-1))*INNER_H;
        return <line key={i} x1={0} y1={y} x2={INNER_W} y2={y} stroke="#fff" strokeWidth={0.8}/>;
      })}
      {Array(xTicks).fill(0).map((_,i)=>{
        const x = (i/(xTicks-1))*INNER_W;
        return <line key={i} x1={x} y1={0} x2={x} y2={INNER_H} stroke="#fff" strokeWidth={0.8}/>;
      })}
    </g>
  );
}

// ── Histogram Chart ───────────────────────────────────────────
function HistogramChart({ values, label }) {
  const [hovered, setHovered] = useState(null);
  const bins = buildHistogram(values, 18);
  if (!bins.length) return <EmptyChart msg="No numeric data"/>;
  const maxCount = Math.max(...bins.map(b=>b.count));
  const scaleX = v => ((v - bins[0].x0) / (bins[bins.length-1].x1 - bins[0].x0)) * INNER_W;
  const scaleY = v => INNER_H - (v/maxCount)*INNER_H;
  const barW = INNER_W / bins.length - 1;

  // KDE overlay
  const kdeXs = Array(60).fill(0).map((_,i)=>bins[0].x0 + (i/59)*(bins[bins.length-1].x1-bins[0].x0));
  const kdeRaw = buildKDE(values, null, kdeXs);
  const kdeMax = Math.max(...kdeRaw.map(p=>p.y),0.001);
  const kdePath = kdeRaw.map((p,i)=>{
    const x = ((p.x-bins[0].x0)/(bins[bins.length-1].x1-bins[0].x0))*INNER_W;
    const y = INNER_H - (p.y/kdeMax)*INNER_H*0.9;
    return `${i===0?"M":"L"}${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");

  const xTicks = [0,0.25,0.5,0.75,1].map(t=>({
    x: t*INNER_W,
    label: (bins[0].x0 + t*(bins[bins.length-1].x1-bins[0].x0)).toFixed(0)
  }));

  return (
    <svg width="100%" viewBox={`0 0 ${CHART_W} ${CHART_H}`}>
      <g transform={`translate(${PAD.left},${PAD.top})`}>
        <GridLines yTicks={4} xTicks={5}/>
        {bins.map((b,i)=>(
          <g key={i} onMouseEnter={()=>setHovered(i)} onMouseLeave={()=>setHovered(null)}>
            <rect x={scaleX(b.x0)+0.5} y={scaleY(b.count)}
              width={Math.max(barW,1)} height={INNER_H-scaleY(b.count)}
              fill={hovered===i?"#93C5FD":"rgba(147,197,253,0.45)"}
              rx={2} style={{transition:"fill 0.15s"}}/>
            {hovered===i && (
              <rect x={scaleX(b.x0)-4} y={scaleY(b.count)-28} width={80} height={22}
                fill="rgba(15,23,42,0.95)" rx={4}/>
            )}
            {hovered===i && (
              <text x={scaleX(b.x0)+36} y={scaleY(b.count)-13}
                textAnchor="middle" fontSize={10} fill="#E2E8F0" fontFamily="'DM Mono',monospace">
                {b.x0.toFixed(1)}–{b.x1.toFixed(1)}: {b.count}
              </text>
            )}
          </g>
        ))}
        <path d={kdePath} fill="none" stroke="#6EE7B7" strokeWidth={2} opacity={0.8}
          strokeLinecap="round"/>
        {xTicks.map((t,i)=>(
          <AxisLabel key={i} x={t.x} y={INNER_H+16} text={t.label}/>
        ))}
        {[0,0.5,1].map((t,i)=>(
          <AxisLabel key={i} x={-8} y={INNER_H-(t*INNER_H)+4}
            text={Math.round(t*maxCount)} anchor="end"/>
        ))}
        <line x1={0} y1={INNER_H} x2={INNER_W} y2={INNER_H} stroke="rgba(255,255,255,0.12)" strokeWidth={1}/>
        <line x1={0} y1={0} x2={0} y2={INNER_H} stroke="rgba(255,255,255,0.12)" strokeWidth={1}/>
      </g>
      <text x={CHART_W/2} y={CHART_H-6} textAnchor="middle" fontSize={11}
        fill="#64748B" fontFamily="'DM Sans',sans-serif">{label}</text>
    </svg>
  );
}

// ── Box Plot Chart ────────────────────────────────────────────
function BoxPlotChart({ colData }) {
  // colData: array of { name, stats }
  if (!colData.length) return <EmptyChart msg="No numeric columns"/>;
  const allVals = colData.flatMap(c=>[c.stats.min,c.stats.max]).filter(v=>v!=null);
  const globalMin=Math.min(...allVals), globalMax=Math.max(...allVals);
  const range=globalMax-globalMin||1;
  const scaleY=v=>PAD.top + (1-(v-globalMin)/range)*INNER_H;
  const slotW = INNER_W/colData.length;

  const yTicks=[0,0.25,0.5,0.75,1].map(t=>({
    y:scaleY(globalMin+t*range), label:(globalMin+t*range).toFixed(0)
  }));

  return (
    <svg width="100%" viewBox={`0 0 ${CHART_W} ${CHART_H}`}>
      <g>
        {yTicks.map((t,i)=>(
          <g key={i}>
            <line x1={PAD.left} y1={t.y} x2={PAD.left+INNER_W} y2={t.y}
              stroke="rgba(255,255,255,0.06)" strokeWidth={1}/>
            <AxisLabel x={PAD.left-6} y={t.y+4} text={t.label} anchor="end"/>
          </g>
        ))}
        {colData.map((col,i)=>{
          const {min,q1,median,q3,max,outlierCount}=col.stats;
          if(min==null) return null;
          const cx=PAD.left+slotW*i+slotW/2;
          const bw=Math.min(slotW*0.45,40);
          const y_min=scaleY(min), y_q1=scaleY(q1), y_med=scaleY(median),
                y_q3=scaleY(q3), y_max=scaleY(max);
          return (
            <g key={col.name}>
              {/* Whiskers */}
              <line x1={cx} y1={y_max} x2={cx} y2={y_q3} stroke="#93C5FD" strokeWidth={1.5} strokeDasharray="3,2"/>
              <line x1={cx} y1={y_q1} x2={cx} y2={y_min} stroke="#93C5FD" strokeWidth={1.5} strokeDasharray="3,2"/>
              <line x1={cx-bw*0.3} y1={y_max} x2={cx+bw*0.3} y2={y_max} stroke="#93C5FD" strokeWidth={1.5}/>
              <line x1={cx-bw*0.3} y1={y_min} x2={cx+bw*0.3} y2={y_min} stroke="#93C5FD" strokeWidth={1.5}/>
              {/* IQR box */}
              <rect x={cx-bw/2} y={y_q3} width={bw} height={y_q1-y_q3}
                fill="rgba(147,197,253,0.15)" stroke="#93C5FD" strokeWidth={1.5} rx={3}/>
              {/* Median line */}
              <line x1={cx-bw/2} y1={y_med} x2={cx+bw/2} y2={y_med}
                stroke="#6EE7B7" strokeWidth={2.5}/>
              {/* Outlier indicator */}
              {outlierCount>0 && (
                <circle cx={cx+bw/2+6} cy={y_q3-6} r={3} fill="#FCA5A5" opacity={0.8}/>
              )}
              <AxisLabel x={cx} y={CHART_H-6} text={col.name.length>10?col.name.slice(0,10)+"…":col.name}/>
            </g>
          );
        })}
        <line x1={PAD.left} y1={PAD.top} x2={PAD.left} y2={PAD.top+INNER_H}
          stroke="rgba(255,255,255,0.1)" strokeWidth={1}/>
      </g>
    </svg>
  );
}

// ── Bar Chart ─────────────────────────────────────────────────
function BarChart({ topValues, label, totalCount }) {
  const [hovered, setHovered] = useState(null);
  if (!topValues?.length) return <EmptyChart msg="No categorical data"/>;
  const maxVal = topValues[0][1];
  const scaleW = v=>(v/maxVal)*INNER_W;
  const rowH = Math.min(36, INNER_H/topValues.length - 4);
  const colors = ["#93C5FD","#6EE7B7","#C4B5FD","#FCD34D","#FDBA74"];

  return (
    <svg width="100%" viewBox={`0 0 ${CHART_W} ${Math.max(CHART_H, topValues.length*44+60)}`}>
      <g transform={`translate(${PAD.left},${PAD.top})`}>
        {topValues.map(([val,cnt],i)=>{
          const y = i*(rowH+8);
          const w = scaleW(cnt);
          const pct = ((cnt/totalCount)*100).toFixed(1);
          return (
            <g key={i} onMouseEnter={()=>setHovered(i)} onMouseLeave={()=>setHovered(null)}>
              <text x={-6} y={y+rowH/2+4} textAnchor="end" fontSize={11}
                fill={hovered===i?"#E2E8F0":"#94A3B8"} fontFamily="'DM Mono',monospace">
                {String(val).length>14?String(val).slice(0,14)+"…":String(val)}
              </text>
              <rect x={0} y={y} width={Math.max(w,2)} height={rowH}
                fill={hovered===i?colors[i%colors.length]:`${colors[i%colors.length]}80`}
                rx={4} style={{transition:"all 0.2s"}}/>
              <text x={Math.max(w,2)+6} y={y+rowH/2+4} fontSize={11}
                fill="#94A3B8" fontFamily="'DM Mono',monospace">
                {cnt} ({pct}%)
              </text>
            </g>
          );
        })}
      </g>
      <text x={CHART_W/2} y={topValues.length*44+50} textAnchor="middle" fontSize={11}
        fill="#64748B" fontFamily="'DM Sans',sans-serif">{label}</text>
    </svg>
  );
}

// ── Scatter Plot ──────────────────────────────────────────────
function ScatterPlot({ rows, xCol, yCol }) {
  const [hovered, setHovered] = useState(null);
  const pts = rows
    .map(r=>({x:Number(r[xCol]),y:Number(r[yCol])}))
    .filter(p=>!isNaN(p.x)&&!isNaN(p.y));
  if (pts.length < 3) return <EmptyChart msg="Need ≥ 3 data points"/>;
  const xs=pts.map(p=>p.x), ys=pts.map(p=>p.y);
  const xMin=Math.min(...xs),xMax=Math.max(...xs),yMin=Math.min(...ys),yMax=Math.max(...ys);
  const xRange=xMax-xMin||1, yRange=yMax-yMin||1;
  const sx=v=>((v-xMin)/xRange)*INNER_W;
  const sy=v=>INNER_H-((v-yMin)/yRange)*INNER_H;

  // Trend line (OLS)
  const n=pts.length;
  const meanX=xs.reduce((a,b)=>a+b,0)/n, meanY=ys.reduce((a,b)=>a+b,0)/n;
  let num=0,den=0;
  pts.forEach(p=>{num+=(p.x-meanX)*(p.y-meanY);den+=(p.x-meanX)**2;});
  const slope=den?num/den:0, intercept=meanY-slope*meanX;
  const trendY1=slope*xMin+intercept, trendY2=slope*xMax+intercept;
  const r=pearsonR(xs,ys);

  // Sample for performance
  const sample = pts.length>400 ? pts.filter((_,i)=>i%Math.ceil(pts.length/400)===0) : pts;

  return (
    <svg width="100%" viewBox={`0 0 ${CHART_W} ${CHART_H}`}>
      <g transform={`translate(${PAD.left},${PAD.top})`}>
        <GridLines/>
        {sample.map((p,i)=>(
          <circle key={i} cx={sx(p.x)} cy={sy(p.y)} r={3}
            fill="rgba(147,197,253,0.55)" stroke="none"
            onMouseEnter={()=>setHovered(i)} onMouseLeave={()=>setHovered(null)}/>
        ))}
        <line x1={sx(xMin)} y1={sy(trendY1)} x2={sx(xMax)} y2={sy(trendY2)}
          stroke="#6EE7B7" strokeWidth={2} opacity={0.8} strokeDasharray="5,3"/>
        <text x={INNER_W-4} y={12} textAnchor="end" fontSize={11}
          fill={Math.abs(r)>0.7?C.green:Math.abs(r)>0.4?C.yellow:C.red}
          fontFamily="'DM Mono',monospace">r = {r.toFixed(2)}</text>
        <line x1={0} y1={INNER_H} x2={INNER_W} y2={INNER_H} stroke="rgba(255,255,255,0.1)"/>
        <line x1={0} y1={0} x2={0} y2={INNER_H} stroke="rgba(255,255,255,0.1)"/>
        {[0,0.5,1].map((t,i)=>(
          <AxisLabel key={i} x={t*INNER_W} y={INNER_H+16}
            text={(xMin+t*xRange).toFixed(0)}/>
        ))}
        {[0,0.5,1].map((t,i)=>(
          <AxisLabel key={i} x={-8} y={INNER_H-(t*INNER_H)+4}
            text={(yMin+t*yRange).toFixed(0)} anchor="end"/>
        ))}
      </g>
      <text x={CHART_W/2} y={CHART_H-6} textAnchor="middle" fontSize={11}
        fill="#64748B" fontFamily="'DM Sans',sans-serif">{xCol} vs {yCol}</text>
    </svg>
  );
}

// ── Correlation Heatmap ───────────────────────────────────────
function CorrelationHeatmap({ corrMatrix }) {
  const [hovered, setHovered] = useState(null);
  const { matrix, cols } = corrMatrix;
  if (!cols.length) return <EmptyChart msg="No numeric columns for correlation"/>;
  const n = cols.length;
  const cell = Math.min(Math.floor(480/n), 64);
  const W = n*cell+120, H = n*cell+80;

  const rToColor = r => {
    const abs = Math.abs(r);
    if (r > 0.7)  return `rgba(110,231,183,${0.3+abs*0.7})`;
    if (r > 0)    return `rgba(147,197,253,${0.1+abs*0.8})`;
    if (r > -0.7) return `rgba(253,186,116,${0.1+abs*0.8})`;
    return `rgba(252,165,165,${0.3+abs*0.7})`;
  };

  return (
    <svg width="100%" viewBox={`0 0 ${W} ${H}`}>
      <g transform="translate(80,40)">
        {cols.map((col,i)=>(
          <text key={i} x={i*cell+cell/2} y={-6} textAnchor="middle"
            fontSize={Math.min(10,cell*0.18)} fill="#94A3B8"
            fontFamily="'DM Mono',monospace" transform={`rotate(-35,${i*cell+cell/2},-6)`}>
            {col.length>8?col.slice(0,8)+"…":col}
          </text>
        ))}
        {cols.map((row,i)=>(
          <text key={i} x={-6} y={i*cell+cell/2+4} textAnchor="end"
            fontSize={Math.min(10,cell*0.18)} fill="#94A3B8" fontFamily="'DM Mono',monospace">
            {row.length>8?row.slice(0,8)+"…":row}
          </text>
        ))}
        {cols.map((row,i)=>cols.map((col,j)=>{
          const r=matrix[row][col];
          const key=`${i}-${j}`;
          const isHov=hovered===key;
          return (
            <g key={key} onMouseEnter={()=>setHovered(key)} onMouseLeave={()=>setHovered(null)}>
              <rect x={j*cell} y={i*cell} width={cell-1} height={cell-1}
                fill={rToColor(r)} rx={2}
                stroke={isHov?"rgba(255,255,255,0.4)":"none"} strokeWidth={1.5}/>
              {cell>28 && (
                <text x={j*cell+cell/2} y={i*cell+cell/2+4} textAnchor="middle"
                  fontSize={Math.min(10,cell*0.2)} fill={Math.abs(r)>0.5?"#0F172A":"#E2E8F0"}
                  fontFamily="'DM Mono',monospace">
                  {r===1?"1.0":r.toFixed(2)}
                </text>
              )}
            </g>
          );
        }))}
      </g>
      {/* Legend */}
      <g transform={`translate(${W-30},${H/2-60})`}>
        {[-1,-0.5,0,0.5,1].map((v,i)=>(
          <g key={i}>
            <rect x={0} y={i*24} width={16} height={22} fill={rToColor(v)} rx={2}/>
            <text x={20} y={i*24+14} fontSize={9} fill="#64748B"
              fontFamily="'DM Mono',monospace">{v.toFixed(1)}</text>
          </g>
        ))}
      </g>
    </svg>
  );
}

function EmptyChart({ msg }) {
  return (
    <div style={{display:"flex",alignItems:"center",justifyContent:"center",
      height:240,color:C.muted,fontSize:13}}>
      {msg}
    </div>
  );
}

// ════════════════════════════════════════════════════════════════
// SECTION 6c — MODULE 3 MAIN COMPONENT
// ════════════════════════════════════════════════════════════════

function Module3({ dataset, cleanResult }) {
  const rows    = cleanResult?.cleanedRows    ?? dataset?.rows ?? [];
  const profile = cleanResult?.afterProfile   ?? dataset?.profile;

  const [activeTab,   setActiveTab]   = useState("charts");
  const [chartType,   setChartType]   = useState("histogram");
  const [selCol,      setSelCol]      = useState(null);
  const [scatterX,    setScatterX]    = useState(null);
  const [scatterY,    setScatterY]    = useState(null);
  const [corrMatrix,  setCorrMatrix]  = useState(null);
  const [insights,    setInsights]    = useState([]);
  const [computed,    setComputed]    = useState(false);

  // On mount — compute correlation matrix + insights
  useEffect(()=>{
    if (!profile || computed) return;
    const cm = buildCorrelationMatrix(rows, profile);
    setCorrMatrix(cm);
    setInsights(generateEDAInsights(rows, profile, cm));
    // Default column selections
    const numCols = profile.columns.filter(c=>c.type==="numeric");
    const catCols = profile.columns.filter(c=>c.type==="categorical");
    if (numCols[0]) setSelCol(numCols[0].name);
    if (numCols[0]) setScatterX(numCols[0].name);
    if (numCols[1]) setScatterY(numCols[1].name);
    else if (numCols[0]) setScatterY(numCols[0].name);
    setComputed(true);
  }, [rows, profile, computed]);

  if (!profile) return (
    <div style={{textAlign:"center",padding:"80px 0",color:C.muted}}>
      Complete Module 1 + 2 first.
    </div>
  );

  const numCols = profile.columns.filter(c=>c.type==="numeric");
  const catCols = profile.columns.filter(c=>c.type==="categorical");
  const activeColMeta = profile.columns.find(c=>c.name===selCol);
  const activeValues  = rows.map(r=>r[selCol]).filter(v=>v!==null&&v!==undefined&&v!=="");

  const CHART_TYPES = [
    { id:"histogram", label:"Distribution",  icon:"▪",  requires:"numeric"     },
    { id:"boxplot",   label:"Box Plot",       icon:"▥",  requires:"numeric"     },
    { id:"bar",       label:"Bar Chart",      icon:"▬",  requires:"categorical" },
    { id:"scatter",   label:"Scatter Plot",   icon:"⁚",  requires:"twoNumeric"  },
  ];

  function renderChart() {
    switch(chartType) {
      case "histogram":
        if (!selCol || !activeColMeta || activeColMeta.type!=="numeric")
          return <EmptyChart msg="Select a numeric column"/>;
        return <HistogramChart values={activeValues.map(Number)} label={selCol}/>;

      case "boxplot":
        return <BoxPlotChart colData={numCols.filter(c=>c.stats.min!=null)}/>;

      case "bar":
        if (!selCol || !activeColMeta || activeColMeta.type!=="categorical")
          return <EmptyChart msg="Select a categorical column"/>;
        return <BarChart topValues={activeColMeta.stats.topValues?.slice(0,8)}
          label={selCol} totalCount={activeColMeta.stats.totalCategorical}/>;

      case "scatter":
        if (!scatterX || !scatterY) return <EmptyChart msg="Select two numeric columns"/>;
        return <ScatterPlot rows={rows} xCol={scatterX} yCol={scatterY}/>;

      default: return <EmptyChart msg="Select a chart type"/>;
    }
  }

  return (
    <div style={{animation:"fadeIn 0.4s ease"}}>
      {/* Dataset info strip */}
      <div style={{display:"flex",alignItems:"center",gap:16,padding:"11px 18px",
        borderRadius:12,background:"rgba(196,181,253,0.06)",
        border:"1px solid rgba(196,181,253,0.12)",marginBottom:24,flexWrap:"wrap"}}>
        <span style={{fontSize:16}}>🔬</span>
        <span style={{fontSize:13,color:C.text,fontWeight:500}}>
          Analysing <strong>{cleanResult?"cleaned":"raw"}</strong> dataset
        </span>
        <div style={{height:14,width:1,background:C.border}}/>
        <span style={{fontSize:12,color:C.muted,fontFamily:"'DM Mono',monospace"}}>
          {fmtK(rows.length)} rows · {profile.colCount} cols · {numCols.length} numeric · {catCols.length} categorical
        </span>
      </div>

      {/* Tabs */}
      <div style={{display:"flex",gap:4,marginBottom:24,borderBottom:`1px solid ${C.border}`}}>
        {[["charts","Charts"],["correlation","Correlation Matrix"],["insights","Auto Insights"]].map(([id,label])=>(
          <button key={id} onClick={()=>setActiveTab(id)} style={{
            padding:"9px 17px",borderRadius:"8px 8px 0 0",cursor:"pointer",
            background:activeTab===id?"rgba(255,255,255,0.05)":"transparent",
            border:activeTab===id?`1px solid ${C.border}`:"1px solid transparent",
            borderBottom:activeTab===id?`1px solid ${C.bg}`:"none",
            color:activeTab===id?C.text:C.muted,fontSize:13,
            fontWeight:activeTab===id?600:400,marginBottom:activeTab===id?-1:0}}>
            {label}
            {id==="insights" && insights.length>0 && (
              <span style={{marginLeft:6,fontSize:10,background:C.purple+"22",
                color:C.purple,borderRadius:99,padding:"0 5px"}}>{insights.length}</span>
            )}
          </button>
        ))}
      </div>

      {/* ── CHARTS TAB ── */}
      {activeTab==="charts" && (
        <div style={{display:"grid",gridTemplateColumns:"220px 1fr",gap:20,animation:"fadeIn 0.3s ease"}}>
          {/* Sidebar */}
          <div style={{display:"flex",flexDirection:"column",gap:16}}>
            <div style={{padding:"16px",borderRadius:12,background:C.surface,border:`1px solid ${C.border}`}}>
              <SectionHeader>Chart Type</SectionHeader>
              <div style={{display:"flex",flexDirection:"column",gap:4}}>
                {CHART_TYPES.map(ct=>(
                  <button key={ct.id} onClick={()=>{
                    setChartType(ct.id);
                    if (ct.requires==="numeric" && numCols[0]) setSelCol(numCols[0].name);
                    if (ct.requires==="categorical" && catCols[0]) setSelCol(catCols[0].name);
                  }} style={{
                    display:"flex",alignItems:"center",gap:8,padding:"9px 12px",
                    borderRadius:8,cursor:"pointer",textAlign:"left",
                    background:chartType===ct.id?"rgba(196,181,253,0.12)":"transparent",
                    border:chartType===ct.id?`1px solid rgba(196,181,253,0.25)`:"1px solid transparent",
                    color:chartType===ct.id?C.purple:C.muted,fontSize:13,
                    transition:"all 0.15s",
                  }}>
                    <span style={{fontSize:14}}>{ct.icon}</span>
                    <span>{ct.label}</span>
                  </button>
                ))}
              </div>
            </div>

            {/* Column selector */}
            {(chartType==="histogram"||chartType==="bar") && (
              <div style={{padding:"16px",borderRadius:12,background:C.surface,border:`1px solid ${C.border}`}}>
                <SectionHeader>Column</SectionHeader>
                <div style={{display:"flex",flexDirection:"column",gap:3,maxHeight:220,overflowY:"auto"}}>
                  {(chartType==="histogram"?numCols:catCols).map(col=>(
                    <button key={col.name} onClick={()=>setSelCol(col.name)} style={{
                      padding:"7px 10px",borderRadius:7,cursor:"pointer",textAlign:"left",
                      background:selCol===col.name?"rgba(196,181,253,0.1)":"transparent",
                      border:selCol===col.name?`1px solid ${C.purple}40`:"1px solid transparent",
                      color:selCol===col.name?C.text:C.muted,fontSize:12,
                      fontFamily:"'DM Mono',monospace",overflow:"hidden",
                      textOverflow:"ellipsis",whiteSpace:"nowrap",
                    }}>
                      {col.name}
                    </button>
                  ))}
                </div>
              </div>
            )}

            {chartType==="scatter" && (
              <div style={{padding:"16px",borderRadius:12,background:C.surface,border:`1px solid ${C.border}`}}>
                <SectionHeader>X Axis</SectionHeader>
                <select value={scatterX||""} onChange={e=>setScatterX(e.target.value)}
                  style={{width:"100%",padding:"7px 10px",borderRadius:7,
                    background:"rgba(255,255,255,0.04)",border:`1px solid ${C.border}`,
                    color:C.text,fontSize:12,outline:"none",marginBottom:12}}>
                  {numCols.map(c=><option key={c.name} value={c.name}>{c.name}</option>)}
                </select>
                <SectionHeader>Y Axis</SectionHeader>
                <select value={scatterY||""} onChange={e=>setScatterY(e.target.value)}
                  style={{width:"100%",padding:"7px 10px",borderRadius:7,
                    background:"rgba(255,255,255,0.04)",border:`1px solid ${C.border}`,
                    color:C.text,fontSize:12,outline:"none"}}>
                  {numCols.map(c=><option key={c.name} value={c.name}>{c.name}</option>)}
                </select>
              </div>
            )}

            {/* Column stats panel */}
            {selCol && activeColMeta && chartType!=="scatter" && chartType!=="boxplot" && (
              <div style={{padding:"16px",borderRadius:12,background:C.surface,border:`1px solid ${C.border}`}}>
                <SectionHeader>Stats — {selCol}</SectionHeader>
                {activeColMeta.type==="numeric" ? (
                  <div style={{display:"flex",flexDirection:"column",gap:8}}>
                    <StatPill label="Mean"    value={fmt(activeColMeta.stats.mean)}   color={C.green}/>
                    <StatPill label="Median"  value={fmt(activeColMeta.stats.median)} color={C.blue}/>
                    <StatPill label="Std Dev" value={fmt(activeColMeta.stats.std)}    color={C.purple}/>
                    <StatPill label="Skew"    value={activeColMeta.stats.skewLabel}
                      color={activeColMeta.stats.skewLabel==="Normal"?C.green:C.yellow}/>
                  </div>
                ) : (
                  <div style={{display:"flex",flexDirection:"column",gap:8}}>
                    <StatPill label="Unique" value={activeColMeta.stats.unique}     color={C.green}/>
                    <StatPill label="Total"  value={activeColMeta.stats.totalCategorical} color={C.blue}/>
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Chart area */}
          <div style={{padding:"20px",borderRadius:12,background:C.surface,
            border:`1px solid ${C.border}`,minHeight:340}}>
            <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:16}}>
              <span style={{fontSize:14,fontWeight:600,color:C.text}}>
                {CHART_TYPES.find(c=>c.id===chartType)?.label}
                {selCol && chartType!=="scatter" && chartType!=="boxplot" && (
                  <span style={{marginLeft:8,fontSize:12,color:C.muted}}>— {selCol}</span>
                )}
              </span>
              <Tag color={C.purple}>{fmtK(rows.length)} rows</Tag>
            </div>
            {renderChart()}
          </div>
        </div>
      )}

      {/* ── CORRELATION TAB ── */}
      {activeTab==="correlation" && (
        <div style={{animation:"fadeIn 0.3s ease"}}>
          {!corrMatrix || corrMatrix.cols.length < 2 ? (
            <div style={{textAlign:"center",padding:"60px 0",color:C.muted}}>
              Need at least 2 numeric columns for a correlation matrix.
            </div>
          ) : (
            <>
              <div style={{display:"grid",gridTemplateColumns:"1fr auto",gap:20,marginBottom:20}}>
                <div style={{padding:"16px 20px",borderRadius:12,
                  background:C.surface,border:`1px solid ${C.border}`}}>
                  <SectionHeader>Pearson Correlation Matrix — {corrMatrix.cols.length} numeric columns</SectionHeader>
                  <CorrelationHeatmap corrMatrix={corrMatrix}/>
                </div>
                <div style={{padding:"16px",borderRadius:12,background:C.surface,
                  border:`1px solid ${C.border}`,minWidth:200}}>
                  <SectionHeader>Colour Scale</SectionHeader>
                  <div style={{display:"flex",flexDirection:"column",gap:6}}>
                    {[{r:1,label:"Perfect +1"},{r:0.7,label:"Strong +"},{r:0,label:"No corr."},{r:-0.7,label:"Strong −"},{r:-1,label:"Perfect −1"}].map(({r,label})=>{
                      const color=r>0.7?"#6EE7B7":r>0?"#93C5FD":r===0?"#64748B":r>-0.7?"#FDBA74":"#FCA5A5";
                      return (
                        <div key={r} style={{display:"flex",alignItems:"center",gap:8}}>
                          <div style={{width:16,height:16,borderRadius:3,background:color,opacity:0.7}}/>
                          <span style={{fontSize:11,color:C.subtle,fontFamily:"'DM Mono',monospace"}}>
                            {r.toFixed(1)} {label}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                  <div style={{marginTop:16,paddingTop:14,borderTop:`1px solid ${C.border}`}}>
                    <SectionHeader>Top Pairs</SectionHeader>
                    {corrMatrix.cols.flatMap((a,i)=>corrMatrix.cols.map((b,j)=>
                      j>i?{a,b,r:corrMatrix.matrix[a][b]}:null
                    ).filter(Boolean))
                      .sort((x,y)=>Math.abs(y.r)-Math.abs(x.r))
                      .slice(0,5)
                      .map(({a,b,r},i)=>(
                        <div key={i} style={{marginBottom:8}}>
                          <div style={{fontSize:11,color:C.subtle,fontFamily:"'DM Mono',monospace",
                            overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>
                            {a.slice(0,10)} ↔ {b.slice(0,10)}
                          </div>
                          <div style={{display:"flex",alignItems:"center",gap:6,marginTop:3}}>
                            <div style={{flex:1,height:4,borderRadius:99,background:"rgba(255,255,255,0.05)"}}>
                              <div style={{height:"100%",borderRadius:99,
                                background:r>0?C.green:C.red,
                                width:`${Math.abs(r)*100}%`}}/>
                            </div>
                            <span style={{fontSize:11,color:Math.abs(r)>0.7?C.green:C.yellow,
                              fontFamily:"'DM Mono',monospace",minWidth:36}}>
                              {r.toFixed(2)}
                            </span>
                          </div>
                        </div>
                      ))
                    }
                  </div>
                </div>
              </div>
            </>
          )}
        </div>
      )}

      {/* ── INSIGHTS TAB ── */}
      {activeTab==="insights" && (
        <div style={{animation:"fadeIn 0.3s ease"}}>
          {insights.length === 0 ? (
            <div style={{textAlign:"center",padding:"60px 0",color:C.muted}}>
              Computing insights…
            </div>
          ) : (
            <>
              <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(420px,1fr))",gap:14,marginBottom:24}}>
                {insights.map((ins,i)=>(
                  <div key={i} style={{
                    padding:"18px 20px",borderRadius:12,
                    background:`${ins.color}08`,
                    border:`1px solid ${ins.color}22`,
                    animation:`fadeIn ${0.15+i*0.07}s ease`,
                  }}>
                    <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:8}}>
                      <span style={{fontSize:20,color:ins.color}}>{ins.icon}</span>
                      <span style={{fontSize:12,fontWeight:600,color:ins.color,
                        textTransform:"uppercase",letterSpacing:"0.07em"}}>{ins.title}</span>
                    </div>
                    <p style={{fontSize:13,color:C.subtle,lineHeight:1.6}}>{ins.text}</p>
                  </div>
                ))}
              </div>

              {/* Numeric summary table */}
              {numCols.length > 0 && (
                <div style={{padding:"18px",borderRadius:12,background:C.surface,border:`1px solid ${C.border}`}}>
                  <SectionHeader>Numeric Column Summary</SectionHeader>
                  <div style={{overflowX:"auto"}}>
                    <table style={{width:"100%",borderCollapse:"collapse",fontSize:12,
                      fontFamily:"'DM Mono',monospace"}}>
                      <thead>
                        <tr>
                          {["Column","Mean","Median","Std Dev","Min","Max","Skew","Outliers"].map(h=>(
                            <th key={h} style={{padding:"8px 12px",textAlign:"left",
                              color:C.muted,fontWeight:600,fontSize:11,
                              textTransform:"uppercase",letterSpacing:"0.06em",
                              borderBottom:`1px solid ${C.border}`}}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {numCols.map((col,i)=>(
                          <tr key={col.name} style={{borderBottom:`1px solid rgba(255,255,255,0.03)`,
                            background:i%2===0?"transparent":"rgba(255,255,255,0.015)"}}>
                            <td style={{padding:"8px 12px",color:C.text,fontWeight:600}}>{col.name}</td>
                            <td style={{padding:"8px 12px",color:C.green}}>{fmt(col.stats.mean)}</td>
                            <td style={{padding:"8px 12px",color:C.blue}}>{fmt(col.stats.median)}</td>
                            <td style={{padding:"8px 12px",color:C.purple}}>{fmt(col.stats.std)}</td>
                            <td style={{padding:"8px 12px",color:C.muted}}>{fmt(col.stats.min,0)}</td>
                            <td style={{padding:"8px 12px",color:C.muted}}>{fmt(col.stats.max,0)}</td>
                            <td style={{padding:"8px 12px",
                              color:col.stats.skewLabel==="Normal"?C.green:C.yellow}}>
                              {col.stats.skewLabel}
                            </td>
                            <td style={{padding:"8px 12px",
                              color:col.stats.outlierCount>0?C.red:C.green}}>
                              {col.stats.outlierCount??0}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      )}
    </div>
  );
}

// ════════════════════════════════════════════════════════════════
// SECTION 7 — MODULE 4 ENGINE: AutoML
// ════════════════════════════════════════════════════════════════

// ── Math helpers ─────────────────────────────────────────────
const dot  = (a, b) => a.reduce((s,v,i) => s + v*b[i], 0);
const addV = (a, b) => a.map((v,i) => v + b[i]);
const scaleV = (a, s) => a.map(v => v*s);
const sigmoid = x => 1 / (1 + Math.exp(-Math.max(-500, Math.min(500, x))));
const shuffle  = (arr, seed=42) => {
  const a=[...arr]; let s=seed;
  for(let i=a.length-1;i>0;i--){
    s=(s*1664525+1013904223)&0xFFFFFFFF;
    const j=Math.abs(s)%(i+1);
    [a[i],a[j]]=[a[j],a[i]];
  }
  return a;
};

// ── Data prep ────────────────────────────────────────────────
function encodeFeatures(rows, featureCols, profile) {
  // Label-encode categoricals, pass numerics through, normalize all
  const encoders = {};
  featureCols.forEach(col => {
    const meta = profile.columns.find(c=>c.name===col);
    if (!meta) return;
    if (meta.type === "categorical" || meta.type === "boolean") {
      const uniq = [...new Set(rows.map(r=>String(r[col])).filter(Boolean))].sort();
      encoders[col] = { type:"label", map: Object.fromEntries(uniq.map((v,i)=>[v,i])) };
    } else {
      encoders[col] = { type:"numeric" };
    }
  });
  return { encoders };
}

function rowToFeatureVec(row, featureCols, encoders, means, stds) {
  return featureCols.map((col,i) => {
    const enc = encoders[col];
    let v = 0;
    if (!enc) return 0;
    if (enc.type === "label") v = enc.map[String(row[col])] ?? 0;
    else v = Number(row[col]) || 0;
    return stds[i] > 0 ? (v - means[i]) / stds[i] : 0;
  });
}

function prepareDataset(rows, targetCol, featureCols, profile) {
  const { encoders } = encodeFeatures(rows, featureCols, profile);
  // Encode target
  const targetMeta = profile.columns.find(c=>c.name===targetCol);
  const isClassification = targetMeta?.type === "categorical" || targetMeta?.type === "boolean";
  let targetEncoder = null;
  if (isClassification) {
    const uniq = [...new Set(rows.map(r=>String(r[targetCol])).filter(Boolean))].sort();
    targetEncoder = { classes: uniq, map: Object.fromEntries(uniq.map((v,i)=>[v,i])) };
  }
  // Raw feature matrix
  const rawX = rows.map(row => featureCols.map(col => {
    const enc = encoders[col];
    if (!enc) return 0;
    if (enc.type === "label") return enc.map[String(row[col])] ?? 0;
    return Number(row[col]) || 0;
  }));
  const rawY = rows.map(row =>
    isClassification ? (targetEncoder.map[String(row[targetCol])] ?? 0)
                     : (Number(row[targetCol]) || 0)
  );
  // Normalise features
  const means = featureCols.map((_,i) => rawX.reduce((s,r)=>s+r[i],0)/rawX.length);
  const stds  = featureCols.map((_,i) => {
    const m=means[i];
    return Math.sqrt(rawX.reduce((s,r)=>s+Math.pow(r[i]-m,2),0)/rawX.length)||1;
  });
  const X = rawX.map(r => r.map((v,i) => (v-means[i])/stds[i]));
  return { X, y:rawY, isClassification, targetEncoder, encoders, means, stds,
           classes: targetEncoder?.classes ?? null };
}

function trainTestSplit(X, y, testRatio=0.2, seed=42) {
  const idx = shuffle([...Array(X.length).keys()], seed);
  const cutoff = Math.floor(X.length * (1 - testRatio));
  const trainIdx = idx.slice(0, cutoff), testIdx = idx.slice(cutoff);
  return {
    Xtr: trainIdx.map(i=>X[i]), ytr: trainIdx.map(i=>y[i]),
    Xte: testIdx.map(i=>X[i]),  yte: testIdx.map(i=>y[i]),
  };
}

// ── Metrics ──────────────────────────────────────────────────
function classificationMetrics(yTrue, yPred, nClasses) {
  const n = yTrue.length;
  const acc = yTrue.filter((v,i)=>v===yPred[i]).length / n;
  // Macro F1
  let totalF1=0;
  for(let c=0;c<nClasses;c++){
    const tp=yTrue.filter((v,i)=>v===c&&yPred[i]===c).length;
    const fp=yTrue.filter((v,i)=>v!==c&&yPred[i]===c).length;
    const fn=yTrue.filter((v,i)=>v===c&&yPred[i]!==c).length;
    const prec=tp/(tp+fp)||0, rec=tp/(tp+fn)||0;
    totalF1 += prec+rec>0 ? 2*prec*rec/(prec+rec) : 0;
  }
  const f1 = totalF1 / nClasses;
  // Confusion matrix
  const cm = Array(nClasses).fill(0).map(()=>Array(nClasses).fill(0));
  yTrue.forEach((v,i)=>{ if(cm[v]) cm[v][yPred[i]]=(cm[v][yPred[i]]||0)+1; });
  return { accuracy:+acc.toFixed(4), f1:+f1.toFixed(4), confusionMatrix:cm };
}

function regressionMetrics(yTrue, yPred) {
  const n = yTrue.length;
  const rmse = Math.sqrt(yTrue.reduce((s,v,i)=>s+Math.pow(v-yPred[i],2),0)/n);
  const mae  = yTrue.reduce((s,v,i)=>s+Math.abs(v-yPred[i]),0)/n;
  const mean = yTrue.reduce((a,b)=>a+b,0)/n;
  const ssTot= yTrue.reduce((s,v)=>s+Math.pow(v-mean,2),0);
  const ssRes= yTrue.reduce((s,v,i)=>s+Math.pow(v-yPred[i],2),0);
  const r2   = ssTot>0 ? 1-(ssRes/ssTot) : 0;
  const mape = yTrue.reduce((s,v,i)=>s+(Math.abs(v)>0.001?Math.abs((v-yPred[i])/v):0),0)/n*100;
  return { rmse:+rmse.toFixed(4), mae:+mae.toFixed(4), r2:+r2.toFixed(4), mape:+mape.toFixed(2) };
}

// ── Models ───────────────────────────────────────────────────

// Logistic Regression — gradient descent
function trainLogisticRegression(Xtr, ytr, nClasses, iters=300, lr=0.1) {
  const nFeat = Xtr[0]?.length || 1;
  if (nClasses === 2) {
    let w = Array(nFeat).fill(0), b=0;
    for(let it=0;it<iters;it++){
      let dw=Array(nFeat).fill(0), db=0;
      Xtr.forEach((x,i)=>{
        const p=sigmoid(dot(w,x)+b), err=p-ytr[i];
        dw=addV(dw,scaleV(x,err));
        db+=err;
      });
      w=addV(w,scaleV(dw,-lr/Xtr.length));
      b-=(lr/Xtr.length)*db;
    }
    return { predict: x => sigmoid(dot(w,x)+b) >= 0.5 ? 1 : 0,
             predictProba: x => sigmoid(dot(w,x)+b), w, b };
  }
  // Multiclass: one-vs-rest
  const classifiers = Array(nClasses).fill(0).map((_,c)=>
    trainLogisticRegression(Xtr, ytr.map(v=>v===c?1:0), 2, iters, lr)
  );
  return { predict: x => {
    const probs = classifiers.map(clf=>clf.predictProba(x));
    return probs.indexOf(Math.max(...probs));
  }};
}

// Gaussian Naive Bayes
function trainNaiveBayes(Xtr, ytr, nClasses) {
  const stats = Array(nClasses).fill(0).map(c=>{
    const cX = Xtr.filter((_,i)=>ytr[i]===c);
    if(!cX.length) return { mean:[], std:[], prior:0 };
    const nF = cX[0].length;
    const mean=Array(nF).fill(0).map((_,j)=>cX.reduce((s,r)=>s+r[j],0)/cX.length);
    const std =Array(nF).fill(0).map((_,j)=>
      Math.sqrt(cX.reduce((s,r)=>s+Math.pow(r[j]-mean[j],2),0)/cX.length)+1e-9);
    return { mean, std, prior: cX.length/Xtr.length };
  });
  return { predict: x => {
    const logProbs = stats.map((s,c)=>{
      if(!s.mean.length) return -Infinity;
      let lp=Math.log(s.prior+1e-9);
      x.forEach((v,j)=>{
        const z=(v-s.mean[j])/s.std[j];
        lp+=(-0.5*z*z)-Math.log(s.std[j]*Math.sqrt(2*Math.PI));
      });
      return lp;
    });
    return logProbs.indexOf(Math.max(...logProbs));
  }};
}

// KNN (shared for classification + regression)
function trainKNN(Xtr, ytr, k=5, isClassification=true, nClasses=2) {
  return { predict: x => {
    const dists = Xtr.map((xi,i)=>({
      d: Math.sqrt(xi.reduce((s,v,j)=>s+Math.pow(v-x[j],2),0)), i
    })).sort((a,b)=>a.d-b.d).slice(0,k);
    const neighbours = dists.map(d=>ytr[d.i]);
    if(isClassification){
      const votes=Array(nClasses).fill(0);
      neighbours.forEach(v=>{if(votes[v]!==undefined)votes[v]++;});
      return votes.indexOf(Math.max(...votes));
    }
    return neighbours.reduce((a,b)=>a+b,0)/neighbours.length;
  }};
}

// CART Decision Tree (classification + regression, max_depth=5)
function trainDecisionTree(Xtr, ytr, isClassification=true, maxDepth=5, nClasses=2) {
  const nFeat = Xtr[0]?.length || 1;

  function gini(labels) {
    const n=labels.length; if(!n) return 0;
    const counts=Array(nClasses).fill(0);
    labels.forEach(l=>{ if(counts[l]!==undefined)counts[l]++; });
    return 1-counts.reduce((s,c)=>s+Math.pow(c/n,2),0);
  }
  function mse(vals) {
    const n=vals.length; if(!n) return 0;
    const m=vals.reduce((a,b)=>a+b,0)/n;
    return vals.reduce((s,v)=>s+Math.pow(v-m,2),0)/n;
  }
  function impurity(labels) { return isClassification ? gini(labels) : mse(labels); }
  function leafVal(labels) {
    if(isClassification){
      const counts=Array(nClasses).fill(0);
      labels.forEach(l=>{ if(counts[l]!==undefined)counts[l]++; });
      return counts.indexOf(Math.max(...counts));
    }
    return labels.reduce((a,b)=>a+b,0)/(labels.length||1);
  }

  const featureGains = Array(nFeat).fill(0);

  function buildNode(indices, depth) {
    const ys = indices.map(i=>ytr[i]);
    if(depth>=maxDepth || indices.length<4 || new Set(ys).size===1)
      return { leaf:true, val:leafVal(ys) };
    let bestGain=-Infinity, bestFeat=-1, bestThresh=0;
    // Sample features (sqrt(nFeat))
    const featSample = shuffle([...Array(nFeat).keys()], depth*13)
      .slice(0, Math.max(1,Math.round(Math.sqrt(nFeat))));
    for(const f of featSample){
      const vals=[...new Set(indices.map(i=>Xtr[i][f]))].sort((a,b)=>a-b);
      for(let t=0;t<vals.length-1;t++){
        const thresh=(vals[t]+vals[t+1])/2;
        const left=indices.filter(i=>Xtr[i][f]<=thresh);
        const right=indices.filter(i=>Xtr[i][f]>thresh);
        if(!left.length||!right.length) continue;
        const gain=impurity(ys)
          -(left.length/indices.length)*impurity(left.map(i=>ytr[i]))
          -(right.length/indices.length)*impurity(right.map(i=>ytr[i]));
        if(gain>bestGain){ bestGain=gain; bestFeat=f; bestThresh=thresh; }
      }
    }
    if(bestFeat<0) return { leaf:true, val:leafVal(ys) };
    featureGains[bestFeat]+=bestGain*indices.length;
    const leftIdx=indices.filter(i=>Xtr[i][bestFeat]<=bestThresh);
    const rightIdx=indices.filter(i=>Xtr[i][bestFeat]>bestThresh);
    return { leaf:false, feat:bestFeat, thresh:bestThresh,
      left:buildNode(leftIdx,depth+1), right:buildNode(rightIdx,depth+1) };
  }

  const tree = buildNode([...Array(Xtr.length).keys()], 0);

  function predict(x) {
    let node=tree;
    while(!node.leaf) node = x[node.feat]<=node.thresh ? node.left : node.right;
    return node.val;
  }

  const totalGain = featureGains.reduce((a,b)=>a+b,0)||1;
  const importance = featureGains.map(g=>+(g/totalGain).toFixed(4));
  return { predict, importance };
}

// Linear Regression — OLS normal equation (X'X)^-1 X'y
function trainLinearRegression(Xtr, ytr, ridge=0) {
  const n=Xtr.length, p=Xtr[0]?.length||1;
  // Add bias col
  const Xb=Xtr.map(r=>[1,...r]);
  const pb=p+1;
  // X'X
  const XtX=Array(pb).fill(0).map(()=>Array(pb).fill(0));
  Xb.forEach(r=>{ r.forEach((v,i)=>{ r.forEach((w,j)=>{ XtX[i][j]+=v*w; }); }); });
  if(ridge>0) for(let i=1;i<pb;i++) XtX[i][i]+=ridge;
  // X'y
  const Xty=Array(pb).fill(0);
  Xb.forEach((r,k)=>{ r.forEach((v,i)=>{ Xty[i]+=v*ytr[k]; }); });
  // Solve via Cholesky (fallback: gaussian elimination)
  function solve(A, b) {
    const n=b.length;
    const M=A.map((r,i)=>[...r,b[i]]);
    for(let col=0;col<n;col++){
      let maxRow=col;
      for(let r=col+1;r<n;r++) if(Math.abs(M[r][col])>Math.abs(M[maxRow][col])) maxRow=r;
      [M[col],M[maxRow]]=[M[maxRow],M[col]];
      if(Math.abs(M[col][col])<1e-12) continue;
      for(let r=0;r<n;r++){
        if(r===col) continue;
        const f=M[r][col]/M[col][col];
        for(let c=col;c<=n;c++) M[r][c]-=f*M[col][c];
      }
    }
    return M.map((r,i)=>r[n]/r[i]);
  }
  const w=solve(XtX,Xty);
  return { predict: x => dot(w,[1,...x]), w };
}

// ── Permutation importance (model-agnostic) ──────────────────
function permutationImportance(model, Xte, yte, baseScore, isClassification, nClasses) {
  const nFeat = Xte[0]?.length||1;
  return Array(nFeat).fill(0).map((_,f)=>{
    const Xperm = Xte.map(r=>{
      const nr=[...r];
      nr[f]=Xte[Math.floor(Math.random()*Xte.length)][f];
      return nr;
    });
    const preds = Xperm.map(x=>model.predict(x));
    const score = isClassification
      ? classificationMetrics(yte,preds,nClasses).accuracy
      : (1 - regressionMetrics(yte,preds).rmse / (regressionMetrics(yte,yte.map(()=>yte.reduce((a,b)=>a+b,0)/yte.length)).rmse||1));
    return Math.max(0, +(baseScore - score).toFixed(4));
  });
}

// ── Main AutoML runner ────────────────────────────────────────
async function runAutoML(rows, profile, targetCol, onProgress) {
  const featureCols = profile.columns
    .filter(c => c.name !== targetCol &&
      ["numeric","categorical","boolean"].includes(c.type))
    .map(c=>c.name);

  const { X, y, isClassification, targetEncoder, encoders, means, stds, classes }
    = prepareDataset(rows, targetCol, featureCols, profile);
  const { Xtr, ytr, Xte, yte } = trainTestSplit(X, y);
  const nClasses = isClassification ? (classes?.length||2) : 2;

  const modelDefs = isClassification
    ? [
        { name:"Logistic Regression", color:"#93C5FD",
          train:()=>trainLogisticRegression(Xtr,ytr,nClasses) },
        { name:"Naive Bayes",          color:"#6EE7B7",
          train:()=>trainNaiveBayes(Xtr,ytr,nClasses) },
        { name:"KNN (k=5)",            color:"#C4B5FD",
          train:()=>trainKNN(Xtr,ytr,5,true,nClasses) },
        { name:"Decision Tree",        color:"#FCD34D",
          train:()=>trainDecisionTree(Xtr,ytr,true,5,nClasses) },
      ]
    : [
        { name:"Linear Regression",   color:"#93C5FD",
          train:()=>trainLinearRegression(Xtr,ytr,0) },
        { name:"Ridge Regression",    color:"#6EE7B7",
          train:()=>trainLinearRegression(Xtr,ytr,0.01) },
        { name:"KNN Regressor (k=5)", color:"#C4B5FD",
          train:()=>trainKNN(Xtr,ytr,5,false) },
        { name:"Decision Tree",       color:"#FCD34D",
          train:()=>trainDecisionTree(Xtr,ytr,false,5,2) },
      ];

  const results = [];
  for(let i=0;i<modelDefs.length;i++){
    const def = modelDefs[i];
    onProgress({ stage:"training", model:def.name, index:i, total:modelDefs.length });
    await new Promise(r=>setTimeout(r,60)); // yield to UI
    const trained = def.train();
    const preds = Xte.map(x=>trained.predict(x));
    const metrics = isClassification
      ? classificationMetrics(yte,preds,nClasses)
      : regressionMetrics(yte,preds);
    const primaryScore = isClassification ? metrics.accuracy : metrics.r2;
    results.push({ name:def.name, color:def.color, model:trained, metrics, primaryScore,
      preds, isClassification });
  }

  // Sort by primary score
  const sorted = [...results].sort((a,b)=>b.primaryScore-a.primaryScore);
  const best = sorted[0];

  // Feature importance for best model
  onProgress({ stage:"importance", model:best.name, index:modelDefs.length, total:modelDefs.length });
  await new Promise(r=>setTimeout(r,60));
  let importance;
  if(best.model.importance) {
    importance = best.model.importance;
  } else {
    const baseScore = best.primaryScore;
    importance = permutationImportance(best.model, Xte, yte, baseScore, isClassification, nClasses);
  }
  const featImportance = featureCols.map((col,i)=>({ col, score:importance[i]||0 }))
    .sort((a,b)=>b.score-a.score);

  // 5-fold CV on best model
  onProgress({ stage:"cv", model:best.name, index:modelDefs.length+1, total:modelDefs.length });
  await new Promise(r=>setTimeout(r,60));
  const foldSize = Math.floor(X.length/5);
  const cvScores = Array(5).fill(0).map((_,f)=>{
    const Xval=X.slice(f*foldSize,(f+1)*foldSize);
    const yval=y.slice(f*foldSize,(f+1)*foldSize);
    const Xtrn=[...X.slice(0,f*foldSize),...X.slice((f+1)*foldSize)];
    const ytrn=[...y.slice(0,f*foldSize),...y.slice((f+1)*foldSize)];
    let m;
    if(best.name==="Logistic Regression") m=trainLogisticRegression(Xtrn,ytrn,nClasses);
    else if(best.name==="Naive Bayes") m=trainNaiveBayes(Xtrn,ytrn,nClasses);
    else if(best.name.startsWith("KNN")) m=trainKNN(Xtrn,ytrn,5,isClassification,nClasses);
    else if(best.name==="Decision Tree") m=trainDecisionTree(Xtrn,ytrn,isClassification,5,nClasses);
    else if(best.name==="Ridge Regression") m=trainLinearRegression(Xtrn,ytrn,0.01);
    else m=trainLinearRegression(Xtrn,ytrn,0);
    const p=Xval.map(x=>m.predict(x));
    return isClassification ? classificationMetrics(yval,p,nClasses).accuracy
                            : regressionMetrics(yval,p).r2;
  });
  const cvMean = +(cvScores.reduce((a,b)=>a+b,0)/5).toFixed(4);
  const cvStd  = +(Math.sqrt(cvScores.reduce((s,v)=>s+Math.pow(v-cvMean,2),0)/5)).toFixed(4);

  return {
    isClassification, targetCol, featureCols, classes, targetEncoder,
    encoders, means, stds, nClasses,
    results: sorted, bestModel: best,
    featImportance, cvScores, cvMean, cvStd,
    trainSize: Xtr.length, testSize: Xte.length,
  };
}

// ════════════════════════════════════════════════════════════════
// SECTION 7b — MODULE 4 UI COMPONENTS
// ════════════════════════════════════════════════════════════════

function MetricCard({ label, value, color, sub }) {
  return (
    <div style={{ padding:"16px 18px", borderRadius:12,
      background:C.surface, border:`1px solid ${C.border}`,
      display:"flex", flexDirection:"column", gap:4 }}>
      <span style={{ fontSize:11, color:C.muted, textTransform:"uppercase",
        letterSpacing:"0.08em" }}>{label}</span>
      <span style={{ fontSize:26, fontWeight:800, color,
        fontFamily:"'DM Mono',monospace", lineHeight:1 }}>{value}</span>
      {sub && <span style={{ fontSize:11, color:C.muted }}>{sub}</span>}
    </div>
  );
}

function ModelRow({ result, rank, isClassification }) {
  const isBest = rank === 0;
  const score  = isClassification ? result.metrics.accuracy : result.metrics.r2;
  const scoreLabel = isClassification ? "Accuracy" : "R²";
  return (
    <div style={{
      display:"grid",
      gridTemplateColumns:"32px 1fr 120px 100px 100px",
      alignItems:"center", gap:12,
      padding:"12px 16px", borderRadius:10,
      background: isBest ? `${result.color}12` : C.surface,
      border: `1px solid ${isBest ? result.color+"40" : C.border}`,
      transition:"all 0.2s",
    }}>
      <span style={{ fontSize:14, fontWeight:700,
        color: isBest ? result.color : C.muted,
        fontFamily:"'DM Mono',monospace" }}>
        {isBest ? "🥇" : `#${rank+1}`}
      </span>
      <div style={{ display:"flex", alignItems:"center", gap:8 }}>
        <div style={{ width:8, height:8, borderRadius:"50%",
          background:result.color, flexShrink:0 }}/>
        <span style={{ fontSize:13, fontWeight:isBest?600:400,
          color: isBest ? C.text : C.subtle }}>{result.name}</span>
        {isBest && <Tag color={result.color}>BEST</Tag>}
      </div>
      <div>
        <div style={{ height:5, borderRadius:99, background:"rgba(255,255,255,0.06)" }}>
          <div style={{ height:"100%", borderRadius:99,
            background:result.color, width:`${Math.max(0,score*100)}%`,
            transition:"width 1s ease" }}/>
        </div>
      </div>
      <span style={{ fontSize:13, fontWeight:600, color:result.color,
        fontFamily:"'DM Mono',monospace", textAlign:"right" }}>
        {(score*100).toFixed(1)}%
      </span>
      <span style={{ fontSize:11, color:C.muted, fontFamily:"'DM Mono',monospace",
        textAlign:"right" }}>{scoreLabel}</span>
    </div>
  );
}

function FeatureImportanceChart({ featImportance }) {
  if (!featImportance?.length) return <div style={{color:C.muted,padding:24}}>No data</div>;
  const top = featImportance.slice(0,12);
  const maxScore = top[0]?.score || 1;
  const colors = ["#6EE7B7","#93C5FD","#C4B5FD","#FCD34D","#FDBA74","#FCA5A5"];
  return (
    <div style={{ display:"flex", flexDirection:"column", gap:10 }}>
      {top.map((f,i) => (
        <div key={f.col} style={{ display:"grid",
          gridTemplateColumns:"160px 1fr 72px", alignItems:"center", gap:12 }}>
          <span style={{ fontSize:12, color:C.text, fontFamily:"'DM Mono',monospace",
            overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap",
            textAlign:"right" }}>{f.col}</span>
          <div style={{ height:8, borderRadius:99, background:"rgba(255,255,255,0.05)" }}>
            <div style={{ height:"100%", borderRadius:99,
              background:colors[i%colors.length],
              width:`${(f.score/maxScore)*100}%`,
              transition:"width 1.2s ease",
              boxShadow:`0 0 8px ${colors[i%colors.length]}66` }}/>
          </div>
          <span style={{ fontSize:11, color:colors[i%colors.length],
            fontFamily:"'DM Mono',monospace", textAlign:"right" }}>
            {(f.score*100).toFixed(1)}%
          </span>
        </div>
      ))}
    </div>
  );
}

function ConfusionMatrix({ cm, classes }) {
  if (!cm?.length) return null;
  const n = cm.length;
  const maxVal = Math.max(...cm.flat());
  return (
    <div>
      <SectionHeader>Confusion Matrix — Best Model</SectionHeader>
      <div style={{ display:"inline-flex", flexDirection:"column", gap:2 }}>
        <div style={{ display:"flex", gap:2, marginLeft:64 }}>
          {classes?.slice(0,n).map((c,j)=>(
            <div key={j} style={{ width:52, textAlign:"center", fontSize:10,
              color:C.muted, fontFamily:"'DM Mono',monospace",
              overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>
              {String(c).length>6?String(c).slice(0,6):String(c)}
            </div>
          ))}
        </div>
        {cm.map((row,i)=>(
          <div key={i} style={{ display:"flex", gap:2, alignItems:"center" }}>
            <div style={{ width:60, fontSize:10, color:C.muted,
              fontFamily:"'DM Mono',monospace", textAlign:"right", paddingRight:6,
              overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>
              {String(classes?.[i]||i).slice(0,6)}
            </div>
            {row.map((val,j)=>{
              const intensity = maxVal>0 ? val/maxVal : 0;
              const isCorrect = i===j;
              return (
                <div key={j} style={{ width:52, height:52,
                  borderRadius:6, display:"flex", alignItems:"center",
                  justifyContent:"center",
                  background: isCorrect
                    ? `rgba(110,231,183,${0.1+intensity*0.7})`
                    : `rgba(252,165,165,${intensity*0.5})`,
                  border:`1px solid ${isCorrect?"rgba(110,231,183,0.2)":"rgba(252,165,165,0.1)"}` }}>
                  <span style={{ fontSize:13, fontWeight:600,
                    color:isCorrect?C.green:intensity>0.3?C.red:C.muted,
                    fontFamily:"'DM Mono',monospace" }}>{val}</span>
                </div>
              );
            })}
          </div>
        ))}
      </div>
    </div>
  );
}

function CVChart({ cvScores }) {
  if (!cvScores?.length) return null;
  const mean = cvScores.reduce((a,b)=>a+b,0)/cvScores.length;
  return (
    <div>
      <SectionHeader>5-Fold Cross-Validation Scores</SectionHeader>
      <div style={{ display:"flex", alignItems:"flex-end", gap:10, height:80 }}>
        {cvScores.map((s,i)=>{
          const h = Math.max(12, s*80);
          const color = s >= mean ? C.green : C.yellow;
          return (
            <div key={i} style={{ flex:1, display:"flex",
              flexDirection:"column", alignItems:"center", gap:4 }}>
              <span style={{ fontSize:10, color, fontFamily:"'DM Mono',monospace" }}>
                {(s*100).toFixed(1)}%
              </span>
              <div style={{ width:"100%", height:h, borderRadius:4,
                background: i===cvScores.indexOf(Math.max(...cvScores))
                  ? `${color}` : `${color}66`,
                transition:"height 0.8s ease" }}/>
              <span style={{ fontSize:9, color:C.muted }}>F{i+1}</span>
            </div>
          );
        })}
        <div style={{ width:1, height:80, position:"relative" }}>
          <div style={{ position:"absolute", top:80*(1-mean), left:-999, right:-4,
            borderTop:`1px dashed ${C.blue}`, opacity:0.5 }}/>
        </div>
      </div>
    </div>
  );
}

function Module4({ dataset, cleanResult, onMLDone }) {
  const rows    = cleanResult?.cleanedRows  ?? dataset?.rows ?? [];
  const profile = cleanResult?.afterProfile ?? dataset?.profile;

  const [activeTab,   setActiveTab]   = useState("setup");
  const [targetCol,   setTargetCol]   = useState(null);
  const [phase,       setPhase]       = useState("idle"); // idle|training|done
  const [progress,    setProgress]    = useState({ stage:"", model:"", index:0, total:4 });
  const [mlResult,    setMlResult]    = useState(null);
  const [predInputs,  setPredInputs]  = useState({});
  const [prediction,  setPrediction]  = useState(null);

  // Auto-pick a target column on load
  useEffect(()=>{
    if (!profile || targetCol) return;
    const catCols = profile.columns.filter(c=>c.type==="categorical"||c.type==="boolean");
    const numCols = profile.columns.filter(c=>c.type==="numeric");
    setTargetCol(catCols[0]?.name ?? numCols[numCols.length-1]?.name ?? null);
  },[profile, targetCol]);

  if (!profile) return (
    <div style={{textAlign:"center",padding:"80px 0",color:C.muted}}>
      Complete Modules 1–3 first.
    </div>
  );

  const targetMeta = profile.columns.find(c=>c.name===targetCol);
  const isClassification = targetMeta?.type==="categorical"||targetMeta?.type==="boolean";
  const featureCols = profile.columns.filter(c=>
    c.name!==targetCol && ["numeric","categorical","boolean"].includes(c.type));

  const handleTrain = async () => {
    if (!targetCol) return;
    setPhase("training");
    setMlResult(null);
    setPrediction(null);
    setActiveTab("training");
    try {
      const result = await runAutoML(rows, profile, targetCol, p => setProgress({...p}));
      setMlResult(result);
      setPhase("done");
      setActiveTab("results");
      // Init predict inputs with means
      const inputs = {};
      result.featureCols.forEach((col,i) => {
        const meta = profile.columns.find(c=>c.name===col);
        if(meta?.type==="numeric") inputs[col] = result.means[i]?.toFixed(2) ?? "0";
        else if(meta?.type==="categorical" && meta.stats.topValues?.[0])
          inputs[col] = meta.stats.topValues[0][0];
        else inputs[col] = "0";
      });
      setPredInputs(inputs);
      onMLDone(result);
    } catch(e) {
      setPhase("idle");
      console.error("AutoML error:", e);
    }
  };

  const handlePredict = () => {
    if (!mlResult) return;
    const { featureCols, encoders, means, stds, bestModel, isClassification, classes } = mlResult;
    const row = {};
    featureCols.forEach(col => { row[col] = predInputs[col] ?? "0"; });
    const vec = rowToFeatureVec(row, featureCols, encoders, means, stds);
    const raw = bestModel.model.predict(vec);
    const label = isClassification ? (classes?.[raw] ?? raw) : raw;
    setPrediction({ raw, label, isClassification });
    setActiveTab("predict");
  };

  const best = mlResult?.bestModel;
  const tabs = [
    ["setup","⚙ Setup"],
    ["training","⟳ Training"],
    ...(mlResult ? [["results","★ Results"],["features","▦ Features"],["predict","▶ Predict"]] : []),
  ];

  return (
    <div style={{animation:"fadeIn 0.4s ease"}}>
      {/* Header card */}
      <div style={{padding:"18px 22px",borderRadius:14,marginBottom:24,
        background:"linear-gradient(135deg,rgba(252,211,77,0.07),rgba(253,186,116,0.04))",
        border:`1px solid rgba(252,211,77,0.15)`}}>
        <div style={{display:"flex",alignItems:"flex-start",justifyContent:"space-between",
          flexWrap:"wrap",gap:16}}>
          <div>
            <h2 style={{fontSize:17,fontWeight:700,color:C.text,marginBottom:5}}>
              AutoML Engine
            </h2>
            <p style={{fontSize:13,color:C.muted,lineHeight:1.6,maxWidth:600}}>
              Select a target column — the engine auto-detects classification vs regression,
              trains 4 models, evaluates metrics, computes feature importance, and runs 5-fold CV.
            </p>
          </div>
          <button onClick={handleTrain} disabled={phase==="training"||!targetCol} style={{
            padding:"10px 22px",borderRadius:8,fontSize:13,fontWeight:600,cursor:"pointer",
            background:phase==="training"?"rgba(255,255,255,0.04)":
              "linear-gradient(135deg,#FCD34D,#F59E0B)",
            color:phase==="training"?C.muted:"#1C1400",border:"none",
            opacity:!targetCol||phase==="training"?0.5:1,
            transition:"all 0.2s", flexShrink:0,
          }}>
            {phase==="training" ? "⟳ Training…" : phase==="done" ? "↺ Re-train" : "▶ Run AutoML"}
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div style={{display:"flex",gap:4,marginBottom:24,borderBottom:`1px solid ${C.border}`}}>
        {tabs.map(([id,label])=>(
          <button key={id} onClick={()=>setActiveTab(id)} style={{
            padding:"9px 17px",borderRadius:"8px 8px 0 0",cursor:"pointer",
            background:activeTab===id?"rgba(255,255,255,0.05)":"transparent",
            border:activeTab===id?`1px solid ${C.border}`:"1px solid transparent",
            borderBottom:activeTab===id?`1px solid ${C.bg}`:"none",
            color:activeTab===id?C.text:C.muted,fontSize:13,
            fontWeight:activeTab===id?600:400,marginBottom:activeTab===id?-1:0}}>
            {label}
          </button>
        ))}
      </div>

      {/* ── SETUP TAB ── */}
      {activeTab==="setup" && (
        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:20,animation:"fadeIn 0.3s ease"}}>
          {/* Target selector */}
          <div style={{padding:"20px",borderRadius:12,background:C.surface,border:`1px solid ${C.border}`}}>
            <SectionHeader>Target Column (What to Predict)</SectionHeader>
            <div style={{display:"flex",flexDirection:"column",gap:5,maxHeight:300,overflowY:"auto"}}>
              {profile.columns
                .filter(c=>["numeric","categorical","boolean"].includes(c.type))
                .map(col=>{
                  const isSel = targetCol===col.name;
                  const isClf = col.type==="categorical"||col.type==="boolean";
                  return (
                    <button key={col.name} onClick={()=>setTargetCol(col.name)} style={{
                      display:"flex",alignItems:"center",gap:10,padding:"10px 12px",
                      borderRadius:8,cursor:"pointer",textAlign:"left",
                      background:isSel?`${C.yellow}12`:"transparent",
                      border:`1px solid ${isSel?C.yellow+"40":C.border}`,
                      color:isSel?C.text:C.subtle,fontSize:13,transition:"all 0.15s",
                    }}>
                      <span style={{fontSize:14}}>{isClf?"🏷":"📈"}</span>
                      <span style={{flex:1,fontFamily:"'DM Mono',monospace",
                        overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>
                        {col.name}
                      </span>
                      <Tag color={isClf?C.blue:C.green}>{isClf?"Classify":"Regress"}</Tag>
                    </button>
                  );
                })}
            </div>
          </div>

          {/* Problem summary */}
          <div style={{display:"flex",flexDirection:"column",gap:16}}>
            {targetCol && (
              <div style={{padding:"20px",borderRadius:12,
                background:isClassification?`rgba(147,197,253,0.07)`:`rgba(110,231,183,0.07)`,
                border:`1px solid ${isClassification?`rgba(147,197,253,0.2)`:`rgba(110,231,183,0.2)`}`}}>
                <SectionHeader>Problem Type Detected</SectionHeader>
                <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:12}}>
                  <span style={{fontSize:28}}>{isClassification?"🏷":"📈"}</span>
                  <div>
                    <div style={{fontSize:16,fontWeight:700,
                      color:isClassification?C.blue:C.green}}>
                      {isClassification?"Classification":"Regression"}
                    </div>
                    <div style={{fontSize:12,color:C.muted}}>
                      Target: <span style={{fontFamily:"'DM Mono',monospace",
                        color:C.text}}>{targetCol}</span>
                    </div>
                  </div>
                </div>
                {isClassification && targetMeta?.stats.topValues && (
                  <div>
                    <SectionHeader>Target Classes</SectionHeader>
                    <div style={{display:"flex",gap:6,flexWrap:"wrap"}}>
                      {targetMeta.stats.topValues.map(([v,c],i)=>(
                        <Tag key={i} color={C.blue}>{v} ({c})</Tag>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}
            <div style={{padding:"18px",borderRadius:12,background:C.surface,
              border:`1px solid ${C.border}`}}>
              <SectionHeader>Feature Columns ({featureCols.length})</SectionHeader>
              <div style={{display:"flex",flexWrap:"wrap",gap:5}}>
                {featureCols.map(c=>(
                  <Tag key={c.name} color={TYPE_META[c.type]?.color??C.subtle}>
                    {c.name}
                  </Tag>
                ))}
              </div>
              <div style={{marginTop:12,display:"grid",gridTemplateColumns:"1fr 1fr",gap:8}}>
                <StatPill label="Train rows" value={Math.floor(rows.length*0.8)} color={C.green}/>
                <StatPill label="Test rows"  value={Math.ceil(rows.length*0.2)}  color={C.blue}/>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* ── TRAINING TAB ── */}
      {activeTab==="training" && (
        <div style={{animation:"fadeIn 0.3s ease"}}>
          {phase==="idle" && !mlResult && (
            <div style={{textAlign:"center",padding:"60px 0",color:C.muted}}>
              <div style={{fontSize:48,marginBottom:16,opacity:0.4}}>🤖</div>
              <p>Click <strong style={{color:C.text}}>Run AutoML</strong> to begin training</p>
            </div>
          )}
          {(phase==="training"||phase==="done") && (
            <div style={{maxWidth:680,margin:"0 auto"}}>
              {/* Overall progress */}
              <div style={{marginBottom:28}}>
                <div style={{display:"flex",justifyContent:"space-between",
                  alignItems:"center",marginBottom:8}}>
                  <span style={{fontSize:13,color:C.text,fontWeight:600}}>
                    {phase==="training"
                      ? `Training: ${progress.model || "…"}`
                      : "✓ All models trained"}
                  </span>
                  <span style={{fontSize:12,color:C.muted,fontFamily:"'DM Mono',monospace"}}>
                    {phase==="done"?progress.total:progress.index}/{progress.total} models
                  </span>
                </div>
                <div style={{height:6,borderRadius:99,background:"rgba(255,255,255,0.07)"}}>
                  <div style={{height:"100%",borderRadius:99,
                    background:"linear-gradient(90deg,#FCD34D,#F59E0B)",
                    width:`${((phase==="done"?progress.total:progress.index)/Math.max(progress.total,1))*100}%`,
                    transition:"width 0.4s ease"}}/>
                </div>
              </div>
              {/* Stage timeline */}
              {["training","importance","cv"].map((stage,si)=>{
                const stageLabels=["Model Training","Feature Importance","Cross-Validation"];
                const stageDone = phase==="done" ||
                  (stage==="training"&&progress.index>=progress.total) ||
                  (stage==="importance"&&progress.stage==="cv");
                const stageActive = progress.stage===stage;
                const color = stageDone?C.green:stageActive?C.yellow:C.muted;
                return (
                  <div key={stage} style={{display:"flex",gap:16,
                    paddingBottom:si<2?20:0,alignItems:"flex-start"}}>
                    <div style={{display:"flex",flexDirection:"column",alignItems:"center"}}>
                      <div style={{width:32,height:32,borderRadius:"50%",
                        display:"flex",alignItems:"center",justifyContent:"center",
                        background:`${color}18`,border:`2px solid ${color}`,
                        fontSize:14,transition:"all 0.3s",
                        boxShadow:stageActive?`0 0 12px ${color}55`:"none"}}>
                        {stageDone?"✓":stageActive?"⟳":["🤖","▦","⊞"][si]}
                      </div>
                      {si<2&&<div style={{width:2,flex:1,marginTop:4,
                        background:stageDone?`${color}40`:"rgba(255,255,255,0.05)"}}/>}
                    </div>
                    <div style={{flex:1,paddingTop:4}}>
                      <div style={{fontSize:13,fontWeight:600,color,marginBottom:8}}>
                        {stageLabels[si]}
                        {stageActive && <span style={{marginLeft:8,fontSize:11,
                          animation:"pulse 1s infinite",color}}>running…</span>}
                      </div>
                      {/* Model mini-results during training */}
                      {stage==="training" && mlResult && (
                        <div style={{display:"flex",flexDirection:"column",gap:5}}>
                          {mlResult.results.map((r,i)=>(
                            <div key={r.name} style={{display:"flex",alignItems:"center",
                              gap:10,padding:"7px 12px",borderRadius:8,
                              background:`${r.color}08`,border:`1px solid ${r.color}18`}}>
                              <div style={{width:8,height:8,borderRadius:"50%",background:r.color}}/>
                              <span style={{fontSize:12,color:C.text,flex:1}}>{r.name}</span>
                              <span style={{fontSize:12,color:r.color,
                                fontFamily:"'DM Mono',monospace"}}>
                                {r.isClassification
                                  ? `${(r.metrics.accuracy*100).toFixed(1)}% acc`
                                  : `R² ${r.metrics.r2.toFixed(3)}`}
                              </span>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}

      {/* ── RESULTS TAB ── */}
      {activeTab==="results" && mlResult && (
        <div style={{animation:"fadeIn 0.3s ease"}}>
          {/* Best model hero */}
          <div style={{padding:"20px 24px",borderRadius:14,marginBottom:24,
            background:`${best.color}10`,border:`1px solid ${best.color}30`}}>
            <div style={{display:"flex",alignItems:"center",gap:16,flexWrap:"wrap"}}>
              <div style={{fontSize:36}}>🥇</div>
              <div style={{flex:1}}>
                <div style={{fontSize:11,color:C.muted,textTransform:"uppercase",
                  letterSpacing:"0.1em",marginBottom:2}}>Best Model</div>
                <div style={{fontSize:20,fontWeight:700,color:best.color}}>
                  {best.name}
                </div>
                <div style={{fontSize:12,color:C.muted,marginTop:2}}>
                  CV Score: {(mlResult.cvMean*100).toFixed(1)}% ± {(mlResult.cvStd*100).toFixed(1)}%
                </div>
              </div>
              {/* Key metrics */}
              <div style={{display:"flex",gap:12,flexWrap:"wrap"}}>
                {mlResult.isClassification ? [
                  {label:"Accuracy", value:`${(best.metrics.accuracy*100).toFixed(1)}%`, color:C.green},
                  {label:"F1 Score", value:`${(best.metrics.f1*100).toFixed(1)}%`,       color:C.blue},
                ] : [
                  {label:"R² Score", value:best.metrics.r2.toFixed(3),            color:C.green},
                  {label:"RMSE",     value:best.metrics.rmse.toFixed(3),           color:C.yellow},
                  {label:"MAE",      value:best.metrics.mae.toFixed(3),            color:C.blue},
                ].map(m=>(
                  <MetricCard key={m.label} label={m.label} value={m.value} color={m.color}/>
                ))}
              </div>
            </div>
          </div>

          {/* Model leaderboard */}
          <div style={{marginBottom:24}}>
            <SectionHeader>Model Leaderboard</SectionHeader>
            <div style={{display:"flex",flexDirection:"column",gap:8}}>
              {mlResult.results.map((r,i)=>(
                <ModelRow key={r.name} result={r} rank={i} isClassification={mlResult.isClassification}/>
              ))}
            </div>
          </div>

          {/* Metrics + confusion matrix */}
          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:20}}>
            <div style={{padding:"18px",borderRadius:12,background:C.surface,
              border:`1px solid ${C.border}`}}>
              <CVChart cvScores={mlResult.cvScores}/>
            </div>
            {mlResult.isClassification && best.metrics.confusionMatrix && (
              <div style={{padding:"18px",borderRadius:12,background:C.surface,
                border:`1px solid ${C.border}`}}>
                <ConfusionMatrix cm={best.metrics.confusionMatrix}
                  classes={mlResult.classes}/>
              </div>
            )}
            {!mlResult.isClassification && (
              <div style={{padding:"18px",borderRadius:12,background:C.surface,
                border:`1px solid ${C.border}`}}>
                <SectionHeader>Regression Metrics — All Models</SectionHeader>
                <div style={{overflowX:"auto"}}>
                  <table style={{width:"100%",borderCollapse:"collapse",
                    fontSize:12,fontFamily:"'DM Mono',monospace"}}>
                    <thead>
                      <tr>{["Model","RMSE","MAE","R²","MAPE"].map(h=>(
                        <th key={h} style={{padding:"7px 10px",textAlign:"left",
                          color:C.muted,fontWeight:600,fontSize:11,
                          borderBottom:`1px solid ${C.border}`}}>{h}</th>
                      ))}</tr>
                    </thead>
                    <tbody>
                      {mlResult.results.map((r,i)=>(
                        <tr key={r.name} style={{borderBottom:`1px solid rgba(255,255,255,0.03)`,
                          background:i===0?`${r.color}08`:"transparent"}}>
                          <td style={{padding:"7px 10px",color:i===0?r.color:C.subtle,fontWeight:i===0?600:400}}>
                            {i===0?"🥇 ":""}{r.name}
                          </td>
                          <td style={{padding:"7px 10px",color:C.yellow}}>{r.metrics.rmse}</td>
                          <td style={{padding:"7px 10px",color:C.blue}}>{r.metrics.mae}</td>
                          <td style={{padding:"7px 10px",color:C.green}}>{r.metrics.r2}</td>
                          <td style={{padding:"7px 10px",color:C.muted}}>{r.metrics.mape}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* ── FEATURES TAB ── */}
      {activeTab==="features" && mlResult && (
        <div style={{animation:"fadeIn 0.3s ease"}}>
          <div style={{padding:"20px",borderRadius:12,background:C.surface,
            border:`1px solid ${C.border}`,marginBottom:20}}>
            <div style={{display:"flex",alignItems:"center",
              justifyContent:"space-between",marginBottom:16}}>
              <div>
                <SectionHeader>Feature Importance — {best.name}</SectionHeader>
                <p style={{fontSize:12,color:C.muted}}>
                  {best.model.importance
                    ? "Tree split-gain importance (built-in)"
                    : "Permutation importance — accuracy drop when feature is shuffled"}
                </p>
              </div>
              <Tag color={C.yellow}>Top {Math.min(12,mlResult.featImportance.length)} features</Tag>
            </div>
            <FeatureImportanceChart featImportance={mlResult.featImportance}/>
          </div>

          {/* Insight cards from importance */}
          <div style={{display:"grid",
            gridTemplateColumns:"repeat(auto-fill,minmax(300px,1fr))",gap:12}}>
            {mlResult.featImportance.slice(0,3).map((f,i)=>{
              const colors=[C.green,C.blue,C.yellow];
              const labels=["Most Important","Second Most","Third Most"];
              return (
                <div key={f.col} style={{padding:"16px",borderRadius:12,
                  background:`${colors[i]}08`,border:`1px solid ${colors[i]}20`}}>
                  <div style={{fontSize:11,color:colors[i],textTransform:"uppercase",
                    letterSpacing:"0.08em",marginBottom:6}}>{labels[i]}</div>
                  <div style={{fontSize:16,fontWeight:700,color:C.text,marginBottom:4,
                    fontFamily:"'DM Mono',monospace"}}>{f.col}</div>
                  <div style={{fontSize:12,color:C.muted}}>
                    Importance score: {(f.score*100).toFixed(1)}%
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* ── PREDICT TAB ── */}
      {activeTab==="predict" && mlResult && (
        <div style={{animation:"fadeIn 0.3s ease"}}>
          <div style={{display:"grid",gridTemplateColumns:"1fr 340px",gap:20}}>
            {/* Input form */}
            <div style={{padding:"20px",borderRadius:12,background:C.surface,
              border:`1px solid ${C.border}`}}>
              <SectionHeader>Enter Feature Values</SectionHeader>
              <div style={{display:"grid",
                gridTemplateColumns:"repeat(auto-fill,minmax(220px,1fr))",gap:12,
                marginBottom:20}}>
                {mlResult.featureCols.map((col,i)=>{
                  const meta=profile.columns.find(c=>c.name===col);
                  const isCat=meta?.type==="categorical"||meta?.type==="boolean";
                  return (
                    <div key={col}>
                      <label style={{display:"block",fontSize:11,color:C.muted,
                        marginBottom:5,fontFamily:"'DM Mono',monospace",
                        textTransform:"uppercase",letterSpacing:"0.06em"}}>
                        {col}
                        <Tag color={TYPE_META[meta?.type]?.color??C.subtle}>
                          {meta?.type}
                        </Tag>
                      </label>
                      {isCat ? (
                        <select value={predInputs[col]??""} onChange={e=>setPredInputs(p=>({...p,[col]:e.target.value}))}
                          style={{width:"100%",padding:"8px 10px",borderRadius:8,
                            background:"rgba(255,255,255,0.04)",border:`1px solid ${C.border}`,
                            color:C.text,fontSize:13,outline:"none"}}>
                          {meta?.stats.topValues?.map(([v])=>(
                            <option key={v} value={v}>{v}</option>
                          ))}
                        </select>
                      ) : (
                        <input type="number" value={predInputs[col]??""} step="any"
                          onChange={e=>setPredInputs(p=>({...p,[col]:e.target.value}))}
                          style={{width:"100%",padding:"8px 10px",borderRadius:8,
                            background:"rgba(255,255,255,0.04)",border:`1px solid ${C.border}`,
                            color:C.text,fontSize:13,outline:"none",
                            fontFamily:"'DM Mono',monospace"}}/>
                      )}
                    </div>
                  );
                })}
              </div>
              <button onClick={handlePredict} style={{
                padding:"11px 28px",borderRadius:8,fontSize:14,fontWeight:600,
                background:"linear-gradient(135deg,#FCD34D,#F59E0B)",
                color:"#1C1400",border:"none",cursor:"pointer"}}>
                ▶ Run Prediction
              </button>
            </div>

            {/* Prediction result */}
            <div style={{display:"flex",flexDirection:"column",gap:14}}>
              {prediction ? (
                <div style={{padding:"24px",borderRadius:14,
                  background:mlResult.isClassification?`rgba(147,197,253,0.08)`:`rgba(110,231,183,0.08)`,
                  border:`1px solid ${mlResult.isClassification?"rgba(147,197,253,0.2)":"rgba(110,231,183,0.2)"}`,
                  animation:"fadeIn 0.3s ease"}}>
                  <div style={{fontSize:11,color:C.muted,textTransform:"uppercase",
                    letterSpacing:"0.1em",marginBottom:12}}>Prediction</div>
                  <div style={{fontSize:42,fontWeight:800,
                    color:mlResult.isClassification?C.blue:C.green,
                    fontFamily:"'DM Mono',monospace",marginBottom:8,
                    lineHeight:1,wordBreak:"break-all"}}>
                    {mlResult.isClassification
                      ? String(prediction.label)
                      : Number(prediction.label).toFixed(3)}
                  </div>
                  <div style={{fontSize:12,color:C.muted,marginBottom:12}}>
                    {mlResult.isClassification ? "Predicted class" : "Predicted value"}
                    {" · "}Model: {best.name}
                  </div>
                  <div style={{padding:"10px 14px",borderRadius:8,
                    background:"rgba(255,255,255,0.04)",border:`1px solid ${C.border}`}}>
                    <div style={{fontSize:11,color:C.muted,marginBottom:4}}>Target column</div>
                    <div style={{fontSize:13,fontWeight:600,color:C.text,
                      fontFamily:"'DM Mono',monospace"}}>{mlResult.targetCol}</div>
                  </div>
                </div>
              ) : (
                <div style={{padding:"24px",borderRadius:14,background:C.surface,
                  border:`1px solid ${C.border}`,textAlign:"center",color:C.muted}}>
                  <div style={{fontSize:36,marginBottom:12,opacity:0.3}}>▶</div>
                  <p style={{fontSize:13}}>Fill in feature values and click Run Prediction</p>
                </div>
              )}

              {/* Model info card */}
              <div style={{padding:"16px",borderRadius:12,background:C.surface,
                border:`1px solid ${C.border}`}}>
                <SectionHeader>Model in Use</SectionHeader>
                <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:10}}>
                  <div style={{width:8,height:8,borderRadius:"50%",background:best.color}}/>
                  <span style={{fontSize:13,fontWeight:600,color:C.text}}>{best.name}</span>
                  <Tag color={best.color}>BEST</Tag>
                </div>
                <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8}}>
                  {mlResult.isClassification ? [
                    {l:"Accuracy",v:`${(best.metrics.accuracy*100).toFixed(1)}%`,c:C.green},
                    {l:"F1 Score",v:`${(best.metrics.f1*100).toFixed(1)}%`,c:C.blue},
                  ] : [
                    {l:"R² Score",v:best.metrics.r2.toFixed(3),c:C.green},
                    {l:"RMSE",v:best.metrics.rmse.toFixed(3),c:C.yellow},
                  ].map(({l,v,c})=><StatPill key={l} label={l} value={v} color={c}/>)}
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ════════════════════════════════════════════════════════════════
// SECTION 8 — MODULE 5: LLM INSIGHTS ENGINE
// ════════════════════════════════════════════════════════════════

// ── Build rich context string from all pipeline results ───────
function buildDataContext(dataset, cleanResult, mlResult) {
  const profile  = cleanResult?.afterProfile ?? dataset?.profile;
  const rows     = cleanResult?.cleanedRows   ?? dataset?.rows ?? [];
  const cleaning = cleanResult?.stats;

  if (!profile) return "No dataset loaded.";

  const numCols = profile.columns.filter(c=>c.type==="numeric");
  const catCols = profile.columns.filter(c=>c.type==="categorical");

  let ctx = `=== DATASET OVERVIEW ===\n`;
  ctx += `File: ${dataset?.fileName ?? "unknown"}\n`;
  ctx += `Rows: ${fmtK(rows.length)} | Columns: ${profile.colCount} | Quality Score: ${profile.qualityScore}/100\n`;
  ctx += `Numeric columns (${numCols.length}): ${numCols.map(c=>c.name).join(", ")}\n`;
  ctx += `Categorical columns (${catCols.length}): ${catCols.map(c=>c.name).join(", ")}\n\n`;

  if (cleaning) {
    ctx += `=== CLEANING RESULTS ===\n`;
    ctx += `Rows dropped: ${cleaning.rowsDropped} | Cols dropped: ${cleaning.colsDropped}\n`;
    ctx += `Cells imputed: ${cleaning.cellsImputed} | Outliers winsorized: ${cleaning.outliersWinsorized}\n`;
    ctx += `Duplicates removed: ${cleaning.duplicatesRemoved}\n\n`;
  }

  ctx += `=== COLUMN STATISTICS ===\n`;
  numCols.forEach(c => {
    ctx += `${c.name} (numeric): mean=${fmt(c.stats.mean)}, median=${fmt(c.stats.median)}, std=${fmt(c.stats.std)}, min=${fmt(c.stats.min,0)}, max=${fmt(c.stats.max,0)}, skew=${c.stats.skewLabel}, outliers=${c.stats.outlierCount??0}\n`;
  });
  catCols.forEach(c => {
    const top = c.stats.topValues?.slice(0,3).map(([v,n])=>`${v}(${n})`).join(", ") ?? "";
    ctx += `${c.name} (categorical): unique=${c.stats.unique}, top values: ${top}\n`;
  });

  if (mlResult) {
    ctx += `\n=== AUTOML RESULTS ===\n`;
    ctx += `Task: ${mlResult.isClassification ? "Classification" : "Regression"}\n`;
    ctx += `Target column: ${mlResult.targetCol}\n`;
    ctx += `Best model: ${mlResult.bestModel.name}\n`;
    if (mlResult.isClassification) {
      ctx += `Accuracy: ${(mlResult.bestModel.metrics.accuracy*100).toFixed(1)}%\n`;
      ctx += `F1 Score: ${(mlResult.bestModel.metrics.f1*100).toFixed(1)}%\n`;
    } else {
      ctx += `R² Score: ${mlResult.bestModel.metrics.r2.toFixed(3)}\n`;
      ctx += `RMSE: ${mlResult.bestModel.metrics.rmse.toFixed(3)}\n`;
    }
    ctx += `CV Score: ${(mlResult.cvMean*100).toFixed(1)}% ± ${(mlResult.cvStd*100).toFixed(1)}%\n`;
    ctx += `Top features: ${mlResult.featImportance.slice(0,5).map(f=>`${f.col}(${(f.score*100).toFixed(1)}%)`).join(", ")}\n`;
    ctx += `All models (ranked): ${mlResult.results.map((r,i)=>`#${i+1} ${r.name}`).join(", ")}\n`;
  }

  return ctx;
}

// ── Call Anthropic API ────────────────────────────────────────
async function callClaude(systemPrompt, messages, onChunk) {
  const response = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: "claude-sonnet-4-20250514",
      max_tokens: 1000,
      stream: true,
      system: systemPrompt,
      messages,
    }),
  });

  if (!response.ok) {
    const err = await response.json().catch(()=>({}));
    throw new Error(err.error?.message ?? `API error ${response.status}`);
  }

  const reader  = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";
    for (const line of lines) {
      if (!line.startsWith("data: ")) continue;
      const data = line.slice(6).trim();
      if (data === "[DONE]") return;
      try {
        const ev = JSON.parse(data);
        if (ev.type === "content_block_delta" && ev.delta?.type === "text_delta")
          onChunk(ev.delta.text);
      } catch {}
    }
  }
}

// ── Minimal markdown renderer ─────────────────────────────────
function RenderMarkdown({ text }) {
  if (!text) return null;
  const lines = text.split("\n");
  const els = [];
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    if (line.startsWith("### ")) {
      els.push(<div key={i} style={{fontSize:14,fontWeight:700,color:C.orange,
        marginTop:18,marginBottom:6,letterSpacing:"-0.01em"}}>{line.slice(4)}</div>);
    } else if (line.startsWith("## ")) {
      els.push(<div key={i} style={{fontSize:16,fontWeight:700,color:C.text,
        marginTop:22,marginBottom:8,letterSpacing:"-0.02em"}}>{line.slice(3)}</div>);
    } else if (line.startsWith("# ")) {
      els.push(<div key={i} style={{fontSize:18,fontWeight:800,color:C.text,
        marginTop:24,marginBottom:10,letterSpacing:"-0.02em"}}>{line.slice(2)}</div>);
    } else if (line.startsWith("- ") || line.startsWith("* ")) {
      els.push(
        <div key={i} style={{display:"flex",gap:8,marginBottom:5,paddingLeft:8}}>
          <span style={{color:C.orange,flexShrink:0,marginTop:2}}>▸</span>
          <span style={{fontSize:14,color:C.subtle,lineHeight:1.65}}>{renderInline(line.slice(2))}</span>
        </div>
      );
    } else if (/^\d+\. /.test(line)) {
      const num = line.match(/^(\d+)\./)?.[1];
      els.push(
        <div key={i} style={{display:"flex",gap:10,marginBottom:6,paddingLeft:8}}>
          <span style={{color:C.orange,flexShrink:0,fontFamily:"'DM Mono',monospace",
            fontSize:12,marginTop:2,minWidth:18}}>{num}.</span>
          <span style={{fontSize:14,color:C.subtle,lineHeight:1.65}}>{renderInline(line.replace(/^\d+\. /,""))}</span>
        </div>
      );
    } else if (line.startsWith("**") && line.endsWith("**") && line.length > 4) {
      els.push(<div key={i} style={{fontSize:14,fontWeight:700,color:C.text,
        marginTop:12,marginBottom:4}}>{line.slice(2,-2)}</div>);
    } else if (line.trim() === "" || line.trim() === "---") {
      els.push(<div key={i} style={{height:10}}/>);
    } else if (line.trim()) {
      els.push(<p key={i} style={{fontSize:14,color:C.subtle,lineHeight:1.7,
        marginBottom:8}}>{renderInline(line)}</p>);
    }
    i++;
  }
  return <div>{els}</div>;
}

function renderInline(text) {
  // Bold **text** and `code`
  const parts = text.split(/(\*\*[^*]+\*\*|`[^`]+`)/g);
  return parts.map((p, i) => {
    if (p.startsWith("**") && p.endsWith("**"))
      return <strong key={i} style={{color:C.text,fontWeight:700}}>{p.slice(2,-2)}</strong>;
    if (p.startsWith("`") && p.endsWith("`"))
      return <code key={i} style={{fontFamily:"'DM Mono',monospace",fontSize:12,
        background:"rgba(255,255,255,0.06)",borderRadius:4,padding:"1px 5px",
        color:C.green}}>{p.slice(1,-1)}</code>;
    return p;
  });
}

// ── Single insight panel ──────────────────────────────────────
function InsightPanel({ title, icon, color, systemPrompt, userPrompt, context }) {
  const [text,     setText]     = useState("");
  const [loading,  setLoading]  = useState(false);
  const [error,    setError]    = useState(null);
  const [copied,   setCopied]   = useState(false);
  const abortRef = useRef(false);

  const generate = async () => {
    abortRef.current = false;
    setText("");
    setError(null);
    setLoading(true);
    try {
      const sys = `${systemPrompt}\n\n${context}`;
      await callClaude(sys, [{ role:"user", content: userPrompt }], chunk => {
        if (!abortRef.current) setText(t => t + chunk);
      });
    } catch(e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const copy = () => {
    navigator.clipboard.writeText(text).then(()=>{
      setCopied(true);
      setTimeout(()=>setCopied(false), 1800);
    });
  };

  return (
    <div style={{animation:"fadeIn 0.35s ease"}}>
      <div style={{display:"flex",alignItems:"center",
        justifyContent:"space-between",marginBottom:20,flexWrap:"wrap",gap:12}}>
        <div style={{display:"flex",alignItems:"center",gap:10}}>
          <div style={{width:36,height:36,borderRadius:10,
            background:`${color}15`,border:`1px solid ${color}30`,
            display:"flex",alignItems:"center",justifyContent:"center",fontSize:18}}>
            {icon}
          </div>
          <div>
            <div style={{fontSize:15,fontWeight:700,color:C.text}}>{title}</div>
            <div style={{fontSize:11,color:C.muted}}>Powered by Claude Sonnet</div>
          </div>
        </div>
        <div style={{display:"flex",gap:8}}>
          {text && (
            <button onClick={copy} style={{
              padding:"7px 14px",borderRadius:7,fontSize:12,cursor:"pointer",
              background:copied?"rgba(110,231,183,0.1)":"rgba(255,255,255,0.04)",
              border:`1px solid ${copied?C.green:C.border}`,
              color:copied?C.green:C.muted,transition:"all 0.2s"}}>
              {copied ? "✓ Copied" : "⎘ Copy"}
            </button>
          )}
          <button onClick={generate} disabled={loading} style={{
            padding:"7px 18px",borderRadius:7,fontSize:13,fontWeight:600,cursor:"pointer",
            background:loading?"rgba(255,255,255,0.04)":`linear-gradient(135deg,${color},${color}bb)`,
            border:"none",color:loading?C.muted:"#fff",
            opacity:loading?0.7:1,transition:"all 0.2s",
            boxShadow:loading?"none":`0 0 16px ${color}44`}}>
            {loading ? "⟳ Generating…" : text ? "↺ Regenerate" : "✦ Generate"}
          </button>
        </div>
      </div>

      {error && (
        <div style={{padding:"12px 16px",borderRadius:10,marginBottom:16,
          background:"rgba(252,165,165,0.08)",border:"1px solid rgba(252,165,165,0.2)",
          color:C.red,fontSize:13}}>
          ⚠ {error}
        </div>
      )}

      {loading && !text && (
        <div style={{padding:"40px 0",textAlign:"center"}}>
          <div style={{display:"flex",alignItems:"center",justifyContent:"center",gap:8,
            color:C.muted,fontSize:13}}>
            <div style={{width:16,height:16,borderRadius:"50%",
              border:`2px solid ${color}`,borderTopColor:"transparent",
              animation:"spin 0.8s linear infinite"}}/>
            Claude is thinking…
          </div>
        </div>
      )}

      {text && (
        <div style={{padding:"20px 24px",borderRadius:14,
          background:C.surface,border:`1px solid ${C.border}`,
          minHeight:120,animation:"fadeIn 0.3s ease"}}>
          <RenderMarkdown text={text}/>
          {loading && (
            <span style={{display:"inline-block",width:8,height:14,
              background:color,borderRadius:2,marginLeft:2,
              animation:"pulse 0.7s infinite"}}/>
          )}
        </div>
      )}

      {!text && !loading && (
        <div style={{padding:"60px 24px",borderRadius:14,textAlign:"center",
          background:C.surface,border:`1px solid ${C.border}`}}>
          <div style={{fontSize:36,marginBottom:12,opacity:0.3}}>{icon}</div>
          <p style={{fontSize:13,color:C.muted,marginBottom:6}}>
            Click <strong style={{color:C.text}}>Generate</strong> to get AI-powered insights
          </p>
          <p style={{fontSize:12,color:"rgba(100,116,139,0.5)"}}>
            Claude will analyse your data pipeline end-to-end
          </p>
        </div>
      )}
    </div>
  );
}

// ── Chat panel ────────────────────────────────────────────────
function ChatPanel({ context }) {
  const [messages,  setMessages]  = useState([]);
  const [input,     setInput]     = useState("");
  const [loading,   setLoading]   = useState(false);
  const bottomRef = useRef(null);

  useEffect(()=>{
    bottomRef.current?.scrollIntoView({ behavior:"smooth" });
  },[messages, loading]);

  const send = async () => {
    const q = input.trim();
    if (!q || loading) return;
    setInput("");
    const newMsgs = [...messages, { role:"user", content:q }];
    setMessages(newMsgs);
    setLoading(true);
    let reply = "";
    setMessages(m=>[...m, { role:"assistant", content:"" }]);
    try {
      const sys = `You are an expert data scientist assistant. Answer questions about the user's dataset concisely and clearly. Use markdown formatting.\n\n${context}`;
      await callClaude(sys, newMsgs, chunk => {
        reply += chunk;
        setMessages(m=>[...m.slice(0,-1), { role:"assistant", content:reply }]);
      });
    } catch(e) {
      setMessages(m=>[...m.slice(0,-1), { role:"assistant", content:`⚠ Error: ${e.message}` }]);
    } finally {
      setLoading(false);
    }
  };

  const STARTERS = [
    "What are the key patterns in this dataset?",
    "Which features are most predictive?",
    "What data quality issues remain?",
    "How can I improve the model accuracy?",
    "What business decisions can I make from this data?",
  ];

  return (
    <div style={{display:"flex",flexDirection:"column",height:"60vh",minHeight:400}}>
      {/* Messages */}
      <div style={{flex:1,overflowY:"auto",display:"flex",flexDirection:"column",gap:12,
        padding:"8px 0",marginBottom:16}}>
        {messages.length===0 && (
          <div style={{flex:1,display:"flex",flexDirection:"column",
            alignItems:"center",justifyContent:"center",gap:16,padding:"40px 0"}}>
            <div style={{fontSize:36,opacity:0.3}}>💬</div>
            <p style={{fontSize:13,color:C.muted,textAlign:"center"}}>
              Ask anything about your dataset, cleaning results, or ML model
            </p>
            <div style={{display:"flex",flexWrap:"wrap",gap:8,justifyContent:"center",
              maxWidth:560}}>
              {STARTERS.map((s,i)=>(
                <button key={i} onClick={()=>setInput(s)} style={{
                  padding:"7px 12px",borderRadius:8,fontSize:12,cursor:"pointer",
                  background:"rgba(253,186,116,0.08)",
                  border:"1px solid rgba(253,186,116,0.2)",
                  color:C.orange,transition:"all 0.15s"}}>
                  {s}
                </button>
              ))}
            </div>
          </div>
        )}
        {messages.map((msg,i)=>(
          <div key={i} style={{display:"flex",
            justifyContent:msg.role==="user"?"flex-end":"flex-start",
            animation:"fadeIn 0.2s ease"}}>
            {msg.role==="assistant" && (
              <div style={{width:28,height:28,borderRadius:8,flexShrink:0,
                background:"linear-gradient(135deg,#6EE7B7,#3B82F6)",
                display:"flex",alignItems:"center",justifyContent:"center",
                fontSize:13,fontWeight:800,marginRight:10,marginTop:2}}>A</div>
            )}
            <div style={{maxWidth:"72%",padding:"12px 16px",borderRadius:12,
              background:msg.role==="user"
                ?"rgba(253,186,116,0.12)":C.surface,
              border:`1px solid ${msg.role==="user"?"rgba(253,186,116,0.2)":C.border}`,
              borderBottomRightRadius:msg.role==="user"?2:12,
              borderBottomLeftRadius:msg.role==="assistant"?2:12}}>
              {msg.role==="user"
                ? <p style={{fontSize:13,color:C.text,margin:0}}>{msg.content}</p>
                : <RenderMarkdown text={msg.content}/>
              }
              {msg.role==="assistant"&&loading&&i===messages.length-1&&!msg.content&&(
                <div style={{display:"flex",gap:4,padding:"4px 0"}}>
                  {[0,1,2].map(d=>(
                    <div key={d} style={{width:6,height:6,borderRadius:"50%",
                      background:C.orange,
                      animation:`pulse 1s infinite ${d*0.2}s`}}/>
                  ))}
                </div>
              )}
            </div>
          </div>
        ))}
        <div ref={bottomRef}/>
      </div>

      {/* Input */}
      <div style={{display:"flex",gap:10,padding:"14px 16px",
        background:C.surface,borderRadius:12,border:`1px solid ${C.border}`}}>
        <input
          value={input}
          onChange={e=>setInput(e.target.value)}
          onKeyDown={e=>e.key==="Enter"&&!e.shiftKey&&send()}
          placeholder="Ask anything about your data…"
          style={{flex:1,background:"transparent",border:"none",outline:"none",
            fontSize:14,color:C.text,fontFamily:"'DM Sans',sans-serif"}}
        />
        <button onClick={send} disabled={!input.trim()||loading} style={{
          padding:"8px 18px",borderRadius:8,fontSize:13,fontWeight:600,cursor:"pointer",
          background:!input.trim()||loading?"rgba(255,255,255,0.04)"
            :"linear-gradient(135deg,#FDBA74,#F97316)",
          border:"none",color:!input.trim()||loading?C.muted:"#1C1400",
          transition:"all 0.2s",opacity:!input.trim()||loading?0.5:1}}>
          Send ↑
        </button>
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════════════════════
// SECTION 8b — MODULE 5 MAIN COMPONENT
// ════════════════════════════════════════════════════════════════

function Module5({ dataset, cleanResult, mlResult }) {
  const [activeTab, setActiveTab] = useState("summary");
  const context = buildDataContext(dataset, cleanResult, mlResult);
  const profile = cleanResult?.afterProfile ?? dataset?.profile;

  if (!profile) return (
    <div style={{textAlign:"center",padding:"80px 0",color:C.muted}}>
      Complete Modules 1–4 first.
    </div>
  );

  const hasMl = !!mlResult;

  const TABS = [
    { id:"summary",  icon:"📋", label:"Dataset Summary",  color:C.orange  },
    { id:"features", icon:"🔍", label:"Feature Analysis",  color:C.blue   },
    { id:"model",    icon:"🤖", label:"Model Insights",    color:C.yellow,  disabled:!hasMl },
    { id:"business", icon:"💼", label:"Business Insights", color:C.green  },
    { id:"chat",     icon:"💬", label:"Ask Anything",      color:"#F9A8D4" },
  ];

  const PANEL_CONFIGS = {
    summary: {
      title: "Dataset Summary",
      icon: "📋",
      color: C.orange,
      systemPrompt: `You are an expert data scientist writing a clear, insightful report about a dataset. Be analytical, precise, and use markdown formatting with headers and bullet points. Focus on what's interesting and actionable. Keep it under 400 words.`,
      userPrompt: `Write a comprehensive dataset summary. Cover: overall shape and quality, the most interesting columns, key statistics worth noting, data quality after cleaning, and what kinds of analyses this data is suited for.`,
    },
    features: {
      title: "Feature Analysis",
      icon: "🔍",
      color: C.blue,
      systemPrompt: `You are an expert data scientist explaining feature relationships and patterns. Be specific, cite actual column names and values from the data. Use markdown with clear sections. Under 500 words.`,
      userPrompt: `Analyse the features in this dataset. Discuss: which numeric columns have the most interesting distributions, any significant correlations between variables, categorical breakdowns worth noting, and which features would be most useful for machine learning.`,
    },
    model: {
      title: "Model Insights",
      icon: "🤖",
      color: C.yellow,
      systemPrompt: `You are an expert ML engineer interpreting AutoML results. Be specific about the numbers. Explain what the metrics mean in plain language. Use markdown. Under 500 words.`,
      userPrompt: `Interpret these AutoML results. Explain: why the best model performed as it did, what the accuracy/R² score means in practical terms, what the top features tell us, whether the model is good enough for production use, and concrete recommendations to improve it further.`,
    },
    business: {
      title: "Business Insights",
      icon: "💼",
      color: C.green,
      systemPrompt: `You are a business analyst translating data science findings into actionable business recommendations. Be concrete, practical, and executive-friendly. Use markdown. Under 500 words.`,
      userPrompt: `Based on this dataset and analysis, generate 5–7 concrete, actionable business insights and recommendations. For each insight, explain the evidence from the data and the recommended action. Focus on decisions that could drive real business value.`,
    },
  };

  return (
    <div style={{animation:"fadeIn 0.4s ease"}}>
      {/* Context pill */}
      <div style={{display:"flex",alignItems:"center",gap:12,padding:"12px 18px",
        borderRadius:12,background:"rgba(253,186,116,0.07)",
        border:"1px solid rgba(253,186,116,0.15)",marginBottom:24,flexWrap:"wrap"}}>
        <span style={{fontSize:16}}>✦</span>
        <span style={{fontSize:13,color:C.text,fontWeight:500}}>
          Claude has full context of your pipeline
        </span>
        <div style={{height:14,width:1,background:C.border}}/>
        <div style={{display:"flex",gap:8,flexWrap:"wrap"}}>
          <Tag color={C.green}>✓ Dataset ({fmtK(cleanResult?.cleanedRows?.length ?? dataset?.rows?.length ?? 0)} rows)</Tag>
          <Tag color={cleanResult?C.green:C.muted}>{cleanResult?"✓ Cleaned":"○ Raw data"}</Tag>
          <Tag color={mlResult?C.yellow:C.muted}>{mlResult?`✓ ${mlResult.bestModel.name}`:"○ No ML yet"}</Tag>
        </div>
      </div>

      {/* Tabs */}
      <div style={{display:"flex",gap:4,marginBottom:24,borderBottom:`1px solid ${C.border}`,
        overflowX:"auto"}}>
        {TABS.map(t=>(
          <button key={t.id}
            onClick={()=>!t.disabled&&setActiveTab(t.id)}
            title={t.disabled?"Run AutoML (Module 4) first":""}
            style={{
              padding:"9px 16px",borderRadius:"8px 8px 0 0",cursor:t.disabled?"not-allowed":"pointer",
              background:activeTab===t.id?"rgba(255,255,255,0.05)":"transparent",
              border:activeTab===t.id?`1px solid ${C.border}`:"1px solid transparent",
              borderBottom:activeTab===t.id?`1px solid ${C.bg}`:"none",
              color:activeTab===t.id?C.text:t.disabled?"rgba(100,116,139,0.3)":C.muted,
              fontSize:13,fontWeight:activeTab===t.id?600:400,
              marginBottom:activeTab===t.id?-1:0,flexShrink:0,
              display:"flex",alignItems:"center",gap:6,
              opacity:t.disabled?0.4:1}}>
            <span>{t.icon}</span>
            <span>{t.label}</span>
            {t.disabled && <span style={{fontSize:10,opacity:0.5}}>🔒</span>}
          </button>
        ))}
      </div>

      {/* Panel content */}
      {activeTab==="chat" ? (
        <ChatPanel context={context}/>
      ) : PANEL_CONFIGS[activeTab] ? (
        <InsightPanel
          key={activeTab}
          title={PANEL_CONFIGS[activeTab].title}
          icon={PANEL_CONFIGS[activeTab].icon}
          color={PANEL_CONFIGS[activeTab].color}
          systemPrompt={PANEL_CONFIGS[activeTab].systemPrompt}
          userPrompt={PANEL_CONFIGS[activeTab].userPrompt}
          context={context}
        />
      ) : null}
    </div>
  );
}

// ════════════════════════════════════════════════════════════════
// SECTION 9 — MODULE 6: CHAT INTERFACE
// ════════════════════════════════════════════════════════════════

const M6_COLOR = "#F9A8D4";

const SLASH_COMMANDS = [
  { cmd:"/profile",  icon:"🔬", label:"Dataset Profile",   prompt:"Give me a comprehensive summary of this dataset — shape, types, quality, and the most interesting patterns." },
  { cmd:"/clean",    icon:"🧹", label:"Clean Summary",      prompt:"Summarise what happened during data cleaning — what was fixed, what was dropped, and how quality improved." },
  { cmd:"/model",    icon:"🤖", label:"Model Results",      prompt:"Explain my AutoML results in detail — why the best model won, what the metrics mean, and how to improve it." },
  { cmd:"/features", icon:"▦",  label:"Feature Importance", prompt:"Which features are most important and why? Explain the relationships and what they mean for prediction." },
  { cmd:"/insights", icon:"💡", label:"Key Insights",       prompt:"What are the top 5 most actionable insights from this entire data analysis pipeline?" },
  { cmd:"/help",     icon:"?",  label:"Show Commands",      prompt:null },
];

function deriveTitle(text) {
  const clean = text.replace(/^\/\w+\s*/,"").trim();
  return clean.length > 42 ? clean.slice(0,42)+"…" : clean || "New conversation";
}

function fmtTime(ts) {
  return new Date(ts).toLocaleTimeString([],{hour:"2-digit",minute:"2-digit"});
}

function ChatBubble({ msg, isLast, isStreaming }) {
  const isUser = msg.role === "user";
  return (
    <div style={{display:"flex",gap:12,justifyContent:isUser?"flex-end":"flex-start",
      animation:"fadeIn 0.2s ease",marginBottom:4}}>
      {!isUser && (
        <div style={{width:30,height:30,borderRadius:9,flexShrink:0,marginTop:2,
          background:"linear-gradient(135deg,#F9A8D4,#C084FC)",
          display:"flex",alignItems:"center",justifyContent:"center",
          fontSize:14,fontWeight:800,color:"#fff"}}>A</div>
      )}
      <div style={{maxWidth:"75%",display:"flex",flexDirection:"column",
        alignItems:isUser?"flex-end":"flex-start",gap:4}}>
        <div style={{padding:"12px 16px",borderRadius:14,
          borderBottomRightRadius:isUser?3:14,borderBottomLeftRadius:isUser?14:3,
          background:isUser
            ?"linear-gradient(135deg,rgba(249,168,212,0.18),rgba(192,132,252,0.12))"
            :C.surface,
          border:`1px solid ${isUser?"rgba(249,168,212,0.25)":C.border}`}}>
          {isUser
            ? <p style={{fontSize:14,color:C.text,margin:0,lineHeight:1.6}}>{msg.content}</p>
            : <RenderMarkdown text={msg.content}/>
          }
          {isStreaming&&isLast&&!isUser&&(
            <span style={{display:"inline-block",width:8,height:14,
              background:M6_COLOR,borderRadius:2,marginLeft:2,
              animation:"pulse 0.7s infinite"}}/>
          )}
        </div>
        <span style={{fontSize:10,color:"rgba(100,116,139,0.5)"}}>{fmtTime(msg.ts)}</span>
      </div>
      {isUser && (
        <div style={{width:30,height:30,borderRadius:9,flexShrink:0,marginTop:2,
          background:"rgba(249,168,212,0.15)",border:"1px solid rgba(249,168,212,0.25)",
          display:"flex",alignItems:"center",justifyContent:"center",
          fontSize:13,color:M6_COLOR}}>U</div>
      )}
    </div>
  );
}

const CHAT_STARTERS = [
  "Summarise this dataset in 3 bullet points",
  "What's the strongest correlation in my data?",
  "Is my ML model production-ready?",
  "What business decision should I make first?",
  "Which column has the biggest quality issue?",
  "Explain the feature importance ranking",
];

const FOLLOWUPS = [
  "What's the most surprising finding?",
  "How would you improve this analysis?",
  "What additional data would help?",
  "Is this dataset ready for production ML?",
  "Which column needs most attention?",
];

function Module6({ dataset, cleanResult, mlResult }) {
  const context = buildDataContext(dataset, cleanResult, mlResult);
  const profile = cleanResult?.afterProfile ?? dataset?.profile;

  const [convs,       setConvs]      = useState([]);
  const [activeId,    setActiveId]   = useState(null);
  const [input,       setInput]      = useState("");
  const [streaming,   setStreaming]  = useState(false);
  const [showCtx,     setShowCtx]    = useState(false);
  const [sidebarOpen, setSidebarOpen]= useState(true);
  const inputRef  = useRef(null);
  const bottomRef = useRef(null);

  const activeConv = convs.find(c=>c.id===activeId) ?? null;
  const messages   = activeConv?.messages ?? [];

  useEffect(()=>{ bottomRef.current?.scrollIntoView({behavior:"smooth"}); },[messages,streaming]);
  useEffect(()=>{ inputRef.current?.focus(); },[]);

  const newConv = (firstMsg) => {
    const id = `conv_${Date.now()}`;
    const conv = { id, title:deriveTitle(firstMsg??"New chat"), messages:[], createdAt:Date.now() };
    setConvs(c=>[conv,...c]);
    setActiveId(id);
    return id;
  };

  const send = async (text) => {
    const q = (text??input).trim();
    if (!q||streaming) return;
    setInput("");

    if (q==="/help") {
      const helpId = activeId ?? newConv(q);
      const helpText = "## Available Commands\n\n" +
        SLASH_COMMANDS.filter(c=>c.cmd!=="/help").map(c=>`- \`${c.cmd}\` — ${c.label}`).join("\n") +
        "\n\nOr just type any question about your data!";
      setConvs(cs=>cs.map(c=> c.id===helpId
        ? {...c, messages:[...c.messages,
            {role:"user",content:q,ts:Date.now()},
            {role:"assistant",content:helpText,ts:Date.now()}]}
        : c));
      return;
    }

    const slash  = SLASH_COMMANDS.find(sc=>q.startsWith(sc.cmd)&&sc.prompt);
    const realQ  = slash ? slash.prompt : q;
    let convId   = activeId;
    let isNew    = false;
    if (!convId||messages.length===0) { convId=newConv(q); isNew=true; }

    const userMsg = {role:"user",content:q,ts:Date.now()};
    setConvs(cs=>cs.map(c=> c.id===convId
      ? {...c, title:isNew?deriveTitle(q):c.title, messages:[...c.messages,userMsg]}
      : c));

    const history = [
      ...messages.map(m=>({role:m.role,
        content:m.role==="user"?(SLASH_COMMANDS.find(sc=>m.content.startsWith(sc.cmd))?.prompt??m.content):m.content})),
      {role:"user",content:realQ}
    ];

    setStreaming(true);
    let reply="";
    const asstMsg = {role:"assistant",content:"",ts:Date.now()};
    setConvs(cs=>cs.map(c=> c.id===convId?{...c,messages:[...c.messages,asstMsg]}:c));

    try {
      const sys = `You are an expert data scientist assistant embedded in a data analysis platform. Answer concisely and precisely. Use markdown formatting. Cite actual column names and numbers.\n\n${context}`;
      await callClaude(sys, history, chunk=>{
        reply+=chunk;
        setConvs(cs=>cs.map(c=> c.id===convId
          ? {...c,messages:[...c.messages.slice(0,-1),{...asstMsg,content:reply}]}
          : c));
      });
    } catch(e) {
      reply=`⚠ Error: ${e.message}`;
      setConvs(cs=>cs.map(c=> c.id===convId
        ? {...c,messages:[...c.messages.slice(0,-1),{...asstMsg,content:reply}]}
        : c));
    } finally { setStreaming(false); }
  };

  const exportConv = () => {
    if (!activeConv) return;
    const txt = activeConv.messages.map(m=>`[${m.role.toUpperCase()}] ${fmtTime(m.ts)}\n${m.content}`).join("\n\n---\n\n");
    const blob = new Blob([txt],{type:"text/plain"});
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement("a");
    a.href=url; a.download=`chat_${activeConv.id}.txt`; a.click();
    URL.revokeObjectURL(url);
  };

  const filteredSlash = input.startsWith("/")
    ? SLASH_COMMANDS.filter(c=>c.cmd.startsWith(input.split(" ")[0])) : [];

  if (!profile) return (
    <div style={{textAlign:"center",padding:"80px 0",color:C.muted}}>Complete Modules 1–5 first.</div>
  );

  return (
    <div style={{display:"flex",height:"78vh",minHeight:520,borderRadius:16,
      border:`1px solid ${C.border}`,overflow:"hidden",animation:"fadeIn 0.4s ease"}}>

      {/* ── SIDEBAR ── */}
      {sidebarOpen && (
        <div style={{width:268,flexShrink:0,borderRight:`1px solid ${C.border}`,
          background:"rgba(255,255,255,0.015)",display:"flex",flexDirection:"column"}}>

          {/* Header */}
          <div style={{padding:"14px 16px",borderBottom:`1px solid ${C.border}`,
            display:"flex",alignItems:"center",justifyContent:"space-between"}}>
            <span style={{fontSize:13,fontWeight:600,color:C.text}}>Conversations</span>
            <div style={{display:"flex",gap:6}}>
              <button onClick={()=>{setActiveId(null);setInput("");}} style={{
                padding:"5px 10px",borderRadius:7,fontSize:12,cursor:"pointer",
                background:`${M6_COLOR}18`,border:`1px solid ${M6_COLOR}30`,
                color:M6_COLOR,fontWeight:600}}>+ New</button>
              <button onClick={()=>setSidebarOpen(false)} style={{
                padding:"5px 8px",borderRadius:7,fontSize:12,cursor:"pointer",
                background:"transparent",border:`1px solid ${C.border}`,color:C.muted}}>←</button>
            </div>
          </div>

          {/* Conv list */}
          <div style={{flex:1,overflowY:"auto",padding:"8px"}}>
            {convs.length===0 && (
              <div style={{padding:"20px 12px",textAlign:"center",color:C.muted,fontSize:12}}>
                No conversations yet.<br/>Ask your first question below.
              </div>
            )}
            {convs.map(c=>(
              <button key={c.id} onClick={()=>setActiveId(c.id)} style={{
                width:"100%",textAlign:"left",padding:"10px 12px",borderRadius:9,
                cursor:"pointer",marginBottom:3,
                background:c.id===activeId?`${M6_COLOR}12`:"transparent",
                border:`1px solid ${c.id===activeId?M6_COLOR+"30":C.border}`,
                color:c.id===activeId?C.text:C.subtle,transition:"all 0.15s"}}>
                <div style={{fontSize:12,fontWeight:c.id===activeId?600:400,
                  overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",
                  marginBottom:3}}>{c.title}</div>
                <div style={{fontSize:10,color:C.muted}}>
                  {c.messages.length} msgs · {fmtTime(c.createdAt)}
                </div>
              </button>
            ))}
          </div>

          {/* Context toggle */}
          <div style={{borderTop:`1px solid ${C.border}`}}>
            <button onClick={()=>setShowCtx(v=>!v)} style={{
              width:"100%",padding:"12px 16px",cursor:"pointer",
              background:"transparent",border:"none",
              display:"flex",alignItems:"center",justifyContent:"space-between",
              color:C.muted,fontSize:12}}>
              <span>📊 Data Context</span>
              <span style={{fontSize:10}}>{showCtx?"▲":"▼"}</span>
            </button>
            {showCtx && (
              <div style={{padding:"0 12px 12px",maxHeight:180,overflowY:"auto"}}>
                <pre style={{fontSize:9.5,color:"rgba(148,163,184,0.6)",
                  fontFamily:"'DM Mono',monospace",whiteSpace:"pre-wrap",lineHeight:1.5}}>
                  {context.slice(0,800)}{context.length>800?"…":""}
                </pre>
              </div>
            )}
          </div>

          {/* Slash commands */}
          <div style={{borderTop:`1px solid ${C.border}`,padding:"12px"}}>
            <div style={{fontSize:10,color:C.muted,textTransform:"uppercase",
              letterSpacing:"0.08em",marginBottom:8}}>Quick Commands</div>
            {SLASH_COMMANDS.filter(c=>c.cmd!=="/help").map(c=>(
              <button key={c.cmd} onClick={()=>{setInput(c.cmd);inputRef.current?.focus();}}
                style={{display:"flex",alignItems:"center",gap:8,width:"100%",
                  padding:"6px 8px",borderRadius:7,cursor:"pointer",
                  background:"transparent",border:"none",color:C.muted,
                  fontSize:12,textAlign:"left",transition:"all 0.15s"}}>
                <span>{c.icon}</span>
                <span style={{fontFamily:"'DM Mono',monospace",color:M6_COLOR}}>{c.cmd}</span>
                <span style={{fontSize:11}}>{c.label}</span>
              </button>
            ))}
          </div>
        </div>
      )}

      {/* ── MAIN CHAT ── */}
      <div style={{flex:1,display:"flex",flexDirection:"column",minWidth:0}}>

        {/* Chat header */}
        <div style={{padding:"12px 18px",borderBottom:`1px solid ${C.border}`,
          display:"flex",alignItems:"center",gap:12,flexShrink:0}}>
          {!sidebarOpen && (
            <button onClick={()=>setSidebarOpen(true)} style={{
              padding:"5px 8px",borderRadius:7,fontSize:12,cursor:"pointer",
              background:"transparent",border:`1px solid ${C.border}`,color:C.muted}}>→</button>
          )}
          <div style={{flex:1,minWidth:0}}>
            <div style={{fontSize:14,fontWeight:600,color:C.text,
              overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>
              {activeConv?.title ?? "New Conversation"}
            </div>
            <div style={{fontSize:11,color:C.muted}}>
              Claude has full context of your {dataset?.fileName??"dataset"}
            </div>
          </div>
          <div style={{display:"flex",gap:8}}>
            {activeConv && (
              <button onClick={exportConv} style={{
                padding:"5px 12px",borderRadius:7,fontSize:12,cursor:"pointer",
                background:"rgba(255,255,255,0.04)",border:`1px solid ${C.border}`,
                color:C.muted}}>⬇ Export</button>
            )}
            {activeConv && (
              <button onClick={()=>{setConvs(cs=>cs.filter(c=>c.id!==activeId));setActiveId(null);}}
                style={{padding:"5px 12px",borderRadius:7,fontSize:12,cursor:"pointer",
                  background:"rgba(252,165,165,0.07)",border:"1px solid rgba(252,165,165,0.15)",
                  color:C.red}}>🗑</button>
            )}
          </div>
        </div>

        {/* Messages */}
        <div style={{flex:1,overflowY:"auto",padding:"20px 20px 8px"}}>
          {messages.length===0 && (
            <div style={{display:"flex",flexDirection:"column",alignItems:"center",
              justifyContent:"center",height:"100%",gap:20,paddingBottom:40}}>
              <div style={{width:60,height:60,borderRadius:16,
                background:"linear-gradient(135deg,rgba(249,168,212,0.2),rgba(192,132,252,0.2))",
                border:`1px solid ${M6_COLOR}30`,
                display:"flex",alignItems:"center",justifyContent:"center",fontSize:28}}>💬</div>
              <div style={{textAlign:"center"}}>
                <div style={{fontSize:16,fontWeight:600,color:C.text,marginBottom:6}}>
                  Ask anything about your data
                </div>
                <div style={{fontSize:13,color:C.muted,maxWidth:400}}>
                  Full pipeline context loaded — profiling, cleaning, EDA, and ML results.
                </div>
              </div>
              <div style={{display:"flex",flexWrap:"wrap",gap:8,
                justifyContent:"center",maxWidth:560}}>
                {CHAT_STARTERS.map((s,i)=>(
                  <button key={i} onClick={()=>send(s)} style={{
                    padding:"8px 14px",borderRadius:20,fontSize:12,cursor:"pointer",
                    background:`${M6_COLOR}0D`,border:`1px solid ${M6_COLOR}25`,
                    color:M6_COLOR,transition:"all 0.15s"}}>{s}</button>
                ))}
              </div>
            </div>
          )}
          {messages.map((msg,i)=>(
            <ChatBubble key={i} msg={msg} isLast={i===messages.length-1} isStreaming={streaming}/>
          ))}
          {!streaming && messages.length>0 && messages[messages.length-1].role==="assistant" && (
            <div style={{display:"flex",flexWrap:"wrap",gap:6,padding:"8px 42px",
              animation:"fadeIn 0.4s ease"}}>
              {FOLLOWUPS.slice(0,3).map((s,i)=>(
                <button key={i} onClick={()=>send(s)} style={{
                  padding:"5px 11px",borderRadius:20,fontSize:11,cursor:"pointer",
                  background:"rgba(255,255,255,0.04)",border:`1px solid ${C.border}`,
                  color:C.muted,transition:"all 0.15s"}}>{s} →</button>
              ))}
            </div>
          )}
          <div ref={bottomRef}/>
        </div>

        {/* Slash autocomplete */}
        {filteredSlash.length>0 && (
          <div style={{marginLeft:20,marginRight:20,marginBottom:4,borderRadius:10,
            border:`1px solid ${C.border}`,background:"rgba(15,23,42,0.97)",
            overflow:"hidden",boxShadow:"0 -8px 24px rgba(0,0,0,0.4)"}}>
            {filteredSlash.map((c,i)=>(
              <button key={c.cmd} onClick={()=>{setInput(c.cmd);inputRef.current?.focus();}} style={{
                display:"flex",alignItems:"center",gap:10,width:"100%",
                padding:"10px 16px",cursor:"pointer",background:"transparent",
                border:"none",borderBottom:i<filteredSlash.length-1?`1px solid ${C.border}`:"none",
                color:C.text,textAlign:"left"}}>
                <span style={{fontSize:16}}>{c.icon}</span>
                <span style={{fontFamily:"'DM Mono',monospace",color:M6_COLOR,
                  fontSize:13,minWidth:90}}>{c.cmd}</span>
                <span style={{fontSize:12,color:C.muted}}>{c.label}</span>
              </button>
            ))}
          </div>
        )}

        {/* Input bar */}
        <div style={{margin:"0 16px 16px",padding:"10px 14px",borderRadius:12,
          border:`1px solid ${streaming?"rgba(249,168,212,0.3)":C.border}`,
          background:"rgba(255,255,255,0.03)",display:"flex",alignItems:"center",gap:10,
          transition:"border-color 0.2s",
          boxShadow:streaming?`0 0 12px rgba(249,168,212,0.1)`:"none"}}>
          <span style={{fontSize:16,opacity:0.4}}>💬</span>
          <input ref={inputRef} value={input}
            onChange={e=>setInput(e.target.value)}
            onKeyDown={e=>{ if(e.key==="Enter"&&!e.shiftKey){e.preventDefault();send();} if(e.key==="Escape")setInput(""); }}
            placeholder={streaming?"Claude is thinking…":"Ask anything, or type / for commands"}
            disabled={streaming}
            style={{flex:1,background:"transparent",border:"none",outline:"none",
              fontSize:14,color:streaming?C.muted:C.text,fontFamily:"'DM Sans',sans-serif"}}/>
          {streaming ? (
            <div style={{display:"flex",gap:3,alignItems:"center"}}>
              {[0,1,2].map(d=>(
                <div key={d} style={{width:5,height:5,borderRadius:"50%",
                  background:M6_COLOR,animation:`pulse 1s infinite ${d*0.2}s`}}/>
              ))}
            </div>
          ) : (
            <button onClick={()=>send()} disabled={!input.trim()} style={{
              padding:"7px 16px",borderRadius:8,fontSize:13,fontWeight:600,
              cursor:input.trim()?"pointer":"default",
              background:input.trim()?"linear-gradient(135deg,#F9A8D4,#C084FC)":"rgba(255,255,255,0.04)",
              border:"none",color:input.trim()?"#1a0a14":C.muted,
              opacity:input.trim()?1:0.4,transition:"all 0.2s"}}>Send ↑</button>
          )}
        </div>
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════════════════════
// SECTION 10 — MODULE 7: REPORTS & EXPORT ENGINE
// ════════════════════════════════════════════════════════════════

// ── Download helper ───────────────────────────────────────────
function downloadBlob(content, filename, mime) {
  const blob = new Blob([content], { type: mime });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement("a");
  a.href = url; a.download = filename; a.click();
  URL.revokeObjectURL(url);
}

// ── Build JSON summary ────────────────────────────────────────
function buildJSONSummary(dataset, cleanResult, mlResult) {
  const profile  = cleanResult?.afterProfile ?? dataset?.profile;
  const rows     = cleanResult?.cleanedRows   ?? dataset?.rows ?? [];
  const numCols  = profile?.columns.filter(c=>c.type==="numeric") ?? [];
  const catCols  = profile?.columns.filter(c=>c.type==="categorical") ?? [];

  return {
    generated: new Date().toISOString(),
    dataset: {
      fileName: dataset?.fileName,
      originalRows: dataset?.rows?.length,
      cleanedRows:  rows.length,
      columns: profile?.colCount,
      qualityScore: profile?.qualityScore,
    },
    cleaning: cleanResult ? {
      rowsDropped:        cleanResult.stats.rowsDropped,
      colsDropped:        cleanResult.stats.colsDropped,
      cellsImputed:       cleanResult.stats.cellsImputed,
      outliersWinsorized: cleanResult.stats.outliersWinsorized,
      duplicatesRemoved:  cleanResult.stats.duplicatesRemoved,
    } : null,
    columns: {
      numeric:     numCols.map(c=>({ name:c.name, mean:c.stats.mean, std:c.stats.std, skew:c.stats.skewLabel })),
      categorical: catCols.map(c=>({ name:c.name, unique:c.stats.unique, top:c.stats.topValues?.[0]?.[0] })),
    },
    autoML: mlResult ? {
      task:      mlResult.isClassification ? "classification" : "regression",
      target:    mlResult.targetCol,
      bestModel: mlResult.bestModel.name,
      metrics:   mlResult.bestModel.metrics,
      cvScore:   { mean:mlResult.cvMean, std:mlResult.cvStd },
      topFeatures: mlResult.featImportance.slice(0,5).map(f=>({ feature:f.col, importance:f.score })),
    } : null,
  };
}

// ── Build Markdown report ─────────────────────────────────────
function buildMarkdownReport(dataset, cleanResult, mlResult, sections) {
  const profile  = cleanResult?.afterProfile ?? dataset?.profile;
  const rows     = cleanResult?.cleanedRows   ?? dataset?.rows ?? [];
  const numCols  = profile?.columns.filter(c=>c.type==="numeric")     ?? [];
  const catCols  = profile?.columns.filter(c=>c.type==="categorical")  ?? [];
  const ts       = new Date().toLocaleString();
  let md = "";

  md += `# Analytica AI Report\n`;
  md += `**Generated:** ${ts}  \n**File:** ${dataset?.fileName ?? "—"}\n\n---\n\n`;

  if (sections.summary) {
    md += `## 1. Executive Summary\n\n`;
    md += `| Metric | Value |\n|---|---|\n`;
    md += `| Dataset | ${dataset?.fileName ?? "—"} |\n`;
    md += `| Rows (cleaned) | ${fmtK(rows.length)} |\n`;
    md += `| Columns | ${profile?.colCount ?? "—"} |\n`;
    md += `| Quality Score | ${profile?.qualityScore ?? "—"}/100 |\n`;
    md += `| Numeric Columns | ${numCols.length} |\n`;
    md += `| Categorical Columns | ${catCols.length} |\n\n`;
  }

  if (sections.profile && profile) {
    md += `## 2. Dataset Profile\n\n`;
    md += `| Column | Type | Missing% | Unique | Outliers |\n|---|---|---|---|---|\n`;
    profile.columns.forEach(c => {
      md += `| ${c.name} | ${c.type} | ${c.stats.missingPct.toFixed(1)}% | ${c.stats.unique} | ${c.stats.outlierCount ?? "—"} |\n`;
    });
    md += "\n";
  }

  if (sections.cleaning && cleanResult) {
    md += `## 3. Data Cleaning Summary\n\n`;
    const s = cleanResult.stats;
    md += `- Rows dropped: **${s.rowsDropped}**\n`;
    md += `- Columns dropped: **${s.colsDropped}**\n`;
    md += `- Cells imputed: **${s.cellsImputed}**\n`;
    md += `- Outliers winsorized: **${s.outliersWinsorized}**\n`;
    md += `- Duplicates removed: **${s.duplicatesRemoved}**\n\n`;
  }

  if (sections.eda && numCols.length) {
    md += `## 4. EDA Highlights\n\n`;
    md += `| Column | Mean | Median | Std Dev | Skew |\n|---|---|---|---|---|\n`;
    numCols.forEach(c => {
      md += `| ${c.name} | ${fmt(c.stats.mean)} | ${fmt(c.stats.median)} | ${fmt(c.stats.std)} | ${c.stats.skewLabel} |\n`;
    });
    md += "\n";
  }

  if (sections.ml && mlResult) {
    md += `## 5. AutoML Results\n\n`;
    md += `**Task:** ${mlResult.isClassification?"Classification":"Regression"}  \n`;
    md += `**Target:** \`${mlResult.targetCol}\`  \n`;
    md += `**Best Model:** ${mlResult.bestModel.name}  \n\n`;
    md += `| Model | Score |\n|---|---|\n`;
    mlResult.results.forEach((r,i) => {
      const score = mlResult.isClassification ? r.metrics.accuracy : r.metrics.r2;
      md += `| ${i===0?"🥇 ":""}${r.name} | ${(score*100).toFixed(1)}% |\n`;
    });
    md += `\n**Top Features:**\n`;
    mlResult.featImportance.slice(0,5).forEach((f,i) => {
      md += `${i+1}. \`${f.col}\` — ${(f.score*100).toFixed(1)}%\n`;
    });
    md += "\n";
  }

  if (sections.recs) {
    md += `## 6. Recommendations\n\n`;
    const recs = buildRecommendations(dataset, cleanResult, mlResult);
    recs.forEach((r,i) => { md += `${i+1}. **${r.title}** — ${r.text}\n`; });
    md += "\n";
  }

  md += `---\n*Generated by Analytica AI*\n`;
  return md;
}

// ── Auto-generate recommendations ────────────────────────────
function buildRecommendations(dataset, cleanResult, mlResult) {
  const profile  = cleanResult?.afterProfile ?? dataset?.profile;
  const numCols  = profile?.columns.filter(c=>c.type==="numeric") ?? [];
  const recs = [];

  // Quality
  const qs = profile?.qualityScore ?? 0;
  recs.push({
    icon:"📊", color:C.green,
    title:"Data Quality",
    text: qs >= 80
      ? `Quality score is ${qs}/100 — dataset is clean and ready for production use.`
      : `Quality score is ${qs}/100. Consider collecting more complete data and revisiting imputation strategy.`
  });

  // Skewed columns
  const skewed = numCols.filter(c=>c.stats.skewLabel!=="Normal");
  if (skewed.length) recs.push({
    icon:"📈", color:C.yellow,
    title:"Transform Skewed Features",
    text:`${skewed.map(c=>c.name).join(", ")} ${skewed.length===1?"is":"are"} skewed. Apply log or Box-Cox transforms before training models to improve performance.`
  });

  // ML improvement
  if (mlResult) {
    const score = mlResult.isClassification ? mlResult.bestModel.metrics.accuracy : mlResult.bestModel.metrics.r2;
    const topFeat = mlResult.featImportance[0];
    recs.push({
      icon:"🤖", color:C.blue,
      title:"Model Improvement",
      text: score < 0.75
        ? `Best model (${mlResult.bestModel.name}) achieved ${(score*100).toFixed(1)}%. Try feature engineering on \`${topFeat?.col}\` and hyperparameter tuning to push past 80%.`
        : `Best model achieved ${(score*100).toFixed(1)}%. Focus on \`${topFeat?.col}\` (most important feature) for further tuning.`
    });

    recs.push({
      icon:"▦", color:C.purple,
      title:"Feature Engineering",
      text:`Top feature \`${topFeat?.col}\` has ${(topFeat?.score*100).toFixed(1)}% importance. Create interaction features between the top-3 columns to capture non-linear relationships.`
    });
  }

  // High cardinality
  const highCard = profile?.columns.filter(c=>c.type==="categorical"&&c.stats.cardinality>90) ?? [];
  if (highCard.length) recs.push({
    icon:"🏷", color:C.orange,
    title:"Encoding Strategy",
    text:`${highCard.map(c=>c.name).join(", ")} ${highCard.length===1?"has":"have"} high cardinality (>90% unique). Use target encoding or frequency encoding instead of one-hot to avoid dimensionality explosion.`
  });

  // Generic if < 5
  if (recs.length < 5) recs.push({
    icon:"🔄", color:C.green,
    title:"Collect More Data",
    text:`With ${fmtK(dataset?.rows?.length??0)} rows, consider collecting more samples. Models typically improve significantly up to 10k rows.`
  });

  return recs.slice(0,6);
}

// ── Build full styled HTML report ────────────────────────────
function buildHTMLReport(dataset, cleanResult, mlResult, sections) {
  const profile  = cleanResult?.afterProfile ?? dataset?.profile;
  const rows     = cleanResult?.cleanedRows   ?? dataset?.rows ?? [];
  const numCols  = profile?.columns.filter(c=>c.type==="numeric")    ?? [];
  const catCols  = profile?.columns.filter(c=>c.type==="categorical") ?? [];
  const ts       = new Date().toLocaleString();
  const recs     = buildRecommendations(dataset, cleanResult, mlResult);

  const css = `
    *{box-sizing:border-box;margin:0;padding:0;}
    body{background:#03060F;color:#E2E8F0;font-family:'Segoe UI',system-ui,sans-serif;
      line-height:1.6;padding:40px 0;}
    .page{max-width:900px;margin:0 auto;padding:0 32px;}
    h1{font-size:28px;font-weight:800;color:#fff;letter-spacing:-0.03em;margin-bottom:6px;}
    h2{font-size:18px;font-weight:700;color:#E2E8F0;margin:36px 0 16px;
      padding-bottom:8px;border-bottom:1px solid rgba(255,255,255,0.07);}
    h3{font-size:14px;font-weight:600;color:#94A3B8;text-transform:uppercase;
      letter-spacing:0.08em;margin:20px 0 10px;}
    p{font-size:14px;color:#94A3B8;margin-bottom:10px;}
    .meta{font-size:12px;color:#475569;margin-bottom:40px;font-family:monospace;}
    .cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px;margin-bottom:24px;}
    .card{background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);
      border-radius:12px;padding:16px;}
    .card-label{font-size:10px;color:#64748B;text-transform:uppercase;letter-spacing:0.1em;margin-bottom:4px;}
    .card-val{font-size:22px;font-weight:800;font-family:monospace;}
    table{width:100%;border-collapse:collapse;font-size:13px;margin-bottom:24px;}
    th{padding:9px 12px;text-align:left;color:#64748B;font-weight:600;font-size:11px;
      text-transform:uppercase;letter-spacing:0.06em;border-bottom:1px solid rgba(255,255,255,0.07);}
    td{padding:9px 12px;border-bottom:1px solid rgba(255,255,255,0.03);color:#CBD5E1;}
    tr:nth-child(even) td{background:rgba(255,255,255,0.015);}
    .badge{display:inline-block;font-size:10px;border-radius:99px;padding:2px 8px;
      font-family:monospace;}
    .rec{background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.07);
      border-radius:12px;padding:16px 18px;margin-bottom:10px;display:flex;gap:14px;align-items:flex-start;}
    .rec-icon{font-size:22px;flex-shrink:0;}
    .rec-title{font-size:13px;font-weight:700;color:#E2E8F0;margin-bottom:4px;}
    .rec-text{font-size:13px;color:#94A3B8;}
    .divider{height:1px;background:rgba(255,255,255,0.06);margin:32px 0;}
    .footer{text-align:center;font-size:12px;color:#334155;margin-top:48px;padding-top:24px;
      border-top:1px solid rgba(255,255,255,0.05);}
    .section{animation:none;}
    .good{color:#6EE7B7;} .warn{color:#FCD34D;} .danger{color:#FCA5A5;}
    .blue{color:#93C5FD;} .purple{color:#C4B5FD;} .orange{color:#FDBA74;}
  `;

  const typeColor = {numeric:"#6EE7B7",categorical:"#93C5FD",datetime:"#FCD34D",boolean:"#F9A8D4",text:"#C4B5FD",empty:"#64748B"};

  let html = `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<title>Analytica AI Report — ${dataset?.fileName ?? "Dataset"}</title>
<style>${css}</style></head><body><div class="page">`;

  // Header
  html += `<h1>📊 Analytica AI Report</h1>
<p class="meta">Generated: ${ts} &nbsp;·&nbsp; File: ${dataset?.fileName ?? "—"} &nbsp;·&nbsp; Pipeline: M1→M2→M3${mlResult?"→M4→M5→M6":""}</p>`;

  // 1. Executive Summary
  if (sections.summary) {
    html += `<h2>1. Executive Summary</h2>`;
    const qs = profile?.qualityScore ?? 0;
    const qColor = qs>=80?"good":qs>=60?"warn":"danger";
    html += `<div class="cards">
      <div class="card"><div class="card-label">Dataset</div><div class="card-val blue" style="font-size:14px;word-break:break-all">${dataset?.fileName??'—'}</div></div>
      <div class="card"><div class="card-label">Total Rows</div><div class="card-val good">${fmtK(rows.length)}</div></div>
      <div class="card"><div class="card-label">Columns</div><div class="card-val blue">${profile?.colCount??'—'}</div></div>
      <div class="card"><div class="card-label">Quality Score</div><div class="card-val ${qColor}">${qs}<span style="font-size:14px">/100</span></div></div>
      <div class="card"><div class="card-label">Numeric Cols</div><div class="card-val purple">${numCols.length}</div></div>
      <div class="card"><div class="card-label">Categorical</div><div class="card-val orange">${catCols.length}</div></div>
    </div>`;
  }

  // 2. Dataset Profile
  if (sections.profile && profile) {
    html += `<h2>2. Dataset Profile</h2>
    <table><thead><tr>
      <th>Column</th><th>Type</th><th>Missing %</th><th>Unique</th><th>Outliers</th><th>Top Value</th>
    </tr></thead><tbody>`;
    profile.columns.forEach(c => {
      const mp = c.stats.missingPct;
      const mpColor = mp>40?"danger":mp>5?"warn":"good";
      const topVal = c.stats.topValues?.[0]?.[0] ?? (c.stats.mean!=null ? fmt(c.stats.mean) : "—");
      html += `<tr>
        <td style="font-weight:600;color:#E2E8F0;font-family:monospace">${c.name}</td>
        <td><span class="badge" style="color:${typeColor[c.type]??'#94A3B8'};background:${typeColor[c.type]??'#94A3B8'}18">${c.type}</span></td>
        <td class="${mpColor}">${mp.toFixed(1)}%</td>
        <td style="color:#94A3B8">${c.stats.unique}</td>
        <td style="color:${(c.stats.outlierCount??0)>0?'#FCA5A5':'#6EE7B7'}">${c.stats.outlierCount??'—'}</td>
        <td style="color:#64748B;font-family:monospace;font-size:12px">${String(topVal).slice(0,20)}</td>
      </tr>`;
    });
    html += `</tbody></table>`;
  }

  // 3. Cleaning Summary
  if (sections.cleaning && cleanResult) {
    const s = cleanResult.stats;
    html += `<h2>3. Data Cleaning Summary</h2>
    <div class="cards">
      <div class="card"><div class="card-label">Rows Dropped</div><div class="card-val ${s.rowsDropped>0?"danger":"good"}">${s.rowsDropped}</div></div>
      <div class="card"><div class="card-label">Cols Dropped</div><div class="card-val ${s.colsDropped>0?"warn":"good"}">${s.colsDropped}</div></div>
      <div class="card"><div class="card-label">Cells Imputed</div><div class="card-val warn">${s.cellsImputed}</div></div>
      <div class="card"><div class="card-label">Outliers Fixed</div><div class="card-val orange">${s.outliersWinsorized}</div></div>
      <div class="card"><div class="card-label">Dupes Removed</div><div class="card-val ${s.duplicatesRemoved>0?"warn":"good"}">${s.duplicatesRemoved}</div></div>
      <div class="card"><div class="card-label">Quality ▲</div><div class="card-val good">${dataset?.profile?.qualityScore??0} → ${cleanResult.afterProfile?.qualityScore??0}</div></div>
    </div>
    <h3>Cleaning Log (last 10 actions)</h3>
    <table><thead><tr><th>Step</th><th>Column</th><th>Action</th><th>Detail</th><th>Affected</th></tr></thead><tbody>`;
    cleanResult.log.slice(-10).forEach(l => {
      const sColor=l.severity==="danger"?"danger":l.severity==="warning"?"warn":l.severity==="good"?"good":"";
      html+=`<tr>
        <td style="color:#64748B;font-family:monospace">${l.step}</td>
        <td style="font-family:monospace;color:#93C5FD">${l.column}</td>
        <td class="${sColor}" style="font-family:monospace;font-size:12px">${l.action}</td>
        <td style="color:#64748B;font-size:12px">${l.detail}</td>
        <td style="color:#94A3B8;font-family:monospace">${l.affected}</td>
      </tr>`;
    });
    html += `</tbody></table>`;
  }

  // 4. EDA Highlights
  if (sections.eda && numCols.length) {
    html += `<h2>4. EDA Highlights</h2>
    <h3>Numeric Column Statistics</h3>
    <table><thead><tr><th>Column</th><th>Mean</th><th>Median</th><th>Std Dev</th><th>Min</th><th>Max</th><th>Skew</th><th>Outliers</th></tr></thead><tbody>`;
    numCols.forEach(c => {
      const sk = c.stats.skewLabel;
      const skColor = sk==="Normal"?"good":"warn";
      html += `<tr>
        <td style="font-weight:600;color:#E2E8F0;font-family:monospace">${c.name}</td>
        <td class="good">${fmt(c.stats.mean)}</td>
        <td class="blue">${fmt(c.stats.median)}</td>
        <td class="purple">${fmt(c.stats.std)}</td>
        <td style="color:#64748B">${fmt(c.stats.min,0)}</td>
        <td style="color:#64748B">${fmt(c.stats.max,0)}</td>
        <td class="${skColor}">${sk}</td>
        <td class="${(c.stats.outlierCount??0)>0?"danger":"good"}">${c.stats.outlierCount??0}</td>
      </tr>`;
    });
    html += `</tbody></table>`;
  }

  // 5. AutoML Results
  if (sections.ml && mlResult) {
    const best = mlResult.bestModel;
    const isClf = mlResult.isClassification;
    const score = isClf ? best.metrics.accuracy : best.metrics.r2;
    html += `<h2>5. AutoML Results</h2>
    <div class="cards">
      <div class="card"><div class="card-label">Task</div><div class="card-val blue" style="font-size:16px">${isClf?"Classification":"Regression"}</div></div>
      <div class="card"><div class="card-label">Best Model</div><div class="card-val good" style="font-size:14px">${best.name}</div></div>
      <div class="card"><div class="card-label">${isClf?"Accuracy":"R² Score"}</div><div class="card-val good">${(score*100).toFixed(1)}<span style="font-size:14px">%</span></div></div>
      <div class="card"><div class="card-label">CV Score</div><div class="card-val blue">${(mlResult.cvMean*100).toFixed(1)}<span style="font-size:12px">%±${(mlResult.cvStd*100).toFixed(1)}</span></div></div>
      ${!isClf?`<div class="card"><div class="card-label">RMSE</div><div class="card-val warn">${best.metrics.rmse}</div></div>`:""}
      ${isClf?`<div class="card"><div class="card-label">F1 Score</div><div class="card-val purple">${(best.metrics.f1*100).toFixed(1)}%</div></div>`:""}
    </div>
    <h3>Model Leaderboard</h3>
    <table><thead><tr><th>Rank</th><th>Model</th><th>${isClf?"Accuracy":"R²"}</th>${isClf?"<th>F1</th>":"<th>RMSE</th><th>MAE</th>"}</tr></thead><tbody>`;
    mlResult.results.forEach((r,i)=>{
      const s = isClf?r.metrics.accuracy:r.metrics.r2;
      html+=`<tr style="${i===0?"background:rgba(110,231,183,0.04)":""}">
        <td style="color:#64748B">${i===0?"🥇":"#"+(i+1)}</td>
        <td style="font-weight:${i===0?700:400};color:${i===0?"#6EE7B7":"#CBD5E1"}">${r.name}</td>
        <td class="${i===0?"good":""}" style="font-family:monospace">${(s*100).toFixed(1)}%</td>
        ${isClf?`<td style="color:#94A3B8;font-family:monospace">${(r.metrics.f1*100).toFixed(1)}%</td>`
               :`<td style="color:#94A3B8;font-family:monospace">${r.metrics.rmse}</td><td style="color:#94A3B8;font-family:monospace">${r.metrics.mae}</td>`}
      </tr>`;
    });
    html += `</tbody></table>
    <h3>Feature Importance (Top 8)</h3>
    <table><thead><tr><th>Rank</th><th>Feature</th><th>Importance</th><th>Bar</th></tr></thead><tbody>`;
    mlResult.featImportance.slice(0,8).forEach((f,i)=>{
      const pct=(f.score*100).toFixed(1);
      html+=`<tr>
        <td style="color:#64748B">${i+1}</td>
        <td style="font-family:monospace;color:#E2E8F0">${f.col}</td>
        <td style="color:#6EE7B7;font-family:monospace">${pct}%</td>
        <td><div style="height:8px;border-radius:99px;background:rgba(255,255,255,0.05);width:120px">
          <div style="height:8px;border-radius:99px;background:#6EE7B7;width:${pct}%"></div>
        </div></td>
      </tr>`;
    });
    html += `</tbody></table>`;
  }

  // 6. Recommendations
  if (sections.recs) {
    html += `<h2>6. Recommendations</h2>`;
    recs.forEach(r => {
      html += `<div class="rec">
        <div class="rec-icon">${r.icon}</div>
        <div><div class="rec-title">${r.title}</div><div class="rec-text">${r.text}</div></div>
      </div>`;
    });
  }

  html += `<div class="footer">Generated by Analytica AI · ${ts}</div>
</div></body></html>`;
  return html;
}

// ════════════════════════════════════════════════════════════════
// SECTION 10b — MODULE 7 MAIN COMPONENT
// ════════════════════════════════════════════════════════════════

function Module7({ dataset, cleanResult, mlResult }) {
  const profile = cleanResult?.afterProfile ?? dataset?.profile;
  const rows    = cleanResult?.cleanedRows   ?? dataset?.rows ?? [];

  const [sections, setSections] = useState({
    summary:true, profile:true, cleaning:true, eda:true, ml:true, recs:true,
  });
  const [copied,   setCopied]   = useState(false);

  if (!profile) return (
    <div style={{textAlign:"center",padding:"80px 0",color:C.muted}}>
      Complete at least Module 1 first.
    </div>
  );

  const htmlReport = buildHTMLReport(dataset, cleanResult, mlResult, sections);
  const mdReport   = buildMarkdownReport(dataset, cleanResult, mlResult, sections);
  const jsonSum    = buildJSONSummary(dataset, cleanResult, mlResult);

  const pipelineSteps = [
    { label:"Dataset Profiled",   done:!!profile,       color:C.blue   },
    { label:"Data Cleaned",       done:!!cleanResult,   color:C.green  },
    { label:"EDA Completed",      done:!!cleanResult,   color:C.purple },
    { label:"AutoML Trained",     done:!!mlResult,      color:C.yellow },
    { label:"LLM Insights",       done:!!mlResult,      color:C.orange },
    { label:"Chat Interface",     done:!!mlResult,      color:"#F9A8D4"},
  ];

  const SECTION_LABELS = [
    { key:"summary",  label:"Executive Summary" },
    { key:"profile",  label:"Dataset Profile"   },
    { key:"cleaning", label:"Cleaning Log",      disabled:!cleanResult },
    { key:"eda",      label:"EDA Highlights"     },
    { key:"ml",       label:"AutoML Results",    disabled:!mlResult    },
    { key:"recs",     label:"Recommendations"    },
  ];

  const EXPORTS = [
    {
      label:"HTML Report", icon:"🌐", color:C.blue, badge:"self-contained",
      action:() => downloadBlob(htmlReport, `report_${Date.now()}.html`, "text/html"),
    },
    {
      label:"Cleaned CSV", icon:"📊", color:C.green, badge:".csv",
      action:() => {
        if (!cleanResult?.cleanedRows) return;
        downloadCSV(cleanResult.cleanedRows, `cleaned_${dataset?.fileName??'data'}.csv`);
      },
      disabled:!cleanResult,
    },
    {
      label:"Raw CSV", icon:"📋", color:C.subtle, badge:".csv",
      action:() => { if (dataset?.rows) downloadCSV(dataset.rows, `raw_${dataset?.fileName??'data'}.csv`); },
      disabled:!dataset?.rows,
    },
    {
      label:"JSON Summary", icon:"{ }", color:C.yellow, badge:".json",
      action:() => downloadBlob(JSON.stringify(jsonSum,null,2), `summary_${Date.now()}.json`, "application/json"),
    },
    {
      label:"Markdown", icon:"📝", color:C.purple, badge:".md",
      action:() => downloadBlob(mdReport, `report_${Date.now()}.md`, "text/markdown"),
    },
  ];

  const copyHTML = () => {
    navigator.clipboard.writeText(htmlReport).then(()=>{
      setCopied(true); setTimeout(()=>setCopied(false),2000);
    });
  };

  return (
    <div style={{display:"grid",gridTemplateColumns:"320px 1fr",gap:20,
      animation:"fadeIn 0.4s ease",alignItems:"start"}}>

      {/* ── LEFT PANEL ── */}
      <div style={{display:"flex",flexDirection:"column",gap:14}}>

        {/* Pipeline checklist */}
        <div style={{padding:"18px",borderRadius:12,background:C.surface,
          border:`1px solid ${C.border}`}}>
          <SectionHeader>Pipeline Completion</SectionHeader>
          {pipelineSteps.map((s,i)=>(
            <div key={i} style={{display:"flex",alignItems:"center",gap:10,
              marginBottom:i<pipelineSteps.length-1?8:0}}>
              <div style={{width:20,height:20,borderRadius:"50%",flexShrink:0,
                background:s.done?`${s.color}22`:"rgba(255,255,255,0.04)",
                border:`2px solid ${s.done?s.color:"rgba(255,255,255,0.1)"}`,
                display:"flex",alignItems:"center",justifyContent:"center",
                fontSize:10,color:s.done?s.color:C.muted}}>
                {s.done?"✓":""}
              </div>
              <span style={{fontSize:13,color:s.done?C.text:C.muted,
                fontWeight:s.done?500:400}}>{s.label}</span>
              {!s.done&&<Tag color={C.muted}>Locked</Tag>}
            </div>
          ))}
        </div>

        {/* Section toggles */}
        <div style={{padding:"18px",borderRadius:12,background:C.surface,
          border:`1px solid ${C.border}`}}>
          <SectionHeader>Report Sections</SectionHeader>
          {SECTION_LABELS.map(s=>(
            <div key={s.key} style={{display:"flex",alignItems:"center",
              justifyContent:"space-between",marginBottom:8}}>
              <span style={{fontSize:13,color:s.disabled?C.muted:C.subtle}}>{s.label}</span>
              <button
                disabled={s.disabled}
                onClick={()=>setSections(p=>({...p,[s.key]:!p[s.key]}))}
                style={{
                  width:38,height:20,borderRadius:99,cursor:s.disabled?"not-allowed":"pointer",
                  border:"none",transition:"background 0.2s",
                  background:sections[s.key]&&!s.disabled?C.green:"rgba(255,255,255,0.08)",
                  position:"relative",opacity:s.disabled?0.4:1,
                }}>
                <div style={{
                  position:"absolute",top:2,
                  left:sections[s.key]&&!s.disabled?18:2,
                  width:16,height:16,borderRadius:"50%",
                  background:"#fff",transition:"left 0.2s",
                }}/>
              </button>
            </div>
          ))}
        </div>

        {/* Export buttons */}
        <div style={{padding:"18px",borderRadius:12,background:C.surface,
          border:`1px solid ${C.border}`}}>
          <SectionHeader>Export</SectionHeader>
          <div style={{display:"flex",flexDirection:"column",gap:8}}>
            {EXPORTS.map((ex,i)=>(
              <button key={i} onClick={ex.action}
                disabled={ex.disabled}
                style={{
                  display:"flex",alignItems:"center",gap:10,padding:"11px 14px",
                  borderRadius:9,cursor:ex.disabled?"not-allowed":"pointer",
                  background:ex.disabled?"rgba(255,255,255,0.02)":`${ex.color}0D`,
                  border:`1px solid ${ex.disabled?"rgba(255,255,255,0.05)":ex.color+"28"}`,
                  color:ex.disabled?C.muted:C.text,
                  transition:"all 0.18s",opacity:ex.disabled?0.4:1,
                  textAlign:"left",
                }}>
                <span style={{fontSize:18,flexShrink:0}}>{ex.icon}</span>
                <span style={{flex:1,fontSize:13,fontWeight:500}}>{ex.label}</span>
                <span style={{fontSize:10,color:ex.color,background:`${ex.color}18`,
                  borderRadius:99,padding:"1px 7px",fontFamily:"'DM Mono',monospace",
                  flexShrink:0}}>{ex.badge}</span>
              </button>
            ))}
          </div>
        </div>

        {/* Meta info */}
        <div style={{padding:"14px 18px",borderRadius:10,
          background:"rgba(255,255,255,0.02)",border:`1px solid ${C.border}`}}>
          <div style={{fontSize:11,color:C.muted,fontFamily:"'DM Mono',monospace",lineHeight:1.8}}>
            <div>Generated: {new Date().toLocaleString()}</div>
            <div>Rows in report: {fmtK(rows.length)}</div>
            <div>Sections active: {Object.values(sections).filter(Boolean).length}/6</div>
          </div>
        </div>
      </div>

      {/* ── RIGHT PANEL — Report Preview ── */}
      <div style={{borderRadius:12,border:`1px solid ${C.border}`,overflow:"hidden",
        display:"flex",flexDirection:"column"}}>

        {/* Preview header */}
        <div style={{padding:"12px 18px",borderBottom:`1px solid ${C.border}`,
          background:"rgba(255,255,255,0.02)",
          display:"flex",alignItems:"center",justifyContent:"space-between",flexShrink:0}}>
          <div style={{display:"flex",alignItems:"center",gap:10}}>
            <span style={{fontSize:13,fontWeight:600,color:C.text}}>Report Preview</span>
            <Tag color={C.subtle}>Live</Tag>
          </div>
          <div style={{display:"flex",gap:8}}>
            <button onClick={copyHTML} style={{
              padding:"5px 12px",borderRadius:7,fontSize:12,cursor:"pointer",
              background:copied?"rgba(110,231,183,0.1)":"rgba(255,255,255,0.04)",
              border:`1px solid ${copied?C.green:C.border}`,
              color:copied?C.green:C.muted,transition:"all 0.2s"}}>
              {copied?"✓ Copied":"⎘ Copy HTML"}
            </button>
            <button onClick={()=>{ const w=window.open("","_blank"); w.document.write(htmlReport); w.document.close(); }}
              style={{padding:"5px 12px",borderRadius:7,fontSize:12,cursor:"pointer",
                background:"rgba(255,255,255,0.04)",border:`1px solid ${C.border}`,
                color:C.muted}}>
              ↗ Open Full
            </button>
          </div>
        </div>

        {/* Iframe preview */}
        <iframe
          srcDoc={htmlReport}
          style={{flex:1,border:"none",height:"72vh",minHeight:500,
            background:"#03060F"}}
          title="Report Preview"
          sandbox="allow-same-origin"
        />
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════════════════════
// SECTION 11 — APP SHELL (Navigation + State Orchestration)
// ════════════════════════════════════════════════════════════════

const MODULES = [
  { id:"m1", label:"Dataset Profiler",    icon:"🔬", num:"01", color:C.blue   },
  { id:"m2", label:"Data Cleaning",       icon:"🧹", num:"02", color:C.green  },
  { id:"m3", label:"EDA Engine",          icon:"📊", num:"03", color:C.purple },
  { id:"m4", label:"AutoML",              icon:"🤖", num:"04", color:C.yellow },
  { id:"m5", label:"Insights (LLM)",      icon:"💡", num:"05", color:C.orange },
  { id:"m6", label:"Chat Interface",      icon:"💬", num:"06", color:"#F9A8D4"},
  { id:"m7", label:"Reports & Export",    icon:"📄", num:"07", color:C.subtle },
];

export default function AIPlatform() {
  const [activeModule, setActiveModule] = useState("m1");
  const [dataset,      setDataset]      = useState(null);   // { rows, profile, fileName, fileSize }
  const [cleanResult,  setCleanResult]  = useState(null);   // { cleanedRows, log, stats, afterProfile }
  const [mlResult,     setMlResult]     = useState(null);   // AutoML result from Module 4

  const unlockedModules = new Set(["m1"]);
  if (dataset)     unlockedModules.add("m2");
  if (cleanResult) { unlockedModules.add("m3"); unlockedModules.add("m4"); }
  if (mlResult)    { unlockedModules.add("m5"); unlockedModules.add("m6"); unlockedModules.add("m7"); }

  const handleUpload = useCallback((data) => {
    setDataset(data);
    setCleanResult(null);
    setMlResult(null);
  }, []);

  const handleCleaningDone = useCallback((result) => {
    setCleanResult(result);
    setMlResult(null);
  }, []);

  const handleMLDone = useCallback((result) => {
    setMlResult(result);
  }, []);

  return (
    <>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,wght@0,400;0,500;0,600;0,700;1,400&family=DM+Mono:wght@400;500;700&display=swap');
        *, *::before, *::after { box-sizing:border-box; margin:0; padding:0; }
        body { background:${C.bg}; }
        ::-webkit-scrollbar { width:5px; height:5px; }
        ::-webkit-scrollbar-track { background:transparent; }
        ::-webkit-scrollbar-thumb { background:rgba(255,255,255,0.1); border-radius:99px; }
        select option { background:#1E293B; color:#E2E8F0; }
        @keyframes fadeIn  { from{opacity:0;transform:translateY(8px)} to{opacity:1;transform:translateY(0)} }
        @keyframes pulse   { 0%,100%{opacity:1} 50%{opacity:0.4} }
        @keyframes spin    { from{transform:rotate(0deg)} to{transform:rotate(360deg)} }
        @keyframes glow    { 0%,100%{box-shadow:0 0 12px rgba(110,231,183,0.3)} 50%{box-shadow:0 0 24px rgba(110,231,183,0.6)} }
      `}</style>

      <div style={{ minHeight:"100vh", background:C.bg,
        fontFamily:"'DM Sans',sans-serif", color:C.text }}>

        {/* ── Top Bar ── */}
        <div style={{ position:"sticky", top:0, zIndex:100,
          background:"rgba(3,6,15,0.85)", backdropFilter:"blur(12px)",
          borderBottom:`1px solid ${C.border}` }}>
          <div style={{ maxWidth:1200, margin:"0 auto",
            padding:"0 24px", display:"flex", alignItems:"center",
            gap:16, height:56 }}>

            {/* Logo */}
            <div style={{ display:"flex", alignItems:"center", gap:10, flexShrink:0 }}>
              <div style={{ width:32, height:32, borderRadius:9,
                background:"linear-gradient(135deg,#6EE7B7,#3B82F6)",
                display:"flex", alignItems:"center", justifyContent:"center",
                fontSize:16, fontWeight:800 }}>
                A
              </div>
              <span style={{ fontSize:15, fontWeight:700, color:C.text,
                letterSpacing:"-0.02em" }}>Analytica AI</span>
            </div>

            <div style={{ height:20, width:1, background:C.border }}/>

            {/* Nav modules */}
            <div style={{ display:"flex", gap:2, overflowX:"auto", flex:1 }}>
              {MODULES.map(m => {
                const unlocked = unlockedModules.has(m.id);
                const active   = activeModule === m.id;
                return (
                  <button key={m.id}
                    onClick={() => unlocked && setActiveModule(m.id)}
                    title={!unlocked ? "Complete previous module to unlock" : ""}
                    style={{
                      display:"flex", alignItems:"center", gap:6,
                      padding:"6px 13px", borderRadius:8, flexShrink:0,
                      background: active ? `${m.color}18` : "transparent",
                      border: active ? `1px solid ${m.color}40` : "1px solid transparent",
                      color: active ? m.color : unlocked ? C.subtle : "rgba(100,116,139,0.35)",
                      cursor: unlocked ? "pointer" : "not-allowed",
                      fontSize:13, fontWeight: active ? 600 : 400,
                      transition:"all 0.18s",
                    }}>
                    <span style={{ opacity: unlocked ? 1 : 0.3 }}>{m.icon}</span>
                    <span style={{ fontSize:11, fontFamily:"'DM Mono',monospace",
                      opacity:0.6 }}>{m.num}</span>
                    <span>{m.label}</span>
                    {!unlocked && <span style={{ fontSize:10, opacity:0.3 }}>🔒</span>}
                  </button>
                );
              })}
            </div>

            {/* Dataset indicator */}
            {dataset && (
              <div style={{ display:"flex", alignItems:"center", gap:8, flexShrink:0,
                padding:"5px 12px", borderRadius:8,
                background:"rgba(110,231,183,0.06)",
                border:"1px solid rgba(110,231,183,0.15)" }}>
                <div style={{ width:7, height:7, borderRadius:"50%",
                  background:C.green, animation:"glow 2s infinite" }}/>
                <span style={{ fontSize:12, color:C.green,
                  maxWidth:140, overflow:"hidden", textOverflow:"ellipsis",
                  whiteSpace:"nowrap" }}>{dataset.fileName}</span>
              </div>
            )}
          </div>
        </div>

        {/* ── Main Content ── */}
        <div style={{ maxWidth:1200, margin:"0 auto", padding:"32px 24px" }}>

          {/* Module page header */}
          <div style={{ marginBottom:28 }}>
            {(() => {
              const m = MODULES.find(x => x.id === activeModule);
              return (
                <div style={{ display:"flex", alignItems:"center", gap:12 }}>
                  <div style={{ width:42, height:42, borderRadius:11,
                    background:`${m.color}18`, border:`1px solid ${m.color}30`,
                    display:"flex", alignItems:"center", justifyContent:"center",
                    fontSize:20 }}>{m.icon}</div>
                  <div>
                    <div style={{ display:"flex", alignItems:"center", gap:8 }}>
                      <h1 style={{ fontSize:20, fontWeight:700, color:C.text }}>{m.label}</h1>
                      <span style={{ fontSize:11, color:m.color,
                        background:`${m.color}14`, borderRadius:99,
                        padding:"1px 8px", fontFamily:"'DM Mono',monospace" }}>
                        MODULE {m.num}
                      </span>
                      {activeModule==="m1" && dataset && (
                        <button onClick={()=>{setDataset(null);setCleanResult(null);}} style={{
                          padding:"3px 10px",borderRadius:6,fontSize:12,cursor:"pointer",
                          background:"rgba(255,255,255,0.04)",border:`1px solid ${C.border}`,
                          color:C.muted}}>← Reset</button>
                      )}
                    </div>
                    <p style={{ fontSize:13, color:C.muted, marginTop:2 }}>
                      {activeModule==="m1" && "Upload and automatically profile any dataset — detect types, missing values, outliers, and quality issues"}
                      {activeModule==="m2" && "Automatically clean your dataset — impute, deduplicate, winsorize outliers, fix types, and standardise values"}
                      {activeModule==="m3" && "Interactive charts, correlation heatmap, and AI-generated insights — all computed from your cleaned data"}
                      {activeModule==="m4" && "Train 4 ML models automatically — classification or regression — with metrics, feature importance, CV, and live prediction"}
                      {activeModule==="m5" && "Ask Claude to explain your dataset, interpret ML results, and generate actionable business insights powered by AI"}
                      {activeModule==="m6" && "Full-featured data chat — multi-turn conversations, slash commands, conversation history, and follow-up suggestions"}
                      {activeModule==="m7" && "Generate a complete HTML/MD/JSON report of your entire pipeline and export your data in multiple formats"}
                    </p>
                  </div>
                </div>
              );
            })()}
          </div>

          {/* Locked state */}
          {!unlockedModules.has(activeModule) && (
            <div style={{ textAlign:"center", padding:"80px 0",
              animation:"fadeIn 0.3s ease" }}>
              <div style={{ fontSize:52, marginBottom:16, opacity:0.3 }}>🔒</div>
              <p style={{ fontSize:16, color:C.muted }}>
                Complete Module {MODULES.findIndex(m=>m.id===activeModule)} first to unlock this module.
              </p>
            </div>
          )}

          {/* Module 1 */}
          {activeModule === "m1" && (
            <Module1
              dataset={dataset}
              onData={handleUpload}
              onReset={()=>{setDataset(null);setCleanResult(null);}}
            />
          )}

          {/* Module 2 */}
          {activeModule === "m2" && unlockedModules.has("m2") && (
            <Module2
              dataset={dataset}
              onCleaningDone={handleCleaningDone}
            />
          )}

          {/* Module 3 */}
          {activeModule === "m3" && unlockedModules.has("m3") && (
            <Module3
              dataset={dataset}
              cleanResult={cleanResult}
            />
          )}

          {/* Module 4 */}
          {activeModule === "m4" && unlockedModules.has("m4") && (
            <Module4
              dataset={dataset}
              cleanResult={cleanResult}
              onMLDone={handleMLDone}
            />
          )}

          {/* Module 5 */}
          {activeModule === "m5" && unlockedModules.has("m5") && (
            <Module5
              dataset={dataset}
              cleanResult={cleanResult}
              mlResult={mlResult}
            />
          )}

          {/* Module 6 */}
          {activeModule === "m6" && unlockedModules.has("m6") && (
            <Module6
              dataset={dataset}
              cleanResult={cleanResult}
              mlResult={mlResult}
            />
          )}

          {/* Module 7 */}
          {activeModule === "m7" && unlockedModules.has("m7") && (
            <Module7
              dataset={dataset}
              cleanResult={cleanResult}
              mlResult={mlResult}
            />
          )}
        </div>
      </div>
    </>
  );
}
