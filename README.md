# Analytica AI

> AI-powered end-to-end data science platform — profile, clean, explore, model, and export your data.

## Modules

| # | Module | Description |
|---|---|---|
| 01 | Dataset Profiler | Upload CSV/Excel, auto-detect types, quality scoring |
| 02 | Data Cleaning | 6-step deterministic pipeline with full audit log |
| 03 | EDA Engine | SVG charts, correlation heatmap, KDE, auto-insights |
| 04 | AutoML | 4 models, CV, feature importance, live prediction |
| 05 | LLM Insights | Claude-powered analysis, streaming responses |
| 06 | Chat Interface | Multi-turn data chat with slash commands |
| 07 | Reports & Export | HTML/MD/JSON/CSV export, live preview |

## Quick Start (local)

```bash
npm install
npm run dev
```

## Deploy to Vercel (free)

### Option A — Vercel CLI
```bash
npm install -g vercel
vercel
```

### Option B — GitHub + Vercel Dashboard
1. Push this folder to a GitHub repo
2. Go to [vercel.com](https://vercel.com) → New Project
3. Import your repo
4. Framework: **Vite** (auto-detected)
5. Click **Deploy** — done in ~60 seconds

## Deploy to Netlify (free)

### Option A — Drag & Drop
```bash
npm run build
# drag the /dist folder to netlify.com/drop
```

### Option B — Netlify CLI
```bash
npm install -g netlify-cli
npm run build
netlify deploy --prod --dir=dist
```

## Tech Stack

- React 18 + Vite
- PapaParse (CSV parsing & export)
- SheetJS/xlsx (Excel parsing)
- Anthropic Claude API (M5 + M6 — needs API key in browser)
- Pure JS ML engines (no sklearn/tensorflow)
- Pure SVG charts (no Recharts/Plotly)

## Notes

- The LLM features (Module 5 & 6) call the Anthropic API directly from the browser.
- No backend required — everything runs client-side.
- For production use, proxy the API calls through a backend to protect your API key.
