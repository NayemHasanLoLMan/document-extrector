# Invoice RPA Extraction System

An end-to-end **Robotic Process Automation (RPA)** pipeline that automatically monitors a folder for PDF invoices, extracts structured data using a deterministic rule-based parser, and outputs a live-updating Excel database with a web dashboard.

---

## Features

- **Zero-cost local processing** — rule-based regex parser, no API keys or cloud services needed
- **Live folder watching** — drop a PDF into `Inbox/` and it's processed automatically within seconds
- **Web dashboard** — browse, search, and export extracted data at `http://localhost:5000`
- **Dual output** — structured `CSV` + formatted `Excel (.xlsx)` updated after every file
- **Fallback engines** — optional Ollama (local LLM) or Google Gemini (cloud) for non-standard formats
- **Fault tolerant** — failed files are moved to `Failed/`, successfully processed files to `Processed/`

---

## Project Structure

```
invoice-rpa/
├── app.py              # Flask web server + REST API
├── rpa_bot.py          # Core RPA engine (parser, watchdog, queue, DB writer)
├── process_all.py      # One-shot batch processor for a full PDF folder
├── config.json         # Runtime configuration (engine, API key, model)
├── index.html          # Web UI frontend
├── requirements.txt    # Python dependencies
│
├── Inbox/              # Drop PDFs here → auto-processed  (auto-created)
├── Processed/          # Successfully extracted PDFs       (auto-created)
├── Failed/             # PDFs that failed extraction       (auto-created)
│
├── Extracted_Database.csv    # Master data store
└── Extracted_Database.xlsx   # Formatted Excel output (auto-rebuilt)
```

---

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Start the web server

```bash
python app.py
```

Open **http://localhost:5000** in your browser.

### 3. Process invoices

**Option A — Live (watchdog):** Drop any PDF into the `Inbox/` folder while the server is running.

**Option B — Web upload:** Use the "Upload & Process" panel in the browser UI.

**Option C — Batch (entire folder):**
```bash
python process_all.py
python process_all.py --folder "path/to/your/invoices"
```

---

## Configuration (`config.json`)

```json
{
  "engine": "regex",
  "api_key": "",
  "ollama_model": "qwen2.5:7b",
  "inter_file_delay": 2
}
```

| Engine | Description | Cost |
|--------|-------------|------|
| `regex` | Deterministic rule-based parser — **default, fastest, most accurate** for SuperStore format | Free |
| `ollama` | Local LLM via [Ollama](https://ollama.com) — use for varied/unknown invoice formats | Free |
| `gemini` | Google Gemini API — use for scanned/image PDFs that have no text layer | Paid |

---

## Extracted Fields

| Column | Description |
|--------|-------------|
| Invoice # | Invoice reference number |
| Order ID | Associated order identifier |
| Date | Invoice issue date |
| Vendor | Issuing company name |
| Client | Billed-to name |
| Ship To Address | Delivery address |
| Ship Mode | Shipping method |
| Subtotal | Pre-discount total |
| Discount | Discount amount |
| Shipping | Shipping cost |
| Tax | Tax amount |
| Total | Final payable amount |
| Balance Due | Outstanding balance |
| Item Description | Product name(s) — pipe-separated for multi-item invoices |
| SKU / Category | Product category and SKU code |
| Qty | Quantity ordered |
| Unit Price | Per-unit rate |
| Line Amount | Line total |

---

## Architecture

```
PDF dropped in Inbox/
        │
        ▼
  Watchdog (watchdog)
  detects new file
        │
        ▼
  Processing Queue
  (one file at a time)
        │
        ▼
  ┌─────────────────────────────────┐
  │        rpa_bot.py               │
  │                                 │
  │  1. Extract text  (pypdf)       │
  │  2. Parse fields  (regex)       │
  │  3. Write CSV     (csv)         │
  │  4. Rebuild Excel (openpyxl)    │
  │  5. Move to Processed/          │
  └─────────────────────────────────┘
        │
        ▼
  Extracted_Database.csv
  Extracted_Database.xlsx
        │
        ▼
  Flask API (app.py)
  Web UI  (index.html)
  http://localhost:5000
```

---

## Requirements

- Python 3.9+
- See `requirements.txt` for all packages

### Optional (for LLM engine)
- [Ollama](https://ollama.com) installed and running (`ollama serve`)
- Recommended model: `ollama pull qwen2.5:7b`

---

## Performance

Tested on a dataset of **990 SuperStore PDF invoices**:

| Metric | Result |
|--------|--------|
| Total files processed | 990 |
| Processing failures | 0 |
| Average time per file | ~0.05 seconds |
| Total processing time | ~50 seconds |
| Output rows | 989 |

---

*Built as a demonstration of RPA principles: automated document intake, structured data extraction, fault-tolerant processing, and live reporting.*
