import os
import re
import time
import json
import queue
import shutil
import csv
import logging
import threading
from datetime import datetime
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

try:
    import google.generativeai as genai
    import requests
    from pypdf import PdfReader
except ImportError:
    print("CRITICAL: Missing dependencies. Run:")
    print("pip install google-generativeai watchdog pypdf requests openpyxl")
    exit(1)

# ─── CONFIGURATION ────────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).parent
INBOX_DIR      = BASE_DIR / "Inbox"
PROCESSED_DIR  = BASE_DIR / "Processed"
FAILED_DIR     = BASE_DIR / "Failed"
DATABASE_FILE  = BASE_DIR / "Extracted_Database.csv"
EXCEL_FILE     = BASE_DIR / "Extracted_Database.xlsx"

CONFIG_FILE    = BASE_DIR / "config.json"

ENGINE         = 'gemini'
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")   # env var wins if set
OLLAMA_URL     = "http://localhost:11434/api/generate"
OLLAMA_MODEL   = "llama3.2"
INTER_FILE_DELAY = 8   # seconds between consecutive Gemini calls

# ─── SHARED STATE (used by app.py for /api/status) ────────────────────────────
currently_processing = None
processing_lock      = threading.Lock()
# ──────────────────────────────────────────────────────────────────────────────

def load_config():
    """Load persisted settings from config.json (survives server restarts)."""
    global ENGINE, GEMINI_API_KEY, OLLAMA_MODEL, INTER_FILE_DELAY
    if CONFIG_FILE.exists():
        try:
            cfg = json.loads(CONFIG_FILE.read_text(encoding='utf-8'))
            ENGINE           = cfg.get("engine", ENGINE)
            if not GEMINI_API_KEY:           # env var has priority
                GEMINI_API_KEY = cfg.get("api_key", "")
            OLLAMA_MODEL     = cfg.get("ollama_model", OLLAMA_MODEL)
            INTER_FILE_DELAY = cfg.get("inter_file_delay", INTER_FILE_DELAY)
            log.info(f"Config loaded  engine={ENGINE}  key={'SET' if GEMINI_API_KEY else 'NOT SET'}")
        except Exception as exc:
            log.warning(f"Could not read config.json: {exc}")

def save_config():
    """Write current settings to disk so they survive restarts."""
    try:
        CONFIG_FILE.write_text(json.dumps({
            "engine":           ENGINE,
            "api_key":          GEMINI_API_KEY,
            "ollama_model":     OLLAMA_MODEL,
            "inter_file_delay": INTER_FILE_DELAY,
        }, indent=2), encoding='utf-8')
    except Exception as exc:
        log.warning(f"Could not save config.json: {exc}")

def is_configured() -> bool:
    """True only when the bot has a usable API key / engine."""
    if ENGINE == 'gemini':
        return bool(GEMINI_API_KEY and len(GEMINI_API_KEY) > 10)
    return True   # Ollama needs no key

PROMPT = """Extract ALL structured data from this PDF document.
Return ONLY a raw JSON object — no markdown fences, no backticks, no explanation.
Use exactly this schema:
{
  "document_type": "Invoice|Receipt|Purchase Order|Bank Statement|Contract|Quotation|Other",
  "reference_number": "document/invoice/PO number as written",
  "order_id": "order ID if present, else empty string",
  "date": "issue/document date",
  "due_date": "payment due date or empty string",
  "vendor_name": "issuer/seller/company name",
  "vendor_address": "full address or empty string",
  "vendor_contact": "email or phone or empty string",
  "client_name": "billed-to/buyer name",
  "client_address": "buyer address or empty string",
  "ship_to_address": "ship-to address if different, else empty string",
  "ship_mode": "ship mode if stated, else empty string",
  "currency": "USD|BDT|EUR|GBP or symbol",
  "subtotal": "pre-tax subtotal or empty string",
  "discount": "discount amount or empty string",
  "shipping_cost": "shipping or freight amount or empty string",
  "tax": "tax amount or empty string",
  "tax_rate": "tax % if stated or empty string",
  "total": "final total amount",
  "balance_due": "balance due if explicitly stated, else empty string",
  "payment_terms": "e.g. Net 30 or empty string",
  "notes": "any other useful info",
  "line_items": [
    {
      "description": "main item description",
      "sku_or_category": "SKU, category, or secondary description text",
      "quantity": "qty or hours",
      "unit_price": "price per unit",
      "amount": "line total"
    }
  ]
}
Rules:
- Use empty string "" for any missing field — never null or omit the key.
- Put every ACTUAL purchased line item as a separate object in line_items.
- CRITICAL: DO NOT extract "Shipping", "Tax", "Discount", or "Subtotal" as line items! They must only go into their dedicated top-level fields.
- CRITICAL: If an item has a secondary description, SKU, or Category underneath it, put it into `sku_or_category` of the SAME line item. Do NOT create a separate line item for it.
- If no line items exist, use [{"description":"(see document)","quantity":"","unit_price":"","amount":""}].
- Return ONLY the JSON object. Nothing else."""

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("rpa_bot")

# ─── GLOBAL PROCESSING QUEUE (FIX #1: serialise all file processing) ─────────
_file_queue   = queue.Queue()
_worker_thread = None
# ─────────────────────────────────────────────────────────────────────────────


def setup_environment():
    """Create folders, load persisted config, and initialise the CSV database."""
    load_config()   # ← load API key from disk FIRST (fixes startup race condition)

    for d in [INBOX_DIR, PROCESSED_DIR, FAILED_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    if not DATABASE_FILE.exists():
        with open(DATABASE_FILE, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow([
                'Timestamp', 'File Name', 'Doc Type', 'Ref Number', 'Order ID', 'Date', 'Due Date',
                'Vendor', 'Vendor Address', 'Vendor Contact',
                'Client', 'Client Address', 'Ship To Address', 'Ship Mode', 'Currency',
                'Total', 'Balance Due', 'Subtotal', 'Discount', 'Shipping', 'Tax', 'Tax Rate', 'Payment Terms',
                'Line Description', 'SKU/Category', 'Qty', 'Unit Price', 'Line Amount', 'Notes'
            ])

    if ENGINE == 'gemini' and is_configured():
        genai.configure(api_key=GEMINI_API_KEY)
        log.info("Gemini API configured successfully.")


# ─── JSON EXTRACTION (FIX #7: robust regex fallback) ─────────────────────────
def extract_json(text: str) -> dict:
    """Reliably extract JSON from LLM output regardless of wrapping."""
    # Strip markdown fences
    text = re.sub(r'^```(?:json)?\s*', '', text.strip(), flags=re.IGNORECASE)
    text = re.sub(r'\s*```$', '', text.strip())
    text = text.strip()

    # Direct parse attempt
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Regex: find outermost JSON object
    match = re.search(r'\{[\s\S]*\}', text)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    raise ValueError(f"Could not parse JSON from model output:\n{text[:300]}")


# ─── WAIT FOR FILE TO FINISH WRITING (FIX #6) ────────────────────────────────
def wait_for_file_stable(path: Path, timeout: int = 30) -> bool:
    """Wait until file size stops changing (file fully written to disk)."""
    deadline = time.time() + timeout
    last_size = -1
    while time.time() < deadline:
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            return False
        if size == last_size and size > 0:
            return True
        last_size = size
        time.sleep(0.5)
    return False


# ─── GEMINI PROCESSING (FIX #2: safe file cleanup, better retry) ─────────────
def process_with_gemini(file_path: Path) -> dict:
    log.info(f"Uploading '{file_path.name}' to Gemini…")
    uploaded_file = None
    try:
        uploaded_file = genai.upload_file(path=str(file_path), mime_type="application/pdf")
        model = genai.GenerativeModel(
            'gemini-2.5-flash',
            generation_config={"response_mime_type": "application/json"}
        )

        max_retries = 4
        for attempt in range(max_retries):
            try:
                time.sleep(4)  # base pacing delay
                response = model.generate_content([uploaded_file, PROMPT])
                return extract_json(response.text)

            except Exception as e:
                err_str = str(e)
                is_rate_limit = "429" in err_str or "quota" in err_str.lower() or "exhausted" in err_str.lower()
                if is_rate_limit and attempt < max_retries - 1:
                    wait = 65 * (attempt + 1)   # exponential back-off: 65s, 130s, 195s
                    log.warning(f"Rate limit hit — waiting {wait}s before retry {attempt+1}/{max_retries}…")
                    time.sleep(wait)
                    continue
                raise   # non-rate-limit error or out of retries

    finally:
        # FIX #2: always clean up the uploaded file, even if an error occurred
        if uploaded_file is not None:
            try:
                genai.delete_file(uploaded_file.name)
            except Exception:
                pass  # ignore cleanup errors


def process_with_ollama(file_path: Path) -> dict:
    log.info(f"Extracting text from '{file_path.name}' for local (Ollama) processing…")
    reader = PdfReader(str(file_path))
    text = "\n".join(page.extract_text() or "" for page in reader.pages)

    if not text.strip():
        raise ValueError("Could not extract any text from the PDF (scanned image PDF?).")

    full_prompt = f"{PROMPT}\n\nDOCUMENT TEXT:\n{text[:6000]}"
    log.info(f"Sending to Ollama ({OLLAMA_MODEL})…")
    resp = requests.post(OLLAMA_URL, json={
        "model":   OLLAMA_MODEL,
        "prompt":  full_prompt,
        "stream":  False,
        "format":  "json",
        "options": {"temperature": 0.1}
    }, timeout=120)

    if resp.status_code != 200:
        raise ConnectionError(f"Ollama HTTP {resp.status_code}: {resp.text[:200]}")

    return extract_json(resp.json().get("response", "{}"))


# ─── DATABASE WRITE ───────────────────────────────────────────────────────────
_csv_lock = threading.Lock()

def append_to_database(file_name: str, data: dict):
    timestamp  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line_items = data.get("line_items") or [{}]

    desc_list, sku_list, qty_list, price_list, amt_list = [], [], [], [], []
    for item in line_items:
        desc = str(item.get("description", "")).strip()
        if desc and desc != "(see document)":
            desc_list.append(desc)
            sku_list.append(str(item.get("sku_or_category", "")).strip())
            qty_list.append(str(item.get("quantity", "")).strip())
            price_list.append(str(item.get("unit_price", "")).strip())
            amt_list.append(str(item.get("amount", "")).strip())

    row = [
        timestamp,
        file_name,
        data.get("document_type", ""),
        data.get("reference_number", ""),
        data.get("order_id", ""),
        data.get("date", ""),
        data.get("due_date", ""),
        data.get("vendor_name", ""),
        data.get("vendor_address", ""),
        data.get("vendor_contact", ""),
        data.get("client_name", ""),
        data.get("client_address", ""),
        data.get("ship_to_address", ""),
        data.get("ship_mode", ""),
        data.get("currency", ""),
        data.get("total", ""),
        data.get("balance_due", ""),
        data.get("subtotal", ""),
        data.get("discount", ""),
        data.get("shipping_cost", ""),
        data.get("tax", ""),
        data.get("tax_rate", ""),
        data.get("payment_terms", ""),
        "\n".join(desc_list),
        "\n".join(sku_list),
        "\n".join(qty_list),
        "\n".join(price_list),
        "\n".join(amt_list),
        data.get("notes", ""),
    ]

    with _csv_lock:
        with open(DATABASE_FILE, 'a', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(row)


# ─── EXCEL EXPORT (FIX #3: use openpyxl directly, no pandas required) ────────
_xlsx_lock = threading.Lock()

HEADERS = [
    'Timestamp', 'File Name', 'Doc Type', 'Ref Number', 'Order ID', 'Date', 'Due Date',
    'Vendor', 'Vendor Address', 'Vendor Contact',
    'Client', 'Client Address', 'Ship To Address', 'Ship Mode', 'Currency',
    'Total', 'Balance Due', 'Subtotal', 'Discount', 'Shipping', 'Tax', 'Tax Rate', 'Payment Terms',
    'Line Description', 'SKU/Category', 'Qty', 'Unit Price', 'Line Amount', 'Notes'
]

COL_WIDTHS = [
    18, 28, 14, 16, 16, 13, 13,
    24, 30, 22,
    24, 30, 30, 16, 10,
    14, 14, 12, 12, 12, 10, 10, 16,
    35, 20, 8, 12, 14, 30
]

def update_excel_file():
    """Rebuild the live Excel file from the master CSV with formatting."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        from openpyxl.utils import get_column_letter

        if not DATABASE_FILE.exists() or DATABASE_FILE.stat().st_size == 0:
            return

        with _csv_lock:
            with open(DATABASE_FILE, 'r', encoding='utf-8') as f:
                rows = list(csv.reader(f))

        if len(rows) < 2:
            return   # header only, nothing to write

        with _xlsx_lock:
            wb = Workbook()
            ws = wb.active
            ws.title = "Extracted Data"

            # ── Header row styling
            header_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
            header_font = Font(name="Calibri", bold=True, color="FFFFFF", size=10)
            thin        = Side(border_style="thin", color="D0D0D0")
            cell_border = Border(left=thin, right=thin, top=thin, bottom=thin)

            for col_idx, (header, width) in enumerate(zip(HEADERS, COL_WIDTHS), start=1):
                cell = ws.cell(row=1, column=col_idx, value=header)
                cell.fill      = header_fill
                cell.font      = header_font
                cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                cell.border    = cell_border
                ws.column_dimensions[get_column_letter(col_idx)].width = width

            ws.row_dimensions[1].height = 22
            ws.freeze_panes = "A2"

            # ── Data rows
            alt_fill  = PatternFill(start_color="EEF3F8", end_color="EEF3F8", fill_type="solid")
            data_font = Font(name="Calibri", size=9)
            money_cols = {col_idx for col_idx, h in enumerate(HEADERS, 1)
                          if h in ('Total','Subtotal','Discount','Tax','Unit Price','Line Amount')}

            for row_idx, row_data in enumerate(rows[1:], start=2):
                for col_idx, value in enumerate(row_data, start=1):
                    cell = ws.cell(row=row_idx, column=col_idx, value=value)
                    cell.font      = data_font
                    cell.border    = cell_border
                    cell.alignment = Alignment(vertical="center", wrap_text=False)
                    if row_idx % 2 == 0:
                        cell.fill = alt_fill
                    if col_idx in money_cols:
                        cell.alignment = Alignment(horizontal="right", vertical="center")

            # ── Auto-filter on header row
            ws.auto_filter.ref = ws.dimensions

            wb.save(EXCEL_FILE)
            log.info(f"Live Excel updated → {EXCEL_FILE.name}  ({len(rows)-1} data rows)")

    except ImportError:
        log.error("openpyxl not installed. Run: pip install openpyxl")
    except Exception as exc:
        log.error(f"Excel update failed: {exc}")


# ─── MAIN FILE HANDLER ────────────────────────────────────────────────────────
def handle_new_file(file_path: Path):
    """Full RPA automation workflow for one PDF."""
    file_path = Path(file_path)
    if file_path.suffix.lower() != '.pdf' or not file_path.exists():
        return

    log.info(f"─── Processing: {file_path.name} ───")

    # Wait until file is fully written to disk
    if not wait_for_file_stable(file_path):
        log.error(f"Timeout waiting for file to stabilise: {file_path.name}")
        _move_to(file_path, FAILED_DIR)
        return

    global currently_processing
    with processing_lock:
        currently_processing = file_path.name

    try:
        # ── Guard: refuse to process if engine is not configured
        if not is_configured():
            log.error(
                f"Cannot process '{file_path.name}' — no API key is set. "
                "Open the Settings panel in the UI and save your Gemini API key first."
            )
            with processing_lock:
                currently_processing = None
            return   # leave the file in Inbox so it gets retried after key is saved

        # ── Extract data
        if ENGINE == 'gemini':
            data = process_with_gemini(file_path)
        else:
            data = process_with_ollama(file_path)

        # ── Write to CSV and Excel
        append_to_database(file_path.name, data)
        update_excel_file()

        # ── Move to Processed
        _move_to(file_path, PROCESSED_DIR)
        log.info(f"✔ Success: '{file_path.name}' → {len(data.get('line_items') or [])} line item(s) extracted.\n")

    except Exception as exc:
        log.error(f"✘ Failed '{file_path.name}': {exc}\n")
        _move_to(file_path, FAILED_DIR)

    finally:
        with processing_lock:
            currently_processing = None

        # Respectful delay between consecutive Gemini API calls to avoid 429s
        if ENGINE == 'gemini' and is_configured():
            time.sleep(INTER_FILE_DELAY)


def _move_to(src: Path, dest_dir: Path):
    dest = dest_dir / src.name
    if dest.exists():
        dest = dest_dir / f"{src.stem}_{int(time.time())}{src.suffix}"
    try:
        shutil.move(str(src), str(dest))
    except Exception as exc:
        log.error(f"Could not move {src.name} → {dest_dir.name}: {exc}")


# ─── WORKER THREAD (FIX #1: serialised queue — one file at a time) ────────────
def _queue_worker():
    """Continuously pull files off the queue and process them one by one."""
    while True:
        file_path = _file_queue.get()
        if file_path is None:   # poison pill — shut down
            break
        handle_new_file(file_path)
        _file_queue.task_done()


def enqueue_file(file_path):
    """Thread-safe way to add a file to the processing queue."""
    _file_queue.put(Path(file_path))


# ─── WATCHDOG ─────────────────────────────────────────────────────────────────
class PDFHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.lower().endswith('.pdf'):
            log.info(f"Watchdog detected new file: {Path(event.src_path).name}")
            enqueue_file(event.src_path)


# ─── BOT ENTRY POINT ──────────────────────────────────────────────────────────
def start_bot():
    global _worker_thread
    setup_environment()

    log.info("═══════════════════════════════════════")
    log.info("  RPA Bot Started")
    log.info(f"  Engine  : {ENGINE.upper()}")
    log.info(f"  Inbox   : {INBOX_DIR}")
    log.info(f"  Database: {DATABASE_FILE}")
    log.info(f"  Excel   : {EXCEL_FILE}")
    log.info("  Drop PDFs into the Inbox folder…")
    log.info("═══════════════════════════════════════")

    # Start the single worker thread (serialises all processing)
    _worker_thread = threading.Thread(target=_queue_worker, name="rpa-worker", daemon=True)
    _worker_thread.start()

    # Enqueue any PDFs already sitting in Inbox
    existing = sorted(INBOX_DIR.glob("*.pdf"))
    if existing:
        log.info(f"Found {len(existing)} existing PDF(s) in Inbox — queuing…")
    for f in existing:
        enqueue_file(f)

    # Watch for new files
    handler  = PDFHandler()
    observer = Observer()
    observer.schedule(handler, str(INBOX_DIR), recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        _file_queue.put(None)   # stop the worker gracefully
        log.info("RPA Bot stopped.")
    observer.join()


if __name__ == "__main__":
    start_bot()