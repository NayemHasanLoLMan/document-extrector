import os
import csv
import threading
from flask import Flask, jsonify, request, send_from_directory, make_response
from flask_cors import CORS
from pathlib import Path

import rpa_bot

app  = Flask(__name__)
CORS(app)

BASE_DIR      = Path(__file__).parent
INBOX_DIR     = BASE_DIR / "Inbox"
PROCESSED_DIR = BASE_DIR / "Processed"
FAILED_DIR    = BASE_DIR / "Failed"
DATABASE_FILE = BASE_DIR / "Extracted_Database.csv"
EXCEL_FILE    = BASE_DIR / "Extracted_Database.xlsx"

# Must match HEADERS in rpa_bot.py exactly
CSV_HEADERS = [
    'Timestamp', 'File Name',
    'Invoice #', 'Order ID', 'Date',
    'Vendor', 'Client', 'Ship To Address', 'Ship Mode',
    'Currency', 'Subtotal', 'Discount', 'Shipping', 'Tax', 'Tax Rate', 'Total', 'Balance Due',
    'Payment Terms', 'Notes',
    'Item Description', 'SKU / Category', 'Qty', 'Unit Price', 'Line Amount',
]


# ── Frontend ──────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    resp = make_response(send_from_directory(BASE_DIR, 'index.html'))
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    resp.headers["Pragma"]        = "no-cache"
    resp.headers["Expires"]       = "0"
    return resp


# ── Data API ──────────────────────────────────────────────────────────────────
@app.route('/api/data', methods=['GET'])
def get_data():
    if not DATABASE_FILE.exists():
        return jsonify([])

    rows = []
    with open(DATABASE_FILE, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            rows.append({
                "timestamp":        row.get("Timestamp", ""),
                "file_name":        row.get("File Name", ""),
                "ref_num":          row.get("Invoice #", ""),
                "order_id":         row.get("Order ID", ""),
                "date":             row.get("Date", ""),
                "vendor_name":      row.get("Vendor", "SuperStore"),
                "client_name":      row.get("Client", ""),
                "ship_to_address":  row.get("Ship To Address", ""),
                "ship_mode":        row.get("Ship Mode", ""),
                "currency":         row.get("Currency", "USD"),
                "subtotal":         row.get("Subtotal", ""),
                "discount":         row.get("Discount", ""),
                "shipping":         row.get("Shipping", ""),
                "tax":              row.get("Tax", ""),
                "tax_rate":         row.get("Tax Rate", ""),
                "total":            row.get("Total", ""),
                "balance_due":      row.get("Balance Due", ""),
                "payment_terms":    row.get("Payment Terms", ""),
                "notes":            row.get("Notes", ""),
                "description":      row.get("Item Description", ""),
                "sku_category":     row.get("SKU / Category", ""),
                "quantity":         row.get("Qty", ""),
                "unit_price":       row.get("Unit Price", ""),
                "amount":           row.get("Line Amount", ""),
            })
    return jsonify(rows)


# ── Status API ────────────────────────────────────────────────────────────────
@app.route('/api/status', methods=['GET'])
def get_status():
    inbox_files  = [f.name for f in INBOX_DIR.glob('*.pdf')]
    failed_files = [f.name for f in FAILED_DIR.glob('*.pdf')]
    queue_size   = rpa_bot._file_queue.qsize()

    with rpa_bot.processing_lock:
        current = rpa_bot.currently_processing

    return jsonify({
        "processing":          inbox_files,
        "failed":              failed_files,
        "currently_processing": current,
        "queue_depth":         queue_size,
        "engine":              rpa_bot.ENGINE,
    })


# ── Upload API ────────────────────────────────────────────────────────────────
@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part in request"}), 400

    file = request.files['file']
    if not file.filename:
        return jsonify({"error": "No file selected"}), 400

    if not file.filename.lower().endswith('.pdf'):
        return jsonify({"error": "Only PDF files are accepted"}), 400

    dest = INBOX_DIR / file.filename
    file.save(str(dest))

    # Enqueue immediately so watchdog is not needed for uploads
    rpa_bot.enqueue_file(dest)
    return jsonify({"success": True, "filename": file.filename})


@app.route('/api/clear', methods=['POST'])
def clear_data():
    errors = []

    # Reset CSV
    try:
        with open(DATABASE_FILE, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(CSV_HEADERS)
    except Exception as e:
        errors.append(f"CSV reset failed: {e}")

    # Reset Excel — write to a temp file first to avoid Windows file-lock errors
    try:
        from openpyxl import Workbook
        import tempfile, shutil, os
        tmp_fd, tmp_path = tempfile.mkstemp(suffix='.xlsx', dir=BASE_DIR)
        os.close(tmp_fd)
        wb = Workbook()
        ws = wb.active
        ws.title = "Extracted Data"
        ws.append(CSV_HEADERS)
        wb.save(tmp_path)
        # On Windows, must delete the locked file before moving the new one in
        try:
            if EXCEL_FILE.exists():
                os.remove(str(EXCEL_FILE))
        except PermissionError:
            os.remove(tmp_path)  # clean up temp
            raise PermissionError("Excel file is open — please close it in Excel and click Clear again.")
        shutil.move(tmp_path, str(EXCEL_FILE))
    except PermissionError as e:
        errors.append(str(e))
    except ImportError:
        pass  # openpyxl not installed — skip Excel reset
    except Exception as e:
        errors.append(f"Excel reset failed: {e}")

    # Remove processed and failed files
    for folder in (PROCESSED_DIR, FAILED_DIR):
        for f in folder.glob("*.pdf"):
            try:
                f.unlink()
            except OSError:
                pass

    if errors:
        return jsonify({"success": False, "errors": errors}), 207  # partial success
    return jsonify({"success": True})


# ── Download API ──────────────────────────────────────────────────────────────
@app.route('/api/download/csv', methods=['GET'])
def download_csv():
    if not DATABASE_FILE.exists():
        return jsonify({"error": "No data yet"}), 404
    return send_from_directory(BASE_DIR, DATABASE_FILE.name, as_attachment=True)


@app.route('/api/download/xlsx', methods=['GET'])
def download_xlsx():
    if not EXCEL_FILE.exists():
        return jsonify({"error": "Excel file not generated yet"}), 404
    return send_from_directory(BASE_DIR, EXCEL_FILE.name, as_attachment=True)


# ── Settings API ──────────────────────────────────────────────────────────────
@app.route('/api/settings', methods=['POST'])
def update_settings():
    body = request.json or {}
    was_unconfigured = not rpa_bot.is_configured()

    if "engine" in body:
        rpa_bot.ENGINE = body["engine"]

    if body.get("api_key"):
        rpa_bot.GEMINI_API_KEY = body["api_key"].strip()
        if rpa_bot.ENGINE == 'gemini':
            import google.generativeai as genai
            genai.configure(api_key=rpa_bot.GEMINI_API_KEY)

    if "ollama_model" in body:
        rpa_bot.OLLAMA_MODEL = body["ollama_model"]

    if "inter_file_delay" in body:
        try:
            rpa_bot.INTER_FILE_DELAY = int(body["inter_file_delay"])
        except ValueError:
            pass

    # Persist to disk so settings survive server restarts (Fix Bug 8)
    rpa_bot.save_config()

    # If the bot was unconfigured and now has a key, re-enqueue any waiting Inbox files
    if was_unconfigured and rpa_bot.is_configured():
        waiting = list(INBOX_DIR.glob("*.pdf"))
        if waiting:
            import logging
            logging.getLogger("rpa_bot").info(
                f"API key received — re-queueing {len(waiting)} waiting file(s)."
            )
            for f in sorted(waiting):
                rpa_bot.enqueue_file(f)

    return jsonify({
        "success": True,
        "engine": rpa_bot.ENGINE,
        "configured": rpa_bot.is_configured()
    })


# ── Startup ───────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    # FIX #5: setup_environment called only once here; start_bot calls it too
    # but it is idempotent (mkdir exist_ok + file-exists check), so no harm.
    rpa_bot.setup_environment()

    # Start the RPA Bot (watchdog + worker queue) in a background daemon thread
    bot_thread = threading.Thread(target=rpa_bot.start_bot, name="rpa-bot", daemon=True)
    bot_thread.start()

    print("\n" + "="*55)
    print("  PDF -> Excel RPA System")
    print("  Web UI  ->  http://localhost:5000")
    print("  Drop PDFs into the Inbox/ folder to process")
    print("="*55 + "\n")

    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)