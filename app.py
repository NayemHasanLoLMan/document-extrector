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

CSV_HEADERS = [
    'Timestamp', 'File Name', 'Doc Type', 'Ref Number', 'Order ID', 'Date', 'Due Date',
    'Vendor', 'Vendor Address', 'Vendor Contact',
    'Client', 'Client Address', 'Ship To Address', 'Ship Mode', 'Currency',
    'Total', 'Balance Due', 'Subtotal', 'Discount', 'Shipping', 'Tax', 'Tax Rate', 'Payment Terms',
    'Line Description', 'SKU/Category', 'Qty', 'Unit Price', 'Line Amount', 'Notes'
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
                "timestamp":    row.get("Timestamp", ""),
                "file_name":    row.get("File Name", ""),
                "doc_type":     row.get("Doc Type", ""),
                "ref_num":      row.get("Ref Number", ""),
                "order_id":     row.get("Order ID", ""),
                "date":         row.get("Date", ""),
                "due_date":     row.get("Due Date", ""),
                "vendor_name":  row.get("Vendor", ""),
                "vendor_addr":  row.get("Vendor Address", ""),
                "vendor_contact": row.get("Vendor Contact", ""),
                "client_name":  row.get("Client", ""),
                "client_addr":  row.get("Client Address", ""),
                "ship_to_address": row.get("Ship To Address", ""),
                "ship_mode":    row.get("Ship Mode", ""),
                "currency":     row.get("Currency", ""),
                "total":        row.get("Total", ""),
                "balance_due":  row.get("Balance Due", ""),
                "subtotal":     row.get("Subtotal", ""),
                "discount":     row.get("Discount", ""),
                "shipping":     row.get("Shipping", ""),
                "tax":          row.get("Tax", ""),
                "tax_rate":     row.get("Tax Rate", ""),
                "payment_terms": row.get("Payment Terms", ""),
                "description":  row.get("Line Description", ""),
                "sku_category": row.get("SKU/Category", ""),
                "quantity":     row.get("Qty", ""),
                "unit_price":   row.get("Unit Price", ""),
                "amount":       row.get("Line Amount", ""),
                "notes":        row.get("Notes", ""),
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


# ── Clear API (FIX #4: removed dead code after return) ───────────────────────
@app.route('/api/clear', methods=['POST'])
def clear_data():
    # Reset CSV
    with open(DATABASE_FILE, 'w', newline='', encoding='utf-8') as f:
        csv.writer(f).writerow(CSV_HEADERS)

    # Reset Excel (FIX #8: no pandas required — use openpyxl directly)
    try:
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Extracted Data"
        ws.append(CSV_HEADERS)
        wb.save(EXCEL_FILE)
    except ImportError:
        pass  # openpyxl not available — skip Excel reset

    # Remove processed and failed files
    for folder in (PROCESSED_DIR, FAILED_DIR):
        for f in folder.glob("*.pdf"):
            try:
                f.unlink()
            except OSError:
                pass

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