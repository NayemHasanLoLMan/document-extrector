"""
ocr_extractor.py
────────────────
OCR pipeline for image-based / scanned PDF invoices.
Uses PyMuPDF to render pages + EasyOCR for text recognition.
Supports 3 detected formats plus a generic Ollama fallback.

Formats detected:
  1. COATS Hong Kong (GTL-style)
  2. Avery Dennison (TAX INVOICE)
  3. Brilliant Summit Limited
  4. Generic → Ollama fallback
"""

import re
import json
import logging
import requests
from pathlib import Path

log = logging.getLogger("rpa_bot")

# ── Lazy-loaded OCR reader (avoid loading at import time) ──────────────────────
_ocr_reader = None

def _get_reader():
    global _ocr_reader
    if _ocr_reader is None:
        import easyocr
        log.info("Loading EasyOCR model (first time — may take 30s)…")
        _ocr_reader = easyocr.Reader(['en'], gpu=False, verbose=False)
        log.info("EasyOCR ready.")
    return _ocr_reader


# ── Render PDF → OCR items list ───────────────────────────────────────────────
def ocr_pdf(file_path: Path, zoom: float = 2.5) -> list:
    """
    Render each PDF page and run EasyOCR.
    Returns list of dicts: {x, y, text, conf}
    sorted in reading order (top→bottom, left→right).
    """
    import fitz
    reader = _get_reader()
    items = []
    doc = fitz.open(str(file_path))
    for page in doc:
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat)
        img_bytes = pix.tobytes('png')
        results = reader.readtext(img_bytes, detail=1, paragraph=False)
        for (bbox, text, conf) in results:
            x = int(bbox[0][0])
            y = int(bbox[0][1])
            items.append({'x': x, 'y': y, 'text': text.strip(), 'conf': conf})
    doc.close()
    items.sort(key=lambda r: (r['y'], r['x']))
    return items


def items_to_text(items: list, conf_threshold: float = 0.3) -> str:
    """
    Group OCR items into lines by y-proximity, return plain text.
    Low-confidence junk is filtered out.
    """
    if not items:
        return ""
    rows: dict = {}
    for it in items:
        if it['conf'] < conf_threshold:
            continue
        row_key = it['y'] // 18
        rows.setdefault(row_key, []).append(it)
    lines = []
    for k in sorted(rows.keys()):
        row_items = sorted(rows[k], key=lambda r: r['x'])
        lines.append("  ".join(r['text'] for r in row_items))
    return "\n".join(lines)


# ── Format detection ───────────────────────────────────────────────────────────
def detect_format(text: str) -> str:
    t = text.upper()
    if "COATS" in t or re.search(r'GTL\d+', t):
        return "coats"
    if "AVERY" in t and "DENNISON" in t:
        return "avery"
    if "BRILLIANT SUMMIT" in t:
        return "brilliant"
    return "generic"


# ── Helpers ───────────────────────────────────────────────────────────────────
def _get(text: str, pattern: str, default: str = "") -> str:
    m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    return m.group(1).strip() if m else default


def _clean_num(s: str) -> str:
    """Remove OCR noise from a number string, keep digits/commas/dots."""
    return re.sub(r'[^\d,.]', '', s)


def _empty_invoice() -> dict:
    return {
        "document_type": "Invoice",
        "reference_number": "",
        "order_id": "",
        "date": "",
        "due_date": "",
        "vendor_name": "",
        "vendor_address": "",
        "vendor_contact": "",
        "client_name": "",
        "client_address": "",
        "ship_to_address": "",
        "ship_mode": "",
        "currency": "",
        "subtotal": "",
        "discount": "",
        "shipping_cost": "",
        "tax": "",
        "tax_rate": "",
        "total": "",
        "balance_due": "",
        "payment_terms": "",
        "notes": "",
        "line_items": [],
    }


# ── COATS Hong Kong parser ─────────────────────────────────────────────────────
def parse_coats(items: list, text: str) -> dict:
    data = _empty_invoice()
    data["vendor_name"] = "COATS HONG KONG LIMITED"
    data["currency"]    = _get(text, r'Currency[:\s]+([A-Z]{3})')

    # Reference: GTLxxxxxxx (OCR may render O as 0 — keep as-is)
    m = re.search(r'\b(GTL[A-Z0-9]+)\b', text, re.IGNORECASE)
    if m:
        data["reference_number"] = m.group(1).strip()

    # Invoice number — OCR garbles label badly ("Tnvoice Nuber", "Tnvoice Number", etc.)
    # Strategy: find any sequence of 7-10 digits that follows label-like text near the Bill-To block
    inv_m = re.search(r'(?:[Ii]nvo?i?c?e?|Nuber|Number)[^\d\n]{0,20}(\d{7,12})', text)
    if inv_m:
        data["order_id"] = inv_m.group(1)

    # Date (OCR: "Date;  30. 03 . 2026" or "Billing Date:")
    m_date = re.search(r'(?:Billing\s*)?Date[;:\s]+([\d]+[.\s]+[\d]+[.\s]+[\d]+)', text, re.IGNORECASE)
    if m_date:
        data["date"] = re.sub(r'\s+', '', m_date.group(1))  # "30.03.2026"

    # Delivery date
    data["due_date"] = _get(text, r'Delivery\s*Date[;:\s]+([\d.\s]+?)(?=\n|Currency)')

    # Payment terms — stop at newline or Payment Method
    data["payment_terms"] = _get(text, r'Payment\s*terms?[:\s]+([^\n]+?)(?:\s{2,}|$)')

    # Client / Bill-To: line after "Bill-To Party  NNNNN  ..."
    # OCR text: "ALPH START LTD  Tnvoice Nuber:  4042350910"
    m = re.search(r'Bill.To\s*Party\s+\d+[^\n]*\n([^\n]+)', text, re.IGNORECASE)
    if m:
        raw = m.group(1).strip()
        # Strip any trailing invoice-number noise
        raw = re.split(r'\s{2,}', raw)[0]
        data["client_name"] = raw

    # Client address lines
    m = re.search(r'Bill.To\s*Party\s+\d+[^\n]*\n.+?\n([^\n]+\n[^\n]+\n[^\n]+)', text, re.IGNORECASE | re.DOTALL)
    if m:
        addr_lines = [l.strip() for l in m.group(1).splitlines() if l.strip()]
        # Stop at ship-to or currency lines
        clean = [l for l in addr_lines if not re.search(r'Currency|Payment|Ship|Shipment', l, re.I)]
        data["client_address"] = ", ".join(clean[:3])

    # Payer Name (more reliable than Bill-To OCR)
    payer = _get(text, r'Payer\s*Name[:\s]+(.+?)(?:\n|$)')
    if payer and not data["client_name"]:
        data["client_name"] = payer.strip()

    # Ship-To (after "Ship-To Party" label)
    m = re.search(r'Ship.To\s*Party[^\n]*\n([^\n]+\n[^\n]+\n[^\n]+)', text, re.IGNORECASE | re.DOTALL)
    if m:
        ship_lines = [l.strip() for l in m.group(1).splitlines() if l.strip()]
        clean = [l for l in ship_lines if not re.search(r'Others|Order|Payer|Route', l, re.I)]
        data["ship_to_address"] = ", ".join(clean[:4])

    # Totals: "Total Invoice Value" may appear AFTER the amount on next line
    # OCR layout:  "Total Net Value  2,405.40"  then  "2,405.40"  then  "Total Invoice Value"
    m_total = re.search(r'Total\s*Invoice\s*Value\s+([\d,]+\.?\d+)', text, re.IGNORECASE)
    if not m_total:
        # value on line before label
        m_total = re.search(r'([\d,]+\.\d{2})\s*\nTotal\s*Invoice\s*Value', text, re.IGNORECASE)
    if not m_total:
        # value on line after label
        m_total = re.search(r'Total\s*Invoice\s*Value\n([\d,]+\.\d+)', text, re.IGNORECASE)
    if m_total:
        data["total"] = m_total.group(1)
        data["balance_due"] = m_total.group(1)

    # Subtotal from "Total Net Value" (same ambiguity)
    m_sub = re.search(r'Total\s*Net\s*Value\s+([\d,]+\.?\d+)', text, re.IGNORECASE)
    if not m_sub:
        m_sub = re.search(r'([\d,]+\.\d{2})\s*\nTotal\s*Net\s*Value', text, re.IGNORECASE)
    if m_sub:
        data["subtotal"] = m_sub.group(1)

    # Line items — regex on OCR text first
    line_items = _coats_line_items_regex(text)
    if not line_items:
        line_items = _coats_items_from_coords(items)

    data["line_items"] = line_items or [{"description": "(see document)", "sku_or_category": "",
                                          "quantity": "", "unit_price": "", "amount": ""}]
    return data


def _coats_line_items_regex(text: str) -> list:
    """
    Extract COATS line items. Each row: optional_code then Vicone qty line.
    Looks backwards from each Vicone row for the NEAREST unique material code,
    bounded by the PREVIOUS Vicone row so we don't reuse the same code.
    """
    items = []
    vicone_re = re.compile(
        r'(\d+)\s+Vicone\s+([\d.]+)\s*KG\s+([\d.]+)\s+([\d,]+\.\d+)',
        re.IGNORECASE
    )
    vicone_matches = list(vicone_re.finditer(text))

    prev_end = 0  # start of search window for each item's code
    for m in vicone_matches:
        window = text[prev_end:m.start()]
        # Find the LAST material code in the window
        code_m = None
        for mc in re.finditer(r'([A-Z0-9]{6,}-[A-Z0-9]+)', window, re.IGNORECASE):
            code_m = mc
        code = code_m.group(1) if code_m else "Material"
        items.append({
            "description":     code,
            "sku_or_category": "Vicone",
            "quantity":        m.group(1) + " Vicone",
            "unit_price":      m.group(3),
            "amount":          m.group(4),
        })
        prev_end = m.end()  # next search starts after this Vicone row
    return items


def _coats_items_from_coords(items: list) -> list:
    """
    Use x-coordinate ranges to identify table columns for COATS format.
    Columns (approx, at 2.5x zoom):
      Material: x < 420
      Qty:      x 600-750
      Weight:   x 760-860
      NetPrice: x 900-1050
      NetValue: x 1080+
    """
    # Find rows in the item table band (y roughly 1000-1200)
    table_items = [it for it in items if 950 < it['y'] < 1250 and it['conf'] > 0.4]
    # Group by y
    rows: dict = {}
    for it in table_items:
        yk = it['y'] // 25
        rows.setdefault(yk, []).append(it)

    line_items = []
    for k in sorted(rows.keys()):
        row = sorted(rows[k], key=lambda r: r['x'])
        # Need at least material + qty + price + value
        if len(row) < 3:
            continue
        texts = [r['text'] for r in row]
        # Check if any text looks like a material code
        if not any(re.match(r'\d{7}', t) for t in texts):
            continue
        desc = row[0]['text']
        qty = ""
        price = ""
        amount = ""
        for r in row:
            if re.search(r'Vicone', r['text'], re.I):
                qty = r['text']
            elif r['x'] > 900 and re.search(r'[\d.]+', r['text']):
                if not price:
                    price = _clean_num(r['text'])
                else:
                    amount = _clean_num(r['text'])
        line_items.append({
            "description": desc,
            "sku_or_category": "Vicone",
            "quantity": qty,
            "unit_price": price,
            "amount": amount,
        })
    return line_items


# ── Avery Dennison parser ──────────────────────────────────────────────────────
def parse_avery(items: list, text: str) -> dict:
    data = _empty_invoice()
    data["vendor_name"]    = "Avery Dennison Gulf FZCO"
    data["currency"]       = "USD"

    data["reference_number"] = _get(text, r'Invoice\s*Number[:\s]+(\d+)')
    data["date"]             = _get(text, r'Date[:\s]+(\d{2}-[A-Z]{3}-\d{4})')
    # Payment terms: stop before "Terms of Delivery"
    data["payment_terms"]    = _get(text, r'Terms\s*of\s*Payments?[:\s]+([^\n]+?)(?:\s{2,}Terms\s*of|$)')

    # Consignee / Ship To (label then name on next line)
    m = re.search(r'Consignee[lI]?Ship\s*To\s*(?:Buyer[^\n]*)?\n([^\n]+)', text, re.IGNORECASE)
    if m:
        data["ship_to_address"] = m.group(1).strip()
        # Next lines = address
        rest = text[m.end():]
        addr_lines = []
        for line in rest.splitlines():
            line = line.strip()
            if not line or re.search(r'TEL|FAX|TRN#|Terms|Buyer|Bill', line, re.I):
                break
            addr_lines.append(line)
        data["client_name"] = data["ship_to_address"]
        data["ship_to_address"] = ", ".join(addr_lines) if addr_lines else data["ship_to_address"]

    # Bill To / Buyer other than Consignee — this is the ACTUAL buyer (overrides consignee)
    m = re.search(r'Buyer\s*other\s*than[^\n]*\n([^\n]+)', text, re.IGNORECASE)
    if m:
        buyer_name = m.group(1).strip()
        rest = text[m.end():]
        addr_lines = []
        for line in rest.splitlines():
            line = line.strip()
            if not line or re.search(r'TEL|FAX|TRN#|Terms|Buyer|Description|Exporter', line, re.I):
                break
            # Skip lines that look like the consignee address (already captured)
            if re.search(r'TOWER|ROAD|HONG KONG|KOWLOON', line, re.I):
                addr_lines.append(line)
        # Bill-To is the buyer — override the consignee client_name
        data["client_name"]    = buyer_name
        data["client_address"] = ", ".join(addr_lines)

    # Vendor address
    data["vendor_address"] = _get(text, r'Avery Dennison Gulf FZCO\n([^\n]+)')

    # Sales Order / PI refs
    sales_orders = re.findall(r'PI\s+\d+', text, re.IGNORECASE)
    if sales_orders:
        data["order_id"] = ", ".join(dict.fromkeys(sales_orders))

    # Line items using coordinate-based approach
    data["line_items"] = _avery_items_from_coords(items)

    # Totals — sum amounts as fallback
    if not data["total"] and data["line_items"]:
        try:
            s = sum(float(_clean_num(it.get("amount", "0")).replace(",", ""))
                    for it in data["line_items"] if it.get("amount"))
            data["total"] = str(round(s, 2))
            data["subtotal"] = data["total"]
            data["balance_due"] = data["total"]
        except Exception:
            pass

    return data


def _avery_items_from_coords(items: list) -> list:
    """
    Avery Dennison table columns (approx at 2.5x zoom):
      Description block: x < 420
      HS Code:           x 430-510
      Country:           x 550-650
      Quantity:          x 680-800
      UOM:               x 820-890
      Rate USD:          x 910-1010
      Amount USD:        x 1030-1180
      Total Amount:      x 1340+
    """
    # Table rows roughly y > 620
    table_items = [it for it in items if it['y'] > 620 and it['conf'] > 0.35]

    # Group into row bands
    rows: dict = {}
    for it in table_items:
        yk = it['y'] // 22
        rows.setdefault(yk, []).append(it)

    line_items = []
    current = None
    num_re = re.compile(r'^[\d,]+\.?\d*$')

    for k in sorted(rows.keys()):
        row = sorted(rows[k], key=lambda r: r['x'])
        texts_by_x = [(r['x'], r['text']) for r in row]

        # Detect a "PI" line — starts a new line item
        pi_match = next((t for _, t in texts_by_x if re.match(r'PI\s+\d+', t, re.I)), None)
        if pi_match:
            if current:
                line_items.append(current)
            current = {
                "description": pi_match,
                "sku_or_category": "",
                "quantity": "",
                "unit_price": "",
                "amount": "",
            }
            # Look for numeric columns in same row
            for x, t in texts_by_x:
                clean = _clean_num(t)
                if x > 680 and x < 800 and re.search(r'[\d,]+', t):
                    current["quantity"] = t
                if x > 910 and x < 1050 and num_re.match(clean):
                    current["unit_price"] = clean
                if x > 1030 and x < 1200 and num_re.match(clean):
                    current["amount"] = clean
            continue

        # Detect quantity / price row (has large number + Each)
        has_each = any(t.lower() == 'each' for _, t in texts_by_x)
        if has_each and current is not None:
            for x, t in texts_by_x:
                clean = _clean_num(t)
                if x > 680 and x < 820 and re.search(r'[\d,]+', t) and t.lower() != 'each':
                    if not current["quantity"]:
                        current["quantity"] = t
                if x > 910 and x < 1050 and num_re.match(clean):
                    current["unit_price"] = clean
                if x > 1030 and x < 1200 and num_re.match(clean):
                    current["amount"] = clean
                if x > 1300 and num_re.match(clean):
                    current["amount"] = clean
            continue

        # Description continuation lines (x < 420)
        if current is not None:
            desc_parts = [t for x, t in texts_by_x if x < 420]
            if desc_parts and not any(w in " ".join(desc_parts).upper()
                                       for w in ["END OF LIST", "PAGE"]):
                current["description"] += " " + " ".join(desc_parts)

    if current:
        line_items.append(current)

    return [it for it in line_items
            if it.get("quantity") or it.get("amount")]


# ── Brilliant Summit parser ────────────────────────────────────────────────────
def parse_brilliant(items: list, text: str) -> dict:
    data = _empty_invoice()
    data["vendor_name"]    = "Brilliant Summit Limited"
    data["vendor_address"] = "Room B, 9/F., Primoknit Industrial Building, 7-9 Kung Yip St, Kwai Chung, N.T., H.K."
    data["currency"]       = "HKD"

    # Invoice No: OCR may split "INV/2026/00040 1" → join and clean
    m_inv = re.search(r'Invoice\s*No\.?\s+([\w/]+(?:\s+\d+)?)', text, re.IGNORECASE)
    if m_inv:
        raw_inv = re.sub(r'\s+', '', m_inv.group(1))  # remove spaces inside
        data["reference_number"] = raw_inv

    data["date"]      = _get(text, r'Invoice\s*Date\s+([\d/]+)')
    data["order_id"] = _get(text, r'Customer\s*P\.?O[.:\s]+([^\n]+?)(?=\n|Salesman|\s{3})')

    # Client name: appears BEFORE "Customer Name" label in OCR reading order
    # OCR line: "Alpha Start Ltd. [CL-AS0002]  Invoice No.  INV/..."
    m_cust = re.search(r'^([A-Za-z][^\n]+?)\s{2,}Invoice\s*No', text, re.IGNORECASE | re.MULTILINE)
    if m_cust:
        data["client_name"] = m_cust.group(1).strip()
    else:
        data["client_name"] = _get(text, r'Customer\s*Name\s*[:\s]*([^\n]+)')

    # Address: after "Address" label
    m_addr = re.search(r'Address\s+(.+?)(?=\n.*(?:Revised|Customer P|Salesman)|$)',
                        text, re.IGNORECASE | re.DOTALL)
    if m_addr:
        addr = " ".join(m_addr.group(1).split())
        # Remove trailing noise like "Revised"
        addr = re.sub(r'\s*Revised\s*$', '', addr, flags=re.IGNORECASE).strip()
        data["client_address"] = addr
        data["ship_to_address"] = addr

    # Totals
    data["total"]    = _clean_num(_get(text, r'Total\s*Amount\s+([\d,]+\.?\d+)'))
    data["subtotal"] = data["total"]
    data["balance_due"] = _clean_num(_get(text, r'Net\s*Amount\s+([\d,]+\.?\d+)'))
    if not data["balance_due"]:
        data["balance_due"] = data["total"]

    # Line items — use OCR text parsing (more reliable for this format)
    data["line_items"] = _brilliant_items_from_text(text) or _brilliant_items_from_coords(items)
    return data


def _brilliant_items_from_text(text: str) -> list:
    """
    Parse Brilliant Summit line items from OCR text.
    OCR lines (may have OCR space inside code e.g. '123/0000 1'):
      123/0000 1  1/2" CELLO TAPE  645.00 ROLL  0.382  246.520
      123/00028   3" PLASTIC ...   17.00  BOX  14.5001  246.500
    Strategy: scan line-by-line looking for lines that START with a
    product-code pattern (digits/digits, possibly with a space).
    """
    items = []
    # Normalise: collapse runs of spaces to two spaces (column separator)
    lines = text.splitlines()

    # Pattern for a product code at start of line (OCR may insert a space before last digit)
    # Matches e.g. "23/00001" or "123/0000 1" or "123/00028"
    code_start = re.compile(
        r'^(\d{2,3}/\d{3,4}(?:\s?\d)?)\s+(.+)',   # code then rest
        re.IGNORECASE
    )
    # Number pattern
    num_re = re.compile(r'^[\d,]+\.\d+')

    for line in lines:
        line = line.strip()
        m = code_start.match(line)
        if not m:
            continue
        code_raw = re.sub(r'\s+', '', m.group(1))   # "23/00001" (strip OCR space)
        rest     = m.group(2).strip()

        # rest looks like: DESCRIPTION  qty  UOM  price  amount
        # Split on 2+ spaces first
        parts = re.split(r'\s{2,}', rest)

        if len(parts) >= 4:
            # parts[0]=desc, then numbers follow
            desc = parts[0].strip()
            nums = []
            for p in parts[1:]:
                p = p.strip()
                # Handle "645.00 ROLL" — split qty+uom
                qty_uom = re.match(r'([\d,]+\.?\d*)\s+([A-Z]+)', p, re.I)
                if qty_uom:
                    nums.append(qty_uom.group(1) + ' ' + qty_uom.group(2))
                elif num_re.match(p):
                    nums.append(p)
            qty    = nums[0] if len(nums) > 0 else ""
            price  = nums[1] if len(nums) > 1 else ""
            amount = nums[2] if len(nums) > 2 else ""
        else:
            # Fallback: just grab last two numbers as price/amount
            all_nums = re.findall(r'[\d,]+\.\d+', rest)
            desc  = re.sub(r'[\d,. ]+$', '', rest).strip()
            qty   = ""
            price = all_nums[-2] if len(all_nums) >= 2 else ""
            amount= all_nums[-1] if all_nums else ""

        # Fix OCR artifact: trailing digit stuck to price (e.g. "14.5001" → "14.500")
        if price and re.match(r'^[\d]+\.\d{3}\d+$', price):
            price = price[:-1]  # drop last digit if > 3 decimal places

        items.append({
            "description":     desc,
            "sku_or_category": code_raw,
            "quantity":        qty,
            "unit_price":      price,
            "amount":          amount,
        })
    return items


def _brilliant_items_from_coords(items: list) -> list:
    """
    Brilliant Summit table columns (at 2.5x zoom):
      Product Code: x ~83
      Description:  x ~274
      Quantity+UOM: x ~880-1040
      Unit Price:   x ~1174
      Amount:       x ~1350
    """
    # Table items (y > 700, below headers)
    table_items = [it for it in items if 700 < it['y'] < 1250 and it['conf'] > 0.35]

    rows: dict = {}
    for it in table_items:
        yk = it['y'] // 22
        rows.setdefault(yk, []).append(it)

    line_items = []
    prod_re = re.compile(r'^\d{2,3}/\d{4,5}$')  # e.g. 23/00001

    for k in sorted(rows.keys()):
        row = sorted(rows[k], key=lambda r: r['x'])
        texts_by_x = [(r['x'], r['text']) for r in row]

        # A product row starts with a product code at far left
        code = next((t for x, t in texts_by_x if x < 200 and prod_re.match(t)), None)
        if not code:
            # Try relaxed match
            code = next((t for x, t in texts_by_x if x < 200 and re.match(r'\d+/\d+', t)), None)

        if code:
            desc = next((t for x, t in texts_by_x if 200 < x < 800), "")
            # Qty — may be "645.00 ROLL" or separate items
            qty_parts = [t for x, t in texts_by_x if 800 < x < 1100]
            qty = " ".join(qty_parts)
            # Unit price
            price = next((t for x, t in texts_by_x if 1100 < x < 1280), "")
            # Amount
            amount = next((t for x, t in texts_by_x if x > 1280), "")
            line_items.append({
                "description":     desc or code,
                "sku_or_category": code,
                "quantity":        qty,
                "unit_price":      _clean_num(price),
                "amount":          _clean_num(amount),
            })

    return line_items or [{"description": "(see document)", "sku_or_category": "",
                           "quantity": "", "unit_price": "", "amount": ""}]


# ── Generic Ollama fallback ────────────────────────────────────────────────────
_GENERIC_OCR_PROMPT = """\
You are a data extraction assistant. The text below was extracted via OCR from an invoice PDF.
Extract all available fields and return ONLY a valid JSON object with these keys (use "" for missing):

{
  "reference_number": "invoice number",
  "order_id": "purchase order or sales order number",
  "date": "invoice date",
  "vendor_name": "seller / vendor company name",
  "vendor_address": "seller address",
  "client_name": "buyer / bill-to company name",
  "client_address": "buyer address",
  "ship_to_address": "ship-to address",
  "currency": "currency code e.g. USD HKD",
  "subtotal": "subtotal amount (no currency symbol)",
  "discount": "discount amount",
  "shipping_cost": "shipping/freight amount",
  "tax": "tax amount",
  "total": "total invoice amount",
  "balance_due": "balance due",
  "payment_terms": "payment terms",
  "notes": "any notes or remarks",
  "line_items": [
    {
      "description": "product/item description",
      "sku_or_category": "product code or SKU",
      "quantity": "quantity with unit",
      "unit_price": "unit price",
      "amount": "line total"
    }
  ]
}

Return ONLY the JSON. No explanation. No markdown.

OCR TEXT:
"""


def parse_generic_ollama(text: str, ollama_url: str, ollama_model: str) -> dict:
    data = _empty_invoice()
    try:
        resp = requests.post(ollama_url, json={
            "model":   ollama_model,
            "prompt":  _GENERIC_OCR_PROMPT + text[:6000],
            "stream":  False,
            "format":  "json",
            "options": {"temperature": 0.0},
        }, timeout=180)
        if resp.status_code == 200:
            raw = resp.json().get("response", "{}")
            parsed = _extract_json(raw)
            data.update({k: v for k, v in parsed.items() if k in data})
    except Exception as e:
        log.warning(f"Ollama OCR fallback failed: {e}")
    data["document_type"] = "Invoice"
    return data


def _extract_json(text: str) -> dict:
    text = re.sub(r'^```(?:json)?\s*', '', text.strip(), flags=re.IGNORECASE)
    text = re.sub(r'\s*```$', '', text.strip())
    try:
        return json.loads(text)
    except Exception:
        m = re.search(r'\{[\s\S]*\}', text)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                pass
    return {}


# ── Main entry point ───────────────────────────────────────────────────────────
def extract_from_ocr_pdf(
    file_path: Path,
    ollama_url: str = "http://localhost:11434/api/generate",
    ollama_model: str = "qwen2.5:7b",
) -> dict:
    """
    Full OCR pipeline:
      1. Render PDF pages → EasyOCR
      2. Detect invoice format
      3. Run format-specific regex parser
      4. Fall back to Ollama if key fields still missing
    """
    log.info(f"OCR pipeline starting for '{file_path.name}'…")
    items = ocr_pdf(file_path)
    text  = items_to_text(items)

    log.debug(f"OCR raw text ({len(text)} chars):\n{text[:500]}")

    fmt = detect_format(text)
    log.info(f"  Detected format: {fmt}")

    if fmt == "coats":
        data = parse_coats(items, text)
    elif fmt == "avery":
        data = parse_avery(items, text)
    elif fmt == "brilliant":
        data = parse_brilliant(items, text)
    else:
        data = _empty_invoice()

    # If critical fields are still missing, try Ollama
    needs_ollama = not data.get("reference_number") and not data.get("client_name")
    if needs_ollama or fmt == "generic":
        log.info(f"  Key fields missing — trying Ollama ({ollama_model})…")
        ollama_data = parse_generic_ollama(text, ollama_url, ollama_model)
        # Merge: only fill in blanks
        for k, v in ollama_data.items():
            if not data.get(k) and v:
                data[k] = v

    data["document_type"] = "Invoice"
    log.info(f"  OCR extraction done. ref='{data.get('reference_number')}' "
             f"client='{data.get('client_name')}' total='{data.get('total')}'")
    return data
