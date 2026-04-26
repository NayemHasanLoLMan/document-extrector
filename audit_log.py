"""
audit_log.py — Structured Audit Logger for the Invoice RPA Bot
================================================================
Writes every action, decision, and exception to:
  • logs/audit.log          — human-readable rotating log (10 MB × 5 backups)
  • logs/audit_events.jsonl — machine-readable JSONL (one JSON object per line)

Usage:
    from audit_log import audit

    audit.log_event("FILE_QUEUED",   file="invoice_001.pdf")
    audit.log_event("EXTRACTION_OK", file="invoice_001.pdf", engine="regex",
                    invoice="#12345", total="$1,234.00", items=3)
    audit.log_event("EXTRACTION_FAIL", file="invoice_001.pdf",
                    error="No text extracted", action="moved to Failed/")
    audit.log_event("SCHEDULER_RUN",  trigger="cron", next_run="09:00")
    audit.log_event("EMAIL_SENT",     recipient="admin@example.com",
                    subject="Human review required")
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
_BASE_DIR  = Path(__file__).parent
_LOG_DIR   = _BASE_DIR / "logs"
_LOG_FILE  = _LOG_DIR / "audit.log"
_JSONL_FILE = _LOG_DIR / "audit_events.jsonl"

_LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── Human-readable rotating logger ────────────────────────────────────────────
_human_logger = logging.getLogger("rpa.audit")
_human_logger.setLevel(logging.DEBUG)
_human_logger.propagate = False          # don't double-print to root logger

_fh = RotatingFileHandler(
    _LOG_FILE,
    maxBytes=10 * 1024 * 1024,          # 10 MB per file
    backupCount=5,
    encoding="utf-8",
)
_fh.setFormatter(logging.Formatter(
    "%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))
_human_logger.addHandler(_fh)

# Also echo audit events to console (INFO+)
_ch = logging.StreamHandler()
_ch.setLevel(logging.INFO)
_ch.setFormatter(logging.Formatter(
    "%(asctime)s  [AUDIT]  %(message)s",
    datefmt="%H:%M:%S"
))
_human_logger.addHandler(_ch)

# ── JSONL writer ───────────────────────────────────────────────────────────────
_jsonl_lock = threading.Lock()


def _write_jsonl(record: dict) -> None:
    with _jsonl_lock:
        with open(_JSONL_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


# ── Public AuditLogger class ───────────────────────────────────────────────────
class AuditLogger:
    """
    Centralised audit logger.  Call ``audit.log_event(event_type, **kwargs)``.

    Standard fields automatically added to every record:
        ts          — ISO-8601 UTC timestamp
        event       — event type string (e.g. "FILE_QUEUED")
    All extra kwargs are merged into the record as-is.
    """

    # Severity mapping: events whose names contain these substrings get WARNING/ERROR level
    _WARN_PREFIXES  = ("FAIL", "SKIP", "WARN", "RETRY")
    _ERROR_PREFIXES = ("ERROR", "EXCEPTION", "CRASH")

    def log_event(self, event: str, **kwargs) -> None:
        record: dict = {
            "ts":    datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "event": event,
            **kwargs,
        }

        # Choose log level
        e_upper = event.upper()
        if any(p in e_upper for p in self._ERROR_PREFIXES):
            level = logging.ERROR
        elif any(p in e_upper for p in self._WARN_PREFIXES):
            level = logging.WARNING
        else:
            level = logging.INFO

        # Human-readable message
        extras = "  ".join(f"{k}={v}" for k, v in kwargs.items())
        _human_logger.log(level, f"[{event}]  {extras}")

        # Machine-readable JSONL
        _write_jsonl(record)

    # ── Convenience shortcuts ──────────────────────────────────────────────────
    def file_queued(self, file: str) -> None:
        self.log_event("FILE_QUEUED", file=file)

    def extraction_ok(self, file: str, engine: str, invoice: str = "",
                      total: str = "", items: int = 0) -> None:
        self.log_event("EXTRACTION_OK",
                       file=file, engine=engine,
                       invoice=invoice, total=total, items=items)

    def extraction_fail(self, file: str, error: str) -> None:
        self.log_event("EXTRACTION_FAIL",
                       file=file, error=error, action="moved to Failed/")

    def scheduler_run(self, trigger: str = "cron", next_run: str = "") -> None:
        self.log_event("SCHEDULER_RUN", trigger=trigger, next_run=next_run)

    def email_sent(self, recipient: str, subject: str, file: str = "") -> None:
        self.log_event("EMAIL_SENT",
                       recipient=recipient, subject=subject, file=file)

    def email_skipped(self, reason: str) -> None:
        self.log_event("EMAIL_SKIPPED", reason=reason)

    def human_review_required(self, file: str, reason: str) -> None:
        self.log_event("HUMAN_REVIEW_REQUIRED", file=file, reason=reason)

    # ── Read-back for the API endpoint ────────────────────────────────────────
    def read_recent(self, n: int = 200) -> list[dict]:
        """Return the last *n* events from the JSONL file (newest last)."""
        if not _JSONL_FILE.exists():
            return []
        try:
            lines = _JSONL_FILE.read_text(encoding="utf-8").splitlines()
            records = []
            for line in lines[-n:]:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
            return records
        except Exception:
            return []

    @property
    def log_path(self) -> Path:
        return _LOG_FILE

    @property
    def jsonl_path(self) -> Path:
        return _JSONL_FILE


# ── Singleton instance ─────────────────────────────────────────────────────────
audit = AuditLogger()
