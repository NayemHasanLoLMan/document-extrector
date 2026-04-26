"""
notifier.py — Human-Review Email Notifier
==========================================
Sends an email (via SMTP / Gmail app-password) when a PDF lands in Failed/,
asking a human to intervene.

Configuration lives in config.json under the "notifications" key:
{
  "notifications": {
    "enabled": true,
    "smtp_host": "smtp.gmail.com",
    "smtp_port": 587,
    "smtp_user": "you@gmail.com",
    "smtp_password": "xxxx xxxx xxxx xxxx",   ← Gmail App Password
    "from_address": "you@gmail.com",
    "to_addresses": ["reviewer@company.com"],
    "subject_prefix": "[RPA ALERT]"
  }
}

If "enabled" is false (or the section is missing) the module is a no-op.
"""

from __future__ import annotations

import smtplib
import threading
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional

from audit_log import audit

# ── Config defaults (overridden by load_notification_config) ──────────────────
_cfg: dict = {
    "enabled":        False,
    "smtp_host":      "smtp.gmail.com",
    "smtp_port":      587,
    "smtp_user":      "",
    "smtp_password":  "",
    "from_address":   "",
    "to_addresses":   [],
    "subject_prefix": "[RPA ALERT]",
}

_lock = threading.Lock()


def load_notification_config(config: dict) -> None:
    """Merge the 'notifications' section of config.json into _cfg."""
    global _cfg
    with _lock:
        section = config.get("notifications", {})
        _cfg.update(section)


def is_enabled() -> bool:
    with _lock:
        return bool(
            _cfg.get("enabled")
            and _cfg.get("smtp_user")
            and _cfg.get("smtp_password")
            and _cfg.get("to_addresses")
        )


# ── Email builder ─────────────────────────────────────────────────────────────
def _build_email(file_name: str, reason: str) -> MIMEMultipart:
    with _lock:
        cfg = dict(_cfg)

    subject = f"{cfg['subject_prefix']} Human review required — {file_name}"
    body_html = f"""
    <html><body style="font-family:Arial,sans-serif;color:#333;max-width:640px;">
      <div style="background:#c0392b;padding:16px;border-radius:6px 6px 0 0;">
        <h2 style="color:#fff;margin:0;">⚠️ RPA Bot — Human Review Required</h2>
      </div>
      <div style="background:#f9f9f9;padding:20px;border:1px solid #ddd;border-radius:0 0 6px 6px;">
        <p>The RPA document-extraction pipeline was <strong>unable to process</strong>
           the following file automatically:</p>
        <table style="border-collapse:collapse;width:100%;">
          <tr><td style="padding:8px;font-weight:bold;width:140px;">File</td>
              <td style="padding:8px;">{file_name}</td></tr>
          <tr style="background:#fff;"><td style="padding:8px;font-weight:bold;">Reason</td>
              <td style="padding:8px;color:#c0392b;">{reason}</td></tr>
          <tr><td style="padding:8px;font-weight:bold;">Time</td>
              <td style="padding:8px;">{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</td></tr>
          <tr style="background:#fff;"><td style="padding:8px;font-weight:bold;">Location</td>
              <td style="padding:8px;"><code>Failed/</code> folder</td></tr>
        </table>
        <hr style="border:none;border-top:1px solid #ddd;margin:16px 0;">
        <p style="font-size:13px;color:#666;">
          Please inspect the file in the <code>Failed/</code> directory,
          correct any issues, and re-drop the PDF into <code>Inbox/</code>
          to retry processing, or manually enter the data.
        </p>
        <p style="font-size:11px;color:#aaa;">Sent by the Invoice RPA Bot · audit log: logs/audit.log</p>
      </div>
    </body></html>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = cfg["from_address"] or cfg["smtp_user"]
    msg["To"]      = ", ".join(cfg["to_addresses"])
    msg.attach(MIMEText(body_html, "html"))
    return msg


# ── Send (non-blocking, fire-and-forget) ──────────────────────────────────────
def _send_in_background(file_name: str, reason: str) -> None:
    with _lock:
        cfg = dict(_cfg)

    try:
        msg = _build_email(file_name, reason)
        with smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"], timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.login(cfg["smtp_user"], cfg["smtp_password"])
            server.sendmail(
                cfg["from_address"] or cfg["smtp_user"],
                cfg["to_addresses"],
                msg.as_string(),
            )
        audit.email_sent(
            recipient=", ".join(cfg["to_addresses"]),
            subject=msg["Subject"],
            file=file_name,
        )
    except Exception as exc:
        audit.log_event("EMAIL_ERROR", file=file_name, error=str(exc))


def notify_human_review(file_name: str, reason: str = "Extraction failed") -> None:
    """
    Call this whenever a file is moved to Failed/.
    If notifications are disabled the call is a documented no-op.
    """
    audit.human_review_required(file=file_name, reason=reason)

    if not is_enabled():
        audit.email_skipped(
            reason="Notifications disabled or SMTP not configured — "
                   "set notifications.enabled=true and SMTP creds in config.json"
        )
        return

    t = threading.Thread(
        target=_send_in_background,
        args=(file_name, reason),
        daemon=True,
        name="rpa-email",
    )
    t.start()
