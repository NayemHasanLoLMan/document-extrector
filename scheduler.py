"""
scheduler.py — APScheduler-based Job Scheduler for the Invoice RPA Bot
========================================================================
Adds two scheduled jobs:

1. INBOX_SCAN  (default: every 30 minutes)
   Re-scans the Inbox/ folder and queues any PDF that is not already queued.
   This is the "belt-and-braces" sweep that catches files added while the
   watchdog wasn't running (e.g. bulk network-share drops).

2. FAILED_REVIEW_REMINDER  (default: every day at 08:00)
   If there are files in Failed/, re-emits an audit event + sends a bundled
   email reminder to the human reviewer.

Configuration lives in config.json under the "schedule" key:
{
  "schedule": {
    "inbox_scan_interval_minutes": 30,
    "reminder_hour": 8,
    "reminder_minute": 0,
    "timezone": "UTC"
  }
}

The scheduler runs inside the main Flask process as a background daemon thread
— no separate process needed.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger
    _APScheduler_available = True
except ImportError:
    _APScheduler_available = False

from audit_log import audit

log = logging.getLogger("rpa.scheduler")

# ── Will be set by init_scheduler() ──────────────────────────────────────────
_scheduler: Optional["BackgroundScheduler"] = None
_inbox_dir: Optional[Path] = None
_failed_dir: Optional[Path] = None
_enqueue_fn = None      # reference to rpa_bot.enqueue_file
_notify_fn  = None      # reference to notifier.notify_human_review


# ── Job: Inbox sweep ──────────────────────────────────────────────────────────
def _job_inbox_scan() -> None:
    if _inbox_dir is None or _enqueue_fn is None:
        return

    pdfs = sorted(_inbox_dir.glob("*.pdf"))
    if not pdfs:
        audit.log_event("SCHEDULER_INBOX_SCAN",
                        trigger="interval", found=0,
                        note="Inbox is empty")
        return

    audit.log_event("SCHEDULER_INBOX_SCAN",
                    trigger="interval", found=len(pdfs),
                    files=[p.name for p in pdfs])
    log.info(f"[Scheduler] Inbox scan: queuing {len(pdfs)} PDF(s).")
    for pdf in pdfs:
        _enqueue_fn(pdf)


# ── Job: Failed-folder daily reminder ────────────────────────────────────────
def _job_failed_reminder() -> None:
    if _failed_dir is None:
        return

    failed = list(_failed_dir.glob("*.pdf"))
    if not failed:
        audit.log_event("SCHEDULER_FAILED_REMINDER",
                        trigger="cron", found=0,
                        note="No files in Failed/ — nothing to remind")
        return

    audit.log_event("SCHEDULER_FAILED_REMINDER",
                    trigger="cron", found=len(failed),
                    files=[f.name for f in failed])
    log.warning(f"[Scheduler] {len(failed)} file(s) still in Failed/ — sending reminder.")

    if _notify_fn is not None:
        names = ", ".join(f.name for f in failed[:5])
        if len(failed) > 5:
            names += f" … and {len(failed) - 5} more"
        _notify_fn(
            file_name=names,
            reason=(
                f"Daily reminder: {len(failed)} file(s) in Failed/ still require "
                "human review. Please inspect and re-queue or handle manually."
            ),
        )


# ── Public API ────────────────────────────────────────────────────────────────
def init_scheduler(
    inbox_dir: Path,
    failed_dir: Path,
    enqueue_fn,
    notify_fn,
    schedule_cfg: dict,
) -> None:
    """
    Initialise and start the background scheduler.

    Parameters
    ----------
    inbox_dir   : Path to the Inbox/ folder.
    failed_dir  : Path to the Failed/ folder.
    enqueue_fn  : Callable — rpa_bot.enqueue_file(path).
    notify_fn   : Callable — notifier.notify_human_review(file, reason).
    schedule_cfg: dict from config.json["schedule"].
    """
    global _scheduler, _inbox_dir, _failed_dir, _enqueue_fn, _notify_fn

    if not _APScheduler_available:
        log.error(
            "APScheduler is not installed — scheduler disabled. "
            "Run: pip install apscheduler"
        )
        audit.log_event("SCHEDULER_ERROR",
                        error="APScheduler not installed — scheduler disabled")
        return

    _inbox_dir  = inbox_dir
    _failed_dir = failed_dir
    _enqueue_fn = enqueue_fn
    _notify_fn  = notify_fn

    interval_min = int(schedule_cfg.get("inbox_scan_interval_minutes", 30))
    reminder_h   = int(schedule_cfg.get("reminder_hour",   8))
    reminder_m   = int(schedule_cfg.get("reminder_minute", 0))
    tz           = schedule_cfg.get("timezone", "UTC")

    _scheduler = BackgroundScheduler(timezone=tz)

    # Job 1 — Inbox sweep every N minutes
    _scheduler.add_job(
        func=_job_inbox_scan,
        trigger=IntervalTrigger(minutes=interval_min, timezone=tz),
        id="inbox_scan",
        name=f"Inbox sweep every {interval_min} min",
        replace_existing=True,
        misfire_grace_time=60,
    )

    # Job 2 — Failed-folder reminder at HH:MM daily
    _scheduler.add_job(
        func=_job_failed_reminder,
        trigger=CronTrigger(hour=reminder_h, minute=reminder_m, timezone=tz),
        id="failed_reminder",
        name=f"Failed-folder reminder at {reminder_h:02d}:{reminder_m:02d}",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    _scheduler.start()

    next_scan     = _scheduler.get_job("inbox_scan").next_run_time
    next_reminder = _scheduler.get_job("failed_reminder").next_run_time

    audit.log_event(
        "SCHEDULER_STARTED",
        inbox_interval_min=interval_min,
        daily_reminder=f"{reminder_h:02d}:{reminder_m:02d}",
        timezone=tz,
        next_scan=str(next_scan),
        next_reminder=str(next_reminder),
    )
    log.info(
        f"[Scheduler] Started — inbox scan every {interval_min} min | "
        f"daily reminder at {reminder_h:02d}:{reminder_m:02d} {tz}"
    )


def shutdown_scheduler() -> None:
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        audit.log_event("SCHEDULER_STOPPED")
        log.info("[Scheduler] Stopped.")


def get_scheduler_status() -> dict:
    """Return a dict suitable for the /api/scheduler-status endpoint."""
    if _scheduler is None or not _APScheduler_available:
        return {"running": False, "jobs": [], "error": "APScheduler not available"}

    jobs = []
    for job in _scheduler.get_jobs():
        jobs.append({
            "id":           job.id,
            "name":         job.name,
            "next_run":     str(job.next_run_time) if job.next_run_time else None,
        })
    return {
        "running":       _scheduler.running,
        "jobs":          jobs,
        "apscheduler":   _APScheduler_available,
    }
