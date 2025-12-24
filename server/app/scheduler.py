# server/app/scheduler.py
from __future__ import annotations

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.backup_service import create_backup
from app.settings_service import get_backup_schedule

_scheduler: BackgroundScheduler | None = None
JOB_ID = "daily_backup"


def start_scheduler() -> BackgroundScheduler:
    """
    Только запускаем BackgroundScheduler.
    JOB НЕ создаём здесь жёстко — его создаёт load_and_apply_backup_schedule() из настроек БД.
    """
    global _scheduler
    if _scheduler is not None:
        return _scheduler

    sched = BackgroundScheduler(timezone="UTC")
    sched.start()
    _scheduler = sched
    return sched


def apply_backup_schedule(schedule: dict) -> dict:
    """
    schedule:
      {
        "enabled": bool,
        "hour": 0..23,
        "minute": 0..59,
        "timezone": "UTC" / "Europe/Helsinki" / ...
      }

    Пересоздаёт job daily_backup под новое расписание.
    """
    sched = start_scheduler()

    # Удаляем старую job, если была
    try:
        sched.remove_job(JOB_ID)
    except Exception:
        pass

    enabled = bool(schedule.get("enabled", True))
    if not enabled:
        return {"enabled": False, "next_run_time": None}

    hour = int(schedule.get("hour", 2))
    minute = int(schedule.get("minute", 0))
    tz = str(schedule.get("timezone", "UTC"))

    # Нормализация
    hour = max(0, min(23, hour))
    minute = max(0, min(59, minute))

    trigger = CronTrigger(hour=hour, minute=minute, timezone=tz)

    def _job():
        # Будет видно в аудите как system/admin
        create_backup(user_login="system", user_role="admin")

    sched.add_job(_job, trigger=trigger, id=JOB_ID, replace_existing=True)

    job = sched.get_job(JOB_ID)
    return {
        "enabled": True,
        "hour": hour,
        "minute": minute,
        "timezone": tz,
        "next_run_time": job.next_run_time.isoformat() if job and job.next_run_time else None,
    }


def load_and_apply_backup_schedule() -> dict:
    """
    Читает schedule из БД (app_settings) и применяет к APScheduler.
    Вызывать при старте сервера.
    """
    schedule = get_backup_schedule()
    return apply_backup_schedule(schedule)
