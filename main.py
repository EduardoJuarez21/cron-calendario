import os
import logging
from datetime import datetime, timedelta

import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

CALENDARIO_URL = os.getenv("CALENDARIO_URL", "http://127.0.0.1/calendario")
TIMEZONE = os.getenv("TZ", "America/Mexico_City")
DAYS_AHEAD = int(os.getenv("DAYS_AHEAD", "1"))
API_TOKEN = os.getenv("API_TOKEN", "")
CRON_HOUR = int(os.getenv("CRON_HOUR", "21"))
CRON_MINUTE = int(os.getenv("CRON_MINUTE", "0"))
INTERVAL_MINUTES = os.getenv("INTERVAL_MINUTES")

LEAGUES = [
    "bundesliga",
    "epl",
    "eredivisie",
    "laliga",
    "ligamx",
    "mls",
    "nba",
    "primeiraliga",
    "segundadivision",
    "seriea",
]


def call_calendario(league: str, day: str):
    payload = {"league": league, "day": day}
    headers = {"X-API-Token": API_TOKEN}
    try:
        resp = requests.post(CALENDARIO_URL, json=payload, headers=headers, timeout=30)
        resp.raise_for_status()
        log.info("OK  league=%-20s day=%s status=%s", league, day, resp.status_code)
    except Exception as e:
        log.error("FAIL league=%-20s day=%s error=%s", league, day, e)


def run():
    log.info("=== Iniciando tarea de calendario ===")
    target_date = (datetime.now() + timedelta(days=DAYS_AHEAD)).strftime("%Y-%m-%d")
    log.info("Fecha objetivo: %s | Ligas: %d", target_date, len(LEAGUES))

    for league in LEAGUES:
        call_calendario(league, target_date)

    log.info("=== Tarea completada ===")


if __name__ == "__main__":
    scheduler = BlockingScheduler(timezone=TIMEZONE)
    if INTERVAL_MINUTES:
        trigger = IntervalTrigger(minutes=int(INTERVAL_MINUTES))
        log.info("Modo prueba: cada %s minutos", INTERVAL_MINUTES)
    else:
        trigger = CronTrigger(hour=CRON_HOUR, minute=CRON_MINUTE, timezone=TIMEZONE)
        log.info("Scheduler iniciado. Ejecutará a las %02d:%02d %s", CRON_HOUR, CRON_MINUTE, TIMEZONE)
    scheduler.add_job(
        run,
        trigger,
        id="calendario_job",
        name="Calendario",
        misfire_grace_time=300,
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler detenido.")
