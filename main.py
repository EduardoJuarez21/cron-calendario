import os
import logging
from datetime import datetime, timedelta

import psycopg2
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

PICKS_URL = os.getenv("PICKS_URL", "http://146.190.167.85/picks/match")
DATABASE_URL = os.getenv("DATABASE_URL", "")

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

PICKS_QUERY = """
    SELECT league, match_id
    FROM public.fixtures_calendar
    WHERE model_triggered_at IS NULL
      AND kickoff_utc >= (now() - interval '12 hours')
      AND kickoff_utc <= (now() + interval '30 minutes')
    ORDER BY kickoff_utc
"""

UPDATE_QUERY = """
    UPDATE public.fixtures_calendar
       SET model_triggered_at = now()
     WHERE league = %s
       AND match_id = %s
       AND model_triggered_at IS NULL
"""


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


def run_picks():
    log.info("=== Iniciando tarea de picks ===")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(PICKS_QUERY)
        rows = cur.fetchall()
        log.info("Matches encontrados: %d", len(rows))

        headers = {"Content-Type": "application/json", "X-API-Token": API_TOKEN}
        for league, match_id in rows:
            try:
                resp = requests.post(
                    PICKS_URL,
                    json={"league": league, "match_id": match_id},
                    headers=headers,
                    timeout=20,
                )
                resp.raise_for_status()
                cur.execute(UPDATE_QUERY, (league, match_id))
                conn.commit()
                log.info("OK  picks league=%-20s match_id=%s status=%s", league, match_id, resp.status_code)
            except Exception as e:
                conn.rollback()
                log.error("FAIL picks league=%-20s match_id=%s error=%s", league, match_id, e)

        cur.close()
        conn.close()
    except Exception as e:
        log.error("Error de conexión DB: %s", e)

    log.info("=== Tarea de picks completada ===")


if __name__ == "__main__":
    scheduler = BlockingScheduler(timezone=TIMEZONE)

    if INTERVAL_MINUTES:
        trigger = IntervalTrigger(minutes=int(INTERVAL_MINUTES))
        log.info("Modo prueba calendario: cada %s minutos", INTERVAL_MINUTES)
    else:
        trigger = CronTrigger(hour=CRON_HOUR, minute=CRON_MINUTE, timezone=TIMEZONE)
        log.info("Scheduler calendario: ejecutará a las %02d:%02d %s", CRON_HOUR, CRON_MINUTE, TIMEZONE)

    scheduler.add_job(
        run,
        trigger,
        id="calendario_job",
        name="Calendario",
        misfire_grace_time=300,
    )

    scheduler.add_job(
        run_picks,
        IntervalTrigger(minutes=10),
        id="picks_job",
        name="Picks",
        misfire_grace_time=60,
    )
    log.info("Scheduler picks: cada 10 minutos")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler detenido.")
