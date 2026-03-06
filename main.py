import os
import logging
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import requests
from requests.adapters import HTTPAdapter
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from urllib3.util.retry import Retry

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
PICKS_INTERVAL_MINUTES = int(os.getenv("PICKS_INTERVAL_MINUTES", "10"))
EARLY_CUTOFF_HOUR = int(os.getenv("EARLY_CUTOFF_HOUR", "10"))
EARLY_CRON_HOUR = int(os.getenv("EARLY_CRON_HOUR", "22"))
EARLY_CRON_MINUTE = int(os.getenv("EARLY_CRON_MINUTE", "0"))

PICKS_URL = os.getenv("PICKS_URL", "http://146.190.167.85/picks/match")
DATABASE_URL = os.getenv("DATABASE_URL", "")
PICKS_CONNECT_TIMEOUT = float(os.getenv("PICKS_CONNECT_TIMEOUT", "5"))
PICKS_READ_TIMEOUT = float(os.getenv("PICKS_READ_TIMEOUT", "60"))
PICKS_MAX_RETRIES = int(os.getenv("PICKS_MAX_RETRIES", "1"))
PICKS_RETRY_BACKOFF = float(os.getenv("PICKS_RETRY_BACKOFF", "1.0"))
QUERY_TZ = timezone(timedelta(hours=-6))

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
      AND (kickoff_utc - INTERVAL '6 hours') >= %s
      AND (kickoff_utc - INTERVAL '6 hours') <= %s
    ORDER BY kickoff_utc
"""

UPDATE_QUERY = """
    UPDATE public.fixtures_calendar
       SET model_triggered_at = NOW() - INTERVAL '6 hours'
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
    target_date = (datetime.now(QUERY_TZ) + timedelta(days=DAYS_AHEAD)).strftime("%Y-%m-%d")
    log.info("Fecha objetivo: %s | Ligas: %d", target_date, len(LEAGUES))

    for league in LEAGUES:
        call_calendario(league, target_date)

    log.info("=== Tarea completada ===")


def build_picks_session():
    retry = Retry(
        total=PICKS_MAX_RETRIES,
        connect=PICKS_MAX_RETRIES,
        read=PICKS_MAX_RETRIES,
        status=PICKS_MAX_RETRIES,
        backoff_factor=PICKS_RETRY_BACKOFF,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def run_picks(window_start=None, window_end=None):
    log.info("=== Iniciando tarea de picks ===")
    conn = None
    cur = None
    session = build_picks_session()
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        if window_start is None:
            window_start = datetime.now(QUERY_TZ).replace(tzinfo=None)
        if window_end is None:
            window_end = window_start + timedelta(minutes=30)
        cur.execute(PICKS_QUERY, (window_start, window_end))
        rows = cur.fetchall()
        log.info("Matches encontrados: %d", len(rows))
        log.info(
            "Picks config: connect_timeout=%.1fs read_timeout=%.1fs retries=%d",
            PICKS_CONNECT_TIMEOUT,
            PICKS_READ_TIMEOUT,
            PICKS_MAX_RETRIES,
        )

        headers = {"Content-Type": "application/json", "X-API-Token": API_TOKEN}
        for league, match_id in rows:
            started = time.monotonic()
            try:
                resp = session.post(
                    PICKS_URL,
                    json={"league": league, "match_id": match_id},
                    headers=headers,
                    timeout=(PICKS_CONNECT_TIMEOUT, PICKS_READ_TIMEOUT),
                )
                resp.raise_for_status()
                cur.execute(UPDATE_QUERY, (league, match_id))
                conn.commit()
                log.info(
                    "OK  picks league=%-20s match_id=%s status=%s elapsed=%.2fs",
                    league,
                    match_id,
                    resp.status_code,
                    time.monotonic() - started,
                )
            except Exception as e:
                conn.rollback()
                log.error(
                    "FAIL picks league=%-20s match_id=%s elapsed=%.2fs error=%s",
                    league,
                    match_id,
                    time.monotonic() - started,
                    e,
                )
    except Exception as e:
        log.error("Error de conexion DB: %s", e)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
        session.close()

    log.info("=== Tarea de picks completada ===")


def run_early_picks():
    """Dispara picks anticipados para partidos antes de las EARLY_CUTOFF_HOUR del día siguiente."""
    tomorrow = (datetime.now(QUERY_TZ) + timedelta(days=1)).replace(tzinfo=None)
    window_start = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
    window_end = tomorrow.replace(hour=EARLY_CUTOFF_HOUR, minute=0, second=0, microsecond=0)
    log.info(
        "=== Picks anticipados: %s - %s (UTC-6) ===",
        window_start.strftime("%Y-%m-%d %H:%M"),
        window_end.strftime("%Y-%m-%d %H:%M"),
    )
    run_picks(window_start=window_start, window_end=window_end)


if __name__ == "__main__":
    scheduler = BlockingScheduler(timezone=TIMEZONE)

    if INTERVAL_MINUTES:
        trigger = IntervalTrigger(minutes=int(INTERVAL_MINUTES))
        log.info("Modo prueba calendario: cada %s minutos", INTERVAL_MINUTES)
    else:
        trigger = CronTrigger(hour=CRON_HOUR, minute=CRON_MINUTE, timezone=TIMEZONE)
        log.info("Scheduler calendario: ejecutara a las %02d:%02d %s", CRON_HOUR, CRON_MINUTE, TIMEZONE)

    scheduler.add_job(
        run,
        trigger,
        id="calendario_job",
        name="Calendario",
        misfire_grace_time=300,
    )

    scheduler.add_job(
        run_picks,
        IntervalTrigger(minutes=PICKS_INTERVAL_MINUTES),
        id="picks_job",
        name="Picks",
        misfire_grace_time=60,
    )
    log.info("Scheduler picks: cada %d minutos", PICKS_INTERVAL_MINUTES)

    scheduler.add_job(
        run_early_picks,
        CronTrigger(hour=EARLY_CRON_HOUR, minute=EARLY_CRON_MINUTE, timezone=TIMEZONE),
        id="early_picks_job",
        name="EarlyPicks",
        misfire_grace_time=300,
    )
    log.info(
        "Scheduler early picks: ejecutara a las %02d:%02d %s (partidos mañana antes de las %02d:00 UTC-6)",
        EARLY_CRON_HOUR, EARLY_CRON_MINUTE, TIMEZONE, EARLY_CUTOFF_HOUR,
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler detenido.")
