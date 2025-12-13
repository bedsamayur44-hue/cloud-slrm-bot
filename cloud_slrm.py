# =====================================================
# CLOUD SLRM BOT – NY SESSION (RENDER SAFE, LIVE ONLY)
# =====================================================

import os, time, logging, requests
from datetime import datetime, timedelta, date, time as dtime
import pytz
import pandas as pd
from flask import Flask, jsonify

# ---------------- ENV CONFIG ----------------
TD_APIKEY = os.environ["TD_APIKEY"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

SYMBOL = os.environ.get("SYMBOL", "XAUUSD")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "60"))

TZ_IST = pytz.timezone("Asia/Kolkata")

DAY_LOCK_FILE = "signal_day_lock.txt"
TP_MULT = 3

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)
logger = logging.getLogger()

# ---------------- FLASK ----------------
app = Flask(__name__)

@app.route("/")
def home():
    return jsonify({"status": "SLRM NY LIVE BOT RUNNING"})

# ---------------- TELEGRAM ----------------
def telegram_send(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        logger.error(f"Telegram error: {e}")

# ---------------- TWELVEDATA FETCH (SAFE) ----------------
def fetch_td(symbol, interval, size):
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": size,
        "apikey": TD_APIKEY,
        "timezone": "Asia/Kolkata",
        "format": "JSON"
    }

    try:
        r = requests.get(url, params=params, timeout=15)
        j = r.json()
    except Exception as e:
        logger.error(f"TwelveData request failed: {e}")
        return None

    # SAFETY CHECK
    if "values" not in j:
        logger.error(f"TwelveData bad response: {j}")
        return None

    df = pd.DataFrame(j["values"])
    if df.empty:
        return None

    df["time"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["time"])
    df["time"] = df["time"].dt.tz_localize(TZ_IST)

    df = df.sort_values("time").set_index("time")
    df[["open", "high", "low", "close"]] = df[
        ["open", "high", "low", "close"]
    ].astype(float)

    return df

# ---------------- US DST ----------------
def second_sunday_march(y):
    d = date(y, 3, 8)
    return d + timedelta(days=(6 - d.weekday()) % 7)

def first_sunday_nov(y):
    d = date(y, 11, 1)
    return d + timedelta(days=(6 - d.weekday()) % 7)

def is_us_dst(d):
    return second_sunday_march(d.year) <= d < first_sunday_nov(d.year)

# ---------------- DAY LOCK ----------------
def signal_sent_today(day):
    if not os.path.exists(DAY_LOCK_FILE):
        return False
    return open(DAY_LOCK_FILE).read().strip() == str(day)

def mark_signal_sent(day):
    with open(DAY_LOCK_FILE, "w") as f:
        f.write(str(day))

# ---------------- CORE STRATEGY (NY ONLY) ----------------
def run_once():
    now = datetime.now(TZ_IST)
    today = now.date()

    # Only one trade per day
    if signal_sent_today(today):
        return

    df15 = fetch_td(SYMBOL, "15min", 300)
    df1  = fetch_td(SYMBOL, "1min", 200)

    # If data missing, skip safely
    if df15 is None or df1 is None:
        return

    # Previous day PDH / PDL (IST calendar day)
    prev_day = today - timedelta(days=1)
    prev_df = df15[df15.index.date == prev_day]
    if prev_df.empty:
        return

    PDH = prev_df["high"].max()
    PDL = prev_df["low"].min()

    # NY session window in IST
    if is_us_dst(today):
        start = TZ_IST.localize(datetime.combine(today, dtime(19, 0)))
        end   = TZ_IST.localize(datetime.combine(today, dtime(21, 0)))
    else:
        start = TZ_IST.localize(datetime.combine(today, dtime(20, 0)))
        end   = TZ_IST.localize(datetime.combine(today, dtime(22, 0)))

    df1 = df1[(df1.index >= start - timedelta(minutes=5)) &
              (df1.index <= end + timedelta(minutes=5))]

    if len(df1) < 3:
        return

    last = df1.iloc[-1]
    prev = df1.iloc[-2]

    # LIVE TIME GUARD (NO REPLAY)
    if abs((now - last.name).total_seconds()) > 90:
        return

    # BUY: PDL sweep & reclaim
    if prev.low < PDL and last.close > PDL:
        msg = (
            f"SLRM BUY (NY)\n"
            f"Symbol: {SYMBOL}\n"
            f"Entry: {last.close}\n"
            f"SL: {prev.low}\n"
            f"TP: {last.close + TP_MULT * (last.close - prev.low)}\n"
            f"Time IST: {last.name.strftime('%H:%M:%S')}"
        )
        telegram_send(msg)
        mark_signal_sent(today)
        logger.info("BUY signal sent")
        return

    # SELL: PDH sweep & reclaim
    if prev.high > PDH and last.close < PDH:
        msg = (
            f"SLRM SELL (NY)\n"
            f"Symbol: {SYMBOL}\n"
            f"Entry: {last.close}\n"
            f"SL: {prev.high}\n"
            f"TP: {last.close - TP_MULT * (prev.high - last.close)}\n"
            f"Time IST: {last.name.strftime('%H:%M:%S')}"
        )
        telegram_send(msg)
        mark_signal_sent(today)
        logger.info("SELL signal sent")
        return

# ---------------- LOOP ----------------
def loop():
    logger.info("SLRM NY LIVE LOOP STARTED")
    while True:
        try:
            run_once()
        except Exception as e:
            logger.exception(e)
        time.sleep(POLL_SECONDS)

# ---------------- START ----------------
if __name__ == "__main__":
    from threading import Thread
    Thread(target=loop, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)
