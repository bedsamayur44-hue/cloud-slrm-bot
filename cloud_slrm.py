# cloud_slrm.py
# Cloud SLRM Option A signal bot (TwelveData API + Telegram alerts)

import os, time, logging, requests, csv
from datetime import datetime, timedelta, date, time as dtime
import pytz, numpy as np, pandas as pd
from flask import Flask, jsonify

TD_APIKEY = os.environ.get("TD_APIKEY", "").strip()
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
SYMBOL = os.environ.get("SYMBOL", "XAU/USD")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "60"))
CSV_FILE = "signals.csv"
LOGFILE = "cloud_slrm.log"

TZ_IST = pytz.timezone("Asia/Kolkata")
RECLAIM_LOOKAHEAD = 3
TP_MULT = 3
RISK_PCT = 1.5

logging.basicConfig(filename=LOGFILE, level=logging.INFO,
                    format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())

app = Flask(__name__)

def telegram_send(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.info("Telegram not configured. Message: %s", text)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            logger.warning("Telegram send failed: %s %s", r.status_code, r.text)
    except Exception as e:
        logger.exception("Telegram send exception: %s", e)

def fetch_td_series(symbol, interval, outputsize=500):
    """
    Fetch time-series from TwelveData and return a tz-aware DataFrame.
    This version tolerates missing 'volume' field and non-standard responses.
    """
    base = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": outputsize,
        "format": "JSON",
        "apikey": TD_APIKEY,
        "timezone": "Asia/Kolkata"
    }
    r = requests.get(base, params=params, timeout=15)
    if r.status_code != 200:
        logger.warning("TD failed %s %s", r.status_code, r.text)
        return None
    j = r.json()
    if "values" not in j:
        logger.warning("TD response missing values: %s", j)
        return None

    # Build DataFrame from values (values are dictionaries)
    df = pd.DataFrame(j["values"])
    if df.empty:
        logger.warning("TD returned empty values for %s %s", symbol, interval)
        return None

    # Ensure datetime column exists and convert to timezone-aware IST
    if 'datetime' not in df.columns:
        # some responses may use 'datetime' or 'date' — try common alternatives
        dt_col = next((c for c in df.columns if 'date' in c.lower()), None)
        if dt_col is None:
            logger.warning("TD response missing datetime-like column: %s", df.columns)
            return None
        df['datetime'] = df[dt_col]

    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    # If conversion failed, bail out
    if df['datetime'].isna().all():
        logger.warning("TD datetime parse failed: %s", df['datetime'].head().tolist())
        return None

    # localize to IST (make tz-aware)
    try:
        df['datetime'] = df['datetime'].dt.tz_localize('Asia/Kolkata')
    except Exception:
        # if already tz-aware, convert to IST
        df['datetime'] = df['datetime'].dt.tz_convert('Asia/Kolkata')

    # rename columns we need (open/high/low/close may be lowercase)
    mapping = {}
    for src in ['open','Open','o']:
        if src in df.columns and 'Open' not in df.columns:
            mapping[src] = 'Open'
    for src in ['high','High','h']:
        if src in df.columns and 'High' not in df.columns:
            mapping[src] = 'High'
    for src in ['low','Low','l']:
        if src in df.columns and 'Low' not in df.columns:
            mapping[src] = 'Low'
    for src in ['close','Close','c']:
        if src in df.columns and 'Close' not in df.columns:
            mapping[src] = 'Close'
    # volume is optional
    if 'volume' in df.columns and 'Volume' not in df.columns:
        mapping['volume'] = 'Volume'
    df = df.rename(columns=mapping)

    # If Volume is missing, create it as NaN so later code can safely reference it
    if 'Volume' not in df.columns:
        df['Volume'] = float('nan')

    # set index and coerce numeric types
    df = df.rename(columns={"datetime":"time"}).set_index('time').sort_index()
    for c in ['Open','High','Low','Close','Volume']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
        else:
            # ensure column exists
            df[c] = float('nan')

    return df

def second_sunday_of_march(y):
    from datetime import date, timedelta
    d = date(y,3,8)
    return d + timedelta(days=((6 - d.weekday())%7))

def first_sunday_of_november(y):
    from datetime import date, timedelta
    d = date(y,11,1)
    return d + timedelta(days=((6 - d.weekday())%7))

def is_us_dst(dt):
    if isinstance(dt, datetime):
        dt = dt.date()
    y = dt.year
    return second_sunday_of_march(y) <= dt < first_sunday_of_november(y)

def compute_prev_day_levels(df15):
    days = sorted({t.date() for t in df15.index})
    levels = {}
    for d in days:
        prev = d - timedelta(days=1)
        prev_df = df15[df15.index.date == prev]
        if not prev_df.empty:
            levels[d] = {
                "PDH": float(prev_df["High"].max()),
                "PDL": float(prev_df["Low"].min())
            }
    return levels

def detect_signals(df1, df15):
    trades = []
    prev_levels = compute_prev_day_levels(df15)
    dates = sorted({t.date() for t in df1.index})

    for day in dates:
        if day not in prev_levels: continue

        PDH = prev_levels[day]["PDH"]
        PDL = prev_levels[day]["PDL"]

        if is_us_dst(day):
            start = datetime.combine(day, dtime(19,0))
            end = datetime.combine(day, dtime(21,0))
        else:
            start = datetime.combine(day, dtime(20,0))
            end = datetime.combine(day, dtime(22,0))

        start = TZ_IST.localize(start)
        end = TZ_IST.localize(end)
        window_df = df1[(df1.index >= start - timedelta(minutes=60)) &
                        (df1.index <= end + timedelta(minutes=60))]
        if window_df.empty: continue

        day15 = df15[df15.index.date == day]
        bias = 'neutral'
        if len(day15) >= 3:
            bias = 'bull' if (day15['Close'].iloc[-1] - day15['Close'].iloc[-3]) > 0 else 'bear'

        order = [('buy', PDL), ('sell', PDH)] if bias != 'bear' else [('sell', PDH), ('buy', PDL)]

        highs = window_df['High'].to_numpy()
        lows = window_df['Low'].to_numpy()
        closes = window_df['Close'].to_numpy()
        times = np.array(window_df.index)
        n = len(times)

        for direction, level in order:
            sweep_idxs = np.where(lows < level)[0] if direction=='buy' else np.where(highs > level)[0]
            if sweep_idxs.size == 0: continue

            for si in sweep_idxs[::-1]:
                start_reclaim = si + 1
                end_reclaim = min(si + 3, n-1)
                if start_reclaim > end_reclaim: continue

                if direction=='buy':
                    rec = np.where(closes[start_reclaim:end_reclaim+1] > level)[0]
                else:
                    rec = np.where(closes[start_reclaim:end_reclaim+1] < level)[0]
                if rec.size == 0: continue

                reclaim_idx = start_reclaim + rec[0]
                reclaim_time = pd.Timestamp(times[reclaim_idx])
                if not (reclaim_time >= start and reclaim_time <= end): continue

                pre_start = max(0, si - 3)
                if direction=='buy':
                    pre_slice = highs[pre_start:si]
                    if len(pre_slice)==0 or closes[reclaim_idx] <= pre_slice.max(): continue
                else:
                    pre_slice = lows[pre_start:si]
                    if len(pre_slice)==0 or closes[reclaim_idx] >= pre_slice.min(): continue

                entry = float(closes[reclaim_idx])
                sl = float(lows[si]) if direction=='buy' else float(highs[si])
                dist = entry - sl if direction=='buy' else sl - entry
                if dist <= 0: continue
                tp = entry + TP_MULT*dist if direction=='buy' else entry - TP_MULT*dist

                trades.append({
                    "entry_time": reclaim_time,
                    "direction": direction,
                    "entry": entry,
                    "sl": sl,
                    "tp": tp,
                    "day": day
                })
                break
    return trades

def run_once():
    try:
        df15 = fetch_td_series(SYMBOL, "15min", outputsize=500)
        df1 = fetch_td_series(SYMBOL, "1min", outputsize=1200)
        if df15 is None or df1 is None: return

        signals = detect_signals(df1, df15)
        for s in signals:
            sid = f"{s['entry_time']}_{s['direction']}_{s['entry']}"
            already = False

            if os.path.exists(CSV_FILE):
                dfc = pd.read_csv(CSV_FILE)
                if 'id' in dfc.columns and sid in dfc['id'].astype(str).values:
                    already = True
            if already: continue

            msg = (
                f"*SLRM Cloud Signal*\n"
                f"Direction: {s['direction']}\n"
                f"Entry: {s['entry']}\n"
                f"SL: {s['sl']}\n"
                f"TP: {s['tp']}\n"
                f"Time (IST): {s['entry_time']}"
            )
            telegram_send(msg)

            row = {
                "id": sid,
                "ts": s["entry_time"],
                "direction": s["direction"],
                "entry": s["entry"],
                "sl": s["sl"],
                "tp": s["tp"]
            }
            exists = os.path.exists(CSV_FILE)
            with open(CSV_FILE, "a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=row.keys())
                if not exists: w.writeheader()
                w.writerow(row)
    except Exception as e:
        logger.exception(e)

@app.route("/ping")
def ping():
    return jsonify({"ok": True, "time": datetime.now().isoformat()})

def main_loop():
    while True:
        run_once()
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    from threading import Thread
    Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
