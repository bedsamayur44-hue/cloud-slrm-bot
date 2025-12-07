# cloud_slrm.py
# Cloud SLRM Option A signal bot (TwelveData API + Telegram alerts)
# Robust version: tolerates missing 'volume', different column names, and handles tz properly.

import os, time, logging, requests, csv
from datetime import datetime, timedelta, date, time as dtime
import pytz, numpy as np, pandas as pd
from flask import Flask, jsonify

# --- CONFIG from environment ---
TD_APIKEY = os.environ.get("TD_APIKEY", "").strip()
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
SYMBOL = os.environ.get("SYMBOL", "XAU/USD").strip()
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "60"))
CSV_FILE = "signals.csv"
LOGFILE = "cloud_slrm.log"

if not TD_APIKEY:
    raise SystemExit("TD_APIKEY environment variable is required")

# --- Constants ---
TZ_IST = pytz.timezone("Asia/Kolkata")
RECLAIM_LOOKAHEAD = 3
TP_MULT = 3
RISK_PCT = 1.5

# --- Logging ---
logging.basicConfig(filename=LOGFILE, level=logging.INFO,
                    format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())

app = Flask(__name__)

# --- Telegram helper ---
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

# --- TwelveData fetch helper ---
def fetch_td_series(symbol, interval, outputsize=500):
    """
    Fetch time-series from TwelveData and return a tz-aware DataFrame (IST).
    Tolerant of missing 'volume' and varied column names.
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
    try:
        r = requests.get(base, params=params, timeout=15)
    except Exception as e:
        logger.warning("TD request exception: %s", e)
        return None

    if r.status_code != 200:
        logger.warning("TD failed %s %s", r.status_code, r.text)
        return None

    try:
        j = r.json()
    except Exception as e:
        logger.warning("TD json decode failed: %s", e)
        return None

    if "values" not in j:
        logger.warning("TD response missing values: %s", j)
        return None

    df = pd.DataFrame(j["values"])
    if df.empty:
        logger.warning("TD returned empty values for %s %s", symbol, interval)
        return None

    # Find datetime-like column
    dt_col = None
    for candidate in ['datetime', 'date', 'time', 'timestamp']:
        if candidate in df.columns:
            dt_col = candidate
            break
    if dt_col is None:
        dt_col = next((c for c in df.columns if 'time' in c.lower() or 'date' in c.lower()), None)
    if dt_col is None:
        logger.warning("TD response missing datetime-like column: %s", df.columns.tolist())
        return None

    df['datetime'] = pd.to_datetime(df[dt_col], errors='coerce')
    if df['datetime'].isna().all():
        logger.warning("TD datetime parse failed sample: %s", df[dt_col].head().tolist())
        return None

    # Ensure tz-aware in IST
    try:
        if df['datetime'].dt.tz is None:
            df['datetime'] = df['datetime'].dt.tz_localize('Asia/Kolkata')
        else:
            df['datetime'] = df['datetime'].dt.tz_convert('Asia/Kolkata')
    except Exception:
        df['datetime'] = pd.to_datetime(df[dt_col], utc=True, errors='coerce').dt.tz_convert('Asia/Kolkata')

    # Normalize OHLC/Volume column names
    mapping = {}
    for src in ['open','Open','o','OPEN']:
        if src in df.columns and 'Open' not in df.columns:
            mapping[src] = 'Open'
    for src in ['high','High','h','HIGH']:
        if src in df.columns and 'High' not in df.columns:
            mapping[src] = 'High'
    for src in ['low','Low','l','LOW']:
        if src in df.columns and 'Low' not in df.columns:
            mapping[src] = 'Low'
    for src in ['close','Close','c','CLOSE']:
        if src in df.columns and 'Close' not in df.columns:
            mapping[src] = 'Close'
    for src in ['volume','Volume','v','VOLUME']:
        if src in df.columns and 'Volume' not in df.columns:
            mapping[src] = 'Volume'

    if mapping:
        df = df.rename(columns=mapping)

    # Ensure Volume exists
    if 'Volume' not in df.columns:
        df['Volume'] = float('nan')

    # Set index and coerce numeric types
    df = df.rename(columns={'datetime': 'time'}).set_index('time').sort_index()
    for c in ['Open', 'High', 'Low', 'Close', 'Volume']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
        else:
            df[c] = float('nan')

    return df

# --- DST helpers ---
def second_sunday_of_march(y):
    from datetime import date, timedelta
    d = date(y,3,8); days=((6 - d.weekday())%7); return d + timedelta(days=days)
def first_sunday_of_november(y):
    from datetime import date, timedelta
    d = date(y,11,1); days=((6 - d.weekday())%7); return d + timedelta(days=days)
def is_us_dst(dt):
    if isinstance(dt, datetime):
        dt = dt.date()
    y = dt.year
    return second_sunday_of_march(y) <= dt < first_sunday_of_november(y)

# --- Strategy helpers (PDH/PDL and detection) ---
def compute_prev_day_levels(df15):
    days = sorted({t.date() for t in df15.index})
    levels = {}
    for d in days:
        prev = d - timedelta(days=1)
        prev_df = df15[df15.index.date == prev]
        if not prev_df.empty:
            levels[d] = {"PDH": float(prev_df['High'].max()), "PDL": float(prev_df['Low'].min())}
    return levels

def detect_signals(df1, df15):
    trades = []
    prev_levels = compute_prev_day_levels(df15)
    dates = sorted({t.date() for t in df1.index})
    for day in dates:
        if day not in prev_levels:
            continue
        PDH = prev_levels[day]['PDH']; PDL = prev_levels[day]['PDL']

        # DST-aware window (IST)
        if is_us_dst(day):
            start = datetime.combine(day, dtime(19,0))
            end   = datetime.combine(day, dtime(21,0))
        else:
            start = datetime.combine(day, dtime(20,0))
            end   = datetime.combine(day, dtime(22,0))
        start = TZ_IST.localize(start); end = TZ_IST.localize(end)
        window_df = df1[(df1.index >= start - timedelta(minutes=60)) & (df1.index <= end + timedelta(minutes=60))]
        if window_df.empty:
            continue

        day15 = df15[df15.index.date == day]
        bias = 'neutral'
        if len(day15) >= 3:
            bias = 'bull' if (day15['Close'].iloc[-1] - day15['Close'].iloc[-3]) > 0 else 'bear'

        order = [('buy', PDL), ('sell', PDH)] if bias != 'bear' else [('sell', PDH), ('buy', PDL)]
        highs = window_df['High'].to_numpy(); lows = window_df['Low'].to_numpy(); closes = window_df['Close'].to_numpy()
        times = np.array(window_df.index); n = len(times)

        for direction, level in order:
            if np.isnan(level): continue
            sweep_idxs = np.where(lows < level)[0] if direction == 'buy' else np.where(highs > level)[0]
            if sweep_idxs.size == 0: continue
            for si in sweep_idxs[::-1]:
                start_reclaim = si + 1
                end_reclaim = min(si + RECLAIM_LOOKAHEAD, n - 1)
                if start_reclaim > end_reclaim: continue
                if direction == 'buy':
                    reclaim_pos = np.where(closes[start_reclaim:end_reclaim+1] > level)[0]
                else:
                    reclaim_pos = np.where(closes[start_reclaim:end_reclaim+1] < level)[0]
                if reclaim_pos.size == 0: continue
                reclaim_idx = start_reclaim + int(reclaim_pos[0])
                reclaim_time = pd.Timestamp(times[reclaim_idx])
                if not (reclaim_time >= start and reclaim_time <= end): continue
                pre_start = max(0, si - 3)
                if direction == 'buy':
                    pre_slice_highs = highs[pre_start:si] if si>pre_start else np.array([])
                    if pre_slice_highs.size == 0: continue
                    if closes[reclaim_idx] <= pre_slice_highs.max(): continue
                else:
                    pre_slice_lows = lows[pre_start:si] if si>pre_start else np.array([])
                    if pre_slice_lows.size == 0: continue
                    if closes[reclaim_idx] >= pre_slice_lows.min(): continue
                entry = float(closes[reclaim_idx])
                if direction == 'buy':
                    sl = float(lows[si]); dist = entry - sl
                    if dist <= 0: continue
                    tp = entry + TP_MULT * dist
                else:
                    sl = float(highs[si]); dist = sl - entry
                    if dist <= 0: continue
                    tp = entry - TP_MULT * dist
                trades.append({'entry_time': reclaim_time, 'direction': direction, 'entry': entry, 'sl': sl, 'tp': tp, 'day': day})
                break
    return trades

# --- Core loop ---
def run_once():
    try:
        df15 = fetch_td_series(SYMBOL, "15min", outputsize=500)
        df1  = fetch_td_series(SYMBOL, "1min", outputsize=1200)
        if df15 is None or df1 is None:
            logger.warning("Data fetch returned None; skipping cycle")
            return
        candidates = detect_signals(df1, df15)
        logger.info("Candidates found: %d", len(candidates))
        for c in candidates:
            sig_id = f"{c['entry_time']}_{c['direction']}_{c['entry']}"
            already = False
            if os.path.exists(CSV_FILE):
                try:
                    dfc = pd.read_csv(CSV_FILE)
                    if 'id' in dfc.columns and sig_id in dfc['id'].astype(str).values:
                        already = True
                except Exception:
                    # corrupted CSV - ignore and append fresh
                    logger.warning("Could not read existing CSV for dedupe; will append anyway")
            if already:
                continue
            msg = (f"*SLRM Cloud Signal*\\nSymbol: {SYMBOL}\\nDirection: {c['direction']}\\n"
                   f"Entry: {c['entry']:.2f}\\nSL: {c['sl']:.2f}\\nTP: {c['tp']:.2f}\\nTime(IST): {c['entry_time'].strftime('%Y-%m-%d %H:%M:%S')}")
            telegram_send(msg)
            row = {'id': sig_id, 'ts': c['entry_time'].strftime("%Y-%m-%d %H:%M:%S"), 'symbol': SYMBOL,
                   'direction': c['direction'], 'entry': c['entry'], 'sl': c['sl'], 'tp': c['tp']}
            file_exists = os.path.exists(CSV_FILE)
            try:
                with open(CSV_FILE, "a", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=list(row.keys()))
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow(row)
            except Exception as e:
                logger.exception("Failed to append signals CSV: %s", e)
            logger.info("Signal sent: %s", sig_id)
    except Exception as e:
        logger.exception("run_once error: %s", e)

# --- Flask / ping endpoint ---
@app.route("/ping")
def ping():
    return jsonify({"ok": True, "time": datetime.now().isoformat()})

def main_loop():
    logger.info("Starting cloud SLRM loop for %s", SYMBOL)
    while True:
        run_once()
        time.sleep(POLL_SECONDS)

# ===============================
# TEST ROUTE (FOR TELEGRAM CHECK)
# ===============================
@app.route("/test")
def test_message():
    try:
        # use the existing helper to send telegram messages
        telegram_send("Test message: Cloud SLRM bot is connected ✔️")
        return {"ok": True, "msg": "Test sent"}
    except Exception as e:
        # return error to browser so we can debug easily
        return {"ok": False, "error": str(e)}

# ===============================
# ROUTE: show recent signals
# ===============================
@app.route("/signals")
def show_signals():
    try:
        if not os.path.exists(CSV_FILE):
            return {"ok": True, "signals": []}
        import pandas as _pd
        df = _pd.read_csv(CSV_FILE)
        # return last 20 rows as JSON
        last = df.tail(20).to_dict(orient="records")
        return {"ok": True, "count": len(last), "signals": last}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ===============================
# ROUTE: force a realistic sample signal (for testing)
# ===============================
@app.route("/force_signal")
def force_signal():
    try:
        sample = {
            'entry_time': datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S"),
            'direction': 'buy',
            'entry': 2100.50,
            'sl': 2098.50,
            'tp': 2110.50
        }
        msg = (f"*SLRM Cloud Signal (FORCED)*\nSymbol: {SYMBOL}\nDirection: {sample['direction']}\n"
               f"Entry: {sample['entry']}\nSL: {sample['sl']}\nTP: {sample['tp']}\nTime(IST): {sample['entry_time']}")
        telegram_send(msg)
        # append to CSV as well so /signals shows it
        row = {'id': f"FORCE_{sample['entry_time']}_{sample['direction']}", 'ts': sample['entry_time'],
               'symbol': SYMBOL, 'direction': sample['direction'], 'entry': sample['entry'],
               'sl': sample['sl'], 'tp': sample['tp']}
        exists = os.path.exists(CSV_FILE)
        with open(CSV_FILE, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=row.keys())
            if not exists: w.writeheader()
            w.writerow(row)
        return {"ok": True, "msg": "Forced signal sent"}
    except Exception as e:
        return {"ok": False, "error": str(e)}



if __name__ == "__main__":
    from threading import Thread
    Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
