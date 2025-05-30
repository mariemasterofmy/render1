# === BEGIN SCRIPT: signal_engine.py ===

import requests
import time
import json
import logging
import os
from datetime import datetime, timezone, timedelta

# === Logging Setup ===
log_file = "signal_engine.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# === BEGIN: Load Configuration ===
import sys

if len(sys.argv) < 2:
    logging.critical("🚨 No config file path provided. Usage: python script.py config_file.json")
    raise SystemExit(1)

config_file_path = sys.argv[1]
logging.info(f"📂 Loading config from file: {config_file_path}")

try:
    with open(config_file_path) as config_file:
        config = json.load(config_file)
except FileNotFoundError:
    logging.critical(f"🚨 Config file not found: {config_file_path}")
    raise SystemExit(1)
except json.JSONDecodeError:
    logging.critical(f"🚨 Config file is not valid JSON: {config_file_path}")
    raise SystemExit(1)

try:
    symbol_to_trade = config["symbol"]
    BTC_THRESHOLD_PCT = config["btc_threshold_pct"]
except KeyError as e:
    logging.critical(f"🚨 Missing required key in config: {e}")
    raise SystemExit(1)

logging.info(f"✅ Config loaded: symbol={symbol_to_trade}, BTC threshold={BTC_THRESHOLD_PCT}")
# === END: Load Configuration ===

# === BEGIN: Signal Output Directory Setup ===
SIGNAL_DIR = "signals"
os.makedirs(SIGNAL_DIR, exist_ok=True)
# === END: Signal Output Directory Setup ===

# === BEGIN: Fetch Last Closed 1-Minute Candle ===
def get_last_closed_1m_candle(symbol):
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1m&limit=2"
    response = requests.get(url)

    try:
        data = response.json()
    except Exception as e:
        logging.error(f"❌ Failed to parse JSON from Binance. Raw text: {response.text}")
        raise

    if isinstance(data, dict) and data.get("code") == -2:
        logging.error(f"❌ Binance returned error -2: {data}")
        raise ValueError(f"Binance error -2: {data}")

    if len(data) < 2:
        raise ValueError(f"Not enough candle data returned for {symbol}")
    
    last_candle = data[-2]

    ts_ms = int(last_candle[0])
    timestamp_utc = datetime.utcfromtimestamp(ts_ms / 1000).replace(tzinfo=timezone.utc).isoformat()
    open_price = float(last_candle[1])
    high_price = float(last_candle[2])
    low_price = float(last_candle[3])
    close_price = float(last_candle[4])
    volume = float(last_candle[5])

    now_utc = datetime.now(timezone.utc)
    delta_seconds = (now_utc - datetime.utcfromtimestamp(ts_ms / 1000).replace(tzinfo=timezone.utc)).total_seconds()

    logging.info(f"[{symbol}] 🔍 Source: Binance API fapi/v1/klines | Using candle index -2 (fully closed)")
    logging.info(f"[{symbol}] 🕒 TS={ts_ms} | Time={timestamp_utc} | O={open_price} H={high_price} L={low_price} C={close_price} V={volume}")
    logging.info(f"[{symbol}] ⏱️ Candle close time was {delta_seconds:.2f} seconds ago")
    logging.debug(f"[{symbol}] 📦 Raw Binance candle JSON: {last_candle}")

    return {
        "timestamp_ms": ts_ms,
        "timestamp_utc": timestamp_utc,
        "open": open_price,
        "close": close_price,
        "volume": volume
    }

# === END: Fetch Last Closed 1-Minute Candle ===

# === BEGIN: Signal Generation Logic ===
def generate_signal():
    try:
        btc_candle = get_last_closed_1m_candle("BTCUSDT")
        asset_candle = get_last_closed_1m_candle(symbol_to_trade)

        btc_move_pct = (btc_candle["close"] - btc_candle["open"]) / btc_candle["open"] * 100

        logging.info(f"BTC move: {btc_move_pct:.5f}%")

        if btc_move_pct > BTC_THRESHOLD_PCT:
            signal = {
                "symbol": symbol_to_trade,
                "action": "BUY",
                "trigger_time": datetime.utcnow().isoformat() + "Z",
                "btc_move_pct": btc_move_pct
            }
            timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            filename = f"{SIGNAL_DIR}/signal_{symbol_to_trade.lower()}_{timestamp}.json"
            with open(filename, "w") as f:
                json.dump(signal, f, indent=4)
            logging.info(f"✅ Signal generated and saved: {filename}")
            logging.info(f"📈 Signal details — BTC move: {btc_move_pct:.5f}%")
        else:
            logging.info("ℹ️ No signal — conditions not met.")

    except Exception as e:
        logging.error(f"❌ Error generating signal: {e}")
# === END: Signal Generation Logic ===

# === BEGIN: Candle Sync Wait Logic ===
def wait_for_next_minute():
    now = datetime.now(timezone.utc)
    next_minute = (now + timedelta(minutes=1)).replace(second=4, microsecond=0)
    sleep_seconds = (next_minute - now).total_seconds()
    logging.info(f"⏳ Current time is {now.isoformat()} UTC")
    logging.info(f"🕒 Scheduled to process next candle at {next_minute.isoformat()} (4s after minute close)")
    logging.info("📌 We wait 4 seconds past the close of each 1-minute candle to make sure Binance has finalized it.")
    logging.info(f"💤 Sleeping for {sleep_seconds:.2f} seconds...")
    time.sleep(sleep_seconds)
# === END: Candle Sync Wait Logic ===

# === BEGIN: Signal Engine Main Loop ===
def main_loop():
    logging.info(f"▶ Running signal engine for {symbol_to_trade}")
    logging.info("⏳ Signal generator started, waiting for full candles...")

    try:
        heartbeat_counter = 0
        while True:
            heartbeat_counter += 1
            if heartbeat_counter % 5 == 0:
                logging.info("❤️ Heartbeat: signal engine still running — last check at " + datetime.utcnow().isoformat() + "Z")

            try:
                wait_for_next_minute()
                logging.info("⏰ Processing last full candle.")
                generate_signal()
            except Exception as loop_err:
                logging.error(f"🔥 Unexpected runtime error in loop: {loop_err}")

    except KeyboardInterrupt:
        logging.info("🛑 KeyboardInterrupt received — shutting down gracefully.")
    except SystemExit:
        logging.info("🛑 SystemExit triggered — exiting.")
    except Exception as fatal:
        logging.critical(f"💥 Fatal uncaught exception: {fatal}")
    finally:
        logging.info("✅ Signal engine stopped.")

# === BEGIN: Entry Point ===
if __name__ == "__main__":
    main_loop()
# === END: Entry Point ===

# === END OF SCRIPT ===
