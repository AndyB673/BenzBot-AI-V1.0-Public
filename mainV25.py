import asyncio
import websockets
import threading
import ccxt
import time
import logging
import os
import json
import hmac
import hashlib
import base64
import requests
import random
from datetime import datetime
from dotenv import load_dotenv
import numpy as np
import atexit
from threading import Lock
from decimal import Decimal
import argparse
import sys
import csv  # CSV-Import
import openai

# Environment-Variablen laden
load_dotenv()
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
PASSWORD = os.getenv("PASSWORD")
if not API_KEY or not API_SECRET or not PASSWORD:
    print("Fehler: Eine oder mehrere API-Credentials (API_KEY, API_SECRET, PASSWORD) fehlen.")
    sys.exit(1)
# Setze den OpenAI API-Key aus der Umgebungsvariablen
openai.api_key = os.getenv("OPENAI_API_KEY")
BASE_URL = "https://api.bitget.com"
sys.stdout.reconfigure(encoding='utf-8')

# Argument Parsing
parser = argparse.ArgumentParser()
parser.add_argument("--trade_amount", type=str, default="100.0", help="Handelsbetrag in USD")
parser.add_argument("--leverage", type=str, default="2.0", help="Hebel")
parser.add_argument("--entry_offset", type=str, default="15.0", help="Entry Trigger Offset in USD")
parser.add_argument("--min_profit", type=str, default="5.0", help="Minimaler erforderlicher Profit in USD")
parser.add_argument("--trailing_drop", type=str, default="10.0", help="Trailing Drop Prozent (zusätzlich)")
parser.add_argument("--trailing_stop_below", type=str, default="0.90", help="Trailing Stop Prozent unter 60 USD")
parser.add_argument("--trailing_stop_60_100", type=str, default="0.85",
                    help="Trailing Stop Prozent zwischen 60 und 100 USD")
parser.add_argument("--trailing_stop_above", type=str, default="0.80", help="Trailing Stop Prozent über 100 USD")
parser.add_argument("--mega_stop", type=str, default="6.0", help="Mega Stop Trigger in USD")
parser.add_argument("--fixed_stoploss_offset", type=str, default="5.0", help="Fixed Stop-Loss Offset in USD")
parser.add_argument("--emergency_stop_percent", type=str, default="20.0", help="Emergency Stop Prozent")
parser.add_argument("--coin", type=str, default="BTC",
                    help="Handelscoin(s), getrennt durch Komma (z.B. BTC,SOL,ETH,XRP,DOGE)")
args = parser.parse_args()

def parse_cli_arg(arg_str, default_value):
    try:
        if ',' in arg_str:
            parts = arg_str.split(',')
            return [float(p.strip()) for p in parts]
        else:
            return float(arg_str)
    except Exception:
        return default_value

args.trade_amount = parse_cli_arg(args.trade_amount, 100.0)
args.leverage = parse_cli_arg(args.leverage, 2.0)
args.entry_offset = parse_cli_arg(args.entry_offset, 15.0)
args.min_profit = parse_cli_arg(args.min_profit, 5.0)
args.trailing_drop = parse_cli_arg(args.trailing_drop, 10.0)
args.trailing_stop_below = parse_cli_arg(args.trailing_stop_below, 0.90)
args.trailing_stop_60_100 = parse_cli_arg(args.trailing_stop_60_100, 0.85)
args.trailing_stop_above = parse_cli_arg(args.trailing_stop_above, 0.80)
args.mega_stop = parse_cli_arg(args.mega_stop, 6.0)
args.fixed_stoploss_offset = parse_cli_arg(args.fixed_stoploss_offset, 5.0)
args.emergency_stop_percent = parse_cli_arg(args.emergency_stop_percent, 20.0)

COIN_LIST = [c.strip().upper() for c in args.coin.split(",") if c.strip()]

# Konfiguration laden
config = {}
config_file = "config.json"
if os.path.exists(config_file):
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
        print(f"config.json erfolgreich geladen: {json.dumps(config, indent=2)}")
    except Exception as e:
        print(f"Fehler beim Laden von config.json: {str(e)}. Standardwerte werden verwendet.")
else:
    print("config.json nicht gefunden. Verwende Kommandozeilenargumente oder Standardwerte.")

def get_gui_value(param_name):
    # Hier kann die GUI-Integration (über Qt Designer) erfolgen, um Parameter manuell zu übersteuern.
    return None

def get_param(param_name, cli_value, coin, config_key, default_value, prompt_text):
    if cli_value is not None:
        if isinstance(cli_value, list):
            try:
                idx = COIN_LIST.index(coin.upper())
                return cli_value[idx] if idx < len(cli_value) else cli_value[0]
            except ValueError:
                return cli_value[0]
        else:
            return cli_value
    gui_value = get_gui_value(param_name)
    if gui_value is not None:
        return gui_value
    coin_config = config.get(f"COIN {coin}", {})
    if config_key in coin_config:
        return coin_config[config_key]
    elif param_name in config:
        return config[param_name]
    try:
        value = float(input(f"{prompt_text} für {coin}: "))
        return value
    except (ValueError, EOFError):
        print(f"Eingabe ungültig oder nicht möglich. Verwende Standardwert: {default_value}")
        return default_value

# Weitere globale Konstanten
minimum_trade_amount = 0.001
FEE_RATE = 0.0004
GLOBAL_STOP_ROE_THRESHOLD = 0.0005

TREND_PERIOD = 10
USE_EMA_TREND = True
USE_MACD_TREND = False
USE_ADX_TREND = False
RSI_THRESHOLD = 45
MACD_FAST = 8
MACD_SLOW = 17
ADX_THRESHOLD = 8

# Logging und CSV
LOG_DIR = "logs"
LOG_FILE = "bot_log.txt"
ERROR_LOG_FILE = "error_log.txt"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
TRADE_LOG_FILE = os.path.join(LOG_DIR, "trades_log.csv")
csv_lock = Lock()

def initialize_csv(file_path):
    is_new = not os.path.exists(file_path) or os.stat(file_path).st_size == 0
    csv_file = open(file_path, mode="a", newline="", encoding="utf-8-sig")
    csv_writer = csv.writer(csv_file, delimiter=';')
    if is_new:
        csv_writer.writerow([
            "Zeit", "Datum", "Coin", "Futures", "Aktion", "Summe", "Hebel", "Betrag", "Gebühr", "Gewinn",
            "Wallet-Saldo", "Transaktionsbetrag"
        ])
        csv_file.flush()
    return csv_file, csv_writer

csv_file, csv_writer = initialize_csv(TRADE_LOG_FILE)
atexit.register(lambda: csv_file.close())
logging.basicConfig(
    filename=os.path.join(LOG_DIR, LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def log_info(msg):
    logging.info(msg)
    print(msg)

def log_error(msg):
    logging.error(msg)
    print(f"ERROR: {msg}")
    with open(os.path.join(LOG_DIR, ERROR_LOG_FILE), "a", encoding="utf-8") as f:
        f.write(f"{datetime.now()} - ERROR - {msg}\n")

def safe_decimal(value, default=Decimal("0.0")):
    if value is None:
        return default
    try:
        return Decimal(str(value))
    except Exception as e:
        log_error(f"Error converting {value} to Decimal: {e}")
        return default

def log_trade(coin_log, futures_log, action, summe, betrag, gebuehr_usdt, wallet_saldo, transaktionsbetrag=""):
    summe_decimal = safe_decimal(summe)
    betrag_decimal = safe_decimal(betrag)
    gebuehr_decimal = safe_decimal(gebuehr_usdt)
    wallet_decimal = safe_decimal(wallet_saldo)
    gewinn = betrag_decimal - gebuehr_decimal
    now = datetime.now()
    zeit = now.strftime("%H:%M")
    datum = now.strftime("%d.%m.%Y")
    with csv_lock:
        csv_writer.writerow([
            zeit,
            datum,
            coin_log,
            futures_log,
            action,
            f"{int(summe_decimal)}",
            f"{float(args.leverage):.0f}",
            f"{betrag_decimal:.8f}",
            f"{gebuehr_decimal:.8f}",
            f"{gewinn:.8f}",
            f"{float(wallet_decimal):.2f}",
            transaktionsbetrag
        ])
        csv_file.flush()

def create_signature(timestamp, method, request_path, body=''):
    message = str(timestamp) + method + request_path + body
    mac = hmac.new(API_SECRET.encode('utf-8'), msg=message.encode('utf-8'), digestmod=hashlib.sha256)
    return base64.b64encode(mac.digest()).decode()

try:
    bitget = ccxt.bitget({
        'apiKey': API_KEY,
        'secret': API_SECRET,
        'password': PASSWORD,
    })
    log_info("Bitget API successfully initialized.")
except Exception as e:
    log_error(f"Error initializing Bitget: {e}")
    sys.exit(1)

#############################################
# KI-Funktionen für dynamische Entry-Signale
#############################################

def predict_trend(prices, window=5):
    if len(prices) < window:
        return prices[-1] if prices else None, "NO_ACTION"
    moving_avg = np.mean(prices[-window:])
    last_price = prices[-1]
    trend = "UPTREND" if last_price > moving_avg else "DOWNTREND" if last_price < moving_avg else "NO_ACTION"
    return moving_avg, trend

def predict_next_price(prices):
    pred, _ = predict_trend(prices, window=5)
    return pred

def calculate_adx(prices, period=14):
    if len(prices) < period + 1:
        return None
    window_prices = np.array(prices[-period:])
    return np.std(window_prices)

def get_sentiment_from_gpt4(text):
    prompt = (
        f"Analysiere den folgenden Text und gib eine numerische Bewertung des Sentiments zurück, "
        f"wobei -1 sehr negativ, 0 neutral und +1 sehr positiv bedeutet:\n\n\"{text}\"\n\nAntworte nur mit der Zahl:"
    )
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "system",
                       "content": "Du bist ein Sentiment-Analyst, der präzise numerische Bewertungen liefert."},
                      {"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=10
        )
        answer = response["choices"][0]["message"]["content"].strip()
        sentiment_score = float(answer.split()[0])
        return sentiment_score
    except Exception as e:
        log_error(f"Fehler in get_sentiment_from_gpt4: {e}")
        return 0.0

def analyze_sentiment():
    sample_text = "Die aktuellen Marktbedingungen sind positiv und deuten auf Aufwärtstrends hin."
    return get_sentiment_from_gpt4(sample_text)

def aggregate_entry_signal(lstm_pred, sentiment_score, reserved_price):
    if lstm_pred is None:
        return "NO_ACTION"
    if (lstm_pred > reserved_price and sentiment_score > 0.1):
        return "LONG"
    elif (lstm_pred < reserved_price and sentiment_score < -0.1):
        return "SHORT"
    else:
        return "NO_ACTION"

def calculate_volatility(prices, window=10):
    if len(prices) < window:
        return 0.0
    return np.std(np.array(prices[-window:]))

#############################################
# TradingBot-Klasse
#############################################

class TradingBot:
    def __init__(self, coin):
        self.coin = coin.upper()
        self.API_SYMBOL = f"{self.coin}USDT_UMCBL"
        self.symbol = f"{self.coin}/USDT:USDT"
        log_info(f"Trading coin: {self.coin} | API_SYMBOL: {self.API_SYMBOL} | Symbol: {self.symbol}")

        self.trade_amount = get_param("TRADE_AMOUNT_USD", args.trade_amount, self.coin, "TRADE_AMOUNT_USD", 100.0,
                                      "Bitte Handelsbetrag in USD eingeben")
        self.leverage = get_param("LEVERAGE", args.leverage, self.coin, "LEVERAGE", 2.0, "Bitte Hebel eingeben")
        self.entry_offset = get_param("ENTRY_TRIGGER_OFFSET_USD", args.entry_offset, self.coin,
                                      "ENTRY_TRIGGER_OFFSET_USD", 15.0, "Bitte Entry Trigger Offset in USD eingeben")
        self.min_profit = get_param("MINIMUM_REQUIRED_PROFIT_USD", args.min_profit, self.coin,
                                    "MINIMUM_REQUIRED_PROFIT_USD", 5.0,
                                    "Bitte minimalen erforderlichen Profit in USD eingeben")
        self.trailing_drop = args.trailing_drop
        self.trailing_stop_below = get_param("TRAILING_STOP_PERCENT_BELOW_60", args.trailing_stop_below, self.coin,
                                             "TRAILING_STOP_PERCENT_BELOW_60", 0.90,
                                             "Bitte Trailing Stop Prozent unter 60 USD eingeben")
        self.trailing_stop_60_100 = get_param("TRAILING_STOP_PERCENT_BETWEEN_60_AND_100", args.trailing_stop_60_100,
                                              self.coin, "TRAILING_STOP_PERCENT_BETWEEN_60_AND_100", 0.85,
                                              "Bitte Trailing Stop Prozent zwischen 60 und 100 USD eingeben")
        self.trailing_stop_above = get_param("TRAILING_STOP_PERCENT_ABOVE_100", args.trailing_stop_above, self.coin,
                                             "TRAILING_STOP_PERCENT_ABOVE_100", 0.80,
                                             "Bitte Trailing Stop Prozent über 100 USD eingeben")
        self.mega_stop = get_param("MEGA_STOP_TRIGGER_USD", args.mega_stop, self.coin, "MEGA_STOP_TRIGGER_USD", 6.0,
                                   "Bitte Mega Stop Trigger in USD eingeben")
        self.fixed_stoploss_offset = get_param("FIXED_STOPLOSS_OFFSET", args.fixed_stoploss_offset, self.coin,
                                               "FIXED_STOPLOSS_OFFSET", 5.0,
                                               "Bitte Fixed Stop-Loss Offset in USD eingeben")
        self.emergency_stop_percent = get_param("EMERGENCY_STOP_PERCENT", args.emergency_stop_percent, self.coin,
                                                "EMERGENCY_STOP_PERCENT", 20.0, "Bitte Emergency Stop Prozent eingeben")

        log_info(
            f"Parameters for {self.coin}: Trade Amount: {self.trade_amount}, Leverage: {self.leverage}, Entry Offset: {self.entry_offset}, Min Profit: {self.min_profit}, Fixed Stop-Loss Offset: {self.fixed_stoploss_offset}, Trailing Stop Below: {self.trailing_stop_below}, Trailing Stop 60-100: {self.trailing_stop_60_100}, Trailing Stop Above: {self.trailing_stop_above}, Mega Stop: {self.mega_stop}, Emergency Stop: {self.emergency_stop_percent}%")

        self.profit_take_stages = get_param("PROFIT_TAKE_STAGES", None, self.coin, "PROFIT_TAKE_STAGES", [],
                                            "Bitte Gewinnmitnahme-Stufen für " + self.coin + " eingeben")
        if not isinstance(self.profit_take_stages, list):
            self.profit_take_stages = []
        for stage in self.profit_take_stages:
            stage.setdefault("executed", False)

        self.current_market_price = None
        self.price_history = []
        self.data_lock = threading.Lock()  # Lock für threadsicheren Datenzugriff
        self.last_message_time = time.time()
        self.force_exit = False
        self.trade_entry_balance = None
        self.trade_side = None
        self.trade_reserved_price = None
        self.trade_quantity = None

    async def process_async_message(self, message):
        self.last_message_time = time.time()
        try:
            data = json.loads(message)
            if "action" in data and "data" in data:
                arg = data.get("arg", {})
                if arg.get("channel") == "ticker" and arg.get("instId") == f"{self.coin}USDT":
                    ticker_data = data["data"][0]
                    market_price = float(ticker_data.get("last", 0))
                    if market_price > 0:
                        with self.data_lock:
                            self.current_market_price = market_price
                            self.price_history.append(market_price)
                            if len(self.price_history) > 20:
                                self.price_history.pop(0)
                        log_info(f"[{self.coin}] Current Market Price: {market_price}")
        except Exception as e:
            log_error(f"[{self.coin}] Error parsing async WebSocket message: {e}")

    async def async_websocket_handler(self):
        url = "wss://ws.bitget.com/mix/v1/stream"
        reconnect_delay = 1
        max_reconnect_delay = 60
        while not self.force_exit:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    reconnect_delay = 1
                    sub_msg = json.dumps({
                        "op": "subscribe",
                        "args": [{"instType": "mc", "channel": "ticker", "instId": f"{self.coin}USDT"}]
                    })
                    await ws.send(sub_msg)
                    log_info(f"[{self.coin}] Async WebSocket subscription successful.")
                    async for message in ws:
                        await self.process_async_message(message)
                        if self.force_exit:
                            break
            except Exception as e:
                log_error(f"[{self.coin}] WebSocket error: {e}")
            if self.force_exit:
                break
            log_info(f"[{self.coin}] Reconnecting in {reconnect_delay} seconds...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

    def start_async_websocket_thread(self):
        t = threading.Thread(target=lambda: asyncio.run(self.async_websocket_handler()), daemon=True)
        t.start()
        return t

    def check_position_open(self):
        try:
            positions = bitget.fetch_positions([self.symbol])
            for pos in positions:
                if pos.get('contracts') and abs(float(pos['contracts'])) > 0:
                    return True
            return False
        except Exception as e:
            log_error(f"[{self.coin}] Error checking position: {e}")
            return False

    def get_actual_position_size(self):
        try:
            positions = bitget.fetch_positions([self.symbol])
            for pos in positions:
                if pos.get('contracts') and abs(float(pos['contracts'])) > 0:
                    return abs(float(pos['contracts']))
            return 0.0
        except Exception as e:
            log_error(f"[{self.coin}] Error getting position size: {e}")
            return 0.0

    def cancel_all_plan_orders_requests(self):
        timestamp = str(int(time.time() * 1000))
        plan_types = ["normal_plan", "profit_plan", "loss_plan", "track_plan"]
        for pt in plan_types:
            endpoint = "/api/mix/v1/plan/cancelSymbolPlan"
            data_ = {"symbol": self.API_SYMBOL, "marginCoin": "USDT", "planType": pt}
            try:
                resp = requests.post(
                    BASE_URL + endpoint, json=data_,
                    headers={
                        "ACCESS-KEY": API_KEY,
                        "ACCESS-SIGN": create_signature(timestamp, "POST", endpoint, json.dumps(data_)),
                        "ACCESS-TIMESTAMP": timestamp,
                        "ACCESS-PASSPHRASE": PASSWORD,
                        "Content-Type": "application/json"
                    }
                )
                log_info(f"[{self.coin}] Plan orders [planType={pt}] canceled: {resp.text}")
            except Exception as e:
                log_error(f"[{self.coin}] Error canceling plan orders [planType={pt}]: {e}")

    def cancel_all_open_orders(self):
        try:
            timestamp = str(int(time.time() * 1000))
            open_orders_resp = requests.get(
                BASE_URL + "/api/mix/v1/order/current",
                params={"symbol": self.API_SYMBOL, "marginCoin": "USDT"},
                headers={
                    "ACCESS-KEY": API_KEY,
                    "ACCESS-SIGN": create_signature(timestamp, "GET", "/api/mix/v1/order/current"),
                    "ACCESS-TIMESTAMP": timestamp,
                    "ACCESS-PASSPHRASE": PASSWORD
                }
            )
            orders_data = open_orders_resp.json().get("data", [])
            if orders_data:
                order_ids = [o["orderId"] for o in orders_data]
                if order_ids:
                    endpoint = "/api/mix/v1/order/cancel-batch-orders"
                    cancel_data = {"symbol": self.API_SYMBOL, "marginCoin": "USDT", "orderIds": order_ids}
                    r = requests.post(
                        BASE_URL + endpoint, json=cancel_data,
                        headers={
                            "ACCESS-KEY": API_KEY,
                            "ACCESS-SIGN": create_signature(timestamp, "POST", endpoint, json.dumps(cancel_data)),
                            "ACCESS-TIMESTAMP": timestamp,
                            "ACCESS-PASSPHRASE": PASSWORD,
                            "Content-Type": "application/json"
                        }
                    )
                    log_info(f"[{self.coin}] All orders canceled (batch): {r.text}")
            self.cancel_all_plan_orders_requests()
            leftover_orders = bitget.fetch_open_orders(self.symbol)
            for leftover in leftover_orders:
                oid = leftover["id"]
                try:
                    bitget.cancel_order(oid, self.symbol)
                    log_info(f"[{self.coin}] Canceled leftover order. ID={oid}")
                except Exception as e:
                    log_error(f"[{self.coin}] Error canceling leftover order: {e}")
        except Exception as e:
            log_error(f"[{self.coin}] Error canceling open orders: {e}")

    def close_all_positions(self):
        try:
            positions = bitget.fetch_positions([self.symbol])
            for pos in positions:
                if pos.get('contracts') and abs(float(pos['contracts'])) > 0:
                    close_side = 'sell' if pos.get('side', '').lower() == 'long' else 'buy'
                    quantity = abs(float(pos['contracts']))
                    try:
                        bitget.create_order(self.symbol, 'market', close_side, quantity)
                        log_info(f"[{self.coin}] Closed {pos.get('side').upper()} position: {close_side.upper()} {quantity} {self.symbol}")
                    except Exception as e:
                        log_error(f"[{self.coin}] Error closing position: {e}")
        except Exception as e:
            log_error(f"[{self.coin}] Error fetching positions: {e}")

    def clear_all_orders_and_positions(self):
        try:
            self.cancel_all_open_orders()
        except Exception as e:
            log_error(f"[{self.coin}] Error canceling open orders: {e}")
        try:
            self.close_all_positions()
        except Exception as e:
            log_error(f"[{self.coin}] Error closing open positions: {e}")
        max_wait = 10
        start_time = time.time()
        while time.time() - start_time < max_wait:
            try:
                orders = bitget.fetch_open_orders(self.symbol)
                positions = bitget.fetch_positions([self.symbol])
            except Exception as e:
                log_error(f"[{self.coin}] Error fetching open orders/positions: {e}")
                orders = []
                positions = []
            open_positions = [pos for pos in positions if abs(float(pos.get('contracts', 0))) > 0]
            if not orders and not open_positions:
                log_info(f"[{self.coin}] All open orders and positions are cleared.")
                return
            else:
                log_info(f"[{self.coin}] {len(orders)} open orders & {len(open_positions)} open positions remain. Waiting 1s...")
                time.sleep(1)
        log_error(f"[{self.coin}] Not all orders/positions could be cleared in time.")

    def exit_trade(self):
        self.cancel_all_open_orders()
        self.close_all_positions()

    def stop_bot(self):
        log_info(f"[{self.coin}] Stop command received. Terminating bot immediately.")
        self.force_exit = True
        self.clear_all_orders_and_positions()
        # Graceful shutdown statt os._exit
        return

    def place_limit_order(self, side, price, quantity):
        try:
            order = bitget.create_order(self.symbol, "limit", side, quantity, price)
            log_info(f"[{self.coin}] Placed LIMIT {side.upper()} @ {price:.2f}, qty={quantity:.6f}")
            return order
        except Exception as e:
            log_error(f"[{self.coin}] Error placing limit order: {e}")
            return None

    def place_market_order(self, side, quantity, price_for_log=None, params={}):
        try:
            order = bitget.create_order(self.symbol, "market", side, quantity, None, params)
            if price_for_log is None:
                log_info(f"[{self.coin}] Placed MARKET {side.upper()} (qty={quantity:.6f})")
            else:
                log_info(f"[{self.coin}] Placed MARKET {side.upper()} @ ~{price_for_log:.2f}, qty={quantity:.6f}")
            return order
        except Exception as e:
            log_error(f"[{self.coin}] Error placing market order: {e}")
            return None

    def wait_for_order_filled(self, timeout=30):
        start_time = time.time()
        while time.time() - start_time < timeout:
            open_orders = bitget.fetch_open_orders(self.symbol)
            if not open_orders:
                return True
            log_info(f"[{self.coin}] Waiting for order fill...")
            time.sleep(1)
        return False

    def wait_for_position(self, timeout=30):
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.check_position_open():
                return True
            time.sleep(1)
        return False

    def get_actual_trade_fee(self, order_id):
        try:
            trades = bitget.fetch_my_trades(self.symbol)
            total_fee = 0.0
            for trade in trades:
                if trade.get('order') == order_id:
                    fee_info = trade.get('fee', {})
                    total_fee += fee_info.get('cost', 0)
            return total_fee
        except Exception as e:
            log_error(f"[{self.coin}] Error fetching trade fee for order {order_id}: {e}")
            return 0.0

    def calculate_ema(self, data, period):
        if len(data) < period:
            return None
        ema = data[0]
        alpha = 2 / (period + 1)
        for price in data[1:]:
            ema = alpha * price + (1 - alpha) * ema
        return ema

    def calculate_macd(self, prices, fast=MACD_FAST, slow=MACD_SLOW):
        if len(prices) < slow:
            return None, None
        ema_fast = self.calculate_ema(prices[-fast:], fast)
        ema_slow = self.calculate_ema(prices[-slow:], slow)
        if ema_fast is None or ema_slow is None:
            return None, None
        macd = ema_fast - ema_slow
        signal = self.calculate_ema(prices[-slow:], slow)
        return macd, signal

    def calculate_adx(self, prices, period=TREND_PERIOD):
        if len(prices) < period + 1:
            return None
        diffs = [abs(prices[i] - prices[i - 1]) for i in range(1, len(prices))]
        adx = np.mean(diffs[-period:])
        return adx

    def calculate_rsi(self, data, period=14):
        if len(data) < period:
            return None
        gains = [max(0, data[i] - data[i - 1]) for i in range(1, len(data))]
        losses = [max(0, data[i - 1] - data[i]) for i in range(1, len(data))]
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        if avg_loss == 0:
            return 100
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def take_profit_stage(self, base_price):
        if not self.profit_take_stages:
            return base_price
        with self.data_lock:
            current_price = self.current_market_price
        if self.trade_side == "LONG":
            profit_pct = ((current_price - base_price) / base_price) * 100
        else:  # SHORT
            profit_pct = ((base_price - current_price) / base_price) * 100
        current_position = self.get_actual_position_size()
        executed_count = sum(1 for stage in self.profit_take_stages if stage.get("executed", False))
        for stage in self.profit_take_stages:
            if profit_pct >= stage["profit_pct"] and not stage.get("executed", False):
                exit_pct = stage["exit_pct"]
                exit_qty = current_position * (exit_pct / 100.0)
                if exit_qty < minimum_trade_amount:
                    required_exit_pct = (minimum_trade_amount / current_position) * 100.0
                    if required_exit_pct > exit_pct:
                        log_info(f"[{self.coin}] Dynamische Anpassung: exit_pct von {exit_pct:.2f}% auf {required_exit_pct:.2f}% erhöht, um den Mindesthandelsbetrag zu erreichen.")
                        exit_pct = required_exit_pct
                        exit_qty = current_position * (exit_pct / 100.0)
                    if exit_qty < minimum_trade_amount:
                        log_info(f"[{self.coin}] Berechnete Exit-Menge {exit_qty:.6f} liegt auch nach dynamischer Anpassung unter dem Mindesthandelsbetrag.")
                        continue
                log_info(f"[{self.coin}] Gewinnstufe erreicht: {profit_pct:.2f}% Profit. Teilposition von {exit_qty:.4f} wird verkauft.")
                order_side = "sell" if self.trade_side == "LONG" else "buy"
                self.place_market_order(order_side, exit_qty, price_for_log=self.current_market_price)
                stage["executed"] = True
                executed_count += 1
                break
        if executed_count == len(self.profit_take_stages):
            for stage in self.profit_take_stages:
                stage["executed"] = False
            new_base = self.current_market_price
            log_info(f"[{self.coin}] Alle Teilverkaufsstufen erreicht. Profit-Take-Stufen zurückgesetzt. Neuer Basispreis: {new_base:.2f}")
            return new_base
        return base_price

    def monitor_trailing_stops(self, order_side_is_long, entry_price, min_trade_duration=600, trend_window=5):
        log_info(f"[{self.coin}] Trailing Stop monitoring started for {'LONG' if order_side_is_long else 'SHORT'}. Entry = {entry_price:.2f}")
        trade_start_time = time.time()
        base_price = entry_price
        if order_side_is_long:
            highest_price = entry_price
        else:
            lowest_price = entry_price

        while not self.force_exit:
            with self.data_lock:
                current_price = self.current_market_price
            if current_price is None:
                time.sleep(0.5)
                continue

            # Mega Stop check
            if order_side_is_long and current_price < entry_price - self.mega_stop:
                log_info(f"[{self.coin}] [Mega Stop] LONG triggered. Exiting position.")
                self.exit_trade()
                return
            elif not order_side_is_long and current_price > entry_price + self.mega_stop:
                log_info(f"[{self.coin}] [Mega Stop] SHORT triggered. Exiting position.")
                self.exit_trade()
                return

            # Emergency Stop – LONG
            if order_side_is_long:
                profit = highest_price - entry_price
                if profit > 0:
                    emergency_level = entry_price + profit * (1 - self.emergency_stop_percent / 100.0)
                    if current_price <= emergency_level:
                        profit_pct = (profit / entry_price) * 100
                        log_info(f"[{self.coin}] [EMERGENCY STOP] LONG triggered: Highest = {highest_price:.2f} USD, Entry = {entry_price:.2f} USD, Current = {current_price:.2f} USD, Emergency level = {emergency_level:.2f} USD, Profit = {profit:.2f} USD ({profit_pct:.2f}%). Emergency Stop = {self.emergency_stop_percent:.0f}%. Exiting trade immediately.")
                        self.exit_trade()
                        return
            else:
                profit = entry_price - lowest_price
                if profit > 0:
                    emergency_level = entry_price - profit * (1 - self.emergency_stop_percent / 100.0)
                    if current_price >= emergency_level:
                        profit_pct = (profit / entry_price) * 100
                        log_info(f"[{self.coin}] [EMERGENCY STOP] SHORT triggered: Lowest = {lowest_price:.2f} USD, Entry = {entry_price:.2f} USD, Current = {current_price:.2f} USD, Emergency level = {emergency_level:.2f} USD, Profit = {profit:.2f} USD ({profit_pct:.2f}%). Emergency Stop = {self.emergency_stop_percent:.0f}%. Exiting trade immediately.")
                        self.exit_trade()
                        return

            # Update highest (LONG) bzw. lowest (SHORT)
            if order_side_is_long:
                if current_price > highest_price:
                    highest_price = current_price
                    log_info(f"[{self.coin}] New highest price: {highest_price:.2f}")
            else:
                if current_price < lowest_price:
                    lowest_price = current_price
                    log_info(f"[{self.coin}] New lowest price: {lowest_price:.2f}")

            profit = (highest_price - base_price) if order_side_is_long else (base_price - lowest_price)
            base_price = self.take_profit_stage(base_price)
            trade_duration = time.time() - trade_start_time
            if trade_duration < min_trade_duration:
                log_info(f"[{self.coin}] Trade duration ({trade_duration:.0f}s) unter minimum ({min_trade_duration}s). Exit ist verzögert.")
                time.sleep(0.5)
                continue

            if len(self.price_history) >= trend_window:
                _, current_trend = predict_trend(self.price_history, window=trend_window)
            else:
                current_trend = "NO_ACTION"

            trend_broken = (current_trend != "UPTREND") if order_side_is_long else (current_trend != "DOWNTREND")

            if profit < self.min_profit:
                log_info(f"[{self.coin}] Profit {profit:.2f} USD is below minimum {self.min_profit:.2f} USD. Waiting...")
                time.sleep(0.5)
                continue

            if profit < 60:
                trailing_pct = self.trailing_stop_below / 100.0
                used_pct = self.trailing_stop_below
            elif profit <= 100:
                trailing_pct = self.trailing_stop_60_100 / 100.0
                used_pct = self.trailing_stop_60_100
            else:
                trailing_pct = self.trailing_stop_above / 100.0
                used_pct = self.trailing_stop_above

            if order_side_is_long:
                final_stop = highest_price - (profit * trailing_pct)
                log_info(f"[{self.coin}] [LONG] Highest = {highest_price:.2f}, Profit = {profit:.2f} USD, final_stop = {final_stop:.2f} ({used_pct}% of profit)")
                if current_price <= final_stop and trend_broken:
                    log_info(f"[{self.coin}] [LONG] Current price {current_price:.2f} <= final_stop {final_stop:.2f} and trend broken. Triggering exit.")
                    self.exit_trade()
                    return
            else:
                final_stop = lowest_price + (profit * trailing_pct)
                log_info(f"[{self.coin}] [SHORT] Lowest = {lowest_price:.2f}, Profit = {profit:.2f} USD, final_stop = {final_stop:.2f} ({used_pct}% of profit)")
                if current_price >= final_stop and trend_broken:
                    log_info(f"[{self.coin}] [SHORT] Current price {current_price:.2f} >= final_stop {final_stop:.2f} and trend broken. Triggering exit.")
                    self.exit_trade()
                    return

            time.sleep(0.5)

    def dynamic_entry_signal(self, reserved_price):
        lstm_pred = predict_next_price(self.price_history)
        sentiment_score = analyze_sentiment()
        with self.data_lock:
            current_price = self.current_market_price
        if current_price <= reserved_price - self.entry_offset:
            signal = "SHORT"
        elif current_price >= reserved_price + self.entry_offset:
            signal = "LONG"
        else:
            if lstm_pred > reserved_price and sentiment_score > 0.1:
                signal = "LONG"
            elif lstm_pred < reserved_price and sentiment_score < -0.1:
                signal = "SHORT"
            else:
                signal = "NO_ACTION"
        log_info(f"[{self.coin}] KI Entry Signal: LSTM_Pred={lstm_pred:.2f} | Sentiment={sentiment_score:.2f} -> {signal}")
        return signal

    def execute_reserved_long(self, reserved_price):
        log_info(f"[{self.coin}] [RESERVED LONG] Entry at {reserved_price:.2f}")
        self.clear_all_orders_and_positions()
        available_balance = self.get_futures_balance()
        log_info(f"[{self.coin}] Available balance: {available_balance:.2f} USDT")
        if self.trade_amount > available_balance:
            log_error(f"[{self.coin}] Insufficient funds: Trade amount {self.trade_amount} exceeds available balance {available_balance:.2f}. Aborting LONG.")
            return
        quantity = round(self.trade_amount / reserved_price, 4)
        log_info(f"[{self.coin}] Calculated LONG quantity: {quantity:.4f} (Reserved price: {reserved_price:.2f})")
        if quantity < minimum_trade_amount:
            log_error(f"[{self.coin}] Calculated quantity {quantity:.6f} is below exchange minimum {minimum_trade_amount}. Aborting LONG.")
            return
        self.trade_entry_balance = available_balance
        self.trade_side = "LONG"
        self.trade_reserved_price = reserved_price
        self.trade_quantity = quantity

        order = self.place_market_order("buy", quantity, price_for_log=self.current_market_price)
        if not order or not self.wait_for_order_filled(timeout=30):
            log_error(f"[{self.coin}] Market BUY fill not confirmed. Aborting LONG.")
            return
        time.sleep(1)
        if not self.wait_for_position(timeout=5):
            log_error(f"[{self.coin}] No position found after LONG entry. Aborting.")
            return

        actual_entry_price = order.get("price") or (self.current_market_price or reserved_price)
        if not self.set_fixed_stop_loss(True, actual_entry_price):
            log_error(f"[{self.coin}] Stop-loss not set. Closing LONG position.")
            self.close_all_positions()
            return

        filled_amount = order.get("filled", self.trade_quantity)
        actual_fee = self.get_actual_trade_fee(order.get("id"))
        current_balance = self.get_futures_balance()
        log_trade(self.coin, self.API_SYMBOL, f"{self.coin} LONG Kaufen", self.trade_amount, filled_amount, actual_fee, current_balance)
        threading.Thread(target=self.monitor_trailing_stops, args=(True, actual_entry_price), daemon=True).start()
        log_info(f"[{self.coin}] [LONG] Trailing-Stops thread started.")

    def get_filled_amount(self, order, fallback):
        filled = order.get("filled")
        if filled in [None, "", 0]:
            info = order.get("info", {})
            filled = info.get("filledSize", fallback)
        try:
            return Decimal(str(filled))
        except Exception as e:
            log_error(f"[{self.coin}] Error converting filled amount {filled} to Decimal: {e}")
            return Decimal(str(fallback))

    def execute_reserved_short(self, reserved_price):
        log_info(f"[{self.coin}] [RESERVED SHORT] Entry at {reserved_price:.2f}")
        self.clear_all_orders_and_positions()
        available_balance = self.get_futures_balance()
        log_info(f"[{self.coin}] Available balance: {available_balance:.2f} USDT")
        if self.trade_amount > available_balance:
            log_error(f"[{self.coin}] Insufficient funds: Trade amount {self.trade_amount} exceeds available balance {available_balance:.2f}. Aborting SHORT.")
            return
        quantity = round(self.trade_amount / reserved_price, 4)
        log_info(f"[{self.coin}] Calculated SHORT quantity: {quantity:.4f} (Reserved price: {reserved_price:.2f})")
        if quantity < minimum_trade_amount:
            log_error(f"[{self.coin}] Calculated quantity {quantity:.6f} is below exchange minimum {minimum_trade_amount}. Aborting SHORT.")
            return
        self.trade_entry_balance = available_balance
        self.trade_side = "SHORT"
        self.trade_reserved_price = reserved_price
        self.trade_quantity = quantity

        params = {"openType": "open", "positionSide": "short"}
        order = self.place_market_order("sell", quantity, price_for_log=self.current_market_price, params=params)
        if not order or not self.wait_for_order_filled(timeout=30):
            log_error(f"[{self.coin}] Market SELL fill not confirmed. Aborting SHORT.")
            return
        time.sleep(1)
        if not self.wait_for_position(timeout=5):
            log_error(f"[{self.coin}] No position found after SHORT entry. Aborting.")
            return

        actual_entry_price = order.get("price") or (self.current_market_price or reserved_price)
        if not self.set_fixed_stop_loss(False, actual_entry_price):
            log_error(f"[{self.coin}] Stop-loss not set. Closing SHORT position.")
            self.close_all_positions()
            return

        filled_amount = self.get_filled_amount(order, self.trade_quantity)
        actual_fee = self.get_actual_trade_fee(order.get("id"))
        current_balance = self.get_futures_balance()
        log_trade(self.coin, self.API_SYMBOL, f"{self.coin} SHORT Verkaufen", self.trade_amount, filled_amount, actual_fee, current_balance)
        threading.Thread(target=self.monitor_trailing_stops, args=(False, actual_entry_price), daemon=True).start()
        log_info(f"[{self.coin}] [SHORT] Trailing-Stops thread started.")

    def get_futures_balance(self):
        try:
            futures_balance = bitget.fetch_balance(params={"type": "swap"})
            if "USDT" in futures_balance.get("free", {}):
                return float(futures_balance["free"]["USDT"])
            else:
                return float(futures_balance["total"]["USDT"])
        except Exception as e:
            log_error(f"[{self.coin}] Error fetching futures balance: {e}")
            return 0.0

    def set_fixed_stop_loss(self, order_side_is_long, entry_price):
        actual_quantity = self.get_actual_position_size()
        if abs(actual_quantity) == 0:
            log_error(f"[{self.coin}] No valid position size -> Cannot set stop-loss.")
            return False
        if self.current_market_price is None:
            log_error(f"[{self.coin}] No market price -> Cannot set stop-loss.")
            return False
        endpoint = "/api/mix/v1/plan/placeTPSL"
        t = str(int(time.time() * 1000))
        if order_side_is_long:
            stop_loss_price = entry_price - self.fixed_stoploss_offset
            if stop_loss_price >= self.current_market_price:
                stop_loss_price = self.current_market_price - 0.1
            hold_side = "long"
        else:
            stop_loss_price = entry_price + self.fixed_stoploss_offset
            if stop_loss_price <= self.current_market_price:
                stop_loss_price = self.current_market_price + 0.1
            hold_side = "short"
        sl_data = {
            "symbol": self.API_SYMBOL,
            "marginCoin": "USDT",
            "planType": "loss_plan",
            "triggerPrice": f"{stop_loss_price:.2f}",
            "executePrice": f"{stop_loss_price:.2f}",
            "holdSide": hold_side,
            "size": f"{abs(actual_quantity):.4f}"
        }
        sign = create_signature(t, "POST", endpoint, json.dumps(sl_data))
        headers = {
            "ACCESS-KEY": API_KEY,
            "ACCESS-SIGN": sign,
            "ACCESS-TIMESTAMP": t,
            "ACCESS-PASSPHRASE": PASSWORD,
            "Content-Type": "application/json"
        }
        try:
            resp = requests.post(BASE_URL + endpoint, json=sl_data, headers=headers)
            resp_data = resp.json()
            if resp_data.get("code") == "00000":
                log_info(f"[{self.coin}] [STOP-LOSS] set at {stop_loss_price:.2f} for {hold_side.upper()}.")
                return True
            else:
                log_error(f"[{self.coin}] [STOP-LOSS] Failed. Response: {resp.text}")
                return False
        except Exception as e:
            log_error(f"[{self.coin}] Error setting stop-loss: {e}")
            return False

    def get_position_roe(self):
        try:
            positions = bitget.fetch_positions([self.symbol])
            for pos in positions:
                if pos.get('contracts') and float(pos['contracts']) > 0:
                    if 'unrealizedPnl' in pos and 'initialMargin' in pos:
                        pnl = float(pos['unrealizedPnl'])
                        im = float(pos['initialMargin']) if float(pos['initialMargin']) != 0 else 1e-8
                        return pnl / im
        except Exception as e:
            log_error(f"[{self.coin}] Error fetching position ROE: {e}")
        return 0.0

    def run(self):
        self.start_async_websocket_thread()
        while True:
            with self.data_lock:
                if self.current_market_price is not None:
                    break
            log_info(f"[{self.coin}] Waiting for initial market data...")
            time.sleep(1)
        try:
            try:
                bitget.set_leverage(self.leverage, self.symbol)
                log_info(f"[{self.coin}] Leverage for {self.symbol} set to {self.leverage}x.")
            except Exception as e:
                log_error(f"[{self.coin}] Error setting leverage: {e}")
            while not self.force_exit:
                try:
                    if self.check_position_open():
                        time.sleep(1)
                        continue
                    with self.data_lock:
                        reserved_price = self.current_market_price
                    log_info(f"[{self.coin}] [MAIN] Reserved price = {reserved_price:.2f}. Waiting for ± {self.entry_offset}...")
                    triggered = False
                    while not triggered and not self.force_exit:
                        with self.data_lock:
                            current_price = self.current_market_price
                        if current_price is None:
                            time.sleep(0.3)
                            continue
                        if self.check_position_open():
                            triggered = True
                            break
                        if (current_price >= reserved_price + self.entry_offset) or (current_price <= reserved_price - self.entry_offset):
                            entry_signal = self.dynamic_entry_signal(reserved_price)
                            if entry_signal == "LONG":
                                log_info(f"[{self.coin}] [MAIN] LONG Signal erkannt. Current={current_price:.2f}")
                                self.execute_reserved_long(reserved_price)
                            elif entry_signal == "SHORT":
                                log_info(f"[{self.coin}] [MAIN] SHORT Signal erkannt. Current={current_price:.2f}")
                                self.execute_reserved_short(reserved_price)
                            else:
                                log_info(f"[{self.coin}] [MAIN] Kein ausreichend starkes Entry-Signal. Trade wird übersprungen.")
                            triggered = True
                            break
                        sys.stdout.flush()
                        time.sleep(1)
                    time.sleep(1)
                except Exception as inner_e:
                    log_error(f"[{self.coin}] Error in main loop iteration: {inner_e}")
                    time.sleep(1)
        except KeyboardInterrupt:
            log_info(f"[{self.coin}] Bot stopped by KeyboardInterrupt.")
        except Exception as e:
            log_error(f"[{self.coin}] Unhandled exception in main: {e}")
        finally:
            csv_file.close()
            log_info(f"[{self.coin}] CSV file closed. Exiting.")

if __name__ == "__main__":
    coin_list = [c.strip() for c in args.coin.split(",") if c.strip()]
    bots = []
    threads = []
    for c in coin_list:
        bot = TradingBot(c)
        t = threading.Thread(target=bot.run, daemon=True)
        t.start()
        bots.append(bot)
        threads.append(t)
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        log_info("Shutting down all bots...")
        for bot in bots:
            bot.stop_bot()
        for t in threads:
            t.join()
        log_info("All bots stopped. Exiting.")
