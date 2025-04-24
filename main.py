import csv
import asyncio
import websockets
import threading
import ccxt
import time
import logging
import os
import json
import random
import hmac
import hashlib
import base64
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import numpy as np
import atexit
from threading import Lock
from decimal import Decimal
import argparse
import sys
from PyQt6 import uic
from PyQt6.QtCore import QProcess, QTimer, QTime
from PyQt6.QtWidgets import (QApplication, QMainWindow, QListWidgetItem,
                             QMessageBox, QTableWidget, QTableWidgetItem,
                             QPushButton, QHeaderView, QLineEdit)
from PyQt6.QtGui import QColor

load_dotenv()

print("Aktuelles Verzeichnis:", os.getcwd())
print("Existiert config.json?", os.path.exists("config.json"))

bitget = ccxt.bitget({
    'apiKey': os.getenv('API_KEY'),
    'secret': os.getenv('API_SECRET'),
    'password': os.getenv('PASSWORD'),
})

logging.basicConfig(
    filename='bot.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def log_info(msg):
    logging.info(msg)
    print(msg)

def log_error(msg):
    logging.error(msg)
    print(f"ERROR: {msg}")

def save_wallet_snapshot(timestamp, balance):
    snapshot = {"timestamp": timestamp, "balance": balance}
    try:
        if os.path.exists("wallet_snapshots.json"):
            with open("wallet_snapshots.json", "r") as f:
                data = json.load(f)
        else:
            data = []
        data.append(snapshot)
        with open("wallet_snapshots.json", "w") as f:
            json.dump(data, f, indent=4)
        log_info(f"Wallet-Snapshot gespeichert: {snapshot}")
    except Exception as e:
        log_error("Fehler beim Speichern des Wallet-Snapshots: " + str(e))

def get_historical_balance(trade_time):
    try:
        if os.path.exists("wallet_snapshots.json"):
            with open("wallet_snapshots.json", "r") as f:
                snapshots = json.load(f)
            snapshots = sorted(snapshots, key=lambda s: s["timestamp"])
            for snapshot in reversed(snapshots):
                if snapshot["timestamp"] <= trade_time:
                    return snapshot["balance"]
        return "N/A"
    except Exception as e:
        log_error("Fehler beim Abrufen des historischen Saldos: " + str(e))
        return "N/A"

WATCHDOG_CHECK_INTERVAL = 3
MAX_RESTARTS_PER_HOUR = 10
FORCE_RESTART_INTERVAL = 0.5

# Laden der Konfiguration, hier wird auch der neue Parameter EMERGENCY_STOP_PERCENT aus der config.json gelesen
def load_config(app_instance):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(script_dir, "config.json")
    log_info(f"Suche nach config.json unter: {config_file}")
    log_info(f"Existiert config.json? {os.path.exists(config_file)}")

    try:
        if os.path.exists(config_file):
            with open(config_file, "r", encoding="utf-8") as f:
                file_content = f.read().strip()
                log_info(f"Rohinhalt von config.json: '{file_content}'")
                if not file_content:
                    log_error("config.json ist leer. Verwende Standardwerte.")
                    app_instance.outputArea.append("config.json ist leer.")
                    config = {}
                else:
                    config = json.loads(file_content)
                    log_info(f"config.json erfolgreich geladen: {json.dumps(config, indent=2)}")
        else:
            config = {}
            log_error(f"config.json nicht gefunden im Pfad: {config_file}. Standardwerte werden verwendet.")
            app_instance.outputArea.append(f"config.json nicht gefunden im Pfad: {config_file}.")
            return

        coins_input = app_instance.coinLineEdit.text().strip().upper()
        app_instance.coins = [coin.strip() for coin in coins_input.split(",")] if coins_input else ["BTC"]
        app_instance.coinLineEdit.setText(",".join(app_instance.coins))
        app_instance.symbols = {coin: f"{coin}/USDT:USDT" for coin in app_instance.coins}

        app_instance.params = {}
        for coin in app_instance.coins:
            coin_key = f"COIN {coin}"
            coin_config = config.get(coin_key, {})
            if coin_config:
                log_info(f"Konfiguration für '{coin_key}' gefunden: {json.dumps(coin_config, indent=2)}")
                app_instance.params[coin] = {
                    "TRADE_AMOUNT_USD": coin_config.get("TRADE_AMOUNT_USD", 100.0),
                    "LEVERAGE": coin_config.get("LEVERAGE", 2.0),
                    "ENTRY_TRIGGER_OFFSET_USD": coin_config.get("ENTRY_TRIGGER_OFFSET_USD", 15.0),
                    "MINIMUM_REQUIRED_PROFIT_USD": coin_config.get("MINIMUM_REQUIRED_PROFIT_USD", 5.0),
                    "TRAILING_DROP_PERCENT": coin_config.get("TRAILING_DROP_PERCENT", 10.0),
                    "TRAILING_STOP_PERCENT_BELOW_60": coin_config.get("TRAILING_STOP_PERCENT_BELOW_60", 0.90),
                    "TRAILING_STOP_PERCENT_BETWEEN_60_AND_100": coin_config.get("TRAILING_STOP_PERCENT_BETWEEN_60_AND_100", 0.85),
                    "TRAILING_STOP_PERCENT_ABOVE_100": coin_config.get("TRAILING_STOP_PERCENT_ABOVE_100", 0.80),
                    "MEGA_STOP_TRIGGER_USD": coin_config.get("MEGA_STOP_TRIGGER_USD", 6.0),
                    "FIXED_STOPLOSS_OFFSET": coin_config.get("FIXED_STOPLOSS_OFFSET", 5.0),
                    "EMERGENCY_STOP_PERCENT": coin_config.get("EMERGENCY_STOP_PERCENT", 20.0)
                }
                app_instance.outputArea.append(f"Konfiguration für {coin} aus config.json geladen.")
            else:
                log_info(f"Keine spezifische Konfiguration für '{coin_key}' gefunden. Standardwerte werden verwendet.")
                app_instance.params[coin] = {
                    "TRADE_AMOUNT_USD": 100.0,
                    "LEVERAGE": 2.0,
                    "ENTRY_TRIGGER_OFFSET_USD": 15.0,
                    "MINIMUM_REQUIRED_PROFIT_USD": 5.0,
                    "TRAILING_DROP_PERCENT": 10.0,
                    "TRAILING_STOP_PERCENT_BELOW_60": 0.90,
                    "TRAILING_STOP_PERCENT_BETWEEN_60_AND_100": 0.85,
                    "TRAILING_STOP_PERCENT_ABOVE_100": 0.80,
                    "MEGA_STOP_TRIGGER_USD": 6.0,
                    "FIXED_STOPLOSS_OFFSET": 5.0,
                    "EMERGENCY_STOP_PERCENT": 20.0
                }
                app_instance.outputArea.append(f"Keine Konfiguration für {coin} in config.json gefunden. Standardwerte geladen.")

        first_coin = app_instance.coins[0]
        app_instance.tradeAmountLineEdit.setText(str(app_instance.params[first_coin]["TRADE_AMOUNT_USD"]))
        app_instance.leverageLineEdit.setText(str(app_instance.params[first_coin]["LEVERAGE"]))
        app_instance.entryOffsetLineEdit.setText(str(app_instance.params[first_coin]["ENTRY_TRIGGER_OFFSET_USD"]))
        app_instance.minProfitLineEdit.setText(str(app_instance.params[first_coin]["MINIMUM_REQUIRED_PROFIT_USD"]))
        app_instance.trailingDropLineEdit.setText(str(app_instance.params[first_coin]["TRAILING_DROP_PERCENT"]))
        app_instance.trailingStopBelowLineEdit.setText(str(app_instance.params[first_coin]["TRAILING_STOP_PERCENT_BELOW_60"]))
        app_instance.trailingStopBetweenLineEdit.setText(str(app_instance.params[first_coin]["TRAILING_STOP_PERCENT_BETWEEN_60_AND_100"]))
        app_instance.trailingStopAboveLineEdit.setText(str(app_instance.params[first_coin]["TRAILING_STOP_PERCENT_ABOVE_100"]))
        app_instance.megaStopLineEdit.setText(str(app_instance.params[first_coin]["MEGA_STOP_TRIGGER_USD"]))
        if hasattr(app_instance, "fixedStopLossLineEdit"):
            fixed_stoploss = app_instance.params[first_coin].get("FIXED_STOPLOSS_OFFSET", 5.0)
            app_instance.fixedStopLossLineEdit.setText(str(fixed_stoploss))
        # Falls vorhanden, setze auch das UI-Feld für Emergency Stop
        if hasattr(app_instance, "emergencyStopLineEdit"):
            app_instance.emergencyStopLineEdit.setText(str(app_instance.params[first_coin]["EMERGENCY_STOP_PERCENT"]))
    except json.JSONDecodeError as e:
        log_error(f"Fehler beim Parsen von config.json: {str(e)}. Standardwerte werden verwendet.")
        app_instance.outputArea.append(f"Fehler beim Parsen von config.json: {str(e)}. Standardwerte geladen.")
        app_instance.coins = ["BTC"]
        app_instance.params["BTC"] = {
            "TRADE_AMOUNT_USD": 100.0,
            "LEVERAGE": 2.0,
            "ENTRY_TRIGGER_OFFSET_USD": 15.0,
            "MINIMUM_REQUIRED_PROFIT_USD": 5.0,
            "TRAILING_DROP_PERCENT": 10.0,
            "TRAILING_STOP_PERCENT_BELOW_60": 0.90,
            "TRAILING_STOP_PERCENT_BETWEEN_60_AND_100": 0.85,
            "TRAILING_STOP_PERCENT_ABOVE_100": 0.80,
            "MEGA_STOP_TRIGGER_USD": 6.0,
            "FIXED_STOPLOSS_OFFSET": 5.0,
            "EMERGENCY_STOP_PERCENT": 20.0
        }
        app_instance.tradeAmountLineEdit.setText("100.0")
        app_instance.leverageLineEdit.setText("2.0")
        app_instance.entryOffsetLineEdit.setText("15.0")
        app_instance.minProfitLineEdit.setText("5.0")
        app_instance.trailingDropLineEdit.setText("10.0")
        app_instance.trailingStopBelowLineEdit.setText("0.90")
        app_instance.trailingStopBetweenLineEdit.setText("0.85")
        app_instance.trailingStopAboveLineEdit.setText("0.80")
        app_instance.megaStopLineEdit.setText("6.0")

# Speichern der Konfiguration inklusive des neuen Parameters
def save_config(app_instance):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(script_dir, "config.json")
    try:
        coins_input = app_instance.coinLineEdit.text().strip().upper()
        coins = [coin.strip() for coin in coins_input.split(",")] if coins_input else ["BTC"]

        first_coin = coins[0]
        # Falls das UI-Feld für Emergency Stop existiert, auslesen, ansonsten Standardwert
        if hasattr(app_instance, "emergencyStopLineEdit"):
            emergency_stop = float(app_instance.emergencyStopLineEdit.text().strip())
        else:
            emergency_stop = 20.0

        app_instance.params[first_coin] = {
            "TRADE_AMOUNT_USD": float(app_instance.tradeAmountLineEdit.text()),
            "LEVERAGE": float(app_instance.leverageLineEdit.text()),
            "ENTRY_TRIGGER_OFFSET_USD": float(app_instance.entryOffsetLineEdit.text()),
            "MINIMUM_REQUIRED_PROFIT_USD": float(app_instance.minProfitLineEdit.text()),
            "TRAILING_DROP_PERCENT": float(app_instance.trailingDropLineEdit.text()),
            "TRAILING_STOP_PERCENT_BELOW_60": float(app_instance.trailingStopBelowLineEdit.text()),
            "TRAILING_STOP_PERCENT_BETWEEN_60_AND_100": float(app_instance.trailingStopBetweenLineEdit.text()),
            "TRAILING_STOP_PERCENT_ABOVE_100": float(app_instance.trailingStopAboveLineEdit.text()),
            "MEGA_STOP_TRIGGER_USD": float(app_instance.megaStopLineEdit.text()),
            "FIXED_STOPLOSS_OFFSET": float(app_instance.fixedStopLossLineEdit.text()),
            "EMERGENCY_STOP_PERCENT": emergency_stop
        }

        new_config = {}
        for coin in coins:
            new_config[f"COIN {coin}"] = app_instance.params.get(coin, {
                "TRADE_AMOUNT_USD": 100.0,
                "LEVERAGE": 2.0,
                "ENTRY_TRIGGER_OFFSET_USD": 15.0,
                "MINIMUM_REQUIRED_PROFIT_USD": 5.0,
                "TRAILING_DROP_PERCENT": 10.0,
                "TRAILING_STOP_PERCENT_BELOW_60": 0.90,
                "TRAILING_STOP_PERCENT_BETWEEN_60_AND_100": 0.85,
                "TRAILING_STOP_PERCENT_ABOVE_100": 0.80,
                "MEGA_STOP_TRIGGER_USD": 6.0,
                "FIXED_STOPLOSS_OFFSET": 5.0,
                "EMERGENCY_STOP_PERCENT": 20.0
            })

        with open(config_file, "w", encoding="utf-8") as f:
            json.dump(new_config, f, indent=4)
        app_instance.outputArea.append(f"Konfiguration für {', '.join(coins)} in {config_file} gespeichert.")
        log_info(f"Konfiguration für {', '.join(coins)} gespeichert.")
    except ValueError as e:
        log_error(f"Fehler beim Konvertieren der Eingabewerte: {str(e)}")
        app_instance.outputArea.append(f"Fehler beim Speichern: Ungültige Eingabewerte ({str(e)}).")
    except Exception as e:
        log_error(f"Fehler beim Speichern der Konfiguration: {str(e)}")
        app_instance.outputArea.append(f"Fehler beim Speichern der Konfiguration: {str(e)}")

def my_excepthook(exc_type, exc_value, exc_tb):
    import traceback
    error_msg = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
    print("Unhandled Exception:", error_msg)
    log_error("Unhandled Exception: " + error_msg)
    sys.__excepthook__(exc_type, exc_value, exc_tb)

sys.excepthook = my_excepthook

def format_datetime(dt_str):
    try:
        dt_obj = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt_obj.strftime("%d.%m.%Y, %H:%M:%S")
    except Exception as e:
        log_error("Fehler beim Formatieren der Zeit: " + str(e))
        return dt_str

def get_wallet_balance(bitget, currency="USDT"):
    balance = {}
    try:
        balance = bitget.fetch_balance({"type": "spot"})
        log_info("Spot-Balance erfolgreich abgerufen.")
    except Exception as e:
        log_error("Fehler beim Abrufen der Spot-Balance: " + str(e))
    if not balance or currency not in balance.get("free", {}):
        try:
            balance = bitget.fetch_balance({"type": "swap"})
            log_info("Swap-Balance erfolgreich abgerufen.")
        except Exception as e:
            log_error("Fehler beim Abrufen der Swap-Balance: " + str(e))
            return 0.0
    if "free" in balance and currency in balance["free"]:
        return balance["free"][currency]
    elif "total" in balance and currency in balance["total"]:
        return balance["total"][currency]
    elif "info" in balance:
        info = balance["info"]
        if isinstance(info, dict) and "freeBalance" in info:
            try:
                return float(info["freeBalance"])
            except Exception as e:
                log_error("Fehler beim Konvertieren von freeBalance: " + str(e))
    log_info("Keine passenden Balance-Daten für " + currency + " gefunden.")
    return 0.0

def print_trade_data(trade):
    log_info("----- Trade Data -----")
    log_info(f"ID: {trade.get('id')}")
    log_info(f"Order: {trade.get('order')}")
    log_info(f"Symbol: {trade.get('symbol')}")
    log_info(f"Side: {trade.get('side')}")
    log_info(f"Type: {trade.get('type')}")
    log_info(f"TakerOrMaker: {trade.get('takerOrMaker')}")
    log_info(f"Price: {trade.get('price')}")
    log_info(f"Amount: {trade.get('amount')}")
    log_info(f"Cost: {trade.get('cost')}")
    log_info(f"Timestamp: {trade.get('timestamp')}")
    log_info(f"Datetime: {trade.get('datetime')}")
    fee = trade.get("fee", {})
    log_info("Fee:")
    if isinstance(fee, dict):
        log_info(f"  Currency: {fee.get('currency')}")
        log_info(f"  Cost: {fee.get('cost')}")
    elif isinstance(fee, list):
        for f in fee:
            log_info(f"Additional Fee - Currency: {f.get('currency')}, Cost: {f.get('cost')}")
    else:
        log_info("Keine Fee-Daten vorhanden.")
    for key, value in trade.items():
        if key not in ["id", "order", "symbol", "side", "type", "takerOrMaker", "price", "amount", "cost", "timestamp",
                       "datetime", "fee", "fees"]:
            log_info(f"{key}: {value}")
    log_info("----------------------")

class BotApp(QMainWindow):
    def __init__(self):
        super().__init__()
        uic.loadUi("mainwindow.ui", self)

        self.coinLineEdit = self.findChild(QLineEdit, "coinLineEdit")
        if self.coinLineEdit is None:
            self.coinLineEdit = QLineEdit(self)
            self.coinLineEdit.setObjectName("coinLineEdit")
            self.coinLineEdit.move(10, 10)
            self.coinLineEdit.resize(100, 30)
            self.outputArea.append("Warnung: coinLineEdit nicht in der UI gefunden, Fallback wurde erstellt.")
            log_error("coinLineEdit nicht in der UI gefunden.")

        self.fixedStopLossLineEdit = self.findChild(QLineEdit, "fixedStopLossLineEdit")
        if self.fixedStopLossLineEdit is None:
            self.fixedStopLossLineEdit = QLineEdit(self)
            self.fixedStopLossLineEdit.setObjectName("fixedStopLossLineEdit")
            self.fixedStopLossLineEdit.move(10, 50)
            self.fixedStopLossLineEdit.resize(100, 30)
            self.outputArea.append("Warnung: fixedStopLossLineEdit nicht in der UI gefunden, Fallback wurde erstellt.")
            log_error("fixedStopLossLineEdit nicht in der UI gefunden.")

        self.megaStopLineEdit = self.findChild(QLineEdit, "megaStopLineEdit")
        if self.megaStopLineEdit is None:
            self.megaStopLineEdit = QLineEdit(self)
            self.megaStopLineEdit.setObjectName("megaStopLineEdit")
            self.megaStopLineEdit.move(10, 90)
            self.megaStopLineEdit.resize(100, 30)
            self.outputArea.append("Warnung: megaStopLineEdit nicht in der UI gefunden, Fallback wurde erstellt.")
            log_error("megaStopLineEdit nicht in der UI gefunden.")

        # Neues Widget für Emergency Stop - falls im UI vorhanden
        self.emergencyStopLineEdit = self.findChild(QLineEdit, "emergencyStopLineEdit")
        if self.emergencyStopLineEdit is None:
            # Fallback: Erstelle ein QLineEdit an einer geeigneten Stelle
            self.emergencyStopLineEdit = QLineEdit(self)
            self.emergencyStopLineEdit.setObjectName("emergencyStopLineEdit")
            self.emergencyStopLineEdit.move(10, 130)
            self.emergencyStopLineEdit.resize(100, 30)
            self.outputArea.append("Warnung: emergencyStopLineEdit nicht in der UI gefunden, Fallback wurde erstellt.")
            log_error("emergencyStopLineEdit nicht in der UI gefunden.")

        self.coinUpdateTimer = QTimer(self)
        self.coinUpdateTimer.setSingleShot(True)
        self.coinUpdateTimer.timeout.connect(self.coin_changed)
        self.coinLineEdit.textChanged.connect(self.start_coin_update_timer)

        self.coins = ["BTC"]
        self.params = {
            "BTC": {
                "TRADE_AMOUNT_USD": 100.0,
                "LEVERAGE": 2.0,
                "ENTRY_TRIGGER_OFFSET_USD": 15.0,
                "MINIMUM_REQUIRED_PROFIT_USD": 5.0,
                "TRAILING_DROP_PERCENT": 10.0,
                "TRAILING_STOP_PERCENT_BELOW_60": 0.90,
                "TRAILING_STOP_PERCENT_BETWEEN_60_AND_100": 0.85,
                "TRAILING_STOP_PERCENT_ABOVE_100": 0.80,
                "MEGA_STOP_TRIGGER_USD": 6.0,
                "FIXED_STOPLOSS_OFFSET": 5.0,
                "EMERGENCY_STOP_PERCENT": 20.0
            }
        }
        self.symbols = {"BTC": "BTC/USDT:USDT"}
        self.coinLineEdit.setText("BTC")
        self.positionsPrinted = False
        self.transactionsPrinted = False

        self.tradeAmountLineEdit.setText("100.0")
        self.leverageLineEdit.setText("2.0")
        self.entryOffsetLineEdit.setText("15.0")
        self.minProfitLineEdit.setText("5.0")
        self.trailingDropLineEdit.setText("10.0")
        self.trailingStopBelowLineEdit.setText("0.90")
        self.trailingStopBetweenLineEdit.setText("0.85")
        self.trailingStopAboveLineEdit.setText("0.80")
        self.megaStopLineEdit.setText("6.0")
        self.fixedStopLossLineEdit.setText("5.0")
        # Setze auch einen Standardwert für den Emergency Stop
        self.emergencyStopLineEdit.setText("20.0")
        self.outputArea.append("Initiale Standardwerte für BTC geladen.")
        log_info("Initiale Standardwerte für BTC geladen.")

        self.startBotButton.clicked.connect(self.start_bot)
        self.stopBotButton.clicked.connect(self.stop_bot)
        try:
            self.enterParameterButton.clicked.connect(self.enter_parameters)
        except AttributeError:
            self.outputArea.append("Enter Parameter Button nicht gefunden. Bitte überprüfen Sie Ihre UI.")
            log_error("Enter Parameter Button nicht gefunden.")

        try:
            self.updateConfigButton = self.findChild(QPushButton, "updateConfigButton")
            if self.updateConfigButton:
                self.updateConfigButton.clicked.connect(self.update_config)
            else:
                self.outputArea.append("updateConfigButton nicht gefunden. Bitte UI überprüfen.")
                log_error("updateConfigButton nicht in der UI gefunden.")
        except AttributeError:
            self.outputArea.append("updateConfigButton nicht gefunden. Bitte UI überprüfen.")
            log_error("updateConfigButton nicht in der UI gefunden.")

        self.process = QProcess(self)
        self.process.setProcessChannelMode(QProcess.ProcessChannelMode.MergedChannels)
        self.process.readyReadStandardOutput.connect(self.handle_output)
        self.process.readyReadStandardError.connect(self.handle_output)
        self.process.errorOccurred.connect(self.handle_error)
        self.process.finished.connect(self.handle_finished)

        self.intentionally_stopped = False

        self.auto_restart_enabled = False
        self.restart_count = 0
        self.last_restart_time = time.time()

        self.watchdog_timer = QTimer(self)
        self.watchdog_timer.timeout.connect(self.check_watchdog)
        self.watchdog_timer.start(WATCHDOG_CHECK_INTERVAL * 1000)

        self.schedule = []
        try:
            self.btnAddSchedule.clicked.connect(self.add_schedule)
            self.btnRemoveSchedule.clicked.connect(self.remove_schedule)
            self.schedule_timer = QTimer(self)
            self.schedule_timer.timeout.connect(self.check_schedule)
            self.schedule_timer.start(1000)
            self.outputArea.append("Schedule-Funktionalität aktiviert und Timer gestartet.")
            log_info("Schedule-Funktionalität aktiviert und Timer gestartet.")
        except AttributeError:
            self.outputArea.append("Scheduling-Widgets nicht gefunden. Überspringe Schedule-Funktionalität.")
            log_error("Scheduling-Widgets nicht gefunden. Überspringe Schedule-Funktionalität.")

        try:
            api_key = os.getenv("API_KEY")
            api_secret = os.getenv("API_SECRET")
            password = os.getenv("PASSWORD")
            log_info(f"API_KEY: {api_key}, API_SECRET: {api_secret}, PASSWORD: {password}")
            self.bitget = ccxt.bitget({
                'apiKey': api_key,
                'secret': api_secret,
                'password': password
            })
        except Exception as e:
            error_message = "Fehler bei API-Initialisierung: " + str(e)
            self.outputArea.append(error_message)
            log_error(error_message)

        self.initialize_schedule_display()
        self.tableWidgetPositions = self.findChild(QTableWidget, "tableWidgetPositions")
        if not self.tableWidgetPositions:
            log_error("tableWidgetPositions nicht gefunden!")
        self.load_trading_data()
        self.tradingDataTimer = QTimer(self)
        self.tradingDataTimer.timeout.connect(self.load_trading_data)
        self.tradingDataTimer.start(3000)

        load_config(self)

    def on_stop_button_clicked(self):
        for bot in bots:
            bot.stop_bot()
        self.outputArea.append("Alle Bots wurden gestoppt.")

    def start_coin_update_timer(self):
        log_info("start_coin_update_timer wurde aufgerufen.")
        self.coinUpdateTimer.start(500)

    def coin_changed(self):
        load_config(self)
        self.display_parameters()

    def update_config(self):
        load_config(self)
        self.outputArea.append(f"Konfiguration für {', '.join(self.coins)} wurde aktualisiert.")
        log_info(f"Konfiguration für {', '.join(self.coins)} wurde aktualisiert.")

    def initialize_schedule_display(self):
        while self.listSchedule.count() < 3:
            self.listSchedule.addItem("")
        self.listSchedule.item(0).setText("")
        self.listSchedule.item(1).setText("")
        self.listSchedule.item(2).setText("")
        QApplication.processEvents()

    def update_schedule_display_mode(self, mode, time_str=None):
        if time_str is None:
            time_str = QTime.currentTime().toString("hh:mm:ss")
        if mode == "start":
            self.listSchedule.item(0).setText(f"START - {time_str}")
            self.listSchedule.item(2).setText("START Bot bitte warten")
        elif mode == "stop":
            self.listSchedule.item(1).setText(f"STOP  - {time_str}")
            self.listSchedule.item(2).setText("STOP Bot bitte warten")
        QApplication.processEvents()

    def enter_parameters(self):
        self.update_parameters()
        self.display_parameters()

    # Hier wird der neue Parameter (emergency_stop_percent) über das QLineEdit nicht explizit in update_parameters
    # ausgelesen, falls du diesen Wert separat kontrollieren möchtest, kannst du ihn in dieser Funktion einfügen.
    def update_parameters(self):
        try:
            coins_input = self.coinLineEdit.text().strip().upper()
            self.coins = [coin.strip() for coin in coins_input.split(",")] if coins_input else ["BTC"]
            self.symbols = {coin: f"{coin}/USDT:USDT" for coin in self.coins}
            first_coin = self.coins[0]
            self.params[first_coin] = {
                "TRADE_AMOUNT_USD": float(self.tradeAmountLineEdit.text().strip()),
                "LEVERAGE": float(self.leverageLineEdit.text().strip()),
                "ENTRY_TRIGGER_OFFSET_USD": float(self.entryOffsetLineEdit.text().strip()),
                "MINIMUM_REQUIRED_PROFIT_USD": float(self.minProfitLineEdit.text().strip()),
                "TRAILING_DROP_PERCENT": float(self.trailingDropLineEdit.text().strip()),
                "TRAILING_STOP_PERCENT_BELOW_60": float(self.trailingStopBelowLineEdit.text().strip()),
                "TRAILING_STOP_PERCENT_BETWEEN_60_AND_100": float(self.trailingStopBetweenLineEdit.text().strip()),
                "TRAILING_STOP_PERCENT_ABOVE_100": float(self.trailingStopAboveLineEdit.text().strip()),
                "MEGA_STOP_TRIGGER_USD": float(self.megaStopLineEdit.text().strip()),
                "FIXED_STOPLOSS_OFFSET": float(self.fixedStopLossLineEdit.text().strip())
            }
            # Neuer Parameter: falls emergencyStopLineEdit vorhanden, lese auch diesen Wert ein
            if hasattr(self, "emergencyStopLineEdit"):
                self.params[first_coin]["EMERGENCY_STOP_PERCENT"] = float(self.emergencyStopLineEdit.text().strip())
            else:
                self.params[first_coin]["EMERGENCY_STOP_PERCENT"] = 20.0
        except Exception as e:
            error_message = f"Fehler beim Aktualisieren der Parameter: {str(e)}"
            self.outputArea.append(error_message)
            log_error(error_message)

    # Anzeige der aktuellen Parameter inklusive Emergency Stop
    def display_parameters(self):
        msg = "Aktuelle Parameter:\n"
        for coin in self.coins:
            params = self.params.get(coin, {
                "TRADE_AMOUNT_USD": 100.0,
                "LEVERAGE": 2.0,
                "ENTRY_TRIGGER_OFFSET_USD": 15.0,
                "MINIMUM_REQUIRED_PROFIT_USD": 5.0,
                "TRAILING_DROP_PERCENT": 10.0,
                "TRAILING_STOP_PERCENT_BELOW_60": 0.90,
                "TRAILING_STOP_PERCENT_BETWEEN_60_AND_100": 0.85,
                "TRAILING_STOP_PERCENT_ABOVE_100": 0.80,
                "MEGA_STOP_TRIGGER_USD": 6.0,
                "FIXED_STOPLOSS_OFFSET": 5.0,
                "EMERGENCY_STOP_PERCENT": 20.0
            })
            msg += (
                f"{coin}:\n"
                f"  TRADE_AMOUNT_USD = {params['TRADE_AMOUNT_USD']}\n"
                f"  LEVERAGE = {params['LEVERAGE']}\n"
                f"  ENTRY_TRIGGER_OFFSET_USD = {params['ENTRY_TRIGGER_OFFSET_USD']}\n"
                f"  MINIMUM_REQUIRED_PROFIT_USD = {params['MINIMUM_REQUIRED_PROFIT_USD']}\n"
                f"  TRAILING_DROP_PERCENT = {params['TRAILING_DROP_PERCENT']}\n"
                f"  TRAILING_STOP_PERCENT_BELOW_60 = {params['TRAILING_STOP_PERCENT_BELOW_60']}\n"
                f"  TRAILING_STOP_PERCENT_BETWEEN_60_AND_100 = {params['TRAILING_STOP_PERCENT_BETWEEN_60_AND_100']}\n"
                f"  TRAILING_STOP_PERCENT_ABOVE_100 = {params['TRAILING_STOP_PERCENT_ABOVE_100']}\n"
                f"  MEGA_STOP_TRIGGER_USD = {params['MEGA_STOP_TRIGGER_USD']}\n"
                f"  FIXED_STOPLOSS_OFFSET = {params['FIXED_STOPLOSS_OFFSET']}\n"
                f"  EMERGENCY_STOP_PERCENT = {params['EMERGENCY_STOP_PERCENT']}\n"
            )
        self.outputArea.append(msg)
        log_info(msg)

    # Beim Starten des Bot-Prozesses werden nun auch alle Parameter als CLI-Argumente übergeben,
    # hier wird der neue Parameter --emergency_stop_percent hinzugefügt.
    def start_bot(self, from_watchdog=False):
        if not from_watchdog:
            self.outputArea.clear()
            self.display_parameters()
            self.auto_restart_enabled = True
            self.intentionally_stopped = False
            current_time_str = QTime.currentTime().toString("hh:mm:ss")
            self.update_schedule_display_mode("start", current_time_str)

        for coin in self.coins:
            self.clear_all_orders_and_positions(coin)
            self.place_market_exit_order(coin)

        try:
            args = [
                "-u", "mainV25.py",
                "--coin", ",".join(self.coins),
                "--trade_amount", ",".join(str(self.params[coin]["TRADE_AMOUNT_USD"]) for coin in self.coins),
                "--leverage", ",".join(str(self.params[coin]["LEVERAGE"]) for coin in self.coins),
                "--entry_offset", ",".join(str(self.params[coin]["ENTRY_TRIGGER_OFFSET_USD"]) for coin in self.coins),
                "--min_profit", ",".join(str(self.params[coin]["MINIMUM_REQUIRED_PROFIT_USD"]) for coin in self.coins),
                "--trailing_drop", ",".join(str(self.params[coin]["TRAILING_DROP_PERCENT"]) for coin in self.coins),
                "--trailing_stop_below", ",".join(str(self.params[coin]["TRAILING_STOP_PERCENT_BELOW_60"]) for coin in self.coins),
                "--trailing_stop_60_100", ",".join(str(self.params[coin]["TRAILING_STOP_PERCENT_BETWEEN_60_AND_100"]) for coin in self.coins),
                "--trailing_stop_above", ",".join(str(self.params[coin]["TRAILING_STOP_PERCENT_ABOVE_100"]) for coin in self.coins),
                "--mega_stop", ",".join(str(self.params[coin]["MEGA_STOP_TRIGGER_USD"]) for coin in self.coins),
                "--fixed_stoploss_offset", ",".join(str(self.params[coin]["FIXED_STOPLOSS_OFFSET"]) for coin in self.coins),
                "--emergency_stop_percent", ",".join(str(self.params[coin]["EMERGENCY_STOP_PERCENT"]) for coin in self.coins)
            ]

            self.outputArea.append(f"Starte Bot für {', '.join(self.coins)} mit den oben genannten Parametern.")
            log_info(f"Starte Bot für {', '.join(self.coins)} mit den Parametern: {args}")
            self.process.start(sys.executable, args)
        except Exception as e:
            error_message = f"Fehler beim Starten des Prozesses: {str(e)}"
            self.outputArea.append(error_message)
            log_error(error_message)

    def place_market_exit_order(self, coin=None):
        symbol = self.symbols[coin] if coin else self.symbols[self.coins[0]]
        try:
            positions = self.bitget.fetch_positions([symbol])
            open_positions = [pos for pos in positions if pos.get('contracts') and float(pos['contracts']) > 0]
            if open_positions:
                for pos in open_positions:
                    qty = float(pos['contracts'])
                    side = 'sell' if pos.get('side', '').lower() == 'long' else 'buy'
                    try:
                        self.bitget.create_order(symbol, 'market', side, qty)
                        msg = f"Synchronisierte Markt-Exit-Order platziert: {side.upper()} {qty} {symbol}"
                        self.outputArea.append(msg)
                        log_info(msg)
                    except Exception as e:
                        error_message = f"Fehler beim Platzieren der synchronisierten Markt-Exit-Order: {e}"
                        self.outputArea.append(error_message)
                        log_error(error_message)
            else:
                msg = f"Keine offenen Positionen für synchronisierte Market-Exit gefunden für {symbol}."
                self.outputArea.append(msg)
                log_info(msg)
        except Exception as e:
            error_message = f"Fehler beim Abrufen offener Positionen für synchronisierte Market-Exit: {str(e)}"
            self.outputArea.append(error_message)
            log_error(error_message)

    def clear_all_orders_and_positions(self, coin=None):
        symbol = self.symbols[coin] if coin else self.symbols[self.coins[0]]
        try:
            open_orders = self.bitget.fetch_open_orders(symbol)
            if open_orders:
                for order in open_orders:
                    try:
                        self.bitget.cancel_order(order['id'], symbol)
                        message = f"Order {order['id']} storniert für {symbol}."
                        self.outputArea.append(message)
                        log_info(message)
                    except Exception as e:
                        error_message = f"Fehler beim Stornieren der Order {order['id']}: {e}"
                        self.outputArea.append(error_message)
                        log_error(error_message)
            else:
                msg = f"Keine offenen Orders gefunden für {symbol}."
                self.outputArea.append(msg)
                log_info(msg)
        except Exception as e:
            error_message = f"Fehler beim Abrufen offener Orders für {symbol}: {str(e)}"
            self.outputArea.append(error_message)
            log_error(error_message)

        max_attempts = 10
        attempt = 0
        while attempt < max_attempts:
            try:
                positions = self.bitget.fetch_positions([symbol])
                open_positions = [pos for pos in positions if pos.get('contracts') and float(pos['contracts']) > 0]
                if not open_positions:
                    msg = f"Alle offenen Positionen wurden geschlossen für {symbol}."
                    self.outputArea.append(msg)
                    log_info(msg)
                    break
                for pos in open_positions:
                    side = 'sell' if pos.get('side', '').lower() == 'long' else 'buy'
                    try:
                        self.bitget.create_order(symbol, 'market', side, pos['contracts'])
                        message = f"Position geschlossen: {side.upper()} {pos['contracts']} {symbol}"
                        self.outputArea.append(message)
                        log_info(message)
                    except Exception as e:
                        error_message = f"Fehler beim Schließen der Position: {e}"
                        self.outputArea.append(error_message)
                        log_error(error_message)
                attempt += 1
                if open_positions:
                    msg_attempt = f"Versuch {attempt}/{max_attempts}: {len(open_positions)} Position(en) noch offen für {symbol}. Warte 3 Sekunden..."
                    self.outputArea.append(msg_attempt)
                    log_info(msg_attempt)
                    time.sleep(3)
            except Exception as e:
                error_message = f"Fehler beim Abrufen offener Positionen für {symbol}: {str(e)}"
                self.outputArea.append(error_message)
                log_error(error_message)
                break
        else:
            msg = f"Nicht alle Positionen konnten innerhalb der max. Versuche geschlossen werden für {symbol}."
            self.outputArea.append(msg)
            log_error(msg)

    def stop_bot(self):
        current_time_str = QTime.currentTime().toString("hh:mm:ss")
        self.update_schedule_display_mode("stop", current_time_str)
        QApplication.processEvents()

        self.intentionally_stopped = True
        for coin in self.coins:
            self.clear_all_orders_and_positions(coin)
            self.place_market_exit_order(coin)

        if self.process.state() == QProcess.ProcessState.Running:
            self.process.terminate()
            if not self.process.waitForFinished(3000):
                self.process.kill()
                self.process.waitForFinished(3000)
            msg = "Bot gestoppt, offene Orders/Positionen wurden geschlossen."
            self.outputArea.append(msg)
            log_info(msg)
        else:
            msg = "Kein Bot läuft."
            self.outputArea.append(msg)
            log_info(msg)
        self.auto_restart_enabled = False

    def add_schedule(self):
        start_time = self.timeEditStart.time()
        stop_time = self.timeEditStop.time()
        entry = {"start": start_time, "stop": stop_time, "triggered": False}
        self.schedule.append(entry)
        self.listSchedule.item(0).setText(f"START - {start_time.toString('hh:mm:ss')}")
        self.listSchedule.item(1).setText(f"STOP  - {stop_time.toString('hh:mm:ss')}")
        self.listSchedule.item(2).setText("START Bot bitte warten")
        QApplication.processEvents()
        msg = ("Neuer Zeitplan hinzugefügt: START - " + start_time.toString("hh:mm:ss") +
               " | STOP - " + stop_time.toString("hh:mm:ss"))
        self.outputArea.append(msg)
        log_info(msg)

    def remove_schedule(self):
        selected_index = self.listSchedule.currentRow()
        if selected_index >= 0:
            self.listSchedule.takeItem(selected_index)
            del self.schedule[selected_index]
            msg = "Zeitplan entfernt."
            self.outputArea.append(msg)
            log_info(msg)
        else:
            msg = "Kein Zeitplaneintrag ausgewählt."
            self.outputArea.append(msg)
            log_info(msg)

    def check_schedule(self):
        current_time = QTime.currentTime()
        current_time_str = current_time.toString("hh:mm:ss")
        tolerance = 5
        for entry in self.schedule[:]:
            start_time = entry["start"]
            stop_time = entry["stop"]
            triggered = entry["triggered"]
            diff_start = start_time.secsTo(current_time)
            diff_stop = stop_time.secsTo(current_time)

            if not triggered and 0 <= diff_start < tolerance:
                self.update_schedule_display_mode("start", current_time_str)
                self.outputArea.append("Zeitplan: Bot wird gestartet.")
                log_info("Zeitplan: Bot wird gestartet.")
                self.display_parameters()
                self.start_bot(from_watchdog=False)
                entry["triggered"] = True
            elif triggered and 0 <= diff_stop < tolerance:
                self.update_schedule_display_mode("stop", current_time_str)
                if self.process.state() == QProcess.ProcessState.Running:
                    self.outputArea.append("Stop-Zeit wurde ausgeführt im laufenden Trade.")
                else:
                    self.outputArea.append("Stop-Zeit wurde ausgeführt ohne Trade.")
                log_info("Stop-Zeit wurde ausgeführt.")
                self.stop_bot()
                self.schedule.remove(entry)

    def check_watchdog(self):
        if not self.auto_restart_enabled:
            return
        if self.process.state() != QProcess.ProcessState.Running:
            msg = "Watchdog: Bot nicht aktiv. Starte neu..."
            self.outputArea.append(msg)
            log_info(msg)
            self.restart_bot()
            self.restart_count += 1
            self.last_restart_time = time.time()
            return
        if time.time() - self.last_restart_time > FORCE_RESTART_INTERVAL * 3600:
            msg = "Watchdog: Geplanter Neustart des Bots..."
            self.outputArea.append(msg)
            log_info(msg)
            if self.process.state() == QProcess.ProcessState.Running:
                self.process.kill()
            QTimer.singleShot(3000, lambda: self.start_bot(from_watchdog=True))
            self.restart_count += 1
            self.last_restart_time = time.time()
        if self.restart_count >= MAX_RESTARTS_PER_HOUR:
            msg = "Watchdog: Zu viele Neustarts! Überwachung pausiert für 120 Sekunden."
            self.outputArea.append(msg)
            log_info(msg)
            self.watchdog_timer.stop()
            QTimer.singleShot(120 * 1000, self.resume_watchdog)

    def restart_bot(self):
        msg = "Watchdog: Bot wird neu gestartet..."
        self.outputArea.append(msg)
        log_info(msg)
        if self.process.state() == QProcess.ProcessState.Running:
            self.process.kill()
        QTimer.singleShot(3000, lambda: self.start_bot(from_watchdog=True))

    def resume_watchdog(self):
        self.restart_count = 0
        msg = "Watchdog: Überwachung fortgesetzt."
        self.outputArea.append(msg)
        log_info(msg)
        self.watchdog_timer.start(WATCHDOG_CHECK_INTERVAL * 1000)

    def handle_output(self):
        try:
            while self.process.canReadLine():
                line = self.process.readLine().data().decode("utf-8", errors="replace").rstrip("\r\n")
                if line:
                    self.outputArea.append(line)
                    QApplication.processEvents()
                self.outputArea.verticalScrollBar().setValue(self.outputArea.verticalScrollBar().maximum())
        except Exception as e:
            error_message = "Fehler beim Lesen der Ausgabe: " + str(e)
            self.outputArea.append(error_message)
            log_error(error_message)

    def handle_error(self, error):
        if self.intentionally_stopped:
            msg = "Prozess beendet (beabsichtigt)."
            self.outputArea.append(msg)
            log_info(msg)
        else:
            msg = f"Prozessfehler: {error}"
            self.outputArea.append(msg)
            log_error(msg)

    def handle_finished(self):
        msg = "Prozess beendet."
        self.outputArea.append(msg)
        log_info(msg)

    def load_trading_data(self):
        self.load_positions_data()

    def load_positions_data(self):
        try:
            all_positions = []
            for coin, symbol in self.symbols.items():
                positions = self.bitget.fetch_positions([symbol])
                if positions:
                    for pos in positions:
                        pos["coin"] = coin
                    all_positions.extend(positions)

            if not all_positions:
                self.tableWidgetPositions.setRowCount(0)
                if not self.positionsPrinted:
                    self.outputArea.append("Keine Positionen gefunden.")
                    log_info("Keine Positionen gefunden.")
                    self.positionsPrinted = True
                return

            keys = ["coin", "symbol", "contracts", "entryPrice", "markPrice", "liquidationPrice", "marginMode", "leverage"]
            self.tableWidgetPositions.setRowCount(len(all_positions))
            self.tableWidgetPositions.setColumnCount(len(keys))
            self.tableWidgetPositions.setHorizontalHeaderLabels(keys)
            for row, pos in enumerate(all_positions):
                for col, key in enumerate(keys):
                    value = pos.get(key, "")
                    self.tableWidgetPositions.setItem(row, col, QTableWidgetItem(str(value)))
            if not self.positionsPrinted:
                self.outputArea.append("Positionen aktualisiert.")
                log_info("Positionen aktualisiert.")
                self.positionsPrinted = True
        except Exception as e:
            error_message = f"Fehler beim Laden der Positionen: {str(e)}"
            self.outputArea.append(error_message)
            log_error(error_message)

    def load_transaction_history_data(self):
        try:
            all_trades = []
            for coin, symbol in self.symbols.items():
                trades = self.bitget.fetch_my_trades(symbol) or []
                for trade in trades:
                    trade["coin"] = coin
                all_trades.extend(trades)

            all_trades.reverse()
            headers = ["Datum/Uhrzeit", "Coin", "Symbol", "Typ", "Betrag", "Gebühr", "Historischer Wallet-Saldo"]
            self.tableWidgetTransactions.setRowCount(len(all_trades))
            self.tableWidgetTransactions.setColumnCount(len(headers))
            self.tableWidgetTransactions.setHorizontalHeaderLabels(headers)
            self.tableWidgetTransactions.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)

            for row, trade in enumerate(all_trades):
                print_trade_data(trade)
                dt_val = format_datetime(trade.get("datetime", "N/A"))
                self.tableWidgetTransactions.setItem(row, 0, QTableWidgetItem(dt_val))
                self.tableWidgetTransactions.setItem(row, 1, QTableWidgetItem(trade.get("coin", "N/A")))
                self.tableWidgetTransactions.setItem(row, 2, QTableWidgetItem(trade.get("symbol", "N/A")))
                typ_val = trade.get("side", "N/A").capitalize()
                self.tableWidgetTransactions.setItem(row, 3, QTableWidgetItem(typ_val))
                self.tableWidgetTransactions.setItem(row, 4, QTableWidgetItem(str(trade.get("amount", "N/A"))))
                fee = trade.get("fee", {})
                if isinstance(fee, dict):
                    fee_cost = fee.get("cost", "N/A")
                    fee_currency = fee.get("currency", "")
                    fee_str = f"{fee_cost} {fee_currency}" if fee_currency else str(fee_cost)
                elif isinstance(fee, list) and fee:
                    fee_entry = fee[0]
                    fee_cost = fee_entry.get("cost", "N/A")
                    fee_currency = fee_entry.get("currency", "")
                    fee_str = f"{fee_cost} {fee_currency}" if fee_currency else str(fee_cost)
                else:
                    fee_str = "N/A"
                self.tableWidgetTransactions.setItem(row, 5, QTableWidgetItem(fee_str))
                trade_time = trade.get("datetime", None)
                if trade_time:
                    hist_balance = get_historical_balance(trade_time)
                else:
                    hist_balance = "N/A"
                self.tableWidgetTransactions.setItem(row, 6, QTableWidgetItem(str(hist_balance)))
            log_info("Transaktionsverlauf und historische Wallet-Salden erfolgreich geladen.")
        except Exception as e:
            error_message = f"Fehler beim Laden des Transaktionsverlaufs: {str(e)}"
            self.outputArea.append(error_message)
            log_error(error_message)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = BotApp()
    window.show()
    sys.exit(app.exec())
