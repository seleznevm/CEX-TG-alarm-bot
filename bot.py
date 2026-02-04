import os
import time
import json
import math
import signal
import datetime as dt
from typing import Any, Dict, Optional, Set, Tuple

import requests
from pybit.unified_trading import WebSocket, HTTP

from dotenv import load_dotenv

load_dotenv()

# ==========================
# Config
# ==========================
BYBIT_API_KEY = os.environ.get("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", "")
BYBIT_TESTNET = os.environ.get("BYBIT_TESTNET", "0") == "1"

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TG_THREAD_ID = os.environ.get("TELEGRAM_THREAD_ID", "")

UTC_OFFSET_HOURS = int(os.environ.get("UTC_OFFSET_HOURS", "7"))

# Safety limits
EXECID_CACHE_MAX = 5000  # keep last N execIds in memory


def require_env(name: str, value: str) -> None:
    if not value:
        raise RuntimeError(f"Missing env var: {name}")


# ==========================
# Helpers: formatting & time
# ==========================
def utc_to_local(ts_ms: int, utc_offset_hours: int) -> Tuple[str, str]:
    """Return (local_dt_str, utc_offset_str)."""
    utc_dt = dt.datetime.fromtimestamp(ts_ms / 1000, tz=dt.timezone.utc)
    offset = dt.timedelta(hours=utc_offset_hours)
    local_tz = dt.timezone(offset)
    local_dt = utc_dt.astimezone(local_tz)
    sign = "+" if utc_offset_hours >= 0 else "-"
    utc_off = f"{sign}{abs(utc_offset_hours)}"
    return local_dt.strftime("%Y-%m-%d %H:%M:%S"), f"UTC{utc_off}"


def fmt_num(x: Any, nd: int = 6) -> str:
    try:
        v = float(x)
        if math.isfinite(v):
            s = f"{v:.{nd}f}".rstrip("0").rstrip(".")
            return s
    except Exception:
        pass
    return str(x)


def map_market_type(category: str) -> str:
    """
    Bybit V5 categories: spot, linear, inverse, option
    """
    c = (category or "").lower()
    if c == "spot":
        return "Spot"
    if c == "linear":
        return "Futures (Linear)"
    if c == "inverse":
        return "Futures (Inverse)"
    if c == "option":
        return "Options"
    return category or "Unknown"


# ==========================
# Telegram sender
# ==========================
def tg_send_message(text: str) -> None:
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"

    payload: Dict[str, Any] = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }

    # Topic/thread support (supergroups with topics)
    if TG_THREAD_ID:
        payload["message_thread_id"] = int(TG_THREAD_ID)

    r = requests.post(url, json=payload, timeout=15)
    if not r.ok:
        raise RuntimeError(f"Telegram sendMessage failed: {r.status_code} {r.text}")


# ==========================
# Bybit REST helpers
# ==========================
class BybitRest:
    def __init__(self, testnet: bool):
        self.testnet = testnet
        self.http = HTTP(
            testnet=testnet,
            api_key=BYBIT_API_KEY,
            api_secret=BYBIT_API_SECRET,
        )

    def get_position_info(self, category: str, symbol: str) -> Dict[str, Any]:
        """
        For derivatives: returns avgPrice, size, leverage, etc.
        For spot: not applicable -> return empty.
        """
        c = (category or "").lower()
        if c not in ("linear", "inverse"):
            return {}

        resp = self.http.get_positions(category=c, symbol=symbol)
        lst = (((resp or {}).get("result") or {}).get("list") or [])
        if not lst:
            return {}
        return lst[0]

    def get_recent_daily_vol_changes(self, category: str, symbol: str, days: int = 7) -> str:
        """
        'day to previous day' volume change list as multiline string.
        Uses kline interval 'D' (1D).
        """
        try:
            resp = self.http.get_kline(
                category=(category or "").lower(),
                symbol=symbol,
                interval="D",
                limit=max(days + 1, 8),  # need prev day
            )
            rows = (((resp or {}).get("result") or {}).get("list") or [])
            if not rows:
                return "  n/a"

            rows = sorted(rows, key=lambda r: int(r[0]))  # ascending by ts
            rows = rows[-(days + 1):]  # last days+1

            out = []
            for i in range(1, len(rows)):
                ts = int(rows[i][0])
                date = dt.datetime.fromtimestamp(ts / 1000, tz=dt.timezone.utc).strftime("%Y-%m-%d")
                vol = float(rows[i][5])
                prev_vol = float(rows[i - 1][5])
                if prev_vol == 0:
                    ch = "n/a"
                else:
                    ch_pct = (vol - prev_vol) / prev_vol * 100.0
                    ch = f"{ch_pct:+.1f}%"
                out.append(f"  {date}: {ch}")

            return "\n".join(out[-days:])
        except Exception:
            return "  n/a"

    def get_rsi_4h(self, category: str, symbol: str, length: int = 14) -> Optional[float]:
        """
        RSI(length) on 4H closes using Wilder smoothing.
        Returns None if not enough data / error.
        """
        c = (category or "").lower()
        try:
            resp = self.http.get_kline(
                category=c,
                symbol=symbol,
                interval="240",  # 4H
                limit=200,
            )
            rows = (((resp or {}).get("result") or {}).get("list") or [])
            if not rows or len(rows) < (length + 2):
                return None

            rows = sorted(rows, key=lambda r: int(r[0]))  # ascending
            closes = [float(r[4]) for r in rows]

            gains = []
            losses = []
            for i in range(1, len(closes)):
                d = closes[i] - closes[i - 1]
                gains.append(max(d, 0.0))
                losses.append(max(-d, 0.0))

            avg_gain = sum(gains[:length]) / length
            avg_loss = sum(losses[:length]) / length

            for i in range(length, len(gains)):
                avg_gain = (avg_gain * (length - 1) + gains[i]) / length
                avg_loss = (avg_loss * (length - 1) + losses[i]) / length

            if avg_loss == 0:
                return 100.0

            rs = avg_gain / avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))
            return rsi
        except Exception:
            return None

    def make_bybit_link(self, category: str, symbol: str) -> str:
        """
        Deep-link to Bybit trading page for the symbol.
        If you're logged in in browser, it will open in your account context.
        """
        c = (category or "").lower()
        base = "https://testnet.bybit.com" if self.testnet else "https://www.bybit.com"

        if c == "spot":
            return f"{base}/trade/spot/{symbol}"
        if c == "inverse":
            return f"{base}/trade/inverse/{symbol}"
        return f"{base}/trade/usdt/{symbol}"

    @staticmethod
    def make_tv_link(symbol: str) -> str:
        """
        TradingView symbol link (generic).
        Exact exchange mapping can be added later.
        """
        return f"https://www.tradingview.com/symbols/{symbol}/"


# ==========================
# Message builder
# ==========================
def build_message(exec_evt: Dict[str, Any], rest: BybitRest) -> str:
    category = exec_evt.get("category", "") or exec_evt.get("categoryType", "") or ""
    market_type = map_market_type(category)

    symbol = exec_evt.get("symbol", "—")
    side = exec_evt.get("side", "—")
    order_type = exec_evt.get("orderType", exec_evt.get("order_type", "—"))
    order_status = exec_evt.get("orderStatus", exec_evt.get("order_status", "—"))
    exec_id = exec_evt.get("execId", exec_evt.get("exec_id", "—"))
    order_id = exec_evt.get("orderId", exec_evt.get("order_id", "—"))

    avg_fill_price = exec_evt.get("execPrice", exec_evt.get("price", "—"))
    filled_qty = exec_evt.get("execQty", exec_evt.get("qty", "—"))
    filled_notional = exec_evt.get("execValue", exec_evt.get("value", "—"))

    fee = exec_evt.get("execFee", "—")
    fee_coin = exec_evt.get("feeCurrency", exec_evt.get("feeCoin", "—"))

    ts_ms = int(exec_evt.get("execTime", exec_evt.get("ts", 0)) or 0)
    local_dt, utc_off = utc_to_local(ts_ms, UTC_OFFSET_HOURS)

    sl = exec_evt.get("stopLoss", exec_evt.get("sl", "—"))
    tp = exec_evt.get("takeProfit", exec_evt.get("tp", "—"))
    rr = exec_evt.get("riskReward", exec_evt.get("rr", "—"))

    pos = rest.get_position_info(category=category, symbol=symbol)
    avg_pos_price_after = pos.get("avgPrice", "—")
    pos_size_after = pos.get("size", "—")
    leverage = pos.get("leverage", "—") if pos else "—"

    exec_pnl = exec_evt.get("execPnl", None)
    net_pnl_str = "—"
    try:
        if exec_pnl is not None and exec_pnl != "":
            net = float(exec_pnl)
            if fee not in (None, "", "—"):
                net -= float(fee)
            net_pnl_str = f"{net:+.4f} {fee_coin if fee_coin not in ('—', None, '') else ''}".strip()
    except Exception:
        net_pnl_str = "—"

    reduce_only = str(exec_evt.get("reduceOnly", "")).lower() in ("true", "1")
    is_closing_hint = reduce_only or (exec_pnl not in (None, "", "—"))

    pnl_block = ""
    if is_closing_hint:
        pnl_block = f"\nЗакрытие сделки:\n• PnL (net): {net_pnl_str}\n"

    # Indicators
    rsi_4h = rest.get_rsi_4h(category=category, symbol=symbol, length=14)
    rsi_4h_str = f"{rsi_4h:.1f}" if isinstance(rsi_4h, (int, float)) else "n/a"

    # Links
    bybit_link = rest.make_bybit_link(category=category, symbol=symbol)
    tv_link = rest.make_tv_link(symbol=symbol)

    msg = (
        f"✅ DEALS | Ордер исполнен: {order_status}\n\n"
        f"Биржа: Bybit\n"
        f"Рынок: {market_type}        | Инструмент: {symbol}\n"
        f"Сторона: {side}             | Тип ордера: {order_type}\n"
        f"Плечо: {fmt_num(leverage, 2)}x\n\n"
        f"Исполнение:\n"
        f"• Avg fill: {fmt_num(avg_fill_price, 8)}\n"
        f"• Qty: {fmt_num(filled_qty, 8)}\n"
        f"• Notional: {fmt_num(filled_notional, 8)}\n"
        f"• Комиссия: {fmt_num(fee, 8)} {fee_coin}\n"
        f"{pnl_block}\n"
        f"Позиция (после исполнения):\n"
        f"• Avg position price: {fmt_num(avg_pos_price_after, 8)}\n"
        f"• Position size: {fmt_num(pos_size_after, 8)}\n\n"
        f"Risk (план):\n"
        f"• SL: {sl}\n"
        f"• TP: {tp}\n"
        f"• R:R: {rr}\n\n"
        f"Индикаторы:\n"
        f"• RSI 4H (14): {rsi_4h_str}\n\n"
        f"Объём (последние 7 дней, день к предыдущему дню):\n"
        f"{rest.get_recent_daily_vol_changes(category=category, symbol=symbol, days=7)}\n\n"
        f"Время:\n"
        f"• {local_dt} ({utc_off})\n\n"
        f"Ссылки:\n"
        f"• Bybit: {bybit_link}\n"
        f"• TradingView: {tv_link}\n\n"
        f"Idempotency:\n"
        f"• execId: {exec_id}\n"
        f"• orderId: {order_id}"
    )

    return msg


# ==========================
# Main: WebSocket execution listener
# ==========================
running = True
execid_seen: Set[str] = set()


def trim_exec_cache() -> None:
    global execid_seen
    if len(execid_seen) <= EXECID_CACHE_MAX:
        return
    execid_seen = set(list(execid_seen)[-EXECID_CACHE_MAX:])


def on_execution(message: Dict[str, Any], rest: BybitRest) -> None:
    data = message.get("data") or []
    if not isinstance(data, list):
        return

    for evt in data:
        exec_type = (evt.get("execType") or evt.get("exec_type") or "").lower()
        if exec_type and exec_type != "trade":
            continue

        exec_id = str(evt.get("execId", "") or "")
        if not exec_id:
            continue

        if exec_id in execid_seen:
            continue

        execid_seen.add(exec_id)
        trim_exec_cache()

        try:
            text = build_message(evt, rest)
            tg_send_message(text)
            print(f"[OK] Sent execId={exec_id}")
        except Exception as e:
            print(f"[ERR] Failed to process execId={exec_id}: {e}")


def handle_stop(signum, frame):
    global running
    running = False
    print("\nStopping...")


def main():
    require_env("BYBIT_API_KEY", BYBIT_API_KEY)
    require_env("BYBIT_API_SECRET", BYBIT_API_SECRET)
    require_env("TELEGRAM_BOT_TOKEN", TG_TOKEN)
    require_env("TELEGRAM_CHAT_ID", TG_CHAT_ID)

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    rest = BybitRest(testnet=BYBIT_TESTNET)

    ws = WebSocket(
        testnet=BYBIT_TESTNET,
        channel_type="private",
        api_key=BYBIT_API_KEY,
        api_secret=BYBIT_API_SECRET,
    )

    ws.subscribe(
        topic="execution",
        callback=lambda msg: on_execution(msg, rest),
    )

    print("MVP started. Listening executions...")

    while running:
        time.sleep(1)

    try:
        ws.exit()
    except Exception:
        pass

    print("Stopped.")


if __name__ == "__main__":
    main()
