import os
import time
import math
import datetime as dt
import logging
from typing import Any, Dict, Optional, Tuple, Set

import requests
from dotenv import load_dotenv
from pybit.unified_trading import HTTP, WebSocket

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

EXECID_CACHE_MAX = int(os.environ.get("EXECID_CACHE_MAX", "5000"))

# Logging
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("bybit_exec_listener")


def require_env(name: str, value: str) -> None:
    if not value:
        raise RuntimeError(f"Missing env var: {name}")


# ==========================
# Helpers
# ==========================
def utc_to_local(ts_ms: int, utc_offset_hours: int) -> Tuple[str, str]:
    utc_dt = dt.datetime.fromtimestamp(ts_ms / 1000, tz=dt.timezone.utc)
    offset = dt.timedelta(hours=utc_offset_hours)
    local_dt = utc_dt.astimezone(dt.timezone(offset))
    sign = "+" if utc_offset_hours >= 0 else "-"
    return local_dt.strftime("%Y-%m-%d %H:%M:%S"), f"UTC{sign}{abs(utc_offset_hours)}"


def fmt_num(x: Any, nd: int = 6) -> str:
    try:
        v = float(x)
        if math.isfinite(v):
            return f"{v:.{nd}f}".rstrip("0").rstrip(".")
    except (ValueError, TypeError):
        pass
    return str(x)


def to_float(x: Any) -> Optional[float]:
    try:
        if x in (None, "", "—"):
            return None
        v = float(x)
        return v if math.isfinite(v) else None
    except (ValueError, TypeError):
        return None


def map_market_type(category: str) -> str:
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


def calc_rr(side: str, entry: Optional[float], sl: Optional[float], tp: Optional[float]) -> Optional[float]:
    if entry is None or sl is None or tp is None:
        return None

    s = (side or "").lower()
    is_long = s in ("buy", "long")

    if is_long:
        risk = entry - sl
        reward = tp - entry
    else:
        risk = sl - entry
        reward = entry - tp

    if risk <= 0 or reward <= 0:
        return None
    return reward / risk


# ==========================
# Telegram
# ==========================
def tg_send_message(text: str) -> None:
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload: Dict[str, Any] = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",  # IMPORTANT for <b> and <a>
        "disable_web_page_preview": True,
    }
    if TG_THREAD_ID:
        payload["message_thread_id"] = int(TG_THREAD_ID)

    r = requests.post(url, json=payload, timeout=15)
    if not r.ok:
        raise RuntimeError(f"Telegram sendMessage failed: {r.status_code} {r.text}")


# ==========================
# Bybit REST helpers + cache
# ==========================
class BybitRest:
    def __init__(self, testnet: bool):
        self.testnet = testnet
        self.http = HTTP(
            testnet=testnet,
            api_key=BYBIT_API_KEY,
            api_secret=BYBIT_API_SECRET,
        )
        self._order_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._position_cache: Dict[Tuple[str, str], Tuple[float, Dict[str, Any]]] = {}

    def _cache_get(self, order_id: str, ttl_sec: int) -> Optional[Dict[str, Any]]:
        item = self._order_cache.get(order_id)
        if not item:
            return None
        ts, data = item
        if time.time() - ts > ttl_sec:
            self._order_cache.pop(order_id, None)
            return None
        return data

    def _cache_put(self, order_id: str, data: Dict[str, Any], max_items: int) -> None:
        self._order_cache[order_id] = (time.time(), data)
        if len(self._order_cache) > max_items:
            items = sorted(self._order_cache.items(), key=lambda kv: kv[1][0])
            for k, _ in items[: max(1, len(items) - max_items)]:
                self._order_cache.pop(k, None)

    def _position_cache_get(self, key: Tuple[str, str], ttl_sec: int) -> Optional[Dict[str, Any]]:
        item = self._position_cache.get(key)
        if not item:
            return None
        ts, data = item
        if time.time() - ts > ttl_sec:
            self._position_cache.pop(key, None)
            return None
        return data

    def _position_cache_put(self, key: Tuple[str, str], data: Dict[str, Any], max_items: int) -> None:
        self._position_cache[key] = (time.time(), data)
        if len(self._position_cache) > max_items:
            items = sorted(self._position_cache.items(), key=lambda kv: kv[1][0])
            for k, _ in items[: max(1, len(items) - max_items)]:
                self._position_cache.pop(k, None)

    def get_order_details(self, category: str, symbol: str, order_id: str,
                          cache_ttl_sec: int = 3600, cache_max: int = 3000) -> Dict[str, Any]:
        if not order_id:
            return {}

        cached = self._cache_get(order_id, cache_ttl_sec)
        if cached is not None:
            return cached

        c = (category or "").lower()
        if not c:
            return {}

        try:
            resp = self.http.get_order_history(category=c, symbol=symbol, orderId=order_id, limit=50)
            lst = (((resp or {}).get("result") or {}).get("list") or [])
            if lst:
                self._cache_put(order_id, lst[0], cache_max)
                return lst[0]
        except Exception as e:
            log.warning(f"Order details by orderId failed: {e}")

        # fallback: without orderId
        try:
            resp = self.http.get_order_history(category=c, symbol=symbol, limit=50)
            lst = (((resp or {}).get("result") or {}).get("list") or [])
            for it in lst:
                if str(it.get("orderId", "")) == str(order_id):
                    self._cache_put(order_id, it, cache_max)
                    return it
        except Exception as e:
            log.warning(f"Order details fallback failed: {e}")

        self._cache_put(order_id, {}, cache_max)
        return {}

    def get_position_details(self, category: str, symbol: str,
                             cache_ttl_sec: int = 15, cache_max: int = 2000) -> Dict[str, Any]:
        c = (category or "").lower()
        if not c or not symbol:
            return {}

        key = (c, symbol)
        cached = self._position_cache_get(key, cache_ttl_sec)
        if cached is not None:
            return cached

        try:
            resp = self.http.get_positions(category=c, symbol=symbol)
            lst = (((resp or {}).get("result") or {}).get("list") or [])
            if lst:
                self._position_cache_put(key, lst[0], cache_max)
                return lst[0]
        except Exception as e:
            log.warning(f"Position details failed: {e}")

        self._position_cache_put(key, {}, cache_max)
        return {}

    def get_rsi_4h(self, category: str, symbol: str, length: int = 14) -> Optional[float]:
        c = (category or "").lower()
        if not c:
            return None
        try:
            resp = self.http.get_kline(category=c, symbol=symbol, interval="240", limit=200)
            rows = (((resp or {}).get("result") or {}).get("list") or [])
            if not rows or len(rows) < (length + 2):
                return None

            rows = sorted(rows, key=lambda r: int(r[0]))
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
            return 100.0 - (100.0 / (1.0 + rs))
        except Exception as e:
            log.warning(f"RSI calc failed: {e}")
            return None

    def make_bybit_link(self, category: str, symbol: str) -> str:
        c = (category or "").lower()
        base = "https://testnet.bybit.com" if self.testnet else "https://www.bybit.com"
        if c == "spot":
            return f"{base}/trade/spot/{symbol}"
        if c == "inverse":
            return f"{base}/trade/inverse/{symbol}"
        return f"{base}/trade/usdt/{symbol}"

    @staticmethod
    def make_tv_link(symbol: str) -> str:
        return f"https://www.tradingview.com/symbols/{symbol}/"


# ==========================
# Message builder
# ==========================
def build_message(exec_evt: Dict[str, Any], rest: BybitRest) -> str:
    category = exec_evt.get("category", "") or exec_evt.get("categoryType", "") or ""
    market_type = map_market_type(category)

    symbol = str(exec_evt.get("symbol", "—"))
    side = str(exec_evt.get("side", "—"))
    order_type = exec_evt.get("orderType", exec_evt.get("order_type", "—"))
    order_status = exec_evt.get("orderStatus", exec_evt.get("order_status", "—"))

    exec_id = str(exec_evt.get("execId", exec_evt.get("exec_id", "—")))
    order_id = str(exec_evt.get("orderId", exec_evt.get("order_id", "")) or "")

    avg_fill_price = exec_evt.get("execPrice", exec_evt.get("price", "—"))
    filled_qty = exec_evt.get("execQty", exec_evt.get("qty", "—"))
    filled_notional = exec_evt.get("execValue", exec_evt.get("value", "—"))

    fee = exec_evt.get("execFee", "—")
    fee_coin = exec_evt.get("feeCurrency", exec_evt.get("feeCoin", "—"))
    pnl_coin = exec_evt.get("pnlCurrency", exec_evt.get("profitCurrency", fee_coin or "USDT"))
    realized_pnl = to_float(exec_evt.get("execPnl") or exec_evt.get("realizedPnl") or exec_evt.get("closedPnl"))
    current_pnl = to_float(
        exec_evt.get("pnl")
        or exec_evt.get("unrealizedPnl")
        or exec_evt.get("unrealisedPnl")
        or exec_evt.get("cumPnl")
        or exec_evt.get("positionPnl")
    )

    ts_ms = int(exec_evt.get("execTime", exec_evt.get("ts", 0)) or 0)
    local_dt, utc_off = utc_to_local(ts_ms, UTC_OFFSET_HOURS)

    # Pull SL/TP from order history (if possible)
    order_details = rest.get_order_details(category, symbol, order_id) if (order_id and category) else {}
    if order_status in ("—", "", None):
        order_status = order_details.get("orderStatus", "—")
    if realized_pnl is None:
        realized_pnl = to_float(order_details.get("closedPnl") or order_details.get("realizedPnl"))
    stop_loss = to_float(order_details.get("stopLoss"))
    take_profit = to_float(order_details.get("takeProfit"))
    entry_price = to_float(avg_fill_price)

    if current_pnl is None and category:
        position_details = rest.get_position_details(category, symbol)
        current_pnl = to_float(
            position_details.get("unrealisedPnl")
            or position_details.get("unrealizedPnl")
            or position_details.get("pnl")
        )

    rr_ratio = calc_rr(side, entry_price, stop_loss, take_profit)

    rsi = rest.get_rsi_4h(category, symbol, length=14)
    rsi_str = fmt_num(rsi, 2) if rsi is not None else "n/a"

    bybit_link = rest.make_bybit_link(category, symbol)
    tv_link = rest.make_tv_link(symbol)

    lines = [
        "🔔 <b>Исполнение ордера</b>",
        "",
        f"<b>Биржа:</b> Bybit",
        f"<b>Тип рынка:</b> {market_type}",
        f"<b>Инструмент:</b> {symbol}",
        f"<b>Сторона:</b> {side}",
        f"<b>Тип ордера:</b> {order_type}",
        f"<b>Статус:</b> {order_status}",
        "",
        f"<b>Цена исполнения:</b> {fmt_num(avg_fill_price)}",
        f"<b>Объем:</b> {fmt_num(filled_qty)}",
        f"<b>Сумма:</b> {fmt_num(filled_notional)}",
        f"<b>Комиссия:</b> {fmt_num(fee)} {fee_coin}",
    ]

    if realized_pnl is not None:
        lines.append(f"<b>Реализованный PnL:</b> {fmt_num(realized_pnl)} {pnl_coin}")
    if current_pnl is not None:
        lines.append(f"<b>Текущий PnL:</b> {fmt_num(current_pnl)} {pnl_coin}")

    if stop_loss is not None:
        lines.append(f"<b>Stop Loss:</b> {fmt_num(stop_loss)}")
    if take_profit is not None:
        lines.append(f"<b>Take Profit:</b> {fmt_num(take_profit)}")
    if rr_ratio is not None:
        lines.append(f"<b>R:R:</b> {fmt_num(rr_ratio, 2)}")

    lines += [
        f"<b>RSI (4H):</b> {rsi_str}",
        "",
        f"<b>Время:</b> {local_dt} ({utc_off})",
        f"<b>ID исполнения:</b> {exec_id}",
        f"<b>Order ID:</b> {order_id}",
        "",
        f"🔗 <a href='{bybit_link}'>Bybit</a> | <a href='{tv_link}'>TradingView</a>",
    ]

    return "\n".join(lines)


# ==========================
# WS handler
# ==========================
execid_seen: Set[str] = set()


def trim_exec_cache() -> None:
    global execid_seen
    if len(execid_seen) <= EXECID_CACHE_MAX:
        return
    execid_seen = set(list(execid_seen)[-EXECID_CACHE_MAX:])


def on_execution_message(message: Dict[str, Any], rest: BybitRest) -> None:
    # Debug one-liner (uncomment if needed)
    # log.debug(f"WS raw: {message}")

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
            return

        execid_seen.add(exec_id)
        trim_exec_cache()

        try:
            text = build_message(evt, rest)
            tg_send_message(text)
            log.info(f"Sent execId={exec_id}")
        except Exception as e:
            log.exception(f"Failed processing execId={exec_id}: {e}")


def main() -> None:
    require_env("BYBIT_API_KEY", BYBIT_API_KEY)
    require_env("BYBIT_API_SECRET", BYBIT_API_SECRET)
    require_env("TELEGRAM_BOT_TOKEN", TG_TOKEN)
    require_env("TELEGRAM_CHAT_ID", TG_CHAT_ID)

    log.info("Starting Bybit WS execution listener...")
    log.info(f"Testnet: {BYBIT_TESTNET}")

    rest = BybitRest(testnet=BYBIT_TESTNET)

    ws = WebSocket(
        testnet=BYBIT_TESTNET,
        channel_type="private",
        api_key=BYBIT_API_KEY,
        api_secret=BYBIT_API_SECRET,
    )

    # IMPORTANT: subscribe to private execution stream
    ws.subscribe(
        topic="execution",
        callback=lambda msg: on_execution_message(msg, rest),
    )

    log.info("Subscribed to private topic: execution")
    log.info("Waiting for execution events...")

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
