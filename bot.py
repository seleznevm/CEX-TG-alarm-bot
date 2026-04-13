import os
import time
import math
import json
import datetime as dt
import logging
import threading
import concurrent.futures as cf
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Set

import requests
from dotenv import load_dotenv
from pybit.unified_trading import HTTP, WebSocket

load_dotenv()

# ==========================
# Config
# ==========================
BOT_VERSION = "1.8"
BYBIT_API_KEY = os.environ.get("BYBIT_API_KEY", "").strip()
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", "").strip()
BYBIT_TESTNET = os.environ.get("BYBIT_TESTNET", "0") == "1"

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TG_THREAD_ID = os.environ.get("TELEGRAM_THREAD_ID", "").strip()

UTC_OFFSET_HOURS = int(os.environ.get("UTC_OFFSET_HOURS", "7"))
EXECID_CACHE_MAX = int(os.environ.get("EXECID_CACHE_MAX", "5000"))
ORDER_AGGREGATION_WINDOW_SEC = int(os.environ.get("ORDER_AGGREGATION_WINDOW_SEC", "8"))
BYBIT_WS_AUTH_EXPIRE_SEC = max(1, int(os.environ.get("BYBIT_WS_AUTH_EXPIRE_SEC", "10")))
LIMIT_ORDER_ALERT_INTERVAL_SEC = max(60, int(os.environ.get("LIMIT_ORDER_ALERT_INTERVAL_SEC", "300")))
LIMIT_ORDER_ALERT_DISTANCE_PCT = max(0.1, float(os.environ.get("LIMIT_ORDER_ALERT_DISTANCE_PCT", "5")))
LIMIT_ORDER_ALERT_CATEGORIES = [
    item.strip().lower()
    for item in os.environ.get("LIMIT_ORDER_ALERT_CATEGORIES", "linear").split(",")
    if item.strip()
]
POSITION_FUNDING_LOOKBACK_DAYS = max(7, int(os.environ.get("POSITION_FUNDING_LOOKBACK_DAYS", "30")))
FUNDING_LOOKUP_TIMEOUT_SEC = max(1, int(os.environ.get("FUNDING_LOOKUP_TIMEOUT_SEC", "3")))
TG_ALLOWED_USER_ID = os.environ.get("TELEGRAM_ALLOWED_USER_ID", "").strip()
TG_COMMAND_POLL_TIMEOUT_SEC = max(1, int(os.environ.get("TG_COMMAND_POLL_TIMEOUT_SEC", "25")))
TG_COMMAND_ERROR_SLEEP_SEC = max(1, int(os.environ.get("TG_COMMAND_ERROR_SLEEP_SEC", "5")))

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


def calc_change_percent(direction: str, entry: Optional[float], exit_price: Optional[float]) -> Optional[float]:
    if entry is None or exit_price is None or entry <= 0:
        return None
    d = (direction or "").lower()
    if d in ("sell", "short"):
        return ((entry - exit_price) / entry) * 100.0
    return ((exit_price - entry) / entry) * 100.0


INTERVAL_TO_MS: Dict[str, int] = {
    "1": 60_000,
    "3": 180_000,
    "5": 300_000,
    "15": 900_000,
    "30": 1_800_000,
    "60": 3_600_000,
    "120": 7_200_000,
    "240": 14_400_000,
    "360": 21_600_000,
    "720": 43_200_000,
    "D": 86_400_000,
    "W": 604_800_000,
}


def format_signed_percent(x: Optional[float], nd: int = 2) -> str:
    if x is None:
        return "n/a"
    sign = "+" if x > 0 else ""
    return f"{sign}{fmt_num(x, nd)}%"


def format_duration(seconds: float) -> str:
    total = max(0, int(seconds))
    days, rem = divmod(total, 86_400)
    hours, rem = divmod(rem, 3_600)
    minutes, _ = divmod(rem, 60)
    parts: List[str] = []
    if days:
        parts.append(f"{days}d")
    if hours or parts:
        parts.append(f"{hours}h")
    parts.append(f"{minutes}m")
    return " ".join(parts)


def safe_int(x: Any) -> int:
    try:
        return int(str(x or "0"))
    except (ValueError, TypeError):
        return 0


def get_interval_ms(interval: str) -> Optional[int]:
    return INTERVAL_TO_MS.get(str(interval))


def parse_kline_rows(rows: List[List[Any]]) -> List[Dict[str, float]]:
    parsed: List[Dict[str, float]] = []
    for row in sorted(rows, key=lambda r: int(r[0])):
        try:
            parsed.append({
                "start": float(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "turnover": float(row[6]),
            })
        except (IndexError, TypeError, ValueError):
            continue
    return parsed


def get_completed_klines(rows: List[Dict[str, float]], interval: str) -> List[Dict[str, float]]:
    if not rows:
        return []
    interval_ms = get_interval_ms(interval)
    if interval_ms is None:
        return rows

    now_ms = int(time.time() * 1000)
    completed = list(rows)
    if completed and now_ms < int(completed[-1]["start"]) + interval_ms:
        completed.pop()
    return completed


def calc_rsi_from_closes(closes: List[float], length: int = 14) -> Optional[float]:
    if len(closes) < length + 1:
        return None
    gains = []
    losses = []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    avg_gain = sum(gains[:length]) / length
    avg_loss = sum(losses[:length]) / length
    for i in range(length, len(gains)):
        avg_gain = (avg_gain * (length - 1) + gains[i]) / length
        avg_loss = (avg_loss * (length - 1) + losses[i]) / length

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def calc_ema_from_closes(closes: List[float], period: int) -> Optional[float]:
    if len(closes) < period:
        return None
    multiplier = 2.0 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = (price - ema) * multiplier + ema
    return ema


def calc_atr_from_rows(rows: List[Dict[str, float]], length: int = 14) -> Optional[float]:
    if len(rows) < length + 1:
        return None
    true_ranges: List[float] = []
    for i in range(1, len(rows)):
        high = rows[i]["high"]
        low = rows[i]["low"]
        prev_close = rows[i - 1]["close"]
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    if len(true_ranges) < length:
        return None
    atr = sum(true_ranges[:length]) / length
    for tr in true_ranges[length:]:
        atr = (atr * (length - 1) + tr) / length
    return atr


def calc_volume_change_percent(rows: List[Dict[str, float]]) -> Optional[float]:
    if len(rows) < 2:
        return None
    current = rows[-1]["turnover"]
    previous = rows[-2]["turnover"]
    if previous <= 0:
        return None
    return ((current - previous) / previous) * 100.0


def resolve_price_vs_ema_band(price: Optional[float], ema20: Optional[float], ema50: Optional[float]) -> str:
    if price is None or ema20 is None or ema50 is None:
        return "n/a"
    band_low = min(ema20, ema50)
    band_high = max(ema20, ema50)
    if price > band_high:
        return "выше"
    if price < band_low:
        return "ниже"
    return "внутри"


# ==========================
# Telegram
# ==========================
def tg_api_request(method: str, *, params: Optional[Dict[str, Any]] = None, payload: Optional[Dict[str, Any]] = None, timeout: int = 15) -> Dict[str, Any]:
    url = f"https://api.telegram.org/bot{TG_TOKEN}/{method}"
    if payload is not None:
        response = requests.post(url, json=payload, timeout=timeout)
    else:
        response = requests.get(url, params=params, timeout=timeout)
    if not response.ok:
        raise RuntimeError(f"Telegram {method} failed: {response.status_code} {response.text}")
    body = response.json()
    if not body.get("ok"):
        raise RuntimeError(f"Telegram {method} error: {json.dumps(body, ensure_ascii=False)}")
    return body


def tg_send_message(
    text: str,
    chat_id: Optional[str] = None,
    thread_id: Optional[str] = None,
    reply_to_message_id: Optional[int] = None,
) -> None:
    payload: Dict[str, Any] = {
        "chat_id": chat_id or TG_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    effective_thread_id = thread_id if thread_id is not None else TG_THREAD_ID
    if effective_thread_id:
        payload["message_thread_id"] = int(effective_thread_id)
    if reply_to_message_id:
        payload["reply_to_message_id"] = int(reply_to_message_id)
        payload["allow_sending_without_reply"] = True
    tg_api_request("sendMessage", payload=payload)


def tg_get_updates(offset: Optional[int] = None, timeout_sec: int = TG_COMMAND_POLL_TIMEOUT_SEC) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "timeout": timeout_sec,
        "allowed_updates": json.dumps(["message"]),
    }
    if offset is not None:
        params["offset"] = offset
    body = tg_api_request("getUpdates", params=params, timeout=timeout_sec + 5)
    result = body.get("result") or []
    return result if isinstance(result, list) else []


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
        self._rsi_cache: Dict[Tuple[str, str, str, int], Tuple[float, Optional[float]]] = {}
        self._funding_cache: Dict[Tuple[str, str, str, str], Tuple[float, Optional[Dict[str, Any]]]] = {}
        self._kline_cache: Dict[Tuple[str, str, str, int], Tuple[float, List[Dict[str, float]]]] = {}
        self._open_interest_cache: Dict[Tuple[str, str], Tuple[float, Optional[float]]] = {}

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

    def get_order_details(
        self,
        category: str,
        symbol: str,
        order_id: str,
        cache_ttl_sec: int = 3600,
        cache_max: int = 3000,
    ) -> Dict[str, Any]:
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

    def get_position_details(
        self,
        category: str,
        symbol: str,
        cache_ttl_sec: int = 15,
        cache_max: int = 2000,
    ) -> Dict[str, Any]:
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

    def get_rsi(
        self,
        category: str,
        symbol: str,
        interval: str,
        length: int = 14,
        cache_ttl_sec: int = 300,
    ) -> Optional[float]:
        c = (category or "").lower()
        if not c or not symbol:
            return None

        cache_key = (c, symbol, str(interval), length)
        cached = self._rsi_cache.get(cache_key)
        if cached and (time.time() - cached[0]) <= cache_ttl_sec:
            return cached[1]

        try:
            resp = self.http.get_kline(category=c, symbol=symbol, interval=str(interval), limit=200)
            rows = (((resp or {}).get("result") or {}).get("list") or [])
            if not rows or len(rows) < (length + 2):
                return None

            rows = sorted(rows, key=lambda r: int(r[0]))
            closes = [float(r[4]) for r in rows]

            gains = []
            losses = []
            for i in range(1, len(closes)):
                delta = closes[i] - closes[i - 1]
                gains.append(max(delta, 0.0))
                losses.append(max(-delta, 0.0))

            avg_gain = sum(gains[:length]) / length
            avg_loss = sum(losses[:length]) / length

            for i in range(length, len(gains)):
                avg_gain = (avg_gain * (length - 1) + gains[i]) / length
                avg_loss = (avg_loss * (length - 1) + losses[i]) / length

            if avg_loss == 0:
                result = 100.0
            else:
                rs = avg_gain / avg_loss
                result = 100.0 - (100.0 / (1.0 + rs))

            self._rsi_cache[cache_key] = (time.time(), result)
            return result
        except Exception as e:
            log.warning(f"RSI calc failed: {e}")
            return None

    def get_rsi_4h(self, category: str, symbol: str, length: int = 14) -> Optional[float]:
        return self.get_rsi(category=category, symbol=symbol, interval="240", length=length)

    def get_rsi_1h(self, category: str, symbol: str, length: int = 14) -> Optional[float]:
        return self.get_rsi(category=category, symbol=symbol, interval="60", length=length)

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

    def get_all_open_positions(self, category: str = "linear") -> List[Dict[str, Any]]:
        """Получает все текущие открытые позиции для категории (по умолчанию linear/фьючерсы)"""
        try:
            resp = self.http.get_positions(category=category, settleCoin="USDT")
            lst = (((resp or {}).get("result") or {}).get("list") or [])
            result: List[Dict[str, Any]] = []
            for item in lst:
                size = to_float(item.get("size"))
                if size and size > 0:
                    enriched = dict(item)
                    enriched["category"] = str(item.get("category") or category)
                    result.append(enriched)
            return result
        except Exception as e:
            log.warning(f"Failed to get open positions: {e}")
            return []

    def get_ticker_info(self, category: str, symbol: str) -> Dict[str, Any]:
        """Получает текущую информацию о тикере, включая ставку фандинга"""
        try:
            resp = self.http.get_tickers(category=category, symbol=symbol)
            lst = (((resp or {}).get("result") or {}).get("list") or [])
            if lst:
                return lst[0]
        except Exception as e:
            log.warning(f"Failed to get ticker info for {symbol}: {e}")
        return {}

    def get_all_open_limit_orders(self, categories: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        orders: List[Dict[str, Any]] = []
        for category in categories or ["linear"]:
            cursor = ""
            for _ in range(20):
                query: Dict[str, Any] = {"category": category, "limit": 50}
                if cursor:
                    query["cursor"] = cursor

                try:
                    resp = self.http.get_open_orders(**query)
                except Exception as e:
                    log.warning(f"Failed to get open orders for {category}: {e}")
                    break

                result = (resp or {}).get("result") or {}
                page = result.get("list") or []
                for item in page:
                    order_type = str(item.get("orderType") or "").lower()
                    status = str(item.get("orderStatus") or "").lower()
                    leaves_qty = to_float(item.get("leavesQty"))
                    qty = to_float(item.get("qty") or item.get("orderQty"))

                    if order_type != "limit":
                        continue
                    if status in {"filled", "cancelled", "rejected", "deactivated", "partiallyfilledcanceled"}:
                        continue
                    if leaves_qty is None:
                        leaves_qty = qty or 0.0
                    if leaves_qty <= 0:
                        continue

                    enriched = dict(item)
                    enriched["category"] = str(item.get("category") or category)
                    orders.append(enriched)

                cursor = str(result.get("nextPageCursor") or "").strip()
                if not cursor or not page:
                    break

        return orders

    def get_current_price(self, category: str, symbol: str) -> Optional[float]:
        ticker = self.get_ticker_info(category, symbol)
        return (
            to_float(ticker.get("lastPrice"))
            or to_float(ticker.get("markPrice"))
            or to_float(ticker.get("indexPrice"))
            or to_float(ticker.get("bid1Price"))
            or to_float(ticker.get("ask1Price"))
        )

    def get_kline_rows(
        self,
        category: str,
        symbol: str,
        interval: str,
        limit: int = 250,
        cache_ttl_sec: int = 90,
    ) -> List[Dict[str, float]]:
        c = (category or "").lower()
        if not c or not symbol:
            return []

        cache_key = (c, symbol, str(interval), limit)
        cached = self._kline_cache.get(cache_key)
        if cached and (time.time() - cached[0]) <= cache_ttl_sec:
            return list(cached[1])

        try:
            resp = self.http.get_kline(category=c, symbol=symbol, interval=str(interval), limit=limit)
            rows = parse_kline_rows((((resp or {}).get("result") or {}).get("list") or []))
            self._kline_cache[cache_key] = (time.time(), rows)
            return list(rows)
        except Exception as e:
            log.warning(f"Failed to get kline rows for {symbol} interval={interval}: {e}")
            return []

    def get_open_interest_value(self, category: str, symbol: str, interval_time: str = "5min", cache_ttl_sec: int = 120) -> Optional[float]:
        c = (category or "").lower()
        if not c or not symbol:
            return None

        cache_key = (c, symbol)
        cached = self._open_interest_cache.get(cache_key)
        if cached and (time.time() - cached[0]) <= cache_ttl_sec:
            return cached[1]

        value = None
        ticker = self.get_ticker_info(c, symbol)
        value = (
            to_float(ticker.get("openInterestValue"))
            or to_float(ticker.get("openInterest"))
        )

        if value is None and c in {"linear", "inverse"}:
            try:
                resp = self.http.get_open_interest(category=c, symbol=symbol, intervalTime=interval_time, limit=1)
                rows = (((resp or {}).get("result") or {}).get("list") or [])
                if rows:
                    value = to_float(rows[0].get("openInterest"))
            except Exception as e:
                log.warning(f"Failed to get open interest for {symbol}: {e}")

        self._open_interest_cache[cache_key] = (time.time(), value)
        return value

    def get_position_funding_total(
        self,
        category: str,
        symbol: str,
        side: str,
        position_created_time: str = "",
        cache_ttl_sec: int = 60,
    ) -> Optional[Dict[str, Any]]:
        cache_key = (
            (category or "").lower(),
            symbol,
            (side or "").lower(),
            str(position_created_time or ""),
        )
        cached = self._funding_cache.get(cache_key)
        if cached and (time.time() - cached[0]) <= cache_ttl_sec:
            return cached[1]

        c = (category or "").lower()
        if not c or not symbol:
            return None

        total_funding = 0.0
        funding_currency = "USDT"
        funding_records = 0
        now_ms = int(time.time() * 1000)
        window_ms = 7 * 24 * 3600 * 1000
        max_lookback_ms = POSITION_FUNDING_LOOKBACK_DAYS * 24 * 3600 * 1000
        found: Optional[Dict[str, Any]] = None

        try:
            end_time = now_ms
            created_time_ms = int(position_created_time or 0)
            min_start_time = max(created_time_ms, now_ms - max_lookback_ms)

            while end_time > min_start_time:
                start_time = max(min_start_time, end_time - window_ms)
                cursor = ""

                while True:
                    query: Dict[str, Any] = {
                        "accountType": "UNIFIED",
                        "category": c,
                        "type": "SETTLEMENT",
                        "startTime": start_time,
                        "endTime": end_time,
                        "limit": 50,
                    }
                    if cursor:
                        query["cursor"] = cursor

                    resp = self.http.get_transaction_log(**query)
                    result = (resp or {}).get("result") or {}
                    rows = result.get("list") or []

                    for item in rows:
                        if str(item.get("symbol") or "") != symbol:
                            continue

                        item_side = str(item.get("side") or "").lower()
                        if item_side and item_side != str(side).lower():
                            continue

                        funding = to_float(item.get("funding"))
                        if funding is None:
                            continue

                        total_funding += funding
                        funding_currency = str(item.get("currency") or funding_currency or "USDT")
                        funding_records += 1

                    cursor = str(result.get("nextPageCursor") or "").strip()
                    if not cursor or not rows:
                        break

                end_time = start_time - 1
        except Exception as e:
            log.warning(f"Failed to get cumulative funding for {symbol}: {e}")

        if funding_records > 0:
            found = {
                "funding": total_funding,
                "currency": funding_currency,
                "records": funding_records,
            }

        self._funding_cache[cache_key] = (time.time(), found)
        return found


# ==========================
# Position state snapshot
# ==========================
position_state_lock = threading.Lock()
position_state: Dict[Tuple[str, str], Dict[str, Any]] = {}


def get_prev_position_snapshot(category: str, symbol: str) -> Dict[str, Any]:
    with position_state_lock:
        return dict(position_state.get((category.lower(), symbol), {}))


def update_position_snapshot(category: str, symbol: str, details: Dict[str, Any]) -> None:
    key = (category.lower(), symbol)
    size = to_float(details.get("size")) or 0.0
    with position_state_lock:
        if size <= 0:
            position_state.pop(key, None)
            return
        position_state[key] = {
            "size": size,
            "avgPrice": to_float(details.get("avgPrice") or details.get("avgEntryPrice") or details.get("entryPrice")),
            "side": str(details.get("side", "")).lower(),
            "updatedAt": time.time(),
        }


# ==========================
# Message builder
# ==========================
def resolve_event_title(
    side: str,
    reduce_only: bool,
    prev_size: float,
    current_size: float,
) -> str:
    if reduce_only:
        return "Закрытие позиции" if current_size <= 0 else "Частичное закрытие"

    if prev_size <= 0 and current_size > 0:
        return "Открыта позиция"

    if current_size > prev_size:
        return "Добавка"

    side_l = (side or "").lower()
    if side_l in ("buy", "sell"):
        return "Открыта позиция"
    return "Исполнение ордера"


def build_message(exec_evt: Dict[str, Any], rest: BybitRest) -> str:
    category = str(exec_evt.get("category", "") or exec_evt.get("categoryType", "") or "")
    market_type = map_market_type(category)

    symbol = str(exec_evt.get("symbol", "—"))
    side = str(exec_evt.get("side", "—"))
    order_type = exec_evt.get("orderType", exec_evt.get("order_type", "—"))
    order_status = exec_evt.get("orderStatus", exec_evt.get("order_status", "—"))

    exec_id = str(exec_evt.get("execId", exec_evt.get("exec_id", "—")))
    exec_count = int(exec_evt.get("aggregatedExecCount", 1) or 1)
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

    order_details = rest.get_order_details(category, symbol, order_id) if (order_id and category) else {}
    if order_status in ("—", "", None):
        order_status = order_details.get("orderStatus", "—")
    if realized_pnl is None:
        realized_pnl = to_float(order_details.get("closedPnl") or order_details.get("realizedPnl"))

    stop_loss = to_float(order_details.get("stopLoss"))
    take_profit = to_float(order_details.get("takeProfit"))

    position_details = rest.get_position_details(category, symbol) if category else {}
    if current_pnl is None:
        current_pnl = to_float(
            position_details.get("unrealisedPnl")
            or position_details.get("unrealizedPnl")
            or position_details.get("pnl")
        )

    current_size = to_float(position_details.get("size")) or 0.0
    prev_snapshot = get_prev_position_snapshot(category, symbol)
    prev_size = to_float(prev_snapshot.get("size")) or 0.0
    prev_entry = to_float(prev_snapshot.get("avgPrice"))

    reduce_only = str(exec_evt.get("reduceOnly", order_details.get("reduceOnly", ""))).lower() in ("1", "true", "t", "yes")
    title = resolve_event_title(side, reduce_only, prev_size, current_size)

    entry_price_for_rr = to_float(avg_fill_price)
    if title in ("Частичное закрытие", "Закрытие позиции") and prev_entry is not None:
        entry_price_for_rr = prev_entry

    rr_ratio = calc_rr(side, entry_price_for_rr, stop_loss, take_profit)

    rsi = rest.get_rsi_4h(category, symbol, length=14)
    rsi_str = fmt_num(rsi, 2) if rsi is not None else "n/a"

    bybit_link = rest.make_bybit_link(category, symbol)
    tv_link = rest.make_tv_link(symbol)

    lines = [
        f"🔔 <b>{title}</b>",
        "",
        f"<b>Биржа:</b> Bybit",
        f"<b>Тип рынка:</b> {market_type}",
        f"<b>Инструмент:</b> {symbol}",
        f"<b>Сторона:</b> {side}",
        f"<b>Тип ордера:</b> {order_type}",
        f"<b>Статус:</b> {order_status}",
        "",
        f"<b>Средняя цена исполнения:</b> {fmt_num(avg_fill_price)}",
        f"<b>Суммарный объем:</b> {fmt_num(filled_qty)}",
        f"<b>Сумма:</b> {fmt_num(filled_notional)}",
        f"<b>Суммарная комиссия:</b> {fmt_num(fee)} {fee_coin}",
    ]

    if exec_count > 1:
        lines.append(f"<b>Количество исполнений:</b> {exec_count}")

    if title == "Открыта позиция" and stop_loss is not None and take_profit is not None:
        lines.append(f"<b>SL:</b> {fmt_num(stop_loss)}")
        lines.append(f"<b>TP:</b> {fmt_num(take_profit)}")

    if title in ("Частичное закрытие", "Закрытие позиции"):
        exit_price = to_float(avg_fill_price)
        position_direction = prev_snapshot.get("side") or ("buy" if str(side).lower() == "sell" else "sell")
        change_pct = calc_change_percent(str(position_direction), prev_entry, exit_price)
        if prev_entry is not None:
            lines.append(f"<b>Цена входа:</b> {fmt_num(prev_entry)}")
        if exit_price is not None:
            lines.append(f"<b>Цена выхода:</b> {fmt_num(exit_price)}")
        if change_pct is not None:
            lines.append(f"<b>Соотношение (%):</b> {fmt_num(change_pct, 2)}%")

    if realized_pnl is not None:
        lines.append(f"<b>Реализованный PnL:</b> {fmt_num(realized_pnl)} {pnl_coin}")
    if current_pnl is not None:
        lines.append(f"<b>Текущий PnL:</b> {fmt_num(current_pnl)} {pnl_coin}")

    if title != "Открыта позиция":
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

    update_position_snapshot(category, symbol, position_details)

    return "\n".join(lines)


# ==========================
# Funding Monitor
# ==========================
funding_state = {
    "next_time": {}, # symbol -> nextFundingTime
    "anomaly": {}    # symbol -> timestamp последнего алерта аномалии
}


def resolve_funding_total(
    rest: BybitRest,
    symbol: str,
    side: str,
    position_created_time: str,
) -> Optional[Dict[str, Any]]:
    executor = cf.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(
        rest.get_position_funding_total,
        "linear",
        symbol,
        side,
        position_created_time,
    )
    try:
        funding_total = future.result(timeout=FUNDING_LOOKUP_TIMEOUT_SEC)
    except cf.TimeoutError:
        future.cancel()
        log.warning(
            "Funding total lookup timed out for symbol=%s side=%s after %ss",
            symbol,
            side,
            FUNDING_LOOKUP_TIMEOUT_SEC,
        )
        return None
    except Exception as e:
        log.warning("Funding total lookup failed for symbol=%s side=%s: %s", symbol, side, e)
        return None
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    return funding_total


def resolve_funding_total_text(
    rest: BybitRest,
    symbol: str,
    side: str,
    position_created_time: str,
) -> str:
    funding_total = resolve_funding_total(rest, symbol, side, position_created_time)
    return format_funding_total_text(funding_total)


def format_funding_total_text(funding_total: Optional[Dict[str, Any]]) -> str:
    if funding_total is None:
        return "n/a"

    funding_value = to_float(funding_total.get("funding")) or 0.0
    funding_currency = str(funding_total.get("currency") or "USDT")
    funding_direction = "получено" if funding_value > 0 else "выплачено" if funding_value < 0 else "0"
    funding_total_str = f"{'+' if funding_value > 0 else ''}{fmt_num(funding_value, 4)} {funding_currency}"
    if funding_direction != "0":
        funding_total_str = f"{funding_total_str} ({funding_direction})"
    return funding_total_str

def send_funding_alert(
    title: str,
    symbol: str,
    side: str,
    size: float,
    mark_price: float,
    funding_rate: float,
    next_funding_time: int,
    rest: BybitRest,
    position_created_time: str = "",
):
    side_ru = "Long" if side.lower() == "buy" else "Short"
    rate_pct = funding_rate * 100

    # Кто кому платит
    if funding_rate > 0:
        pays_text = "Лонги платят Шортам"
    elif funding_rate < 0:
        pays_text = "Шорты платят Лонгам"
    else:
        pays_text = "Никто никому не платит (0%)"

    # Прогноз выплаты/получения
    forecast_value = size * mark_price * abs(funding_rate)
    will_pay = False
    if side.lower() == "buy" and funding_rate > 0:
        will_pay = True
    elif side.lower() == "sell" and funding_rate < 0:
        will_pay = True

    action_text = "Выплата" if will_pay else "Получение"
    sign_text = "-" if will_pay else "+"

    # Таймер до фандинга
    now_ms = int(time.time() * 1000)
    time_diff = next_funding_time - now_ms
    if time_diff < 0: time_diff = 0
    hours = time_diff // 3600000
    mins = (time_diff % 3600000) // 60000
    secs = (time_diff % 60000) // 1000
    timer_text = f"{hours:02d}:{mins:02d}:{secs:02d}"

    funding_total_str = resolve_funding_total_text(rest, symbol, side, position_created_time)

    lines = [
        f"⏱ <b>{title}</b>",
        "",
        f"<b>Инструмент:</b> {symbol} ({side_ru})",
        f"<b>Текущая ставка фандинга:</b> {fmt_num(rate_pct, 4)}%",
        f"<b>Кто кому платит:</b> {pays_text}",
        f"<b>Прогноз ({action_text}):</b> {sign_text}{fmt_num(forecast_value, 4)} USDT",
        f"<b>До фандинга:</b> {timer_text}",
        f"<b>Суммарный funding по позиции:</b> {funding_total_str}",
    ]
    tg_send_message("\n".join(lines))


def funding_monitor_loop(rest: BybitRest):
    """Фоновый поток для проверки фандинга каждые 2 минуты (120 секунд)"""
    while True:
        try:
            now = time.time()
            now_ms = int(now * 1000)

            positions = rest.get_all_open_positions("linear")

            for pos in positions:
                symbol = pos.get("symbol")
                side = pos.get("side") # Buy или Sell
                size = to_float(pos.get("size")) or 0.0

                if not symbol or size <= 0:
                    continue

                ticker = rest.get_ticker_info("linear", symbol)
                if not ticker:
                    continue

                funding_rate = to_float(ticker.get("fundingRate")) or 0.0
                next_time = int(ticker.get("nextFundingTime") or 0)
                mark_price = to_float(ticker.get("markPrice")) or 0.0

                if next_time > 0:
                    funding_state.setdefault("next_time", {})[symbol] = next_time

                target_time = funding_state["next_time"].get(symbol, next_time)
                time_to_funding_ms = target_time - now_ms

                # 1. Проверка аномалии (например, > 0.1% за период)
                if abs(funding_rate) >= 0.001: 
                    last_anomaly = funding_state.get("anomaly", {}).get(symbol, 0)
                    if now - last_anomaly > 1800: # 1800 секунд = 30 минут
                        send_funding_alert(
                            "🚨 Аномальный фандинг!",
                            symbol,
                            side,
                            size,
                            mark_price,
                            funding_rate,
                            target_time,
                            rest,
                            position_created_time=str(pos.get("createdTime") or pos.get("updatedTime") or ""),
                        )
                        funding_state.setdefault("anomaly", {})[symbol] = now

                # 2. Таймер: За 3 минуты до наступления (180 000 мс)
                if 0 < time_to_funding_ms <= 180_000:
                    key = f"pre_{symbol}_{target_time}"
                    if not funding_state.get(key):
                        send_funding_alert(
                            "⏳ Скоро списание фандинга (3 мин)",
                            symbol,
                            side,
                            size,
                            mark_price,
                            funding_rate,
                            target_time,
                            rest,
                            position_created_time=str(pos.get("createdTime") or pos.get("updatedTime") or ""),
                        )
                        funding_state[key] = True

                # 3. Таймер: Через 1-5 минут после наступления (чтобы с интервалом в 2 минуты гарантированно поймать)
                if -300_000 < time_to_funding_ms <= -60_000: 
                    key = f"post_{symbol}_{target_time}"
                    if not funding_state.get(key):
                        send_funding_alert(
                            "💸 Фактический отчет по фандингу",
                            symbol,
                            side,
                            size,
                            mark_price,
                            funding_rate,
                            target_time,
                            rest,
                            position_created_time=str(pos.get("createdTime") or pos.get("updatedTime") or ""),
                        )
                        funding_state[key] = True

        except Exception as e:
            log.exception(f"Funding monitor error: {e}")

        # Сон 120 секунд (2 минуты)
        time.sleep(120)


# ==========================
# Positions Command
# ==========================
def is_positions_command(text: str) -> bool:
    normalized = (text or "").strip().split()[0].lower() if (text or "").strip() else ""
    return normalized == "/positions" or normalized.startswith("/positions@")


def should_handle_command_message(message: Dict[str, Any]) -> bool:
    chat = message.get("chat") or {}
    chat_id = str(chat.get("id") or "")
    chat_type = str(chat.get("type") or "")

    if chat_type == "private":
        pass
    else:
        if TG_CHAT_ID and chat_id != TG_CHAT_ID:
            return False

        if TG_THREAD_ID:
            thread_id = str(message.get("message_thread_id") or "")
            if thread_id != TG_THREAD_ID:
                return False

    from_user = message.get("from") or {}
    if from_user.get("is_bot"):
        return False

    if TG_ALLOWED_USER_ID and str(from_user.get("id") or "") != TG_ALLOWED_USER_ID:
        return False

    return is_positions_command(str(message.get("text") or ""))


def format_position_age(position: Dict[str, Any]) -> str:
    opened_at_ms = safe_int(position.get("createdTime") or position.get("updatedTime"))
    if opened_at_ms <= 0:
        return "n/a"
    local_dt, utc_off = utc_to_local(opened_at_ms, UTC_OFFSET_HOURS)
    age = format_duration((time.time() * 1000 - opened_at_ms) / 1000.0)
    return f"{local_dt} ({utc_off}) | {age}"


def calc_liq_distance_percent(current_price: Optional[float], liq_price: Optional[float]) -> Optional[float]:
    if current_price is None or liq_price is None or current_price <= 0 or liq_price <= 0:
        return None
    return abs(current_price - liq_price) / current_price * 100.0


def calc_position_pnl_percent(entry_price: Optional[float], size: float, pnl_value: Optional[float]) -> Optional[float]:
    if entry_price is None or entry_price <= 0 or size <= 0 or pnl_value is None:
        return None
    notional = entry_price * size
    if notional <= 0:
        return None
    return (pnl_value / notional) * 100.0


def build_indicator_snapshot(rest: BybitRest, category: str, symbol: str, current_price: Optional[float]) -> Dict[str, Any]:
    intervals = {
        "24h": "D",
        "4h": "240",
        "1h": "60",
        "15m": "15",
    }
    data: Dict[str, Any] = {
        "volume_change": {},
        "rsi": {},
        "atr": {},
        "ema_24h": {"ema20": None, "ema50": None, "position": "n/a"},
        "open_interest": rest.get_open_interest_value(category, symbol),
    }

    completed_by_label: Dict[str, List[Dict[str, float]]] = {}
    for label, interval in intervals.items():
        rows = rest.get_kline_rows(category, symbol, interval=interval, limit=250)
        completed = get_completed_klines(rows, interval)
        completed_by_label[label] = completed
        closes = [row["close"] for row in completed]

        data["volume_change"][label] = calc_volume_change_percent(completed)
        data["rsi"][label] = calc_rsi_from_closes(closes, length=14)

        if label != "24h":
            data["atr"][label] = calc_atr_from_rows(completed, length=14)

    daily_closes = [row["close"] for row in completed_by_label["24h"]]
    ema20 = calc_ema_from_closes(daily_closes, 20)
    ema50 = calc_ema_from_closes(daily_closes, 50)
    data["ema_24h"] = {
        "ema20": ema20,
        "ema50": ema50,
        "position": resolve_price_vs_ema_band(current_price, ema20, ema50),
    }
    return data


def build_position_message(position: Dict[str, Any], rest: BybitRest) -> str:
    category = str(position.get("category") or "linear").lower()
    symbol = str(position.get("symbol") or "n/a")
    side = str(position.get("side") or "")
    direction = "Long" if side.lower() == "buy" else "Short"
    size = to_float(position.get("size")) or 0.0
    entry_price = (
        to_float(position.get("avgPrice"))
        or to_float(position.get("avgEntryPrice"))
        or to_float(position.get("entryPrice"))
    )
    current_price = (
        to_float(position.get("markPrice"))
        or rest.get_current_price(category, symbol)
    )
    leverage = to_float(position.get("leverage"))
    liq_price = to_float(position.get("liqPrice"))
    unrealized_pnl = to_float(position.get("unrealisedPnl"))
    if unrealized_pnl is None:
        unrealized_pnl = to_float(position.get("unrealizedPnl"))
    position_created_time = str(position.get("createdTime") or position.get("updatedTime") or "")
    funding_total = resolve_funding_total(rest, symbol, side, position_created_time)
    funding_value = to_float((funding_total or {}).get("funding")) or 0.0
    pnl_with_funding = (unrealized_pnl or 0.0) + funding_value
    pnl_pct = calc_position_pnl_percent(entry_price, size, pnl_with_funding)
    liq_distance_pct = calc_liq_distance_percent(current_price, liq_price)

    indicators = build_indicator_snapshot(rest, category, symbol, current_price)
    ema_24h = indicators["ema_24h"]
    bybit_link = rest.make_bybit_link(category, symbol)
    tv_link = rest.make_tv_link(symbol)

    lines = [
        f"📌 <b>Открытая позиция: {symbol}</b>",
        "",
        f"<b>Символ:</b> {symbol}",
        f"<b>Side:</b> {direction}",
        f"<b>Размер позиции:</b> {fmt_num(size)}",
        f"<b>Средняя цена входа:</b> {fmt_num(entry_price)}",
        f"<b>Текущая цена:</b> {fmt_num(current_price)}",
        f"<b>Unrealized PnL + funding:</b> {fmt_num(pnl_with_funding, 4)} USDT",
        f"<b>PnL (%):</b> {format_signed_percent(pnl_pct)}",
        f"<b>Leverage:</b> {fmt_num(leverage, 2)}x" if leverage is not None else "<b>Leverage:</b> n/a",
        f"<b>До ликвидации:</b> {fmt_num(liq_distance_pct, 2)}%" if liq_distance_pct is not None else "<b>До ликвидации:</b> n/a",
        f"<b>Время жизни позиции:</b> {format_position_age(position)}",
        f"<b>Funding по позиции:</b> {format_funding_total_text(funding_total)}",
        "",
        (
            "<b>Изменение объема:</b> "
            f"24h {format_signed_percent(indicators['volume_change'].get('24h'))} | "
            f"4h {format_signed_percent(indicators['volume_change'].get('4h'))} | "
            f"1h {format_signed_percent(indicators['volume_change'].get('1h'))} | "
            f"15m {format_signed_percent(indicators['volume_change'].get('15m'))}"
        ),
        (
            "<b>RSI:</b> "
            f"24h {fmt_num(indicators['rsi'].get('24h'), 2) if indicators['rsi'].get('24h') is not None else 'n/a'} | "
            f"4h {fmt_num(indicators['rsi'].get('4h'), 2) if indicators['rsi'].get('4h') is not None else 'n/a'} | "
            f"1h {fmt_num(indicators['rsi'].get('1h'), 2) if indicators['rsi'].get('1h') is not None else 'n/a'} | "
            f"15m {fmt_num(indicators['rsi'].get('15m'), 2) if indicators['rsi'].get('15m') is not None else 'n/a'}"
        ),
        (
            f"<b>EMA20/EMA50 (24h):</b> {fmt_num(ema_24h.get('ema20'))} / {fmt_num(ema_24h.get('ema50'))} | "
            f"цена {ema_24h.get('position')}"
        ),
        (
            "<b>ATR:</b> "
            f"4h {fmt_num(indicators['atr'].get('4h'), 4) if indicators['atr'].get('4h') is not None else 'n/a'} | "
            f"1h {fmt_num(indicators['atr'].get('1h'), 4) if indicators['atr'].get('1h') is not None else 'n/a'} | "
            f"15m {fmt_num(indicators['atr'].get('15m'), 4) if indicators['atr'].get('15m') is not None else 'n/a'}"
        ),
        f"<b>Open interest:</b> {fmt_num(indicators.get('open_interest'), 2) if indicators.get('open_interest') is not None else 'n/a'}",
        "",
        f"🔗 <a href='{bybit_link}'>Bybit</a> | <a href='{tv_link}'>TradingView</a>",
    ]
    return "\n".join(lines)


def send_positions_report(message: Dict[str, Any], rest: BybitRest) -> None:
    chat_id = str((message.get("chat") or {}).get("id") or TG_CHAT_ID)
    thread_id = str(message.get("message_thread_id") or "") if message.get("message_thread_id") is not None else None
    reply_to_message_id = safe_int(message.get("message_id"))

    positions = rest.get_all_open_positions("linear")
    if not positions:
        tg_send_message(
            "Открытых позиций сейчас нет.",
            chat_id=chat_id,
            thread_id=thread_id,
            reply_to_message_id=reply_to_message_id,
        )
        return

    positions_sorted = sorted(positions, key=lambda p: safe_int(p.get("createdTime") or p.get("updatedTime")))
    for position in positions_sorted:
        tg_send_message(
            build_position_message(position, rest),
            chat_id=chat_id,
            thread_id=thread_id,
            reply_to_message_id=reply_to_message_id,
        )


def telegram_command_loop(rest: BybitRest) -> None:
    offset: Optional[int] = None
    try:
        initial_updates = tg_get_updates(timeout_sec=1)
        if initial_updates:
            offset = max(safe_int(item.get("update_id")) for item in initial_updates) + 1
    except Exception as e:
        log.warning(f"Failed to initialize Telegram command offset: {e}")

    while True:
        try:
            updates = tg_get_updates(offset=offset, timeout_sec=TG_COMMAND_POLL_TIMEOUT_SEC)
            for update in updates:
                offset = safe_int(update.get("update_id")) + 1
                message = update.get("message") or {}
                if not should_handle_command_message(message):
                    continue
                try:
                    send_positions_report(message, rest)
                    log.info("Processed Telegram /positions command chatId=%s", (message.get("chat") or {}).get("id"))
                except Exception as e:
                    log.exception(f"Failed handling /positions command: {e}")
                    tg_send_message(
                        "Не удалось собрать snapshot по позициям. Попробуй еще раз через минуту.",
                        chat_id=str((message.get("chat") or {}).get("id") or TG_CHAT_ID),
                        thread_id=str(message.get("message_thread_id") or "") if message.get("message_thread_id") is not None else None,
                        reply_to_message_id=safe_int(message.get("message_id")),
                    )
        except Exception as e:
            log.exception(f"Telegram command loop error: {e}")
            time.sleep(TG_COMMAND_ERROR_SLEEP_SEC)


# ==========================
# Limit Order Monitor
# ==========================
limit_order_alert_state_lock = threading.Lock()
limit_order_alert_state: Dict[str, Tuple[str, str, str, str]] = {}


def map_side_to_direction(side: str) -> str:
    return "Long" if str(side).lower() == "buy" else "Short"


def calc_limit_distance_percent(limit_price: Optional[float], current_price: Optional[float]) -> Optional[float]:
    if limit_price is None or current_price is None or limit_price <= 0:
        return None
    return abs(current_price - limit_price) / limit_price * 100.0


def build_limit_order_alert_message(
    order: Dict[str, Any],
    current_price: float,
    distance_pct: float,
    rsi_1h: Optional[float],
    rest: BybitRest,
) -> str:
    category = str(order.get("category") or "")
    symbol = str(order.get("symbol") or "n/a")
    side = str(order.get("side") or "")
    market_type = map_market_type(category)
    direction = map_side_to_direction(side)

    limit_price = to_float(order.get("price") or order.get("orderPrice"))
    qty = to_float(order.get("leavesQty"))
    if qty is None:
        qty = to_float(order.get("qty") or order.get("orderQty")) or 0.0
    notional_usdt = (limit_price or 0.0) * qty
    rsi_str = fmt_num(rsi_1h, 2) if rsi_1h is not None else "n/a"

    bybit_link = rest.make_bybit_link(category, symbol)
    tv_link = rest.make_tv_link(symbol)

    return "\n".join([
        "🎯 <b>Лимитная заявка рядом с рынком</b>",
        "",
        "<b>Биржа:</b> Bybit",
        f"<b>Рынок:</b> {market_type}",
        f"<b>Тикер:</b> {symbol}",
        f"<b>Направление:</b> {direction}",
        f"<b>Цена лимитки:</b> {fmt_num(limit_price)}",
        f"<b>Текущая цена:</b> {fmt_num(current_price)}",
        f"<b>Остаток лотов:</b> {fmt_num(qty)}",
        f"<b>Остаток в USDT:</b> {fmt_num(notional_usdt, 2)} USDT",
        f"<b>До лимитки:</b> {fmt_num(distance_pct, 2)}%",
        f"<b>RSI (1H):</b> {rsi_str}",
        "",
        f"🔗 <a href='{bybit_link}'>Bybit</a> | <a href='{tv_link}'>TradingView</a>",
    ])


def should_send_limit_order_alert(order: Dict[str, Any], signature: Tuple[str, str, str, str]) -> bool:
    order_id = str(order.get("orderId") or "")
    if not order_id:
        return False
    with limit_order_alert_state_lock:
        previous_signature = limit_order_alert_state.get(order_id)
        if previous_signature == signature:
            return False
        limit_order_alert_state[order_id] = signature
        return True


def cleanup_limit_order_alert_state(active_order_ids: Set[str]) -> None:
    with limit_order_alert_state_lock:
        stale_ids = [order_id for order_id in limit_order_alert_state if order_id not in active_order_ids]
        for order_id in stale_ids:
            limit_order_alert_state.pop(order_id, None)


def limit_order_monitor_loop(rest: BybitRest) -> None:
    while True:
        active_order_ids: Set[str] = set()
        try:
            orders = rest.get_all_open_limit_orders(LIMIT_ORDER_ALERT_CATEGORIES)
            price_cache: Dict[Tuple[str, str], Optional[float]] = {}
            rsi_cache: Dict[Tuple[str, str], Optional[float]] = {}

            for order in orders:
                order_id = str(order.get("orderId") or "")
                if not order_id:
                    continue
                active_order_ids.add(order_id)

                category = str(order.get("category") or "").lower()
                symbol = str(order.get("symbol") or "")
                limit_price = to_float(order.get("price") or order.get("orderPrice"))
                if not category or not symbol or limit_price is None or limit_price <= 0:
                    continue

                cache_key = (category, symbol)
                if cache_key not in price_cache:
                    price_cache[cache_key] = rest.get_current_price(category, symbol)
                current_price = price_cache[cache_key]
                distance_pct = calc_limit_distance_percent(limit_price, current_price)
                if current_price is None or distance_pct is None:
                    continue

                if distance_pct > LIMIT_ORDER_ALERT_DISTANCE_PCT:
                    with limit_order_alert_state_lock:
                        limit_order_alert_state.pop(order_id, None)
                    continue

                if cache_key not in rsi_cache:
                    rsi_cache[cache_key] = rest.get_rsi_1h(category, symbol, length=14)
                rsi_1h = rsi_cache[cache_key]

                qty = to_float(order.get("leavesQty"))
                if qty is None:
                    qty = to_float(order.get("qty") or order.get("orderQty")) or 0.0

                signature = (
                    fmt_num(limit_price, 8),
                    fmt_num(qty, 8),
                    str(order.get("side") or ""),
                    str(order.get("updatedTime") or order.get("createdTime") or ""),
                )
                if not should_send_limit_order_alert(order, signature):
                    continue

                tg_send_message(build_limit_order_alert_message(order, current_price, distance_pct, rsi_1h, rest))
                log.info(
                    "Sent limit order alert orderId=%s symbol=%s distancePct=%s",
                    order_id,
                    symbol,
                    fmt_num(distance_pct, 2),
                )
        except Exception as e:
            log.exception(f"Limit order monitor error: {e}")
        finally:
            cleanup_limit_order_alert_state(active_order_ids)
            time.sleep(LIMIT_ORDER_ALERT_INTERVAL_SEC)

# ==========================
# Aggregation
# ==========================
@dataclass
class AggregatedOrder:
    key: str
    category: str
    symbol: str
    side: str
    order_type: str
    order_status: str
    order_id: str
    fee_coin: str
    pnl_coin: str
    total_qty: float = 0.0
    total_notional: float = 0.0
    total_fee: float = 0.0
    total_realized_pnl: float = 0.0
    has_realized_pnl: bool = False
    current_pnl: Optional[float] = None
    first_ts_ms: int = 0
    last_ts_ms: int = 0
    last_exec_id: str = ""
    exec_ids: List[str] = field(default_factory=list)


class ExecutionAggregator:
    def __init__(self, window_sec: int):
        self.window_sec = max(1, window_sec)
        self._lock = threading.Lock()
        self._execid_seen: Set[str] = set()
        self._pending_orders: Dict[str, AggregatedOrder] = {}

    @staticmethod
    def _order_key(evt: Dict[str, Any]) -> Optional[str]:
        order_id = str(evt.get("orderId", evt.get("order_id", "")) or "")
        if order_id:
            return f"order:{order_id}"
        return None

    @staticmethod
    def _is_trade(evt: Dict[str, Any]) -> bool:
        exec_type = (evt.get("execType") or evt.get("exec_type") or "").lower()
        return not exec_type or exec_type == "trade"

    @staticmethod
    def _should_flush(evt: Dict[str, Any]) -> bool:
        status = str(evt.get("orderStatus", evt.get("order_status", "")) or "").lower()
        if status in {"filled", "cancelled", "rejected", "deactivated", "partiallyfilledcanceled"}:
            return True

        leaves_qty = to_float(evt.get("leavesQty") or evt.get("leaves_qty"))
        if leaves_qty is not None and leaves_qty <= 0:
            return True

        cum_exec_qty = to_float(evt.get("cumExecQty") or evt.get("cum_exec_qty"))
        order_qty = to_float(evt.get("orderQty") or evt.get("order_qty"))
        if cum_exec_qty is not None and order_qty is not None and order_qty > 0 and cum_exec_qty >= order_qty:
            return True

        return False

    @staticmethod
    def _build_event_from_agg(agg: AggregatedOrder) -> Dict[str, Any]:
        avg_price = agg.total_notional / agg.total_qty if agg.total_qty > 0 else 0.0
        return {
            "category": agg.category,
            "symbol": agg.symbol,
            "side": agg.side,
            "orderType": agg.order_type,
            "orderStatus": agg.order_status,
            "orderId": agg.order_id,
            "execId": agg.last_exec_id,
            "execTime": agg.last_ts_ms,
            "execPrice": avg_price,
            "execQty": agg.total_qty,
            "execValue": agg.total_notional,
            "execFee": agg.total_fee,
            "feeCurrency": agg.fee_coin,
            "pnlCurrency": agg.pnl_coin,
            "aggregatedExecCount": len(agg.exec_ids),
            "realizedPnl": agg.total_realized_pnl if agg.has_realized_pnl else None,
            "pnl": agg.current_pnl,
        }

    def _trim_exec_cache(self) -> None:
        if len(self._execid_seen) <= EXECID_CACHE_MAX:
            return
        self._execid_seen = set(list(self._execid_seen)[-EXECID_CACHE_MAX:])

    def _apply_execution(self, evt: Dict[str, Any], key: str) -> AggregatedOrder:
        ts_ms = int(evt.get("execTime", evt.get("ts", 0)) or 0)
        agg = self._pending_orders.get(key)

        if agg is None:
            agg = AggregatedOrder(
                key=key,
                category=str(evt.get("category", evt.get("categoryType", "")) or ""),
                symbol=str(evt.get("symbol", "—")),
                side=str(evt.get("side", "—")),
                order_type=str(evt.get("orderType", evt.get("order_type", "—"))),
                order_status=str(evt.get("orderStatus", evt.get("order_status", "—"))),
                order_id=str(evt.get("orderId", evt.get("order_id", "")) or ""),
                fee_coin=str(evt.get("feeCurrency", evt.get("feeCoin", "—"))),
                pnl_coin=str(evt.get("pnlCurrency", evt.get("profitCurrency", "USDT"))),
                first_ts_ms=ts_ms,
                last_ts_ms=ts_ms,
            )
            self._pending_orders[key] = agg

        qty = to_float(evt.get("execQty", evt.get("qty", 0))) or 0.0
        notional = to_float(evt.get("execValue", evt.get("value", 0)))
        exec_price = to_float(evt.get("execPrice", evt.get("price", 0))) or 0.0
        fee = to_float(evt.get("execFee")) or 0.0

        if notional is None:
            notional = exec_price * qty

        realized = to_float(evt.get("execPnl") or evt.get("realizedPnl") or evt.get("closedPnl"))
        current_pnl = to_float(
            evt.get("pnl")
            or evt.get("unrealizedPnl")
            or evt.get("unrealisedPnl")
            or evt.get("cumPnl")
            or evt.get("positionPnl")
        )

        agg.total_qty += qty
        agg.total_notional += notional
        agg.total_fee += fee
        if realized is not None:
            agg.has_realized_pnl = True
            agg.total_realized_pnl += realized
        if current_pnl is not None:
            agg.current_pnl = current_pnl

        agg.last_ts_ms = ts_ms
        agg.last_exec_id = str(evt.get("execId", evt.get("exec_id", "—")))
        agg.exec_ids.append(agg.last_exec_id)

        status = str(evt.get("orderStatus", evt.get("order_status", "")) or "")
        if status:
            agg.order_status = status

        return agg

    def process_ws_message(self, message: Dict[str, Any]) -> List[Dict[str, Any]]:
        ready_events: List[Dict[str, Any]] = []
        data = message.get("data") or []
        if not isinstance(data, list):
            return ready_events

        with self._lock:
            for evt in data:
                if not self._is_trade(evt):
                    continue

                exec_id = str(evt.get("execId", evt.get("exec_id", "")) or "")
                if not exec_id:
                    continue

                if exec_id in self._execid_seen:
                    continue

                self._execid_seen.add(exec_id)
                self._trim_exec_cache()

                key = self._order_key(evt)
                if not key:
                    ready_events.append(evt)
                    continue

                agg = self._apply_execution(evt, key)
                if self._should_flush(evt):
                    self._pending_orders.pop(key, None)
                    ready_events.append(self._build_event_from_agg(agg))

        return ready_events

    def flush_due(self) -> List[Dict[str, Any]]:
        now = time.time()
        ready_events: List[Dict[str, Any]] = []

        with self._lock:
            due_keys = [
                key for key, agg in self._pending_orders.items()
                if agg.last_ts_ms and (now - agg.last_ts_ms / 1000.0) >= self.window_sec
            ]
            for key in due_keys:
                agg = self._pending_orders.pop(key, None)
                if not agg:
                    continue
                ready_events.append(self._build_event_from_agg(agg))

        return ready_events


# ==========================
# WS handler
# ==========================
aggregator = ExecutionAggregator(window_sec=ORDER_AGGREGATION_WINDOW_SEC)


def send_event_message(evt: Dict[str, Any], rest: BybitRest, reason: str) -> None:
    text = build_message(evt, rest)
    tg_send_message(text)
    log.info(
        "Sent aggregated order reason=%s orderId=%s executions=%s",
        reason,
        evt.get("orderId", ""),
        evt.get("aggregatedExecCount", 1),
    )


def on_execution_message(message: Dict[str, Any], rest: BybitRest) -> None:
    try:
        for evt in aggregator.process_ws_message(message):
            send_event_message(evt, rest, reason="event")
    except Exception as e:
        log.exception(f"Failed processing WS message: {e}")


def flush_loop(rest: BybitRest) -> None:
    while True:
        try:
            for evt in aggregator.flush_due():
                send_event_message(evt, rest, reason="timeout")
        except Exception as e:
            log.exception(f"Failed flushing aggregated orders: {e}")
        time.sleep(1)


def main() -> None:
    require_env("BYBIT_API_KEY", BYBIT_API_KEY)
    require_env("BYBIT_API_SECRET", BYBIT_API_SECRET)
    require_env("TELEGRAM_BOT_TOKEN", TG_TOKEN)
    require_env("TELEGRAM_CHAT_ID", TG_CHAT_ID)

    print("BOT VERSION: " + BOT_VERSION)
    log.info("Starting Bybit WS execution listener...")
    log.info("Testnet: %s", BYBIT_TESTNET)

    rest = BybitRest(testnet=BYBIT_TESTNET)

    flush_thread = threading.Thread(target=flush_loop, args=(rest,), daemon=True, name="agg-flush-loop")
    flush_thread.start()

    funding_thread = threading.Thread(target=funding_monitor_loop, args=(rest,), daemon=True, name="funding-monitor-loop")
    funding_thread.start()

    limit_order_thread = threading.Thread(target=limit_order_monitor_loop, args=(rest,), daemon=True, name="limit-order-monitor-loop")
    limit_order_thread.start()

    telegram_command_thread = threading.Thread(target=telegram_command_loop, args=(rest,), daemon=True, name="telegram-command-loop")
    telegram_command_thread.start()

    ws = WebSocket(
        testnet=BYBIT_TESTNET,
        channel_type="private",
        api_key=BYBIT_API_KEY,
        api_secret=BYBIT_API_SECRET,
        private_auth_expire=BYBIT_WS_AUTH_EXPIRE_SEC,
    )

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
