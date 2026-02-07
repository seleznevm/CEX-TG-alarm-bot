import os
import time
import math
import datetime as dt
import logging
from typing import Any, Dict, Optional, Tuple

import requests
from dotenv import load_dotenv
from pybit.unified_trading import HTTP

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
EXECID_CACHE_MAX = int(os.environ.get("EXECID_CACHE_MAX", "5000"))

# WS reconnect tuning
WS_PING_INTERVAL_SEC = int(os.environ.get("WS_PING_INTERVAL_SEC", "20"))
WS_PING_TIMEOUT_SEC = int(os.environ.get("WS_PING_TIMEOUT_SEC", "10"))
WS_RECONNECT_MAX_SLEEP_SEC = int(os.environ.get("WS_RECONNECT_MAX_SLEEP_SEC", "60"))

# REST cache tuning
ORDER_CACHE_MAX = int(os.environ.get("ORDER_CACHE_MAX", "3000"))
ORDER_CACHE_TTL_SEC = int(os.environ.get("ORDER_CACHE_TTL_SEC", "3600"))

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
    """
    R:R = Reward / Risk
    Long: reward = tp-entry, risk = entry-sl
    Short: reward = entry-tp, risk = sl-entry
    """
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
        # order cache: orderId -> (ts, details)
        self._order_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}

    def _cache_get(self, order_id: str) -> Optional[Dict[str, Any]]:
        item = self._order_cache.get(order_id)
        if not item:
            return None
        ts, data = item
        if time.time() - ts > ORDER_CACHE_TTL_SEC:
            self._order_cache.pop(order_id, None)
            return None
        return data

    def _cache_put(self, order_id: str, data: Dict[str, Any]) -> None:
        self._order_cache[order_id] = (time.time(), data)
        if len(self._order_cache) > ORDER_CACHE_MAX:
            items = sorted(self._order_cache.items(), key=lambda kv: kv[1][0])
            for k, _ in items[: max(1, len(items) - ORDER_CACHE_MAX)]:
                self._order_cache.pop(k, None)

    def get_position_info(self, category: str, symbol: str) -> Dict[str, Any]:
        c = (category or "").lower()
        if c not in ("linear", "inverse"):
            return {}
        resp = self.http.get_positions(category=c, symbol=symbol)
        lst = (((resp or {}).get("result") or {}).get("list") or [])
        return lst[0] if lst else {}

    def get_order_details(self, category: str, symbol: str, order_id: str) -> Dict[str, Any]:
        """
        Пытаемся вытащить stopLoss/takeProfit из истории ордера.
        """
        if not order_id:
            return {}

        cached = self._cache_get(order_id)
        if cached is not None:
            return cached

        c = (category or "").lower() or "linear"
        try:
            resp = self.http.get_order_history(category=c, symbol=symbol, orderId=order_id, limit=50)
            lst = (((resp or {}).get("result") or {}).get("list") or [])
            if lst:
                self._cache_put(order_id, lst[0])
                return lst[0]
        except (KeyError, ValueError, TypeError) as e:
            log.warning(f"Error getting order details with orderId: {e}")

        # fallback: без orderId (дороже)
        try:
            resp = self.http.get_order_history(category=c, symbol=symbol, limit=50)
            lst = (((resp or {}).get("result") or {}).get("list") or [])
            for it in lst:
                if str(it.get("orderId", "")) == str(order_id):
                    self._cache_put(order_id, it)
                    return it
        except (KeyError, ValueError, TypeError) as e:
            log.warning(f"Error getting order details (fallback): {e}")

        self._cache_put(order_id, {})
        return {}

    def get_rsi_4h(self, category: str, symbol: str, length: int = 14) -> Optional[float]:
        c = (category or "").lower()
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
        except (KeyError, ValueError, TypeError, IndexError) as e:
            log.warning(f"Error calculating RSI: {e}")
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
# Message builder (SL/TP/RR fixed)
# ==========================
def build_message(exec_evt: Dict[str, Any], rest: BybitRest) -> Optional[str]:
    """
    Строит сообщение для Telegram на основе события исполнения ордера.

    Args:
        exec_evt: Событие исполнения от Bybit
        rest: Экземпляр BybitRest для получения дополнительных данных

    Returns:
        Отформатированное сообщение или None если не удалось построить
    """
    category = exec_evt.get("category", "") or exec_evt.get("categoryType", "") or ""
    market_type = map_market_type(category)

    symbol = exec_evt.get("symbol", "—")
    side = exec_evt.get("side", "—")
    order_type = exec_evt.get("orderType", exec_evt.get("order_type", "—"))
    order_status = exec_evt.get("orderStatus", exec_evt.get("order_status", "—"))

    exec_id = exec_evt.get("execId", exec_evt.get("exec_id", "—"))
    order_id = str(exec_evt.get("orderId", exec_evt.get("order_id", "")) or "")

    avg_fill_price = exec_evt.get("execPrice", exec_evt.get("price", "—"))
    filled_qty = exec_evt.get("execQty", exec_evt.get("qty", "—"))
    filled_notional = exec_evt.get("execValue", exec_evt.get("value", "—"))

    fee = exec_evt.get("execFee", "—")
    fee_coin = exec_evt.get("feeCurrency", exec_evt.get("feeCoin", "—"))

    ts_ms = int(exec_evt.get("execTime", exec_evt.get("ts", 0)) or 0)
    local_dt, utc_off = utc_to_local(ts_ms, UTC_OFFSET_HOURS)

    # Получаем детали ордера для SL/TP
    order_details = {}
    if order_id and category:
        order_details = rest.get_order_details(category, str(symbol), order_id)

    # Извлекаем SL/TP
    stop_loss = to_float(order_details.get("stopLoss"))
    take_profit = to_float(order_details.get("takeProfit"))
    entry_price = to_float(avg_fill_price)

    # Вычисляем R:R
    rr_ratio = None
    if entry_price and stop_loss and take_profit:
        rr_ratio = calc_rr(str(side), entry_price, stop_loss, take_profit)

    # Получаем RSI
    rsi = None
    if category:
        rsi = rest.get_rsi_4h(category, str(symbol))

    # Строим сообщение
    lines = [
        f"🔔 <b>Исполнение ордера</b>",
        f"",
        f"<b>Инструмент:</b> {symbol}",
        f"<b>Тип рынка:</b> {market_type}",
        f"<b>Сторона:</b> {side}",
        f"<b>Тип ордера:</b> {order_type}",
        f"<b>Статус:</b> {order_status}",
        f"",
        f"<b>Цена исполнения:</b> {fmt_num(avg_fill_price)}",
        f"<b>Объем:</b> {fmt_num(filled_qty)}",
        f"<b>Сумма:</b> {fmt_num(filled_notional)}",
        f"<b>Комиссия:</b> {fmt_num(fee)} {fee_coin}",
        f"",
    ]

    # Добавляем SL/TP если есть
    if stop_loss:
        lines.append(f"<b>Stop Loss:</b> {fmt_num(stop_loss)}")
    if take_profit:
        lines.append(f"<b>Take Profit:</b> {fmt_num(take_profit)}")
    if rr_ratio:
        lines.append(f"<b>R:R:</b> {fmt_num(rr_ratio, 2)}")

    # Добавляем RSI если есть
    if rsi is not None:
        lines.append(f"<b>RSI (4H):</b> {fmt_num(rsi, 2)}")

    lines.extend([
        f"",
        f"<b>Время:</b> {local_dt} ({utc_off})",
        f"<b>ID исполнения:</b> {exec_id}",
        f"",
        f"🔗 <a href='{rest.make_bybit_link(category, str(symbol))}'>Bybit</a> | "
        f"<a href='{rest.make_tv_link(str(symbol))}'>TradingView</a>",
    ])

    return "\n".join(lines)


# ==========================
# Main execution (example)
# ==========================
if __name__ == "__main__":
    # Проверяем обязательные переменные окружения
    require_env("BYBIT_API_KEY", BYBIT_API_KEY)
    require_env("BYBIT_API_SECRET", BYBIT_API_SECRET)
    require_env("TELEGRAM_BOT_TOKEN", TG_TOKEN)
    require_env("TELEGRAM_CHAT_ID", TG_CHAT_ID)

    log.info("Starting Bybit execution listener...")
    log.info(f"Testnet mode: {BYBIT_TESTNET}")

    # Инициализация REST API клиента
    rest_client = BybitRest(testnet=BYBIT_TESTNET)

    # Пример обработки события (в реальном коде здесь будет WebSocket listener)
    # В вашем оригинальном коде, вероятно, был WebSocket listener
    # который вызывал build_message при получении события исполнения

    log.info("Bot is ready. Waiting for execution events...")

    # Здесь должна быть логика WebSocket подписки на события исполнения
    # Например:
    # ws = WebSocket(...)
    # ws.execution_stream(callback=lambda msg: handle_execution(msg, rest_client))

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down...")