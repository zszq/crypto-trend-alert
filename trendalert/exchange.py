from __future__ import annotations

import logging
import time
from collections import deque
from collections.abc import Callable

import ccxt.async_support as ccxt_async
import ccxt.pro as ccxtpro

from .config import Config

log = logging.getLogger("trendalert")


def now_ms() -> int:
    return int(time.time() * 1000)


class GateWS(ccxtpro.gate):
    """在 ccxt 解析之前拿到原始推送。

    ccxt 的 parse_ohlcv 会丢掉 Gate 的 w（窗口已关闭）字段和消息里的服务器时间，
    而这两个信息分别用于"尽快确认收盘"和"估算网络延迟"，所以这里旁路一份原始消息。
    """

    candle_sink: Callable[[dict], None] | None = None

    def handle_ohlcv(self, client, message):
        sink = self.candle_sink
        if sink is not None:
            try:
                sink(message)
            except Exception:
                # 业务处理出错不能影响 ccxt 自身的订阅状态，否则会导致整条连接异常
                log.exception("处理 WS K 线消息出错")
        return super().handle_ohlcv(client, message)


def _exchange_config(cfg: Config) -> dict:
    return {
        "enableRateLimit": True,
        "timeout": cfg.feed.rest_timeout_ms,
        "options": {
            "defaultType": "swap",
            # 只加载 USDT 永续，Gate 全量市场（现货/期权/交割）加载非常慢
            "fetchMarkets": {"types": ["swap"]},
            "swap": {"fetchMarkets": {"settlementCurrencies": ["usdt"]}},
        },
    }


def build_exchanges(cfg: Config) -> tuple[ccxt_async.gate, GateWS]:
    rest = ccxt_async.gate(_exchange_config(cfg))
    ws = GateWS(_exchange_config(cfg))
    # load_markets 默认还会拉取现货币种列表，本项目用不到，网络差时它经常超时拖慢启动
    rest.has["fetchCurrencies"] = False
    if cfg.proxy.http:
        rest.https_proxy = cfg.proxy.http
        ws.https_proxy = cfg.proxy.http
        ws.wss_proxy = cfg.proxy.http
    elif cfg.proxy.socks:
        rest.socks_proxy = cfg.proxy.socks
        ws.socks_proxy = cfg.proxy.socks
        ws.ws_socks_proxy = cfg.proxy.socks
    return rest, ws


class ServerClock:
    """用 WS 推送里的服务器时间估算本地时钟偏差和网络延迟。

    offset = 最近 10 分钟内 (本地时间 - 服务器时间) 的最小值，
    即"时钟偏差 + 最小网络延迟"。用最小值是因为网络抖动只会让差值变大，
    最小值最接近真实偏差；由此得到的 server_now 略偏早，判断收盘时更保守。
    """

    BUCKET_MS = 10_000

    def __init__(self) -> None:
        self._mins: deque[int] = deque(maxlen=60)
        self._bucket: int | None = None
        self._cur_min: int | None = None
        self._window_min: int | None = None
        self.offset = 0
        self.latency_ms = 0.0
        self.ws_through = 0          # 已收到的最新服务器时间
        self.last_msg_local = 0      # 最近一次收到 WS K 线消息的本地时间

    def seed(self, offset: int) -> None:
        self._window_min = offset
        self.offset = offset

    def observe(self, server_ms: int) -> None:
        local = now_ms()
        diff = local - server_ms
        b = local // self.BUCKET_MS
        if b != self._bucket:
            if self._cur_min is not None:
                self._mins.append(self._cur_min)
                self._window_min = min(self._mins)
            self._bucket = b
            self._cur_min = diff
        elif diff < self._cur_min:
            self._cur_min = diff
        self.offset = self._cur_min if self._window_min is None else min(self._window_min, self._cur_min)
        excess = max(0, diff - self.offset)
        self.latency_ms = self.latency_ms * 0.9 + excess * 0.1
        if server_ms > self.ws_through:
            self.ws_through = server_ms
        self.last_msg_local = local

    def server_now(self) -> int:
        return now_ms() - self.offset
