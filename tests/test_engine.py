import asyncio
import unittest
from unittest import mock

from trendalert.candles import MIN, Bar, SymbolSeries
from trendalert.config import Config, UniverseConfig
from trendalert.engine import Engine, FailureLog
from trendalert.exchange import resolve_proxy
from trendalert.notifier import Notifier
from trendalert.universe import Universe, to_symbol

T0 = 1_790_000_100_000 // (15 * MIN) * (15 * MIN)


def market(contract_type="", status="trading", in_delisting=False):
    return {
        "swap": True, "linear": True, "settle": "USDT", "active": True,
        "info": {"contract_type": contract_type, "status": status, "in_delisting": in_delisting},
    }


class TestUniverse(unittest.TestCase):
    def test_only_crypto_and_hysteresis(self):
        markets = {
            "BTC/USDT:USDT": market(),
            "AAPL/USDT:USDT": market("stocks"),
            "XAU/USDT:USDT": market("metals"),
            "OLD/USDT:USDT": market(in_delisting=True),
            "NOTYPE/USDT:USDT": {**market(), "info": {"status": "trading"}},
        }
        tickers = {s: {"quoteVolume": 1e8} for s in markets}
        u = Universe(UniverseConfig())
        added, _ = u.update(markets, tickers, 0)
        # 字段缺失也不收，避免接口变化时误收非币类资产
        self.assertEqual(added, {"BTC/USDT:USDT"})

        tickers["BTC/USDT:USDT"] = {"quoteVolume": 3e6}
        _, removed = u.update(markets, tickers, 60)
        self.assertEqual(removed, set())
        _, removed = u.update(markets, tickers, 60 + 30 * 60)
        self.assertEqual(removed, {"BTC/USDT:USDT"})

    def test_include_exclude_accept_underscore(self):
        self.assertEqual(to_symbol("btc_usdt"), "BTC/USDT:USDT")
        self.assertEqual(to_symbol("BTC/USDT:USDT"), "BTC/USDT:USDT")
        markets = {"AAPL/USDT:USDT": market("stocks"), "BTC/USDT:USDT": market()}
        tickers = {s: {"quoteVolume": 1e8} for s in markets}
        u = Universe(UniverseConfig(include=["AAPL_USDT"], exclude=["BTC_USDT"]))
        added, _ = u.update(markets, tickers, 0)
        self.assertEqual(added, {"AAPL/USDT:USDT"})


class TestProxy(unittest.TestCase):
    def test_explicit_wins(self):
        cfg = Config()
        cfg.proxy.http = "http://127.0.0.1:1"
        self.assertEqual(resolve_proxy(cfg), "http://127.0.0.1:1")

    def test_system_proxy(self):
        cfg = Config()
        with mock.patch("urllib.request.getproxies", return_value={"https": "http://127.0.0.1:7897"}):
            self.assertEqual(resolve_proxy(cfg), "http://127.0.0.1:7897")
        with mock.patch("urllib.request.getproxies", return_value={"socks": "socks://127.0.0.1:7897"}):
            self.assertEqual(resolve_proxy(cfg), "socks5://127.0.0.1:7897")
        with mock.patch("urllib.request.getproxies", return_value={}):
            self.assertEqual(resolve_proxy(cfg), "")
        cfg.proxy.system = False
        with mock.patch("urllib.request.getproxies", return_value={"https": "http://x:1"}):
            self.assertEqual(resolve_proxy(cfg), "")


class _EngineCase(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        cfg = Config()
        cfg.proxy.system = False
        cfg.notify.console = False
        self.engine = Engine(cfg, Notifier(cfg.notify, "low"))
        self.engine._request_backfill = mock.Mock()
        self.engine.clock.ws_through = 0
        s = SymbolSeries("BTC/USDT:USDT", [1], name="BTC_USDT")
        s.finalized_ts = T0 - MIN
        s.warmed = True
        self.s = s

    def tearDown(self):
        self.loop.run_until_complete(self.engine.close())
        self.loop.close()



class TestAdvance(_EngineCase):
    """收盘确认：REST 为准，WS 只在 REST 长时间失败时兜底。"""

    def ws_bar(self, ts, c):
        return Bar(ts, c, c, c, c, 1, "ws", closed=True)

    def test_ws_close_push_waits_for_rest(self):
        self.s.on_ws(self.ws_bar(T0, 100))
        self.engine._advance(self.s, T0 + MIN + 2000)
        self.assertEqual(self.s.finalized_ts, T0 - MIN)
        self.engine._request_backfill.assert_called_once()

    def test_rest_bar_finalizes_immediately(self):
        self.s.on_ws(self.ws_bar(T0, 100))
        self.s.put_trusted(Bar(T0, 100, 101, 99, 100.5, 1, "rest", True))
        self.engine._advance(self.s, T0 + MIN + 1500)
        self.assertEqual(self.s.finalized_ts, T0)
        self.assertEqual(self.s.bars[1][-1].c, 100.5)

    def test_ws_fallback_after_timeout(self):
        self.s.on_ws(self.ws_bar(T0, 100))
        self.engine._advance(self.s, T0 + MIN + 16_000)
        self.assertEqual(self.s.finalized_ts, T0)
        self.assertIn(T0, self.s.unverified)
        self.assertEqual(self.engine.stats["ws_fallback"], 1)

    def test_no_fallback_when_ws_was_down(self):
        self.s.mark_down(T0 + 30_000)
        self.s.on_ws(self.ws_bar(T0, 100))
        self.engine._advance(self.s, T0 + MIN + 16_000)
        self.assertEqual(self.s.finalized_ts, T0 - MIN)


class TestStoreRest(_EngineCase):
    def rest_bar(self, i, c):
        return Bar(T0 + i * MIN, c, c, c, c, 1, "rest", True)

    def test_gap_in_middle_is_flat_fill(self):
        now = T0 + 3 * MIN + 1000
        last = self.engine._store_rest(self.s, T0, [self.rest_bar(-1, 99), self.rest_bar(0, 100), self.rest_bar(2, 102)], now)
        self.assertEqual(last, T0 + 2 * MIN)
        self.assertEqual(self.s.raw[T0 + MIN].src, "fill")
        self.assertEqual(self.s.raw[T0 + MIN].c, 100)

    def test_missing_at_edge_is_not_filled(self):
        # 目标分钟缺失而只有更新的数据：可能是返回不完整，不能当作无成交
        now = T0 + 2 * MIN + 1000
        last = self.engine._store_rest(self.s, T0, [self.rest_bar(1, 101)], now)
        self.assertIsNone(last)
        self.assertNotIn(T0, self.s.raw)

    def test_unclosed_or_just_closed_bar_not_stored(self):
        bars = [self.rest_bar(0, 100), self.rest_bar(1, 101)]
        # 收盘不足 1 秒的数据可能还缺最后几笔成交
        self.engine._store_rest(self.s, T0, bars, T0 + MIN + 500)
        self.assertNotIn(T0, self.s.raw)
        self.engine._store_rest(self.s, T0, bars, T0 + MIN + 1000)
        self.assertIn(T0, self.s.raw)
        self.assertNotIn(T0 + MIN, self.s.raw)

    def test_fetch_uses_limit_without_since(self):
        calls = []

        async def fake(symbol, tf, since=None, limit=None):
            calls.append((since, limit))
            return []

        self.engine.rest.fetch_ohlcv = fake
        self.engine.clock.server_now = lambda: T0 + 5 * MIN + 1000
        self.loop.run_until_complete(self.engine._fetch_1m("BTC/USDT:USDT", T0))
        # 从 T0 前一根拉到当前（T0-1 .. T0+5 共 7 根），再多一根余量
        self.assertEqual(calls, [(None, 8)])


class TestFailureLog(unittest.TestCase):
    def test_aggregates_and_respects_interval(self):
        fl = FailureLog("回补", interval=3600)
        with self.assertLogs("trendalert", "WARNING") as cm:
            fl.add("A_USDT", TimeoutError("x"))
            fl.add("B_USDT", TimeoutError("y"))
            fl.flush()                       # 首次立即输出汇总
            fl.add("C_USDT", TimeoutError("z"))
            fl.flush()                       # 间隔未到，不输出
            fl.flush(force=True)
        self.assertEqual(len(cm.output), 2)
        self.assertIn("2 次（2 个合约：A_USDT,B_USDT）", cm.output[0])
        self.assertIn("1 次（1 个合约：C_USDT）", cm.output[1])


if __name__ == "__main__":
    unittest.main()
