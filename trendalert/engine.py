from __future__ import annotations

import asyncio
import logging
import random
import time

import ccxt

from .candles import MIN, Bar, SymbolSeries
from .config import Config
from .exchange import GateWS, ServerClock, build_exchanges, now_ms
from .notifier import Alert, Notifier
from .signals import Detector
from .universe import Universe

log = logging.getLogger("trendalert")


class Engine:
    def __init__(self, cfg: Config, notifier: Notifier):
        self.cfg = cfg
        self.notifier = notifier
        self.rest, self.ws = build_exchanges(cfg)
        self.ws.candle_sink = self._on_ws_message
        self.clock = ServerClock()
        self.universe = Universe(cfg.universe)
        self.series: dict[str, SymbolSeries] = {}
        self._tasks: dict[str, list[asyncio.Task]] = {}
        self._rest_sem = asyncio.Semaphore(cfg.feed.rest_concurrency)
        self.detector = Detector(cfg.signal)
        self.stats = {"backfill": 0, "backfill_err": 0, "corrected": 0, "alerts": 0, "stale": 0, "ws_reconnect": 0}
        self._last_ws_error_log = 0.0

    # ================= 启动 / 停止 =================

    async def run(self) -> None:
        await self._load_markets()
        await self._seed_clock()
        await self._refresh_universe(first=True)
        loops = [
            self._universe_loop(),
            self._finalizer_loop(),
            self._reconcile_loop(),
            self._watchdog_loop(),
        ]
        await asyncio.gather(*loops)

    async def close(self) -> None:
        for tasks in self._tasks.values():
            for t in tasks:
                t.cancel()
        for ex in (self.ws, self.rest):
            try:
                await ex.close()
            except Exception:
                pass

    async def _load_markets(self) -> None:
        delay = 2
        while True:
            try:
                t0 = time.time()
                await self.rest.load_markets()
                # WS 实例直接复用市场数据，避免再加载一次
                self.ws.set_markets_from_exchange(self.rest)
                log.info("市场加载完成：%d 个合约，用时 %.1fs", len(self.rest.markets), time.time() - t0)
                return
            except Exception as e:
                log.warning("加载市场失败，%ds 后重试：%r", delay, e)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)

    async def _seed_clock(self) -> None:
        try:
            t0 = now_ms()
            server = await self.rest.fetch_time()
            t1 = now_ms()
            offset = (t0 + t1) // 2 - int(server)
            self.clock.seed(offset)
            if abs(offset) > 3000:
                log.warning("本地时钟与交易所相差约 %.1fs，已自动校正，建议同步系统时间", offset / 1000)
        except Exception as e:
            log.warning("获取服务器时间失败，先按本地时间运行：%r", e)

    # ================= 标的池 =================

    async def _universe_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.universe.refresh_seconds)
            await self._refresh_universe()

    async def _refresh_universe(self, first: bool = False) -> None:
        delay = 5
        while True:
            try:
                tickers = await self.rest.fetch_tickers(params={"type": "swap"})
                break
            except Exception as e:
                if not first:
                    log.warning("刷新标的池失败，下次再试：%r", e)
                    return
                log.warning("获取 tickers 失败，%ds 后重试：%r", delay, e)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
        added, removed = self.universe.update(self.rest.markets, tickers, time.time())
        for s in removed:
            self._remove_symbol(s)
        for s in sorted(added, key=lambda x: -self.universe.quote_volume.get(x, 0)):
            self._add_symbol(s)
        if added or removed or first:
            log.info(
                "标的池：共 %d 个，新增 %d %s，移除 %d %s",
                len(self.universe.active), len(added), _short(added), len(removed), _short(removed),
            )

    def _add_symbol(self, symbol: str) -> None:
        s = SymbolSeries(symbol, self.cfg.signal.timeframes)
        # 订阅建立之前的数据一律不信任 WS，由预热/回补负责
        s.mark_down(self.clock.server_now())
        self.series[symbol] = s
        self._tasks[symbol] = [
            asyncio.create_task(self._watch(s), name=f"watch:{symbol}"),
            asyncio.create_task(self._warmup(s), name=f"warmup:{symbol}"),
        ]

    def _remove_symbol(self, symbol: str) -> None:
        # ccxt 对 Gate K 线没有实现退订，只能停止处理；推送会被 _on_ws_message 忽略
        for t in self._tasks.pop(symbol, []):
            t.cancel()
        self.series.pop(symbol, None)

    # ================= WebSocket =================

    async def _watch(self, s: SymbolSeries) -> None:
        backoff = 1.0
        while True:
            try:
                await self.ws.watch_ohlcv(s.symbol, "1m")
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # 连接是所有合约共用的，断开时这里会同时报很多条，日志做节流
                s.mark_down(self.clock.ws_through or self.clock.server_now())
                now = time.time()
                if now - self._last_ws_error_log > 10:
                    self._last_ws_error_log = now
                    self.stats["ws_reconnect"] += 1
                    log.warning("WS 断开，%.0fs 后重连：%r", backoff, e)
                await asyncio.sleep(backoff + random.uniform(0, backoff / 2))
                # 上限不宜太大，否则网络恢复后要等很久才重连
                backoff = min(backoff * 2, 15)

    def _on_ws_message(self, message: dict) -> None:
        server_ms = message.get("time_ms") or int(message.get("time", 0)) * 1000
        if server_ms:
            self.clock.observe(int(server_ms))
        result = message.get("result")
        if not isinstance(result, list):
            result = [result]
        for item in result:
            if not isinstance(item, dict):
                continue
            n = str(item.get("n", ""))
            if not n.startswith("1m_"):
                continue
            symbol = self.ws.safe_symbol(n[3:], None, "_", "contract")
            s = self.series.get(symbol)
            if s is None:
                continue
            if s.down_since is not None:
                s.mark_up(int(server_ms) if server_ms else self.clock.server_now())
            try:
                bar = Bar(
                    int(item["t"]) * 1000,
                    float(item["o"]), float(item["h"]), float(item["l"]), float(item["c"]),
                    float(item.get("v") or 0), "ws", bool(item.get("w")),
                )
            except (KeyError, TypeError, ValueError):
                continue
            s.on_ws(bar)

    async def _watchdog_loop(self) -> None:
        last_health = 0.0
        while True:
            await asyncio.sleep(5)
            now = now_ms()
            idle = now - self.clock.last_msg_local
            if self.series and self.clock.last_msg_local and idle > self.cfg.feed.stale_ws_seconds * 1000:
                # 连接可能处于 TCP 半开的假死状态，ping 未必能及时发现，主动断开让订阅任务重连
                log.warning("WS 已 %.0fs 无消息，强制重连", idle / 1000)
                self.clock.last_msg_local = now
                for s in self.series.values():
                    s.mark_down(self.clock.ws_through)
                for client in list(self.ws.clients.values()):
                    client.on_error(ccxt.NetworkError("stale connection"))
            if time.time() - last_health >= 60:
                last_health = time.time()
                self._log_health()

    def _log_health(self) -> None:
        warmed = sum(1 for s in self.series.values() if s.warmed)
        down = sum(1 for s in self.series.values() if s.down_since is not None)
        # 已收盘但尚未确认的时长，正常应接近 0；持续变大说明回补跟不上
        lag = [
            max(0, self.clock.server_now() - s.next_ts() - MIN) // 1000
            for s in self.series.values() if s.warmed
        ]
        log.info(
            "状态：标的 %d（预热完成 %d，WS 断开 %d）| WS 延迟 %.0fms%s | 最大确认滞后 %ss | "
            "回补 %d 失败 %d 修正 %d 报警 %d 过期 %d",
            len(self.series), warmed, down, self.clock.latency_ms,
            "（阻塞）" if self._congested() else "", max(lag) if lag else "-",
            self.stats["backfill"], self.stats["backfill_err"], self.stats["corrected"],
            self.stats["alerts"], self.stats["stale"],
        )

    def _congested(self) -> bool:
        return self.clock.latency_ms > self.cfg.feed.congestion_ms

    # ================= REST：预热 / 回补 / 对账 =================

    async def _fetch_1m(self, symbol: str, since: int | None, limit: int) -> list[Bar]:
        async with self._rest_sem:
            rows = await self.rest.fetch_ohlcv(symbol, "1m", since=since, limit=min(limit, 1999))
        return [Bar.from_ohlcv(r, "rest") for r in rows]

    async def _warmup(self, s: SymbolSeries) -> None:
        delay = 2
        big = max(self.cfg.signal.timeframes) * MIN
        while True:
            try:
                now = self.clock.server_now()
                # 起点对齐到最大周期的整点，保证第一个合成桶是完整的
                since = (now - self.cfg.feed.warmup_minutes * MIN) // big * big
                bars = await self._fetch_1m(s.symbol, since, (now - since) // MIN + 2)
                break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("%s 预热失败，%ds 后重试：%r", s.symbol, delay, e)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
        now = self.clock.server_now()
        if bars and bars[0].ts > since:
            # 新上线的合约历史不足，从第一根有数据的 K 线开始，否则会一直卡在回补
            since = bars[0].ts
        s.finalized_ts = since - MIN
        s.silent_until = now
        self._store_rest(s, since, bars, now)
        self._advance(s, now)
        s.warmed = True
        log.debug("%s 预热完成，已确认到 %s", s.symbol, _fmt_ts(s.finalized_ts))

    def _store_rest(self, s: SymbolSeries, since: int, bars: list[Bar], now: int) -> int:
        """写入 REST 数据，返回连续可信覆盖到的最后一根开盘时间。"""
        got = {b.ts: b for b in bars}
        # REST 返回的最后一根通常还没收盘，不能当作确认数据
        limit = now // MIN * MIN
        newest = max(got) if got else None
        last_ok = None
        prev_close = s.last_close()
        ts = since
        while ts < limit:
            b = got.get(ts)
            if b is not None:
                s.put_trusted(b)
                prev_close = b.c
            elif newest is not None and newest > ts and prev_close is not None:
                # 之后还有数据而这一分钟没有，说明确实没有成交，补一根平线保证序列连续
                s.put_trusted(Bar.flat(ts, prev_close))
            else:
                break
            last_ok = ts
            ts += MIN
        return last_ok

    def _request_backfill(self, s: SymbolSeries, ts: int, now: int) -> None:
        if s.backfill_pending or now < s.backfill_retry_at:
            return
        s.backfill_pending = True
        asyncio.create_task(self._backfill(s, ts))

    async def _backfill(self, s: SymbolSeries, ts: int) -> None:
        try:
            now = self.clock.server_now()
            bars = await self._fetch_1m(s.symbol, ts, (now - ts) // MIN + 2)
            self.stats["backfill"] += 1
            if self._store_rest(s, ts, bars, self.clock.server_now()) is None:
                raise RuntimeError("REST 未返回所需 K 线")
            s.backfill_failures = 0
            s.backfill_retry_at = 0
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.stats["backfill_err"] += 1
            s.backfill_failures += 1
            wait = min(2 ** s.backfill_failures, 60)
            if isinstance(e, (ccxt.RateLimitExceeded, ccxt.DDoSProtection)):
                wait = max(wait, 10)
            s.backfill_retry_at = self.clock.server_now() + wait * 1000
            log.warning("%s 回补 %s 失败(%d)，%ds 后重试：%r", s.symbol, _fmt_ts(ts), s.backfill_failures, wait, e)
        finally:
            s.backfill_pending = False
        if self.series.get(s.symbol) is s and s.warmed:
            self._advance(s, self.clock.server_now())

    async def _reconcile_loop(self) -> None:
        n = self.cfg.feed.reconcile_bars
        while True:
            await asyncio.sleep(self.cfg.feed.reconcile_seconds)
            fixed = 0

            async def one(s: SymbolSeries) -> int:
                try:
                    bars = await self._fetch_1m(s.symbol, None, n + 1)
                except Exception as e:
                    log.debug("%s 对账失败：%r", s.symbol, e)
                    return 0
                c = 0
                for b in bars:
                    if s.finalized_ts is not None and b.ts <= s.finalized_ts and s.correct(b):
                        c += 1
                        log.debug("%s 修正 %s -> %r", s.symbol, _fmt_ts(b.ts), b)
                return c

            results = await asyncio.gather(*(one(s) for s in list(self.series.values()) if s.warmed))
            fixed = sum(results)
            self.stats["corrected"] += fixed
            if fixed:
                log.info("对账修正 %d 根 1m K 线（WS 收尾数据与 REST 不一致）", fixed)

    # ================= 收盘确认与信号 =================

    async def _finalizer_loop(self) -> None:
        while True:
            await asyncio.sleep(0.5)
            now = self.clock.server_now()
            for s in list(self.series.values()):
                if s.warmed:
                    self._advance(s, now)

    def _advance(self, s: SymbolSeries, now: int) -> None:
        f = self.cfg.feed
        grace = int(f.close_grace_seconds * 1000)
        fallback = int(f.rest_fallback_seconds * 1000)
        while True:
            ts = s.next_ts()
            end = ts + MIN
            bar = s.raw.get(ts)
            if bar is not None and bar.src != "ws":
                self._finalize(s, bar, now)
                continue
            if now < end + grace:
                return
            ws_ok = s.ws_trusted_for(ts)
            if bar is not None and ws_ok and (
                bar.closed                                   # Gate 明确推送了 w=true
                or s.max_ws_ts > ts                          # 已经收到下一分钟的数据
                or self.clock.ws_through >= end + grace      # 连接正常且过了收盘，没有新成交
            ):
                self._finalize(s, bar, now)
                continue
            if not ws_ok or now >= end + fallback or (bar is None and self.clock.ws_through >= end + grace):
                self._request_backfill(s, ts, now)
            return

    def _finalize(self, s: SymbolSeries, bar: Bar, now: int) -> None:
        closed = s.finalize(bar)
        hits, stale, rejected = self.detector.check(s.symbol, s.bars, closed, now, s.silent_until)
        if now > s.silent_until:
            for ev in rejected:
                log.debug("%s %dm 连续%d次但未通过：%s", s.symbol, ev.tf, ev.length, ",".join(ev.reasons))
        for ev in stale:
            self.stats["stale"] += 1
            log.info("过期信号（不报警）%s %dm %s %d次 %+.2f%% 延迟 %.0fs", s.symbol, ev.tf,
                     "涨" if ev.direction == 1 else "跌", ev.length, ev.move_pct,
                     (now - ev.bar_ts - ev.tf * MIN) / 1000)
        for hit in hits:
            self.stats["alerts"] += 1
            self.notifier.emit(Alert(
                symbol=s.symbol,
                direction="up" if hit.direction == 1 else "down",
                level=hit.level,
                price=bar.c,
                time=bar.ts + MIN,
                triggered=hit.evaluations,
                resonance=hit.resonance,
                quote_volume_24h=self.universe.quote_volume.get(s.symbol, 0),
                delay_ms=max(0, now - bar.ts - MIN),
                congested=self._congested(),
            ))


def _fmt_ts(ts: int | None) -> str:
    if ts is None:
        return "-"
    return time.strftime("%m-%d %H:%M", time.localtime(ts / 1000))


def _short(items: set[str], n: int = 8) -> str:
    names = sorted(x.split("/")[0] for x in items)
    return "[" + ",".join(names[:n]) + (",..." if len(names) > n else "") + "]" if names else ""
