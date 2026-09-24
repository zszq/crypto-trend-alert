from __future__ import annotations

import asyncio
import logging
import random
import time

import ccxt

from .candles import MIN, Bar, SymbolSeries
from .config import Config
from .exchange import ServerClock, build_exchanges, now_ms
from .notifier import Alert, Notifier
from .signals import Detector
from .universe import Universe

log = logging.getLogger("trendalert")


class FailureLog:
    """同类失败短时间内大量出现（如断网时所有合约同时回补失败）时，合并成一条汇总日志。"""

    def __init__(self, what: str, interval: float = 30):
        self.what = what
        self.interval = interval
        self._count = 0
        self._names: set[str] = set()
        self._last_err: BaseException | None = None
        self._last_emit = 0.0

    def add(self, name: str, err: BaseException) -> None:
        # 只累计不输出，由看门狗定期调用 flush，保证断网期间每个间隔最多一条
        self._count += 1
        self._names.add(name)
        self._last_err = err

    def flush(self, force: bool = False) -> None:
        if not self._count or (not force and time.time() - self._last_emit < self.interval):
            return
        names = sorted(self._names)
        log.warning(
            "%s失败 %d 次（%d 个合约：%s%s），最近错误：%s",
            self.what, self._count, len(names), ",".join(names[:5]), "..." if len(names) > 5 else "",
            brief_error(self._last_err),
        )
        self._count = 0
        self._names.clear()
        self._last_emit = time.time()


def brief_error(e: BaseException | None) -> str:
    # ccxt 的异常信息里带完整 URL，aiohttp 的连接异常 repr 很长，只保留足够定位问题的部分
    if e is None:
        return "-"
    msg = str(e).replace("\n", " ")
    return f"{type(e).__name__}: {msg[:160]}" if msg else type(e).__name__


class Engine:
    def __init__(self, cfg: Config, notifier: Notifier):
        self.cfg = cfg
        self.notifier = notifier
        ex = build_exchanges(cfg)
        self.rest, self.bulk, self.ws, self.proxy = ex.rest, ex.bulk, ex.ws, ex.proxy
        self.ws.candle_sink = self._on_ws_message
        self.clock = ServerClock()
        self.universe = Universe(cfg.universe)
        self.series: dict[str, SymbolSeries] = {}
        self._tasks: dict[str, list[asyncio.Task]] = {}
        self._rest_sem = asyncio.Semaphore(cfg.feed.rest_concurrency)
        self.detector = Detector(cfg.signal)
        self.stats = {"confirm": 0, "rest_err": 0, "ws_fallback": 0, "corrected": 0, "alerts": 0, "stale": 0}
        self._last_ws_error_log = 0.0
        self._markets_loaded_at = 0.0
        self._backfill_fail = FailureLog("REST 确认")
        self._warmup_fail = FailureLog("预热")

    # ================= 启动 / 停止 =================

    async def run(self) -> None:
        log.info("网络：%s", f"使用代理 {self.proxy}" if self.proxy else "直连（未配置代理，系统代理也未开启）")
        await self._load_markets(first=True)
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
        self._backfill_fail.flush(force=True)
        self._warmup_fail.flush(force=True)
        for tasks in self._tasks.values():
            for t in tasks:
                t.cancel()
        for ex in (self.ws, self.rest, self.bulk):
            try:
                await ex.close()
            except Exception:
                pass

    async def _load_markets(self, first: bool = False) -> None:
        delay = 2
        while True:
            try:
                t0 = time.time()
                await self.bulk.load_markets(reload=not first)
                # 其他实例直接复用市场数据，避免重复下载约 1MB 的合约列表
                self.rest.set_markets_from_exchange(self.bulk)
                self.ws.set_markets_from_exchange(self.bulk)
                self._markets_loaded_at = time.time()
                log.info("市场加载完成：%d 个合约，用时 %.1fs", len(self.bulk.markets), time.time() - t0)
                return
            except Exception as e:
                if not first:
                    # 已有旧数据可用，定期重载失败不影响运行
                    log.warning("重新加载市场失败，继续使用旧数据：%s", brief_error(e))
                    return
                log.warning("加载市场失败，%ds 后重试：%s", delay, brief_error(e))
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
            log.warning("获取服务器时间失败，先按本地时间运行：%s", brief_error(e))

    # ================= 标的池 =================

    async def _universe_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.universe.refresh_seconds)
            if time.time() - self._markets_loaded_at >= self.cfg.feed.markets_reload_hours * 3600:
                await self._load_markets()
            await self._refresh_universe()

    async def _refresh_universe(self, first: bool = False) -> None:
        delay = 5
        while True:
            try:
                tickers = await self.bulk.fetch_tickers(params={"type": "swap"})
                break
            except Exception as e:
                if not first:
                    log.warning("刷新标的池失败，下次再试：%s", brief_error(e))
                    return
                log.warning("获取 tickers 失败，%ds 后重试：%s", delay, brief_error(e))
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
        added, removed = self.universe.update(self.bulk.markets, tickers, time.time())
        removed_names = {self._name(s) for s in removed}
        for s in removed:
            self._remove_symbol(s)
        for s in sorted(added, key=lambda x: -self.universe.quote_volume.get(x, 0)):
            self._add_symbol(s)
        if added or removed or first:
            log.info(
                "标的池：共 %d 个，新增 %d %s，移除 %d %s",
                len(self.universe.active), len(added), _short({self._name(s) for s in added}),
                len(removed), _short(removed_names),
            )

    def _name(self, symbol: str) -> str:
        market = self.bulk.markets.get(symbol) if self.bulk.markets else None
        return market["id"] if market else symbol

    def _add_symbol(self, symbol: str) -> None:
        s = SymbolSeries(symbol, self.cfg.signal.timeframes, name=self._name(symbol))
        # 订阅建立之前的数据一律不信任 WS，由预热/REST 确认负责
        s.mark_down(self.clock.server_now())
        self.series[symbol] = s
        self._tasks[symbol] = [
            asyncio.create_task(self._watch(s), name=f"watch:{s.name}"),
            asyncio.create_task(self._warmup(s), name=f"warmup:{s.name}"),
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
                if now - self._last_ws_error_log > 30:
                    self._last_ws_error_log = now
                    log.warning("WS 断开，%.0fs 后重连：%s", backoff, brief_error(e))
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
            self._backfill_fail.flush()
            self._warmup_fail.flush()
            now = now_ms()
            idle = now - self.clock.last_msg_local
            # 没有已打开的连接说明订阅任务正在退避重连，此时再强制断开只会打乱退避节奏
            has_open = any(not c.closed() for c in self.ws.clients.values())
            if self.series and has_open and self.clock.last_msg_local and idle > self.cfg.feed.stale_ws_seconds * 1000:
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
        # 已收盘但尚未确认的时长，正常应只有几秒；持续变大说明 REST 跟不上
        lag = [
            max(0, self.clock.server_now() - s.next_ts() - MIN) // 1000
            for s in self.series.values() if s.warmed
        ]
        log.info(
            "状态：标的 %d（预热完成 %d，WS 断开 %d）| WS 延迟 %.0fms%s | 最大确认滞后 %ss | "
            "REST 确认 %d 失败 %d | WS 兜底 %d 修正 %d | 报警 %d 过期 %d",
            len(self.series), warmed, down, self.clock.latency_ms,
            "（阻塞）" if self._congested() else "", max(lag) if lag else "-",
            self.stats["confirm"], self.stats["rest_err"], self.stats["ws_fallback"],
            self.stats["corrected"], self.stats["alerts"], self.stats["stale"],
        )

    def _congested(self) -> bool:
        return self.clock.latency_ms > self.cfg.feed.congestion_ms

    # ================= REST：预热 / 收盘确认 / 对账 =================

    async def _fetch_1m(self, symbol: str, since: int) -> list[Bar]:
        """拉取从 since 的前一根到当前的 1m。

        尽量只传 limit 取最新 N 根：ccxt 带 since 时会用本地时钟计算 to 参数，
        本地时钟偏慢时 to 会落在刚收盘那根之前，Gate 的返回就可能缺少这根。
        多拉前一根是为了让 _store_rest 能分辨“这一分钟无成交”和“返回里缺数据”。
        """
        need = (self.clock.server_now() - since) // MIN + 3
        async with self._rest_sem:
            if need <= 1999:
                rows = await self.rest.fetch_ohlcv(symbol, "1m", limit=need)
            else:
                rows = await self.rest.fetch_ohlcv(symbol, "1m", since=since - MIN, limit=1999)
        return [Bar.from_ohlcv(r, "rest") for r in rows]

    async def _warmup(self, s: SymbolSeries) -> None:
        delay = 2
        big = max(self.cfg.signal.timeframes) * MIN
        while True:
            try:
                now = self.clock.server_now()
                # 起点对齐到最大周期的整点，保证第一个合成桶是完整的
                since = (now - self.cfg.feed.warmup_minutes * MIN) // big * big
                bars = await self._fetch_1m(s.symbol, since)
                break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._warmup_fail.add(s.name, e)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
        now = self.clock.server_now()
        bars = [b for b in bars if b.ts >= since]
        if bars and bars[0].ts > since:
            # 新上线的合约历史不足，从第一根有数据的 K 线开始，否则会一直卡在回补
            since = bars[0].ts
        s.finalized_ts = since - MIN
        s.silent_until = now
        self._store_rest(s, since, bars, now)
        self._advance(s, now)
        s.warmed = True
        log.debug("%s 预热完成，已确认到 %s", s.name, _fmt_ts(s.finalized_ts))

    def _store_rest(self, s: SymbolSeries, since: int, bars: list[Bar], now: int) -> int | None:
        """写入 REST 数据，返回连续可信覆盖到的最后一根开盘时间。"""
        got = {b.ts: b for b in bars}
        # REST 返回的最后一根通常还没收盘；刚收盘不到 rest_confirm_delay 的也可能还缺最后几笔成交
        # （预热请求恰好跨过整分钟时会遇到），都不能当作确认数据
        limit = (now - int(self.cfg.feed.rest_confirm_delay_seconds * 1000)) // MIN * MIN
        newest = max(got) if got else None
        oldest = min(got) if got else None
        last_ok = None
        prev_close = s.last_close()
        ts = since
        while ts < limit:
            b = got.get(ts)
            if b is not None:
                s.put_trusted(b)
                prev_close = b.c
            elif oldest is not None and oldest < ts < newest and prev_close is not None:
                # 返回区间的中间缺了这一分钟，说明确实没有成交，补一根平线保证序列连续。
                # 缺在区间两端则可能只是返回不完整，不能当作无成交
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
            bars = await self._fetch_1m(s.symbol, ts)
            if self._store_rest(s, ts, bars, self.clock.server_now()) is None:
                raise RuntimeError("REST 未返回所需 K 线")
            self.stats["confirm"] += 1
            s.backfill_failures = 0
            s.backfill_retry_at = 0
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.stats["rest_err"] += 1
            s.backfill_failures += 1
            wait = min(2 ** s.backfill_failures, 60)
            if isinstance(e, (ccxt.RateLimitExceeded, ccxt.DDoSProtection)):
                wait = max(wait, 10)
            s.backfill_retry_at = self.clock.server_now() + wait * 1000
            self._backfill_fail.add(s.name, e)
        finally:
            s.backfill_pending = False
        if self.series.get(s.symbol) is s and s.warmed:
            self._advance(s, self.clock.server_now())

    async def _reconcile_loop(self) -> None:
        n = self.cfg.feed.reconcile_bars
        while True:
            await asyncio.sleep(self.cfg.feed.reconcile_seconds)

            async def one(s: SymbolSeries) -> int:
                try:
                    bars = await self._fetch_1m(s.symbol, self.clock.server_now() - n * MIN)
                except Exception as e:
                    log.debug("%s 对账失败：%s", s.name, brief_error(e))
                    return 0
                c = 0
                for b in bars:
                    if s.finalized_ts is None or b.ts > s.finalized_ts:
                        continue
                    old = s.correct(b)
                    if old is not None:
                        c += 1
                        log.debug("%s 修正 %s：%r -> %r", s.name, _fmt_ts(b.ts), old, b)
                if s.finalized_ts is not None:
                    # 超出对账窗口的已无法再核实，放弃跟踪
                    cutoff = s.finalized_ts - n * MIN
                    s.unverified = {t for t in s.unverified if t > cutoff}
                return c

            results = await asyncio.gather(*(one(s) for s in list(self.series.values()) if s.warmed))
            fixed = sum(results)
            self.stats["corrected"] += fixed
            if fixed:
                log.info("对账修正 %d 根 1m K 线（多为 REST 失败时用 WS 数据兜底确认的）", fixed)

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
        confirm_delay = int(f.rest_confirm_delay_seconds * 1000)
        fallback = int(f.ws_fallback_seconds * 1000)
        while True:
            ts = s.next_ts()
            end = ts + MIN
            bar = s.raw.get(ts)
            if bar is not None and bar.src != "ws":
                self._finalize(s, bar, now)
                continue
            if now < end + confirm_delay:
                return
            # 收盘数据以 REST 为准：Gate WS 的收盘推送约 10% 与最终 K 线不一致（多为收盘价差一跳），
            # 而连涨判断恰恰依赖收盘价
            self._request_backfill(s, ts, now)
            if (
                now >= end + fallback
                and bar is not None
                and s.ws_trusted_for(ts)
                and (bar.closed or s.max_ws_ts > ts or self.clock.ws_through >= end + confirm_delay)
            ):
                # REST 长时间不可用时不能让信号停摆，先用 WS 数据确认，事后由对账修正
                self.stats["ws_fallback"] += 1
                s.unverified.add(ts)
                self._finalize(s, bar, now)
                continue
            return

    def _finalize(self, s: SymbolSeries, bar: Bar, now: int) -> None:
        closed = s.finalize(bar)
        hits, stale, rejected = self.detector.check(s.symbol, s.bars, closed, now, s.silent_until)
        if now > s.silent_until:
            for ev in rejected:
                log.debug("%s %dm 连续%d次但未通过：%s", s.name, ev.tf, ev.length, ",".join(ev.reasons))
        for ev in stale:
            self.stats["stale"] += 1
            log.info("过期信号（不报警）%s %dm %s %d次 %+.2f%% 延迟 %.0fs", s.name, ev.tf,
                     "涨" if ev.direction == 1 else "跌", ev.length, ev.move_pct,
                     (now - ev.bar_ts - ev.tf * MIN) / 1000)
        for hit in hits:
            self.stats["alerts"] += 1
            self.notifier.emit(Alert(
                symbol=s.name,
                direction="up" if hit.direction == 1 else "down",
                level=hit.level,
                price=bar.c,
                time=bar.ts + MIN,
                triggered=hit.evaluations,
                resonance=hit.resonance,
                quote_volume_24h=self.universe.quote_volume.get(s.symbol, 0),
                delay_ms=max(0, now - bar.ts - MIN),
                congested=self._congested(),
                extra={"unverified": True} if bar.ts in s.unverified else {},
            ))


def _fmt_ts(ts: int | None) -> str:
    if ts is None:
        return "-"
    return time.strftime("%m-%d %H:%M", time.localtime(ts / 1000))


def _short(names: set[str], n: int = 8) -> str:
    names = sorted(names)
    return "[" + ",".join(names[:n]) + (",..." if len(names) > n else "") + "]" if names else ""
