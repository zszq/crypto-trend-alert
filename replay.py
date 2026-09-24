"""历史回放：用最近几天的 1m 数据跑一遍规则，统计报警频率，用来调过滤阈值。

用法：python replay.py --days 2 [--top 30] [-c config.yaml]
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import time
from collections import Counter

from trendalert.candles import MIN, Bar, SymbolSeries
from trendalert.config import load_config
from trendalert.exchange import build_exchanges
from trendalert.signals import Detector
from trendalert.universe import Universe


async def fetch_history(rest, symbol: str, since: int, end: int, sem: asyncio.Semaphore) -> list[Bar]:
    out: list[Bar] = []
    cursor = since
    while cursor < end:
        async with sem:
            rows = await rest.fetch_ohlcv(symbol, "1m", since=cursor, limit=1999)
        if not rows:
            break
        out.extend(Bar.from_ohlcv(r, "rest") for r in rows if r[0] >= cursor)
        nxt = int(rows[-1][0]) + MIN
        if nxt <= cursor:
            break
        cursor = nxt
    # 去掉未收盘的最后一根
    return [b for b in out if b.ts + MIN <= end]


def simulate(symbol: str, bars: list[Bar], cfg, warm: int, detector: Detector, stats: dict) -> None:
    s = SymbolSeries(symbol, cfg.signal.timeframes)
    if not bars:
        return
    big = max(cfg.signal.timeframes) * MIN
    start = (bars[0].ts + big - 1) // big * big
    s.finalized_ts = start - MIN
    prev = None
    by_ts = {b.ts: b for b in bars}
    ts = start
    while ts <= bars[-1].ts:
        b = by_ts.get(ts) or (Bar.flat(ts, prev) if prev is not None else None)
        if b is None:
            ts += MIN
            s.finalized_ts = ts - MIN
            continue
        prev = b.c
        closed = s.finalize(b)
        now = ts + MIN
        # 回放按"刚收盘就确认"处理，前 warm 分钟只用于积累历史
        hits, _, rejected = detector.check(symbol, s.bars, closed, now, silent_until=start + warm * MIN)
        if now > start + warm * MIN:
            for ev in rejected:
                for r in ev.reasons:
                    stats["reject"][(ev.tf, r.split("<")[0].rstrip("0123456789.+-%"))] += 1
            for h in hits:
                stats["level"][h.level] += 1
                stats["dir"]["涨" if h.direction == 1 else "跌"] += 1
                for ev in h.evaluations:
                    stats["tf"][ev.tf] += 1
                stats["symbol"][symbol] += 1
        ts += MIN


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--config", default="config.yaml")
    ap.add_argument("--days", type=float, default=2)
    ap.add_argument("--top", type=int, default=0, help="只回放成交额前 N 个，0 表示全部")
    args = ap.parse_args()
    cfg = load_config(args.config)
    ex = build_exchanges(cfg)
    rest = ex.rest
    try:
        await ex.bulk.load_markets()
        rest.set_markets_from_exchange(ex.bulk)
        tickers = await ex.bulk.fetch_tickers(params={"type": "swap"})
        uni = Universe(cfg.universe)
        uni.update(ex.bulk.markets, tickers, time.time())
        symbols = sorted(uni.active, key=lambda s: -uni.quote_volume.get(s, 0))
        if args.top:
            symbols = symbols[: args.top]
        warm = 400
        end = int(time.time() * 1000) // MIN * MIN
        since = end - int(args.days * 1440 + warm) * MIN
        print(f"回放 {len(symbols)} 个合约，{args.days} 天，拉取历史中...", flush=True)
        sem = asyncio.Semaphore(cfg.feed.rest_concurrency)
        histories = await asyncio.gather(*(fetch_history(rest, s, since, end, sem) for s in symbols), return_exceptions=True)
    finally:
        for e in (ex.rest, ex.bulk, ex.ws):
            await e.close()

    detector = Detector(cfg.signal)
    stats = {"level": Counter(), "dir": Counter(), "tf": Counter(), "symbol": Counter(), "reject": Counter()}
    for sym, hist in zip(symbols, histories):
        if isinstance(hist, Exception):
            print(f"{rest.market(sym)['id']} 拉取失败：{hist!r}")
            continue
        simulate(rest.market(sym)["id"], hist, cfg, warm, detector, stats)

    total = sum(stats["level"].values())
    per_day = lambda n: f"{n / args.days:.0f}/天"
    print(f"\n报警总数 {total}（{per_day(total)}）")
    print("按等级：", {k: per_day(v) for k, v in stats["level"].items()})
    print("按方向：", {k: per_day(v) for k, v in stats["dir"].items()})
    print("按周期触发：", {f"{k}m": per_day(v) for k, v in sorted(stats["tf"].items())})
    print("报警最多的合约：", stats["symbol"].most_common(10))
    print("未通过过滤的原因（次数，含同一段连涨的重复判断）：")
    for (tf, reason), n in sorted(stats["reject"].items()):
        print(f"  {tf}m {reason}: {n}")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    kwargs = {"loop_factory": asyncio.SelectorEventLoop} if sys.platform == "win32" else {}
    asyncio.run(main(), **kwargs)
