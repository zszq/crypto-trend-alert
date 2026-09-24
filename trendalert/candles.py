from __future__ import annotations

from collections import deque

MIN = 60_000


class Bar:
    __slots__ = ("ts", "o", "h", "l", "c", "v", "src", "closed")

    def __init__(self, ts: int, o: float, h: float, l: float, c: float, v: float, src: str, closed: bool = False):
        self.ts = ts
        self.o = o
        self.h = h
        self.l = l
        self.c = c
        self.v = v
        # ws / rest / fill：决定这根 K 线是否可以直接信任
        self.src = src
        # 仅对 WS 数据有意义：收到了 Gate 的 w=true（窗口已关闭）
        self.closed = closed

    @classmethod
    def from_ohlcv(cls, row: list, src: str) -> "Bar":
        return cls(int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5] or 0), src, True)

    @classmethod
    def flat(cls, ts: int, price: float) -> "Bar":
        return cls(ts, price, price, price, price, 0.0, "fill", True)

    def same_as(self, other: "Bar") -> bool:
        # 成交量允许极小误差，WS 与 REST 的累计口径可能有尾差
        return (
            self.o == other.o and self.h == other.h and self.l == other.l and self.c == other.c
            and abs(self.v - other.v) <= max(1e-9, abs(other.v) * 0.001)
        )

    def __repr__(self) -> str:
        return f"Bar({self.ts}, o={self.o}, h={self.h}, l={self.l}, c={self.c}, v={self.v}, {self.src})"


def aggregate(ts: int, bars: list[Bar]) -> Bar:
    return Bar(
        ts,
        bars[0].o,
        max(b.h for b in bars),
        min(b.l for b in bars),
        bars[-1].c,
        sum(b.v for b in bars),
        "agg",
        True,
    )


class SymbolSeries:
    """单个合约的 1m 原始数据、已确认序列与合成周期。

    1m 必须严格按时间顺序、无缺口地确认（finalize），合成周期和信号判断才是确定的；
    缺口由引擎通过 REST 回补或补平线解决，这里只负责存储与合成。
    """

    def __init__(self, symbol: str, timeframes: list[int], keep: int = 500):
        self.symbol = symbol
        self.timeframes = sorted(timeframes)
        # 尚未确认的 1m（可能仍在变化）
        self.raw: dict[int, Bar] = {}
        # 已确认的序列，键为周期（分钟）
        self.bars: dict[int, deque[Bar]] = {tf: deque(maxlen=keep) for tf in self.timeframes}
        self.finalized_ts: int | None = None
        self.max_ws_ts = 0
        # WS 不可用的区间（服务器时间 ms），与之重叠的 1m 不信任 WS 数据
        self.down_since: int | None = None
        self.down_intervals: deque[tuple[int, int]] = deque(maxlen=20)
        self.warmed = False
        self.silent_until = 0
        self.backfill_pending = False
        self.backfill_retry_at = 0
        self.backfill_failures = 0

    # ---------- 原始数据写入 ----------

    def on_ws(self, bar: Bar) -> None:
        if self.finalized_ts is not None and bar.ts <= self.finalized_ts:
            # 已确认的 K 线不再被 WS 改写，修正统一走 REST 对账，避免信号结果来回变
            return
        existing = self.raw.get(bar.ts)
        if existing is not None and existing.src != "ws":
            return
        self.raw[bar.ts] = bar
        if bar.ts > self.max_ws_ts:
            self.max_ws_ts = bar.ts

    def put_trusted(self, bar: Bar) -> None:
        if self.finalized_ts is not None and bar.ts <= self.finalized_ts:
            return
        self.raw[bar.ts] = bar

    # ---------- WS 可用性 ----------

    def mark_down(self, server_ms: int) -> None:
        if self.down_since is None:
            self.down_since = server_ms

    def mark_up(self, server_ms: int) -> None:
        if self.down_since is not None:
            self.down_intervals.append((self.down_since, server_ms))
            self.down_since = None

    def ws_trusted_for(self, ts: int) -> bool:
        end = ts + MIN
        if self.down_since is not None and self.down_since < end:
            return False
        for a, b in self.down_intervals:
            if a < end and b > ts:
                return False
        return True

    # ---------- 确认与合成 ----------

    def next_ts(self) -> int:
        assert self.finalized_ts is not None
        return self.finalized_ts + MIN

    def last_close(self) -> float | None:
        b1 = self.bars[1]
        return b1[-1].c if b1 else None

    def finalize(self, bar: Bar) -> list[tuple[int, Bar]]:
        """确认一根 1m，返回本次收盘的所有周期 (tf, bar)，1m 总在第一个。"""
        self.raw.pop(bar.ts, None)
        bar.closed = True
        self.finalized_ts = bar.ts
        b1 = self.bars[1]
        b1.append(bar)
        closed = [(1, bar)]
        end = bar.ts + MIN
        for tf in self.timeframes:
            if tf == 1 or end % (tf * MIN) != 0:
                continue
            start = end - tf * MIN
            if len(b1) < tf or b1[-tf].ts != start:
                # 预热起点落在桶中间，这个桶不完整，丢弃
                continue
            agg = aggregate(start, [b1[i] for i in range(-tf, 0)])
            self.bars[tf].append(agg)
            closed.append((tf, agg))
        # raw 里比已确认更早的数据已无用
        for ts in [t for t in self.raw if t <= bar.ts]:
            del self.raw[ts]
        return closed

    def correct(self, bar: Bar) -> bool:
        """用 REST 数据修正已确认的 1m，并重算受影响的合成周期；返回是否有改动。"""
        b1 = self.bars[1]
        idx = _find(b1, bar.ts)
        if idx is None or b1[idx].same_as(bar):
            return False
        b1[idx] = bar
        for tf in self.timeframes:
            if tf == 1:
                continue
            start = bar.ts // (tf * MIN) * (tf * MIN)
            tidx = _find(self.bars[tf], start)
            if tidx is None:
                continue
            first = _find(b1, start)
            if first is None or first + tf > len(b1):
                continue
            self.bars[tf][tidx] = aggregate(start, [b1[i] for i in range(first, first + tf)])
        return True


def _find(dq: deque[Bar], ts: int) -> int | None:
    # 需要查找的基本都是最近的数据，从尾部往前扫
    for i in range(len(dq) - 1, -1, -1):
        t = dq[i].ts
        if t == ts:
            return i
        if t < ts:
            return None
    return None
