from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from .candles import Bar
from .config import SignalConfig

LEVELS = ("low", "medium", "high")


def streak(bars: Sequence[Bar]) -> tuple[int, int]:
    """返回 (方向, 连续比较次数)。方向 1=连涨 -1=连跌 0=无；收盘价相等即中断。"""
    n = len(bars)
    if n < 2:
        return 0, 0
    last, prev = bars[-1].c, bars[-2].c
    if last == prev:
        return 0, 0
    d = 1 if last > prev else -1
    k = 1
    i = n - 2
    while i >= 1:
        a, b = bars[i].c, bars[i - 1].c
        if (a > b and d == 1) or (a < b and d == -1):
            k += 1
            i -= 1
        else:
            break
    return d, k


@dataclass
class Evaluation:
    tf: int
    direction: int
    length: int
    start_ts: int          # 连涨/连跌起点（第 0 根）开盘时间，用作去重键
    bar_ts: int            # 触发时最新一根的开盘时间
    move_pct: float
    vol_ratio: float | None
    passed: bool
    reasons: list[str] = field(default_factory=list)


def evaluate(bars: Sequence[Bar], tf: int, cfg: SignalConfig) -> Evaluation | None:
    d, k = streak(bars)
    n = cfg.comparisons
    if k < n or d == 0:
        return None
    if ("up" if d == 1 else "down") not in cfg.directions:
        return None
    bars = list(bars)
    base = bars[-(n + 1)]
    recent = bars[-n:]
    reasons: list[str] = []

    # 过滤条件都只看最近 N 根：连涨延长时，判断的是"最近这一段"是否仍然有力度
    move_pct = (recent[-1].c / base.c - 1) * 100 if base.c else 0.0
    min_move = cfg.min_move_pct.get(tf, 0.0)
    if abs(move_pct) < min_move:
        reasons.append(f"涨跌幅{move_pct:+.2f}%<{min_move}%")

    vol_ratio = None
    if cfg.volume.enabled:
        lb = cfg.volume.lookback
        if len(bars) < n + lb:
            reasons.append("量能历史不足")
        else:
            hist = bars[-(n + lb):-n]
            hist_avg = sum(b.v for b in hist) / lb
            recent_avg = sum(b.v for b in recent) / n
            vol_ratio = recent_avg / hist_avg if hist_avg > 0 else float("inf")
            if vol_ratio < cfg.volume.min_ratio:
                reasons.append(f"量比{vol_ratio:.2f}<{cfg.volume.min_ratio}")

    if cfg.body.enabled:
        for b in recent:
            rng = b.h - b.l
            body = (b.c - b.o) * d
            if body <= 0 or rng <= 0 or body / rng < cfg.body.min_body_ratio:
                reasons.append("实体不达标")
                break

    return Evaluation(
        tf=tf,
        direction=d,
        length=k,
        start_ts=bars[-(k + 1)].ts,
        bar_ts=recent[-1].ts,
        move_pct=move_pct,
        vol_ratio=vol_ratio,
        passed=not reasons,
        reasons=reasons,
    )


def resonance(series_bars: dict[int, Sequence[Bar]], direction: int, n: int) -> list[int]:
    """当前各周期最新已收盘序列中，满足同方向连续 N 次比较的周期（不看过滤条件）。"""
    out = []
    for tf, bars in sorted(series_bars.items()):
        d, k = streak(bars)
        if d == direction and k >= n:
            out.append(tf)
    return out


@dataclass
class Hit:
    direction: int
    evaluations: list[Evaluation]
    resonance: list[int]
    level: str


class Detector:
    """对每次 1m 确认产生的收盘周期做判断、去重与共振分级；实盘和回放共用。"""

    def __init__(self, cfg: SignalConfig):
        self.cfg = cfg
        # (symbol, tf, direction, streak_start_ts) -> 记录时间，同一段连涨/连跌只报一次
        self.alerted: dict[tuple, int] = {}

    def check(
        self,
        symbol: str,
        bars: dict[int, Sequence[Bar]],
        closed: list[tuple[int, Bar]],
        now: int,
        silent_until: int = 0,
    ) -> tuple[list[Hit], list[Evaluation], list[Evaluation]]:
        """返回 (报警, 过期信号, 未通过过滤的候选)。"""
        cfg = self.cfg
        fresh: list[Evaluation] = []
        stale: list[Evaluation] = []
        rejected: list[Evaluation] = []
        for tf, tfbar in closed:
            ev = evaluate(bars[tf], tf, cfg)
            if ev is None:
                continue
            if not ev.passed:
                # 未通过过滤的不记入去重，连涨延长后若满足条件仍可报警
                rejected.append(ev)
                continue
            key = (symbol, tf, ev.direction, ev.start_ts)
            if key in self.alerted:
                continue
            self.alerted[key] = tfbar.ts
            end = tfbar.ts + tf * 60_000
            if end <= silent_until:
                continue
            if now - end > cfg.stale_periods * tf * 60_000:
                stale.append(ev)
                continue
            fresh.append(ev)
        hits = []
        for d in (1, -1):
            evs = [e for e in fresh if e.direction == d]
            if evs:
                res = resonance(bars, d, cfg.comparisons)
                hits.append(Hit(d, evs, res, level_of(len(res), cfg)))
        if len(self.alerted) > 20000:
            cutoff = now - 2 * 86400_000
            self.alerted = {k: v for k, v in self.alerted.items() if v >= cutoff}
        return hits, stale, rejected


def level_of(count: int, cfg: SignalConfig) -> str:
    if count >= cfg.levels.get("high", 3):
        return "high"
    if count >= cfg.levels.get("medium", 2):
        return "medium"
    return "low"
