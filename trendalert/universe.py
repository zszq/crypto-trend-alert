from __future__ import annotations

from .config import UniverseConfig


class Universe:
    """按 24h 成交额维护监控标的池，进出门槛分开设置（滞回）。"""

    def __init__(self, cfg: UniverseConfig):
        self.cfg = cfg
        self.active: set[str] = set()
        self.quote_volume: dict[str, float] = {}
        self._below_since: dict[str, float] = {}

    def update(self, markets: dict, tickers: dict, now_s: float) -> tuple[set[str], set[str]]:
        eligible = {
            s for s, m in markets.items()
            if m.get("swap") and m.get("linear") and m.get("settle") == "USDT" and m.get("active", True)
        }
        self.quote_volume = {s: float(t.get("quoteVolume") or 0) for s, t in tickers.items() if s in eligible}

        target = set(self.active)
        for s in eligible:
            qv = self.quote_volume.get(s, 0)
            if qv >= self.cfg.min_quote_volume:
                target.add(s)
                self._below_since.pop(s, None)
            elif s in target and qv < self.cfg.exit_quote_volume:
                since = self._below_since.setdefault(s, now_s)
                if now_s - since >= self.cfg.exit_after_minutes * 60:
                    target.discard(s)
                    self._below_since.pop(s, None)
            else:
                self._below_since.pop(s, None)

        # 下架或暂停的合约立即移除，否则会一直触发回补请求
        target &= eligible
        target |= {s for s in self.cfg.include if s in eligible}
        target -= set(self.cfg.exclude)

        added, removed = target - self.active, self.active - target
        self.active = target
        return added, removed
