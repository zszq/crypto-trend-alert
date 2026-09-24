from __future__ import annotations

from .config import UniverseConfig


def is_crypto_contract(market: dict) -> bool:
    info = market.get("info") or {}
    # Gate 会给股票、指数、贵金属、外汇、大宗商品填写 contract_type；只接受未分类的币类标的。
    # 严格要求字段存在且为空，接口结构变化时宁可少收也不误收非币类资产。
    return (
        info.get("contract_type") == ""
        and str(info.get("status", "")).lower() == "trading"
        # 下架流程中的合约仍显示 trading，但流动性会迅速枯竭，继续监控只会制造噪声
        and not info.get("in_delisting")
    )


def to_symbol(name: str) -> str:
    """把配置里的 BTC_USDT 写法转成 ccxt 统一符号 BTC/USDT:USDT，两种写法都接受。"""
    name = name.strip().upper()
    if "/" in name:
        return name
    base, _, quote = name.rpartition("_")
    return f"{base}/{quote}:{quote}"


class Universe:
    """按 24h 成交额维护监控标的池，进出门槛分开设置（滞回）。"""

    def __init__(self, cfg: UniverseConfig):
        self.cfg = cfg
        self.active: set[str] = set()
        self.quote_volume: dict[str, float] = {}
        self._below_since: dict[str, float] = {}
        self._include = {to_symbol(x) for x in cfg.include}
        self._exclude = {to_symbol(x) for x in cfg.exclude}

    def update(self, markets: dict, tickers: dict, now_s: float) -> tuple[set[str], set[str]]:
        tradable = {
            s for s, m in markets.items()
            if m.get("swap") and m.get("linear") and m.get("settle") == "USDT" and m.get("active", True)
        }
        eligible = {s for s in tradable if is_crypto_contract(markets[s])}
        self.quote_volume = {s: float(t.get("quoteVolume") or 0) for s, t in tickers.items() if s in tradable}

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

        # 下架、暂停或被重新分类为非币类的合约立即移除，否则会一直触发回补请求
        target &= eligible
        # include 是用户显式指定，允许绕过币类限制
        target |= {s for s in self._include if s in tradable}
        target -= self._exclude

        added, removed = target - self.active, self.active - target
        self.active = target
        return added, removed
