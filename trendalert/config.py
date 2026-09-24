from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class ProxyConfig:
    http: str = ""
    socks: str = ""


@dataclass
class UniverseConfig:
    min_quote_volume: float = 5_000_000
    exit_quote_volume: float = 4_000_000
    exit_after_minutes: float = 30
    refresh_seconds: float = 600
    include: list[str] = field(default_factory=list)
    exclude: list[str] = field(default_factory=list)


@dataclass
class VolumeFilter:
    enabled: bool = True
    lookback: int = 20
    min_ratio: float = 1.5


@dataclass
class BodyFilter:
    enabled: bool = True
    min_body_ratio: float = 0.3


@dataclass
class SignalConfig:
    timeframes: list[int] = field(default_factory=lambda: [1, 3, 5, 15])
    comparisons: int = 4
    directions: list[str] = field(default_factory=lambda: ["up", "down"])
    min_move_pct: dict[int, float] = field(default_factory=lambda: {1: 0.3, 3: 0.5, 5: 0.7, 15: 1.2})
    volume: VolumeFilter = field(default_factory=VolumeFilter)
    body: BodyFilter = field(default_factory=BodyFilter)
    levels: dict[str, int] = field(default_factory=lambda: {"medium": 2, "high": 3})
    min_level: str = "low"
    stale_periods: float = 1.0


@dataclass
class FeedConfig:
    close_grace_seconds: float = 2
    rest_fallback_seconds: float = 8
    stale_ws_seconds: float = 30
    congestion_ms: float = 5000
    warmup_minutes: int = 600
    reconcile_seconds: float = 300
    reconcile_bars: int = 10
    rest_concurrency: int = 5
    rest_timeout_ms: int = 20000


@dataclass
class WebhookConfig:
    url: str
    format: str = "generic"
    keyword: str = ""


@dataclass
class NotifyConfig:
    console: bool = True
    log_dir: str = "logs"
    webhooks: list[WebhookConfig] = field(default_factory=list)


@dataclass
class Config:
    proxy: ProxyConfig = field(default_factory=ProxyConfig)
    universe: UniverseConfig = field(default_factory=UniverseConfig)
    signal: SignalConfig = field(default_factory=SignalConfig)
    feed: FeedConfig = field(default_factory=FeedConfig)
    notify: NotifyConfig = field(default_factory=NotifyConfig)


def load_config(path: str | Path) -> Config:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    sig = dict(raw.get("signal") or {})
    notify = dict(raw.get("notify") or {})
    cfg = Config(
        proxy=ProxyConfig(**(raw.get("proxy") or {})),
        universe=UniverseConfig(**(raw.get("universe") or {})),
        signal=SignalConfig(
            **{k: v for k, v in sig.items() if k not in ("volume", "body", "min_move_pct")},
            volume=VolumeFilter(**(sig.get("volume") or {})),
            body=BodyFilter(**(sig.get("body") or {})),
            # YAML 的键可能被解析成字符串，统一成 int 方便按周期查表
            min_move_pct={int(k): float(v) for k, v in (sig.get("min_move_pct") or SignalConfig().min_move_pct).items()},
        ),
        feed=FeedConfig(**(raw.get("feed") or {})),
        notify=NotifyConfig(
            console=notify.get("console", True),
            log_dir=notify.get("log_dir", "logs"),
            webhooks=[WebhookConfig(**w) for w in (notify.get("webhooks") or [])],
        ),
    )
    _validate(cfg)
    return cfg


def _validate(cfg: Config) -> None:
    tfs = cfg.signal.timeframes
    if 1 not in tfs:
        raise ValueError("signal.timeframes 必须包含 1（其他周期都由 1m 合成）")
    for tf in tfs:
        # 只允许能整除一天的周期，保证与交易所 UTC 整点对齐方式一致
        if 1440 % tf != 0:
            raise ValueError(f"周期 {tf}m 不能整除 1440，无法与交易所对齐")
    if cfg.feed.warmup_minutes > 1990:
        # Gate 合约 K 线单次最多返回 1999 根，预热只发一次请求
        raise ValueError("feed.warmup_minutes 不能超过 1990")
    if cfg.proxy.http and cfg.proxy.socks:
        raise ValueError("proxy.http 与 proxy.socks 只能配置一个")
    if cfg.signal.min_level not in ("low", "medium", "high"):
        raise ValueError("signal.min_level 只能是 low / medium / high")
