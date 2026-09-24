from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

import aiohttp

from .config import NotifyConfig, WebhookConfig
from .signals import LEVELS, Evaluation

log = logging.getLogger("trendalert")

LEVEL_CN = {"low": "低", "medium": "中", "high": "高"}


@dataclass
class Alert:
    symbol: str
    direction: str                 # up / down
    level: str
    price: float
    time: int                      # 触发的 1m 收盘时间（服务器时间 ms）
    triggered: list[Evaluation]
    resonance: list[int]
    quote_volume_24h: float
    delay_ms: int
    congested: bool
    extra: dict = field(default_factory=dict)

    def text(self) -> str:
        arrow = "连涨" if self.direction == "up" else "连跌"
        trig = " ".join(
            f"{e.tf}m({e.length}次 {e.move_pct:+.2f}%"
            + (f" 量x{e.vol_ratio:.1f}" if e.vol_ratio is not None else "")
            + ")"
            for e in self.triggered
        )
        res = ",".join(f"{tf}m" for tf in self.resonance)
        t = datetime.fromtimestamp(self.time / 1000, timezone.utc).astimezone().strftime("%H:%M:%S")
        s = (
            f"[{LEVEL_CN[self.level]}] {arrow} {self.symbol} 价格 {self.price:g} | 触发 {trig} | "
            f"共振 {res} | 24h额 {self.quote_volume_24h / 1e6:.1f}M | {t} 延迟 {self.delay_ms / 1000:.1f}s"
        )
        if self.congested:
            s += " | 网络阻塞"
        return s

    def to_dict(self) -> dict:
        d = asdict(self)
        d["text"] = self.text()
        return d


def setup_logging(cfg: NotifyConfig) -> logging.Logger:
    Path(cfg.log_dir).mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    root = logging.getLogger("trendalert")
    root.setLevel(logging.DEBUG)
    root.handlers.clear()
    fh = RotatingFileHandler(Path(cfg.log_dir) / "trendalert.log", maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    root.addHandler(fh)
    if cfg.console:
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(fmt)
        root.addHandler(ch)
    return root


class Notifier:
    def __init__(self, cfg: NotifyConfig, min_level: str):
        self.cfg = cfg
        self.min_level = LEVELS.index(min_level)
        self._alert_file = Path(cfg.log_dir) / "alerts.jsonl"
        self._queue: asyncio.Queue[tuple[WebhookConfig, dict]] = asyncio.Queue(maxsize=1000)
        self._session: aiohttp.ClientSession | None = None
        self._worker: asyncio.Task | None = None

    async def start(self) -> None:
        if self.cfg.webhooks:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
            self._worker = asyncio.create_task(self._run())

    async def close(self) -> None:
        if self._worker:
            self._worker.cancel()
        if self._session:
            await self._session.close()

    def emit(self, alert: Alert) -> None:
        log.warning("预警 %s", alert.text())
        try:
            with self._alert_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(alert.to_dict(), ensure_ascii=False) + "\n")
        except OSError:
            log.exception("写入 alerts.jsonl 失败")
        if LEVELS.index(alert.level) < self.min_level:
            return
        for hook in self.cfg.webhooks:
            try:
                self._queue.put_nowait((hook, alert.to_dict()))
            except asyncio.QueueFull:
                # webhook 长时间不可用时丢弃，不能让报警堆积拖垮主流程
                log.error("webhook 队列已满，丢弃报警 %s", alert.symbol)

    async def _run(self) -> None:
        assert self._session is not None
        while True:
            hook, payload = await self._queue.get()
            body = _format(hook, payload)
            for attempt in range(3):
                try:
                    async with self._session.post(hook.url, json=body) as resp:
                        if resp.status < 300:
                            break
                        log.warning("webhook %s 返回 %s: %s", hook.format, resp.status, (await resp.text())[:200])
                except Exception as e:
                    log.warning("webhook %s 发送失败(%d): %r", hook.format, attempt + 1, e)
                await asyncio.sleep(2 ** attempt)


def _format(hook: WebhookConfig, payload: dict) -> dict:
    text = payload["text"]
    if hook.keyword:
        text = f"{hook.keyword} {text}"
    if hook.format == "dingtalk" or hook.format == "wecom":
        return {"msgtype": "text", "text": {"content": text}}
    if hook.format == "feishu":
        return {"msg_type": "text", "content": {"text": text}}
    return payload
