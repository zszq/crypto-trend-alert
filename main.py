from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from trendalert.config import load_config
from trendalert.engine import Engine
from trendalert.notifier import Notifier, setup_logging


async def main(config_path: str) -> None:
    cfg = load_config(config_path)
    log = setup_logging(cfg.notify)
    notifier = Notifier(cfg.notify, cfg.signal.min_level)
    await notifier.start()
    engine = Engine(cfg, notifier)
    log.info("启动：周期 %s，连续 %d 次比较，方向 %s", cfg.signal.timeframes, cfg.signal.comparisons, cfg.signal.directions)
    try:
        await engine.run()
    finally:
        await engine.close()
        await notifier.close()
        log.info("已退出")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gate USDT 永续合约连涨/连跌预警")
    parser.add_argument("-c", "--config", default="config.yaml")
    args = parser.parse_args()
    # 输出被重定向到文件时 Windows 默认用 GBK 编码，统一成 UTF-8 避免中文乱码
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    kwargs = {}
    if sys.platform == "win32":
        # aiodns 等依赖在 Windows 默认的 Proactor 循环下不可用
        kwargs["loop_factory"] = asyncio.SelectorEventLoop
    try:
        asyncio.run(main(args.config), **kwargs)
    except KeyboardInterrupt:
        logging.getLogger("trendalert").info("收到中断信号")
