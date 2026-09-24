# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

监控 Gate USDT 永续合约（24h 成交额 ≥ 500 万 U），在 1m/3m/5m/15m 上检测“连续 N 次收盘价高于/低于上一根”，经过滤、去重、共振分级后报警。纯 Python asyncio，依赖 ccxt / ccxt.pro、aiohttp、PyYAML。业务规则与数据链路的完整说明见 `README.md`。

## 常用命令

```bash
pip install -r requirements.txt
python main.py [-c config.yaml]                  # 实盘运行，需要能连上 Gate（可在 config.yaml 配置代理）
python replay.py --days 2 [--top 30]             # 拉历史 1m 回放，统计报警频率，用于调阈值（需联网）
python -m unittest discover -s tests -t .        # 全部测试（离线，只覆盖 candles/signals 纯逻辑）
python -m unittest tests.test_core.TestSignals.test_streak   # 单个测试
```

没有 lint/格式化配置，也没有构建步骤。运行输出在 `logs/trendalert.log`（DEBUG 含未通过过滤的原因）和 `logs/alerts.jsonl`。

## 架构要点

模块分层：`exchange`（ccxt 封装、服务器时钟）→ `engine`（数据链路编排）→ `candles`（1m 存储与合成）→ `signals`（判断/去重/分级）→ `notifier`（日志、jsonl、webhook）。`universe` 独立维护标的池（进出门槛滞回）。

**核心不变式：1m 必须严格按时间顺序、无缺口地确认（`SymbolSeries.finalize`）。** 合成周期和信号判断都依赖这一点才是确定的。
- `SymbolSeries.raw` 存未确认的 1m，`bars[tf]` 存已确认序列；`finalized_ts` 是已确认到的最后一根开盘时间，`next_ts()` 是下一根要确认的。
- 已确认的 K 线不会再被 WS 改写；修正只能走 REST 对账（`SymbolSeries.correct`，会重算受影响的合成桶）。避免信号结果来回变。
- `Bar.src` 决定信任度：`rest`/`fill` 可直接确认；`ws` 必须满足收盘条件（`w=true`、已有下一分钟数据、或 `ws_through` 超过收盘+宽限）且该分钟不与 WS 断开区间重叠（`ws_trusted_for`）。否则由 `Engine._advance` 触发 REST 回补。
- 缺口处理：REST 后面还有数据而某分钟没有，视为无成交，补一根平线（`Bar.flat`）保持连续。
- 合成周期按 UTC 整点分桶，桶内 1m 不完整（如预热起点落在桶中间）则丢弃该桶。因此周期必须整除 1440。

**时间：** 所有时间戳是毫秒、服务器时间，`Bar.ts` 为开盘时间。`ServerClock` 用 WS 推送里的服务器时间估算偏差（取窗口最小值，偏保守），判断收盘一律用 `clock.server_now()` / `clock.ws_through`，不要用本地时间。

**GateWS 依赖 ccxt 内部实现：** `exchange.GateWS` 重写 `handle_ohlcv` 旁路原始推送，以拿到 ccxt 解析时丢弃的 `w` 字段和服务器时间；`Engine._watchdog_loop` 直接对 `self.ws.clients` 调 `client.on_error` 强制重连。升级 ccxt 时需要确认这些内部接口仍然存在。WS 回调里的异常必须吞掉，否则会破坏 ccxt 的订阅状态。

**`signals.Detector` 由实盘 `engine._finalize` 和 `replay.simulate` 共用**，修改信号逻辑会同时影响两者。要点：
- 过滤条件（涨跌幅、量比、实体）都只看最近 N 根；需要 N+1 根 K 线。
- 去重键 `(symbol, tf, direction, streak_start_ts)`，同一段连涨只报一次；未通过过滤的不记入去重。
- `silent_until`：预热期间产生的信号只记入去重、不报警；超过 `stale_periods` 个周期才确认的信号记为过期、只写日志。
- 共振 = 当前同方向满足连续条件的周期数（不看过滤条件），决定 low/medium/high 等级；`min_level` 只影响 webhook 推送。

## 配置

`config.yaml` ↔ `trendalert/config.py` 的 dataclass。新增配置项需同时改两处；嵌套项（`signal.volume`、`signal.body`、`notify.webhooks`）在 `load_config` 里手动构造，`min_move_pct` 的键会被转成 int。约束在 `_validate`：`timeframes` 必须含 1 且都整除 1440；`warmup_minutes` ≤ 1990（Gate 单次最多返回 1999 根，预热只发一次请求）；`proxy.http` 与 `proxy.socks` 互斥。

## Windows 注意事项

入口脚本在 Windows 上使用 `asyncio.SelectorEventLoop`（aiodns 等在 Proactor 循环下不可用），并把 stdout/stderr 重设为 UTF-8。新增入口脚本时保持一致。
