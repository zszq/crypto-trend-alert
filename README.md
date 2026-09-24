# crypto-trend-alert

监控 Gate USDT 永续合约中 24h 成交额大于 500 万 U 的交易对，在 1m / 3m / 5m / 15m 周期上检测“连续 4 次收盘价高于（或低于）上一根收盘价”，经过滤后报警。

## 运行

```bash
pip install -r requirements.txt
python main.py                 # 默认读取 config.yaml
python replay.py --days 2      # 用历史数据回放，统计报警频率，用来调阈值
python -m unittest discover -s tests -t .
```

需要代理时修改 `config.yaml` 中的 `proxy.http` 或 `proxy.socks`（socks 依赖 `aiohttp-socks`）。

## 数据链路

- **标的池**：每 10 分钟调用一次 `fetch_tickers`。成交额达到 500 万才加入，低于 400 万并持续 30 分钟才移除。
- **WebSocket**：通过 ccxt.pro 订阅 `futures.candlesticks` 1m，所有合约共用一条连接。`GateWS` 会旁路一份原始推送，拿到 ccxt 丢弃的 `w`（窗口关闭）字段和服务器时间。
- **收盘确认**：1m 严格按时间顺序、无缺口地确认。满足以下任一条件即视为收盘：收到 `w=true`；已收到下一分钟的数据；连接正常且已过收盘时间加宽限期。
- **REST 回补**：以下情况改用 REST 拉取：K 线所在时段内 WS 断开过、超过 `rest_fallback_seconds` 仍无法确认、某分钟缺数据。REST 确认该分钟没有成交时补一根平线。WS 整体不可用时，这条逻辑会自动变成每分钟 REST 轮询。
- **对账**：每 5 分钟用 REST 核对最近 10 根 1m。有差异就修正，并重算合成周期。
- **合成**：3m/5m/15m 按 UTC 整点分桶，桶内 1m 到齐才算收盘。
- **健康检查**：用服务器时间估算延迟，超过 5 秒标记为网络阻塞；超过 30 秒没有任何消息，就强制断开重连。
- **过期信号**：断线恢复后回补出来的旧信号，如果已超过 1 个周期，只记日志不报警。

## 信号

- 触发：某个周期在收盘时满足同方向连续 N 次比较（需要 N+1 根 K 线）。
- 过滤（均看最近 N 根）：
  - 最小累计涨跌幅
  - 量比（最近 N 根均量 ÷ 之前 20 根均量）
  - 实体方向与占比
- 去重：同一段连涨/连跌只报一次。未通过过滤的不计入去重，连涨延长后满足条件仍会报。
- 共振：当前同方向满足条件的周期个数。2 个为中级，3 个及以上为高级。

## 输出

- 控制台，以及 `logs/trendalert.log`（DEBUG 级别包含未通过过滤的原因）
- `logs/alerts.jsonl`：每条报警一行 JSON
- webhook：支持 generic / dingtalk / wecom / feishu。只推送达到 `min_level` 等级的报警。
