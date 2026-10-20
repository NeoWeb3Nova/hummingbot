# Funding Rate Arbitrage 审计与修正记录（2026-10-20）

本轮修复的是模拟交易的成交、记账、结算、恢复及资金约束，并修正资金费策略的入场成本判断。尚未证明策略具备正期望，也没有宣称已达到实盘标准。

## 隔离范围

- API 基线：`0ac49e0df7624252f3c34c160a6d034304f0fdc8`。
- Core 基线：`242c957e35a77671aa212f1301e3187d2929f41f`。
- Dashboard 基线：`93ffe003ed82421d544215903526229bea1d1f10`。
- 三库修复分支均为 `audit/funding-rate-20261020`，目录位于 `/home/neo/projects/hummingbot-funding-audit-20261020/`。
- 原目录 `/home/neo/projects/hummingbot/` 的源代码、运行实例、配置和账本未被本次修改。原机器人继续运行的是旧版本。
- 实际运行版本来自 API 的 `bots/scripts/v2_funding_rate_arb.py`；Core 的同名脚本明显落后。本次仅同步这组自定义策略脚本和新增测试，未修改官方连接器、订单簿、Executor 或 StrategyV2Base。

## 核心发现与处理

| 等级 | 原问题 | 修正 |
| --- | --- | --- |
| P0 | Live 分支发出停止指令后直接归档、移除仓位，尚未确认两腿真实成交，也没有可靠的单腿失败补偿 | 审计版配置拒绝 `dry_run=false`，关闭未经验证的实盘路径；没有把模拟修复冒充实盘执行修复 |
| P0 | Dry-run 在存在已保存密钥时可能创建私有交易连接器 | 策略的连接器初始化插槽强制公开行情模式、空密钥；用真实 Binance/Hyperliquid 连接器构造器验证 |
| P1 | 浮动盈亏与平仓盈亏口径不同，平仓只扣一次 entry_fee，却将其称为 exit_fee | 同一计算函数覆盖两腿价格损益、两笔入场费用、两笔退出费用和有符号资金费 |
| P1 | 以等美元名义金额独立计算两腿币数，价格不同就存在剩余方向敞口 | 按统一标的币数及千倍/百万倍合约乘数配平，逐所数量精度量化并检查残差 |
| P1 | 深度报价使用最后一档价格；深度不足时回退中间价、末价甚至常数 1 | 按完整数量逐档求成交均价；缺失、过期、交叉或不足的盘口均拒绝新开仓，原仓位保留并显示估值不可用 |
| P1 | 只要一所取得深度就可能开仓；容量不足时强行抬高到最小金额 | 同时检查两所开仓和退出方向的 Top 25 档，在价格冲击限制内计算容量；不足则拒绝 |
| P1 | 使用当前预测费率作为过去周期的已收资金费；停机漏周期；币安固定按 8 小时 | 使用公共历史已结算费率、按连接器和时间去重并补记；每个币种从 fundingInfo 获取实际周期 |
| P1 | 重启从展示快照恢复，原始币数、手续费、杠杆和冷却状态丢失；JSON 失败可能被空账本覆盖 | schema v2 保存原始仓位、分腿费用、资金费事件、观察名单及风控状态；原子写入；恢复失败禁止覆盖源账本和新增仓位 |
| P1 | 分批止盈没有同比分摊原始费用，且后续判断可能复用拆分前损益 | 拆分币数、费用、保证金及已有资金费金额并保留事件元数据；两腿与剩余仓位须满足最小量/精度；同 tick 不复用原损益再次平仓 |
| P1 | 没有组合总资金及每所抵押资金限制，模拟能无限重复使用资金 | 总模拟资金两所各分一半，独立扣除保证金、费用与不利损益，并保留资金储备；新增仓位数上限和组合损失预算 |
| P1 | 单币行情缺失会阻塞其他仓位风控；1x 被解释为零爆仓风险 | 退出先于入场、逐持仓独立执行；1x 空头仍会强平；采用明确标注的模拟逐仓强平模型并至少保留 8% 防线 |
| P1 | 成功试运行由“止盈/保本”原因字符串判定，即使净亏损也能毕业 | 必须是扣除模型成本后净盈利；关闭理由不能替代财务结果 |
| P2 | 界面部分汇总重复加上已包含在历史净利中的资金费；首笔亏损不计回撤 | 修正重复计数；回撤峰值包含初始 0；优先使用记录的双腿保证金 |
| P2 | 事后剔除亏损币种被称为真实盈利能力，雷达按成交量猜测安全下单金额 | 默认保留全部账本；筛选明确标记假设子集与选择偏差；成交量只能作为参考，安全金额必须来自盘口 |

## 模拟账本口径

`trading_pnl = 两腿平仓价格损益 - 两腿入场费用 - 两腿退出费用`。

`total_net_pnl = trading_pnl + funding_pnl`。UI、入场摩擦测算、风控、平仓、持仓净值使用该口径。对冲匹配使用标的数量，不能把跨所入场价差直接记成已实现利润。

`paper_account_balance_quote` 是虚拟账户起始资金，默认 10,000 USD 等值；每个场所分配一半。`paper_margin_reserve_pct` 默认 25%，避免将全部预算压成仓位。已实现盈亏留在对应交易所，不能自动跨所转移；旧记录缺失分腿归属时只作平均分配估计。

`min_hold_hours` / `min_hold_until` 仅保留为观察指标，本版不以其阻挡风险退出、人工移除或已有止盈条件。原固定持有保护不能成为压住亏损仓位的理由。预算耗尽或信号无法覆盖成本时，无交易是有效结果。

USDT 与 USD 当前按 1:1 估值。模型不模拟脱锚、排队、网络延迟、自己的订单冲击、交易所维持保证金阶梯/交叉保证金、自动减仓、真实清算及账户真实余额。盘口深度模拟不是可成交承诺。

## 资金费的确定性边界

Binance 的预测费率用于比较机会，历史 `fundingRate` 用于补记已结算周期。成功读取完整 `fundingInfo` 后，未调整品种才使用 8 小时默认；接口失败时不能猜测周期。

Hyperliquid 的费率按小时，资金费基于标的数量和 oracle 参考价。公共 fundingHistory 没有同时返回每笔准确 oracle：优先使用结算附近观察到的 oracle，否则使用入场参考价，并明确记录 `price_source` 和 `amount_estimated=true`。已公布费率不等于已获取虚拟仓位的真实账户收款证明。

每笔事件保留连接器、币对、结算时间、方向、币数、费率、参考价、金额和数据来源。持仓起点之后、平仓时刻以内的已发布事件才参与记账，不按“参与了几分钟”连续计提。平仓后仍可能补入先前持仓期间的迟到记录，并更新历史净损益和该所模拟现金。历史缺失保持 pending；长时间缺失阻止新增仓位。

参考官方接口与规则：

- [Binance Funding Rate History](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History)
- [Binance Funding Rate Info](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-Info)
- [Hyperliquid Funding](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/funding)
- [Hyperliquid Perpetual Info API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals)

## 历史数据与收益判断

本轮开始时保存的旧模拟快照：40 条平仓，9 盈利、31 非盈利，交易净损益约 -366.07、资金费约 +21.10、合计约 -344.97（以完整快照复核值为准）。该统计是旧模拟器记账结果，不是已审计真实收益。记录缺失原始费用、币数或入场价格时不能精确重算，不能伪造补齐。迁移保留 closed_trades 的原值；旧持仓标记 `legacy_reconstructed`，资金费从迁移切点之后补记，避免重复算此前现金流。

示例：两所每笔 taker 均为 0.05%，每所单边名义 2,000，则四笔成交费用约 4；资金费差 0.12%/天约 2.4/天，只算费用回本就要 40 小时。项目要求回本不超过 12 小时，因此必须拒绝，即便它达到了原先的最小费率阈值。真实费率取各连接器费用模型，此处数字只用于解释，不代表当前交易所收费报价。

本版保留 12 小时回本、最多 30% 深度、倒挂 -0.1%/天和至少 8% 强平防线。严格计费后机会可能更少；不能调高杠杆或删去历史亏损，来证明策略获得了经济优势。

## 验证与复现

- API/Core：`test/test_funding_arb_audit.py`，31 项回归测试。使用现有 `hummingbot/hummingbot:latest` 本地镜像（镜像 ID `sha256:b676cb7279ca147b76a510777abf515a0d2f1b17e45a70da4831bb03a587c8e1`），无网络临时容器，策略脚本只读挂载。覆盖真实策略类、原生时钟/记录器/停止流程、真实两所公开连接器初始化、四笔费用、数量配平、盘口缺失/过期、资金预算、部分平仓、重启、损坏文件、资金费补记/去重/迟到、冷却/黑名单与 MQTT 一致性。
- Dashboard：`test/test_funding_audit.py`，4 项回归：首笔亏损回撤、资金费只计一次、币种周期、未知周期拒绝信号。使用现有 dashboard Python 环境，不启动网页服务。
- 两套策略脚本必须字节一致。`git diff --check` 通过。原项目审计范围源文件 SHA-256 与基线一致。
- 公共接口只读探测成功：获取 1,059 个合约元数据（Binance 881，HL 178）；Binance 451 个 4h、429 个 8h、1 个 1h。最近 9 小时 BTC 历史取得 Binance 1 条、HL 9 条。这是一次接口连通性核对，不是收益回测或连续运行验收。

测试命令（API worktree 内；不使用账户密钥）：

```bash
docker run --rm --network none --entrypoint python \
  -v "$PWD/bots/scripts:/home/hummingbot/scripts:ro" \
  -v "$PWD/test/test_funding_arb_audit.py:/audit-tests/test_funding_arb_audit.py:ro" \
  hummingbot/hummingbot:latest \
  -m unittest discover -s /audit-tests -p test_funding_arb_audit.py -v
```

Core worktree 对应挂载 `scripts` 和同一个测试文件。仅挂载此测试文件可避免 Core 自带测试包与已安装 hummingbot 模块同名遮蔽。Dashboard worktree 内运行 `python -m unittest discover -s test -p test_funding_audit.py -v`。

## 接入原运行环境的后续边界

当前仅提交独立分支，没有合并、重启或替换现有机器人。新的审计配置样例位于 `docs/examples/funding_rate_arb_paper_audited.yml`。后续接入时必须：

1. 备份旧账本与配置；修正版使用独立实例名称、独立 data 目录和新 ledger，保留旧盘作为对照。若迁移旧仓，只在复制件上做，并持续显示 legacy 标志。
2. 同步模板 `bots/conf/scripts/` 与新实例 `bots/instances/<id>/conf/scripts/`，校验二者配置一致，属主为 1000:1000。不能只替换旧模板，让运行实例继续用 24h 回本或 4% 安全垫。
3. 从完整费用与实际成交深度出发，记录每个候选被拒绝的原因。跨越多个真实结算周期后，逐笔对照公共历史费率、事件去重、总账/分所预算、重启前后状态一致性。
4. 进行前瞻观测和故障演练，再讨论实盘。实盘仍需完成双腿订单状态机、部分成交/拒单/超时补偿、幂等重试、实际仓位核对、成交手续费与账户资金费对账；本次没有实现这些能力。

验收标准是数据与执行模型可靠、费用后机会真实存在、资金使用可解释。无法通过这些条件时，保留不开仓结果，不以“测试通过”替代盈利证明。
