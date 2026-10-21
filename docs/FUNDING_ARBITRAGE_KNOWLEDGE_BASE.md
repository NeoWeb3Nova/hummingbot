# Custom funding strategy audit

[2026-10-21 dynamic universe and fee-net selection](FUNDING_UNIVERSE_2026-10-21.md): independent funding observations, safe two-venue admission/rollback, position protection and ranked capital allocation. See the document for test and deployment status.

[2026-10-20 audit and validation](FUNDING_RATE_ARB_AUDIT_2026-10-20.md)

The custom scripts are synchronized with the API audit branch. Official connectors and executor implementations were not changed.


## 2026-10-20：实际部署与公开模式初始化修正

参见 [模拟盘部署与连续验收](FUNDING_PAPER_DEPLOYMENT_2026-10-20.md)：逐币对 readiness、公开交易规则加载、MQTT 延迟初始化、旧账本归档与工程/收益验收边界。资金费回归增至 36 项。


## 2026-10-21：动态池实际部署与新验收窗口

参见 [动态选币模拟盘部署](FUNDING_DYNAMIC_DEPLOYMENT_2026-10-21.md)：原项目已同步，新实例于 13:56 启动；真实发现池外 W 并成功完成双方订阅、完整成本拒绝与回滚。新的独立 24h 窗口于 13:57:34 开始；原过载窗口保留，盈利仍缺成交结算证据。


## 2026-10-21 19:55：基差开仓多维度金额穿透与双边敞口透明化架构 (v2.4.2)

同步基差统计套利策略内核至 Core 仓库，与 API 保持 100% 逐行一致：在 `v2_basis_stat_arb.py` 中完整落地两腿币数、单边名义货值、双边总敞口、各腿与总实缴保证金，以及 4 腿 Taker 摩擦透明化。

