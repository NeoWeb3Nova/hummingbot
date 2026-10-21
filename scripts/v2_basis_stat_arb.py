import asyncio
import json
import math
import os
import time
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
from pydantic import Field, field_validator

from hummingbot.client.ui.interface_utils import format_df_for_printout
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import MarketDict, OrderType, PositionAction, PositionMode, PriceType, TradeType
from hummingbot.core.event.events import FundingPaymentCompletedEvent
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class BasisStatArbitrageConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    dry_run: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": lambda mi: "Run in dry-run simulation mode without real orders? (True/False): ",
            "prompt_on_new": True,
        }
    )
    connectors: Set[str] = Field(
        default="hyperliquid_perpetual,binance_perpetual",
        json_schema_extra={
            "prompt": lambda mi: "Enter the connectors separated by commas (e.g. hyperliquid_perpetual,binance_perpetual): ",
            "prompt_on_new": True}
    )
    tokens: Set[str] = Field(
        default="BTC,ETH,SOL,AVAX,SUI,DOGE,NEAR,APT,ARB,OP,LINK",
        json_schema_extra={
            "prompt": lambda mi: "Enter high-liquidity tokens separated by commas (e.g. BTC,ETH,SOL,AVAX,SUI,DOGE): ",
            "prompt_on_new": True
        }
    )
    position_size_quote: Decimal = Field(
        default=Decimal("1500.0"),
        json_schema_extra={
            "prompt": lambda mi: "Enter position size in quote asset per leg (e.g. 1500.0 USD): ",
            "prompt_on_new": True
        }
    )
    leverage: int = Field(
        default=3, gt=0,
        json_schema_extra={"prompt": lambda mi: "Enter leverage (e.g. 3): ", "prompt_on_new": True}
    )
    min_entry_basis_pct: Decimal = Field(
        default=Decimal("0.0070"),
        json_schema_extra={
            "prompt": lambda mi: "Min absolute basis deviation to enter (e.g. 0.0070 for 0.70%): ",
            "prompt_on_new": True
        }
    )
    take_profit_basis_compression_pct: Decimal = Field(
        default=Decimal("0.0015"),
        json_schema_extra={
            "prompt": lambda mi: "Take profit when basis compresses below (e.g. 0.0015 for 0.15%): ",
            "prompt_on_new": True
        }
    )
    min_take_profit_net_pct: Decimal = Field(
        default=Decimal("0.0050"),
        json_schema_extra={
            "prompt": lambda mi: "Min net PnL ratio to take profit (e.g. 0.0050 for +0.50%): ",
            "prompt_on_new": True
        }
    )
    max_basis_loss_pct: Decimal = Field(
        default=Decimal("0.0150"),
        json_schema_extra={
            "prompt": lambda mi: "Emergency stop loss if basis diverges against us by (e.g. 0.0150 for 1.50%): ",
            "prompt_on_new": True
        }
    )
    max_hold_hours: Decimal = Field(
        default=Decimal("6.0"),
        json_schema_extra={
            "prompt": lambda mi: "Max holding hours before evaluating timeout exit (e.g. 6.0): ",
            "prompt_on_new": True
        }
    )
    entry_persistence_ticks: int = Field(
        default=2,
        json_schema_extra={
            "prompt": lambda mi: "Number of consecutive ticks basis must persist before entry (e.g. 2): ",
            "prompt_on_new": True
        }
    )
    stop_loss_confirmation_ticks: int = Field(
        default=5,
        json_schema_extra={
            "prompt": lambda mi: "Number of consecutive ticks divergence must persist before stop loss (e.g. 5): ",
            "prompt_on_new": True
        }
    )
    reentry_cooldown_seconds: int = Field(
        default=600,
        json_schema_extra={
            "prompt": lambda mi: "Cooldown seconds after closing a token before re-entering (e.g. 600): ",
            "prompt_on_new": True
        }
    )
    trailing_stop_activation_pct: Decimal = Field(
        default=Decimal("0.0060"),
        json_schema_extra={
            "prompt": lambda mi: "Net profit threshold to activate trailing stop (e.g. 0.0060 for +0.60%): ",
            "prompt_on_new": True
        }
    )
    trailing_stop_distance_pct: Decimal = Field(
        default=Decimal("0.0020"),
        json_schema_extra={
            "prompt": lambda mi: "Trailing stop retracement distance (e.g. 0.0020 for 0.20%): ",
            "prompt_on_new": True
        }
    )
    require_favorable_funding: bool = Field(
        default=False,
        json_schema_extra={
            "prompt": lambda mi: "Require expected funding carry to be strictly positive? (True/False): ",
            "prompt_on_new": True
        }
    )
    max_adverse_daily_funding_pct: Decimal = Field(
        default=Decimal("0.0010"),
        json_schema_extra={
            "prompt": lambda mi: "Max allowable negative funding per day if favorable funding not required (e.g. 0.0010 for -0.10%/d): ",
            "prompt_on_new": True
        }
    )
    max_orderbook_depth_ratio: Decimal = Field(
        default=Decimal("0.20"),
        json_schema_extra={
            "prompt": lambda mi: "Max position size ratio vs top 25 orderbook depth (e.g. 0.20): ",
            "prompt_on_new": True
        }
    )
    min_holding_seconds: int = Field(
        default=60,
        gt=0,
        json_schema_extra={
            "prompt": lambda mi: "Min holding seconds before allowing take-profit exit (e.g. 60): ",
            "prompt_on_new": True
        }
    )
    exit_persistence_ticks: int = Field(
        default=2,
        ge=1,
        json_schema_extra={
            "prompt": lambda mi: "Number of consecutive ticks take-profit condition must persist (e.g. 2): ",
            "prompt_on_new": True
        }
    )
    taker_fee_rate: Decimal = Field(
        default=Decimal("0.0005"),
        json_schema_extra={
            "prompt": lambda mi: "Estimated taker fee rate per leg (e.g. 0.0005 for 0.05%): ",
            "prompt_on_new": True
        }
    )
    settlement_blackout_seconds: int = Field(
        default=60,
        json_schema_extra={
            "prompt": lambda mi: "Seconds before settlement to avoid adverse entry (e.g. 60): ",
            "prompt_on_new": True
        }
    )

    @field_validator("tokens", mode="before")
    def validate_tokens(cls, v):
        if isinstance(v, str):
            return [val.strip().upper() for val in v.split(",") if val.strip()]
        if isinstance(v, (list, set, tuple)):
            return [str(val).strip().upper() for val in v if str(val).strip()]
        return v

    @field_validator("connectors", mode="before")
    def validate_connectors(cls, v):
        if isinstance(v, str):
            return [val.strip().lower() for val in v.split(",") if val.strip()]
        if isinstance(v, (list, set, tuple)):
            return [str(val).strip().lower() for val in v if str(val).strip()]
        return v

    def update_markets(self, markets: MarketDict) -> MarketDict:
        for connector in self.connectors:
            trading_pairs = set()
            for token in self.tokens:
                trading_pairs.add(BasisStatArbitrage.get_trading_pair_for_connector(token, connector))
            markets[connector] = markets.get(connector, set()) | trading_pairs
        return markets


class BasisStatArbitrage(StrategyV2Base):
    """
    ⚡ 基差统计套利加资金费双击流 (Basis Statistical Arbitrage + Carry Double-Dip Alpha Engine)
    
    量化核心逻辑：
    1. 捕捉主流高流动性代币在 Hyperliquid 与 Binance 盘口深度因做市商补单滞后产生的瞬时基差脱锚（|ΔP| >= 0.7%~1.2%）；
    2. “做空高估交易所 + 做多低估交易所”，同时结合实时资金费率利差实现双击加成（Double Dip）；
    3. 极速均值回归收割：一旦基差压缩回正常区间（|ΔP| <= 0.15%）或综合净收益率达标，立即双边平仓收割利润；
    4. 独立状态持久化至 data/basis_stat_arb_trades.json，与现有 Funding Rate Arb 策略完全物理隔离，0 干扰。
    """

    quote_markets_map = {
        "hyperliquid_perpetual": "USD",
        "binance_perpetual": "USDT",
        "bybit_perpetual": "USDT"
    }

    funding_payment_interval_map = {
        "binance_perpetual": 60 * 60 * 8,
        "hyperliquid_perpetual": 60 * 60 * 1,
        "bybit_perpetual": 60 * 60 * 8
    }

    canonical_token_registry = {
        "PEPE": {
            "binance_perpetual": {"symbol": "1000PEPE", "multiplier": 1000.0},
            "hyperliquid_perpetual": {"symbol": "kPEPE", "multiplier": 1000.0},
            "bybit_perpetual": {"symbol": "1000PEPE", "multiplier": 1000.0},
        },
        "BONK": {
            "binance_perpetual": {"symbol": "1000BONK", "multiplier": 1000.0},
            "hyperliquid_perpetual": {"symbol": "kBONK", "multiplier": 1000.0},
            "bybit_perpetual": {"symbol": "1000BONK", "multiplier": 1000.0},
        },
        "SHIB": {
            "binance_perpetual": {"symbol": "1000SHIB", "multiplier": 1000.0},
            "hyperliquid_perpetual": {"symbol": "kSHIB", "multiplier": 1000.0},
            "bybit_perpetual": {"symbol": "1000SHIB", "multiplier": 1000.0},
        },
        "FLOKI": {
            "binance_perpetual": {"symbol": "1000FLOKI", "multiplier": 1000.0},
            "hyperliquid_perpetual": {"symbol": "kFLOKI", "multiplier": 1000.0},
            "bybit_perpetual": {"symbol": "1000FLOKI", "multiplier": 1000.0},
        },
        "LUNC": {
            "binance_perpetual": {"symbol": "1000LUNC", "multiplier": 1000.0},
            "hyperliquid_perpetual": {"symbol": "kLUNC", "multiplier": 1000.0},
            "bybit_perpetual": {"symbol": "1000LUNC", "multiplier": 1000.0},
        },
        "NEIRO": {
            "binance_perpetual": {"symbol": "NEIRO", "multiplier": 1.0},
            "hyperliquid_perpetual": {"symbol": "kNEIRO", "multiplier": 1000.0},
            "bybit_perpetual": {"symbol": "1000NEIRO", "multiplier": 1000.0},
        },
        "SATS": {
            "binance_perpetual": {"symbol": "1000SATS", "multiplier": 1000.0},
            "hyperliquid_perpetual": {"symbol": "kSATS", "multiplier": 1000.0},
        },
    }

    @classmethod
    def get_token_multiplier(cls, token: str, connector: str) -> float:
        info = cls.canonical_token_registry.get(token, {}).get(connector)
        return info["multiplier"] if info else 1.0

    @classmethod
    def get_token_symbol_for_connector(cls, token: str, connector: str) -> str:
        info = cls.canonical_token_registry.get(token, {}).get(connector)
        return info["symbol"] if info else token

    @classmethod
    def get_trading_pair_for_connector(cls, token: str, connector: str) -> str:
        sym = cls.get_token_symbol_for_connector(token, connector)
        quote = cls.quote_markets_map.get(connector, "USDT")
        return f"{sym}-{quote}"

    @classmethod
    def init_markets(cls, config):
        super().init_markets(config)
        if getattr(config, "dry_run", False):
            cls._enable_dry_run_connectors()

    @classmethod
    def _enable_dry_run_connectors(cls):
        """
        In dry-run mode, patch ConnectorManager to allow perpetual connectors to initialize
        with trading_required=False and empty API keys, enabling live market data streaming
        without requiring private credentials.
        """
        try:
            from hummingbot.client.settings import AllConnectorSettings
            from hummingbot.core.connector_manager import ConnectorManager, get_connector_class
            from hummingbot.core.rate_oracle.rate_oracle import RateOracle

            if getattr(ConnectorManager, "_dry_run_patched", False):
                return

            orig_create = ConnectorManager.create_connector

            def patched_create_connector(self, connector_name: str, trading_pairs=None, trading_required=True, api_keys=None):
                if not api_keys:
                    from hummingbot.client.config.security import Security
                    keys = Security.api_keys(connector_name)
                else:
                    keys = api_keys

                if not keys:
                    conn_setting = AllConnectorSettings.get_connector_settings().get(connector_name)
                    if conn_setting:
                        init_params = conn_setting.conn_init_parameters(
                            trading_pairs=trading_pairs,
                            trading_required=False,
                            api_keys={},
                            balance_asset_limit=self.client_config_map.hb_config.balance_asset_limit,
                            rate_limits_share_pct=self.client_config_map.hb_config.rate_limits_share_pct,
                            gateway_config=self.client_config_map.hb_config.gateway,
                        )
                        connector_class = get_connector_class(connector_name)
                        connector = connector_class(**init_params)
                        self.connectors[connector_name] = connector
                        RateOracle.get_instance().register_connector(connector)
                        self._logger.info(f"[DRY-RUN] Initialized public market connector for '{connector_name}'.")
                        return connector

                return orig_create(self, connector_name, trading_pairs, trading_required, api_keys)

            ConnectorManager.create_connector = patched_create_connector
            ConnectorManager._dry_run_patched = True
        except Exception as e:
            pass

    def __init__(self, connectors: Dict[str, ConnectorBase], config: BasisStatArbitrageConfig):
        super().__init__(connectors, config)
        self.config = config
        self.active_basis_arbitrages: Dict[str, Dict] = {}
        self.stopped_basis_arbitrages: Dict[str, List[Dict]] = {}
        self.entry_candidate_ticks: Dict[str, int] = {}
        self.exit_candidate_ticks: Dict[str, int] = {}
        self.divergence_tick_counter: Dict[str, int] = {}
        self.token_last_closed_timestamp: Dict[str, float] = {}

        # Set leverage and position mode for real trading
        if not self.config.dry_run:
            for connector_name, connector in connectors.items():
                for token in config.tokens:
                    try:
                        tp = self.get_trading_pair_for_connector(token, connector_name)
                        connector.set_position_mode(PositionMode.ONEWAY)
                        connector.set_leverage(tp, config.leverage)
                    except Exception as e:
                        self.logger().warning(f"Error initializing leverage for {connector_name}: {e}")

        # Isolated Persistence Setup: basis_stat_arb_trades.json
        self.data_file_candidates = [
            "/home/hummingbot/data/basis_stat_arb_trades.json",
            os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/basis_stat_arb_trades.json")),
            os.path.abspath(f"/home/neo/projects/hummingbot/hummingbot-api/bots/instances/{getattr(self, 'instance_id', 'basis_stat_arb')}/data/basis_stat_arb_trades.json"),
        ]
        self._load_persisted_state()

        # Initialize MQTT Publisher if available
        self.mqtt_publisher = None
        try:
            from hummingbot.notifier.mqtt_notifier import MqttNotifier
            self.mqtt_publisher = MqttNotifier.get_instance()
            self.logger().info("[BASIS-STAT-ARB] Connected to MQTT for live streaming.")
        except Exception:
            pass

    def _load_persisted_state(self):
        """Load closed trades history and active positions from basis_stat_arb_trades.json if available."""
        for path in self.data_file_candidates:
            if os.path.exists(path):
                try:
                    with open(path, "r") as f:
                        data = json.load(f)
                    closed_list = data.get("closed_trades", [])
                    for item in closed_list:
                        tok = item.get("token", "UNKNOWN")
                        if tok not in self.stopped_basis_arbitrages:
                            self.stopped_basis_arbitrages[tok] = []
                        self.stopped_basis_arbitrages[tok].append(item)
                    active_list = data.get("active_positions", [])
                    now_ts = time.time()
                    for pos in active_list:
                        tok = pos.get("token")
                        if tok and tok not in self.active_basis_arbitrages:
                            pos.setdefault("settled_funding_pnl", 0.0)
                            pos.setdefault("funding_payments", [])
                            if "next_funding_time_1" not in pos:
                                pos["next_funding_time_1"] = int(now_ts + 28800)
                            if "next_funding_time_2" not in pos:
                                pos["next_funding_time_2"] = int(now_ts + 3600)
                            self.active_basis_arbitrages[tok] = pos
                    self.logger().info(f"[BASIS-STAT-ARB] Loaded {len(closed_list)} closed trades & {len(self.active_basis_arbitrages)} active positions from {path}")
                    break
                except Exception as e:
                    self.logger().warning(f"Could not load state from {path}: {e}")

    def on_tick(self):
        if self.ready_to_trade and self.market_data_provider.ready:
            self.check_strategy_commands()
            self.check_funding_settlement()
            actions = self.determine_executor_actions()
            for action in actions:
                self.executor_orchestrator.execute_action(action)
            self.publish_basis_arbitrage_performance()

            # Periodic scan logging (every 30s)
            now = self.current_timestamp
            if not hasattr(self, "_last_scan_log_ts"):
                self._last_scan_log_ts = 0.0
            if now - self._last_scan_log_ts >= 30.0:
                self._last_scan_log_ts = now
                comp = getattr(self, "_latest_market_comparison", [])
                spread_strs = [f"{m['token']}: {m['basis_spread_pct']:+.2%}" for m in comp[:6]]
                thresh = float(self.config.min_entry_basis_pct)
                self.logger().info(
                    f"⚡ [BASIS-SCAN] Monitoring {len(self.config.tokens)} liquid pairs (Threshold: ±{thresh:.2%}). "
                    f"Top spreads: [{', '.join(spread_strs)}]. Active Positions: {len(self.active_basis_arbitrages)}"
                )

    def check_strategy_commands(self):
        """Poll and execute real-time commands from Dashboard UI."""
        now = self.current_timestamp
        if now - getattr(self, "_last_cmd_check_ts", 0.0) < 2.0:
            return
        self._last_cmd_check_ts = now

        cmd_path = "/home/hummingbot/data/basis_commands.json"
        if os.path.exists(cmd_path):
            try:
                with open(cmd_path, "r") as f:
                    cmd = json.load(f)
                os.remove(cmd_path)
                self.logger().info(f"[COMMAND] Received basis stat arb command: {cmd}")
                act = cmd.get("action")
                if act == "close_position":
                    t = cmd.get("token")
                    if t and t in self.active_basis_arbitrages:
                        self._force_close_virtual_position(t, "Manual Force Close via Dashboard")
            except Exception as e:
                self.logger().error(f"Error handling basis command: {e}")

    def check_funding_settlement(self):
        """
        Account for discrete real funding payments across active positions.
        Only when current_timestamp crosses the actual exchange funding settlement window
        is a funding settlement recorded and added to settled_funding_pnl.
        """
        now = self.current_timestamp
        for token, pos in list(self.active_basis_arbitrages.items()):
            if not pos.get("is_virtual", True):
                # In live mode, events are captured by did_complete_funding_payment hook
                continue

            funding_info = self.get_funding_info_by_token(token)
            legs = [
                ("connector_1", "next_funding_time_1", pos["side_1"]),
                ("connector_2", "next_funding_time_2", pos["side_2"]),
            ]
            pos_size = Decimal(str(pos["position_size"]))

            for conn_key, time_key, side_str in legs:
                conn_name = pos[conn_key]
                next_time = pos.get(time_key, 0)

                if now >= next_time and next_time > 0:
                    f_info = funding_info.get(conn_name)
                    if f_info is not None:
                        raw_rate = Decimal(str(f_info.rate))
                        if abs(raw_rate) > Decimal("0.5"):
                            raw_rate = Decimal("0.0001")
                    else:
                        raw_rate = Decimal("0")

                    # Settlement cashflow calculation:
                    # Long (BUY): pays if rate > 0, receives if rate < 0 -> payment = -1 * pos_size * rate
                    # Short (SELL): receives if rate > 0, pays if rate < 0 -> payment = +1 * pos_size * rate
                    if side_str == "BUY":
                        payment = -1 * pos_size * raw_rate
                    else:
                        payment = pos_size * raw_rate

                    payment_float = float(payment)
                    pos.setdefault("funding_payments", []).append({
                        "timestamp": int(now),
                        "connector": conn_name,
                        "side": side_str,
                        "rate": float(raw_rate),
                        "amount": payment_float,
                    })
                    pos["settled_funding_pnl"] = float(pos.get("settled_funding_pnl", 0.0) + payment_float)

                    interval = self.funding_payment_interval_map.get(conn_name, 3600 if "hyperliquid" in conn_name else 28800)
                    next_utc = getattr(f_info, "next_funding_utc_timestamp", 0) if f_info else 0
                    if next_utc > now:
                        pos[time_key] = int(next_utc)
                    else:
                        pos[time_key] = int(now + interval)

                    self.logger().info(
                        f"💰 [BASIS-FUNDING-SETTLED] Token '{token}' on {conn_name} ({side_str}): "
                        f"Rate {float(raw_rate):+.4%}, Cashflow ${payment_float:+.4f}. "
                        f"Cumulative Settled: ${pos['settled_funding_pnl']:+.4f} ({len(pos['funding_payments'])} settlements)"
                    )

    def did_complete_funding_payment(self, funding_payment_completed_event: FundingPaymentCompletedEvent):
        """
        Listen to real exchange funding payments in live trading mode and record actual settlements.
        """
        try:
            raw_token = funding_payment_completed_event.trading_pair.split("-")[0]
            for token, pos in self.active_basis_arbitrages.items():
                if token in raw_token or raw_token in token:
                    amount = float(funding_payment_completed_event.amount)
                    pos.setdefault("funding_payments", []).append({
                        "timestamp": int(self.current_timestamp),
                        "connector": funding_payment_completed_event.connector_name,
                        "trading_pair": funding_payment_completed_event.trading_pair,
                        "rate": float(funding_payment_completed_event.funding_rate),
                        "amount": amount,
                    })
                    pos["settled_funding_pnl"] = pos.get("settled_funding_pnl", 0.0) + amount
                    self.logger().info(
                        f"💰 [LIVE-FUNDING-SETTLED] Token '{token}' received real funding on {funding_payment_completed_event.connector_name}: "
                        f"${amount:+.4f}. Cumulative Settled: ${pos['settled_funding_pnl']:+.4f}"
                    )
                    break
        except Exception as e:
            self.logger().error(f"Error handling live funding payment: {e}")

    def get_executable_quote_price(
        self,
        connector_name: str,
        trading_pair: str,
        quote_volume: Decimal,
        is_buy: bool
    ) -> Optional[Decimal]:
        """
        Strictly retrieve executable price from orderbook for quote volume.
        When is_buy is True, walks the Ask side (cost to buy).
        When is_buy is False, walks the Bid side (proceeds from selling).
        Returns None if orderbook depth is insufficient, price is NaN, <= 0, or an error occurs.
        CRITICAL: Never fall back to MidPrice, LastTradePrice, or dummy constant 1.0!
        """
        try:
            res = self.market_data_provider.get_price_for_quote_volume(
                connector_name=connector_name,
                trading_pair=trading_pair,
                quote_volume=float(quote_volume),
                is_buy=is_buy,
            )
            p = res.result_price
            if p is not None and not math.isnan(p) and p > 0:
                return Decimal(str(p))
        except Exception:
            pass
        return None

    def get_indicative_price(
        self,
        connector_name: str,
        trading_pair: str
    ) -> Optional[Decimal]:
        """
        Retrieve indicative reference price (MidPrice -> LastTradePrice) for valuation and display only.
        Returns None if unavailable. NEVER returns dummy constants like 1.0.
        """
        try:
            mid = self.market_data_provider.get_price_by_type(connector_name, trading_pair, PriceType.MidPrice)
            if mid is not None and not math.isnan(float(mid)) and float(mid) > 0:
                return Decimal(str(mid))
        except Exception:
            pass

        try:
            last = self.market_data_provider.get_price_by_type(connector_name, trading_pair, PriceType.LastTradePrice)
            if last is not None and not math.isnan(float(last)) and float(last) > 0:
                return Decimal(str(last))
        except Exception:
            pass

        return None

    def get_funding_info_by_token(self, token: str) -> Dict:
        funding_rates = {}
        for connector_name in self.config.connectors:
            trading_pair = self.get_trading_pair_for_connector(token, connector_name)
            connector = self.connectors[connector_name]
            funding_info = connector.get_funding_info(trading_pair)
            funding_rates[connector_name] = funding_info
        return funding_rates

    def get_normalized_funding_rate_in_seconds(self, funding_info_report: Dict, connector_name: str) -> Decimal:
        try:
            f_info = funding_info_report.get(connector_name)
            if not f_info:
                return Decimal("0")
            raw_rate = Decimal(str(f_info.rate))
            # Sanity guard: perpetual funding rates are small decimals like 0.0001 (0.01%).
            # Upstream docker image has a known bug reading openInterest instead of funding for Hyperliquid.
            if abs(raw_rate) > Decimal("1.0"):
                raw_rate = Decimal("0.0001")
            interval = self.funding_payment_interval_map.get(connector_name, 60 * 60 * 8)
            return Decimal(str(raw_rate / interval)) if interval > 0 else Decimal("0")
        except Exception:
            return Decimal("0")

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        """
        Evaluate if cross-exchange basis dislocation has opened up on any configured token.
        """
        create_actions = []
        if len(self.config.connectors) < 2:
            return create_actions

        c1, c2 = list(self.config.connectors)[:2]

        for token in self.config.tokens:
            if token in self.active_basis_arbitrages:
                continue

            # Cooldown check
            last_closed = self.token_last_closed_timestamp.get(token, 0)
            if self.current_timestamp - last_closed < self.config.reentry_cooldown_seconds:
                continue

            tp1 = self.get_trading_pair_for_connector(token, c1)
            tp2 = self.get_trading_pair_for_connector(token, c2)

            pos_size = self.config.position_size_quote
            m1 = Decimal(str(self.get_token_multiplier(token, c1)))
            m2 = Decimal(str(self.get_token_multiplier(token, c2)))

            # Query real executable orderbook Bid and Ask for pos_size on BOTH exchanges
            # Sell leg hits Bid (is_buy=False); Buy leg hits Ask (is_buy=True)
            p1_bid = self.get_executable_quote_price(c1, tp1, pos_size, is_buy=False)
            p1_ask = self.get_executable_quote_price(c1, tp1, pos_size, is_buy=True)
            p2_bid = self.get_executable_quote_price(c2, tp2, pos_size, is_buy=False)
            p2_ask = self.get_executable_quote_price(c2, tp2, pos_size, is_buy=True)

            # Strict Guard: If any order book quote cannot be obtained, skip token completely
            # Zero tolerance for dummy fallback prices like MidPrice, LastTradePrice, or 1.0!
            if p1_bid is None or p1_ask is None or p2_bid is None or p2_ask is None:
                self.entry_candidate_ticks[token] = 0
                continue

            # Normalized executable prices
            p1_bid_norm = p1_bid / m1
            p1_ask_norm = p1_ask / m1
            p2_bid_norm = p2_bid / m2
            p2_ask_norm = p2_ask / m2

            # Direction A: Sell C1 @ Bid1, Buy C2 @ Ask2 (Overvalued C1, Undervalued C2)
            spread_A = (p1_bid_norm - p2_ask_norm) / p2_ask_norm

            # Direction B: Buy C1 @ Ask1, Sell C2 @ Bid2 (Undervalued C1, Overvalued C2)
            spread_B = (p2_bid_norm - p1_ask_norm) / p1_ask_norm

            min_entry = self.config.min_entry_basis_pct

            # Determine best executable direction
            if spread_A >= min_entry and spread_A >= spread_B:
                c1_side = TradeType.SELL
                c2_side = TradeType.BUY
                c1_exec_price = p1_bid
                c2_exec_price = p2_ask
                c1_norm = p1_bid_norm
                c2_norm = p2_ask_norm
                executable_basis_spread = spread_A
            elif spread_B >= min_entry and spread_B > spread_A:
                c1_side = TradeType.BUY
                c2_side = TradeType.SELL
                c1_exec_price = p1_ask
                c2_exec_price = p2_bid
                c1_norm = p1_ask_norm
                c2_norm = p2_bid_norm
                executable_basis_spread = spread_B
            else:
                self.entry_candidate_ticks[token] = 0
                continue

            # Double-Dip Funding Synergy Check for selected direction
            funding_info = self.get_funding_info_by_token(token)
            rate1_sec = self.get_normalized_funding_rate_in_seconds(funding_info, c1)
            rate2_sec = self.get_normalized_funding_rate_in_seconds(funding_info, c2)

            if c1_side == TradeType.SELL:
                daily_carry = (rate1_sec - rate2_sec) * 86400
            else:
                daily_carry = (rate2_sec - rate1_sec) * 86400

            if self.config.require_favorable_funding and daily_carry < Decimal("0"):
                self.entry_candidate_ticks[token] = 0
                continue

            if daily_carry < -self.config.max_adverse_daily_funding_pct:
                self.logger().debug(
                    f"[BASIS-SKIP] Token {token} executable basis {float(executable_basis_spread):+.2%}, "
                    f"but adverse funding {float(daily_carry):+.2%}/d exceeds limit."
                )
                self.entry_candidate_ticks[token] = 0
                continue

            # Hysteresis confirmation counter
            req_ticks = self.config.entry_persistence_ticks
            self.entry_candidate_ticks[token] = self.entry_candidate_ticks.get(token, 0) + 1
            if self.entry_candidate_ticks[token] < req_ticks:
                self.logger().info(
                    f"⚡ [BASIS-PULSE] Token '{token}' true executable basis {float(executable_basis_spread):+.2%} "
                    f"({self.entry_candidate_ticks[token]}/{req_ticks} ticks). "
                    f"Direction: {c1_side.name} {c1} @ ${float(c1_exec_price):.4f} / {c2_side.name} {c2} @ ${float(c2_exec_price):.4f}. "
                    f"Indicative Carry: {float(daily_carry):+.2%}/d."
                )
                continue

            # Determine next discrete settlement times
            funding_info_1 = funding_info.get(c1)
            funding_info_2 = funding_info.get(c2)
            next_f1 = getattr(funding_info_1, "next_funding_utc_timestamp", 0) if funding_info_1 else 0
            next_f2 = getattr(funding_info_2, "next_funding_utc_timestamp", 0) if funding_info_2 else 0
            if next_f1 == 0 or next_f1 <= self.current_timestamp:
                next_f1 = int(self.current_timestamp + self.funding_payment_interval_map.get(c1, 3600 if "hyperliquid" in c1 else 28800))
            if next_f2 == 0 or next_f2 <= self.current_timestamp:
                next_f2 = int(self.current_timestamp + self.funding_payment_interval_map.get(c2, 3600 if "hyperliquid" in c2 else 28800))

            # Settlement Blackout Protection:
            # If within settlement_blackout_seconds of funding settlement and carry is adverse, skip!
            blackout_s = getattr(self.config, "settlement_blackout_seconds", 60)
            time_to_f1 = next_f1 - self.current_timestamp
            time_to_f2 = next_f2 - self.current_timestamp
            if (0 <= time_to_f1 <= blackout_s or 0 <= time_to_f2 <= blackout_s) and daily_carry < 0:
                self.logger().info(
                    f"⏳ [SETTLEMENT-BLACKOUT] Token '{token}' within {blackout_s}s of funding settlement "
                    f"with adverse carry ({float(daily_carry):+.2%}/d). Skipping entry."
                )
                self.entry_candidate_ticks[token] = 0
                continue

            # Confirmed Opportunity! Reset counter and enter
            self.entry_candidate_ticks[token] = 0
            c1_amount = pos_size / c1_exec_price
            c2_amount = pos_size / c2_exec_price
            lev_val = Decimal(str(self.config.leverage))
            margin_1 = pos_size / lev_val if lev_val > 0 else pos_size
            margin_2 = pos_size / lev_val if lev_val > 0 else pos_size
            tot_margin = margin_1 + margin_2
            tot_notional = pos_size * Decimal("2")

            self.logger().info(
                f"🚀 [BASIS-ENTRY-TRIGGER] Token '{token}' confirmed executable dislocation: {float(executable_basis_spread):+.3%}!\n"
                f"  Leg 1 ({c1}): {c1_side.name} {float(c1_amount):,.4f} {token} @ ${float(c1_exec_price):.4f} (货值 ${float(pos_size):,.2f} | 保证金 ${float(margin_1):,.2f})\n"
                f"  Leg 2 ({c2}): {c2_side.name} {float(c2_amount):,.4f} {token} @ ${float(c2_exec_price):.4f} (货值 ${float(pos_size):,.2f} | 保证金 ${float(margin_2):,.2f})\n"
                f"  Capital Sizing: 单边货值 ${float(pos_size):,.2f} | 双边总货值 ${float(tot_notional):,.2f} | 实缴总保证金 ${float(tot_margin):,.2f} ({self.config.leverage}x)\n"
                f"  Indicative Carry: {float(daily_carry):+.2%}/d"
            )

            pos_data = {
                "token": token,
                "is_virtual": self.config.dry_run,
                "connector_1": c1,
                "connector_2": c2,
                "side_1": c1_side.name,
                "side_2": c2_side.name,
                "amount_1": float(c1_amount),
                "amount_2": float(c2_amount),
                "entry_price_1": float(c1_exec_price),
                "entry_price_2": float(c2_exec_price),
                "entry_norm_price_1": float(c1_norm),
                "entry_norm_price_2": float(c2_norm),
                "entry_basis_pct": float(executable_basis_spread),
                "target_compression_pct": float(self.config.take_profit_basis_compression_pct),
                "position_size": float(pos_size),
                "single_notional": float(pos_size),
                "total_notional": float(tot_notional),
                "leverage": self.config.leverage,
                "margin_1": float(margin_1),
                "margin_2": float(margin_2),
                "total_margin": float(tot_margin),
                "open_timestamp": self.current_timestamp,
                "expected_daily_carry": float(daily_carry),
                "trailing_active": False,
                "peak_pnl_pct": 0.0,
                # Discrete Funding Settlement Ledger
                "settled_funding_pnl": 0.0,
                "funding_payments": [],
                "next_funding_time_1": int(next_f1),
                "next_funding_time_2": int(next_f2),
            }

            if self.config.dry_run:
                self.active_basis_arbitrages[token] = pos_data
            else:
                pos_cfg_1 = PositionExecutorConfig(
                    timestamp=self.current_timestamp,
                    connector_name=c1,
                    trading_pair=tp1,
                    side=c1_side,
                    entry_price=c1_exec_price,
                    amount=pos_size / c1_exec_price,
                    leverage=self.config.leverage,
                    triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.MARKET),
                )
                pos_cfg_2 = PositionExecutorConfig(
                    timestamp=self.current_timestamp,
                    connector_name=c2,
                    trading_pair=tp2,
                    side=c2_side,
                    entry_price=c2_exec_price,
                    amount=pos_size / c2_exec_price,
                    leverage=self.config.leverage,
                    triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.MARKET),
                )
                pos_data["is_virtual"] = False
                pos_data["executors_ids"] = [pos_cfg_1.id, pos_cfg_2.id]
                self.active_basis_arbitrages[token] = pos_data
                create_actions.extend([CreateExecutorAction(executor_config=pos_cfg_1), CreateExecutorAction(executor_config=pos_cfg_2)])

        return create_actions

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        """
        Evaluate exit conditions for active basis arbitrage positions:
        1. Basis Compression Take-Profit (均值回归压平)
        2. Net Profit Ratio Target Take-Profit (综合净收益达标)
        3. Trailing Take-Profit (利润回撤锁定)
        4. Max Hold Duration Timeout (超时退出释放资金)
        5. Emergency Basis Divergence Stop-Loss (基差反向撕裂硬防爆)
        """
        stop_actions = []
        tokens_to_close = []

        for token, pos in list(self.active_basis_arbitrages.items()):
            c1 = pos["connector_1"]
            c2 = pos["connector_2"]
            tp1 = self.get_trading_pair_for_connector(token, c1)
            tp2 = self.get_trading_pair_for_connector(token, c2)

            pos_size = Decimal(str(pos["position_size"]))
            p1_entry = Decimal(str(pos["entry_price_1"]))
            p2_entry = Decimal(str(pos["entry_price_2"]))

            # To CLOSE leg 1:
            # If opened BUY -> must SELL (is_buy=False, hitting Bid)
            # If opened SELL -> must BUY (is_buy=True, hitting Ask)
            is_buy_close_1 = (pos["side_1"] != "BUY")
            is_buy_close_2 = (pos["side_2"] != "BUY")

            p1_curr = self.get_executable_quote_price(c1, tp1, pos_size, is_buy=is_buy_close_1)
            p2_curr = self.get_executable_quote_price(c2, tp2, pos_size, is_buy=is_buy_close_2)

            # Strict guard: If either executable quote is unavailable, do NOT evaluate exit at corrupted dummy prices!
            if p1_curr is None or p2_curr is None:
                continue

            m1 = Decimal(str(self.get_token_multiplier(token, c1)))
            m2 = Decimal(str(self.get_token_multiplier(token, c2)))
            p1_curr_norm = p1_curr / m1
            p2_curr_norm = p2_curr / m2

            # Current basis: (P1 - P2) / P2
            curr_basis = (p1_curr_norm - p2_curr_norm) / p2_curr_norm
            entry_basis = Decimal(str(pos["entry_basis_pct"]))

            # Trading PnL calculation:
            # Leg 1
            if pos["side_1"] == "BUY":
                leg1_pnl = (p1_curr - p1_entry) * (pos_size / p1_entry)
            else:
                leg1_pnl = (p1_entry - p1_curr) * (pos_size / p1_entry)

            # Leg 2
            if pos["side_2"] == "BUY":
                leg2_pnl = (p2_curr - p2_entry) * (pos_size / p2_entry)
            else:
                leg2_pnl = (p2_entry - p2_curr) * (pos_size / p2_entry)

            # Explicit 4-leg Taker fee calculation (Open Leg1 + Open Leg2 + Close Leg1 + Close Leg2)
            fee_rate = Decimal(str(getattr(self.config, "taker_fee_rate", Decimal("0.0005"))))
            est_roundtrip_fees = pos_size * fee_rate * Decimal("4")
            gross_trading_pnl = leg1_pnl + leg2_pnl
            net_trading_pnl = gross_trading_pnl - est_roundtrip_fees

            # Discrete settled funding cashflow (strictly settled at actual exchange settlement windows)
            settled_funding = Decimal(str(pos.get("settled_funding_pnl", 0.0)))

            # Theoretical indicative accrued carry (for reference only, NEVER counted as realized cashflow)
            duration_s = self.current_timestamp - pos["open_timestamp"]
            duration_h = duration_s / 3600.0
            daily_carry = Decimal(str(pos.get("expected_daily_carry", 0.0)))
            indicative_carry = pos_size * daily_carry * Decimal(str(duration_s / 86400.0))

            # Total Net PnL = Net Trading PnL + Actual Settled Funding!
            total_net_pnl = net_trading_pnl + settled_funding
            net_pnl_pct = total_net_pnl / pos_size if pos_size > 0 else Decimal("0")

            # True ROE on real margin (Dual leg margin = (pos_size / leverage) * 2)
            pos_leverage = Decimal(str(pos.get("leverage", self.config.leverage)))
            real_margin = (pos_size / pos_leverage) * Decimal("2")
            net_roe_pct = total_net_pnl / real_margin if real_margin > 0 else Decimal("0")

            # Update peak PnL for trailing
            peak_pnl = max(pos.get("peak_pnl_pct", 0.0), float(net_pnl_pct))
            pos["peak_pnl_pct"] = peak_pnl

            # ----------------- Exit Conditions -----------------
            # Anti-HFT Churn Defense: Enforce minimum holding time before allowing normal take-profit
            min_hold_s = getattr(self.config, "min_holding_seconds", 60)
            is_min_hold_passed = (duration_s >= min_hold_s)

            # 1. Basis Compression Take-Profit:
            target_comp = Decimal(str(self.config.take_profit_basis_compression_pct))
            is_basis_compressed = abs(curr_basis) <= target_comp
            basis_tp_raw = is_basis_compressed and (total_net_pnl > Decimal("0"))

            # 2. Net Profit Target Take-Profit:
            net_tp_target = self.config.min_take_profit_net_pct
            net_tp_raw = net_pnl_pct >= net_tp_target

            # 3. Trailing Stop:
            trail_active_thresh = self.config.trailing_stop_activation_pct
            trail_distance = self.config.trailing_stop_distance_pct
            if net_pnl_pct >= trail_active_thresh and not pos.get("trailing_active", False):
                pos["trailing_active"] = True
                self.logger().info(f"🛡️ [TRAILING-ARMED] Token '{token}' net PnL reached {float(net_pnl_pct):+.2%}. Trailing stop active!")

            trailing_stop_raw = (
                pos.get("trailing_active", False)
                and Decimal(str(peak_pnl)) - net_pnl_pct >= trail_distance
            )

            # Normal exits require minimum holding time:
            normal_exit_candidate = is_min_hold_passed and (basis_tp_raw or net_tp_raw or trailing_stop_raw)

            # Exit persistence: require 2 consecutive ticks before closing
            req_exit_ticks = getattr(self.config, "exit_persistence_ticks", 2)
            if normal_exit_candidate:
                self.exit_candidate_ticks[token] = self.exit_candidate_ticks.get(token, 0) + 1
            else:
                self.exit_candidate_ticks[token] = 0

            is_exit_confirmed = (self.exit_candidate_ticks.get(token, 0) >= req_exit_ticks)

            basis_tp_condition = is_exit_confirmed and basis_tp_raw
            net_tp_condition = is_exit_confirmed and net_tp_raw
            trailing_stop_condition = is_exit_confirmed and trailing_stop_raw

            # 4. Max Hold Timeout Expiry:
            max_h = float(self.config.max_hold_hours)
            timeout_condition = (duration_h >= max_h) and (total_net_pnl >= -pos_size * Decimal("0.003"))

            # 5. Emergency Basis Divergence Stop-Loss (NEVER restricted by min holding time!):
            max_loss_pct = self.config.max_basis_loss_pct
            divergence_breach = net_pnl_pct <= -max_loss_pct

            req_divergence_ticks = self.config.stop_loss_confirmation_ticks
            if divergence_breach:
                self.divergence_tick_counter[token] = self.divergence_tick_counter.get(token, 0) + 1
            else:
                self.divergence_tick_counter[token] = 0

            stop_loss_condition = self.divergence_tick_counter.get(token, 0) >= req_divergence_ticks

            # Check if any condition triggered
            if basis_tp_condition or net_tp_condition or trailing_stop_condition or timeout_condition or stop_loss_condition:
                if stop_loss_condition:
                    reason = f"Emergency Basis Divergence Stop-Loss ({float(net_pnl_pct):.2%} <= -{float(max_loss_pct):.2%})"
                elif basis_tp_condition:
                    reason = f"Basis Mean-Reversion TP (Basis compressed to {float(curr_basis):+.2%})"
                elif net_tp_condition:
                    reason = f"Net PnL Target Take-Profit (+{float(net_pnl_pct):.2%} >= +{float(net_tp_target):.2%})"
                elif trailing_stop_condition:
                    reason = f"Trailing Stop Take-Profit (Peak: +{peak_pnl:.2%}, Retraced to {float(net_pnl_pct):+.2%})"
                elif timeout_condition:
                    reason = f"Max Duration Timeout Exit ({duration_h:.1f}h >= {max_h:.1f}h)"
                else:
                    reason = "Close Signal"

                tokens_to_close.append({
                    "token": token,
                    "reason": reason,
                    "gross_trading_pnl": float(gross_trading_pnl),
                    "trading_fee": float(est_roundtrip_fees),
                    "net_trading_pnl": float(net_trading_pnl),
                    "settled_funding_pnl": float(settled_funding),
                    "indicative_accrued_carry": float(indicative_carry),
                    "funding_income": float(settled_funding),
                    "total_net_pnl": float(total_net_pnl),
                    "net_pnl_pct": float(net_pnl_pct),
                    "net_roe_pct": float(net_roe_pct),
                    "close_p1": float(p1_curr),
                    "close_p2": float(p2_curr),
                    "close_basis_pct": float(curr_basis),
                    "duration_seconds": int(duration_s),
                })

        for item in tokens_to_close:
            token = item["token"]
            pos = self.active_basis_arbitrages.pop(token)
            self.token_last_closed_timestamp[token] = self.current_timestamp

            # Count unique settlement window timestamps
            unique_windows = len(set(p.get("timestamp") for p in pos.get("funding_payments", []) if p.get("timestamp")))

            pos_size_val = float(pos["position_size"])
            p1_entry_val = float(pos["entry_price_1"])
            p2_entry_val = float(pos["entry_price_2"])
            amt1_val = float(pos.get("amount_1", pos_size_val / p1_entry_val if p1_entry_val > 0 else 0.0))
            amt2_val = float(pos.get("amount_2", pos_size_val / p2_entry_val if p2_entry_val > 0 else 0.0))
            lev_val = float(pos.get("leverage", self.config.leverage))
            tot_notional_val = float(pos.get("total_notional", pos_size_val * 2.0))
            margin1_val = float(pos.get("margin_1", pos_size_val / lev_val if lev_val > 0 else pos_size_val))
            margin2_val = float(pos.get("margin_2", pos_size_val / lev_val if lev_val > 0 else pos_size_val))
            tot_margin_val = float(pos.get("total_margin", (pos_size_val / lev_val) * 2.0 if lev_val > 0 else pos_size_val * 2.0))

            record = {
                "token": token,
                "connector_1": pos["connector_1"],
                "connector_2": pos["connector_2"],
                "side_1": pos["side_1"],
                "side_2": pos["side_2"],
                "amount_1": amt1_val,
                "amount_2": amt2_val,
                "entry_price_1": pos["entry_price_1"],
                "entry_price_2": pos["entry_price_2"],
                "close_price_1": item["close_p1"],
                "close_price_2": item["close_p2"],
                "entry_basis_pct": pos["entry_basis_pct"],
                "close_basis_pct": item["close_basis_pct"],
                "position_size": pos["position_size"],
                "single_notional": pos_size_val,
                "total_notional": tot_notional_val,
                "leverage": pos["leverage"],
                "margin_1": margin1_val,
                "margin_2": margin2_val,
                "total_margin": tot_margin_val,
                "gross_trading_pnl": item["gross_trading_pnl"],
                "trading_fee": item["trading_fee"],
                "net_trading_pnl": item["net_trading_pnl"],
                "trading_pnl": item["net_trading_pnl"],
                "funding_pnl": item["settled_funding_pnl"],
                "indicative_accrued_carry": item["indicative_accrued_carry"],
                "settlement_count": unique_windows,
                "funding_payments": pos.get("funding_payments", []),
                "total_net_pnl": item["total_net_pnl"],
                "net_pnl_pct": item["net_pnl_pct"],
                "net_roe_pct": item["net_roe_pct"],
                "close_reason": item["reason"],
                "open_timestamp": pos["open_timestamp"],
                "close_timestamp": self.current_timestamp,
                "duration_seconds": item["duration_seconds"],
            }

            if token not in self.stopped_basis_arbitrages:
                self.stopped_basis_arbitrages[token] = []
            self.stopped_basis_arbitrages[token].append(record)

            self.logger().info(
                f"🎯 [BASIS-POSITION-CLOSED] Token '{token}' ({item['reason']})!\n"
                f"  Capital Sizing: 单边 ${pos_size_val:,.2f} | 双边 ${tot_notional_val:,.2f} | 实缴保证金 ${tot_margin_val:,.2f} ({lev_val:.0f}x)\n"
                f"  Quantities: {pos['side_1']} {amt1_val:,.4f} {token} ({pos['connector_1']}) + {pos['side_2']} {amt2_val:,.4f} {token} ({pos['connector_2']})\n"
                f"  Trading PnL: Gross ${item['gross_trading_pnl']:+.2f} - Fee ${item['trading_fee']:.2f} = Net ${item['net_trading_pnl']:+.2f}\n"
                f"  Settled Funding: ${item['settled_funding_pnl']:+.2f} ({unique_windows} windows), Total Net: ${item['total_net_pnl']:+.2f} (ROE {item['net_roe_pct']:+.2%}), Held: {item['duration_seconds']}s"
            )

            # If real trading, stop executors
            if not pos.get("is_virtual", True):
                for exec_id in pos.get("executors_ids", []):
                    stop_actions.append(StopExecutorAction(executor_id=exec_id))

        if tokens_to_close:
            self._persist_state()

        return stop_actions

    def _force_close_virtual_position(self, token: str, reason: str):
        if token in self.active_basis_arbitrages:
            pos = self.active_basis_arbitrages.pop(token)
            settled = float(pos.get("settled_funding_pnl", 0.0))
            record = {
                "token": token,
                "connector_1": pos["connector_1"],
                "connector_2": pos["connector_2"],
                "side_1": pos["side_1"],
                "side_2": pos["side_2"],
                "entry_price_1": pos["entry_price_1"],
                "entry_price_2": pos["entry_price_2"],
                "close_price_1": pos["entry_price_1"],
                "close_price_2": pos["entry_price_2"],
                "entry_basis_pct": pos["entry_basis_pct"],
                "close_basis_pct": pos["entry_basis_pct"],
                "position_size": pos["position_size"],
                "leverage": pos["leverage"],
                "trading_pnl": 0.0,
                "funding_pnl": settled,
                "indicative_accrued_carry": 0.0,
                "settlement_count": len(pos.get("funding_payments", [])),
                "funding_payments": pos.get("funding_payments", []),
                "total_net_pnl": settled,
                "net_pnl_pct": settled / pos["position_size"] if pos.get("position_size") else 0.0,
                "close_reason": reason,
                "open_timestamp": pos["open_timestamp"],
                "close_timestamp": self.current_timestamp,
                "duration_seconds": int(self.current_timestamp - pos["open_timestamp"]),
            }
            if token not in self.stopped_basis_arbitrages:
                self.stopped_basis_arbitrages[token] = []
            self.stopped_basis_arbitrages[token].append(record)
            self._persist_state()
            self.logger().info(f"[FORCE-CLOSE] Closed {token}: {reason}")

    def _persist_state(self):
        """Save closed trades and active positions to basis_stat_arb_trades.json."""
        all_closed = []
        for t_list in self.stopped_basis_arbitrages.values():
            all_closed.extend(t_list)

        data = {
            "strategy": "basis_stat_arb",
            "is_dry_run": self.config.dry_run,
            "leverage": self.config.leverage,
            "min_holding_seconds": getattr(self.config, "min_holding_seconds", 60),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "active_positions": list(self.active_basis_arbitrages.values()),
            "closed_trades": all_closed,
            "market_comparison": getattr(self, "_latest_market_comparison", []),
        }

        for path in self.data_file_candidates:
            try:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w") as f:
                    json.dump(data, f, indent=2)
                break
            except Exception:
                pass

    def publish_basis_arbitrage_performance(self):
        """Generate structured report and publish via MQTT & JSON for dashboard."""
        all_closed = []
        for t_list in self.stopped_basis_arbitrages.values():
            all_closed.extend(t_list)

        active_list = []
        for t, pos in self.active_basis_arbitrages.items():
            duration_s = int(self.current_timestamp - pos["open_timestamp"])
            c1 = pos["connector_1"]
            c2 = pos["connector_2"]
            tp1 = self.get_trading_pair_for_connector(t, c1)
            tp2 = self.get_trading_pair_for_connector(t, c2)
            pos_size = float(pos["position_size"])

            # To evaluate current floating exit price, query executable exit side
            # If opened BUY -> exit is SELL (is_buy=False, hitting Bid)
            # If opened SELL -> exit is BUY (is_buy=True, hitting Ask)
            p1_exec = self.get_executable_quote_price(c1, tp1, Decimal(str(pos_size)), is_buy=(pos["side_1"] != "BUY"))
            p2_exec = self.get_executable_quote_price(c2, tp2, Decimal(str(pos_size)), is_buy=(pos["side_2"] != "BUY"))

            p1_curr = float(p1_exec) if p1_exec is not None else float(self.get_indicative_price(c1, tp1) or pos["entry_price_1"])
            p2_curr = float(p2_exec) if p2_exec is not None else float(self.get_indicative_price(c2, tp2) or pos["entry_price_2"])

            m1 = float(self.get_token_multiplier(t, c1))
            m2 = float(self.get_token_multiplier(t, c2))
            p1_norm = p1_curr / m1 if m1 > 0 else p1_curr
            p2_norm = p2_curr / m2 if m2 > 0 else p2_curr
            curr_basis = (p1_norm - p2_norm) / p2_norm if p2_norm > 0 else 0.0

            # Floating PnL estimate
            p1_entry = float(pos["entry_price_1"])
            p2_entry = float(pos["entry_price_2"])

            if pos["side_1"] == "BUY":
                l1 = (p1_curr - p1_entry) * (pos_size / p1_entry)
            else:
                l1 = (p1_entry - p1_curr) * (pos_size / p1_entry)

            if pos["side_2"] == "BUY":
                l2 = (p2_curr - p2_entry) * (pos_size / p2_entry)
            else:
                l2 = (p2_entry - p2_curr) * (pos_size / p2_entry)

            gross_pnl = l1 + l2
            fee_rate = float(getattr(self.config, "taker_fee_rate", 0.0005))
            fees = pos_size * fee_rate * 4.0
            net_trading = gross_pnl - fees
            settled_funding = float(pos.get("settled_funding_pnl", 0.0))
            indicative_carry = float(pos_size * pos.get("expected_daily_carry", 0.0) * (duration_s / 86400.0))

            net_unrealized = net_trading + settled_funding
            unrealized_pct = net_unrealized / pos_size if pos_size > 0 else 0.0

            leverage = float(pos.get("leverage", self.config.leverage))
            real_margin = (pos_size / leverage) * 2.0 if leverage > 0 else pos_size
            unrealized_roe_pct = net_unrealized / real_margin if real_margin > 0 else 0.0

            unique_windows = len(set(p.get("timestamp") for p in pos.get("funding_payments", []) if p.get("timestamp")))

            amt1_val = float(pos.get("amount_1", pos_size / p1_entry if p1_entry > 0 else 0.0))
            amt2_val = float(pos.get("amount_2", pos_size / p2_entry if p2_entry > 0 else 0.0))
            tot_notional_val = float(pos.get("total_notional", pos_size * 2.0))
            margin1_val = float(pos.get("margin_1", pos_size / leverage if leverage > 0 else pos_size))
            margin2_val = float(pos.get("margin_2", pos_size / leverage if leverage > 0 else pos_size))
            tot_margin_val = float(pos.get("total_margin", real_margin))

            active_list.append({
                **pos,
                "amount_1": amt1_val,
                "amount_2": amt2_val,
                "single_notional": pos_size,
                "total_notional": tot_notional_val,
                "margin_1": margin1_val,
                "margin_2": margin2_val,
                "total_margin": tot_margin_val,
                "current_price_1": p1_curr,
                "current_price_2": p2_curr,
                "current_basis_pct": curr_basis,
                "leg1_trading_pnl": l1,
                "leg2_trading_pnl": l2,
                "gross_trading_pnl": gross_pnl,
                "trading_fee": fees,
                "net_trading_pnl": net_trading,
                "unrealized_trading_pnl": net_unrealized,
                "unrealized_pnl_pct": unrealized_pct,
                "unrealized_roe_pct": unrealized_roe_pct,
                "funding_income": settled_funding,
                "settled_funding_pnl": settled_funding,
                "indicative_accrued_carry": indicative_carry,
                "settlement_count": unique_windows,
                "duration_seconds": duration_s,
                "min_holding_seconds": getattr(self.config, "min_holding_seconds", 60),
            })

        # Market basis scan comparison
        basis_comparison = []
        if len(self.config.connectors) >= 2:
            c1, c2 = list(self.config.connectors)[:2]
            for t in self.config.tokens:
                try:
                    tp1 = self.get_trading_pair_for_connector(t, c1)
                    tp2 = self.get_trading_pair_for_connector(t, c2)
                    m1 = float(self.get_token_multiplier(t, c1))
                    m2 = float(self.get_token_multiplier(t, c2))
                    quote_vol = self.config.position_size_quote

                    # Indicative prices for display
                    p1_ind = self.get_indicative_price(c1, tp1)
                    p2_ind = self.get_indicative_price(c2, tp2)

                    # Executable Bid/Ask quotes for determining true executable spreads
                    p1_bid = self.get_executable_quote_price(c1, tp1, quote_vol, is_buy=False)
                    p1_ask = self.get_executable_quote_price(c1, tp1, quote_vol, is_buy=True)
                    p2_bid = self.get_executable_quote_price(c2, tp2, quote_vol, is_buy=False)
                    p2_ask = self.get_executable_quote_price(c2, tp2, quote_vol, is_buy=True)

                    f_info = self.get_funding_info_by_token(t)
                    r1 = float(self.get_normalized_funding_rate_in_seconds(f_info, c1) * 86400)
                    r2 = float(self.get_normalized_funding_rate_in_seconds(f_info, c2) * 86400)

                    if p1_bid is not None and p1_ask is not None and p2_bid is not None and p2_ask is not None:
                        p1_bid_n = float(p1_bid) / m1
                        p1_ask_n = float(p1_ask) / m1
                        p2_bid_n = float(p2_bid) / m2
                        p2_ask_n = float(p2_ask) / m2

                        spread_sell1_buy2 = (p1_bid_n - p2_ask_n) / p2_ask_n
                        spread_buy1_sell2 = (p2_bid_n - p1_ask_n) / p1_ask_n
                        best_spread = max(spread_sell1_buy2, spread_buy1_sell2)

                        thresh = float(self.config.min_entry_basis_pct)
                        if best_spread >= thresh:
                            dir_str = "Short C1/Long C2" if spread_sell1_buy2 >= spread_buy1_sell2 else "Long C1/Short C2"
                            status_str = f"⚡ 偏离超标 {best_spread:+.2%} ({dir_str})"
                        else:
                            status_str = f"⚖️ 窄幅正常 (最高 {best_spread:+.2%})"

                        display_p1 = float(p1_ind) if p1_ind is not None else float(p1_ask)
                        display_p2 = float(p2_ind) if p2_ind is not None else float(p2_ask)
                    else:
                        best_spread = 0.0
                        display_p1 = float(p1_ind) if p1_ind is not None else 0.0
                        display_p2 = float(p2_ind) if p2_ind is not None else 0.0
                        status_str = "⏳ 深度暂缺/无法撮合"

                    basis_comparison.append({
                        "token": t,
                        "price_1": display_p1,
                        "price_2": display_p2,
                        "basis_spread_pct": best_spread,
                        "abs_basis_spread_pct": abs(best_spread),
                        "c1_funding_daily": r1,
                        "c2_funding_daily": r2,
                        "carry_if_short_c1": r1 - r2,
                        "dislocation_status": status_str,
                    })
                except Exception:
                    pass

        self._latest_market_comparison = basis_comparison

        custom_info = {
            "strategy": "basis_stat_arb",
            "is_dry_run": self.config.dry_run,
            "leverage": self.config.leverage,
            "min_entry_basis_pct": float(self.config.min_entry_basis_pct),
            "take_profit_basis_compression_pct": float(self.config.take_profit_basis_compression_pct),
            "active_positions": active_list,
            "closed_trades": all_closed,
            "market_comparison": basis_comparison,
        }

        report = {
            "basis_stat_arb": {
                "performance": {
                    "global_pnl_quote": sum(t.get("total_net_pnl", 0.0) for t in all_closed),
                    "unrealized_pnl_quote": sum(p.get("unrealized_trading_pnl", 0.0) for p in active_list),
                    "volume_traded": sum(t.get("position_size", 0.0) * 4 for t in all_closed) + sum(p.get("position_size", 0.0) * 2 for p in active_list),
                },
                "custom_info": custom_info,
            }
        }

        # Save to disk
        self._persist_state()

        # Publish MQTT
        if self.mqtt_publisher:
            try:
                self.mqtt_publisher.publish(topic="hummingbot/basis_stat_arb/performance", payload=json.dumps(report))
            except Exception:
                pass

    def format_status(self) -> str:
        if not self.ready_to_trade:
            return "Strategy not ready to trade."
        lines = []
        pos_cfg = float(self.config.position_size_quote)
        lev = self.config.leverage
        dual_cfg = pos_cfg * 2.0
        margin_cfg = (dual_cfg / lev) if lev > 0 else dual_cfg

        lines.append("⚡ [Basis Stat Arbitrage + Carry Double-Dip Engine]")
        lines.append(f"  Mode: {'Dry-Run (Paper)' if self.config.dry_run else 'Live Real Trading'}")
        lines.append(f"  Connectors: {', '.join(self.config.connectors)} | Leverage: {lev}x")
        lines.append(f"  Position Sizing Matrix: 单边货值 ${pos_cfg:,.2f} | 双边总敞口 ${dual_cfg:,.2f} | 双边实缴保证金 ${margin_cfg:,.2f}")
        lines.append(f"  Active Positions: {len(self.active_basis_arbitrages)}")
        for tok, pos in self.active_basis_arbitrages.items():
            amt1 = float(pos.get("amount_1", float(pos["position_size"]) / float(pos["entry_price_1"]) if float(pos["entry_price_1"]) > 0 else 0.0))
            amt2 = float(pos.get("amount_2", float(pos["position_size"]) / float(pos["entry_price_2"]) if float(pos["entry_price_2"]) > 0 else 0.0))
            lines.append(
                f"    - {tok}: {pos['side_1']} {amt1:,.2f} @ ${pos['entry_price_1']:.4f} ({pos['connector_1']}) "
                f"+ {pos['side_2']} {amt2:,.2f} @ ${pos['entry_price_2']:.4f} ({pos['connector_2']}) "
                f"[单边 ${pos['position_size']:,.0f} | 双边 ${pos['position_size']*2:,.0f} | 保证金 ${(pos['position_size']/pos['leverage'])*2:,.0f}]"
            )
        all_closed = sum(len(l) for l in self.stopped_basis_arbitrages.values())
        lines.append(f"  Closed Trades: {all_closed}")
        return "\n".join(lines)
