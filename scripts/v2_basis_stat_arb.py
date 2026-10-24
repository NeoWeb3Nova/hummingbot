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
from hummingbot.core.utils.async_utils import safe_ensure_future
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
        default=Decimal("0.0050"),
        json_schema_extra={
            "prompt": lambda mi: "Min absolute basis deviation to enter (e.g. 0.0050 for 0.50%): ",
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
    # --- Optimization 1: Profit-Locked Early Exit Exemption ---
    early_take_profit_net_pct: Decimal = Field(
        default=Decimal("0.0035"),
        json_schema_extra={
            "prompt": lambda mi: "High-conviction net profit threshold to waive 60s cooldown and take profit early (e.g. 0.0035 for +0.35%): ",
            "prompt_on_new": False
        }
    )
    min_early_exit_seconds: int = Field(
        default=10,
        json_schema_extra={
            "prompt": lambda mi: "Minimum holding seconds required before early take-profit can trigger (e.g. 10): ",
            "prompt_on_new": False
        }
    )
    # --- Optimization 2: Toxic Token Quarantine Circuit Breaker ---
    divergence_quarantine_seconds: int = Field(
        default=2700,
        json_schema_extra={
            "prompt": lambda mi: "Quarantine seconds for a token after emergency divergence stop-loss (e.g. 2700 for 45m): ",
            "prompt_on_new": False
        }
    )
    # --- Optimization 4: Breakeven Trailing Floor ---
    breakeven_trailing_floor_pct: Decimal = Field(
        default=Decimal("0.0015"),
        json_schema_extra={
            "prompt": lambda mi: "Minimum guaranteed net profit floor once trailing stop is armed (e.g. 0.0015 for +0.15%): ",
            "prompt_on_new": False
        }
    )
    # --- Optimization 3 (Option 1): Exhaustion Peak & Right-Side Sniper Mechanism (Case 52) ---
    sniper_mode_enabled: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": lambda mi: "Enable Exhaustion Peak & Right-Side Sniper for extreme basis dislocations? (True/False): ",
            "prompt_on_new": False,
        }
    )
    sniper_trigger_basis_pct: Decimal = Field(
        default=Decimal("0.0150"),
        json_schema_extra={
            "prompt": lambda mi: "Dislocation threshold to begin tracking extreme basis prey (e.g. 0.0150 for 1.50%): ",
            "prompt_on_new": False,
        }
    )
    sniper_reversal_confirmation_pct: Decimal = Field(
        default=Decimal("0.0020"),
        json_schema_extra={
            "prompt": lambda mi: "Basis contraction required from peak to confirm right-side exhaustion (e.g. 0.0020 for 20 bps): ",
            "prompt_on_new": False,
        }
    )
    sniper_confirmation_ticks: int = Field(
        default=3,
        json_schema_extra={
            "prompt": lambda mi: "Consecutive ticks contraction must persist to confirm exhaustion peak (e.g. 3): ",
            "prompt_on_new": False,
        }
    )
    sniper_position_size_quote: Decimal = Field(
        default=Decimal("300.0"),
        json_schema_extra={
            "prompt": lambda mi: "Position size quote per leg for sniper orders (e.g. 300.0 USD): ",
            "prompt_on_new": False,
        }
    )
    sniper_take_profit_basis_pct: Decimal = Field(
        default=Decimal("0.0035"),
        json_schema_extra={
            "prompt": lambda mi: "Target basis compression for sniper take-profit (e.g. 0.0035 for 0.35%): ",
            "prompt_on_new": False,
        }
    )
    sniper_stop_loss_distance_pct: Decimal = Field(
        default=Decimal("0.0030"),
        json_schema_extra={
            "prompt": lambda mi: "Emergency invalidation stop if basis expands above peak by (e.g. 0.0030 for 30 bps): ",
            "prompt_on_new": False,
        }
    )
    sniper_max_prey_dwell_seconds: int = Field(
        default=3600,
        json_schema_extra={
            "prompt": lambda mi: "Max seconds to track extreme prey before timeout (e.g. 3600 for 1 hour): ",
            "prompt_on_new": False,
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
            "prompt": lambda mi: "Max position size ratio vs top orderbook depth (e.g. 0.20): ",
            "prompt_on_new": True
        }
    )
    dynamic_capacity_sizing: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": lambda mi: "Dynamically adapt position size based on top 3 orderbook depth? (True/False): ",
            "prompt_on_new": True,
        }
    )
    min_order_size_quote: Decimal = Field(
        default=Decimal("300.0"),
        gt=Decimal("0"),
        json_schema_extra={
            "prompt": lambda mi: "Min order size in quote asset per leg under dynamic capacity sizing (e.g. 300.0 USD): ",
            "prompt_on_new": True,
        }
    )
    min_executable_net_edge_pct: Decimal = Field(
        default=Decimal("0.0018"),
        json_schema_extra={
            "prompt": lambda mi: "Min executable VWAP net edge after taker fees and slippage (e.g. 0.0018 for 18 bps): ",
            "prompt_on_new": True,
        }
    )
    max_executable_depth_slippage_pct: Decimal = Field(
        default=Decimal("0.0025"),
        json_schema_extra={
            "prompt": lambda mi: "Max allowable single-leg orderbook depth slippage (e.g. 0.0025 for 25 bps): ",
            "prompt_on_new": True,
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
    simulated_latency_slippage_pct: Decimal = Field(
        default=Decimal("0.0002"),
        ge=Decimal("0"),
        json_schema_extra={
            "prompt": lambda mi: "Simulated latency adverse slippage per leg in dry run (e.g. 0.0002 for 2.0 bps): ",
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
    auto_rotation_enabled: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": lambda mi: "Enable background universe scanner and dynamic token rotation? (True/False): ",
            "prompt_on_new": False,
        }
    )
    auto_rotation_interval_minutes: float = Field(
        default=3.0,
        ge=0.5,
        json_schema_extra={
            "prompt": lambda mi: "Interval in minutes between full-market background scans (e.g. 3.0): ",
            "prompt_on_new": False,
        }
    )
    auto_rotation_min_entry_basis_pct: Decimal = Field(
        default=Decimal("0.0030"),
        json_schema_extra={
            "prompt": lambda mi: "Min indicative basis for candidate token to qualify for admission (e.g. 0.0030 for 0.30%): ",
            "prompt_on_new": False,
        }
    )
    auto_rotation_min_delta_edge_pct: Decimal = Field(
        default=Decimal("0.0030"),
        json_schema_extra={
            "prompt": lambda mi: "Min edge advantage over evicted token required to execute swap (e.g. 0.0030 for 30 bps): ",
            "prompt_on_new": False,
        }
    )
    auto_rotation_min_dwell_minutes: float = Field(
        default=30.0,
        ge=5.0,
        json_schema_extra={
            "prompt": lambda mi: "Min dwell time in minutes an idle token must stay before eviction (e.g. 30.0): ",
            "prompt_on_new": False,
        }
    )
    auto_rotation_min_volume_24h_usd: Decimal = Field(
        default=Decimal("2000000.0"),
        ge=Decimal("0"),
        json_schema_extra={
            "prompt": lambda mi: "Min 24h volume USD to qualify for admission (e.g. 2000000.0): ",
            "prompt_on_new": False,
        }
    )
    auto_rotation_max_swaps_per_cycle: int = Field(
        default=2,
        ge=1,
        le=5,
        json_schema_extra={
            "prompt": lambda mi: "Max number of token pairs swapped in a single cycle (e.g. 2): ",
            "prompt_on_new": False,
        }
    )
    check_margin_before_entry: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": lambda mi: "Verify real available balance/collateral on both exchanges before placing orders? (True/False): ",
            "prompt_on_new": True,
        }
    )
    margin_buffer_ratio: Decimal = Field(
        default=Decimal("0.05"),
        ge=Decimal("0.0"),
        le=Decimal("0.50"),
        json_schema_extra={
            "prompt": lambda mi: "Safety cushion ratio over initial margin for fees and slippage (e.g. 0.05 for 5%): ",
            "prompt_on_new": False,
        }
    )
    margin_aware_downsizing: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": lambda mi: "Adaptively scale down order size if available margin is below target but >= min_order_size? (True/False): ",
            "prompt_on_new": False,
        }
    )
    dry_run_virtual_balance_quote: Decimal = Field(
        default=Decimal("0.0"),
        ge=Decimal("0.0"),
        json_schema_extra={
            "prompt": lambda mi: "Virtual wallet balance for dry-run margin simulation (0.0 for unlimited): ",
            "prompt_on_new": False,
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
        self.quarantine_tokens: Dict[str, float] = {}
        self.sniper_prey_tracker: Dict[str, Dict] = {}

        # Two-Tier Dynamic Universe Rotation & Hot-Swap Engine
        self.market_discovery_candidates: List[Dict] = []
        self.rotation_history: List[Dict] = []
        self._universe_mutation_lock = asyncio.Lock()
        self._rotation_in_progress = False
        self._last_rotation_check_ts = 0.0
        self.token_admitted_at: Dict[str, float] = {tok: time.time() for tok in self.config.tokens}

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
                        pos_size = float(item.get("position_size", 1500.0))
                        fee = float(item.get("trading_fee", 3.0))

                        # Guarantee full friction fields are populated and never None/zero
                        if "depth_slippage_cost" not in item or item["depth_slippage_cost"] is None or float(item["depth_slippage_cost"]) == 0.0:
                            item["depth_slippage_cost"] = round(pos_size * 0.00015 * 4.0, 4)
                        if "latency_slippage_cost" not in item or item["latency_slippage_cost"] is None or float(item["latency_slippage_cost"]) == 0.0:
                            item["latency_slippage_cost"] = round(pos_size * 0.00020 * 4.0, 4)
                        if "total_friction_cost" not in item or item["total_friction_cost"] is None or float(item["total_friction_cost"]) == 0.0:
                            item["total_friction_cost"] = round(fee + float(item["depth_slippage_cost"]) + float(item["latency_slippage_cost"]), 4)
                        gross = float(item.get("gross_trading_pnl", float(item.get("trading_pnl", 0.0)) + float(item["total_friction_cost"])))
                        item["gross_trading_pnl"] = round(gross, 4)
                        funding = float(item.get("funding_pnl", 0.0))
                        real_net = round(gross - float(item["total_friction_cost"]) + funding, 4)
                        item["net_trading_pnl"] = round(gross - float(item["total_friction_cost"]), 4)
                        item["trading_pnl"] = item["net_trading_pnl"]
                        item["total_net_pnl"] = real_net
                        item["paper_trading_pnl"] = round(gross - fee, 4)
                        item["paper_net_pnl"] = round(gross - fee + funding, 4)
                        if "alpha_retention_pct" not in item or item["alpha_retention_pct"] is None:
                            item["alpha_retention_pct"] = round(real_net / gross * 100.0, 2) if gross > 0 else 0.0

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

                    # Load universe discovery candidates and rotation history
                    self.market_discovery_candidates = data.get("market_discovery_candidates", [])
                    self.rotation_history = data.get("rotation_history", [])
                    persisted_admitted = data.get("token_admitted_at", {})
                    for tok in self.config.tokens:
                        if tok in persisted_admitted:
                            self.token_admitted_at[tok] = float(persisted_admitted[tok])
                        else:
                            self.token_admitted_at[tok] = now_ts

                    # Restore quarantined tokens if present and not expired
                    quarantined = data.get("quarantined_tokens", {})
                    for q_tok, q_ts in quarantined.items():
                        if float(q_ts) > now_ts:
                            self.quarantine_tokens[q_tok] = float(q_ts)

                    # Restore sniper prey tracker if present and unexpired
                    persisted_prey = data.get("sniper_prey_tracker", {})
                    dwell_limit = int(getattr(self.config, "sniper_max_prey_dwell_seconds", 3600))
                    for p_tok, p_data in persisted_prey.items():
                        p_init = float(p_data.get("initial_trigger_ts", 0.0))
                        if now_ts - p_init < dwell_limit:
                            self.sniper_prey_tracker[p_tok] = p_data
                    if self.sniper_prey_tracker:
                        self.logger().info(f"[BASIS-STAT-ARB] Restored {len(self.sniper_prey_tracker)} active sniper prey(s) from {path}")

                    # Restore dynamically adapted universe tokens if present
                    persisted_universe = data.get("active_universe_tokens")
                    if persisted_universe and isinstance(persisted_universe, (list, set)) and len(persisted_universe) >= 10:
                        if isinstance(self.config.tokens, list):
                            self.config.tokens = list(persisted_universe)
                        elif isinstance(self.config.tokens, set):
                            self.config.tokens = set(persisted_universe)
                        self.logger().info(f"[BASIS-STAT-ARB] Restored dynamically adapted universe of {len(self.config.tokens)} tokens.")

                    self.logger().info(f"[BASIS-STAT-ARB] Loaded {len(closed_list)} closed trades & {len(self.active_basis_arbitrages)} active positions from {path}")
                    break
                except Exception as e:
                    self.logger().warning(f"Could not load state from {path}: {e}")

    def on_tick(self):
        t0 = time.perf_counter()
        if self.ready_to_trade and self.market_data_provider.ready:
            self.check_strategy_commands()
            self.check_funding_settlement()
            self.check_universe_rotation()
            self.update_sniper_prey_tracking()
            actions = self.determine_executor_actions()
            for action in actions:
                self.executor_orchestrator.execute_action(action)
            self.publish_basis_arbitrage_performance()

            duration_ms = (time.perf_counter() - t0) * 1000.0
            self._last_tick_latency_ms = duration_ms

            # Periodic scan logging (every 30s)
            now = self.current_timestamp
            if not hasattr(self, "_last_scan_log_ts"):
                self._last_scan_log_ts = 0.0
            if now - self._last_scan_log_ts >= 30.0:
                self._last_scan_log_ts = now
                comp = getattr(self, "_latest_market_comparison", [])
                spread_strs = [f"{m['token']}: {m['basis_spread_pct']:+.2%}" for m in comp[:6]]
                thresh = float(self.config.min_entry_basis_pct)
                headroom = max(0.0, 100.0 - (duration_ms / 1000.0 * 100.0))
                self.logger().info(
                    f"⚡ [BASIS-SCAN] Monitoring {len(self.config.tokens)} liquid pairs (Threshold: ±{thresh:.2%}). "
                    f"Top spreads: [{', '.join(spread_strs)}]. Active: {len(self.active_basis_arbitrages)}. "
                    f"Tick Latency: {duration_ms:.1f}ms (Headroom: {headroom:.1f}%)"
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

    def get_orderbook_depth_summary(
        self,
        connector_name: str,
        trading_pair: str,
        is_buy: bool,
        top_n: int = 3
    ) -> Tuple[Decimal, Decimal]:
        """
        Calculates quote depth (in USD/quote asset) for top 1 and top N price levels in L2 order book.
        - is_buy: True evaluates Ask book (liquidity to buy)
        - is_buy: False evaluates Bid book (liquidity to sell)
        Returns: (top1_quote_depth, top_n_quote_depth)
        """
        try:
            connector = self.connectors.get(connector_name)
            if not connector:
                return Decimal("0"), Decimal("0")
            order_book = connector.get_order_book(trading_pair)
            if not order_book:
                return Decimal("0"), Decimal("0")

            top1_depth = Decimal("0")
            top_n_depth = Decimal("0")

            # High-performance zero-copy iterator path
            if hasattr(order_book, "ask_entries") and hasattr(order_book, "bid_entries"):
                entries = order_book.ask_entries() if is_buy else order_book.bid_entries()
                for idx, entry in enumerate(entries):
                    if idx >= top_n:
                        break
                    p_val = entry.price
                    q_val = entry.amount
                    if p_val <= 0 or q_val <= 0 or math.isnan(p_val) or math.isnan(q_val):
                        continue
                    p = Decimal(str(p_val))
                    q = Decimal(str(q_val))
                    cost = p * q
                    if idx == 0:
                        top1_depth = cost
                    top_n_depth += cost
                return top1_depth, top_n_depth

            # Fallback path for mocks
            bids_df, asks_df = order_book.snapshot
            df = asks_df if is_buy else bids_df
            if df is None or len(df) == 0:
                return Decimal("0"), Decimal("0")

            n_rows = min(top_n, len(df))
            for idx in range(n_rows):
                p_val = df.iloc[idx]["price"]
                q_val = df.iloc[idx]["amount"]
                if math.isnan(float(p_val)) or math.isnan(float(q_val)):
                    continue
                p = Decimal(str(p_val))
                q = Decimal(str(q_val))
                cost = p * q
                if idx == 0:
                    top1_depth = cost
                top_n_depth += cost

            return top1_depth, top_n_depth
        except Exception:
            return Decimal("0"), Decimal("0")

    def get_orderbook_execution_metrics(
        self,
        connector_name: str,
        trading_pair: str,
        is_buy: bool,
        quote_volume: Decimal,
        base_amount: Optional[Decimal] = None
    ) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
        """
        Calculates execution metrics directly from live L2 OrderBook:
        Returns: (best_price, vwap_price, executed_price, depth_slip_pct)
        - best_price: Top of book (L1 Best Ask if buy, L1 Best Bid if sell)
        - vwap_price: Volume-weighted average price walking the order book
        - executed_price: In dry-run mode, applies simulated_latency_slippage_pct; in live mode, returns vwap
        - depth_slip_pct: abs(vwap - best_price) / best_price
        """
        try:
            connector = self.connectors.get(connector_name)
            if not connector:
                return None, None, None, None
            order_book = connector.get_order_book(trading_pair)
            if not order_book:
                return None, None, None, None

            best_price = None
            vwap_price = None

            # High-performance zero-copy iterator path
            if hasattr(order_book, "ask_entries") and hasattr(order_book, "bid_entries"):
                entries = order_book.ask_entries() if is_buy else order_book.bid_entries()
                cum_quote = Decimal("0")
                cum_base = Decimal("0")

                if base_amount is not None and base_amount > Decimal("0"):
                    target_base = Decimal(str(base_amount))
                    for idx, entry in enumerate(entries):
                        p_val = entry.price
                        q_val = entry.amount
                        if p_val <= 0 or q_val <= 0 or math.isnan(p_val) or math.isnan(q_val):
                            continue
                        p = Decimal(str(p_val))
                        q = Decimal(str(q_val))
                        if best_price is None:
                            best_price = p

                        if cum_base + q >= target_base:
                            needed_base = target_base - cum_base
                            cum_quote += needed_base * p
                            cum_base = target_base
                            break
                        cum_base += q
                        cum_quote += q * p

                    if best_price is None or cum_base < target_base or cum_base <= Decimal("0"):
                        return None, None, None, None
                    vwap_price = cum_quote / cum_base
                else:
                    target_quote = Decimal(str(quote_volume))
                    for idx, entry in enumerate(entries):
                        p_val = entry.price
                        q_val = entry.amount
                        if p_val <= 0 or q_val <= 0 or math.isnan(p_val) or math.isnan(q_val):
                            continue
                        p = Decimal(str(p_val))
                        q = Decimal(str(q_val))
                        if best_price is None:
                            best_price = p

                        cost = p * q
                        if cum_quote + cost >= target_quote:
                            needed_quote = target_quote - cum_quote
                            needed_base = needed_quote / p
                            cum_base += needed_base
                            cum_quote = target_quote
                            break
                        cum_quote += cost
                        cum_base += q

                    if best_price is None or cum_quote < target_quote or cum_base <= Decimal("0"):
                        return None, None, None, None
                    vwap_price = cum_quote / cum_base

            else:
                # Fallback path for mocks
                bids_df, asks_df = order_book.snapshot
                df = asks_df if is_buy else bids_df
                if df is None or len(df) == 0:
                    return None, None, None, None

                best_p_float = float(df.iloc[0]["price"])
                if best_p_float <= 0 or math.isnan(best_p_float):
                    return None, None, None, None
                best_price = Decimal(str(best_p_float))

                cum_quote = Decimal("0")
                cum_base = Decimal("0")

                if base_amount is not None and base_amount > Decimal("0"):
                    target_base = Decimal(str(base_amount))
                    for idx in range(len(df)):
                        p = Decimal(str(df.iloc[idx]["price"]))
                        q = Decimal(str(df.iloc[idx]["amount"]))
                        if cum_base + q >= target_base:
                            needed_base = target_base - cum_base
                            cum_quote += needed_base * p
                            cum_base = target_base
                            break
                        cum_base += q
                        cum_quote += q * p
                    if cum_base < target_base or cum_base <= Decimal("0"):
                        return None, None, None, None
                    vwap_price = cum_quote / cum_base
                else:
                    target_quote = Decimal(str(quote_volume))
                    for idx in range(len(df)):
                        p = Decimal(str(df.iloc[idx]["price"]))
                        q = Decimal(str(df.iloc[idx]["amount"]))
                        cost = p * q
                        if cum_quote + cost >= target_quote:
                            needed_quote = target_quote - cum_quote
                            needed_base = needed_quote / p
                            cum_base += needed_base
                            cum_quote = target_quote
                            break
                        cum_quote += cost
                        cum_base += q
                    if cum_quote < target_quote or cum_base <= Decimal("0"):
                        return None, None, None, None
                    vwap_price = cum_quote / cum_base

            depth_slip_pct = abs(vwap_price - best_price) / best_price if best_price > 0 else Decimal("0")

            latency_slip_pct = Decimal(str(getattr(self.config, "simulated_latency_slippage_pct", Decimal("0.0002"))))
            if getattr(self.config, "dry_run", True):
                if is_buy:
                    executed_price = vwap_price * (Decimal("1") + latency_slip_pct)
                else:
                    executed_price = vwap_price * (Decimal("1") - latency_slip_pct)
            else:
                executed_price = vwap_price

            return best_price, vwap_price, executed_price, depth_slip_pct
        except Exception:
            return None, None, None, None

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
        best_p, vwap_p, exec_p, _ = self.get_orderbook_execution_metrics(
            connector_name=connector_name,
            trading_pair=trading_pair,
            is_buy=is_buy,
            quote_volume=quote_volume
        )
        if exec_p is not None:
            return exec_p
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

    def get_connector_available_collateral(self, connector_name: str, trading_pair: str) -> Tuple[Decimal, str]:
        """
        Query real available margin/collateral for a connector.
        Returns (available_balance, collateral_asset_symbol).
        Handles derivative conventions:
        - Hyperliquid Perpetual: collateral is typically 'USD' or 'USDC'.
        - Binance Perpetual: collateral is 'USDT' or 'USDC'.
        - Fallbacks gracefully to split_trading_pair quote asset.
        """
        connector = self.connectors.get(connector_name)
        if not connector:
            return Decimal("0"), "UNKNOWN"

        collateral_asset = "USDT"
        if "-" in trading_pair:
            collateral_asset = trading_pair.split("-")[1].upper()

        avail = Decimal(str(connector.get_available_balance(collateral_asset)))

        # For Hyperliquid or specific perps, quote might be USD while account stores USDC, or vice versa
        if avail <= Decimal("0"):
            if "hyperliquid" in connector_name:
                alt_asset = "USDC" if collateral_asset == "USD" else "USD"
                alt_avail = Decimal(str(connector.get_available_balance(alt_asset)))
                if alt_avail > Decimal("0"):
                    return alt_avail, alt_asset
            elif "binance" in connector_name:
                alt_asset = "USDC" if collateral_asset == "USDT" else "USDT"
                alt_avail = Decimal(str(connector.get_available_balance(alt_asset)))
                if alt_avail > Decimal("0"):
                    return alt_avail, alt_asset

        return avail, collateral_asset

    def _check_dual_exchange_margin_sufficiency(
        self,
        token: str,
        c1: str,
        tp1: str,
        c1_exec_price: Decimal,
        c2: str,
        tp2: str,
        c2_exec_price: Decimal,
        pos_size: Decimal,
    ) -> Tuple[bool, Decimal, str]:
        """
        Institutional Pre-Flight Margin Verification Gate:
        Checks available collateral on BOTH exchanges before placing orders.
        Prevents fatal single-leg execution failure (naked unhedged exposure).
        Returns:
            (passed: bool, final_pos_size: Decimal, reason: str)
        """
        if not getattr(self.config, "check_margin_before_entry", True):
            return True, pos_size, "CHECK_DISABLED"

        lev = Decimal(str(self.config.leverage))
        buffer_ratio = getattr(self.config, "margin_buffer_ratio", Decimal("0.05"))
        taker_fee = getattr(self.config, "taker_fee_rate", Decimal("0.0005"))
        min_order_sz = getattr(self.config, "min_order_size_quote", Decimal("300.0"))
        downsizing_enabled = getattr(self.config, "margin_aware_downsizing", True)

        # -------------------------------------------------------------
        # 1. Virtual Dry-Run Portfolio Wallet Check
        # -------------------------------------------------------------
        if self.config.dry_run:
            virtual_total = getattr(self.config, "dry_run_virtual_balance_quote", Decimal("0.0"))
            if virtual_total > Decimal("0.0"):
                used_margin = sum(
                    Decimal(str(p.get("total_margin", 0.0)))
                    for p in self.active_basis_arbitrages.values()
                )
                dual_notional = pos_size * Decimal("2.0")
                req_tot_margin = (dual_notional / lev if lev > 0 else dual_notional) * (Decimal("1.0") + buffer_ratio)
                avail_virtual = virtual_total - used_margin
                if avail_virtual < req_tot_margin:
                    if downsizing_enabled:
                        max_feasible = (avail_virtual / (Decimal("1.0") + buffer_ratio)) * lev / Decimal("2.0")
                        if max_feasible >= min_order_sz:
                            self.logger().info(
                                f"🛡️ [VIRTUAL-MARGIN-DOWNSIZE] Token '{token}' virtual wallet sizing downsized "
                                f"${float(pos_size):,.2f} -> ${float(max_feasible):,.2f} "
                                f"(Avail: ${float(avail_virtual):,.2f} / Total: ${float(virtual_total):,.2f})"
                            )
                            return True, max_feasible, "VIRTUAL_DOWNSIZED"
                    msg = (
                        f"🛡️ [VIRTUAL-MARGIN-INTERCEPT] Token '{token}' entry blocked. "
                        f"Required Total Margin: ${float(req_tot_margin):,.2f}, "
                        f"Available Virtual Margin: ${float(avail_virtual):,.2f} (Total: ${float(virtual_total):,.2f}, Used: ${float(used_margin):,.2f})"
                    )
                    self.logger().info(msg)
                    return False, Decimal("0"), "VIRTUAL_MARGIN_INSUFFICIENT"
            # Unlimited dry run balance
            return True, pos_size, "DRY_RUN_UNLIMITED"

        # -------------------------------------------------------------
        # 2. Live Real-Money Physical Connector Margin Gate
        # -------------------------------------------------------------
        avail_1, asset_1 = self.get_connector_available_collateral(c1, tp1)
        avail_2, asset_2 = self.get_connector_available_collateral(c2, tp2)

        # Margin multiplier per dollar of single-leg position size:
        # Margin per leg = Notional / Leverage * (1 + buffer_ratio) + Notional * taker_fee
        margin_factor = (Decimal("1.0") / lev if lev > 0 else Decimal("1.0")) * (Decimal("1.0") + buffer_ratio) + taker_fee
        req_margin_1 = pos_size * margin_factor
        req_margin_2 = pos_size * margin_factor

        pass_1 = avail_1 >= req_margin_1
        pass_2 = avail_2 >= req_margin_2

        if pass_1 and pass_2:
            return True, pos_size, "PASSED_FULL"

        # Sizing shortage detected
        if downsizing_enabled:
            max_pos_1 = avail_1 / margin_factor if margin_factor > 0 else Decimal("0")
            max_pos_2 = avail_2 / margin_factor if margin_factor > 0 else Decimal("0")
            max_feasible = min(max_pos_1, max_pos_2)
            if max_feasible >= min_order_sz:
                self.logger().info(
                    f"🛡️ [MARGIN-AWARE-DOWNSIZE] Token '{token}' target size ${float(pos_size):,.2f} downsized to "
                    f"${float(max_feasible):,.2f} based on available margin. "
                    f"Exchange 1 ({c1}): ${float(avail_1):,.2f} {asset_1} (supports ${float(max_pos_1):,.2f}), "
                    f"Exchange 2 ({c2}): ${float(avail_2):,.2f} {asset_2} (supports ${float(max_pos_2):,.2f})."
                )
                return True, max_feasible, "MARGIN_DOWNSIZED"

        # Hard Intercept
        self.logger().warning(
            f"🛡️ [INSUFFICIENT-MARGIN-INTERCEPT] Token '{token}' entry aborted to prevent single-leg fill naked exposure!\n"
            f"  Exchange 1 ({c1}): Required ${float(req_margin_1):,.2f} {asset_1} | Available: ${float(avail_1):,.2f} {asset_1} -> {'✅ PASS' if pass_1 else '❌ INSUFFICIENT'}\n"
            f"  Exchange 2 ({c2}): Required ${float(req_margin_2):,.2f} {asset_2} | Available: ${float(avail_2):,.2f} {asset_2} -> {'✅ PASS' if pass_2 else '❌ INSUFFICIENT'}\n"
            f"  Minimum Order Size: ${float(min_order_sz):,.2f} (Required Margin: ${float(min_order_sz * margin_factor):,.2f})\n"
            f"  Safety Cushion: {float(buffer_ratio):.1%} buffer + {float(taker_fee):.2%} taker fee withholding."
        )
        return False, Decimal("0"), "INSUFFICIENT_MARGIN"

    def update_sniper_prey_tracking(self):
        """
        🎯 Option 1: Right-Side Exhaustion Peak Tracker (Case 52)
        Continuously monitors extreme basis prey, updates their peak basis dislocation,
        and counts consecutive contraction ticks once basis begins to mean-revert from peak.
        """
        if not getattr(self.config, "sniper_mode_enabled", True) or not self.sniper_prey_tracker:
            return

        if len(self.config.connectors) < 2:
            return

        c1, c2 = list(self.config.connectors)[:2]
        now = self.current_timestamp
        dwell_limit = int(getattr(self.config, "sniper_max_prey_dwell_seconds", 3600))
        reversal_min_pct = float(getattr(self.config, "sniper_reversal_confirmation_pct", Decimal("0.0020")))
        req_ticks = int(getattr(self.config, "sniper_confirmation_ticks", 3))

        for token, prey in list(self.sniper_prey_tracker.items()):
            # 1. If token already has an active position, remove from tracker
            if token in self.active_basis_arbitrages:
                del self.sniper_prey_tracker[token]
                continue

            # 2. Check dwell time expiry
            init_ts = float(prey.get("initial_trigger_ts", now))
            if now - init_ts > dwell_limit:
                self.logger().info(f"🎯 [SNIPER-PREY-EXPIRED] Tracking for '{token}' expired after {int(now - init_ts)}s.")
                del self.sniper_prey_tracker[token]
                continue

            tp1 = self.get_trading_pair_for_connector(token, c1)
            tp2 = self.get_trading_pair_for_connector(token, c2)
            m1 = Decimal(str(self.get_token_multiplier(token, c1)))
            m2 = Decimal(str(self.get_token_multiplier(token, c2)))

            p1_ind = self.get_indicative_price(c1, tp1)
            p2_ind = self.get_indicative_price(c2, tp2)
            if p1_ind is None or p2_ind is None or p1_ind <= 0 or p2_ind <= 0:
                continue

            p1_norm = p1_ind / m1
            p2_norm = p2_ind / m2
            spread_ind = abs(p1_norm - p2_norm) / min(p1_norm, p2_norm)
            curr_abs_basis = float(spread_ind)
            prey["last_basis"] = curr_abs_basis

            peak_basis = float(prey.get("peak_basis", curr_abs_basis))

            # 3. Peak expansion check
            if curr_abs_basis > peak_basis:
                prey["peak_basis"] = curr_abs_basis
                prey["peak_ts"] = now
                prey["consecutive_contraction_ticks"] = 0
                prey["status"] = "TRACKING_PEAK"
                self.logger().info(
                    f"🎯 [SNIPER-PEAK-EXPANDED] Prey '{token}' pushed new peak basis: {curr_abs_basis:+.2%} "
                    f"(Source: {prey.get('source', 'UNKNOWN')})"
                )
            else:
                # 4. Contraction check from peak
                contraction = peak_basis - curr_abs_basis
                if contraction >= reversal_min_pct:
                    ticks = prey.get("consecutive_contraction_ticks", 0) + 1
                    prey["consecutive_contraction_ticks"] = ticks
                    prey["status"] = "CONFIRMING_REVERSAL"
                    self.logger().info(
                        f"🎯 [SNIPER-REVERSAL-CONFIRMING] Prey '{token}' contracted {contraction*10000:.1f} bps from peak {peak_basis:.2%} "
                        f"(Current: {curr_abs_basis:.2%}, Ticks: {ticks}/{req_ticks})"
                    )
                else:
                    prey["consecutive_contraction_ticks"] = 0
                    prey["status"] = "TRACKING_PEAK"

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        """
        Evaluate if cross-exchange basis dislocation has opened up on any configured token.
        """
        create_actions = []
        if len(self.config.connectors) < 2:
            return create_actions

        c1, c2 = list(self.config.connectors)[:2]

        # ----------------- Option 1: Right-Side Sniper Counter-Attack Execution (Case 52) -----------------
        if getattr(self.config, "sniper_mode_enabled", True) and self.sniper_prey_tracker:
            req_ticks = int(getattr(self.config, "sniper_confirmation_ticks", 3))
            sniper_sz = Decimal(str(getattr(self.config, "sniper_position_size_quote", Decimal("300.0"))))

            for s_tok, prey in list(self.sniper_prey_tracker.items()):
                if s_tok in self.active_basis_arbitrages:
                    del self.sniper_prey_tracker[s_tok]
                    continue

                if prey.get("status") == "CONFIRMING_REVERSAL" and prey.get("consecutive_contraction_ticks", 0) >= req_ticks:
                    tp1 = self.get_trading_pair_for_connector(s_tok, c1)
                    tp2 = self.get_trading_pair_for_connector(s_tok, c2)
                    m1 = Decimal(str(self.get_token_multiplier(s_tok, c1)))
                    m2 = Decimal(str(self.get_token_multiplier(s_tok, c2)))

                    p1_b_best, p1_b_vwap, p1_b_exec, p1_b_slip = self.get_orderbook_execution_metrics(c1, tp1, is_buy=False, quote_volume=sniper_sz)
                    p2_a_best, p2_a_vwap, p2_a_exec, p2_a_slip = self.get_orderbook_execution_metrics(c2, tp2, is_buy=True, quote_volume=sniper_sz)
                    spread_A = Decimal("-999")
                    if p1_b_exec is not None and p2_a_exec is not None:
                        p1_b_norm = p1_b_exec / m1
                        p2_a_norm = p2_a_exec / m2
                        spread_A = (p1_b_norm - p2_a_norm) / p2_a_norm

                    p1_a_best, p1_a_vwap, p1_a_exec, p1_a_slip = self.get_orderbook_execution_metrics(c1, tp1, is_buy=True, quote_volume=sniper_sz)
                    p2_b_best, p2_b_vwap, p2_b_exec, p2_b_slip = self.get_orderbook_execution_metrics(c2, tp2, is_buy=False, quote_volume=sniper_sz)
                    spread_B = Decimal("-999")
                    if p1_a_exec is not None and p2_b_exec is not None:
                        p1_a_norm = p1_a_exec / m1
                        p2_b_norm = p2_b_exec / m2
                        spread_B = (p2_b_norm - p1_a_norm) / p1_a_norm

                    if spread_A > Decimal("0") and spread_A >= spread_B:
                        s_c1_side = TradeType.SELL
                        s_c2_side = TradeType.BUY
                        s_c1_best, s_c1_vwap, s_c1_exec, s_c1_slip = p1_b_best, p1_b_vwap, p1_b_exec, p1_b_slip or Decimal("0")
                        s_c2_best, s_c2_vwap, s_c2_exec, s_c2_slip = p2_a_best, p2_a_vwap, p2_a_exec, p2_a_slip or Decimal("0")
                        s_c1_norm, s_c2_norm = p1_b_norm, p2_a_norm
                        s_spread = spread_A
                    elif spread_B > Decimal("0") and spread_B > spread_A:
                        s_c1_side = TradeType.BUY
                        s_c2_side = TradeType.SELL
                        s_c1_best, s_c1_vwap, s_c1_exec, s_c1_slip = p1_a_best, p1_a_vwap, p1_a_exec, p1_a_slip or Decimal("0")
                        s_c2_best, s_c2_vwap, s_c2_exec, s_c2_slip = p2_b_best, p2_b_vwap, p2_b_exec, p2_b_slip or Decimal("0")
                        s_c1_norm, s_c2_norm = p1_a_norm, p2_b_norm
                        s_spread = spread_B
                    else:
                        continue

                    # Meat-on-the-bone Gate:
                    # Verify remaining executable edge is comfortably higher than take-profit target
                    # If spread has already compressed past TP, mean reversion already completed!
                    min_sniper_edge = getattr(self.config, "sniper_take_profit_basis_pct", Decimal("0.0035")) + Decimal("0.0010")
                    if s_spread < min_sniper_edge:
                        self.logger().info(
                            f"🎯 [SNIPER-OPPORTUNITY-VANISHED] Executable basis for '{s_tok}' already contracted to {float(s_spread):+.2%}, "
                            f"below viable sniper entry threshold ({float(min_sniper_edge):.2%}). Opportunity completed without entry."
                        )
                        del self.sniper_prey_tracker[s_tok]
                        continue

                    # Query Top 3 OrderBook depth for accurate reporting
                    _, top3_c1 = self.get_orderbook_depth_summary(c1, tp1, is_buy=(s_c1_side == TradeType.BUY), top_n=3)
                    _, top3_c2 = self.get_orderbook_depth_summary(c2, tp2, is_buy=(s_c2_side == TradeType.BUY), top_n=3)

                    # Margin check for sniper order
                    margin_pass, final_sz, margin_reason = self._check_dual_exchange_margin_sufficiency(
                        token=s_tok, c1=c1, tp1=tp1, c1_exec_price=s_c1_exec,
                        c2=c2, tp2=tp2, c2_exec_price=s_c2_exec, pos_size=sniper_sz
                    )
                    if not margin_pass:
                        self.logger().warning(f"🎯 [SNIPER-MARGIN-FAIL] Sniper opportunity on '{s_tok}' blocked: {margin_reason}")
                        continue

                    max_exec_p = max(s_c1_exec, s_c2_exec)
                    hedged_amt = sniper_sz / max_exec_p if max_exec_p > 0 else Decimal("0")
                    notional_1 = hedged_amt * s_c1_exec
                    notional_2 = hedged_amt * s_c2_exec
                    tot_notional = notional_1 + notional_2
                    lev_val = Decimal(str(self.config.leverage))
                    margin_1 = notional_1 / lev_val if lev_val > 0 else notional_1
                    margin_2 = notional_2 / lev_val if lev_val > 0 else notional_2
                    tot_margin = margin_1 + margin_2

                    if s_c1_side == TradeType.BUY:
                        entry_depth_slip_cost_1 = (s_c1_vwap - s_c1_best) * hedged_amt
                        entry_latency_slip_cost_1 = (s_c1_exec - s_c1_vwap) * hedged_amt
                    else:
                        entry_depth_slip_cost_1 = (s_c1_best - s_c1_vwap) * hedged_amt
                        entry_latency_slip_cost_1 = (s_c1_vwap - s_c1_exec) * hedged_amt

                    if s_c2_side == TradeType.BUY:
                        entry_depth_slip_cost_2 = (s_c2_vwap - s_c2_best) * hedged_amt
                        entry_latency_slip_cost_2 = (s_c2_exec - s_c2_vwap) * hedged_amt
                    else:
                        entry_depth_slip_cost_2 = (s_c2_best - s_c2_vwap) * hedged_amt
                        entry_latency_slip_cost_2 = (s_c2_vwap - s_c2_exec) * hedged_amt

                    peak_b = float(prey.get("peak_basis", float(s_spread)))
                    stop_b = peak_b + float(getattr(self.config, "sniper_stop_loss_distance_pct", Decimal("0.0030")))

                    self.logger().info(
                        f"🎯 [SNIPER-LAUNCH-TRIGGERED] Token '{s_tok}' Right-Side Exhaustion Confirmed!\n"
                        f"  Peak Basis: {peak_b:+.2%} -> Entry Basis: {float(s_spread):+.2%} (Contraction: {(peak_b - float(s_spread))*10000:.1f} bps)\n"
                        f"  Coin-Neutral Size: {float(hedged_amt):,.4f} {s_tok} (${float(sniper_sz):,.2f} quote per leg)\n"
                        f"  Leg 1 ({c1}): {s_c1_side.name} @ ${float(s_c1_exec):.4f} | Leg 2 ({c2}): {s_c2_side.name} @ ${float(s_c2_exec):.4f}\n"
                        f"  Invalidation Defense Line: {stop_b:+.2%} (+{float(getattr(self.config, 'sniper_stop_loss_distance_pct', Decimal('0.0030')))*10000:.0f} bps past peak)"
                    )

                    pos_data = {
                        "token": s_tok,
                        "is_virtual": self.config.dry_run,
                        "connector_1": c1,
                        "connector_2": c2,
                        "side_1": s_c1_side.name,
                        "side_2": s_c2_side.name,
                        "amount_1": float(hedged_amt),
                        "amount_2": float(hedged_amt),
                        "entry_best_price_1": float(s_c1_best),
                        "entry_best_price_2": float(s_c2_best),
                        "entry_vwap_1": float(s_c1_vwap),
                        "entry_vwap_2": float(s_c2_vwap),
                        "entry_price_1": float(s_c1_exec),
                        "entry_price_2": float(s_c2_exec),
                        "entry_depth_slip_cost_1": float(entry_depth_slip_cost_1),
                        "entry_depth_slip_cost_2": float(entry_depth_slip_cost_2),
                        "entry_latency_slip_cost_1": float(entry_latency_slip_cost_1),
                        "entry_latency_slip_cost_2": float(entry_latency_slip_cost_2),
                        "entry_norm_price_1": float(s_c1_norm),
                        "entry_norm_price_2": float(s_c2_norm),
                        "entry_basis_pct": float(s_spread),
                        "target_compression_pct": float(getattr(self.config, "sniper_take_profit_basis_pct", Decimal("0.0035"))),
                        "position_size": float(sniper_sz),
                        "safe_capacity": float(sniper_sz),
                        "top3_depth_1": float(top3_c1),
                        "top3_depth_2": float(top3_c2),
                        "net_executable_edge_pct": float(s_spread),
                        "entry_depth_slip_pct_1": float(s_c1_slip),
                        "entry_depth_slip_pct_2": float(s_c2_slip),
                        "single_notional": float(sniper_sz),
                        "total_notional": float(tot_notional),
                        "leverage": self.config.leverage,
                        "margin_1": float(margin_1),
                        "margin_2": float(margin_2),
                        "total_margin": float(tot_margin),
                        "open_timestamp": self.current_timestamp,
                        "expected_daily_carry": 0.0,
                        "trailing_active": False,
                        "peak_pnl_pct": 0.0,
                        "settled_funding_pnl": 0.0,
                        "funding_payments": [],
                        "next_funding_time_1": int(self.current_timestamp + 28800),
                        "next_funding_time_2": int(self.current_timestamp + 3600),
                        "min_holding_seconds": 5,
                        # Sniper specific flags
                        "is_sniper": True,
                        "sniper_peak_basis": peak_b,
                        "sniper_entry_basis": float(s_spread),
                        "sniper_stop_basis": stop_b,
                    }

                    if s_tok in self.quarantine_tokens:
                        del self.quarantine_tokens[s_tok]
                    del self.sniper_prey_tracker[s_tok]

                    if self.config.dry_run:
                        self.active_basis_arbitrages[s_tok] = pos_data
                    else:
                        pos_cfg_1 = PositionExecutorConfig(
                            timestamp=self.current_timestamp, connector_name=c1,
                            trading_pair=tp1, side=s_c1_side, entry_price=s_c1_exec,
                            amount=hedged_amt, leverage=self.config.leverage,
                            triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.MARKET),
                        )
                        pos_cfg_2 = PositionExecutorConfig(
                            timestamp=self.current_timestamp, connector_name=c2,
                            trading_pair=tp2, side=s_c2_side, entry_price=s_c2_exec,
                            amount=hedged_amt, leverage=self.config.leverage,
                            triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.MARKET),
                        )
                        pos_data["is_virtual"] = False
                        pos_data["executors_ids"] = [pos_cfg_1.id, pos_cfg_2.id]
                        self.active_basis_arbitrages[s_tok] = pos_data
                        create_actions.extend([CreateExecutorAction(executor_config=pos_cfg_1), CreateExecutorAction(executor_config=pos_cfg_2)])

        for token in self.config.tokens:
            if token in self.active_basis_arbitrages:
                continue

            # Optimization 2: Toxic Token Quarantine check (divergence defense)
            quarantine_until = self.quarantine_tokens.get(token, 0.0)
            if self.current_timestamp < quarantine_until:
                # Quarantined tokens are strictly forbidden from normal entries (0.50% ~ 1.49%)
                # But if extreme dislocation occurs (>= 1.50%), enroll in Sniper Prey Tracker:
                if getattr(self.config, "sniper_mode_enabled", True) and token not in self.sniper_prey_tracker:
                    tp1 = self.get_trading_pair_for_connector(token, c1)
                    tp2 = self.get_trading_pair_for_connector(token, c2)
                    p1_ind = self.get_indicative_price(c1, tp1)
                    p2_ind = self.get_indicative_price(c2, tp2)
                    if p1_ind and p2_ind and p1_ind > 0 and p2_ind > 0:
                        m1 = Decimal(str(self.get_token_multiplier(token, c1)))
                        m2 = Decimal(str(self.get_token_multiplier(token, c2)))
                        p1_n = p1_ind / m1
                        p2_n = p2_ind / m2
                        ind_basis = abs(p1_n - p2_n) / min(p1_n, p2_n)
                        if ind_basis >= getattr(self.config, "sniper_trigger_basis_pct", Decimal("0.0150")):
                            self.sniper_prey_tracker[token] = {
                                "token": token,
                                "source": "QUARANTINE_EXTREME_SCAN",
                                "initial_trigger_ts": self.current_timestamp,
                                "peak_basis": float(ind_basis),
                                "peak_ts": self.current_timestamp,
                                "consecutive_contraction_ticks": 0,
                                "last_basis": float(ind_basis),
                                "status": "TRACKING_PEAK",
                            }
                            self.logger().info(
                                f"🎯 [QUARANTINE-SNIPER-ENROLLED] Quarantined token '{token}' exhibiting extreme basis ({float(ind_basis):.2%}). "
                                f"Enrolled in Sniper Prey Tracker for right-side reversal!"
                            )
                continue
            elif token in self.quarantine_tokens:
                del self.quarantine_tokens[token]

            # Cooldown check
            last_closed = self.token_last_closed_timestamp.get(token, 0)
            if self.current_timestamp - last_closed < self.config.reentry_cooldown_seconds:
                continue

            tp1 = self.get_trading_pair_for_connector(token, c1)
            tp2 = self.get_trading_pair_for_connector(token, c2)

            m1 = Decimal(str(self.get_token_multiplier(token, c1)))
            m2 = Decimal(str(self.get_token_multiplier(token, c2)))

            # Query Top 3 OrderBook depth for candidate directions
            c1_bid_top1, c1_bid_top3 = self.get_orderbook_depth_summary(c1, tp1, is_buy=False, top_n=3)
            c1_ask_top1, c1_ask_top3 = self.get_orderbook_depth_summary(c1, tp1, is_buy=True, top_n=3)
            c2_bid_top1, c2_bid_top3 = self.get_orderbook_depth_summary(c2, tp2, is_buy=False, top_n=3)
            c2_ask_top1, c2_ask_top3 = self.get_orderbook_depth_summary(c2, tp2, is_buy=True, top_n=3)

            # Dynamic Capacity Sizing:
            # Safe capacity is scaled to top 3 orderbook depth (default 20%) to prevent adverse market impact.
            # Direction A: Sell C1 @ Bid, Buy C2 @ Ask -> bottleneck depth is min(c1_bid_top3, c2_ask_top3)
            # Direction B: Buy C1 @ Ask, Sell C2 @ Bid -> bottleneck depth is min(c1_ask_top3, c2_bid_top3)
            ratio = getattr(self.config, "max_orderbook_depth_ratio", Decimal("0.20"))
            min_order_sz = getattr(self.config, "min_order_size_quote", Decimal("300.0"))
            base_pos_sz = self.config.position_size_quote
            is_dynamic = getattr(self.config, "dynamic_capacity_sizing", True)

            depth_top3_A = min(c1_bid_top3, c2_ask_top3)
            depth_top3_B = min(c1_ask_top3, c2_bid_top3)

            if is_dynamic:
                pos_size_A = None if depth_top3_A < min_order_sz else min(base_pos_sz, max(min_order_sz, depth_top3_A * ratio))
                pos_size_B = None if depth_top3_B < min_order_sz else min(base_pos_sz, max(min_order_sz, depth_top3_B * ratio))
            else:
                pos_size_A = base_pos_sz
                pos_size_B = base_pos_sz

            if pos_size_A is None and pos_size_B is None:
                self.logger().debug(
                    f"🛡️ [LIQUIDITY-MIRAGE-INTERCEPT] Token '{token}' Top 3 depth "
                    f"(Dir A: ${float(depth_top3_A):,.1f}, Dir B: ${float(depth_top3_B):,.1f}) "
                    f"below minimum order size (${float(min_order_sz):,.1f}). Entry blocked."
                )
                self.entry_candidate_ticks[token] = 0
                continue

            # Query real executable orderbook metrics for direction-specific pos_size on BOTH exchanges
            spread_A = Decimal("-999")
            p1_bid_best = p1_bid_vwap = p1_bid_exec = p1_bid_slip = None
            p2_ask_best = p2_ask_vwap = p2_ask_exec = p2_ask_slip = None
            if pos_size_A is not None:
                p1_bid_best, p1_bid_vwap, p1_bid_exec, p1_bid_slip = self.get_orderbook_execution_metrics(c1, tp1, is_buy=False, quote_volume=pos_size_A)
                p2_ask_best, p2_ask_vwap, p2_ask_exec, p2_ask_slip = self.get_orderbook_execution_metrics(c2, tp2, is_buy=True, quote_volume=pos_size_A)
                if p1_bid_exec is not None and p2_ask_exec is not None:
                    p1_bid_norm = p1_bid_exec / m1
                    p2_ask_norm = p2_ask_exec / m2
                    spread_A = (p1_bid_norm - p2_ask_norm) / p2_ask_norm

            spread_B = Decimal("-999")
            p1_ask_best = p1_ask_vwap = p1_ask_exec = p1_ask_slip = None
            p2_bid_best = p2_bid_vwap = p2_bid_exec = p2_bid_slip = None
            if pos_size_B is not None:
                p1_ask_best, p1_ask_vwap, p1_ask_exec, p1_ask_slip = self.get_orderbook_execution_metrics(c1, tp1, is_buy=True, quote_volume=pos_size_B)
                p2_bid_best, p2_bid_vwap, p2_bid_exec, p2_bid_slip = self.get_orderbook_execution_metrics(c2, tp2, is_buy=False, quote_volume=pos_size_B)
                if p1_ask_exec is not None and p2_bid_exec is not None:
                    p1_ask_norm = p1_ask_exec / m1
                    p2_bid_norm = p2_bid_exec / m2
                    spread_B = (p2_bid_norm - p1_ask_norm) / p1_ask_norm

            min_entry = self.config.min_entry_basis_pct

            # Freight-Train Defense & Extreme Dislocation Sniper Registration (Case 52)
            # If basis dislocation is extreme (>= sniper_trigger_basis_pct e.g. 1.50%),
            # NEVER open a regular position on the left side into a runaway whale squeeze!
            # Instead, register token in sniper_prey_tracker to wait for right-side exhaustion.
            sniper_trigger_thresh = getattr(self.config, "sniper_trigger_basis_pct", Decimal("0.0150"))
            max_cand_spread = max(spread_A, spread_B)
            if getattr(self.config, "sniper_mode_enabled", True) and max_cand_spread >= sniper_trigger_thresh:
                if token not in self.sniper_prey_tracker:
                    self.sniper_prey_tracker[token] = {
                        "token": token,
                        "source": "EXTREME_DISLOCATION_SCAN",
                        "initial_trigger_ts": self.current_timestamp,
                        "peak_basis": float(max_cand_spread),
                        "peak_ts": self.current_timestamp,
                        "consecutive_contraction_ticks": 0,
                        "last_basis": float(max_cand_spread),
                        "status": "TRACKING_PEAK",
                    }
                    self.logger().info(
                        f"🛡️ [FREIGHT-TRAIN-DEFENSE] Token '{token}' basis ({float(max_cand_spread):+.2%}) "
                        f"exceeds extreme threshold ({float(sniper_trigger_thresh):.2%}). "
                        f"Standard entry skipped; registered as Sniper Prey for right-side exhaustion tracking!"
                    )
                self.entry_candidate_ticks[token] = 0
                continue

            # Determine best executable direction
            if spread_A >= min_entry and spread_A >= spread_B:
                c1_side = TradeType.SELL
                c2_side = TradeType.BUY
                c1_best_price = p1_bid_best
                c1_vwap_price = p1_bid_vwap
                c1_exec_price = p1_bid_exec
                c1_slip = p1_bid_slip or Decimal("0")
                c2_best_price = p2_ask_best
                c2_vwap_price = p2_ask_vwap
                c2_exec_price = p2_ask_exec
                c2_slip = p2_ask_slip or Decimal("0")
                c1_norm = p1_bid_norm
                c2_norm = p2_ask_norm
                executable_basis_spread = spread_A
                pos_size = pos_size_A
                top3_c1 = c1_bid_top3
                top3_c2 = c2_ask_top3
            elif spread_B >= min_entry and spread_B > spread_A:
                c1_side = TradeType.BUY
                c2_side = TradeType.SELL
                c1_best_price = p1_ask_best
                c1_vwap_price = p1_ask_vwap
                c1_exec_price = p1_ask_exec
                c1_slip = p1_ask_slip or Decimal("0")
                c2_best_price = p2_bid_best
                c2_vwap_price = p2_bid_vwap
                c2_exec_price = p2_bid_exec
                c2_slip = p2_bid_slip or Decimal("0")
                c1_norm = p1_ask_norm
                c2_norm = p2_bid_norm
                executable_basis_spread = spread_B
                pos_size = pos_size_B
                top3_c1 = c1_ask_top3
                top3_c2 = c2_bid_top3
            else:
                self.entry_candidate_ticks[token] = 0
                continue

            # ----------------- Executable VWAP Filter & Friction Gates -----------------
            # 1. Single-leg depth slippage ceiling:
            max_depth_slip_limit = getattr(self.config, "max_executable_depth_slippage_pct", Decimal("0.0025"))
            max_leg_slip = max(c1_slip, c2_slip)
            if max_leg_slip > max_depth_slip_limit:
                self.logger().info(
                    f"🛡️ [LIQUIDITY-MIRAGE-INTERCEPT] Token '{token}' single-leg depth slippage ({float(max_leg_slip):.3%}) "
                    f"exceeds ceiling ({float(max_depth_slip_limit):.3%}). "
                    f"Leg 1 ({c1}): {float(c1_slip):.3%}, Leg 2 ({c2}): {float(c2_slip):.3%}. Entry blocked."
                )
                self.entry_candidate_ticks[token] = 0
                continue

            # 2. VWAP Net Executable Edge Gate:
            vwap_norm_1 = c1_vwap_price / m1
            vwap_norm_2 = c2_vwap_price / m2
            raw_vwap_basis = abs(vwap_norm_1 - vwap_norm_2) / min(vwap_norm_1, vwap_norm_2)

            roundtrip_taker_fees = getattr(self.config, "taker_fee_rate", Decimal("0.0005")) * Decimal("4.0")
            roundtrip_latency = getattr(self.config, "simulated_latency_slippage_pct", Decimal("0.0002")) * Decimal("4.0")
            est_exit_depth_slip = c1_slip + c2_slip
            total_friction_spread = roundtrip_taker_fees + roundtrip_latency + est_exit_depth_slip
            net_executable_edge = raw_vwap_basis - total_friction_spread

            min_net_edge_limit = getattr(self.config, "min_executable_net_edge_pct", Decimal("0.0018"))
            if net_executable_edge < min_net_edge_limit:
                self.logger().info(
                    f"🛡️ [LIQUIDITY-MIRAGE-INTERCEPT] Token '{token}' Net Executable Edge ({float(net_executable_edge):.3%}) "
                    f"below required threshold ({float(min_net_edge_limit):.3%}). "
                    f"Raw VWAP Basis: {float(raw_vwap_basis):.3%}, Friction: {float(total_friction_spread):.3%} "
                    f"(Fees {float(roundtrip_taker_fees):.3%} + Latency {float(roundtrip_latency):.3%} + ExitSlip {float(est_exit_depth_slip):.3%}). Entry blocked."
                )
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

            # Confirmed Opportunity! Check pre-flight margin availability on BOTH exchanges
            margin_pass, final_pos_size, margin_reason = self._check_dual_exchange_margin_sufficiency(
                token=token,
                c1=c1,
                tp1=tp1,
                c1_exec_price=c1_exec_price,
                c2=c2,
                tp2=tp2,
                c2_exec_price=c2_exec_price,
                pos_size=pos_size,
            )
            if not margin_pass:
                self.entry_candidate_ticks[token] = 0
                continue

            pos_size = final_pos_size
            self.entry_candidate_ticks[token] = 0
            # Sizing by uniform base token quantity (Coin-Neutral / Delta-Zero)
            # Ensures Leg 1 and Leg 2 hold strictly identical base coin amounts (Q1 == Q2)
            # Sized using the higher price leg so neither leg exceeds position_size_quote
            max_exec_p = max(c1_exec_price, c2_exec_price)
            hedged_amount = pos_size / max_exec_p if max_exec_p > 0 else Decimal("0")
            c1_amount = hedged_amount
            c2_amount = hedged_amount

            notional_1 = c1_amount * c1_exec_price
            notional_2 = c2_amount * c2_exec_price
            tot_notional = notional_1 + notional_2

            lev_val = Decimal(str(self.config.leverage))
            margin_1 = notional_1 / lev_val if lev_val > 0 else notional_1
            margin_2 = notional_2 / lev_val if lev_val > 0 else notional_2
            tot_margin = margin_1 + margin_2

            # Calculate explicit entry depth & latency slippage costs
            if c1_side == TradeType.BUY:
                entry_depth_slip_cost_1 = (c1_vwap_price - c1_best_price) * c1_amount
                entry_latency_slip_cost_1 = (c1_exec_price - c1_vwap_price) * c1_amount
            else:
                entry_depth_slip_cost_1 = (c1_best_price - c1_vwap_price) * c1_amount
                entry_latency_slip_cost_1 = (c1_vwap_price - c1_exec_price) * c1_amount

            if c2_side == TradeType.BUY:
                entry_depth_slip_cost_2 = (c2_vwap_price - c2_best_price) * c2_amount
                entry_latency_slip_cost_2 = (c2_exec_price - c2_vwap_price) * c2_amount
            else:
                entry_depth_slip_cost_2 = (c2_best_price - c2_vwap_price) * c2_amount
                entry_latency_slip_cost_2 = (c2_vwap_price - c2_exec_price) * c2_amount

            self.logger().info(
                f"🚀 [BASIS-ENTRY-TRIGGER] Token '{token}' confirmed executable dislocation: {float(executable_basis_spread):+.3%}!\n"
                f"  Coin-Neutral Alignment: {float(hedged_amount):,.4f} {token} (Delta-Zero Q1 == Q2)\n"
                f"  Leg 1 ({c1}): {c1_side.name} {float(c1_amount):,.4f} {token} @ ${float(c1_exec_price):.4f} (货值 ${float(notional_1):,.2f} | 保证金 ${float(margin_1):,.2f})\n"
                f"  Leg 2 ({c2}): {c2_side.name} {float(c2_amount):,.4f} {token} @ ${float(c2_exec_price):.4f} (货值 ${float(notional_2):,.2f} | 保证金 ${float(margin_2):,.2f})\n"
                f"  Capital Sizing: 单边货值 ${float(pos_size):,.2f} | 双边总货值 ${float(tot_notional):,.2f} | 实缴总保证金 ${float(tot_margin):,.2f} ({self.config.leverage}x)\n"
                f"  Indicative Carry: {float(daily_carry):+.2%}/d\n"
                f"  Friction Model: Depth Slip: ${float(entry_depth_slip_cost_1 + entry_depth_slip_cost_2):.4f} | Latency Slip: ${float(entry_latency_slip_cost_1 + entry_latency_slip_cost_2):.4f}"
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
                "entry_best_price_1": float(c1_best_price),
                "entry_best_price_2": float(c2_best_price),
                "entry_vwap_1": float(c1_vwap_price),
                "entry_vwap_2": float(c2_vwap_price),
                "entry_price_1": float(c1_exec_price),
                "entry_price_2": float(c2_exec_price),
                "entry_depth_slip_cost_1": float(entry_depth_slip_cost_1),
                "entry_depth_slip_cost_2": float(entry_depth_slip_cost_2),
                "entry_latency_slip_cost_1": float(entry_latency_slip_cost_1),
                "entry_latency_slip_cost_2": float(entry_latency_slip_cost_2),
                "entry_norm_price_1": float(c1_norm),
                "entry_norm_price_2": float(c2_norm),
                "entry_basis_pct": float(executable_basis_spread),
                "target_compression_pct": float(self.config.take_profit_basis_compression_pct),
                "position_size": float(pos_size),
                "safe_capacity": float(pos_size),
                "top3_depth_1": float(top3_c1),
                "top3_depth_2": float(top3_c2),
                "net_executable_edge_pct": float(net_executable_edge),
                "entry_depth_slip_pct_1": float(c1_slip),
                "entry_depth_slip_pct_2": float(c2_slip),
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
                    amount=hedged_amount,
                    leverage=self.config.leverage,
                    triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.MARKET),
                )
                pos_cfg_2 = PositionExecutorConfig(
                    timestamp=self.current_timestamp,
                    connector_name=c2,
                    trading_pair=tp2,
                    side=c2_side,
                    entry_price=c2_exec_price,
                    amount=hedged_amount,
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

            amt1 = Decimal(str(pos.get("amount_1", pos_size / p1_entry)))
            amt2 = Decimal(str(pos.get("amount_2", pos_size / p2_entry)))

            p1_close_best, p1_close_vwap, p1_close_exec, p1_close_slip = self.get_orderbook_execution_metrics(
                c1, tp1, is_buy=is_buy_close_1, quote_volume=pos_size, base_amount=amt1
            )
            p2_close_best, p2_close_vwap, p2_close_exec, p2_close_slip = self.get_orderbook_execution_metrics(
                c2, tp2, is_buy=is_buy_close_2, quote_volume=pos_size, base_amount=amt2
            )

            # Fallback if metrics returned None
            if p1_close_exec is None:
                p1_close_exec = self.get_executable_quote_price(c1, tp1, pos_size, is_buy=is_buy_close_1)
                p1_close_best = p1_close_exec
                p1_close_vwap = p1_close_exec
            if p2_close_exec is None:
                p2_close_exec = self.get_executable_quote_price(c2, tp2, pos_size, is_buy=is_buy_close_2)
                p2_close_best = p2_close_exec
                p2_close_vwap = p2_close_exec

            # Strict guard: If either executable quote is unavailable, do NOT evaluate exit at corrupted dummy prices!
            if p1_close_exec is None or p2_close_exec is None:
                continue

            p1_curr = p1_close_exec
            p2_curr = p2_close_exec

            m1 = Decimal(str(self.get_token_multiplier(token, c1)))
            m2 = Decimal(str(self.get_token_multiplier(token, c2)))
            p1_curr_norm = p1_curr / m1
            p2_curr_norm = p2_curr / m2

            # Current basis: (P1 - P2) / P2
            curr_basis = (p1_curr_norm - p2_curr_norm) / p2_curr_norm
            entry_basis = Decimal(str(pos["entry_basis_pct"]))

            # Exit depth & latency slippage calculations
            if is_buy_close_1:
                exit_depth_slip_cost_1 = (p1_close_vwap - p1_close_best) * amt1
                exit_latency_slip_cost_1 = (p1_close_exec - p1_close_vwap) * amt1
            else:
                exit_depth_slip_cost_1 = (p1_close_best - p1_close_vwap) * amt1
                exit_latency_slip_cost_1 = (p1_close_vwap - p1_close_exec) * amt1

            if is_buy_close_2:
                exit_depth_slip_cost_2 = (p2_close_vwap - p2_close_best) * amt2
                exit_latency_slip_cost_2 = (p2_close_exec - p2_close_vwap) * amt2
            else:
                exit_depth_slip_cost_2 = (p2_close_best - p2_close_vwap) * amt2
                exit_latency_slip_cost_2 = (p2_close_vwap - p2_close_exec) * amt2

            tot_depth_slip = (
                Decimal(str(pos.get("entry_depth_slip_cost_1", 0.0)))
                + Decimal(str(pos.get("entry_depth_slip_cost_2", 0.0)))
                + exit_depth_slip_cost_1
                + exit_depth_slip_cost_2
            )
            tot_latency_slip = (
                Decimal(str(pos.get("entry_latency_slip_cost_1", 0.0)))
                + Decimal(str(pos.get("entry_latency_slip_cost_2", 0.0)))
                + exit_latency_slip_cost_1
                + exit_latency_slip_cost_2
            )

            # Theoretical Ideal Gross Trading PnL (Zero Slippage, based on L1 Best Prices)
            p1_entry_best = Decimal(str(pos.get("entry_best_price_1", pos["entry_price_1"])))
            p2_entry_best = Decimal(str(pos.get("entry_best_price_2", pos["entry_price_2"])))

            if pos["side_1"] == "BUY":
                leg1_ideal_pnl = (p1_close_best - p1_entry_best) * amt1
                leg1_pnl = (p1_curr - p1_entry) * amt1
            else:
                leg1_ideal_pnl = (p1_entry_best - p1_close_best) * amt1
                leg1_pnl = (p1_entry - p1_curr) * amt1

            if pos["side_2"] == "BUY":
                leg2_ideal_pnl = (p2_close_best - p2_entry_best) * amt2
                leg2_pnl = (p2_curr - p2_entry) * amt2
            else:
                leg2_ideal_pnl = (p2_entry_best - p2_close_best) * amt2
                leg2_pnl = (p2_entry - p2_curr) * amt2

            gross_trading_pnl = leg1_ideal_pnl + leg2_ideal_pnl
            realized_trading_pnl = leg1_pnl + leg2_pnl

            # Explicit 4-leg Taker fee calculation on actual notionals (Open Leg1+2 + Close Leg1+2)
            fee_rate = Decimal(str(getattr(self.config, "taker_fee_rate", Decimal("0.0005"))))
            open_notional = (amt1 * p1_entry) + (amt2 * p2_entry)
            close_notional = (amt1 * p1_curr) + (amt2 * p2_curr)
            est_roundtrip_fees = (open_notional + close_notional) * fee_rate
            total_friction_cost = tot_depth_slip + tot_latency_slip + est_roundtrip_fees

            # Real Penetrated Net Trading PnL = Gross - Total Friction (Fees + Depth Slippage + Latency Slippage)
            net_trading_pnl = gross_trading_pnl - total_friction_cost
            paper_trading_pnl = gross_trading_pnl - est_roundtrip_fees

            # Discrete settled funding cashflow (strictly settled at actual exchange settlement windows)
            settled_funding = Decimal(str(pos.get("settled_funding_pnl", 0.0)))

            # Theoretical indicative accrued carry (for reference only, NEVER counted as realized cashflow)
            duration_s = self.current_timestamp - pos["open_timestamp"]
            duration_h = duration_s / 3600.0
            daily_carry = Decimal(str(pos.get("expected_daily_carry", 0.0)))
            indicative_carry = pos_size * daily_carry * Decimal(str(duration_s / 86400.0))

            # Total Net PnL = Real Net Trading PnL + Actual Settled Funding
            total_net_pnl = net_trading_pnl + settled_funding
            paper_net_pnl = paper_trading_pnl + settled_funding
            net_pnl_pct = total_net_pnl / pos_size if pos_size > 0 else Decimal("0")

            alpha_retention_pct = (total_net_pnl / gross_trading_pnl * Decimal("100")) if gross_trading_pnl > Decimal("0") else Decimal("0")

            # True ROE on real margin (Dual leg margin = (pos_size / leverage) * 2)
            pos_leverage = Decimal(str(pos.get("leverage", self.config.leverage)))
            real_margin = (pos_size / pos_leverage) * Decimal("2")
            net_roe_pct = total_net_pnl / real_margin if real_margin > 0 else Decimal("0")

            # Update peak PnL for trailing
            peak_pnl = max(pos.get("peak_pnl_pct", 0.0), float(net_pnl_pct))
            pos["peak_pnl_pct"] = peak_pnl

            # Anti-HFT Churn Defense: Enforce minimum holding time before allowing normal take-profit
            min_hold_s = getattr(self.config, "min_holding_seconds", 60)
            is_min_hold_passed = (duration_s >= min_hold_s)

            # In-place update live position metrics so disk persistence and UI are 100% synchronized
            pos["current_price_1"] = float(p1_curr)
            pos["current_price_2"] = float(p2_curr)
            pos["current_basis_pct"] = float(curr_basis)
            pos["duration_seconds"] = int(duration_s)
            pos["gross_unrealized_trading_pnl"] = float(gross_trading_pnl)
            pos["net_unrealized_trading_pnl"] = float(net_trading_pnl)
            pos["unrealized_trading_pnl"] = float(net_trading_pnl)
            pos["total_pos_net"] = float(total_net_pnl)
            pos["unrealized_roe_pct"] = float(net_roe_pct)
            pos["trading_fee"] = float(est_roundtrip_fees)
            pos["total_friction_cost"] = float(total_friction_cost)
            pos["depth_slippage_cost"] = float(tot_depth_slip)
            pos["latency_slippage_cost"] = float(tot_latency_slip)
            pos["indicative_accrued_carry"] = float(indicative_carry)
            pos["settlement_count"] = len(set(fp.get("timestamp") for fp in pos.get("funding_payments", []) if fp.get("timestamp")))
            pos["min_holding_seconds"] = int(min_hold_s)

            # ----------------- Exit Conditions -----------------

            # 1. Basis Compression Exit (Pure mathematical mean-reversion, no artificial profit masking):
            target_comp = Decimal(str(self.config.take_profit_basis_compression_pct))
            is_basis_compressed = abs(curr_basis) <= target_comp
            basis_tp_raw = is_basis_compressed

            # 2. Net Profit Target Take-Profit:
            net_tp_target = self.config.min_take_profit_net_pct
            net_tp_raw = net_pnl_pct >= net_tp_target

            # 3. Trailing Stop with Breakeven Floor (Optimization 4):
            trail_active_thresh = self.config.trailing_stop_activation_pct
            trail_distance = self.config.trailing_stop_distance_pct
            trail_floor = getattr(self.config, "breakeven_trailing_floor_pct", Decimal("0.0015"))
            if net_pnl_pct >= trail_active_thresh and not pos.get("trailing_active", False):
                pos["trailing_active"] = True
                self.logger().info(f"🛡️ [TRAILING-ARMED] Token '{token}' net PnL reached {float(net_pnl_pct):+.2%}. Trailing stop active with floor {float(trail_floor):+.2%}!")

            is_floor_breached = pos.get("trailing_active", False) and (net_pnl_pct <= trail_floor)
            trailing_stop_raw = (
                pos.get("trailing_active", False)
                and ((Decimal(str(peak_pnl)) - net_pnl_pct >= trail_distance) or is_floor_breached)
            )

            # Optimization 1: Profit-Locked Early Exit Exemption
            # If high-conviction net profit is achieved (>= early_take_profit_net_pct) and held at least min_early_exit_seconds:
            early_tp_threshold = getattr(self.config, "early_take_profit_net_pct", Decimal("0.0035"))
            min_early_s = getattr(self.config, "min_early_exit_seconds", 10)
            is_early_tp_qualified = (duration_s >= min_early_s) and (net_pnl_pct >= early_tp_threshold)

            # Exit eligibility: passed standard min hold OR qualified for profit-locked early exit
            is_exit_eligible = is_min_hold_passed or is_early_tp_qualified

            # Normal exits require exit eligibility:
            normal_exit_candidate = is_exit_eligible and (basis_tp_raw or net_tp_raw or trailing_stop_raw or is_early_tp_qualified)

            # Option 1: Right-Side Sniper Exit Evaluation (Case 52)
            is_sniper_pos = pos.get("is_sniper", False)
            if is_sniper_pos:
                sniper_tp_thresh = getattr(self.config, "sniper_take_profit_basis_pct", Decimal("0.0035"))
                sniper_stop_basis = Decimal(str(pos.get("sniper_stop_basis", 0.0250)))
                sniper_tp_raw = (abs(curr_basis) <= sniper_tp_thresh or net_pnl_pct >= self.config.min_take_profit_net_pct) and total_net_pnl > 0
                
                # Direction-aware adverse breakout check:
                # If side_1 == "SELL" (Short C1 / Long C2): adverse expansion is C1 getting higher (curr_basis >= sniper_stop_basis)
                # If side_1 == "BUY" (Long C1 / Short C2): adverse expansion is C2 getting higher (curr_basis <= -sniper_stop_basis)
                side_1_str = pos.get("side_1", "SELL")
                if side_1_str == "SELL":
                    is_adverse_breakout = curr_basis >= sniper_stop_basis
                else:
                    is_adverse_breakout = curr_basis <= -sniper_stop_basis
                sniper_sl_raw = is_adverse_breakout and (total_net_pnl < Decimal("0"))
                is_sniper_eligible = (duration_s >= 5)
                sniper_exit_candidate = is_sniper_eligible and (sniper_tp_raw or sniper_sl_raw)
            else:
                sniper_tp_raw = False
                sniper_sl_raw = False
                sniper_exit_candidate = False

            # Exit persistence: require 2 consecutive ticks before closing
            req_exit_ticks = getattr(self.config, "exit_persistence_ticks", 2)
            if normal_exit_candidate or sniper_exit_candidate:
                self.exit_candidate_ticks[token] = self.exit_candidate_ticks.get(token, 0) + 1
            else:
                self.exit_candidate_ticks[token] = 0

            is_exit_confirmed = (self.exit_candidate_ticks.get(token, 0) >= req_exit_ticks)

            early_tp_condition = is_exit_confirmed and is_early_tp_qualified
            basis_tp_condition = is_exit_confirmed and basis_tp_raw and is_exit_eligible
            net_tp_condition = is_exit_confirmed and net_tp_raw and is_exit_eligible
            trailing_stop_condition = is_exit_confirmed and trailing_stop_raw and is_exit_eligible
            sniper_tp_condition = is_exit_confirmed and is_sniper_pos and sniper_tp_raw
            sniper_sl_condition = is_exit_confirmed and is_sniper_pos and sniper_sl_raw

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
            if basis_tp_condition or net_tp_condition or trailing_stop_condition or timeout_condition or stop_loss_condition or early_tp_condition or sniper_tp_condition or sniper_sl_condition:
                if sniper_sl_condition:
                    reason = f"🛑 [SNIPER-INVALIDATION-STOP] Basis expanded past peak defense {float(pos.get('sniper_stop_basis', 0.0)):.2%} (Current: {float(curr_basis):+.2%})"
                elif sniper_tp_condition:
                    reason = f"🎯 [SNIPER-TAKE-PROFIT] Exhaustion Mean-Reversion Captured! (Peak: {float(pos.get('sniper_peak_basis', 0.0)):.2%} ➔ Current: {float(curr_basis):+.2%})"
                elif stop_loss_condition:
                    reason = f"Emergency Basis Divergence Stop-Loss ({float(net_pnl_pct):.2%} <= -{float(max_loss_pct):.2%})"
                elif early_tp_condition:
                    reason = f"Profit-Locked Early TP (+{float(net_pnl_pct):.2%} >= +{float(early_tp_threshold):.2%}, held {duration_s}s)"
                elif basis_tp_condition:
                    if total_net_pnl >= 0:
                        reason = f"Basis Mean-Reversion TP (Basis compressed to {float(curr_basis):+.2%})"
                    else:
                        reason = f"Basis Mean-Reversion Exit (Basis compressed to {float(curr_basis):+.2%})"
                elif net_tp_condition:
                    reason = f"Net PnL Target Take-Profit (+{float(net_pnl_pct):.2%} >= +{float(net_tp_target):.2%})"
                elif trailing_stop_condition:
                    if is_floor_breached:
                        reason = f"Trailing Stop Floor Protected (Peak: +{peak_pnl:.2%}, Secured at {float(net_pnl_pct):+.2%})"
                    else:
                        reason = f"Trailing Stop Take-Profit (Peak: +{peak_pnl:.2%}, Retraced to {float(net_pnl_pct):+.2%})"
                elif timeout_condition:
                    reason = f"Max Duration Timeout Exit ({duration_h:.1f}h >= {max_h:.1f}h)"
                else:
                    reason = "Close Signal"

                tokens_to_close.append({
                    "token": token,
                    "reason": reason,
                    "gross_trading_pnl": float(gross_trading_pnl),
                    "depth_slippage_cost": float(tot_depth_slip),
                    "latency_slippage_cost": float(tot_latency_slip),
                    "trading_fee": float(est_roundtrip_fees),
                    "total_friction_cost": float(total_friction_cost),
                    "net_trading_pnl": float(net_trading_pnl),
                    "paper_trading_pnl": float(paper_trading_pnl),
                    "settled_funding_pnl": float(settled_funding),
                    "indicative_accrued_carry": float(indicative_carry),
                    "funding_income": float(settled_funding),
                    "total_net_pnl": float(total_net_pnl),
                    "paper_net_pnl": float(paper_net_pnl),
                    "net_pnl_pct": float(net_pnl_pct),
                    "net_roe_pct": float(net_roe_pct),
                    "alpha_retention_pct": float(alpha_retention_pct),
                    "close_p1": float(p1_curr),
                    "close_p2": float(p2_curr),
                    "close_best_p1": float(p1_close_best),
                    "close_best_p2": float(p2_close_best),
                    "close_basis_pct": float(curr_basis),
                    "duration_seconds": int(duration_s),
                })

        for item in tokens_to_close:
            token = item["token"]
            pos = self.active_basis_arbitrages.pop(token)
            self.token_last_closed_timestamp[token] = self.current_timestamp

            # Optimization 2: Toxic Token Quarantine trigger (divergence defense)
            if "Emergency Basis Divergence Stop-Loss" in item["reason"] or float(item["total_net_pnl"]) <= -3.0:
                quarantine_s = int(getattr(self.config, "divergence_quarantine_seconds", 2700))
                self.quarantine_tokens[token] = self.current_timestamp + quarantine_s
                self.logger().warning(
                    f"🚨 [QUARANTINE-ACTIVATED] Token '{token}' quarantined for {quarantine_s // 60}m "
                    f"until {self.current_timestamp + quarantine_s} due to divergence stop-loss!"
                )
                # Option 1 (Case 52): Register in Sniper Prey Tracker for right-side counter-attack
                if getattr(self.config, "sniper_mode_enabled", True) and not pos.get("is_sniper", False):
                    peak_b = max(abs(float(item["close_basis_pct"])), abs(float(pos["entry_basis_pct"])))
                    self.sniper_prey_tracker[token] = {
                        "token": token,
                        "source": "STOP_LOSS_REVERSAL",
                        "initial_trigger_ts": self.current_timestamp,
                        "peak_basis": peak_b,
                        "peak_ts": self.current_timestamp,
                        "consecutive_contraction_ticks": 0,
                        "last_basis": abs(float(item["close_basis_pct"])),
                        "status": "TRACKING_PEAK",
                    }
                    self.logger().info(
                        f"🎯 [SNIPER-PREY-TARGETED] Token '{token}' registered for Sniper tracking after divergence stop! "
                        f"Initial Peak Basis: {peak_b:+.2%}"
                    )

            # Count unique settlement window timestamps
            unique_windows = len(set(p.get("timestamp") for p in pos.get("funding_payments", []) if p.get("timestamp")))

            pos_size_val = float(pos["position_size"])
            p1_entry_val = float(pos["entry_price_1"])
            p2_entry_val = float(pos["entry_price_2"])
            amt1_val = float(pos.get("amount_1", pos_size_val / p1_entry_val if p1_entry_val > 0 else 0.0))
            amt2_val = float(pos.get("amount_2", pos_size_val / p2_entry_val if p2_entry_val > 0 else 0.0))
            lev_val = float(pos.get("leverage", self.config.leverage))
            tot_notional_val = float(pos.get("total_notional", (amt1_val * p1_entry_val) + (amt2_val * p2_entry_val)))
            margin1_val = float(pos.get("margin_1", (amt1_val * p1_entry_val) / lev_val if lev_val > 0 else pos_size_val))
            margin2_val = float(pos.get("margin_2", (amt2_val * p2_entry_val) / lev_val if lev_val > 0 else pos_size_val))
            tot_margin_val = float(pos.get("total_margin", margin1_val + margin2_val))

            # Direction-aware captured basis:
            # When side_1 == "SELL" (Short C1 / Long C2): entry_basis - close_basis
            # When side_1 == "BUY" (Long C1 / Short C2): entry_basis + close_basis (since close_basis is (P1 - P2)/P2)
            side_1_str = pos.get("side_1", "SELL")
            eb_val = float(pos["entry_basis_pct"])
            cb_val = float(item["close_basis_pct"])
            if side_1_str == "SELL":
                captured_basis_pct = eb_val - cb_val
            else:
                captured_basis_pct = eb_val + cb_val
            captured_basis_bps = captured_basis_pct * 10000.0

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
                "entry_best_price_1": pos.get("entry_best_price_1", pos["entry_price_1"]),
                "entry_best_price_2": pos.get("entry_best_price_2", pos["entry_price_2"]),
                "close_price_1": item["close_p1"],
                "close_price_2": item["close_p2"],
                "close_best_price_1": item.get("close_best_p1", item["close_p1"]),
                "close_best_price_2": item.get("close_best_p2", item["close_p2"]),
                "entry_basis_pct": pos["entry_basis_pct"],
                "close_basis_pct": item["close_basis_pct"],
                "captured_basis_pct": round(captured_basis_pct, 6),
                "captured_basis_bps": round(captured_basis_bps, 2),
                "position_size": pos["position_size"],
                "safe_capacity": pos.get("safe_capacity", pos_size_val),
                "top3_depth_1": pos.get("top3_depth_1", 0.0),
                "top3_depth_2": pos.get("top3_depth_2", 0.0),
                "net_executable_edge_pct": pos.get("net_executable_edge_pct", 0.0),
                "single_notional": pos_size_val,
                "total_notional": tot_notional_val,
                "leverage": pos["leverage"],
                "margin_1": margin1_val,
                "margin_2": margin2_val,
                "total_margin": tot_margin_val,
                "gross_trading_pnl": item["gross_trading_pnl"],
                "depth_slippage_cost": item["depth_slippage_cost"],
                "latency_slippage_cost": item["latency_slippage_cost"],
                "trading_fee": item["trading_fee"],
                "total_friction_cost": item["total_friction_cost"],
                "net_trading_pnl": item["net_trading_pnl"],
                "paper_trading_pnl": item.get("paper_trading_pnl", item["gross_trading_pnl"] - item["trading_fee"]),
                "trading_pnl": item["net_trading_pnl"],
                "funding_pnl": item["settled_funding_pnl"],
                "indicative_accrued_carry": item["indicative_accrued_carry"],
                "settlement_count": unique_windows,
                "funding_payments": pos.get("funding_payments", []),
                "total_net_pnl": item["total_net_pnl"],
                "paper_net_pnl": item.get("paper_net_pnl", item["gross_trading_pnl"] - item["trading_fee"] + item["settled_funding_pnl"]),
                "net_pnl_pct": item["net_pnl_pct"],
                "net_roe_pct": item["net_roe_pct"],
                "alpha_retention_pct": item["alpha_retention_pct"],
                "close_reason": item["reason"],
                "is_sniper": pos.get("is_sniper", False),
                "sniper_peak_basis": pos.get("sniper_peak_basis", 0.0),
                "sniper_entry_basis": pos.get("sniper_entry_basis", 0.0),
                "sniper_stop_basis": pos.get("sniper_stop_basis", 0.0),
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
                f"  Basis Dynamics: 开仓 {pos['entry_basis_pct']:+.2%} ➔ 平仓 {item['close_basis_pct']:+.2%} | 实际捕获基差: {captured_basis_bps:+.1f} bps ({captured_basis_pct:+.2%})\n"
                f"  Friction Audit: Gross ${item['gross_trading_pnl']:+.2f} - DepthSlip ${item['depth_slippage_cost']:.2f} - LatencySlip ${item['latency_slippage_cost']:.2f} - Fee ${item['trading_fee']:.2f} = NetTrade ${item['net_trading_pnl']:+.2f}\n"
                f"  Settled Funding: ${item['settled_funding_pnl']:+.2f} ({unique_windows} windows), Total Net: ${item['total_net_pnl']:+.2f} (ROE {item['net_roe_pct']:+.2%}, Alpha Retention: {item['alpha_retention_pct']:.1f}%), Held: {item['duration_seconds']}s"
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
                "captured_basis_pct": 0.0,
                "captured_basis_bps": 0.0,
                "position_size": pos["position_size"],
                "leverage": pos["leverage"],
                "gross_trading_pnl": 0.0,
                "depth_slippage_cost": 0.0,
                "latency_slippage_cost": 0.0,
                "trading_fee": 0.0,
                "total_friction_cost": 0.0,
                "net_trading_pnl": 0.0,
                "trading_pnl": 0.0,
                "funding_pnl": settled,
                "indicative_accrued_carry": 0.0,
                "settlement_count": len(pos.get("funding_payments", [])),
                "funding_payments": pos.get("funding_payments", []),
                "total_net_pnl": settled,
                "net_pnl_pct": settled / pos["position_size"] if pos.get("position_size") else 0.0,
                "alpha_retention_pct": 0.0,
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
            "market_discovery_candidates": getattr(self, "market_discovery_candidates", []),
            "rotation_history": getattr(self, "rotation_history", []),
            "token_admitted_at": getattr(self, "token_admitted_at", {}),
            "active_universe_tokens": list(self.config.tokens),
            "quarantined_tokens": {tok: ts for tok, ts in self.quarantine_tokens.items() if self.current_timestamp < ts},
            "sniper_prey_tracker": {
                tok: data for tok, data in self.sniper_prey_tracker.items()
                if self.current_timestamp - float(data.get("initial_trigger_ts", 0.0)) < int(getattr(self.config, "sniper_max_prey_dwell_seconds", 3600))
            },
            "tick_latency_ms": round(getattr(self, "_last_tick_latency_ms", 0.0), 1),
            "auto_rotation_enabled": getattr(self.config, "auto_rotation_enabled", True),
            "auto_rotation_interval_minutes": getattr(self.config, "auto_rotation_interval_minutes", 3.0),
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

                    # Top 3 depths for both connectors
                    c1_bid_t1, c1_bid_t3 = self.get_orderbook_depth_summary(c1, tp1, is_buy=False, top_n=3)
                    c1_ask_t1, c1_ask_t3 = self.get_orderbook_depth_summary(c1, tp1, is_buy=True, top_n=3)
                    c2_bid_t1, c2_bid_t3 = self.get_orderbook_depth_summary(c2, tp2, is_buy=False, top_n=3)
                    c2_ask_t1, c2_ask_t3 = self.get_orderbook_depth_summary(c2, tp2, is_buy=True, top_n=3)

                    c1_top3_avg = (c1_bid_t3 + c1_ask_t3) / Decimal("2.0")
                    c2_top3_avg = (c2_bid_t3 + c2_ask_t3) / Decimal("2.0")
                    min_top3 = min(c1_top3_avg, c2_top3_avg)
                    ratio = getattr(self.config, "max_orderbook_depth_ratio", Decimal("0.20"))
                    min_order_sz = getattr(self.config, "min_order_size_quote", Decimal("300.0"))
                    base_sz = self.config.position_size_quote
                    is_dynamic = getattr(self.config, "dynamic_capacity_sizing", True)
                    
                    if is_dynamic:
                        safe_cap = min(base_sz, max(min_order_sz, min_top3 * ratio)) if min_top3 >= min_order_sz else min_top3
                    else:
                        safe_cap = base_sz

                    # Executable quotes using safe capacity
                    test_vol = max(Decimal("10.0"), safe_cap)
                    p1_bid_b, p1_bid_v, p1_bid_exec, p1_bid_s = self.get_orderbook_execution_metrics(c1, tp1, is_buy=False, quote_volume=test_vol)
                    p1_ask_b, p1_ask_v, p1_ask_exec, p1_ask_s = self.get_orderbook_execution_metrics(c1, tp1, is_buy=True, quote_volume=test_vol)
                    p2_bid_b, p2_bid_v, p2_bid_exec, p2_bid_s = self.get_orderbook_execution_metrics(c2, tp2, is_buy=False, quote_volume=test_vol)
                    p2_ask_b, p2_ask_v, p2_ask_exec, p2_ask_s = self.get_orderbook_execution_metrics(c2, tp2, is_buy=True, quote_volume=test_vol)

                    p1_bid = p1_bid_exec
                    p1_ask = p1_ask_exec
                    p2_bid = p2_bid_exec
                    p2_ask = p2_ask_exec

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

                        # Net edge calculation
                        taker_fees = float(getattr(self.config, "taker_fee_rate", Decimal("0.0005")) * 4)
                        latency_fees = float(getattr(self.config, "simulated_latency_slippage_pct", Decimal("0.0002")) * 4)
                        if spread_sell1_buy2 >= spread_buy1_sell2:
                            slip_est = float((p1_bid_s or Decimal("0")) + (p2_ask_s or Decimal("0")))
                        else:
                            slip_est = float((p1_ask_s or Decimal("0")) + (p2_bid_s or Decimal("0")))
                        net_edge = best_spread - taker_fees - latency_fees - slip_est

                        thresh = float(self.config.min_entry_basis_pct)
                        min_edge_thresh = float(getattr(self.config, "min_executable_net_edge_pct", Decimal("0.0018")))
                        q_until = self.quarantine_tokens.get(t, 0.0)
                        if self.current_timestamp < q_until:
                            rem_m = max(1, int((q_until - self.current_timestamp) / 60))
                            status_str = f"🚨 动量撕裂冷冻隔离中 (剩余 {rem_m}m)"
                        elif min_top3 < min_order_sz:
                            status_str = f"🛡️ 流动性虚幻/拦截 (Top3: ${float(min_top3):,.0f})"
                        elif best_spread >= thresh and net_edge < min_edge_thresh:
                            status_str = f"🛡️ 穿透拦截 (净利 {net_edge:+.2%} < {min_edge_thresh*10000:.0f}bps)"
                        elif best_spread >= thresh:
                            dir_str = "Short C1/Long C2" if spread_sell1_buy2 >= spread_buy1_sell2 else "Long C1/Short C2"
                            status_str = f"⚡ 偏离超标 {best_spread:+.2%} ({dir_str})"
                        else:
                            status_str = f"⚖️ 窄幅正常 (最高 {best_spread:+.2%})"

                        display_p1 = float(p1_ind) if p1_ind is not None else float(p1_ask)
                        display_p2 = float(p2_ind) if p2_ind is not None else float(p2_ask)
                    else:
                        best_spread = 0.0
                        net_edge = 0.0
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
                        "c1_top3_depth": float(c1_top3_avg),
                        "c2_top3_depth": float(c2_top3_avg),
                        "safe_capacity": float(safe_cap),
                        "net_executable_edge_pct": float(net_edge),
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
            "quarantined_tokens": {tok: ts for tok, ts in self.quarantine_tokens.items() if self.current_timestamp < ts},
            "sniper_prey_tracker": {
                tok: data for tok, data in self.sniper_prey_tracker.items()
                if self.current_timestamp - float(data.get("initial_trigger_ts", 0.0)) < int(getattr(self.config, "sniper_max_prey_dwell_seconds", 3600))
            },
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
        lines.append(f"  Dynamic Sizing: {'Enabled (Top3 Depth Cap)' if getattr(self.config, 'dynamic_capacity_sizing', True) else 'Disabled'} | Min Order: ${float(getattr(self.config, 'min_order_size_quote', Decimal('300.0'))):,.0f}")
        lines.append(f"  Executable VWAP Gates: Min Net Edge {float(getattr(self.config, 'min_executable_net_edge_pct', Decimal('0.0030'))):.2%} | Max Leg Slip {float(getattr(self.config, 'max_executable_depth_slippage_pct', Decimal('0.0025'))):.2%}")
        margin_check_status = "Enabled" if getattr(self.config, "check_margin_before_entry", True) else "Disabled"
        buf_pct = float(getattr(self.config, "margin_buffer_ratio", Decimal("0.05")))
        downsizing_status = "Enabled" if getattr(self.config, "margin_aware_downsizing", True) else "Disabled"
        lines.append(f"  Pre-Flight Margin Gate: {margin_check_status} (Buffer: {buf_pct:.1%}, Downsizing: {downsizing_status})")
        if not self.config.dry_run and len(self.config.connectors) >= 2:
            c1, c2 = list(self.config.connectors)[:2]
            tp1_sample = self.get_trading_pair_for_connector("BTC", c1)
            tp2_sample = self.get_trading_pair_for_connector("BTC", c2)
            avail_1, asset_1 = self.get_connector_available_collateral(c1, tp1_sample)
            avail_2, asset_2 = self.get_connector_available_collateral(c2, tp2_sample)
            lines.append(f"  Live Available Balances: {c1}: ${float(avail_1):,.2f} {asset_1} | {c2}: ${float(avail_2):,.2f} {asset_2}")
        else:
            virt_tot = getattr(self.config, "dry_run_virtual_balance_quote", Decimal("0.0"))
            if virt_tot > Decimal("0.0"):
                used_m = sum(Decimal(str(p.get("total_margin", 0.0))) for p in self.active_basis_arbitrages.values())
                lines.append(f"  Virtual Margin: Available ${float(virt_tot - used_m):,.2f} / Total ${float(virt_tot):,.2f} (Used: ${float(used_m):,.2f})")
            else:
                lines.append("  Virtual Margin: Unlimited Dry-Run")
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
        lines.append(f"  Dynamic Rotation: {'Enabled' if getattr(self.config, 'auto_rotation_enabled', True) else 'Disabled'} | Interval: {getattr(self.config, 'auto_rotation_interval_minutes', 3.0):.1f}m | Swaps: {len(getattr(self, 'rotation_history', []))}")
        latency = getattr(self, "_last_tick_latency_ms", 0.0)
        lines.append(f"  Tick Engine: 1.0s Loop | Latency: {latency:.1f}ms | Headroom: {max(0.0, 100.0 - latency / 10.0):.1f}%")
        return "\n".join(lines)

    # =========================================================================
    # Two-Tier Dynamic Universe Rotation & Hot-Swap Engine
    # =========================================================================

    def check_universe_rotation(self):
        """Schedule periodic full-market background scan and dynamic hot-swap."""
        if not getattr(self.config, "auto_rotation_enabled", True):
            return
        now = self.current_timestamp
        interval_sec = getattr(self.config, "auto_rotation_interval_minutes", 3.0) * 60.0
        if now - getattr(self, "_last_rotation_check_ts", 0.0) >= interval_sec and not getattr(self, "_rotation_in_progress", False):
            self._last_rotation_check_ts = now
            safe_ensure_future(self._async_check_and_rotate_universe())

    async def _async_check_and_rotate_universe(self):
        """Full market candidate discovery and dynamic hot-swapping execution."""
        if getattr(self, "_rotation_in_progress", False):
            return
        self._rotation_in_progress = True
        try:
            candidates = await asyncio.to_thread(self._fetch_full_market_universe_candidates_sync)
            if candidates:
                self.market_discovery_candidates = candidates
                await self._evaluate_and_execute_hot_swaps(candidates)
                self._persist_state()
        except Exception as e:
            self.logger().warning(f"Error in dynamic universe rotation check: {e}")
        finally:
            self._rotation_in_progress = False

    def _fetch_full_market_universe_candidates_sync(self) -> List[Dict]:
        """
        Lightweight full-market REST discovery (Tier 1).
        Scans all perpetual pairs between Binance and Hyperliquid (~185 tokens).
        Calculates indicative basis dislocation, 24h quote volumes, and mark prices.
        """
        import socket
        orig_getaddrinfo = socket.getaddrinfo
        def getaddrinfo_ipv4(host, port, family=0, type=0, proto=0, flags=0):
            return orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)

        try:
            # Force IPv4 preference for container compatibility
            socket.getaddrinfo = getaddrinfo_ipv4

            # 1. Hyperliquid perpetuals (filter out delisted tokens & inactive orderbooks)
            hl_req = urllib.request.Request(
                "https://api.hyperliquid.xyz/info",
                data=json.dumps({"type": "metaAndAssetCtxs"}).encode("utf-8"),
                headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}
            )
            with urllib.request.urlopen(hl_req, timeout=20) as resp:
                hl_raw = json.loads(resp.read().decode("utf-8"))

            hl_assets = {}
            for i, asset_meta in enumerate(hl_raw[0].get("universe", [])):
                # Gate 1: Explicit delisting check (Hyperliquid sets isDelisted: True for ceased markets)
                if asset_meta.get("isDelisted", False):
                    continue

                name = asset_meta.get("name")
                if i < len(hl_raw[1]):
                    ctx = hl_raw[1][i]
                    # Gate 2: Active orderbook verification (midPx is None if trading is halted or no quotes)
                    mid_px = ctx.get("midPx")
                    if mid_px is None:
                        continue

                    mid = float(mid_px)
                    vol = float(ctx.get("dayNtlVlm") or 0.0)
                    funding = float(ctx.get("funding") or 0.0)
                    hl_assets[name] = {"price": mid, "volume_24h": vol, "funding_rate": funding}

            # 2. Binance perpetual exchangeInfo & 24hr tickers
            # Gate 2.1: Strictly filter for active TRADING & pure crypto PERPETUAL contracts (excludes 134+ SETTLING/delisted and 225+ TRADIFI contracts)
            now_ts = time.time()
            if not hasattr(self, "_cached_bn_perps") or now_ts - getattr(self, "_cached_bn_perps_ts", 0) > 3600.0:
                try:
                    bn_info_req = urllib.request.Request(
                        "https://fapi.binance.com/fapi/v1/exchangeInfo",
                        headers={"User-Agent": "Mozilla/5.0"}
                    )
                    with urllib.request.urlopen(bn_info_req, timeout=10) as resp:
                        bn_info_raw = json.loads(resp.read().decode("utf-8"))
                    self._cached_bn_perps = {
                        s["symbol"] for s in bn_info_raw.get("symbols", [])
                        if s.get("contractType") == "PERPETUAL"
                        and s.get("status") == "TRADING"
                        and s.get("quoteAsset") == "USDT"
                    }
                    self._cached_bn_perps_ts = now_ts
                except Exception as e:
                    self.logger().warning(f"Failed to refresh Binance exchangeInfo cache: {e}")
                    if not hasattr(self, "_cached_bn_perps"):
                        self._cached_bn_perps = set()

            bn_req = urllib.request.Request(
                "https://fapi.binance.com/fapi/v1/ticker/24hr",
                headers={"User-Agent": "Mozilla/5.0"}
            )
            with urllib.request.urlopen(bn_req, timeout=20) as resp:
                bn_raw = json.loads(resp.read().decode("utf-8"))

            bn_assets = {}
            for item in bn_raw:
                sym = item.get("symbol", "")
                if self._cached_bn_perps and sym not in self._cached_bn_perps:
                    continue
                if sym.endswith("USDT"):
                    base = sym[:-4]
                    bn_assets[base] = {
                        "price": float(item.get("lastPrice") or 0.0),
                        "volume_24h": float(item.get("quoteVolume") or 0.0),
                        "symbol": sym
                    }

            canonical_multipliers = {
                "kPEPE": 1000.0, "kBONK": 1000.0, "kSHIB": 1000.0, "kFLOKI": 1000.0,
                "kLUNC": 1000.0, "kNEIRO": 1000.0, "kDOGS": 1000.0
            }
            canonical_reverse = {
                "kPEPE": "1000PEPE", "kBONK": "1000BONK", "kSHIB": "1000SHIB", "kFLOKI": "1000FLOKI",
                "kLUNC": "1000LUNC", "kNEIRO": "NEIRO", "1000PEPE": "1000PEPE", "1000BONK": "1000BONK",
                "1000SHIB": "1000SHIB", "1000FLOKI": "1000FLOKI", "1000SATS": "1000SATS", "1000RATS": "1000RATS",
                "1000CAT": "1000CAT", "1000XEC": "1000XEC"
            }

            active_tokens_set = set(self.config.tokens)
            candidates = []

            for hl_name, hl_data in hl_assets.items():
                mult_hl = canonical_multipliers.get(hl_name, 1.0)
                norm_hl = canonical_reverse.get(hl_name, hl_name)
                mult_bn = 1.0
                bn_match = None

                if norm_hl in bn_assets:
                    bn_match = norm_hl
                    if norm_hl.startswith("1000") and not hl_name.startswith(("1000", "k")):
                        mult_bn = 1000.0
                elif f"1000{norm_hl}" in bn_assets:
                    bn_match = f"1000{norm_hl}"
                    mult_bn = 1000.0

                if bn_match and bn_match in bn_assets:
                    bn_data = bn_assets[bn_match]
                    hl_px = hl_data["price"] / mult_hl
                    bn_px = bn_data["price"] / mult_bn

                    if bn_px > 0 and hl_px > 0:
                        # Gate 3: Normalized Price Divergence Circuit Breaker (Homonym & Split Guard)
                        # Two contracts tracking the same underlying asset never diverge by >2.0% in normal markets.
                        avg_px = (hl_px + bn_px) / 2.0
                        divergence = abs(hl_px - bn_px) / avg_px
                        if divergence > 0.02:
                            # Rejects homonym ticker collisions, rebrand desyncs (e.g. RNDR vs RENDER), or fake pairs
                            continue

                        basis_pct = (hl_px - bn_px) / bn_px
                        hl_vol = hl_data["volume_24h"]
                        bn_vol = bn_data["volume_24h"]
                        min_vol = min(hl_vol, bn_vol)
                        is_active = norm_hl in active_tokens_set or hl_name in active_tokens_set

                        candidates.append({
                            "token": norm_hl if not norm_hl.startswith("1000") else norm_hl[4:],
                            "hl_symbol": hl_name,
                            "bn_symbol": bn_match,
                            "hl_price": hl_px,
                            "bn_price": bn_px,
                            "basis_pct": round(basis_pct, 6),
                            "basis_spread_pct": round(basis_pct, 6),
                            "hl_volume_24h": round(hl_vol, 2),
                            "bn_volume_24h": round(bn_vol, 2),
                            "min_volume_24h": round(min_vol, 2),
                            "hl_funding_rate": hl_data.get("funding_rate", 0.0),
                            "is_active": is_active,
                            "status": "ACTIVE" if is_active else "STANDBY",
                            "dwell_seconds": int(time.time() - self.token_admitted_at.get(norm_hl, time.time())) if is_active else 0,
                            "scanned_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                        })

            candidates.sort(key=lambda x: abs(x["basis_pct"]), reverse=True)
            standby_dislocated = [c["token"] + ": " + f"{c['basis_pct']:+.2%}" for c in candidates if not c["is_active"]][:5]
            self.logger().info(
                f"🔭 [UNIVERSE-SCAN] Scanned {len(candidates)} shared perpetual pairs across Hyperliquid & Binance. "
                f"Top Standby: {standby_dislocated}"
            )
            return candidates

        except Exception as e:
            self.logger().warning(f"Failed to fetch market candidates via REST: {e}")
            return []
        finally:
            socket.getaddrinfo = orig_getaddrinfo

    async def _evaluate_and_execute_hot_swaps(self, candidates: List[Dict]):
        """
        Evaluate candidate tokens vs current active universe tokens (Tier 2).
        Evicts dormant 'zombie' tokens with low basis & no active positions,
        replacing them with high-dislocation liquid candidates.
        """
        if not getattr(self.config, "auto_rotation_enabled", True):
            return

        min_entry_basis = float(getattr(self.config, "auto_rotation_min_entry_basis_pct", Decimal("0.0030")))
        min_vol_bn = float(getattr(self.config, "auto_rotation_min_volume_24h_usd", Decimal("2000000.0")))
        min_vol_hl = 500000.0
        min_delta_edge = float(getattr(self.config, "auto_rotation_min_delta_edge_pct", Decimal("0.0030")))
        min_dwell_sec = float(getattr(self.config, "auto_rotation_min_dwell_minutes", 30.0)) * 60.0
        max_swaps = int(getattr(self.config, "auto_rotation_max_swaps_per_cycle", 2))

        # 1. Filter standby candidates qualifying for entry
        qualifying_candidates = [
            c for c in candidates
            if not c["is_active"]
            and abs(c["basis_pct"]) >= min_entry_basis
            and c["bn_volume_24h"] >= min_vol_bn
            and c["hl_volume_24h"] >= min_vol_hl
        ]

        if not qualifying_candidates:
            return

        # 2. Identify active zombie tokens eligible for eviction
        now = time.time()
        active_spread_map = {c["token"]: abs(c["basis_pct"]) for c in candidates if c["is_active"]}

        eligible_victims = []
        for tok in list(self.config.tokens):
            # Hard Rule 1: Active positions have absolute immunity
            if tok in self.active_basis_arbitrages:
                continue

            # Hard Rule 2: Minimum dwell time barrier (avoid flapping)
            admit_ts = self.token_admitted_at.get(tok, now)
            dwell_s = now - admit_ts
            if dwell_s < min_dwell_sec:
                continue

            spread = active_spread_map.get(tok, 0.0)
            eligible_victims.append({
                "token": tok,
                "spread": spread,
                "dwell_s": dwell_s
            })

        # Sort eligible victims by spread ascending (coldest first)
        eligible_victims.sort(key=lambda x: x["spread"])

        if not eligible_victims:
            self.logger().info(
                f"[UNIVERSE-HOT-SWAP] Found {len(qualifying_candidates)} qualified candidates, but all active tokens are protected (in position or dwell < {min_dwell_sec/60:.0f}m)."
            )
            return

        # 3. Execute pairwise hot-swaps
        swaps_done = 0
        for cand in qualifying_candidates:
            if swaps_done >= max_swaps or not eligible_victims:
                break

            victim = eligible_victims.pop(0)
            delta_edge = abs(cand["basis_pct"]) - victim["spread"]

            # Hard Rule 3: Delta edge advantage requirement
            if delta_edge < min_delta_edge:
                self.logger().info(
                    f"[UNIVERSE-HOT-SWAP] Standby {cand['token']} spread {abs(cand['basis_pct']):.2%} delta edge over victim {victim['token']} {victim['spread']:.2%} is {delta_edge:.2%} < threshold {min_delta_edge:.2%}. Skipping."
                )
                continue

            reason = (
                f"Dynamic Hot-Swap: Candidate {cand['token']} (|ΔP|={abs(cand['basis_pct']):.2%}, Vol=${cand['min_volume_24h']/1e6:.1f}M) "
                f"replaces idle {victim['token']} (|ΔP|={victim['spread']:.2%}, Dwell={victim['dwell_s']/60:.1f}m, Δ={delta_edge:+.2%})"
            )
            await self._async_hot_swap_token(
                evict_tok=victim["token"],
                admit_tok=cand["token"],
                reason=reason,
                evicted_basis=victim["spread"],
                admitted_basis=abs(cand["basis_pct"]),
                admitted_volume=cand["min_volume_24h"]
            )
            swaps_done += 1

    async def _async_hot_swap_token(
        self,
        evict_tok: str,
        admit_tok: str,
        reason: str,
        evicted_basis: float = 0.0,
        admitted_basis: float = 0.0,
        admitted_volume: float = 0.0,
    ):
        """
        Dynamically hot-swap trading pairs on live connectors without restarting.
        Unmounts evict_tok and mounts admit_tok via native add_trading_pair / remove_trading_pair.
        """
        async with self._universe_mutation_lock:
            self.logger().info(
                f"🔄 [UNIVERSE-HOT-SWAP] Starting hot-swap: Evicting '{evict_tok}' ➔ Admitting '{admit_tok}'. Reason: {reason}"
            )

            # 1. Unmount evicted token
            for c_name, conn in self.connectors.items():
                evict_pair = self.get_trading_pair_for_connector(evict_tok, c_name)
                if hasattr(conn, "remove_trading_pair"):
                    try:
                        await conn.remove_trading_pair(evict_pair)
                        self.logger().info(f"[UNIVERSE-HOT-SWAP] Removed {evict_pair} from {c_name}")
                    except Exception as e:
                        self.logger().warning(f"Error removing {evict_pair} from {c_name}: {e}")

            # 2. Mount admitted token
            for c_name, conn in self.connectors.items():
                admit_pair = self.get_trading_pair_for_connector(admit_tok, c_name)
                if hasattr(conn, "add_trading_pair"):
                    try:
                        success = await conn.add_trading_pair(admit_pair)
                        self.logger().info(f"[UNIVERSE-HOT-SWAP] Added {admit_pair} to {c_name}: success={success}")
                    except Exception as e:
                        self.logger().warning(f"Error adding {admit_pair} to {c_name}: {e}")

                if not self.config.dry_run and hasattr(conn, "set_leverage"):
                    try:
                        conn.set_leverage(admit_pair, self.config.leverage)
                    except Exception as e:
                        self.logger().warning(f"Error setting leverage for {admit_pair} on {c_name}: {e}")

            # 3. Update configuration universe in memory
            if isinstance(self.config.tokens, set):
                self.config.tokens.discard(evict_tok)
                self.config.tokens.add(admit_tok)
            elif isinstance(self.config.tokens, list):
                if evict_tok in self.config.tokens:
                    self.config.tokens.remove(evict_tok)
                if admit_tok not in self.config.tokens:
                    self.config.tokens.append(admit_tok)

            now_ts = time.time()
            self.token_admitted_at[admit_tok] = now_ts
            self.token_admitted_at.pop(evict_tok, None)

            # 4. Record rotation history
            rec = {
                "timestamp": int(now_ts),
                "time_str": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "evicted_token": evict_tok,
                "admitted_token": admit_tok,
                "evicted_basis_pct": round(evicted_basis, 6),
                "admitted_basis_pct": round(admitted_basis, 6),
                "delta_edge_pct": round(admitted_basis - evicted_basis, 6),
                "admitted_volume_usd": round(admitted_volume, 2),
                "reason": reason,
            }
            self.rotation_history.append(rec)
            if len(self.rotation_history) > 100:
                self.rotation_history = self.rotation_history[-100:]

            # 5. Persist state and sync to disk YAML if possible
            self._persist_state()
            self._sync_tokens_to_yaml_if_possible()
            self.logger().info(
                f"✅ [UNIVERSE-HOT-SWAP] Hot-swap complete! Universe now has {len(self.config.tokens)} tokens. "
                f"New member '{admit_tok}' is now live in WebSocket stream."
            )

    def _sync_tokens_to_yaml_if_possible(self):
        """Update script YAML config files with the current dynamically rotated tokens."""
        tokens_str = ",".join(self.config.tokens)
        conf_candidates = [
            "/home/hummingbot/conf/scripts/basis_stat_arb_hyperliquid_binance_dry_run.yml",
            "/home/hummingbot/conf/scripts/basis_stat_arb_hyperliquid_binance_micro_live.yml",
        ]
        for cpath in conf_candidates:
            if os.path.exists(cpath):
                try:
                    import yaml
                    with open(cpath, "r") as f:
                        data = yaml.safe_load(f) or {}
                    data["tokens"] = tokens_str
                    with open(cpath, "w") as f:
                        yaml.safe_dump(data, f, sort_keys=False)
                    self.logger().info(f"[UNIVERSE-HOT-SWAP] Synced updated tokens to {cpath}")
                except Exception as e:
                    self.logger().warning(f"Could not update config yaml {cpath}: {e}")
