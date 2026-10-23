import asyncio
import copy
import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple

from pydantic import Field, field_validator

from hummingbot.client.config.config_data_types import ClientConfigEnum
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate
from hummingbot.core.event.events import BuyOrderCompletedEvent, FundingPaymentCompletedEvent, SellOrderCompletedEvent
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase


class SpotPerpCashCarryConfig(StrategyV2ConfigBase):
    strategy_name: str = "spot_perp_cash_carry"
    spot_connector: str = Field(
        default="binance",
        json_schema_extra={
            "prompt": lambda mi: "Enter Spot exchange connector (e.g. binance): ",
            "prompt_on_new": False,
        }
    )
    perp_connector: str = Field(
        default="hyperliquid_perpetual",
        json_schema_extra={
            "prompt": lambda mi: "Enter Perpetual exchange connector (e.g. hyperliquid_perpetual): ",
            "prompt_on_new": False,
        }
    )
    tokens: List[str] = Field(
        default=[
            "ENA", "SAGA", "W", "REZ", "ALT", "OP", "TIA", "SEI",
            "NEAR", "JUP", "WIF", "LDO", "RENDER", "SUI", "APT"
        ],
        json_schema_extra={
            "prompt": lambda mi: "Enter candidate tokens watchlist (comma-separated): ",
            "prompt_on_new": True,
        }
    )
    position_size_quote: Decimal = Field(
        default=Decimal("1000.0"),
        json_schema_extra={
            "prompt": lambda mi: "Enter default position size in quote asset per leg (e.g. 1000.0 USD): ",
            "prompt_on_new": True,
        }
    )
    max_concurrent_tokens: int = Field(
        default=3, gt=0,
        json_schema_extra={
            "prompt": lambda mi: "Enter maximum concurrent tokens held (e.g. 3): ",
            "prompt_on_new": True,
        }
    )
    perp_leverage: int = Field(
        default=3, gt=0, le=10,
        json_schema_extra={
            "prompt": lambda mi: "Enter leverage for Perpetual short leg (e.g. 3): ",
            "prompt_on_new": True,
        }
    )
    min_entry_annualized_apr: Decimal = Field(
        default=Decimal("0.20"),
        json_schema_extra={
            "prompt": lambda mi: "Min annualized funding APR to enter (e.g. 0.20 for 20% APR): ",
            "prompt_on_new": True,
        }
    )
    exit_annualized_apr: Decimal = Field(
        default=Decimal("0.08"),
        json_schema_extra={
            "prompt": lambda mi: "Exit when annualized funding APR drops below (e.g. 0.08 for 8% APR): ",
            "prompt_on_new": True,
        }
    )
    inversion_exit_apr: Decimal = Field(
        default=Decimal("0.00"),
        json_schema_extra={
            "prompt": lambda mi: "Emergency exit threshold when funding turns adverse/negative (e.g. 0.00): ",
            "prompt_on_new": False,
        }
    )

    # --- Realistic Payback Horizon & Friction Allocation ---
    max_payback_hours: Decimal = Field(
        default=Decimal("72.0"),
        json_schema_extra={
            "prompt": lambda mi: "Max hours of funding to recover roundtrip friction (e.g. 72.0): ",
            "prompt_on_new": True,
        }
    )
    target_horizon_hours: Decimal = Field(
        default=Decimal("336.0"),
        json_schema_extra={
            "prompt": lambda mi: "Target holding horizon in hours for cash & carry yield allocation (e.g. 336.0 for 14d): ",
            "prompt_on_new": False,
        }
    )
    max_friction_to_yield_ratio: Decimal = Field(
        default=Decimal("0.30"),
        json_schema_extra={
            "prompt": lambda mi: "Max allowable ratio of entry friction over expected horizon yield (e.g. 0.30 for 30%): ",
            "prompt_on_new": False,
        }
    )

    # --- Multi-Tier Asset Classification Matrix ---
    tier_1_tokens: List[str] = Field(
        default=["BTC", "ETH", "SOL"],
        json_schema_extra={"prompt": lambda mi: "Tier 1 mega-cap tokens: ", "prompt_on_new": False}
    )
    tier_1_max_quote: Decimal = Field(default=Decimal("5000.0"))
    tier_1_min_apr: Decimal = Field(default=Decimal("0.12"))
    tier_1_liquidation_buffer: Decimal = Field(default=Decimal("0.08"))

    tier_2_tokens: List[str] = Field(
        default=[
            "AVAX", "SUI", "APT", "NEAR", "LINK", "ARB", "OP", "INJ",
            "DOGE", "XRP", "ADA", "DOT", "UNI", "LTC", "BCH", "BNB"
        ],
        json_schema_extra={"prompt": lambda mi: "Tier 2 large/mid-cap tokens: ", "prompt_on_new": False}
    )
    tier_2_max_quote: Decimal = Field(default=Decimal("2000.0"))
    tier_2_min_apr: Decimal = Field(default=Decimal("0.18"))
    tier_2_liquidation_buffer: Decimal = Field(default=Decimal("0.12"))

    tier_3_max_quote: Decimal = Field(default=Decimal("500.0"))
    tier_3_min_apr: Decimal = Field(default=Decimal("0.25"))
    tier_3_liquidation_buffer: Decimal = Field(default=Decimal("0.16"))

    # --- Dynamic Order Book Depth Sizing ---
    dynamic_position_sizing: bool = Field(default=True)
    max_orderbook_depth_ratio: Decimal = Field(default=Decimal("0.20"))
    min_position_size_quote: Decimal = Field(default=Decimal("100.0"))

    # --- Hourly Micro-Blackout Window (:58 ~ :01) ---
    hourly_blackout_enabled: bool = Field(default=True)
    hourly_blackout_start_minute: int = Field(default=58, ge=0, le=59)
    hourly_blackout_end_minute: int = Field(default=1, ge=0, le=59)

    # --- Breakeven Trailing Stop & Tranche Take-Profit ---
    breakeven_trailing_enabled: bool = Field(default=True)
    breakeven_trailing_activation_pct: Decimal = Field(default=Decimal("0.008"))
    breakeven_trailing_stop_pct: Decimal = Field(default=Decimal("0.002"))
    scaling_tp_enabled: bool = Field(default=True)
    partial_tp_ratio: Decimal = Field(default=Decimal("0.50"))
    partial_tp_threshold_pct: Decimal = Field(default=Decimal("0.015"))

    # --- Toxic Token Quarantine & Risk Controls ---
    token_quarantine_hours: float = Field(default=24.0)
    single_loss_quarantine_threshold: Decimal = Field(default=Decimal("10.0"))
    liquidation_buffer_pct: Decimal = Field(default=Decimal("0.15"))
    min_holding_seconds: int = Field(default=3600)
    max_hold_hours: float = Field(default=168.0)
    entry_persistence_ticks: int = Field(default=2, ge=1)
    exit_persistence_ticks: int = Field(default=2, ge=1)
    auto_rotation_enabled: bool = Field(default=True)
    auto_rotation_interval_minutes: float = Field(default=3.0, ge=0.5)
    auto_rotation_min_delta_apr: Decimal = Field(default=Decimal("0.20"))
    min_order_book_depth_quote: Decimal = Field(default=Decimal("1000.0"))
    spot_taker_fee_rate: Decimal = Field(default=Decimal("0.0010"))
    perp_taker_fee_rate: Decimal = Field(default=Decimal("0.00035"))
    simulated_latency_slippage_pct: Decimal = Field(default=Decimal("0.0002"))
    dry_run: bool = Field(default=True)
    dry_run_virtual_spot_quote: Decimal = Field(default=Decimal("5000.0"))
    dry_run_virtual_perp_quote: Decimal = Field(default=Decimal("2500.0"))

    @field_validator("tokens", "tier_1_tokens", "tier_2_tokens", mode="before")
    def validate_tokens(cls, v):
        if isinstance(v, str):
            return [val.strip().upper() for val in v.split(",") if val.strip()]
        if isinstance(v, (list, set, tuple)):
            return [str(val).strip().upper() for val in v if str(val).strip()]
        return v


class SpotPerpCashCarryStrategy(StrategyV2Base):
    """
    Spot-Perp Cash & Carry Quantitative Yield Harvesting Engine.
    Pairs Binance Spot (Long physical coin) with Hyperliquid Perpetual (Short contract).
    Harvests continuous 1-hour funding cashflows with physical spot delivery anchor.
    Features:
    - Realistic 48h payback ceiling & Contango basis surplus offset.
    - Tier-1/2/3 multi-tier asset matrix & dynamic order book depth sizing.
    - Hourly micro-blackout window (:58 ~ :01) protecting against funding settlement volatility.
    - Breakeven trailing stop & scaling-out tranche take profit.
    - Single-leg (Perp Short) liquidation cushion defense & toxic token quarantine.
    """

    _logger: Optional[logging.Logger] = None

    @classmethod
    def logger(cls) -> logging.Logger:
        if cls._logger is None:
            cls._logger = logging.getLogger("hummingbot.strategy.spot_perp_cash_carry")
        return cls._logger

    @classmethod
    def init_markets(cls, config: SpotPerpCashCarryConfig):
        """Register market connectors and trading pairs for Spot and Perp."""
        cls._enable_dry_run_connectors()
        spot_conn = config.spot_connector
        perp_conn = config.perp_connector
        cls.markets = {spot_conn: set(), perp_conn: set()}

        tokens_list = config.tokens
        if isinstance(tokens_list, str):
            tokens_list = [val.strip().upper() for val in tokens_list.split(",") if val.strip()]

        for tok in tokens_list:
            cls.markets[spot_conn].add(f"{tok}-USDT")
            cls.markets[perp_conn].add(f"{tok}-USD")

    @classmethod
    def _enable_dry_run_connectors(cls):
        """Allow connectors to initialize for public market orderbook streaming in dry-run mode."""
        try:
            from hummingbot.client.settings import AllConnectorSettings
            from hummingbot.core.connector_manager import ConnectorManager, get_connector_class
            from hummingbot.core.rate_oracle.rate_oracle import RateOracle

            if getattr(ConnectorManager, "_dry_run_spot_perp_patched", False):
                return

            orig_create = ConnectorManager.create_connector

            def patched_create(self, connector_name: str, trading_pairs=None, trading_required=True, api_keys=None):
                if not api_keys:
                    from hummingbot.client.config.security import Security
                    keys = Security.api_keys(connector_name)
                    if not keys:
                        conn_setting = AllConnectorSettings.get_connector_settings().get(connector_name)
                        if conn_setting:
                            connector_class = get_connector_class(connector_name)
                            dummy_keys = {}
                            if connector_name == "binance":
                                dummy_keys = {"binance_api_key": "dummy_key", "binance_api_secret": "dummy_secret"}
                            elif hasattr(conn_setting, "config_keys") and conn_setting.config_keys:
                                if isinstance(conn_setting.config_keys, dict):
                                    dummy_keys = {k: "dummy" for k in conn_setting.config_keys.keys()}

                            init_params = conn_setting.conn_init_parameters(
                                trading_pairs=trading_pairs,
                                trading_required=False,
                                api_keys=dummy_keys,
                                balance_asset_limit=getattr(self.client_config_map.hb_config, "balance_asset_limit", None),
                                rate_limits_share_pct=getattr(self.client_config_map.hb_config, "rate_limits_share_pct", Decimal("100")),
                                gateway_config=getattr(self.client_config_map.hb_config, "gateway", None),
                            )
                            init_params["trading_required"] = False
                            connector = connector_class(**init_params)
                            self.connectors[connector_name] = connector
                            try:
                                RateOracle.get_instance().register_connector(connector)
                            except Exception:
                                pass
                            cls.logger().info(f"[DRY-RUN] Initialized public market connector for '{connector_name}'.")
                            return connector
                return orig_create(self, connector_name, trading_pairs, trading_required, api_keys)

            ConnectorManager.create_connector = patched_create
            ConnectorManager._dry_run_spot_perp_patched = True
        except Exception as e:
            cls.logger().warning(f"Failed to patch ConnectorManager for dry-run: {e}")

    def __init__(self, connectors: Dict[str, ConnectorBase], config: SpotPerpCashCarryConfig):
        super().__init__(connectors, config)
        self.config = config
        self.active_positions: Dict[str, Dict] = {}
        self.closed_trades: Dict[str, List[Dict]] = {}
        self.entry_candidate_ticks: Dict[str, int] = {}
        self.exit_candidate_ticks: Dict[str, int] = {}
        self.token_last_closed_ts: Dict[str, float] = {}
        self.token_quarantine_until: Dict[str, float] = {}
        self.token_admitted_at: Dict[str, float] = {tok: time.time() for tok in self.config.tokens}

        # Dynamic Universe Rotation & Hot-Swap
        self.market_candidates: List[Dict] = []
        self.rotation_history: List[Dict] = []
        self._universe_lock = asyncio.Lock()
        self._rotation_in_progress = False
        self._last_rotation_check_ts = 0.0
        self._last_candidates_scan_ts = 0.0
        self._last_telemetry_log_ts = 0.0

        # Set leverage and position mode on Perp connector for live trading
        if not self.config.dry_run:
            perp_conn = self.connectors.get(self.config.perp_connector)
            if perp_conn and hasattr(perp_conn, "set_leverage"):
                for tok in self.config.tokens:
                    tp = f"{tok}-USD"
                    try:
                        perp_conn.set_leverage(tp, self.config.perp_leverage)
                        if hasattr(perp_conn, "set_position_mode"):
                            perp_conn.set_position_mode(PositionMode.ONEWAY)
                    except Exception as e:
                        self.logger().warning(f"Could not set leverage for {tp}: {e}")

        # Persistence candidates paths
        self.data_file_candidates = [
            os.path.abspath(f"data/spot_perp_cash_carry_trades.json"),
            os.path.abspath(f"../data/spot_perp_cash_carry_trades.json"),
            os.path.abspath(f"/home/hummingbot/data/spot_perp_cash_carry_trades.json"),
            os.path.abspath(f"/home/neo/projects/hummingbot/hummingbot-api/bots/data/spot_perp_cash_carry_trades.json"),
        ]
        self._load_persisted_state()
        self._persist_state()

    # =========================================================================
    # Strategy Main Loop Hooks
    # =========================================================================

    def on_tick(self):
        """Execute tick loop every 1.0s."""
        t0 = time.perf_counter()
        super().on_tick()

        if not self.ready_to_trade:
            return

        # 1. Discrete funding settlement accounting on Hyperliquid (1h cycle)
        self.account_for_discrete_funding_settlements()

        # 2. Dynamic universe background scan & rotation
        self.check_universe_rotation()

        # 3. Evaluate entry proposals (Long Spot + Short Perp)
        self.create_actions_proposal()

        # 4. Evaluate exit proposals (Sell Spot + Buy to Close Perp Short)
        self.stop_actions_proposal()

        # 5. Publish real-time status and telemetry
        self.publish_performance_report()

        duration_ms = (time.perf_counter() - t0) * 1000.0
        self._last_tick_latency_ms = duration_ms

    def get_trading_pair_for_connector(self, token: str, connector_name: str) -> str:
        """Return standardized trading pair name for Spot vs Perp."""
        if "hyperliquid" in connector_name:
            return f"{token}-USD"
        return f"{token}-USDT"

    # =========================================================================
    # Multi-Tier Asset Classification & Risk Controls
    # =========================================================================

    def get_token_tier(self, token: str) -> int:
        """Classify token into Tier 1 (Mega), Tier 2 (Mid/Large), or Tier 3 (Alt/Meme)."""
        tok_up = token.upper()
        if tok_up in self.config.tier_1_tokens:
            return 1
        if tok_up in self.config.tier_2_tokens:
            return 2
        return 3

    def get_tier_config(self, token: str) -> Tuple[Decimal, Decimal, Decimal]:
        """Returns (max_quote_size, min_apr, liquidation_buffer) for token's tier."""
        tier = self.get_token_tier(token)
        if tier == 1:
            return self.config.tier_1_max_quote, self.config.tier_1_min_apr, self.config.tier_1_liquidation_buffer
        if tier == 2:
            return self.config.tier_2_max_quote, self.config.tier_2_min_apr, self.config.tier_2_liquidation_buffer
        return self.config.tier_3_max_quote, self.config.tier_3_min_apr, self.config.tier_3_liquidation_buffer

    def is_in_hourly_blackout(self) -> bool:
        """Returns True if current time falls within hourly funding payment blackout window (:58 ~ :01)."""
        if not self.config.hourly_blackout_enabled:
            return False
        dt = datetime.fromtimestamp(self.current_timestamp, tz=timezone.utc)
        minute = dt.minute
        start_m = self.config.hourly_blackout_start_minute
        end_m = self.config.hourly_blackout_end_minute
        if start_m <= end_m:
            return start_m <= minute <= end_m
        return minute >= start_m or minute <= end_m

    def is_token_quarantined(self, token: str) -> bool:
        """Check if toxic token is currently cooling down in quarantine."""
        until = self.token_quarantine_until.get(token, 0.0)
        return self.current_timestamp < until

    def quarantine_token(self, token: str, hours: float, reason: str):
        """Put toxic token into quarantine cooldown."""
        self.token_quarantine_until[token] = self.current_timestamp + (hours * 3600.0)
        self.logger().warning(
            f"🚫 [QUARANTINE] Token '{token}' quarantined for {hours:.1f}h! Reason: {reason}"
        )

    def calculate_position_size(self, token: str, depth_spot: Decimal, depth_perp: Decimal) -> Decimal:
        """Computes dynamic position sizing based on tier limits and safe order book depth."""
        tier_max, _, _ = self.get_tier_config(token)
        base_size = min(self.config.position_size_quote, tier_max)
        if not self.config.dynamic_position_sizing:
            return base_size

        min_depth = min(depth_spot, depth_perp)
        safe_capacity = min_depth * self.config.max_orderbook_depth_ratio
        target = min(tier_max, safe_capacity)
        return max(self.config.min_position_size_quote, target)

    def calculate_friction_and_payback(
        self, p_spot_exec: Decimal, p_perp_exec: Decimal,
        p_spot_slip: Optional[Decimal], p_perp_slip: Optional[Decimal],
        rate_1h: Decimal
    ) -> Tuple[Decimal, Decimal, Decimal, bool]:
        """
        Calculates:
        - basis_spread: (p_perp - p_spot) / p_spot
        - total_friction: spot fees + perp fees + latency + slippage
        - payback_hours: hours of funding to recover friction
        - is_acceptable: True if meets either payback_hours <= max_payback_hours OR friction-to-yield ratio <= max_friction_to_yield_ratio
        """
        basis_spread = (p_perp_exec - p_spot_exec) / p_spot_exec
        roundtrip_fees = (self.config.spot_taker_fee_rate * Decimal("2.0")) + (self.config.perp_taker_fee_rate * Decimal("2.0"))
        roundtrip_latency = self.config.simulated_latency_slippage_pct * Decimal("4.0")
        est_slippage = (p_spot_slip or Decimal("0")) + (p_perp_slip or Decimal("0"))
        total_friction = roundtrip_fees + roundtrip_latency + (est_slippage * Decimal("2.0"))

        # Net hurdle: If Perp > Spot (Contango), basis surplus directly offsets friction!
        net_hurdle = total_friction - basis_spread

        if net_hurdle <= Decimal("0"):
            # Basis surplus alone fully pays for all roundtrip friction!
            payback_hours = Decimal("0.0")
            is_acceptable = True
        elif rate_1h > Decimal("0"):
            payback_hours = net_hurdle / rate_1h
            expected_funding = rate_1h * self.config.target_horizon_hours
            friction_ratio = net_hurdle / expected_funding if expected_funding > Decimal("0") else Decimal("999.0")
            is_acceptable = (payback_hours <= self.config.max_payback_hours) or (friction_ratio <= self.config.max_friction_to_yield_ratio)
        else:
            payback_hours = Decimal("999.0")
            is_acceptable = False

        return basis_spread, total_friction, payback_hours, is_acceptable

    # =========================================================================
    # Discrete Funding Settlement Engine (Hyperliquid 1h Ledger)
    # =========================================================================

    def account_for_discrete_funding_settlements(self):
        """
        Record actual funding cashflows for active positions.
        Hyperliquid settles funding every 1 hour (3600s).
        Since we are SHORT on Hyperliquid:
        Positive funding rate = WE RECEIVE CASH (+).
        Negative funding rate = WE PAY CASH (-).
        """
        now = self.current_timestamp
        for token, pos in list(self.active_positions.items()):
            if not pos.get("is_virtual", True):
                continue

            next_f = pos.get("next_funding_time", 0)
            if now >= next_f and next_f > 0:
                perp_conn_name = pos["perp_connector"]
                tp = self.get_trading_pair_for_connector(token, perp_conn_name)
                f_info = self.connectors[perp_conn_name].get_funding_info(tp)

                raw_rate = Decimal(str(f_info.rate)) if f_info else Decimal("0")
                if abs(raw_rate) > Decimal("0.5"):
                    raw_rate = Decimal("0.0001")

                notional = Decimal(str(pos["position_size"]))
                # Short receives when rate > 0
                cashflow = notional * raw_rate

                pos["settled_funding_pnl"] = float(Decimal(str(pos.get("settled_funding_pnl", 0.0))) + cashflow)
                pos.setdefault("funding_payments", []).append({
                    "timestamp": int(now),
                    "connector": perp_conn_name,
                    "rate": float(raw_rate),
                    "notional": float(notional),
                    "amount": float(cashflow),
                })

                # Schedule next 1-hour settlement
                pos["next_funding_time"] = int(now + 3600)
                self.logger().info(
                    f"💳 [FUNDING-PAYMENT-SETTLED] Token '{token}' on {perp_conn_name}: "
                    f"Rate {float(raw_rate):+.4%} ➔ Received ${float(cashflow):+,.4f} USDC. "
                    f"Cumulative Funding: ${pos['settled_funding_pnl']:+,.4f}"
                )
                self._persist_state()

    # =========================================================================
    # Entry Proposals: Long Spot + Short Perp
    # =========================================================================

    def create_actions_proposal(self) -> List:
        """Evaluate watchlist candidates and propose Spot-Perp Cash & Carry entries."""
        if len(self.active_positions) >= self.config.max_concurrent_tokens:
            return []

        # 1. Hourly Blackout Window Check (:58 ~ :01)
        if self.is_in_hourly_blackout():
            return []

        c_spot = self.config.spot_connector
        c_perp = self.config.perp_connector
        conn_spot = self.connectors.get(c_spot)
        conn_perp = self.connectors.get(c_perp)
        if not conn_spot or not conn_perp:
            return []

        # Available slot budget check
        available_slots = self.config.max_concurrent_tokens - len(self.active_positions)
        if available_slots <= 0:
            return []

        for token in self.config.tokens:
            if len(self.active_positions) >= self.config.max_concurrent_tokens:
                break
            if token in self.active_positions:
                continue

            # Anti-churn cooldown & Quarantine checks
            last_close = self.token_last_closed_ts.get(token, 0.0)
            if self.current_timestamp - last_close < 300.0:
                continue
            if self.is_token_quarantined(token):
                continue

            tp_spot = self.get_trading_pair_for_connector(token, c_spot)
            tp_perp = self.get_trading_pair_for_connector(token, c_perp)

            if not conn_spot.order_book_tracker.ready or not conn_perp.order_book_tracker.ready:
                continue

            # 2. Query Hyperliquid Perpetual funding info & calculate annualized APR
            f_info = conn_perp.get_funding_info(tp_perp)
            if not f_info:
                continue

            rate_1h = Decimal(str(f_info.rate))
            if abs(rate_1h) > Decimal("0.5"):
                rate_1h = Decimal("0.0001")

            daily_rate = rate_1h * Decimal("24")
            annual_apr = daily_rate * Decimal("365")

            # Entry threshold: must meet tier-specific min_apr
            tier_max_quote, tier_min_apr, tier_liq_buffer = self.get_tier_config(token)
            eff_min_apr = max(tier_min_apr, self.config.min_entry_annualized_apr)
            if annual_apr < eff_min_apr:
                self.entry_candidate_ticks[token] = 0
                continue

            # 3. Liquidity Depth Gate & Dynamic Sizing
            depth_spot = self.get_top_depth_quote(c_spot, tp_spot, is_buy=True, levels=25)
            depth_perp = self.get_top_depth_quote(c_perp, tp_perp, is_buy=False, levels=25)
            if depth_spot < self.config.min_order_book_depth_quote or depth_perp < self.config.min_order_book_depth_quote:
                self.entry_candidate_ticks[token] = 0
                continue

            pos_size = self.calculate_position_size(token, depth_spot, depth_perp)

            # Pre-flight virtual margin verification
            spot_req = pos_size * (Decimal("1.0") + self.config.spot_taker_fee_rate)
            perp_req = (pos_size / Decimal(str(self.config.perp_leverage))) * Decimal("1.20")
            if self.config.dry_run:
                used_spot = sum(Decimal(str(p.get("position_size", 0.0))) for p in self.active_positions.values())
                used_perp = sum(Decimal(str(p.get("perp_margin", 0.0))) for p in self.active_positions.values())
                avail_spot = self.config.dry_run_virtual_spot_quote - used_spot
                avail_perp = self.config.dry_run_virtual_perp_quote - used_perp
                if avail_spot < spot_req or avail_perp < perp_req:
                    continue

            # 4. Query execution orderbooks with dynamically calculated size
            p_spot_best, p_spot_vwap, p_spot_exec, p_spot_slip = self.get_orderbook_execution_metrics(
                c_spot, tp_spot, is_buy=True, quote_volume=pos_size
            )
            p_perp_best, p_perp_vwap, p_perp_exec, p_perp_slip = self.get_orderbook_execution_metrics(
                c_perp, tp_perp, is_buy=False, quote_volume=pos_size
            )

            if p_spot_exec is None or p_perp_exec is None:
                self.entry_candidate_ticks[token] = 0
                continue

            # 5. Entry Basis & Payback Period Barrier
            basis_spread, total_friction, payback_hours, is_payback_ok = self.calculate_friction_and_payback(
                p_spot_exec, p_perp_exec, p_spot_slip, p_perp_slip, rate_1h
            )

            if not is_payback_ok:
                self.entry_candidate_ticks[token] = 0
                continue

            # 6. Persistence confirmation ticks (prevent momentary spikes)
            self.entry_candidate_ticks[token] = self.entry_candidate_ticks.get(token, 0) + 1
            if self.entry_candidate_ticks[token] < self.config.entry_persistence_ticks:
                self.logger().info(
                    f"⚡ [SPOT-PERP-PULSE] Token '{token}' (Tier {self.get_token_tier(token)}) APR {float(annual_apr):+.1%} "
                    f"({self.entry_candidate_ticks[token]}/{self.config.entry_persistence_ticks} ticks). "
                    f"Spot Ask ${float(p_spot_exec):.4f} / Perp Bid ${float(p_perp_exec):.4f} "
                    f"(Basis {float(basis_spread):+.2%}, Payback: {float(payback_hours):.1f}h, Size: ${float(pos_size):,.0f})."
                )
                continue

            # 7. Execute Entry
            coin_amount = pos_size / p_spot_exec
            perp_margin = pos_size / Decimal(str(self.config.perp_leverage))
            total_capital = pos_size + perp_margin
            est_liq_price = p_perp_exec * (Decimal("1.0") + (Decimal("1.0") / Decimal(str(self.config.perp_leverage))) - Decimal("0.03"))

            pos_data = {
                "token": token,
                "tier": self.get_token_tier(token),
                "is_virtual": self.config.dry_run,
                "spot_connector": c_spot,
                "perp_connector": c_perp,
                "spot_trading_pair": tp_spot,
                "perp_trading_pair": tp_perp,
                "amount": float(coin_amount),
                "position_size": float(pos_size),
                "perp_leverage": self.config.perp_leverage,
                "spot_entry_price": float(p_spot_exec),
                "perp_entry_price": float(p_perp_exec),
                "entry_basis_pct": float(basis_spread),
                "entry_apr": float(annual_apr),
                "spot_margin": float(pos_size),
                "perp_margin": float(perp_margin),
                "total_margin": float(total_capital),
                "estimated_liq_price": float(est_liq_price),
                "tier_liq_buffer": float(tier_liq_buffer),
                "open_timestamp": self.current_timestamp,
                "settled_funding_pnl": 0.0,
                "funding_payments": [],
                "next_funding_time": int(self.current_timestamp + 3600),
                "peak_roe_pct": 0.0,
                "breakeven_active": False,
                "partial_tp_done": False,
            }

            self.active_positions[token] = pos_data
            self.entry_candidate_ticks[token] = 0
            self.logger().info(
                f"🚀 [SPOT-PERP-ENTRY-TRIGGER] Token '{token}' (Tier {pos_data['tier']}) Cash & Carry Activated!\n"
                f"  Leg 1 (Spot {c_spot}): BUY {float(coin_amount):,.4f} {token} @ ${float(p_spot_exec):.4f} (Notional ${float(pos_size):,.2f})\n"
                f"  Leg 2 (Perp {c_perp}): SHORT {float(coin_amount):,.4f} {token} @ ${float(p_perp_exec):.4f} (Margin ${float(perp_margin):,.2f}, {self.config.perp_leverage}x)\n"
                f"  Funding Yield: Annualized APR {float(annual_apr):+.1%} (Daily {float(daily_rate):+.2%})\n"
                f"  Friction & Payback: Payback {float(payback_hours):.1f}h (Basis: {float(basis_spread):+.2%}, Liq Price: ${float(est_liq_price):.4f}, Buffer: {float(tier_liq_buffer):.1%})"
            )
            self._persist_state()

        return []

    # =========================================================================
    # Exit Proposals: Sell Spot + Buy to Close Perp Short
    # =========================================================================

    def stop_actions_proposal(self) -> List:
        """Evaluate active spot-perp positions and close when exit conditions met."""
        tokens_to_close = []
        c_spot = self.config.spot_connector
        c_perp = self.config.perp_connector
        conn_spot = self.connectors.get(c_spot)
        conn_perp = self.connectors.get(c_perp)

        for token, pos in list(self.active_positions.items()):
            tp_spot = pos["spot_trading_pair"]
            tp_perp = pos["perp_trading_pair"]
            coin_amt = Decimal(str(pos["amount"]))
            pos_size = Decimal(str(pos["position_size"]))
            duration_s = self.current_timestamp - pos["open_timestamp"]
            duration_h = duration_s / 3600.0

            # Query executable exit prices:
            # Spot Leg: SELL at Bid
            # Perp Leg: BUY at Ask
            p_spot_best, p_spot_vwap, p_spot_exec, p_spot_slip = self.get_orderbook_execution_metrics(
                c_spot, tp_spot, is_buy=False, base_amount=coin_amt
            )
            p_perp_best, p_perp_vwap, p_perp_exec, p_perp_slip = self.get_orderbook_execution_metrics(
                c_perp, tp_perp, is_buy=True, base_amount=coin_amt
            )

            if p_spot_exec is None or p_perp_exec is None:
                continue

            # Query live funding APR
            f_info = conn_perp.get_funding_info(tp_perp) if conn_perp else None
            rate_1h = Decimal(str(f_info.rate)) if f_info else Decimal("0")
            if abs(rate_1h) > Decimal("0.5"):
                rate_1h = Decimal("0.0001")
            curr_apr = rate_1h * Decimal("24") * Decimal("365")

            # Financial PnL attribution
            p_spot_entry = Decimal(str(pos["spot_entry_price"]))
            p_perp_entry = Decimal(str(pos["perp_entry_price"]))

            spot_pnl = (p_spot_exec - p_spot_entry) * coin_amt
            perp_pnl = (p_perp_entry - p_perp_exec) * coin_amt
            trading_pnl = spot_pnl + perp_pnl

            # Roundtrip fees and slippage
            spot_notional_open = coin_amt * p_spot_entry
            spot_notional_close = coin_amt * p_spot_exec
            perp_notional_open = coin_amt * p_perp_entry
            perp_notional_close = coin_amt * p_perp_exec

            spot_fee = (spot_notional_open + spot_notional_close) * self.config.spot_taker_fee_rate
            perp_fee = (perp_notional_open + perp_notional_close) * self.config.perp_taker_fee_rate
            total_fees = spot_fee + perp_fee
            total_slippage = (p_spot_slip or Decimal("0")) * coin_amt + (p_perp_slip or Decimal("0")) * coin_amt
            total_friction = total_fees + total_slippage

            net_trading_pnl = trading_pnl - total_friction
            settled_funding = Decimal(str(pos.get("settled_funding_pnl", 0.0)))
            total_net_pnl = net_trading_pnl + settled_funding

            total_capital = Decimal(str(pos.get("total_margin", pos_size * Decimal("1.333"))))
            roe_pct = total_net_pnl / total_capital if total_capital > Decimal("0") else Decimal("0")

            # Liquidation safety cushion on Hyperliquid (Short leg only)
            est_liq = Decimal(str(pos.get("estimated_liq_price", p_perp_entry * Decimal("1.30"))))
            safety_cushion_pct = (est_liq - p_perp_exec) / p_perp_exec if p_perp_exec > Decimal("0") else Decimal("0")
            tier_liq_buf = Decimal(str(pos.get("tier_liq_buffer", self.config.liquidation_buffer_pct)))

            # Breakeven Trailing Update
            peak_roe = max(Decimal(str(pos.get("peak_roe_pct", 0.0))), roe_pct)
            pos["peak_roe_pct"] = float(peak_roe)
            if self.config.breakeven_trailing_enabled:
                if roe_pct >= self.config.breakeven_trailing_activation_pct:
                    pos["breakeven_active"] = True

            # Update live position in-place
            pos["current_spot_price"] = float(p_spot_exec)
            pos["current_perp_price"] = float(p_perp_exec)
            pos["current_apr"] = float(curr_apr)
            pos["current_safety_cushion_pct"] = float(safety_cushion_pct)
            pos["net_trading_pnl"] = float(net_trading_pnl)
            pos["total_net_pnl"] = float(total_net_pnl)
            pos["roe_pct"] = float(roe_pct)
            pos["duration_seconds"] = int(duration_s)

            # ----------------- Scaling-Out Take Profit Check -----------------
            if (self.config.scaling_tp_enabled and
                not pos.get("partial_tp_done", False) and
                roe_pct >= self.config.partial_tp_threshold_pct and
                duration_s >= self.config.min_holding_seconds):

                ratio = self.config.partial_tp_ratio
                closed_coin_amt = coin_amt * ratio
                rem_coin_amt = coin_amt - closed_coin_amt

                part_spot_pnl = (p_spot_exec - p_spot_entry) * closed_coin_amt
                part_perp_pnl = (p_perp_entry - p_perp_exec) * closed_coin_amt
                part_trading_pnl = part_spot_pnl + part_perp_pnl
                part_friction = total_friction * ratio
                part_settled_funding = settled_funding * ratio
                part_net_pnl = part_trading_pnl - part_friction + part_settled_funding
                part_capital = total_capital * ratio

                self.logger().info(
                    f"🎯 [SPOT-PERP-PARTIAL-TP] Token '{token}' locked {float(ratio):.0%} tranche profit!\n"
                    f"  Partial Net PnL: ${float(part_net_pnl):+,.2f} USD (ROE {float(roe_pct):+.2%})\n"
                    f"  Remaining Position: {float(rem_coin_amt):,.4f} {token} (${float(pos_size * (Decimal('1') - ratio)):,.2f}) continues harvesting."
                )

                record = {
                    "token": token,
                    "tier": pos.get("tier", self.get_token_tier(token)),
                    "spot_connector": pos["spot_connector"],
                    "perp_connector": pos["perp_connector"],
                    "amount": float(closed_coin_amt),
                    "position_size": float(pos_size * ratio),
                    "perp_leverage": pos["perp_leverage"],
                    "spot_entry_price": pos["spot_entry_price"],
                    "perp_entry_price": pos["perp_entry_price"],
                    "spot_close_price": float(p_spot_exec),
                    "perp_close_price": float(p_perp_exec),
                    "entry_basis_pct": pos["entry_basis_pct"],
                    "close_basis_pct": float((p_perp_exec - p_spot_exec) / p_spot_exec),
                    "entry_apr": pos["entry_apr"],
                    "spot_margin": float(pos["spot_margin"] * float(ratio)),
                    "perp_margin": float(pos["perp_margin"] * float(ratio)),
                    "total_margin": float(part_capital),
                    "spot_pnl": float(part_spot_pnl),
                    "perp_pnl": float(part_perp_pnl),
                    "trading_pnl": float(part_trading_pnl),
                    "total_friction_cost": float(part_friction),
                    "net_trading_pnl": float(part_trading_pnl - part_friction),
                    "settled_funding_pnl": float(part_settled_funding),
                    "funding_payments": pos.get("funding_payments", []),
                    "settlement_count": len(pos.get("funding_payments", [])),
                    "total_net_pnl": float(part_net_pnl),
                    "roe_pct": float(roe_pct),
                    "close_reason": f"Partial Scaling Take-Profit ({float(ratio):.0%})",
                    "open_timestamp": pos["open_timestamp"],
                    "close_timestamp": self.current_timestamp,
                    "duration_seconds": int(duration_s),
                }
                if token not in self.closed_trades:
                    self.closed_trades[token] = []
                self.closed_trades[token].append(record)

                pos["amount"] = float(rem_coin_amt)
                pos["position_size"] = float(pos_size * (Decimal("1") - ratio))
                pos["spot_margin"] = float(pos["spot_margin"] * float(Decimal("1") - ratio))
                pos["perp_margin"] = float(pos["perp_margin"] * float(Decimal("1") - ratio))
                pos["total_margin"] = float(total_capital * (Decimal("1") - ratio))
                pos["settled_funding_pnl"] = float(settled_funding * (Decimal("1") - ratio))
                pos["partial_tp_done"] = True
                self._persist_state()
                continue

            # ----------------- Exit Conditions -----------------
            # 1. Rate Inversion Stop (funding turned negative)
            is_inverted = curr_apr <= self.config.inversion_exit_apr

            # 2. Funding Yield Decay Exit (APR dropped below exit threshold & min holding passed)
            is_decayed = (curr_apr < self.config.exit_annualized_apr) and (duration_s >= self.config.min_holding_seconds)

            # 3. Emergency Liquidation Cushion Breach (Perp short leg)
            is_liq_danger = safety_cushion_pct <= tier_liq_buf

            # 4. Breakeven Trailing Stop Loss
            is_breakeven_stop = (
                pos.get("breakeven_active", False) and
                roe_pct < self.config.breakeven_trailing_stop_pct and
                duration_s >= self.config.min_holding_seconds
            )

            # 5. Max Duration Timeout Expiry
            is_timeout = (duration_h >= self.config.max_hold_hours) and (total_net_pnl >= Decimal("0"))

            should_exit = is_inverted or is_decayed or is_liq_danger or is_breakeven_stop or is_timeout
            if should_exit:
                self.exit_candidate_ticks[token] = self.exit_candidate_ticks.get(token, 0) + 1
            else:
                self.exit_candidate_ticks[token] = 0

            if self.exit_candidate_ticks.get(token, 0) >= self.config.exit_persistence_ticks:
                if is_liq_danger:
                    reason = f"Liquidation Defense Exit (Cushion {float(safety_cushion_pct):.1%} <= {float(tier_liq_buf):.1%})"
                elif is_breakeven_stop:
                    reason = f"Breakeven Trailing Stop Exit (ROE {float(roe_pct):+.2%} < {float(self.config.breakeven_trailing_stop_pct):+.2%})"
                elif is_inverted:
                    reason = f"Funding Rate Inversion Exit (APR {float(curr_apr):+.1%} <= 0%)"
                elif is_decayed:
                    reason = f"Funding Yield Decay Exit (APR {float(curr_apr):.1%} < {float(self.config.exit_annualized_apr):.1%})"
                elif is_timeout:
                    reason = f"Max Duration Timeout Exit ({duration_h:.1f}h >= {self.config.max_hold_hours:.1f}h)"
                else:
                    reason = "Close Signal"

                tokens_to_close.append({
                    "token": token,
                    "reason": reason,
                    "is_liq_danger": is_liq_danger,
                    "spot_close_price": float(p_spot_exec),
                    "perp_close_price": float(p_perp_exec),
                    "spot_pnl": float(spot_pnl),
                    "perp_pnl": float(perp_pnl),
                    "trading_pnl": float(trading_pnl),
                    "total_friction_cost": float(total_friction),
                    "net_trading_pnl": float(net_trading_pnl),
                    "settled_funding_pnl": float(settled_funding),
                    "total_net_pnl": float(total_net_pnl),
                    "roe_pct": float(roe_pct),
                    "duration_seconds": int(duration_s),
                })

        for item in tokens_to_close:
            token = item["token"]
            pos = self.active_positions.pop(token)
            self.token_last_closed_ts[token] = self.current_timestamp

            # Quarantine toxic token if liquidated or heavy loss
            if item["is_liq_danger"] or item["total_net_pnl"] < -float(self.config.single_loss_quarantine_threshold):
                self.quarantine_token(token, self.config.token_quarantine_hours, item["reason"])

            record = {
                "token": token,
                "tier": pos.get("tier", self.get_token_tier(token)),
                "spot_connector": pos["spot_connector"],
                "perp_connector": pos["perp_connector"],
                "amount": pos["amount"],
                "position_size": pos["position_size"],
                "perp_leverage": pos["perp_leverage"],
                "spot_entry_price": pos["spot_entry_price"],
                "perp_entry_price": pos["perp_entry_price"],
                "spot_close_price": item["spot_close_price"],
                "perp_close_price": item["perp_close_price"],
                "entry_basis_pct": pos["entry_basis_pct"],
                "close_basis_pct": (item["perp_close_price"] - item["spot_close_price"]) / item["spot_close_price"],
                "entry_apr": pos["entry_apr"],
                "spot_margin": pos["spot_margin"],
                "perp_margin": pos["perp_margin"],
                "total_margin": pos["total_margin"],
                "spot_pnl": item["spot_pnl"],
                "perp_pnl": item["perp_pnl"],
                "trading_pnl": item["trading_pnl"],
                "total_friction_cost": item["total_friction_cost"],
                "net_trading_pnl": item["net_trading_pnl"],
                "settled_funding_pnl": item["settled_funding_pnl"],
                "funding_payments": pos.get("funding_payments", []),
                "settlement_count": len(pos.get("funding_payments", [])),
                "total_net_pnl": item["total_net_pnl"],
                "roe_pct": item["roe_pct"],
                "close_reason": item["reason"],
                "open_timestamp": pos["open_timestamp"],
                "close_timestamp": self.current_timestamp,
                "duration_seconds": item["duration_seconds"],
            }

            if token not in self.closed_trades:
                self.closed_trades[token] = []
            self.closed_trades[token].append(record)

            self.logger().info(
                f"🎯 [SPOT-PERP-CLOSED] Token '{token}' ({item['reason']})!\n"
                f"  Financial Outcome: Net PnL ${item['total_net_pnl']:+,.2f} USD (ROE {item['roe_pct']:+.2%})\n"
                f"  Funding Harvested: ${item['settled_funding_pnl']:+,.2f} USD across {record['settlement_count']} hourly cycles\n"
                f"  Price Attribution: Spot PnL ${item['spot_pnl']:+,.2f} + Perp PnL ${item['perp_pnl']:+,.2f} - Friction ${item['total_friction_cost']:,.2f} = NetTrade ${item['net_trading_pnl']:+,.2f}\n"
                f"  Holding Time: {item['duration_seconds']/3600.0:.2f} hours"
            )

        if tokens_to_close:
            self._persist_state()

        return []

    # =========================================================================
    # Universe Scanner & Dynamic Hot-Swap
    # =========================================================================

    def check_universe_rotation(self):
        """Periodically scan candidate pool and evaluate hot-swap rotation."""
        if not self.config.auto_rotation_enabled:
            return

        now = self.current_timestamp
        if now - self._last_candidates_scan_ts < 15.0:
            return
        self._last_candidates_scan_ts = now

        c_spot = self.config.spot_connector
        c_perp = self.config.perp_connector
        conn_spot = self.connectors.get(c_spot)
        conn_perp = self.connectors.get(c_perp)
        if not conn_spot or not conn_perp:
            return

        candidates = []
        is_blackout = self.is_in_hourly_blackout()

        for tok in self.config.tokens:
            tp_spot = self.get_trading_pair_for_connector(tok, c_spot)
            tp_perp = self.get_trading_pair_for_connector(tok, c_perp)

            try:
                f_info = conn_perp.get_funding_info(tp_perp)
                rate_1h = Decimal(str(f_info.rate)) if f_info else Decimal("0")
                if abs(rate_1h) > Decimal("0.5"):
                    rate_1h = Decimal("0.0001")
                annual_apr = rate_1h * Decimal("24") * Decimal("365")

                p_spot = self.get_price_from_orderbook(c_spot, tp_spot, is_buy=True)
                p_perp = self.get_price_from_orderbook(c_perp, tp_perp, is_buy=False)
                basis = (p_perp - p_spot) / p_spot if p_spot and p_perp else Decimal("0")

                depth_spot = self.get_top_depth_quote(c_spot, tp_spot, is_buy=True, levels=25)
                depth_perp = self.get_top_depth_quote(c_perp, tp_perp, is_buy=False, levels=25)

                tier = self.get_token_tier(tok)
                tier_max_quote, tier_min_apr, tier_liq_buf = self.get_tier_config(tok)
                safe_size = self.calculate_position_size(tok, depth_spot, depth_perp)

                if p_spot and p_perp:
                    basis_spread, total_friction, payback_h, is_payback_ok = self.calculate_friction_and_payback(
                        p_spot, p_perp, None, None, rate_1h
                    )
                else:
                    basis_spread, total_friction, payback_h, is_payback_ok = basis, Decimal("0.0035"), Decimal("999.0"), False

                is_active = tok in self.active_positions
                is_quarantined = self.is_token_quarantined(tok)

                if is_active:
                    status = "In Position"
                elif is_quarantined:
                    status = "Quarantined"
                elif is_blackout:
                    status = "Hourly Blackout"
                elif is_payback_ok and annual_apr >= max(tier_min_apr, self.config.min_entry_annualized_apr):
                    status = "Eligible"
                else:
                    status = "Standby"

                candidates.append({
                    "token": tok,
                    "tier": tier,
                    "annual_apr": float(annual_apr),
                    "hourly_rate": float(rate_1h),
                    "spot_price": float(p_spot) if p_spot else 0.0,
                    "perp_price": float(p_perp) if p_perp else 0.0,
                    "basis_pct": float(basis),
                    "payback_hours": float(payback_h),
                    "safe_capacity": float(safe_size),
                    "depth_spot": float(depth_spot),
                    "depth_perp": float(depth_perp),
                    "is_active": is_active,
                    "status": status,
                })
            except Exception as e:
                import traceback
                self.logger().warning(f"Error scanning candidate {tok}: {e}\n{traceback.format_exc()}")

        candidates.sort(key=lambda x: x["annual_apr"], reverse=True)
        self.market_candidates = candidates
        self._persist_state()

        # Evaluate hot-swap if full and rotation interval has elapsed
        rotation_due = (now - self._last_rotation_check_ts >= (self.config.auto_rotation_interval_minutes * 60.0))
        if rotation_due and len(self.active_positions) >= self.config.max_concurrent_tokens:
            self._last_rotation_check_ts = now
            top_standby = next((c for c in candidates if not c["is_active"] and c["status"] == "Eligible"), None)
            if top_standby:
                active_tokens_info = [
                    (tok, pos.get("current_apr", pos.get("entry_apr", 0.0)))
                    for tok, pos in self.active_positions.items()
                ]
                active_tokens_info.sort(key=lambda x: x[1])
                lowest_tok, lowest_apr = active_tokens_info[0]

                if Decimal(str(top_standby["annual_apr"])) - Decimal(str(lowest_apr)) >= self.config.auto_rotation_min_delta_apr:
                    self.logger().info(
                        f"🔄 [HOT-SWAP-ROTATION] Triggering eviction of '{lowest_tok}' (APR {lowest_apr:.1%}) "
                        f"in favor of '{top_standby['token']}' (APR {top_standby['annual_apr']:.1%})!"
                    )
                    self.rotation_history.append({
                        "timestamp": self.current_timestamp,
                        "evicted_token": lowest_tok,
                        "evicted_apr": lowest_apr,
                        "admitted_token": top_standby["token"],
                        "admitted_apr": top_standby["annual_apr"],
                    })
                    self.exit_candidate_ticks[lowest_tok] = 999

    # =========================================================================
    # Helpers & Orderbook Metric Resolvers
    # =========================================================================

    def get_price_from_orderbook(self, connector_name: str, trading_pair: str, is_buy: bool) -> Optional[Decimal]:
        """Safely retrieve top-of-book price directly from orderbook without relying on initialized trading rules."""
        connector = self.connectors.get(connector_name)
        if not connector:
            return None
        try:
            ob = connector.get_order_book(trading_pair)
            if ob:
                p = ob.get_price(is_buy)
                if p is not None and not math.isnan(p) and p > 0:
                    return Decimal(str(p))
        except Exception:
            pass
        try:
            p = connector.get_price(trading_pair, is_buy)
            if p is not None and not math.isnan(float(p)) and float(p) > 0:
                return Decimal(str(p))
        except Exception:
            pass
        return None

    def get_orderbook_execution_metrics(
        self, connector_name: str, trading_pair: str, is_buy: bool,
        quote_volume: Optional[Decimal] = None, base_amount: Optional[Decimal] = None
    ) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
        """Compute top-of-book, VWAP, executed price, and depth slippage."""
        connector = self.connectors.get(connector_name)
        if not connector:
            return None, None, None, None

        ob = connector.get_order_book(trading_pair)
        if not ob:
            return None, None, None, None

        best_price = self.get_price_from_orderbook(connector_name, trading_pair, is_buy)
        if best_price is None or best_price <= Decimal("0"):
            return None, None, None, None

        if base_amount is None and quote_volume is not None:
            base_amount = quote_volume / best_price

        if base_amount is None or base_amount <= Decimal("0"):
            return best_price, best_price, best_price, Decimal("0")

        try:
            cum_base = Decimal("0")
            cum_quote = Decimal("0")
            book_entries = ob.ask_entries() if is_buy else ob.bid_entries()

            for entry in book_entries:
                p = Decimal(str(entry.price))
                amt = Decimal(str(entry.amount))
                needed = base_amount - cum_base
                if needed <= Decimal("0"):
                    break
                fill = min(needed, amt)
                cum_base += fill
                cum_quote += fill * p

            if cum_base <= Decimal("0"):
                return best_price, best_price, best_price, Decimal("0")

            vwap = cum_quote / cum_base
            depth_slip_pct = abs(vwap - best_price) / best_price if best_price > Decimal("0") else Decimal("0")
            exec_price = vwap * (Decimal("1.0") + (self.config.simulated_latency_slippage_pct if is_buy else -self.config.simulated_latency_slippage_pct))
            return best_price, vwap, exec_price, depth_slip_pct
        except Exception:
            return best_price, best_price, best_price, Decimal("0")

    def get_top_depth_quote(self, connector_name: str, trading_pair: str, is_buy: bool, levels: int = 25) -> Decimal:
        """Calculate cumulative quote volume in top N levels (default 25 levels)."""
        connector = self.connectors.get(connector_name)
        if not connector:
            return Decimal("0")
        ob = connector.get_order_book(trading_pair)
        if not ob:
            return Decimal("0")

        total_quote = Decimal("0")
        try:
            entries = ob.ask_entries() if is_buy else ob.bid_entries()
            for i, entry in enumerate(entries):
                if i >= levels:
                    break
                total_quote += Decimal(str(entry.price)) * Decimal(str(entry.amount))
        except Exception:
            pass
        return total_quote

    # =========================================================================
    # Persistence & Reporting
    # =========================================================================

    def _persist_state(self):
        """Save active positions, closed trades, and telemetry to disk."""
        all_closed = []
        for t_list in self.closed_trades.values():
            all_closed.extend(t_list)

        data = {
            "strategy": "spot_perp_cash_carry",
            "is_dry_run": self.config.dry_run,
            "spot_connector": self.config.spot_connector,
            "perp_connector": self.config.perp_connector,
            "perp_leverage": self.config.perp_leverage,
            "position_size": float(self.config.position_size_quote),
            "max_concurrent_tokens": self.config.max_concurrent_tokens,
            "min_entry_annualized_apr": float(self.config.min_entry_annualized_apr),
            "exit_annualized_apr": float(self.config.exit_annualized_apr),
            "max_payback_hours": float(self.config.max_payback_hours),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "active_positions": list(self.active_positions.values()),
            "closed_trades": all_closed,
            "market_candidates": getattr(self, "market_candidates", []),
            "rotation_history": getattr(self, "rotation_history", []),
            "quarantined_tokens": {k: float(v) for k, v in self.token_quarantine_until.items()},
        }

        for path in self.data_file_candidates:
            try:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w") as f:
                    json.dump(data, f, indent=2)
                break
            except Exception:
                pass

    def _load_persisted_state(self):
        """Load state on startup."""
        for path in self.data_file_candidates:
            if os.path.exists(path):
                try:
                    with open(path, "r") as f:
                        data = json.load(f)
                    if data.get("strategy") == "spot_perp_cash_carry":
                        for item in data.get("closed_trades", []):
                            tok = item.get("token")
                            if tok:
                                if tok not in self.closed_trades:
                                    self.closed_trades[tok] = []
                                self.closed_trades[tok].append(item)
                        for pos in data.get("active_positions", []):
                            tok = pos.get("token")
                            if tok and tok not in self.active_positions:
                                self.active_positions[tok] = pos
                        self.rotation_history = data.get("rotation_history", [])
                        self.token_quarantine_until = {k: float(v) for k, v in data.get("quarantined_tokens", {}).items()}
                        break
                except Exception:
                    pass

    def publish_performance_report(self):
        """Format strategy status report string for Hummingbot client."""
        all_closed = []
        for t_list in self.closed_trades.values():
            all_closed.extend(t_list)

        custom_info = {
            "strategy": "spot_perp_cash_carry",
            "is_dry_run": self.config.dry_run,
            "spot_connector": self.config.spot_connector,
            "perp_connector": self.config.perp_connector,
            "perp_leverage": self.config.perp_leverage,
            "active_positions": list(self.active_positions.values()),
            "closed_trades": all_closed,
            "market_candidates": getattr(self, "market_candidates", []),
        }

        report = {
            "spot_perp_cash_carry": {
                "performance": {
                    "total_net_pnl": sum(t.get("total_net_pnl", 0.0) for t in all_closed),
                    "total_funding_harvested": sum(t.get("settled_funding_pnl", 0.0) for t in all_closed),
                    "closed_trades_count": len(all_closed),
                    "active_positions_count": len(self.active_positions),
                },
                "custom_info": custom_info,
            }
        }
        now = time.time()
        if not hasattr(self, "_last_telemetry_log_ts") or now - self._last_telemetry_log_ts >= 60.0:
            self._last_telemetry_log_ts = now
            self.logger().info(f"[TELEMETRY] Spot-Perp report published. Active: {len(self.active_positions)}, Closed: {len(all_closed)}")
        else:
            self.logger().debug(f"[TELEMETRY] Spot-Perp report published. Active: {len(self.active_positions)}, Closed: {len(all_closed)}")

    def format_status(self) -> str:
        """User status display command."""
        lines = []
        lines.append("=======================================================================")
        lines.append(f"  Spot-Perp Cash & Carry Yield Engine ({'DRY-RUN' if self.config.dry_run else 'LIVE'})")
        lines.append("=======================================================================")
        lines.append(f"  Connectors: Spot '{self.config.spot_connector}' + Perp '{self.config.perp_connector}' ({self.config.perp_leverage}x)")
        lines.append(f"  Active Positions: {len(self.active_positions)} / {self.config.max_concurrent_tokens}")

        for tok, pos in self.active_positions.items():
            lines.append(
                f"    - {tok} (Tier {pos.get('tier', 3)}): Spot BUY {pos['amount']:,.2f} @ ${pos['spot_entry_price']:.4f} + "
                f"Perp SHORT {pos['amount']:,.2f} @ ${pos['perp_entry_price']:.4f} | "
                f"Funding Accrued: ${pos.get('settled_funding_pnl', 0.0):+,.2f} | "
                f"APR: {pos.get('current_apr', pos.get('entry_apr', 0.0)):.1%} | "
                f"Safety Cushion: {pos.get('current_safety_cushion_pct', 0.35):.1%}"
            )

        all_closed = sum(len(l) for l in self.closed_trades.values())
        tot_net = sum(sum(t.get("total_net_pnl", 0.0) for t in l) for l in self.closed_trades.values())
        tot_funding = sum(sum(t.get("settled_funding_pnl", 0.0) for t in l) for l in self.closed_trades.values())
        lines.append(f"  Closed Trades: {all_closed} | Total Net: ${tot_net:+,.2f} (Harvested Funding: ${tot_funding:+,.2f})")
        latency = getattr(self, "_last_tick_latency_ms", 0.0)
        lines.append(f"  Engine Tick: 1.0s Loop | Latency: {latency:.1f}ms (Headroom: {max(0.0, 100.0 - latency / 10.0):.1f}%)")
        return "\n".join(lines)
