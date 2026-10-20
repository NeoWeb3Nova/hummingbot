import asyncio
import json
import os
import urllib.request
from decimal import Decimal
from typing import Dict, List, Set, Tuple

from pydantic import Field, field_validator, model_validator

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import MarketDict, TradeType
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction

from scripts.funding_arb_safety import FundingArbSafety
from scripts.funding_arb_accounting import dec


class FundingRateArbitrageConfig(StrategyV2ConfigBase):
    model_config = {"validate_default": True}
    script_file_name: str = os.path.basename(__file__)
    dry_run: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": lambda mi: "Run in dry-run simulation mode without real orders? (True/False): ",
            "prompt_on_new": True,
        }
    )
    leverage: int = Field(
        default=1, ge=1, le=3,
        json_schema_extra={"prompt": lambda mi: "Enter paper leverage (1 to 3): ", "prompt_on_new": True},
    )
    min_funding_rate_profitability: Decimal = Field(
        default=0.001,
        json_schema_extra={
            "prompt": lambda mi: "Enter the min funding rate profitability to enter in a position (e.g. 0.001): ",
            "prompt_on_new": True}
    )
    connectors: Set[str] = Field(
        default="hyperliquid_perpetual,binance_perpetual",
        json_schema_extra={
            "prompt": lambda mi: "Enter the connectors separated by commas (e.g. hyperliquid_perpetual,binance_perpetual): ",
            "prompt_on_new": True}
    )
    tokens: Set[str] = Field(
        default="WIF,FET",
        json_schema_extra={"prompt": lambda mi: "Enter the tokens separated by commas (e.g. WIF,FET): ", "prompt_on_new": True},
    )
    position_size_quote: Decimal = Field(
        default=100,
        json_schema_extra={
            "prompt": lambda mi: "Enter the position size in quote asset (e.g. order amount 100 will open 100 long on hyperliquid and 100 short on binance): ",
            "prompt_on_new": True
        }
    )
    profitability_to_take_profit: Decimal = Field(
        default=Decimal("0.02"),
        json_schema_extra={
            "prompt": lambda mi: "Enter the profitability to take profit (including PNL of positions and fundings received): ",
            "prompt_on_new": True}
    )
    funding_rate_diff_stop_loss: Decimal = Field(
        default=-0.001,
        json_schema_extra={
            "prompt": lambda mi: "Enter the funding rate difference to stop the position (e.g. -0.001): ",
            "prompt_on_new": True}
    )
    trade_profitability_condition_to_enter: bool = Field(
        default=False,
        json_schema_extra={
            "prompt": lambda mi: "Do you want to check the trade profitability condition to enter? (True/False): ",
            "prompt_on_new": True}
    )
    max_payback_hours: Decimal = Field(
        default=Decimal("12.0"),
        json_schema_extra={
            "prompt": lambda mi: "Max payback hours to accept entry friction vs funding yield (e.g. 24.0): ",
            "prompt_on_new": True,
        }
    )
    max_entry_spread_pct: Decimal = Field(
        default=Decimal("0.0010"),
        json_schema_extra={
            "prompt": lambda mi: "Max allowed entry price divergence ratio between exchanges (e.g. 0.0010 for 0.10%): ",
            "prompt_on_new": False,
        }
    )
    duration_linked_holding_factor: float = Field(
        default=2.5,
        json_schema_extra={
            "prompt": lambda mi: "Multiplier on payback hours to set minimum holding protection duration (e.g. 2.5): ",
            "prompt_on_new": False,
        }
    )
    min_holding_hours_floor: float = Field(
        default=24.0,
        json_schema_extra={
            "prompt": lambda mi: "Absolute floor in hours for minimum holding lock (e.g. 24.0): ",
            "prompt_on_new": False,
        }
    )
    dynamic_position_sizing: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": lambda mi: "Enable dynamic position sizing based on order book depth? (True/False): ",
            "prompt_on_new": True,
        }
    )
    max_orderbook_depth_ratio: Decimal = Field(
        default=Decimal("0.15"),
        json_schema_extra={
            "prompt": lambda mi: "Max ratio of top order book depth to consume (e.g. 0.15 for 15%): ",
            "prompt_on_new": True,
        }
    )
    min_position_size_quote: Decimal = Field(
        default=Decimal("100"),
        json_schema_extra={
            "prompt": lambda mi: "Minimum position size in quote asset to open (e.g. 100): ",
            "prompt_on_new": True,
        }
    )
    liquidation_buffer_pct: Decimal = Field(
        default=Decimal("0.08"),
        json_schema_extra={
            "prompt": lambda mi: "Safety distance threshold to liquidation price for defense exit (e.g. 0.08 for 8%): ",
            "prompt_on_new": True,
        }
    )
    min_leverage_capital_threshold: Decimal = Field(
        default=Decimal("500.0"),
        json_schema_extra={
            "prompt": lambda mi: "Min capital threshold for applying leverage (below this size defaults to 1x safe mode, e.g. 500): ",
            "prompt_on_new": False,
        }
    )
    max_allowed_price_divergence: Decimal = Field(
        default=Decimal("0.03"),
        json_schema_extra={
            "prompt": lambda mi: "Max allowed cross-exchange price divergence to prevent ticker collisions (e.g. 0.03 for 3%): ",
            "prompt_on_new": False,
        }
    )
    token_quarantine_hours: Decimal = Field(
        default=Decimal("24.0"),
        json_schema_extra={
            "prompt": lambda mi: "Hours to quarantine a toxic coin after heavy loss or consecutive stop losses (e.g. 24.0): ",
            "prompt_on_new": False,
        }
    )
    single_loss_quarantine_threshold: Decimal = Field(
        default=Decimal("15.0"),
        json_schema_extra={
            "prompt": lambda mi: "Single trade loss USD threshold to trigger quarantine (e.g. 15.0): ",
            "prompt_on_new": False,
        }
    )
    rate_inversion_confirmation_ticks: int = Field(
        default=3,
        json_schema_extra={
            "prompt": lambda mi: "Consecutive ticks of rate inversion required to confirm stop loss (e.g. 3): ",
            "prompt_on_new": False,
        }
    )
    reentry_cooldown_seconds: int = Field(
        default=1800,
        json_schema_extra={
            "prompt": lambda mi: "Cooldown seconds before re-entering a recently closed token (e.g. 1800 for 30m): ",
            "prompt_on_new": False,
        }
    )
    max_entry_spread_friction_pct: Decimal = Field(
        default=Decimal("0.005"),
        json_schema_extra={
            "prompt": lambda mi: "Max allowable entry friction (spread + fee discount) to open position (e.g. 0.005 for 0.5%): ",
            "prompt_on_new": False,
        }
    )
    entry_persistence_ticks: int = Field(
        default=3,
        json_schema_extra={
            "prompt": lambda mi: "Consecutive ticks of positive spread/payback required to confirm entry (e.g. 3): ",
            "prompt_on_new": False,
        }
    )
    max_basis_loss_pct: Decimal = Field(
        default=Decimal("0.012"),
        json_schema_extra={
            "prompt": lambda mi: "Max allowed trading net loss ratio before emergency cut (e.g. 0.012 for 1.2%): ",
            "prompt_on_new": False,
        }
    )
    basis_divergence_confirmation_ticks: int = Field(
        default=5,
        json_schema_extra={
            "prompt": lambda mi: "Consecutive ticks of basis divergence required to confirm stop loss (e.g. 5): ",
            "prompt_on_new": False,
        }
    )
    settlement_blackout_minutes: int = Field(
        default=30,
        json_schema_extra={
            "prompt": lambda mi: "Minutes before 8h funding settlement to freeze new entries on volatile tokens (e.g. 30): ",
            "prompt_on_new": False,
        }
    )
    settlement_blackout_tier3_only: bool = Field(
        default=False,
        json_schema_extra={
            "prompt": lambda mi: "Only apply settlement blackout to Tier 3 tokens? (True/False): ",
            "prompt_on_new": False,
        }
    )
    tier_1_tokens: Set[str] = Field(
        default="BTC,ETH,SOL",
        json_schema_extra={"prompt": lambda mi: "Tier 1 mega-cap tokens (e.g. BTC,ETH,SOL): ", "prompt_on_new": False}
    )
    tier_1_max_quote: Decimal = Field(
        default=Decimal("10000.0"),
        json_schema_extra={"prompt": lambda mi: "Tier 1 maximum position size quote (e.g. 10000.0): ", "prompt_on_new": False}
    )
    tier_2_tokens: Set[str] = Field(
        default="AVAX,SUI,APT,NEAR,LINK,ARB,OP,INJ,DOGE,XRP,ADA,MATIC,DOT,UNI,LTC,BCH,BNB,SAND",
        json_schema_extra={"prompt": lambda mi: "Tier 2 large/mid-cap tokens: ", "prompt_on_new": False}
    )
    tier_2_max_quote: Decimal = Field(
        default=Decimal("2000.0"),
        json_schema_extra={"prompt": lambda mi: "Tier 2 maximum position size quote (e.g. 2000.0): ", "prompt_on_new": False}
    )
    tier_3_max_quote: Decimal = Field(
        default=Decimal("500.0"),
        json_schema_extra={"prompt": lambda mi: "Tier 3 micro-cap / meme tokens maximum position size quote (e.g. 500.0): ", "prompt_on_new": False}
    )

    # --- v1.3.0 Autonomous Universe Auto-Rotation Fields ---
    auto_rotation_enabled: bool = Field(
        default=True,
        json_schema_extra={"prompt": lambda mi: "Enable autonomous token universe rotation? (True/False): ", "prompt_on_new": False}
    )
    auto_rotation_interval_minutes: int = Field(
        default=30,
        json_schema_extra={"prompt": lambda mi: "Interval in minutes between full market rotation checks (e.g. 30): ", "prompt_on_new": False}
    )
    auto_rotation_min_apr: float = Field(
        default=0.35,
        json_schema_extra={"prompt": lambda mi: "Minimum APR required for candidate tokens to enter universe (e.g. 0.35 for 35%): ", "prompt_on_new": False}
    )
    auto_rotation_min_apr_delta: float = Field(
        default=0.40,
        json_schema_extra={"prompt": lambda mi: "Minimum APR advantage over idle token to trigger rotation (e.g. 0.40 for 40%): ", "prompt_on_new": False}
    )
    auto_rotation_max_active_tokens: int = Field(
        default=6,
        json_schema_extra={"prompt": lambda mi: "Maximum number of tokens in active watchlist (e.g. 6): ", "prompt_on_new": False}
    )
    auto_rotation_min_volume_24h_usd: float = Field(
        default=2000000.0,
        json_schema_extra={"prompt": lambda mi: "Minimum 24h volume USD to qualify for auto-rotation (e.g. 2000000.0): ", "prompt_on_new": False}
    )

    # --- v1.3.0 Tier-Aware Dynamic Liquidation Buffers ---
    tier_1_liquidation_buffer: Decimal = Field(
        default=Decimal("0.08"),
        json_schema_extra={"prompt": lambda mi: "Tier 1 liquidation safety buffer (e.g. 0.04 for 4%): ", "prompt_on_new": False}
    )
    tier_2_liquidation_buffer: Decimal = Field(
        default=Decimal("0.08"),
        json_schema_extra={"prompt": lambda mi: "Tier 2 liquidation safety buffer (e.g. 0.08 for 8%): ", "prompt_on_new": False}
    )
    tier_3_liquidation_buffer: Decimal = Field(
        default=Decimal("0.12"),
        json_schema_extra={"prompt": lambda mi: "Tier 3 liquidation safety buffer (e.g. 0.12 for 12%): ", "prompt_on_new": False}
    )

    # --- v1.3.0 Scaling-Out Take Profit Fields (Option B) ---
    scaling_tp_enabled: bool = Field(
        default=True,
        json_schema_extra={"prompt": lambda mi: "Enable scaling-out tranche take profit? (True/False): ", "prompt_on_new": False}
    )
    partial_tp_ratio: Decimal = Field(
        default=Decimal("0.50"),
        json_schema_extra={"prompt": lambda mi: "Ratio of position to close on tranche 1 (e.g. 0.50 for 50%): ", "prompt_on_new": False}
    )
    partial_tp_threshold_pct: Decimal = Field(
        default=Decimal("0.010"),
        json_schema_extra={"prompt": lambda mi: "Net profit threshold on position to trigger tranche 1 (e.g. 0.010 for 1.0%): ", "prompt_on_new": False}
    )

    # --- v1.4.0 Institutional Risk & Profit Overhaul Fields ---
    post_tp_cooldown_hours: float = Field(
        default=4.0,
        json_schema_extra={"prompt": lambda mi: "Cooldown hours before re-entering a token after take profit (e.g. 4.0): ", "prompt_on_new": False}
    )
    breakeven_trailing_activation_pct: Decimal = Field(
        default=Decimal("0.006"),
        json_schema_extra={"prompt": lambda mi: "Net profit pct to activate breakeven stop loss (e.g. 0.006 for 0.6%): ", "prompt_on_new": False}
    )
    breakeven_trailing_stop_pct: Decimal = Field(
        default=Decimal("0.0005"),
        json_schema_extra={"prompt": lambda mi: "Locked breakeven stop loss level after activation (e.g. 0.0005 for +0.05%): ", "prompt_on_new": False}
    )
    cumulative_loss_hard_blacklist_usd: float = Field(
        default=30.0,
        json_schema_extra={"prompt": lambda mi: "Cumulative historical net loss threshold to permanently blacklist toxic token (e.g. 30.0 for $30): ", "prompt_on_new": False}
    )

    # --- v1.5.0 Asset Graduation & Rehabilitation Engine Fields ---
    asset_graduation_enabled: bool = Field(
        default=True,
        json_schema_extra={"prompt": lambda mi: "Enable periodic asset graduation review for blacklisted tokens? (True/False): ", "prompt_on_new": False}
    )
    asset_graduation_interval_days: float = Field(
        default=7.0,
        json_schema_extra={"prompt": lambda mi: "Days interval between asset graduation reviews (e.g. 7.0): ", "prompt_on_new": False}
    )
    graduation_min_volume_24h_usd: float = Field(
        default=30000000.0,
        json_schema_extra={"prompt": lambda mi: "Min 24h volume across Binance & HL to qualify for graduation (e.g. 30000000.0 for $30M): ", "prompt_on_new": False}
    )
    graduation_max_price_divergence: float = Field(
        default=0.005,
        json_schema_extra={"prompt": lambda mi: "Max mark price divergence allowed for graduating token (e.g. 0.005 for 0.5%): ", "prompt_on_new": False}
    )
    graduation_probe_size_quote: Decimal = Field(
        default=Decimal("200.0"),
        json_schema_extra={"prompt": lambda mi: "Probe trade size in USD for probationary tokens (e.g. 200.0 for $200): ", "prompt_on_new": False}
    )

    @field_validator("connectors", "tokens", "tier_1_tokens", "tier_2_tokens", mode="before")
    @classmethod
    def validate_sets(cls, v):
        if isinstance(v, str):
            return {x.strip() for x in v.split(",") if x.strip()}
        if isinstance(v, (list, tuple)):
            return set(v)
        return v

    paper_state_path: str = "/home/hummingbot/data/arbitrage_trades.json"
    paper_account_balance_quote: Decimal = Field(default=Decimal("10000"), gt=0)
    paper_margin_reserve_pct: Decimal = Field(default=Decimal("0.25"), ge=0, lt=1)
    max_portfolio_drawdown_pct: Decimal = Field(default=Decimal("0.05"), gt=0, lt=1)
    max_concurrent_positions: int = Field(default=6, ge=1)
    max_orderbook_age_seconds: float = Field(default=15, gt=0)
    max_funding_data_age_seconds: float = Field(default=120, gt=0)
    max_leg_price_impact_pct: Decimal = Field(default=Decimal("0.002"), gt=0, lt=1)
    max_hedge_mismatch_pct: Decimal = Field(default=Decimal("0.001"), ge=0, le=Decimal("0.001"))
    paper_maintenance_margin_pct: Decimal = Field(default=Decimal("0.02"), gt=0, lt=1)

    @model_validator(mode="after")
    def validate_paper_risk(self):
        for name in type(self).model_fields:
            value = getattr(self, name)
            if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
                dec(value)
        if not self.dry_run:
            raise ValueError("Live execution requires paired-fill reconciliation; only paper is accepted in this audited revision")
        if set(self.connectors) != {"binance_perpetual", "hyperliquid_perpetual"}:
            raise ValueError("Audited rate histories currently support Binance USDT and Hyperliquid USD only")
        if not Decimal("0") < self.max_payback_hours <= Decimal("12"):
            raise ValueError("Project rule: max_payback_hours must be in (0, 12]")
        if not Decimal("0") < self.max_orderbook_depth_ratio <= Decimal("0.30"):
            raise ValueError("Project rule: displayed-depth fraction must be in (0, 0.30]")
        for name in ("liquidation_buffer_pct", "tier_1_liquidation_buffer", "tier_2_liquidation_buffer", "tier_3_liquidation_buffer"):
            if not Decimal("0.08") <= getattr(self, name) < 1:
                raise ValueError("Project rule: liquidation buffers must be at least 8% and below 100%")
        for name in ("position_size_quote", "min_position_size_quote", "tier_1_max_quote", "tier_2_max_quote", "tier_3_max_quote", "min_funding_rate_profitability", "max_basis_loss_pct", "profitability_to_take_profit"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        for name in ("entry_persistence_ticks", "rate_inversion_confirmation_ticks", "basis_divergence_confirmation_ticks"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be at least one")
        if self.funding_rate_diff_stop_loss > Decimal("-0.001"):
            raise ValueError("Project rule: rate inversion threshold must be <= -0.1% daily")
        if not 0 < self.partial_tp_ratio < 1:
            raise ValueError("partial_tp_ratio must be strictly between zero and one")
        if not 0 <= self.breakeven_trailing_stop_pct < self.breakeven_trailing_activation_pct:
            raise ValueError("trailing stop must be below its positive activation threshold")
        object.__setattr__(self, "tokens", {FundingRateArbitrage.get_canonical_token(t.strip()).upper() for t in self.tokens if t.strip()})
        return self

    def update_markets(self, markets: MarketDict) -> MarketDict:
        for connector in self.connectors:
            trading_pairs = {FundingRateArbitrage.get_trading_pair_for_connector(token, connector) for token in self.tokens}
            markets[connector] = markets.get(connector, set()) | trading_pairs
        return markets


class FundingRateArbitrage(StrategyV2Base):
    quote_markets_map = {
        "hyperliquid_perpetual": "USD",
        "binance_perpetual": "USDT"
    }
    funding_payment_interval_map = {
        "binance_perpetual": 60 * 60 * 8,
        "hyperliquid_perpetual": 60 * 60 * 1
    }
    funding_profitability_interval = 60 * 60 * 24

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
        "XEC": {
            "binance_perpetual": {"symbol": "1000XEC", "multiplier": 1000.0},
            "hyperliquid_perpetual": {"symbol": "kXEC", "multiplier": 1000.0},
        },
        "RATS": {
            "binance_perpetual": {"symbol": "1000RATS", "multiplier": 1000.0},
            "hyperliquid_perpetual": {"symbol": "kRATS", "multiplier": 1000.0},
        },
        "SATS": {
            "binance_perpetual": {"symbol": "1000SATS", "multiplier": 1000.0},
            "hyperliquid_perpetual": {"symbol": "kSATS", "multiplier": 1000.0},
        },
        "CAT": {
            "binance_perpetual": {"symbol": "1000CAT", "multiplier": 1000.0},
            "hyperliquid_perpetual": {"symbol": "kCAT", "multiplier": 1000.0},
        },
        "CHEEMS": {
            "binance_perpetual": {"symbol": "1000CHEEMS", "multiplier": 1000.0},
            "hyperliquid_perpetual": {"symbol": "kCHEEMS", "multiplier": 1000.0},
        },
        "WHY": {
            "binance_perpetual": {"symbol": "1000WHY", "multiplier": 1000.0},
            "hyperliquid_perpetual": {"symbol": "kWHY", "multiplier": 1000.0},
        },
        "MOG": {
            "binance_perpetual": {"symbol": "1000000MOG", "multiplier": 1000000.0},
            "hyperliquid_perpetual": {"symbol": "MOG", "multiplier": 1.0},
        },
    }

    @classmethod
    def get_canonical_token(cls, symbol: str, connector: str = "") -> str:
        if not symbol:
            return ""
        raw_clean = symbol.strip().split("-")[0]
        clean_sym = raw_clean.upper()
        c_lower = (connector or "").lower()

        # Check registry
        for canon, conns in cls.canonical_token_registry.items():
            for conn_key, data in conns.items():
                if raw_clean == data["symbol"] or clean_sym == data["symbol"].upper():
                    if not c_lower or conn_key.startswith(c_lower) or c_lower.startswith(conn_key.split("_")[0]):
                        return canon

        if clean_sym in cls.canonical_token_registry:
            return clean_sym

        # Heuristic: HL lowercase k-prefix
        if raw_clean.startswith("k") and len(raw_clean) >= 4 and raw_clean[1:].isupper():
            return raw_clean[1:]
        # dYdX '1k' prefix (e.g. 1kPEPE)
        if clean_sym.startswith("1K") and len(clean_sym) > 3:
            return clean_sym[2:]
        # Binance / Bybit / Gate '1000000' prefix
        if clean_sym.startswith("1000000") and len(clean_sym) > 7:
            return clean_sym[7:]
        # Bybit '10000' prefix (e.g. 10000LADYS)
        if clean_sym.startswith("10000") and len(clean_sym) > 5:
            return clean_sym[5:]
        # Binance / Bybit / Bitget / KuCoin '1000' prefix
        if clean_sym.startswith("1000") and len(clean_sym) > 4:
            return clean_sym[4:]
        return clean_sym

    @classmethod
    def get_exchange_symbol(cls, token: str, connector: str) -> str:
        canon = cls.get_canonical_token(token, connector)
        c_lower = (connector or "").lower()
        if canon in cls.canonical_token_registry:
            conns = cls.canonical_token_registry[canon]
            for conn_key, val in conns.items():
                if conn_key in c_lower or c_lower.startswith(conn_key.split("_")[0]):
                    return val["symbol"]

        if "hyperliquid" in c_lower:
            if canon in ["PEPE", "SHIB", "BONK", "FLOKI", "LUNC", "CAT", "CHEEMS"]:
                return f"k{canon}"
        elif "dydx" in c_lower:
            if canon in ["PEPE", "SHIB", "BONK"]:
                return f"1k{canon}"
        elif any(k in c_lower for k in ["binance", "bybit", "bitget", "kucoin"]):
            if canon in ["PEPE", "SHIB", "BONK", "FLOKI", "LUNC", "CAT", "CHEEMS", "WHY", "RATS", "SATS", "XEC"]:
                return f"1000{canon}"
            if canon in ["MOG", "BOB"]:
                return f"1000000{canon}"
        return canon

    @classmethod
    def get_token_multiplier(cls, token: str, connector: str) -> float:
        canon = cls.get_canonical_token(token, connector)
        c_lower = (connector or "").lower()
        if canon in cls.canonical_token_registry:
            conns = cls.canonical_token_registry[canon]
            for conn_key, val in conns.items():
                if conn_key in c_lower or c_lower.startswith(conn_key.split("_")[0]):
                    return float(val.get("multiplier", 1.0))

        raw_clean = cls.get_exchange_symbol(canon, connector).split("-")[0]
        clean_sym = raw_clean.upper()
        if clean_sym.startswith("1000000"):
            return 1000000.0
        if clean_sym.startswith("10000"):
            return 10000.0
        if clean_sym.startswith("1000") or clean_sym.startswith("1K"):
            return 1000.0
        if raw_clean.startswith("k") and len(raw_clean) >= 4 and raw_clean[1:].isupper():
            return 1000.0
        return 1.0

    @classmethod
    def get_trading_pair_for_connector(cls, token, connector):
        symbol = cls.get_exchange_symbol(token, connector)
        quote = cls.quote_markets_map.get(connector, 'USDT')
        return f"{symbol}-{quote}"

    @classmethod
    def init_markets(cls, config):
        super().init_markets(config)
        if getattr(config, "dry_run", False):
            cls._enable_dry_run_connectors()

    @classmethod
    def _enable_dry_run_connectors(cls):
        from hummingbot.client.settings import AllConnectorSettings
        from hummingbot.core.connector_manager import ConnectorManager, get_connector_class
        from hummingbot.core.rate_oracle.rate_oracle import RateOracle

        if getattr(ConnectorManager, "_funding_paper_public_only", False):
            return
        original = ConnectorManager.create_connector

        def create_public(manager, connector_name, trading_pairs=None, trading_required=True, api_keys=None):
            if connector_name not in cls.quote_markets_map:
                return original(manager, connector_name, trading_pairs, trading_required, api_keys)
            setting = AllConnectorSettings.get_connector_settings()[connector_name]
            params = setting.conn_init_parameters(
                trading_pairs=trading_pairs, trading_required=False, api_keys={},
                balance_asset_limit=manager.client_config_map.hb_config.balance_asset_limit,
                rate_limits_share_pct=manager.client_config_map.hb_config.rate_limits_share_pct,
                gateway_config=manager.client_config_map.hb_config.gateway)
            connector = get_connector_class(connector_name)(**params)
            manager.connectors[connector_name] = connector
            RateOracle.get_instance().register_connector(connector)
            return connector

        ConnectorManager.create_connector = create_public
        ConnectorManager._funding_paper_public_only = True


    def __init__(self, connectors: Dict[str, ConnectorBase], config: FundingRateArbitrageConfig):
        super().__init__(connectors, config)
        self.config = config
        self.active_funding_arbitrages = {}
        self.stopped_funding_arbitrages = {token: [] for token in self.config.tokens}
        # v1.1.0 Institutional Risk Shields
        self.token_quarantine_ledger: Dict[str, Dict] = {}  # token -> {"quarantine_until": float, "reason": str, "consecutive_losses": int}
        self.inversion_tick_counter: Dict[str, int] = {}    # token -> int (ticks of sustained rate inversion)
        self.token_last_closed_timestamp: Dict[str, float] = {} # token -> float (timestamp of last closure)
        # Defense 1: Entry Hysteresis persistence counters
        self.entry_candidate_ticks: Dict[str, int] = {}     # token -> int (consecutive ticks meeting entry criteria)
        # v1.3.0 Autonomous Auto-Rotation & Scaling TP Engine
        self.rotation_event_history: List[Dict] = []
        self.last_rotation_timestamp: float = 0.0
        self._last_rotation_check_ts: float = 0.0
        self._last_cmd_check_ts: float = 0.0
        self.is_rotating: bool = False
        # v1.4.0 Institutional Risk & Profit Overhaul State
        self.token_post_tp_lockout: Dict[str, float] = {}   # token -> timestamp until which entries are locked post-TP
        self.token_permanent_blacklist: Set[str] = set()     # tokens permanently banned due to cumulative deficit
        # v1.5.0 Asset Graduation & Rehabilitation Engine State
        self.probationary_tokens: Dict[str, Dict] = {}      # token -> {"admitted_ts": float, "probe_size": float, "status": str, "reason": str}
        self._last_graduation_check_ts: float = 0.0
        self.is_evaluating_graduation: bool = False
        # v1.5.1 Basis Divergence Hysteresis Persistence
        self.basis_divergence_tick_counter: Dict[str, int] = {}
        self.audit = FundingArbSafety(self)

    def get_token_tier(self, token: str) -> Tuple[int, Decimal]:
        """
        Determine token risk tier and maximum position size quote.
        Tier 1: Mega-cap (BTC, ETH, SOL)
        Tier 2: Major alts (AVAX, SUI, APT, NEAR, LINK, ARB, OP, INJ, SAND, etc.)
        Tier 3: Micro-cap / meme / high-volatility tokens (BLUR, 2Z, BERA, RSR, etc.)
        """
        canon = self.get_canonical_token(token).upper()
        t1_tokens = getattr(self.config, "tier_1_tokens", {"BTC", "ETH", "SOL"})
        t1_set = {self.get_canonical_token(x).upper() for x in (t1_tokens if isinstance(t1_tokens, (set, list, tuple)) else str(t1_tokens).split(","))}

        t2_tokens = getattr(self.config, "tier_2_tokens", {
            "AVAX", "SUI", "APT", "NEAR", "LINK", "ARB", "OP", "INJ",
            "DOGE", "XRP", "ADA", "MATIC", "DOT", "UNI", "LTC", "BCH", "BNB", "SAND"
        })
        t2_set = {self.get_canonical_token(x).upper() for x in (t2_tokens if isinstance(t2_tokens, (set, list, tuple)) else str(t2_tokens).split(","))}

        if canon in t1_set:
            return 1, Decimal(str(getattr(self.config, "tier_1_max_quote", Decimal("10000.0"))))
        elif canon in t2_set:
            return 2, Decimal(str(getattr(self.config, "tier_2_max_quote", Decimal("2000.0"))))
        else:
            return 3, Decimal(str(getattr(self.config, "tier_3_max_quote", Decimal("500.0"))))

    def get_effective_leverage(self, token: str, position_size_quote: Decimal) -> int:
        """
        专业动态杠杆引擎 (Dynamic Adaptive Leverage Engine):
        1. 全局配置为 1x 时，两腿仍是永续合约，分别检查模拟保证金风险；
        2. 若单仓名义价值较小，自动降为 1x，
           避免小资金产生单腿爆仓风险与频繁防御止损；
        3. 若仓位规模充足 (> 阈值)，才启用目标杠杆（如 2x~3x）提升资金使用效率。
        """
        cfg_lev = max(1, int(getattr(self.config, "leverage", 3)))
        if cfg_lev == 1:
            return 1
        threshold = Decimal(str(getattr(self.config, "min_leverage_capital_threshold", Decimal("500.0"))))
        if position_size_quote <= threshold:
            return 1
        return cfg_lev


    def start(self, clock: Clock, timestamp: float) -> None:
        super().start(clock, timestamp)
        self.audit.restore(timestamp)


    def on_tick(self):
        self.audit.tick()

    async def on_stop(self):
        self._is_stop_triggered = True
        await self.audit.shutdown()
        await super().on_stop()


    def check_strategy_commands(self):
        """
        Poll and execute real-time commands from Dashboard UI (Zero-Downtime Hot Control Channel).
        """
        now = self.current_timestamp
        if now - getattr(self, "_last_cmd_check_ts", 0.0) < 2.0:
            return
        self._last_cmd_check_ts = now

        cmd_path = "/home/hummingbot/data/strategy_commands.json"
        if os.path.exists(cmd_path):
            try:
                with open(cmd_path, "r") as f:
                    cmd = json.load(f)
                os.remove(cmd_path)
                self.logger().info(f"[COMMAND] Received strategy command from Dashboard: {cmd}")
                act = cmd.get("action")
                if act == "add_token":
                    t = cmd.get("token")
                    if t:
                        self.audit._spawn(self.async_add_token(t))
                elif act == "remove_token":
                    t = cmd.get("token")
                    if t:
                        self.audit._spawn(self.async_remove_token(t))
                elif act == "trigger_rotation":
                    self.audit._spawn(self.run_universe_auto_rotation(force=True))
                elif act == "clear_quarantine":
                    t = cmd.get("token")
                    if t and t in self.token_quarantine_ledger:
                        del self.token_quarantine_ledger[t]
                        self.logger().info(f"[COMMAND] Cleared quarantine for {t}")
                elif act == "unblacklist_token":
                    t = cmd.get("token")
                    if t:
                        if t in self.token_permanent_blacklist:
                            self.token_permanent_blacklist.discard(t)
                            self.logger().info(f"[COMMAND] Pardoned & removed token {t} from permanent toxic blacklist.")
                        if t in self.token_quarantine_ledger:
                            del self.token_quarantine_ledger[t]
                            self.logger().info(f"[COMMAND] Cleared quarantine ledger for {t}.")
                elif act == "trigger_graduation_audit":
                    self.logger().info("[COMMAND] Manual graduation audit requested from Dashboard.")
                    self.audit._spawn(self.async_run_asset_graduation(force=True))
            except Exception as e:
                self.logger().warning(f"[COMMAND] Failed processing strategy command: {e}")

    def check_universe_auto_rotation(self):
        """
        Periodically evaluate cross-exchange perpetual universe and rotate stale idle tokens.
        """
        if not getattr(self.config, "auto_rotation_enabled", True):
            return
        if getattr(self, "is_rotating", False):
            return
        now = self.current_timestamp
        interval_sec = float(getattr(self.config, "auto_rotation_interval_minutes", 30)) * 60.0
        if not hasattr(self, "_last_rotation_check_ts") or self._last_rotation_check_ts == 0.0:
            # On startup, give 60s for connectors to establish streams before initial scan
            self._last_rotation_check_ts = now - interval_sec + 60.0
        if now - self._last_rotation_check_ts >= interval_sec:
            self._last_rotation_check_ts = now
            self.audit._spawn(self.run_universe_auto_rotation(force=False))

    async def async_add_token(self, token: str) -> bool:
        """
        Dynamically add a token to connector WebSocket streams and strategy watchlist without restart.
        """
        token = self.get_canonical_token(token).upper()
        if token in self.config.tokens:
            self.logger().info(f"[DYNAMIC-MARKET] Token {token} already in active watchlist.")
            return True
        self.logger().info(f"[DYNAMIC-MARKET] Dynamically mounting token {token} to connectors...")
        for c_name in self.connectors:
            connector = self.connectors[c_name]
            pair = self.get_trading_pair_for_connector(token, c_name)
            if hasattr(connector, "add_trading_pair"):
                try:
                    success = await connector.add_trading_pair(pair)
                    if not success:
                        self.logger().warning(f"[DYNAMIC-MARKET] Connector {c_name} reported failure adding {pair}")
                        return False
                except Exception as e:
                    self.logger().warning(f"[DYNAMIC-MARKET] Error adding {pair} on {c_name}: {e}")
                    return False
        if isinstance(self.config.tokens, list):
            self.config.tokens.append(token)
        elif isinstance(self.config.tokens, set):
            self.config.tokens.add(token)
        self.stopped_funding_arbitrages.setdefault(token, [])
        self.logger().info(f"[DYNAMIC-MARKET] Successfully mounted {token} to watchlist: {list(self.config.tokens)}")
        self._sync_tokens_to_config_files()
        return True

    async def async_remove_token(self, token: str) -> bool:
        """
        Dynamically remove a token from active watchlist and unregister from connectors if no active position.
        """
        token = self.get_canonical_token(token).upper()
        self.logger().info(f"[DYNAMIC-MARKET] Requesting dynamic removal of token {token}...")
        if token in self.config.tokens:
            if isinstance(self.config.tokens, list):
                self.config.tokens.remove(token)
            elif isinstance(self.config.tokens, set):
                self.config.tokens.discard(token)
        if token not in self.active_funding_arbitrages:
            for c_name in self.connectors:
                connector = self.connectors[c_name]
                pair = self.get_trading_pair_for_connector(token, c_name)
                if hasattr(connector, "remove_trading_pair"):
                    try:
                        await connector.remove_trading_pair(pair)
                    except Exception as e:
                        self.logger().warning(f"[DYNAMIC-MARKET] Error unmounting {pair} on {c_name}: {e}")
            self.logger().info(f"[DYNAMIC-MARKET] Successfully unmounted market {token} from connectors")
        else:
            self.logger().info(f"[DYNAMIC-MARKET] Token {token} has active position; market will remain mounted until graceful exit completes.")
        self._sync_tokens_to_config_files()
        return True

    def _sync_tokens_to_config_files(self):
        # Persist the universe in the same atomic state as positions. Rewriting
        # a hard-coded YAML file can change a different running instance.
        self.audit.persist()


    async def run_universe_auto_rotation(self, force: bool = False):
        """
        Execute full market radar scan and rotate idle low-yield tokens to high-APR candidates.
        """
        if self.is_rotating:
            return
        self.is_rotating = True
        try:
            self.logger().info(f"[AUTO-ROTATION] Starting universe opportunity scan (force={force})...")
            candidates = await asyncio.to_thread(self._fetch_market_rotation_candidates)
            if not candidates:
                self.logger().info("[AUTO-ROTATION] No viable cross-exchange market candidates found.")
                return

            min_apr = float(getattr(self.config, "auto_rotation_min_apr", 0.35))
            min_delta = float(getattr(self.config, "auto_rotation_min_apr_delta", 0.40))
            max_tokens = int(getattr(self.config, "auto_rotation_max_active_tokens", 6))

            # Filter candidates above threshold and not quarantined or permanently blacklisted
            viable_candidates = [
                c for c in candidates
                if c["apr"] >= min_apr * 100.0
                and c["token"] not in self.token_quarantine_ledger
                and c["token"] not in self.token_permanent_blacklist
                and c["token"] not in self.config.tokens
            ]

            current_tokens = list(self.config.tokens)
            idle_tokens = [t for t in current_tokens if t not in self.active_funding_arbitrages]

            top_cand_preview = [f"{c['token']} ({c['apr']:+.1f}%)" for c in viable_candidates[:5]]
            self.logger().info(
                f"[AUTO-ROTATION] Watchlist: {len(current_tokens)} tokens (Idle: {idle_tokens}). "
                f"Top viable candidates: {top_cand_preview}"
            )

            # 1. If watchlist is below max_tokens, simply add top candidate
            if len(current_tokens) < max_tokens and viable_candidates:
                cand = viable_candidates.pop(0)
                success = await self.async_add_token(cand["token"])
                if success:
                    event = {
                        "timestamp": int(self.current_timestamp),
                        "retired_token": "NONE (Slot Expansion)",
                        "retired_apr": 0.0,
                        "added_token": cand["token"],
                        "added_apr": cand["apr"],
                        "apr_delta": cand["apr"],
                        "reason": f"Slot Expansion: Added high-yield candidate {cand['token']} ({cand['apr']:+.1f}% APR)"
                    }
                    self.rotation_event_history.insert(0, event)
                    self.rotation_event_history = self.rotation_event_history[:25]
                    self.last_rotation_timestamp = self.current_timestamp
                    return

            # 2. Check if an idle token should be rotated out
            idle_apr_map = {}
            for t in idle_tokens:
                if t in self.token_quarantine_ledger:
                    idle_apr_map[t] = -9999.0
                    continue
                try:
                    f_rep = self.get_funding_info_by_token(t)
                    best = self.get_most_profitable_combination(f_rep)
                    idle_apr_map[t] = (best[3] * 365.0 * 100.0) if best else 0.0
                except Exception:
                    idle_apr_map[t] = 0.0

            sorted_idle = sorted(idle_apr_map.keys(), key=lambda t: idle_apr_map[t])

            for idle_tok in sorted_idle:
                idle_apr = idle_apr_map[idle_tok]
                if not viable_candidates:
                    break
                best_cand = viable_candidates[0]
                cand_tok = best_cand["token"]
                cand_apr = best_cand["apr"]
                delta_apr = cand_apr - idle_apr

                is_quarantined = (idle_tok in self.token_quarantine_ledger)
                is_stale_low = (idle_apr < 5.0 and cand_apr >= min_apr * 100.0)
                has_major_delta = (delta_apr >= min_delta * 100.0)

                if is_quarantined or is_stale_low or has_major_delta or force:
                    viable_candidates.pop(0)
                    self.logger().info(
                        f"[AUTO-ROTATION] Evicting idle token {idle_tok} (APR {idle_apr:+.1f}%) -> "
                        f"Injecting candidate {cand_tok} (APR {cand_apr:+.1f}%, ΔAPR {delta_apr:+.1f}%)..."
                    )
                    await self.async_remove_token(idle_tok)
                    success = await self.async_add_token(cand_tok)
                    if success:
                        reason_desc = (
                            f"Quarantine Eviction ({idle_tok})" if is_quarantined else
                            (f"Stale Low APR Eviction ({idle_tok} {idle_apr:+.1f}%)" if is_stale_low else
                             f"Yield Super-Spread (ΔAPR {delta_apr:+.1f}%)")
                        )
                        event = {
                            "timestamp": int(self.current_timestamp),
                            "retired_token": idle_tok,
                            "retired_apr": float(idle_apr),
                            "added_token": cand_tok,
                            "added_apr": float(cand_apr),
                            "apr_delta": float(delta_apr),
                            "reason": f"Auto-Rotation: {reason_desc} replaced by {cand_tok} ({cand_apr:+.1f}% APR)"
                        }
                        self.rotation_event_history.insert(0, event)
                        self.rotation_event_history = self.rotation_event_history[:25]
                        self.last_rotation_timestamp = self.current_timestamp
                        self.logger().info(f"[AUTO-ROTATION] Completed rotation: {event['reason']}")
                        break
        except Exception as e:
            self.logger().warning(f"[AUTO-ROTATION] Error during universe rotation: {e}")
        finally:
            self.is_rotating = False

    def _fetch_market_rotation_candidates(self) -> List[Dict]:
        from scripts import funding_arb_public_data as public
        try:
            metadata = public.metadata_snapshot()
            volumes = public.request_json("https://fapi.binance.com/fapi/v1/ticker/24hr")
            bn_volume = {r["symbol"]: dec(r["quoteVolume"], nonnegative=True) for r in volumes}
            contexts = public.request_json("https://api.hyperliquid.xyz/info", {"type": "metaAndAssetCtxs"})
            hl_volume = {a["name"]: dec(r["dayNtlVlm"], nonnegative=True) for a,r in zip(contexts[0]["universe"],contexts[1]) if not a.get("isDelisted")}
            results = []
            for symbol, volume in hl_volume.items():
                token = self.get_canonical_token(symbol, public.HYPERLIQUID)
                hp = self.get_trading_pair_for_connector(token, public.HYPERLIQUID)
                bp = self.get_trading_pair_for_connector(token, public.BINANCE)
                # Do not guess symbol collisions: both resolved instruments must exist.
                if hp != symbol + "-USD" or (public.HYPERLIQUID,hp) not in metadata or (public.BINANCE,bp) not in metadata:
                    continue
                h, b = metadata[(public.HYPERLIQUID,hp)], metadata[(public.BINANCE,bp)]
                bvol = bn_volume.get(bp.replace("-", ""), Decimal("0"))
                if bvol < dec(self.config.auto_rotation_min_volume_24h_usd) or volume < Decimal("500000"):
                    continue
                ph = h["mark_price"] / dec(self.get_token_multiplier(token,public.HYPERLIQUID))
                pb = b["mark_price"] / dec(self.get_token_multiplier(token,public.BINANCE))
                if abs(ph-pb)/pb > min(self.config.max_allowed_price_divergence, self.config.max_entry_spread_pct):
                    continue
                diff = abs(h["rate"]/h["interval"] - b["rate"]/b["interval"])*86400
                results.append(dict(token=token,apr=float(diff*365*100),daily_diff=float(diff*100),bn_vol=float(bvol),hl_vol=float(volume)))
            return sorted(results,key=lambda r:(-r["apr"],r["token"]))
        except Exception as exc:
            self.logger().warning("[AUTO-ROTATION] Public schedule/metadata unavailable: %s",exc)
            return []


    # --- v1.5.0 Asset Graduation & Rehabilitation Engine ---
    def check_asset_graduation(self):
        """
        Periodically evaluate blacklisted tokens for asset graduation eligibility.
        """
        if not getattr(self.config, "asset_graduation_enabled", True):
            return
        if getattr(self, "is_evaluating_graduation", False):
            return
        now = self.current_timestamp
        interval_sec = float(getattr(self.config, "asset_graduation_interval_days", 7.0)) * 86400.0
        if not hasattr(self, "_last_graduation_check_ts") or self._last_graduation_check_ts == 0.0:
            # On startup, check after 120s if blacklisted tokens exist
            self._last_graduation_check_ts = now - interval_sec + 120.0
        if now - self._last_graduation_check_ts >= interval_sec:
            self._last_graduation_check_ts = now
            self.audit._spawn(self.async_run_asset_graduation(force=False))

    async def async_run_asset_graduation(self, force: bool = False):
        """
        Conduct liquidity & depth review on blacklisted tokens to assess graduation eligibility.
        Tokens meeting $30M+ volume and <=0.5% price divergence enter probationary probe testing.
        """
        if getattr(self, "is_evaluating_graduation", False):
            self.logger().info("[ASSET-GRADUATION] Graduation audit already running. Skipping duplicate call.")
            return
        self.is_evaluating_graduation = True
        try:
            candidates = list(self.token_permanent_blacklist)
            if not candidates:
                self.logger().info("[ASSET-GRADUATION] Permanent blacklist is empty. No candidates for graduation review.")
                return

            self.logger().info(f"[ASSET-GRADUATION] Starting graduation audit for {len(candidates)} blacklisted tokens: {candidates} (force={force})...")

            # Fetch market metrics across Binance and Hyperliquid
            metrics = await asyncio.to_thread(self._fetch_tokens_market_metrics, candidates)

            min_vol = float(getattr(self.config, "graduation_min_volume_24h_usd", 30000000.0))
            max_div = float(getattr(self.config, "graduation_max_price_divergence", 0.005))
            probe_size = float(getattr(self.config, "graduation_probe_size_quote", Decimal("200.0")))

            graduated_tokens = []
            for tok in candidates:
                data = metrics.get(tok)
                if not data:
                    self.logger().info(f"[ASSET-GRADUATION] Token '{tok}' not found on both Binance & Hyperliquid. Audit failed.")
                    continue

                b_vol = data.get("bn_vol", 0.0)
                h_vol = data.get("hl_vol", 0.0)
                total_vol = b_vol + h_vol
                div_pct = data.get("divergence", 1.0)

                has_vol = (total_vol >= min_vol)
                has_tight_div = (div_pct <= max_div)

                if has_vol and has_tight_div:
                    # Grant Probationary Testing Status!
                    self.token_permanent_blacklist.discard(tok)
                    self.token_quarantine_ledger.pop(tok, None)
                    self.probationary_tokens[tok] = {
                        "admitted_ts": int(self.current_timestamp),
                        "probe_size": probe_size,
                        "status": "PROBATIONARY_TESTING",
                        "reason": f"Matured: Vol ${total_vol/1e6:.1f}M >= ${min_vol/1e6:.1f}M, Div {div_pct:.3%} <= {max_div:.3%}",
                        "bn_vol": b_vol,
                        "hl_vol": h_vol,
                        "divergence": div_pct,
                        "probe_executed": False,
                    }
                    graduated_tokens.append(tok)
                    self.logger().warning(
                        f"🎓 [ASSET-GRADUATION-ADMIT] Token '{tok}' PASSED graduation review!\n"
                        f"  Metrics: Total 24h Vol=${total_vol/1e6:.2f}M (BN:${b_vol/1e6:.1f}M, HL:${h_vol/1e6:.1f}M) >= ${min_vol/1e6:.1f}M | Divergence={div_pct:.3%} <= {max_div:.3%}\n"
                        f"  Status: Admitted to Probationary Testing (Probe Size: ${probe_size:,.0f} @ 1x Leverage)."
                    )
                    # Mount token dynamically to strategy watchlist so it can be monitored
                    await self.async_add_token(tok)
                else:
                    self.logger().info(
                        f"[ASSET-GRADUATION-FAIL] Token '{tok}' failed graduation audit: "
                        f"Total Vol=${total_vol/1e6:.2f}M (Req >=${min_vol/1e6:.1f}M), "
                        f"Divergence={div_pct:.3%} (Req <={max_div:.3%}). Remains blacklisted."
                    )

            if graduated_tokens:
                self.logger().info(f"🎓 [ASSET-GRADUATION] Audit complete. Tokens admitted to probation: {graduated_tokens}")
            else:
                self.logger().info("[ASSET-GRADUATION] Audit complete. No tokens met the strict graduation criteria.")
        except Exception as e:
            self.logger().warning(f"[ASSET-GRADUATION] Error during graduation audit: {e}", exc_info=True)
        finally:
            self.is_evaluating_graduation = False

    def _fetch_tokens_market_metrics(self, tokens: List[str]) -> Dict[str, Dict]:
        """
        Fetch 24h volume and mark prices for specific candidate tokens across Binance and Hyperliquid.
        """
        metrics = {}
        try:
            # 1. Binance Ticker 24hr & PremiumIndex
            t_req = urllib.request.Request("https://fapi.binance.com/fapi/v1/ticker/24hr", headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(t_req, timeout=8) as r:
                bn_tickers = {x["symbol"][:-4]: float(x.get("quoteVolume", 0)) for x in json.loads(r.read()) if x.get("symbol", "").endswith("USDT")}

            p_req = urllib.request.Request("https://fapi.binance.com/fapi/v1/premiumIndex", headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(p_req, timeout=8) as r:
                bn_rates = {x["symbol"][:-4]: float(x.get("markPrice", 0)) for x in json.loads(r.read()) if x.get("symbol", "").endswith("USDT")}

            # 2. Hyperliquid metaAndAssetCtxs
            hl_req = urllib.request.Request(
                "https://api.hyperliquid.xyz/info",
                data=json.dumps({"type": "metaAndAssetCtxs"}).encode(),
                headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}
            )
            with urllib.request.urlopen(hl_req, timeout=8) as r:
                hl_data = json.loads(r.read())
                universe = hl_data[0].get("universe", [])
                ctxs = hl_data[1] if len(hl_data) > 1 else []
                hl_info = {}
                for m, c in zip(universe, ctxs):
                    coin = m.get("name")
                    if coin:
                        hl_info[coin] = {
                            "price": float(c.get("markPx", 0)),
                            "vol_usd": float(c.get("dayNtlVlm", 0))
                        }

            for tok in tokens:
                canon = self.get_canonical_token(tok).upper()
                bn_sym = self.get_exchange_symbol(canon, "binance_perpetual").replace("-USDT", "").replace("USDT", "")
                hl_sym = self.get_exchange_symbol(canon, "hyperliquid_perpetual").replace("-USDT", "").replace("USDT", "")

                b_vol = bn_tickers.get(bn_sym, bn_tickers.get(canon, 0.0))
                b_p = bn_rates.get(bn_sym, bn_rates.get(canon, 0.0))

                hl_item = hl_info.get(hl_sym, hl_info.get(canon))
                if hl_item and b_p > 0:
                    h_p = hl_item["price"]
                    h_vol = hl_item["vol_usd"]
                    # Apply multiplier normalization if necessary
                    m_bn = float(self.get_token_multiplier(canon, "binance_perpetual"))
                    m_hl = float(self.get_token_multiplier(canon, "hyperliquid_perpetual"))
                    norm_b_p = b_p / m_bn if m_bn > 0 else b_p
                    norm_h_p = h_p / m_hl if m_hl > 0 else h_p
                    div_pct = abs(norm_h_p - norm_b_p) / norm_b_p if norm_b_p > 0 else 1.0

                    metrics[tok] = {
                        "bn_vol": b_vol,
                        "hl_vol": h_vol,
                        "divergence": div_pct,
                        "bn_price": norm_b_p,
                        "hl_price": norm_h_p,
                    }
        except Exception as e:
            self.logger().debug(f"[ASSET-GRADUATION] Failed fetching market metrics: {e}")
        return metrics

    def _handle_probation_result(self, token: str, net_pnl: float, close_reason: str):
        """
        Process probationary testing outcome upon position closure:
        1. Graduate only when net PnL after all modelled costs is positive.
        2. If net PnL is negative -> Return to Permanent Blacklist!
        """
        if token in self.probationary_tokens:
            is_success = net_pnl > 0
            del self.probationary_tokens[token]
            if is_success:
                self.token_permanent_blacklist.discard(token)
                self.token_quarantine_ledger.pop(token, None)
                self.logger().warning(
                    f"🎓 [ASSET-GRADUATION-SUCCESS] Token '{token}' successfully completed probationary probe test "
                    f"with Net PnL ${net_pnl:+.4f} ({close_reason})! Formally graduated back to normal asset universe!"
                )
            else:
                self.token_permanent_blacklist.add(token)
                self.token_quarantine_ledger[token] = {
                    "quarantine_until": self.current_timestamp + 86400 * 3650,
                    "reason": f"Probation Failed: Net Loss (${net_pnl:.2f})",
                    "consecutive_losses": 1,
                    "loss_amount": net_pnl,
                    "triggered_at": self.current_timestamp,
                }
                self.logger().warning(
                    f"🚫 [ASSET-PROBATION-FAILED] Token '{token}' failed probationary probe trial "
                    f"with loss ${net_pnl:+.4f} ({close_reason}). Re-assigned to permanent toxic blacklist."
                )

    def publish_arbitrage_performance(self):
        self.audit.publish()


    def apply_initial_setting(self):
        if not self.config.dry_run:
            raise RuntimeError("Live funding execution requires paired-fill reconciliation; use audited paper mode.")



    def get_funding_info_by_token(self, token):
        return self.audit.funding_info(token)


    def get_safe_quote_price(self, connector_name, trading_pair, quote_volume, is_buy, fallback_price=None):
        return self.audit.quote(connector_name, trading_pair, buy=is_buy, notional=quote_volume).price


    def get_current_profitability_after_fees(self, token, connector_1, connector_2, side, quote_volume=None):
        position = self.audit.entry_position(token, connector_1, connector_2, side,
                                             quote_volume or self.config.position_size_quote,
                                             self.get_funding_info_by_token(token))
        return position["entry_profitability_after_fees"]


    def get_most_profitable_combination(self, funding_info_report: Dict):
        best_combination = None
        highest_profitability = 0
        for connector_1 in sorted(funding_info_report):
            for connector_2 in sorted(funding_info_report):
                if connector_1 != connector_2:
                    rate_connector_1 = self.get_normalized_funding_rate_in_seconds(funding_info_report, connector_1)
                    rate_connector_2 = self.get_normalized_funding_rate_in_seconds(funding_info_report, connector_2)
                    funding_rate_diff = abs(rate_connector_1 - rate_connector_2) * self.funding_profitability_interval
                    if funding_rate_diff > highest_profitability:
                        trade_side = TradeType.BUY if rate_connector_1 < rate_connector_2 else TradeType.SELL
                        highest_profitability = funding_rate_diff
                        best_combination = (connector_1, connector_2, trade_side, funding_rate_diff)
        return best_combination

    def get_normalized_funding_rate_in_seconds(self, funding_info_report, connector_name):
        info = funding_info_report[connector_name]
        return dec(info.rate) / dec(info.interval, positive=True)


    def check_virtual_funding_payments(self):
        self.audit.refresh()


    def calculate_dynamic_position_size(self, token, connector_1, connector_2, side):
        return self.audit.capacity(token, connector_1, connector_2, side, self.config.position_size_quote)


    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        return self.audit.create()


    def _evaluate_token_quarantine(self, token: str, net_loss: float):
        """
        Evaluate if a closed token should be quarantined due to severe loss or consecutive losses,
        or permanently blacklisted if cumulative historical net loss exceeds threshold.
        """
        hard_blacklist_usd = float(getattr(self.config, "cumulative_loss_hard_blacklist_usd", 30.0))
        loss_thresh = float(getattr(self.config, "single_loss_quarantine_threshold", Decimal("15.0")))
        quarantine_h = float(getattr(self.config, "token_quarantine_hours", Decimal("24.0")))

        # Partial tranches of one position are one outcome for loss streaks.
        grouped = {}
        for index, row in enumerate(self.stopped_funding_arbitrages.get(token, [])):
            key = row.get("position_id") or f"legacy:{index}"
            grouped[key] = grouped.get(key, 0.0) + float(row.get("total_net_pnl", 0))
        past_trades = [{"total_net_pnl": pnl} for pnl in grouped.values()]
        cum_net_pnl = sum(float(tr.get("total_net_pnl", 0)) for tr in past_trades)

        # 1. Module 3: Permanent Hard Blacklist (毒币终生累计亏损熔断)
        if cum_net_pnl <= -hard_blacklist_usd:
            self.token_permanent_blacklist.add(token)
            self.token_quarantine_ledger[token] = {
                "quarantine_until": self.current_timestamp + 86400 * 3650,  # 10 years (permanent)
                "reason": f"Permanent Blacklist: Cumulative Deficit (${cum_net_pnl:.2f} <= -${hard_blacklist_usd:.2f})",
                "consecutive_losses": len(past_trades),
                "triggered_at": self.current_timestamp,
                "loss_amount": cum_net_pnl,
            }
            self.logger().error(
                f"🚫 [HARD-BLACKLIST-TRIGGER] Token '{token}' permanently blacklisted! "
                f"Cumulative historical net deficit: ${cum_net_pnl:.2f} (Threshold: -${hard_blacklist_usd:.2f}). "
                f"Strategy will never enter this token again."
            )
            return

        # 2. Standard Quarantine for severe or consecutive losses
        consecutive_losses = 0
        for tr in reversed(past_trades):
            if float(tr.get("total_net_pnl", 0)) < 0:
                consecutive_losses += 1
            else:
                break

        should_quarantine = False
        q_reason = ""
        if net_loss < -loss_thresh:
            should_quarantine = True
            q_reason = f"Single Heavy Loss (${net_loss:.2f} < -${loss_thresh:.2f})"
        elif consecutive_losses >= 2:
            should_quarantine = True
            q_reason = f"Consecutive Losses ({consecutive_losses} trades in a row)"

        if should_quarantine:
            self.token_quarantine_ledger[token] = {
                "quarantine_until": self.current_timestamp + quarantine_h * 3600,
                "reason": q_reason,
                "consecutive_losses": consecutive_losses,
                "triggered_at": self.current_timestamp,
                "loss_amount": net_loss,
            }
            self.logger().warning(
                f"🚨 [QUARANTINE-TRIGGER] Token '{token}' quarantined for {quarantine_h:.0f}h! "
                f"Reason: {q_reason}."
            )

    def evaluate_liquidation_defense(self, token, funding_arbitrage_info):
        return self.audit.liquidation(token, funding_arbitrage_info)


    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        return self.audit.stop()


    def did_complete_funding_payment(self, funding_payment_completed_event):
        # Account events must never be mixed with paper history estimates.
        self.logger().warning("Ignored private funding event in paper-only strategy.")


    def get_position_executors_config(self, *args, **kwargs):
        raise RuntimeError("Live execution is unavailable until paired fills and unwind are reconciled.")


    def format_status(self) -> str:
        report = self.audit.last_report
        if report is None:
            return "Audited funding paper: waiting for validated public market data."
        account = report["paper_account"]
        return (f"Audited funding PAPER (no real orders) | positions={len(report['active_positions'])} "
                f"| equity={account['equity']} | net={report['performance']['global_pnl_quote']:.6f}\n"
                f"Entry block: {self.audit.blocked_reason or 'none'}\n"
                f"Rejections: {dict(self.audit.rejections)}\n"
                "Funding uses confirmed public rates; reference-price estimates are labelled. "
                "Liquidation is an illustrative model, not a venue guarantee.")
