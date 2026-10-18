import os
import time
from decimal import Decimal
from typing import Dict, List, Set

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


class FundingRateArbitrageConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    dry_run: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": lambda mi: "Run in dry-run simulation mode without real orders? (True/False): ",
            "prompt_on_new": True,
        }
    )
    leverage: int = Field(
        default=20, gt=0,
        json_schema_extra={"prompt": lambda mi: "Enter the leverage (e.g. 20): ", "prompt_on_new": True},
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
        default=0.01,
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

    @field_validator("connectors", "tokens", mode="before")
    @classmethod
    def validate_sets(cls, v):
        if isinstance(v, str):
            return set(v.split(","))
        return v

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

    @classmethod
    def get_trading_pair_for_connector(cls, token, connector):
        return f"{token}-{cls.quote_markets_map.get(connector, 'USDT')}"

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
            cls.logger().error(f"Failed to patch ConnectorManager for dry-run: {e}")

    def __init__(self, connectors: Dict[str, ConnectorBase], config: FundingRateArbitrageConfig):
        super().__init__(connectors, config)
        self.config = config
        self.active_funding_arbitrages = {}
        self.stopped_funding_arbitrages = {token: [] for token in self.config.tokens}

    def start(self, clock: Clock, timestamp: float) -> None:
        """
        Start the strategy.
        :param clock: Clock to use.
        :param timestamp: Current time.
        """
        super().start(clock, timestamp)
        self._last_timestamp = timestamp
        self.apply_initial_setting()

        # Patch Hyperliquid WebSocket funding rate bug if needed
        try:
            from hummingbot.connector.derivative.hyperliquid_perpetual.hyperliquid_perpetual_api_order_book_data_source import HyperliquidPerpetualAPIOrderBookDataSource
            if not getattr(HyperliquidPerpetualAPIOrderBookDataSource, "_funding_rate_patch_applied", False):
                _orig_parse = HyperliquidPerpetualAPIOrderBookDataSource._parse_funding_info_message
                async def _patched_parse(ds_self, raw_message, message_queue):
                    try:
                        data = raw_message.get("data", {})
                        ctx = data.get("ctx", data)
                        if isinstance(ctx, dict) and "funding" in ctx and "openInterest" in ctx:
                            oi = ctx.pop("openInterest", None)
                            try:
                                return await _orig_parse(ds_self, raw_message, message_queue)
                            finally:
                                if oi is not None:
                                    ctx["openInterest"] = oi
                    except Exception:
                        pass
                    return await _orig_parse(ds_self, raw_message, message_queue)
                HyperliquidPerpetualAPIOrderBookDataSource._parse_funding_info_message = _patched_parse
                HyperliquidPerpetualAPIOrderBookDataSource._funding_rate_patch_applied = True
                self.logger().info("[PATCH] Applied Hyperliquid funding rate WebSocket patch.")
        except Exception as e:
            self.logger().debug(f"Could not patch Hyperliquid data source: {e}")

        try:
            from hummingbot.client.hummingbot_application import HummingbotApplication
            app = HummingbotApplication.main_application()
            if app and getattr(app, "_mqtt", None) is not None:
                from hummingbot.remote_iface.mqtt import ETopicPublisher
                self.mqtt_enabled = True
                self._pub = ETopicPublisher("performance", use_bot_prefix=True)
                self.logger().info("[DRY-RUN] Initialized MQTT publisher for live UI performance stream.")
        except Exception as e:
            self.logger().debug(f"MQTT publisher not initialized: {e}")

    def on_tick(self):
        if self.ready_to_trade and self.market_data_provider.ready and not getattr(self, "_is_stop_triggered", False):
            actions = self.determine_executor_actions()
            for action in actions:
                self.executor_orchestrator.execute_action(action)
            self.publish_arbitrage_performance()

    def publish_arbitrage_performance(self):
        """
        Aggregate and publish real-time arbitrage performance and trade details via MQTT and local disk.
        """
        now = self.current_timestamp
        active_list = []
        total_unrealized_pnl = Decimal("0")
        total_funding_pnl = Decimal("0")
        total_volume = Decimal("0")

        for token, arb in self.active_funding_arbitrages.items():
            funding_income = sum(Decimal(str(p.get("amount", 0))) for p in arb.get("funding_payments", []))
            total_funding_pnl += funding_income
            pos_size = Decimal(str(arb.get("position_size_quote", self.config.position_size_quote)))
            total_volume += pos_size * 2

            p1_curr = Decimal(str(arb.get("price_1", 0)))
            p2_curr = Decimal(str(arb.get("price_2", 0)))
            unrealized_trading_pnl = Decimal("0")

            try:
                trading_pair_1 = self.get_trading_pair_for_connector(token, arb["connector_1"])
                trading_pair_2 = self.get_trading_pair_for_connector(token, arb["connector_2"])
                p1_curr = Decimal(str(self.market_data_provider.get_price_by_type(
                    connector_name=arb["connector_1"],
                    trading_pair=trading_pair_1,
                    price_type=PriceType.MidPrice
                )))
                p2_curr = Decimal(str(self.market_data_provider.get_price_by_type(
                    connector_name=arb["connector_2"],
                    trading_pair=trading_pair_2,
                    price_type=PriceType.MidPrice
                )))
                amt1 = arb.get("amount_1", pos_size / p1_curr)
                amt2 = arb.get("amount_2", pos_size / p2_curr)
                p1_open = arb.get("price_1", p1_curr)
                p2_open = arb.get("price_2", p2_curr)

                leg1_pnl = (p1_curr - p1_open) * amt1 if arb["side"] == TradeType.BUY else (p1_open - p1_curr) * amt1
                leg2_pnl = (p2_open - p2_curr) * amt2 if arb["side"] == TradeType.BUY else (p2_curr - p2_open) * amt2
                unrealized_trading_pnl = leg1_pnl + leg2_pnl
            except Exception:
                pass

            total_unrealized_pnl += unrealized_trading_pnl
            net_position_pnl = unrealized_trading_pnl + funding_income
            elapsed_sec = max(0, now - arb.get("open_timestamp", now))

            active_list.append({
                "token": token,
                "connector_1": arb["connector_1"],
                "connector_2": arb["connector_2"],
                "side": arb["side"].name if hasattr(arb["side"], "name") else str(arb["side"]),
                "entry_price_1": float(arb.get("price_1", 0)),
                "entry_price_2": float(arb.get("price_2", 0)),
                "current_price_1": float(p1_curr),
                "current_price_2": float(p2_curr),
                "position_size": float(pos_size),
                "funding_income": float(funding_income),
                "settlement_count": len(arb.get("funding_payments", [])),
                "unrealized_trading_pnl": float(unrealized_trading_pnl),
                "net_pnl": float(net_position_pnl),
                "net_pnl_pct": float(net_position_pnl / pos_size) if pos_size else 0.0,
                "duration_seconds": int(elapsed_sec),
                "next_funding_1": int(arb.get("next_funding_time_1", 0)),
                "next_funding_2": int(arb.get("next_funding_time_2", 0)),
            })

        # Closed trades
        closed_list = []
        total_realized_pnl = Decimal("0")
        tp_count = 0
        sl_count = 0

        for token, pos_list in self.stopped_funding_arbitrages.items():
            for pos in pos_list:
                t_pnl = Decimal(str(pos.get("trading_pnl", 0)))
                f_pnl = Decimal(str(pos.get("funding_pnl", 0)))
                tot_pnl = Decimal(str(pos.get("total_net_pnl", t_pnl + f_pnl)))
                total_realized_pnl += tot_pnl
                reason = pos.get("close_reason", "Completed")
                if "Take Profit" in reason:
                    tp_count += 1
                elif "Stop Loss" in reason:
                    sl_count += 1
                closed_list.append({
                    "token": token,
                    "connector_1": pos.get("connector_1", ""),
                    "connector_2": pos.get("connector_2", ""),
                    "close_reason": reason,
                    "close_timestamp": int(pos.get("close_timestamp", 0)),
                    "open_timestamp": int(pos.get("open_timestamp", 0)),
                    "trading_pnl": float(t_pnl),
                    "funding_pnl": float(f_pnl),
                    "total_net_pnl": float(tot_pnl),
                })

        # Live Market Comparison across watched tokens
        market_list = []
        try:
            for token in self.config.tokens:
                f_report = self.get_funding_info_by_token(token)
                best_comb = self.get_most_profitable_combination(f_report)
                row = {"token": token}
                for c_name, c_info in f_report.items():
                    norm_rate = self.get_normalized_funding_rate_in_seconds(f_report, c_name) * self.funding_profitability_interval * 100
                    row[f"{c_name}_rate_daily"] = float(norm_rate)
                    row[f"{c_name}_next_funding"] = int(c_info.next_funding_utc_timestamp)
                if best_comb:
                    c1, c2, side, diff = best_comb
                    row["best_path"] = f"{c1} -> {c2}"
                    row["daily_rate_diff_pct"] = float(diff * 100)
                market_list.append(row)
        except Exception:
            pass

        global_pnl = total_realized_pnl + total_unrealized_pnl + total_funding_pnl

        report = {
            "funding_rate_arb": {
                "controller_name": "funding_rate_arb",
                "trading_pair": ",".join(self.config.tokens),
                "connector_name": ",".join(self.config.connectors),
                "performance": {
                    "realized_pnl_quote": float(total_realized_pnl),
                    "unrealized_pnl_quote": float(total_unrealized_pnl + total_funding_pnl),
                    "global_pnl_quote": float(global_pnl),
                    "volume_traded": float(total_volume),
                    "close_type_counts": {
                        "CloseType.TAKE_PROFIT": tp_count,
                        "CloseType.STOP_LOSS": sl_count,
                    }
                },
                "custom_info": {
                    "is_dry_run": bool(self.config.dry_run),
                    "active_positions": active_list,
                    "closed_trades": closed_list,
                    "market_comparison": market_list,
                }
            }
        }

        # 1. Publish via MQTT if available
        if getattr(self, "_pub", None):
            try:
                self._pub(report)
            except Exception as e:
                self.logger().debug(f"Failed to publish arbitrage performance via MQTT: {e}")

        # 2. Persist local JSON copy to data directory for instant UI read
        try:
            import json
            data_dir = "/home/hummingbot/data"
            if not os.path.isdir(data_dir):
                cfg = getattr(self, "client_config_map", None)
                data_dir = getattr(cfg, "data_file_path", None) if cfg else None
            if data_dir and os.path.isdir(data_dir):
                json_path = os.path.join(data_dir, "arbitrage_trades.json")
                with open(json_path, "w") as f:
                    json.dump(report["funding_rate_arb"]["custom_info"], f, indent=2)
        except Exception as e:
            self.logger().debug(f"Failed to persist arbitrage_trades.json: {e}")

    def apply_initial_setting(self):
        if self.config.dry_run:
            self.logger().info("[DRY-RUN] Dry run mode active: skipping exchange leverage and position mode API calls.")
            return

        for connector_name, connector in self.connectors.items():
            if self.is_perpetual(connector_name):
                position_mode = PositionMode.ONEWAY if connector_name == "hyperliquid_perpetual" else PositionMode.HEDGE
                connector.set_position_mode(position_mode)
                for trading_pair in self.market_data_provider.get_trading_pairs(connector_name):
                    connector.set_leverage(trading_pair, self.config.leverage)

    def get_funding_info_by_token(self, token):
        """
        This method provides the funding rates across all the connectors
        """
        funding_rates = {}
        for connector_name, connector in self.connectors.items():
            trading_pair = self.get_trading_pair_for_connector(token, connector_name)
            funding_rates[connector_name] = connector.get_funding_info(trading_pair)
        return funding_rates

    def get_current_profitability_after_fees(self, token: str, connector_1: str, connector_2: str, side: TradeType):
        """
        This methods compares the profitability of buying at market in the two exchanges. If the side is TradeType.BUY
        means that the operation is long on connector 1 and short on connector 2.
        """
        trading_pair_1 = self.get_trading_pair_for_connector(token, connector_1)
        trading_pair_2 = self.get_trading_pair_for_connector(token, connector_2)

        connector_1_price = Decimal(self.market_data_provider.get_price_for_quote_volume(
            connector_name=connector_1,
            trading_pair=trading_pair_1,
            quote_volume=self.config.position_size_quote,
            is_buy=side == TradeType.BUY,
        ).result_price)
        connector_2_price = Decimal(self.market_data_provider.get_price_for_quote_volume(
            connector_name=connector_2,
            trading_pair=trading_pair_2,
            quote_volume=self.config.position_size_quote,
            is_buy=side != TradeType.BUY,
        ).result_price)
        estimated_fees_connector_1 = self.connectors[connector_1].get_fee(
            base_currency=trading_pair_1.split("-")[0],
            quote_currency=trading_pair_1.split("-")[1],
            order_type=OrderType.MARKET,
            order_side=TradeType.BUY,
            amount=self.config.position_size_quote / connector_1_price,
            price=connector_1_price,
            is_maker=False,
            position_action=PositionAction.OPEN
        ).percent
        estimated_fees_connector_2 = self.connectors[connector_2].get_fee(
            base_currency=trading_pair_2.split("-")[0],
            quote_currency=trading_pair_2.split("-")[1],
            order_type=OrderType.MARKET,
            order_side=TradeType.BUY,
            amount=self.config.position_size_quote / connector_2_price,
            price=connector_2_price,
            is_maker=False,
            position_action=PositionAction.OPEN
        ).percent

        if side == TradeType.BUY:
            estimated_trade_pnl_pct = (connector_2_price - connector_1_price) / connector_1_price
        else:
            estimated_trade_pnl_pct = (connector_1_price - connector_2_price) / connector_2_price
        return estimated_trade_pnl_pct - estimated_fees_connector_1 - estimated_fees_connector_2

    def get_most_profitable_combination(self, funding_info_report: Dict):
        best_combination = None
        highest_profitability = 0
        for connector_1 in funding_info_report:
            for connector_2 in funding_info_report:
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
        return funding_info_report[connector_name].rate / self.funding_payment_interval_map.get(connector_name, 60 * 60 * 8)

    def check_virtual_funding_payments(self):
        """
        Check and simulate funding payments for active virtual positions when in dry-run mode.
        """
        now = self.current_timestamp
        for token, arb in list(self.active_funding_arbitrages.items()):
            if not arb.get("is_virtual"):
                continue

            funding_info_report = self.get_funding_info_by_token(token)
            legs = [
                ("connector_1", "next_funding_time_1", arb["side"]),
                ("connector_2", "next_funding_time_2", TradeType.SELL if arb["side"] == TradeType.BUY else TradeType.BUY)
            ]

            for conn_key, time_key, side in legs:
                conn_name = arb[conn_key]
                next_time = arb.get(time_key, 0)
                if now >= next_time and next_time > 0:
                    current_info = funding_info_report[conn_name]
                    rate = current_info.rate
                    # Long pays if rate > 0, receives if rate < 0 -> payment = - rate * quote_size
                    # Short receives if rate > 0, pays if rate < 0 -> payment = + rate * quote_size
                    payment_amount = -1 * arb["position_size_quote"] * rate if side == TradeType.BUY else arb["position_size_quote"] * rate

                    arb["funding_payments"].append({
                        "timestamp": now,
                        "connector": conn_name,
                        "side": side.name,
                        "rate": rate,
                        "amount": payment_amount,
                    })

                    interval = self.funding_payment_interval_map.get(conn_name, 60 * 60 * 8)
                    if current_info.next_funding_utc_timestamp > now:
                        arb[time_key] = current_info.next_funding_utc_timestamp
                    else:
                        arb[time_key] = next_time + interval

                    self.logger().info(
                        f"[DRY-RUN] Simulated Funding Payment on {conn_name} for {token} ({side.name}): "
                        f"Rate={rate:+.6%}, Settlement={payment_amount:+.4f} Quote"
                    )

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        """
        Evaluate if a new set of positions has to be created for each token without active arbitrage.
        In dry-run mode, record virtual positions without dispatching real CreateExecutorActions.
        """
        create_actions = []
        for token in self.config.tokens:
            if token not in self.active_funding_arbitrages:
                funding_info_report = self.get_funding_info_by_token(token)
                best_combination = self.get_most_profitable_combination(funding_info_report)
                if not best_combination:
                    continue

                connector_1, connector_2, trade_side, expected_profitability = best_combination
                if expected_profitability >= self.config.min_funding_rate_profitability:
                    current_profitability = self.get_current_profitability_after_fees(
                        token, connector_1, connector_2, trade_side
                    )
                    if self.config.trade_profitability_condition_to_enter:
                        if current_profitability < 0:
                            self.logger().info(
                                f"Best Combination: {connector_1} | {connector_2} | {trade_side} "
                                f"Funding rate profitability: {expected_profitability} "
                                f"Trading profitability after fees: {current_profitability} "
                                f"Trade profitability is negative, skipping..."
                            )
                            continue

                    self.logger().info(
                        f"Best Combination: {connector_1} | {connector_2} | {trade_side} "
                        f"Funding rate profitability: {expected_profitability:.4%} "
                        f"Trading profitability after fees: {current_profitability:.4%}"
                    )

                    if self.config.dry_run:
                        # Open Virtual Position in Sandbox
                        trading_pair_1 = self.get_trading_pair_for_connector(token, connector_1)
                        trading_pair_2 = self.get_trading_pair_for_connector(token, connector_2)
                        p1 = Decimal(str(self.market_data_provider.get_price_for_quote_volume(
                            connector_name=connector_1,
                            trading_pair=trading_pair_1,
                            quote_volume=self.config.position_size_quote,
                            is_buy=trade_side == TradeType.BUY,
                        ).result_price))
                        p2 = Decimal(str(self.market_data_provider.get_price_for_quote_volume(
                            connector_name=connector_2,
                            trading_pair=trading_pair_2,
                            quote_volume=self.config.position_size_quote,
                            is_buy=trade_side != TradeType.BUY,
                        ).result_price))
                        fee_pct_1 = self.connectors[connector_1].get_fee(
                            base_currency=trading_pair_1.split("-")[0],
                            quote_currency=trading_pair_1.split("-")[1],
                            order_type=OrderType.MARKET,
                            order_side=TradeType.BUY,
                            amount=self.config.position_size_quote / p1,
                            price=p1,
                            is_maker=False,
                            position_action=PositionAction.OPEN
                        ).percent
                        fee_pct_2 = self.connectors[connector_2].get_fee(
                            base_currency=trading_pair_2.split("-")[0],
                            quote_currency=trading_pair_2.split("-")[1],
                            order_type=OrderType.MARKET,
                            order_side=TradeType.BUY,
                            amount=self.config.position_size_quote / p2,
                            price=p2,
                            is_maker=False,
                            position_action=PositionAction.OPEN
                        ).percent

                        self.active_funding_arbitrages[token] = {
                            "is_virtual": True,
                            "connector_1": connector_1,
                            "connector_2": connector_2,
                            "side": trade_side,
                            "open_timestamp": self.current_timestamp,
                            "price_1": p1,
                            "price_2": p2,
                            "amount_1": self.config.position_size_quote / p1,
                            "amount_2": self.config.position_size_quote / p2,
                            "position_size_quote": self.config.position_size_quote,
                            "entry_fee_quote": (fee_pct_1 + fee_pct_2) * self.config.position_size_quote,
                            "expected_profitability": expected_profitability,
                            "entry_profitability_after_fees": current_profitability,
                            "funding_payments": [],
                            "next_funding_time_1": funding_info_report[connector_1].next_funding_utc_timestamp,
                            "next_funding_time_2": funding_info_report[connector_2].next_funding_utc_timestamp,
                            "executors_ids": ["DRY_RUN_SANDBOX"],
                        }
                        self.logger().info(
                            f"[DRY-RUN] Virtual Arbitrage Position Established for {token}!\n"
                            f"  Leg 1 ({connector_1}): {'BUY' if trade_side == TradeType.BUY else 'SELL'} @ {p1:.4f}\n"
                            f"  Leg 2 ({connector_2}): {'SELL' if trade_side == TradeType.BUY else 'BUY'} @ {p2:.4f}\n"
                            f"  Daily Rate Diff: {expected_profitability:.4%}, Net Entry Spread: {current_profitability:.4%}"
                        )
                    else:
                        # Real Live Trading Executors
                        self.logger().info("Starting live executors...")
                        position_executor_config_1, position_executor_config_2 = self.get_position_executors_config(
                            token, connector_1, connector_2, trade_side
                        )
                        self.active_funding_arbitrages[token] = {
                            "is_virtual": False,
                            "connector_1": connector_1,
                            "connector_2": connector_2,
                            "executors_ids": [position_executor_config_1.id, position_executor_config_2.id],
                            "side": trade_side,
                            "funding_payments": [],
                        }
                        return [CreateExecutorAction(executor_config=position_executor_config_1),
                                CreateExecutorAction(executor_config=position_executor_config_2)]
        return create_actions

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        """
        Monitor active arbitrages for take-profit or stop-loss conditions.
        """
        # First update any virtual funding payments
        if self.config.dry_run:
            self.check_virtual_funding_payments()

        stop_executor_actions = []
        for token, funding_arbitrage_info in list(self.active_funding_arbitrages.items()):
            if funding_arbitrage_info.get("is_virtual"):
                # Handle Virtual Position Exit Logic
                trading_pair_1 = self.get_trading_pair_for_connector(token, funding_arbitrage_info["connector_1"])
                trading_pair_2 = self.get_trading_pair_for_connector(token, funding_arbitrage_info["connector_2"])
                p1_curr = Decimal(str(self.market_data_provider.get_price_for_quote_volume(
                    connector_name=funding_arbitrage_info["connector_1"],
                    trading_pair=trading_pair_1,
                    quote_volume=funding_arbitrage_info["position_size_quote"],
                    is_buy=funding_arbitrage_info["side"] != TradeType.BUY,
                ).result_price))
                p2_curr = Decimal(str(self.market_data_provider.get_price_for_quote_volume(
                    connector_name=funding_arbitrage_info["connector_2"],
                    trading_pair=trading_pair_2,
                    quote_volume=funding_arbitrage_info["position_size_quote"],
                    is_buy=funding_arbitrage_info["side"] == TradeType.BUY,
                ).result_price))

                amt1 = funding_arbitrage_info["amount_1"]
                p1_open = funding_arbitrage_info["price_1"]
                leg1_pnl = (p1_curr - p1_open) * amt1 if funding_arbitrage_info["side"] == TradeType.BUY else (p1_open - p1_curr) * amt1

                amt2 = funding_arbitrage_info["amount_2"]
                p2_open = funding_arbitrage_info["price_2"]
                leg2_pnl = (p2_open - p2_curr) * amt2 if funding_arbitrage_info["side"] == TradeType.BUY else (p2_curr - p2_open) * amt2

                exit_fee_quote = funding_arbitrage_info.get("entry_fee_quote", Decimal("0"))
                trading_pnl = leg1_pnl + leg2_pnl - exit_fee_quote
                funding_pnl = sum(p["amount"] for p in funding_arbitrage_info["funding_payments"])
                total_net_pnl = trading_pnl + funding_pnl
                total_pnl_pct = total_net_pnl / funding_arbitrage_info["position_size_quote"]

                take_profit_condition = total_net_pnl > self.config.profitability_to_take_profit * funding_arbitrage_info["position_size_quote"]

                funding_info_report = self.get_funding_info_by_token(token)
                rate1 = self.get_normalized_funding_rate_in_seconds(funding_info_report, funding_arbitrage_info["connector_1"])
                rate2 = self.get_normalized_funding_rate_in_seconds(funding_info_report, funding_arbitrage_info["connector_2"])
                if funding_arbitrage_info["side"] == TradeType.BUY:
                    funding_rate_diff = rate2 - rate1
                else:
                    funding_rate_diff = rate1 - rate2
                current_funding_condition = funding_rate_diff * self.funding_profitability_interval < self.config.funding_rate_diff_stop_loss

                if take_profit_condition or current_funding_condition:
                    reason = "Take Profit" if take_profit_condition else "Stop Loss"
                    self.logger().info(
                        f"[DRY-RUN] Virtual Arbitrage Closed ({reason}) for {token}!\n"
                        f"  Trading PnL: {trading_pnl:+.4f}, Funding PnL: {funding_pnl:+.4f}, Total Net PnL: {total_net_pnl:+.4f} ({total_pnl_pct:+.2%})"
                    )
                    funding_arbitrage_info["close_timestamp"] = self.current_timestamp
                    funding_arbitrage_info["trading_pnl"] = trading_pnl
                    funding_arbitrage_info["funding_pnl"] = funding_pnl
                    funding_arbitrage_info["total_net_pnl"] = total_net_pnl
                    funding_arbitrage_info["close_reason"] = reason
                    self.stopped_funding_arbitrages[token].append(funding_arbitrage_info)
                    del self.active_funding_arbitrages[token]
            else:
                # Live Trading Executors Stop Logic
                executors = self.filter_executors(
                    executors=self.get_all_executors(),
                    filter_func=lambda x: x.id in funding_arbitrage_info["executors_ids"]
                )
                funding_payments_pnl = sum(funding_payment.amount for funding_payment in funding_arbitrage_info["funding_payments"])
                executors_pnl = sum(executor.net_pnl_quote for executor in executors)
                take_profit_condition = executors_pnl + funding_payments_pnl > self.config.profitability_to_take_profit * self.config.position_size_quote
                funding_info_report = self.get_funding_info_by_token(token)
                if funding_arbitrage_info["side"] == TradeType.BUY:
                    funding_rate_diff = self.get_normalized_funding_rate_in_seconds(funding_info_report, funding_arbitrage_info["connector_2"]) - self.get_normalized_funding_rate_in_seconds(funding_info_report, funding_arbitrage_info["connector_1"])
                else:
                    funding_rate_diff = self.get_normalized_funding_rate_in_seconds(funding_info_report, funding_arbitrage_info["connector_1"]) - self.get_normalized_funding_rate_in_seconds(funding_info_report, funding_arbitrage_info["connector_2"])
                current_funding_condition = funding_rate_diff * self.funding_profitability_interval < self.config.funding_rate_diff_stop_loss
                if take_profit_condition:
                    self.logger().info("Take profit profitability reached, stopping executors")
                    self.stopped_funding_arbitrages[token].append(funding_arbitrage_info)
                    stop_executor_actions.extend([StopExecutorAction(executor_id=executor.id) for executor in executors])
                elif current_funding_condition:
                    self.logger().info("Funding rate difference reached for stop loss, stopping executors")
                    self.stopped_funding_arbitrages[token].append(funding_arbitrage_info)
                    stop_executor_actions.extend([StopExecutorAction(executor_id=executor.id) for executor in executors])
        return stop_executor_actions

    def did_complete_funding_payment(self, funding_payment_completed_event: FundingPaymentCompletedEvent):
        """
        Based on the funding payment event received (in live mode), check if one of the active arbitrages matches.
        """
        token = funding_payment_completed_event.trading_pair.split("-")[0]
        if token in self.active_funding_arbitrages:
            self.active_funding_arbitrages[token]["funding_payments"].append(funding_payment_completed_event)

    def get_position_executors_config(self, token, connector_1, connector_2, trade_side):
        price = self.market_data_provider.get_price_by_type(
            connector_name=connector_1,
            trading_pair=self.get_trading_pair_for_connector(token, connector_1),
            price_type=PriceType.MidPrice
        )
        position_amount = self.config.position_size_quote / price

        position_executor_config_1 = PositionExecutorConfig(
            timestamp=self.current_timestamp,
            connector_name=connector_1,
            trading_pair=self.get_trading_pair_for_connector(token, connector_1),
            side=trade_side,
            amount=position_amount,
            leverage=self.config.leverage,
            triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.MARKET),
        )
        position_executor_config_2 = PositionExecutorConfig(
            timestamp=self.current_timestamp,
            connector_name=connector_2,
            trading_pair=self.get_trading_pair_for_connector(token, connector_2),
            side=TradeType.BUY if trade_side == TradeType.SELL else TradeType.SELL,
            amount=position_amount,
            leverage=self.config.leverage,
            triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.MARKET),
        )
        return position_executor_config_1, position_executor_config_2

    def format_status(self) -> str:
        original_status = super().format_status()
        funding_rate_status = []
        mode_str = "DRY-RUN SIMULATION (Paper Trading - No Real Orders)" if self.config.dry_run else "LIVE TRADING (Real Orders Active)"
        funding_rate_status.append(f"\n==================== [ {mode_str} ] ====================")

        if self.ready_to_trade:
            all_funding_info = []
            all_best_paths = []
            for token in self.config.tokens:
                token_info = {"token": token}
                best_paths_info = {"token": token}
                funding_info_report = self.get_funding_info_by_token(token)
                best_combination = self.get_most_profitable_combination(funding_info_report)
                for connector_name, info in funding_info_report.items():
                    token_info[f"{connector_name} Rate (%)"] = self.get_normalized_funding_rate_in_seconds(funding_info_report, connector_name) * self.funding_profitability_interval * 100
                if best_combination:
                    connector_1, connector_2, side, funding_rate_diff = best_combination
                    profitability_after_fees = self.get_current_profitability_after_fees(token, connector_1, connector_2, side)
                    best_paths_info["Best Path"] = f"{connector_1}_{connector_2}"
                    best_paths_info["Rate Diff (%)"] = funding_rate_diff * 100
                    best_paths_info["Trade Net Spread (%)"] = profitability_after_fees * 100
                    best_paths_info["Days Trade Prof"] = (- profitability_after_fees / funding_rate_diff) if funding_rate_diff > 0 else 0
                    best_paths_info["Days to TP"] = ((self.config.profitability_to_take_profit - profitability_after_fees) / funding_rate_diff) if funding_rate_diff > 0 else 0

                    time_to_c1 = max(0, funding_info_report[connector_1].next_funding_utc_timestamp - self.current_timestamp)
                    time_to_c2 = max(0, funding_info_report[connector_2].next_funding_utc_timestamp - self.current_timestamp)
                    best_paths_info["Min to F1"] = time_to_c1 / 60
                    best_paths_info["Min to F2"] = time_to_c2 / 60

                all_funding_info.append(token_info)
                all_best_paths.append(best_paths_info)

            funding_rate_status.append(f"Min Funding Profitability: {self.config.min_funding_rate_profitability:.2%}")
            funding_rate_status.append(f"Take Profit Target: {self.config.profitability_to_take_profit:.2%}")
            funding_rate_status.append("Funding Rate Comparison (Daily Equivalent):")
            funding_rate_status.append(format_df_for_printout(df=pd.DataFrame(all_funding_info), table_format="psql"))
            funding_rate_status.append(format_df_for_printout(df=pd.DataFrame(all_best_paths), table_format="psql"))

            # Active Arbitrages status
            if self.active_funding_arbitrages:
                funding_rate_status.append("\n--- Active Arbitrage Positions ---")
                for token, arb in self.active_funding_arbitrages.items():
                    long_c = arb["connector_1"] if arb["side"] == TradeType.BUY else arb["connector_2"]
                    short_c = arb["connector_2"] if arb["side"] == TradeType.BUY else arb["connector_1"]
                    funding_rate_status.append(f"Token: {token} | Long: {long_c} | Short: {short_c}")
                    if arb.get("is_virtual"):
                        funding_pnl = sum(p["amount"] for p in arb.get("funding_payments", []))
                        elapsed_hours = (self.current_timestamp - arb.get("open_timestamp", self.current_timestamp)) / 3600
                        funding_rate_status.append(
                            f"  [Sandbox] Open Entry Prices: {arb['connector_1']}={arb.get('price_1', 0):.4f}, "
                            f"{arb['connector_2']}={arb.get('price_2', 0):.4f} | Size: {arb.get('position_size_quote')} Quote"
                        )
                        funding_rate_status.append(
                            f"  [Sandbox] Duration: {elapsed_hours:.1f}h | Collected Funding Income: {funding_pnl:+.4f} Quote "
                            f"({len(arb.get('funding_payments', []))} settlements)"
                        )
                    else:
                        funding_rate_status.append(f"  Funding Payments: {len(arb.get('funding_payments', []))}")
                        funding_rate_status.append(f"  Executors: {arb.get('executors_ids')}")
                    funding_rate_status.append("-" * 50)
            else:
                funding_rate_status.append("\nNo active arbitrage positions currently held.")

            # History of closed positions
            closed_count = sum(len(positions) for positions in self.stopped_funding_arbitrages.values())
            if closed_count > 0:
                funding_rate_status.append(f"\n--- Closed Arbitrage History ({closed_count} closed) ---")
                for token, pos_list in self.stopped_funding_arbitrages.items():
                    for pos in pos_list:
                        if pos.get("is_virtual"):
                            funding_rate_status.append(
                                f"Token: {token} | Reason: {pos.get('close_reason')} | "
                                f"Trading PnL: {pos.get('trading_pnl', 0):+.4f} | "
                                f"Funding PnL: {pos.get('funding_pnl', 0):+.4f} | "
                                f"Total Net: {pos.get('total_net_pnl', 0):+.4f}"
                            )

        return original_status + "\n" + "\n".join(funding_rate_status)
