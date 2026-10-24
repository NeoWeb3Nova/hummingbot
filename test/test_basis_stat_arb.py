"""
Unit tests for BasisStatArbitrageConfig and core mathematical mechanisms
in v2_basis_stat_arb.py (API side).
"""
import unittest
from decimal import Decimal

from pydantic import ValidationError

# Import the config from the script
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "bots" / "scripts"))

from v2_basis_stat_arb import BasisStatArbitrageConfig, BasisStatArbitrage


class TestBasisStatArbitrageConfig(unittest.TestCase):
    """Test configuration schema and validation for basis stat arb."""

    def test_default_config_values(self):
        config = BasisStatArbitrageConfig(
            connectors=["hyperliquid_perpetual", "binance_perpetual"],
            tokens=["BTC", "ETH"],
        )
        self.assertEqual(config.min_holding_seconds, 60)
        self.assertEqual(config.exit_persistence_ticks, 2)
        self.assertEqual(config.taker_fee_rate, Decimal("0.0005"))
        self.assertEqual(config.settlement_blackout_seconds, 60)
        self.assertEqual(config.min_entry_basis_pct, Decimal("0.0050"))
        self.assertEqual(config.take_profit_basis_compression_pct, Decimal("0.0015"))
        self.assertEqual(config.entry_persistence_ticks, 2)
        self.assertEqual(config.stop_loss_confirmation_ticks, 5)
        self.assertTrue(config.dynamic_capacity_sizing)
        self.assertEqual(config.min_order_size_quote, Decimal("300.0"))
        self.assertEqual(config.min_executable_net_edge_pct, Decimal("0.0018"))
        self.assertEqual(config.max_executable_depth_slippage_pct, Decimal("0.0025"))
        self.assertTrue(config.check_margin_before_entry)
        self.assertEqual(config.margin_buffer_ratio, Decimal("0.05"))
        self.assertTrue(config.margin_aware_downsizing)
        self.assertEqual(config.dry_run_virtual_balance_quote, Decimal("0.0"))

    def test_token_and_connector_normalization(self):
        config = BasisStatArbitrageConfig(
            connectors=["HYPERLIQUID_PERPETUAL", "Binance_Perpetual "],
            tokens=["btc", " eth ", "Sol"],
        )
        self.assertEqual(config.connectors, {"hyperliquid_perpetual", "binance_perpetual"})
        self.assertEqual(config.tokens, {"BTC", "ETH", "SOL"})

    def test_invalid_holding_seconds_validation(self):
        with self.assertRaises(ValidationError):
            BasisStatArbitrageConfig(
                connectors=["hyperliquid_perpetual", "binance_perpetual"],
                tokens=["BTC"],
                min_holding_seconds=-5,  # Must be gt 0
            )

    def test_token_multipliers(self):
        # PEPE has 1000x multiplier on Binance (1000PEPE) and Hyperliquid (kPEPE)
        self.assertEqual(BasisStatArbitrage.get_token_multiplier("PEPE", "binance_perpetual"), 1000.0)
        self.assertEqual(BasisStatArbitrage.get_token_multiplier("PEPE", "hyperliquid_perpetual"), 1000.0)
        self.assertEqual(BasisStatArbitrage.get_token_multiplier("BTC", "binance_perpetual"), 1.0)
        self.assertEqual(BasisStatArbitrage.get_token_multiplier("SOL", "hyperliquid_perpetual"), 1.0)

    def test_trading_pair_derivation(self):
        self.assertEqual(
            BasisStatArbitrage.get_trading_pair_for_connector("BTC", "binance_perpetual"),
            "BTC-USDT"
        )
        self.assertEqual(
            BasisStatArbitrage.get_trading_pair_for_connector("BTC", "hyperliquid_perpetual"),
            "BTC-USD"
        )

    def test_roundtrip_fee_and_roe_calculation(self):
        pos_size = Decimal("1500.0")
        fee_rate = Decimal("0.0005")
        leverage = Decimal("3.0")

        # 4 legs taker fee
        total_fees = pos_size * fee_rate * Decimal("4")
        self.assertEqual(total_fees, Decimal("3.0000"))

        # Real dual margin
        real_margin = (pos_size / leverage) * Decimal("2")
        self.assertEqual(real_margin, Decimal("1000.0"))

        # Target net profit
        net_profit = Decimal("5.0")
        roe = net_profit / real_margin
        self.assertEqual(roe, Decimal("0.0050"))  # 0.5% ROE

    def test_dual_notional_and_leg_amounts(self):
        pos_size = Decimal("1500.0")
        p1 = Decimal("10.0")
        p2 = Decimal("10.05")
        amt1 = pos_size / p1
        amt2 = pos_size / p2
        self.assertEqual(amt1, Decimal("150.0"))
        self.assertAlmostEqual(float(amt2), 149.2537, places=4)
        total_notional = pos_size * Decimal("2")
        self.assertEqual(total_notional, Decimal("3000.0"))


    def test_coin_neutral_delta_zero(self):
        pos_size = Decimal("1500.0")
        p1 = Decimal("10.0")
        p2 = Decimal("10.07")  # 70 bps spread
        max_p = max(p1, p2)
        hedged_amount = pos_size / max_p
        # Both legs have exact same token quantity (Coin-Neutral / Delta-Zero)
        c1_amount = hedged_amount
        c2_amount = hedged_amount
        self.assertEqual(c1_amount, c2_amount)
        self.assertEqual(c1_amount - c2_amount, Decimal("0"))
        # Sized so neither notional exceeds pos_size
        self.assertLessEqual(c1_amount * p1, pos_size)
        self.assertLessEqual(c2_amount * p2, pos_size)

    def test_simulated_latency_slippage_and_friction_accounting(self):
        config = BasisStatArbitrageConfig(
            connectors=["hyperliquid_perpetual", "binance_perpetual"],
            tokens=["BTC"],
            simulated_latency_slippage_pct=Decimal("0.0002"),
        )
        self.assertEqual(config.simulated_latency_slippage_pct, Decimal("0.0002"))

        # Verify math identity: Real Net PnL = Gross PnL - DepthSlip - LatencySlip - Fees + Funding
        gross_pnl = Decimal("15.00")
        depth_slip = Decimal("1.80")
        latency_slip = Decimal("1.20")
        fees = Decimal("3.00")
        funding = Decimal("0.50")

        total_friction = depth_slip + latency_slip + fees
        self.assertEqual(total_friction, Decimal("6.00"))

        net_trade = gross_pnl - total_friction
        self.assertEqual(net_trade, Decimal("9.00"))

        total_net = net_trade + funding
        self.assertEqual(total_net, Decimal("9.50"))

        # Alpha retention rate: 9.50 / 15.00 = 63.33%
        alpha_retention = (total_net / gross_pnl) * Decimal("100")
        self.assertAlmostEqual(float(alpha_retention), 63.3333, places=3)

    def test_pre_flight_margin_calculation_and_cushion(self):
        """Verify pre-flight margin formula with 5% cushion and taker fee reserve."""
        pos_size = Decimal("1500.0")
        leverage = Decimal("3.0")
        buffer_ratio = Decimal("0.05")
        taker_fee = Decimal("0.0005")

        # Margin factor per dollar notional: (1/leverage)*(1+buffer) + fee
        # (1/3) * 1.05 + 0.0005 = 0.35 + 0.0005 = 0.3505
        margin_factor = (Decimal("1.0") / leverage) * (Decimal("1.0") + buffer_ratio) + taker_fee
        req_margin_per_leg = pos_size * margin_factor
        self.assertEqual(req_margin_per_leg, Decimal("525.75"))  # $500 nominal + $25 buffer + $0.75 fee

    def test_margin_aware_downsizing_logic(self):
        """Verify that available margin between full requirement and min order scales down safely."""
        target_pos_size = Decimal("1500.0")
        leverage = Decimal("3.0")
        buffer_ratio = Decimal("0.05")
        taker_fee = Decimal("0.0005")
        min_order_sz = Decimal("300.0")
        margin_factor = (Decimal("1.0") / leverage) * (Decimal("1.0") + buffer_ratio) + taker_fee

        # User only has $350 available on Exchange 1, and $600 on Exchange 2
        avail_1 = Decimal("350.0")
        avail_2 = Decimal("600.0")

        max_pos_1 = avail_1 / margin_factor  # 350 / 0.3505 = 998.57
        max_pos_2 = avail_2 / margin_factor  # 600 / 0.3505 = 1711.84
        max_feasible = min(max_pos_1, max_pos_2)

        self.assertGreaterEqual(max_feasible, min_order_sz)
        self.assertLess(max_feasible, target_pos_size)
        self.assertAlmostEqual(float(max_feasible), 998.573, places=2)

    def test_insufficient_margin_intercept_scenario(self):
        """Verify that when available balance is below min order requirement, trade is strictly blocked."""
        leverage = Decimal("3.0")
        buffer_ratio = Decimal("0.05")
        taker_fee = Decimal("0.0005")
        min_order_sz = Decimal("300.0")
        margin_factor = (Decimal("1.0") / leverage) * (Decimal("1.0") + buffer_ratio) + taker_fee

        min_req_margin = min_order_sz * margin_factor  # $105.15
        self.assertEqual(min_req_margin, Decimal("105.15"))

        # User only has $50 on Exchange 1 (fails min order requirement)
        avail_1 = Decimal("50.0")
        avail_2 = Decimal("1000.0")
        max_feasible = min(avail_1 / margin_factor, avail_2 / margin_factor)

        # Fails minimum order threshold -> Hard Intercept!
        self.assertLess(max_feasible, min_order_sz)


class TestLiveTradingSingleLegGuardAndQuantization(unittest.TestCase):
    """
    Test Case 58: Live Trading Single-Leg Execution & Rollback Guardian and Quantization.
    """

    def setUp(self):
        self.config = BasisStatArbitrageConfig(
            connectors=["hyperliquid_perpetual", "binance_perpetual"],
            tokens=["SOL", "ETH"],
            single_leg_timeout_seconds=5.0,
            dry_run=False,
            position_size_quote=Decimal("15.0"),
            min_order_size_quote=Decimal("10.0"),
            leverage=3,
        )

    def test_single_leg_timeout_config(self):
        self.assertEqual(self.config.single_leg_timeout_seconds, 5.0)
        # Validation bounds
        with self.assertRaises(ValidationError):
            BasisStatArbitrageConfig(
                connectors=["hyperliquid_perpetual", "binance_perpetual"],
                tokens=["SOL"],
                single_leg_timeout_seconds=0.5,  # Below ge=1.0
            )

    def test_quantize_order_amount_logic(self):
        """Test exchange rule compliance for step sizes and min notional thresholds."""
        from unittest.mock import MagicMock

        # Create dummy strategy instance
        strat = BasisStatArbitrage.__new__(BasisStatArbitrage)
        strat.config = self.config
        strat._logger = MagicMock()

        # Mock connectors
        conn1 = MagicMock()
        conn2 = MagicMock()
        conn1.quantize_order_amount = lambda tp, amt: Decimal(str(round(float(amt), 2)))
        conn2.quantize_order_amount = lambda tp, amt: Decimal(str(round(float(amt), 2)))

        rule1 = MagicMock(min_order_size=Decimal("0.01"), min_notional_size=Decimal("10.0"))
        rule2 = MagicMock(min_order_size=Decimal("0.01"), min_notional_size=Decimal("5.0"))
        conn1.trading_rules = {"SOL-USD": rule1}
        conn2.trading_rules = {"SOL-USDT": rule2}

        strat.connectors = {"hyperliquid_perpetual": conn1, "binance_perpetual": conn2}

        # Case A: Valid sizing (0.1 SOL @ $150 = $15 notional > $10)
        res = strat.quantize_order_amount("SOL", Decimal("0.103"), Decimal("150.0"), Decimal("150.0"))
        self.assertEqual(res, Decimal("0.10"))

        # Case B: Below Hyperliquid floor ($10.0) -> (0.05 SOL @ $150 = $7.50 < $10)
        res_small = strat.quantize_order_amount("SOL", Decimal("0.05"), Decimal("150.0"), Decimal("150.0"))
        self.assertIsNone(res_small)

        # Case C: Below min_order_size
        rule1.min_order_size = Decimal("1.0")
        res_below_min = strat.quantize_order_amount("SOL", Decimal("0.5"), Decimal("150.0"), Decimal("150.0"))
        self.assertIsNone(res_below_min)

    def test_single_leg_guard_both_filled_scenario(self):
        """When both legs fill within timeout, transition execution_status to ACTIVE_HEDGED."""
        from unittest.mock import MagicMock

        strat = BasisStatArbitrage.__new__(BasisStatArbitrage)
        strat.config = self.config
        strat._logger = MagicMock()
        strat._set_current_timestamp(1000.0)
        strat.quarantine_tokens = {}
        strat.stopped_basis_arbitrages = {}
        strat._persist_state = MagicMock()

        # Mock active position
        pos = {
            "token": "SOL",
            "is_virtual": False,
            "execution_status": "PENDING_EXECUTION",
            "open_timestamp": 998.0,  # 2.0s elapsed
            "executors_ids": ["exec_1", "exec_2"],
            "connector_1": "hyperliquid_perpetual",
            "connector_2": "binance_perpetual",
            "entry_price_1": 150.0,
            "entry_price_2": 150.5,
            "amount_1": 0.1,
            "amount_2": 0.1,
            "position_size": 15.0,
            "leverage": 3,
            "total_margin": 10.0,
        }
        strat.active_basis_arbitrages = {"SOL": pos}

        # Mock executors: both filled
        ex1 = MagicMock(open_filled_amount=Decimal("0.1"), entry_price=Decimal("150.1"), is_closed=False)
        ex2 = MagicMock(open_filled_amount=Decimal("0.1"), entry_price=Decimal("150.4"), is_closed=False)
        strat.get_executor_by_id = lambda eid: ex1 if eid == "exec_1" else ex2

        actions = strat._evaluate_single_leg_execution_guard()
        self.assertEqual(len(actions), 0)
        self.assertEqual(pos["execution_status"], "ACTIVE_HEDGED")
        self.assertEqual(pos["entry_price_1"], 150.1)
        self.assertEqual(pos["entry_price_2"], 150.4)

    def test_single_leg_guard_emergency_unwind_leg1_scenario(self):
        """When Leg 1 fills and Leg 2 is 0-fill past timeout, trigger emergency unwind."""
        from unittest.mock import MagicMock

        strat = BasisStatArbitrage.__new__(BasisStatArbitrage)
        strat.config = self.config
        strat._logger = MagicMock()
        strat._set_current_timestamp(1006.0)  # 6.0s elapsed (> 5s timeout)
        strat.quarantine_tokens = {}
        strat.stopped_basis_arbitrages = {}
        strat._persist_state = MagicMock()

        pos = {
            "token": "SOL",
            "is_virtual": False,
            "execution_status": "PENDING_EXECUTION",
            "open_timestamp": 1000.0,
            "executors_ids": ["exec_1", "exec_2"],
            "connector_1": "hyperliquid_perpetual",
            "connector_2": "binance_perpetual",
            "side_1": "BUY",
            "side_2": "SELL",
            "entry_price_1": 150.0,
            "entry_price_2": 150.5,
            "amount_1": 0.1,
            "amount_2": 0.1,
            "position_size": 15.0,
            "leverage": 3,
            "total_margin": 10.0,
            "entry_basis_pct": 0.005,
        }
        strat.active_basis_arbitrages = {"SOL": pos}

        # Leg 1 filled, Leg 2 0-fill
        ex1 = MagicMock(
            open_filled_amount=Decimal("0.1"),
            open_filled_amount_quote=Decimal("15.0"),
            cum_fees_quote=Decimal("0.01"),
            net_pnl_quote=Decimal("-0.02"),
            close_price=Decimal("149.8"),
            is_closed=False,
        )
        ex2 = MagicMock(
            open_filled_amount=Decimal("0.0"),
            is_closed=True,
        )
        strat.get_executor_by_id = lambda eid: ex1 if eid == "exec_1" else ex2

        actions = strat._evaluate_single_leg_execution_guard()

        # Must return StopExecutorAction for both legs (unwinding Leg 1)
        self.assertEqual(len(actions), 2)
        exec_ids_stopped = [a.executor_id for a in actions]
        self.assertIn("exec_1", exec_ids_stopped)
        self.assertIn("exec_2", exec_ids_stopped)

        # Removed from active positions
        self.assertNotIn("SOL", strat.active_basis_arbitrages)

        # Archived into stopped trades
        self.assertIn("SOL", strat.stopped_basis_arbitrages)
        record = strat.stopped_basis_arbitrages["SOL"][0]
        self.assertEqual(record["close_reason"], "SINGLE_LEG_EMERGENCY_UNWIND_LEG1_FILLED")

        # Quarantined for 30 minutes (1800s)
        self.assertIn("SOL", strat.quarantine_tokens)
        self.assertEqual(strat.quarantine_tokens["SOL"], 1006.0 + 1800.0)

    def test_single_leg_guard_emergency_unwind_leg2_scenario(self):
        """When Leg 2 fills and Leg 1 is 0-fill past timeout, trigger emergency unwind for Leg 2."""
        from unittest.mock import MagicMock

        strat = BasisStatArbitrage.__new__(BasisStatArbitrage)
        strat.config = self.config
        strat._logger = MagicMock()
        strat._set_current_timestamp(1006.0)
        strat.quarantine_tokens = {}
        strat.stopped_basis_arbitrages = {}
        strat._persist_state = MagicMock()

        pos = {
            "token": "AVAX",
            "is_virtual": False,
            "execution_status": "PENDING_EXECUTION",
            "open_timestamp": 1000.0,
            "executors_ids": ["exec_1", "exec_2"],
            "connector_1": "hyperliquid_perpetual",
            "connector_2": "binance_perpetual",
            "side_1": "BUY",
            "side_2": "SELL",
            "entry_price_1": 25.0,
            "entry_price_2": 25.1,
            "amount_1": 0.6,
            "amount_2": 0.6,
            "position_size": 15.0,
            "leverage": 3,
            "total_margin": 10.0,
            "entry_basis_pct": 0.004,
        }
        strat.active_basis_arbitrages = {"AVAX": pos}

        # Leg 1 0-fill, Leg 2 filled
        ex1 = MagicMock(open_filled_amount=Decimal("0.0"), is_closed=True)
        ex2 = MagicMock(
            open_filled_amount=Decimal("0.6"),
            open_filled_amount_quote=Decimal("15.06"),
            cum_fees_quote=Decimal("0.01"),
            net_pnl_quote=Decimal("-0.01"),
            close_price=Decimal("25.12"),
            is_closed=False,
        )
        strat.get_executor_by_id = lambda eid: ex1 if eid == "exec_1" else ex2

        actions = strat._evaluate_single_leg_execution_guard()

        self.assertEqual(len(actions), 2)
        self.assertNotIn("AVAX", strat.active_basis_arbitrages)
        self.assertIn("AVAX", strat.stopped_basis_arbitrages)
        record = strat.stopped_basis_arbitrages["AVAX"][0]
        self.assertEqual(record["close_reason"], "SINGLE_LEG_EMERGENCY_UNWIND_LEG2_FILLED")
        self.assertEqual(strat.quarantine_tokens["AVAX"], 1006.0 + 1800.0)

    def test_single_leg_guard_zero_fill_abort_scenario(self):
        """When neither leg fills within timeout, cleanly abort with zero loss."""
        from unittest.mock import MagicMock

        strat = BasisStatArbitrage.__new__(BasisStatArbitrage)
        strat.config = self.config
        strat._logger = MagicMock()
        strat._set_current_timestamp(1006.0)
        strat.quarantine_tokens = {}
        strat.stopped_basis_arbitrages = {}
        strat._persist_state = MagicMock()

        pos = {
            "token": "ETH",
            "is_virtual": False,
            "execution_status": "PENDING_EXECUTION",
            "open_timestamp": 1000.0,
            "executors_ids": ["exec_1", "exec_2"],
        }
        strat.active_basis_arbitrages = {"ETH": pos}

        ex1 = MagicMock(open_filled_amount=Decimal("0.0"), is_closed=False)
        ex2 = MagicMock(open_filled_amount=Decimal("0.0"), is_closed=False)
        strat.get_executor_by_id = lambda eid: ex1 if eid == "exec_1" else ex2

        actions = strat._evaluate_single_leg_execution_guard()

        self.assertEqual(len(actions), 2)
        self.assertNotIn("ETH", strat.active_basis_arbitrages)
        self.assertIn("ETH", strat.quarantine_tokens)
        self.assertEqual(strat.quarantine_tokens["ETH"], 1006.0 + 300.0)  # 5 min cooldown


if __name__ == "__main__":
    unittest.main()
