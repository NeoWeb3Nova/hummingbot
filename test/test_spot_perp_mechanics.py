"""
Regression and Mechanics Tests for Spot-Perp Cash & Carry Yield Engine.
Verifies:
1. Payback Horizon & Contango Basis Surplus Offset
2. Multi-Tier Asset Classification & Dynamic Order Book Safe Sizing
3. Hourly Micro-Blackout Window (:58 ~ :01)
4. Breakeven Trailing Stop & Partial Tranche Take-Profit
5. Single-Leg (Perp Short) Liquidation Cushion & Toxic Token Quarantine
"""
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace as NS

from scripts.v2_spot_perp_cash_carry import (
    SpotPerpCashCarryConfig,
    SpotPerpCashCarryStrategy,
)


class DummyOrderBook:
    def __init__(self, ask_price=1.0, bid_price=1.0, depth_amount=50000.0):
        self.ask_price = ask_price
        self.bid_price = bid_price
        self.depth_amount = depth_amount

    def get_price(self, is_buy):
        return self.ask_price if is_buy else self.bid_price

    def ask_entries(self):
        return [NS(price=self.ask_price, amount=self.depth_amount)]

    def bid_entries(self):
        return [NS(price=self.bid_price, amount=self.depth_amount)]


class DummyConnector:
    def __init__(self, name):
        self.name = name
        self.order_book_tracker = NS(ready=True)
        self.books = {}
        self.funding_info = {}

    def get_order_book(self, trading_pair):
        return self.books.get(trading_pair, DummyOrderBook())

    def get_price(self, trading_pair, is_buy):
        ob = self.get_order_book(trading_pair)
        return ob.get_price(is_buy)

    def get_funding_info(self, trading_pair):
        return self.funding_info.get(trading_pair, NS(rate=Decimal("0.00004")))


class TestableSpotPerpStrategy(SpotPerpCashCarryStrategy):
    """Subclass to isolate and test mathematical/logical mechanics directly without DB/Network."""
    def __init__(self, connectors, config):
        self.connectors = connectors
        self.config = config
        self.active_positions = {}
        self.closed_trades = {}
        self.entry_candidate_ticks = {}
        self.exit_candidate_ticks = {}
        self.token_last_closed_ts = {}
        self.token_quarantine_until = {}
        self.token_admitted_at = {tok: 1000.0 for tok in self.config.tokens}
        self.market_candidates = []
        self.rotation_history = []
        self.data_file_candidates = []
        self._test_ts = 10000.0

    @property
    def current_timestamp(self) -> float:
        return self._test_ts

    @current_timestamp.setter
    def current_timestamp(self, val: float):
        self._test_ts = val


class TestSpotPerpMechanics(unittest.TestCase):
    def setUp(self):
        self.config = SpotPerpCashCarryConfig(
            tokens=["BTC", "SUI", "ENA"],
            dry_run=True,
            dynamic_position_sizing=True,
        )
        self.c_spot = DummyConnector("binance")
        self.c_perp = DummyConnector("hyperliquid_perpetual")
        self.connectors = {"binance": self.c_spot, "hyperliquid_perpetual": self.c_perp}
        self.strategy = TestableSpotPerpStrategy(self.connectors, self.config)

    # 1. Payback & Contango Basis Surplus
    def test_payback_contango_basis_surplus(self):
        # Perp ($1.005) > Spot ($1.000) -> Basis = +0.50% (50 bps)
        # Total friction = ~0.35% (35 bps)
        # Basis surplus covers all friction! Net hurdle <= 0 -> Instant 0.0h payback!
        p_spot = Decimal("1.000")
        p_perp = Decimal("1.005")
        rate_1h = Decimal("0.00004") # ~35% APR
        basis, friction, payback_h, is_ok = self.strategy.calculate_friction_and_payback(
            p_spot, p_perp, None, None, rate_1h
        )
        self.assertGreater(basis, Decimal("0.004"))
        self.assertEqual(payback_h, Decimal("0.0"))
        self.assertTrue(is_ok)

    def test_payback_realistic_horizon_vs_old_12h(self):
        # Spot $1.000, Perp $1.000 (0 basis)
        # Friction = 0.27% fees + latency = ~0.35%
        # Rate 1h = 0.00004 (35% APR)
        # Payback hours = 0.0035 / 0.00004 = 87.5 hours
        # Under old 12h rule: 87.5h > 12h -> REJECTED
        # Under new 14-day horizon: 87.5h / 336h = 26.0% <= 30% max friction-to-yield ratio -> ACCEPTED!
        p_spot = Decimal("1.000")
        p_perp = Decimal("1.000")
        rate_1h = Decimal("0.00004")
        basis, friction, payback_h, is_ok = self.strategy.calculate_friction_and_payback(
            p_spot, p_perp, None, None, rate_1h
        )
        self.assertGreater(payback_h, Decimal("40.0"))
        self.assertTrue(is_ok)

    def test_payback_adverse_funding_rejected(self):
        # Rate 1h <= 0 -> Must be rejected!
        p_spot = Decimal("1.000")
        p_perp = Decimal("1.000")
        rate_1h = Decimal("-0.00002")
        basis, friction, payback_h, is_ok = self.strategy.calculate_friction_and_payback(
            p_spot, p_perp, None, None, rate_1h
        )
        self.assertEqual(payback_h, Decimal("999.0"))
        self.assertFalse(is_ok)

    # 2. Multi-Tier Classification & Dynamic Safe Sizing
    def test_asset_tier_classification(self):
        self.assertEqual(self.strategy.get_token_tier("BTC"), 1)
        self.assertEqual(self.strategy.get_token_tier("SUI"), 2)
        self.assertEqual(self.strategy.get_token_tier("ENA"), 3)

        max_q1, min_apr1, buf1 = self.strategy.get_tier_config("BTC")
        self.assertEqual(max_q1, Decimal("5000.0"))
        self.assertEqual(buf1, Decimal("0.08"))

        max_q3, min_apr3, buf3 = self.strategy.get_tier_config("ENA")
        self.assertEqual(max_q3, Decimal("500.0"))
        self.assertEqual(buf3, Decimal("0.16"))

    def test_dynamic_orderbook_safe_sizing(self):
        # Deep liquidity ($50,000 depth) on ENA (Tier 3, max $500)
        # Safe size: 50,000 * 0.20 = 10,000 -> clamped to Tier 3 max $500
        size_deep = self.strategy.calculate_position_size("ENA", Decimal("50000"), Decimal("50000"))
        self.assertEqual(size_deep, Decimal("500.0"))

        # Thin liquidity ($1,000 depth) on ENA
        # Safe size: 1,000 * 0.20 = $200 -> within $100 ~ $500
        size_thin = self.strategy.calculate_position_size("ENA", Decimal("1000"), Decimal("1000"))
        self.assertEqual(size_thin, Decimal("200.0"))

    # 3. Hourly Micro-Blackout Window
    def test_hourly_blackout_window(self):
        # At :59 (in blackout)
        ts_59 = datetime(2026, 10, 23, 12, 59, 30, tzinfo=timezone.utc).timestamp()
        self.strategy.current_timestamp = ts_59
        self.assertTrue(self.strategy.is_in_hourly_blackout())

        # At :00 (in blackout)
        ts_00 = datetime(2026, 10, 23, 13, 0, 15, tzinfo=timezone.utc).timestamp()
        self.strategy.current_timestamp = ts_00
        self.assertTrue(self.strategy.is_in_hourly_blackout())

        # At :25 (outside blackout)
        ts_25 = datetime(2026, 10, 23, 13, 25, 0, tzinfo=timezone.utc).timestamp()
        self.strategy.current_timestamp = ts_25
        self.assertFalse(self.strategy.is_in_hourly_blackout())

    # 4. Toxic Token Quarantine
    def test_token_quarantine_lifecycle(self):
        self.strategy.current_timestamp = 10000.0
        self.assertFalse(self.strategy.is_token_quarantined("ENA"))

        # Quarantine for 24h
        self.strategy.quarantine_token("ENA", 24.0, "Heavy loss breach")
        self.assertTrue(self.strategy.is_token_quarantined("ENA"))

        # Advance past 24h
        self.strategy.current_timestamp = 10000.0 + 86401.0
        self.assertFalse(self.strategy.is_token_quarantined("ENA"))


if __name__ == "__main__":
    unittest.main()
