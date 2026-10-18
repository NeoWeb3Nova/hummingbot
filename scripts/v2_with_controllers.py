import os
import time
from decimal import Decimal
from typing import Dict, List, Optional, Union

from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
)
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.xemm_executor.xemm_executor import XEMMExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.models.executors import TrackedOrder

# Compatibility patch for PaperTradeExchange (which doesn't implement in_flight_orders or _order_tracker)
_paper_trade_orders: Dict[str, InFlightOrder] = {}


def _safe_get_in_flight_order(self, connector_name: str, order_id: str):
    connector = self.connectors.get(connector_name)
    if not connector:
        return _paper_trade_orders.get(order_id)

    # 1. Try real connector's _order_tracker
    try:
        if hasattr(connector, "_order_tracker") and connector._order_tracker is not None:
            order = connector._order_tracker.fetch_order(client_order_id=order_id)
            if order is not None:
                return order
    except Exception:
        pass

    # 2. Try real connector's in_flight_orders property safely (avoiding NotImplementedError)
    try:
        in_flight = getattr(connector, "in_flight_orders", None)
        if in_flight is not None:
            order = in_flight.get(order_id)
            if order is not None:
                return order
    except Exception:
        pass

    # 3. Check cached paper trade orders
    if order_id in _paper_trade_orders:
        return _paper_trade_orders[order_id]

    # 4. Check paper trade connector's limit_orders
    try:
        if hasattr(connector, "limit_orders"):
            for lo in connector.limit_orders:
                if lo.client_order_id == order_id:
                    created_ts = float(lo.creation_timestamp)
                    if created_ts > 1e11:
                        created_ts = created_ts / 1e6
                    order = InFlightOrder(
                        client_order_id=lo.client_order_id,
                        trading_pair=lo.trading_pair,
                        order_type=OrderType.LIMIT,
                        trade_type=TradeType.BUY if lo.is_buy else TradeType.SELL,
                        amount=Decimal(str(lo.quantity)),
                        creation_timestamp=created_ts,
                        price=Decimal(str(lo.price)),
                        initial_state=OrderState.OPEN,
                    )
                    _paper_trade_orders[order_id] = order
                    return order
    except Exception:
        pass

    # 5. Create a placeholder InFlightOrder for paper trade if not found
    order = InFlightOrder(
        client_order_id=order_id,
        trading_pair="",
        order_type=OrderType.MARKET,
        trade_type=TradeType.BUY,
        amount=Decimal("0"),
        creation_timestamp=time.time(),
        price=Decimal("0"),
        initial_state=OrderState.OPEN,
    )
    _paper_trade_orders[order_id] = order
    return order


ExecutorBase.get_in_flight_order = _safe_get_in_flight_order

# Patch TrackedOrder so properties safely fall back to _paper_trade_orders
_orig_tracked_is_done = TrackedOrder.is_done
_orig_tracked_is_open = TrackedOrder.is_open
_orig_tracked_price = TrackedOrder.price


def _safe_tracked_order_is_done(self):
    if self.order:
        return self.order.is_done
    if self.order_id and self.order_id in _paper_trade_orders:
        return _paper_trade_orders[self.order_id].is_done
    return False


def _safe_tracked_order_is_open(self):
    if self.order:
        return self.order.is_open
    if self.order_id and self.order_id in _paper_trade_orders:
        return _paper_trade_orders[self.order_id].is_open
    return False


def _safe_tracked_order_price(self):
    if self.order:
        return self.order.price
    if self.order_id and self.order_id in _paper_trade_orders:
        return _paper_trade_orders[self.order_id].price
    return None


if not hasattr(TrackedOrder, "_is_safe_patched"):
    _orig_tracked_is_done = TrackedOrder.is_done
    _orig_tracked_is_open = TrackedOrder.is_open
    _orig_tracked_price = TrackedOrder.price

    TrackedOrder.is_done = property(_safe_tracked_order_is_done)
    TrackedOrder.is_open = property(_safe_tracked_order_is_open)
    TrackedOrder.price = property(_safe_tracked_order_price)
    TrackedOrder._is_safe_patched = True

# Proxy detection and aiohttp monkey patch for WebSocket / REST connections
import socket
from urllib.parse import urlparse
import aiohttp


def _find_working_proxy() -> Optional[str]:
    candidates = []
    for env_var in ["HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy"]:
        val = os.environ.get(env_var)
        if val:
            candidates.append(val)
    candidates.extend([
        "http://192.168.65.254:10808",
        "http://192.168.65.254:7890",
        "http://127.0.0.1:10808",
        "http://127.0.0.1:7890",
    ])
    for cand in candidates:
        try:
            parsed = urlparse(cand)
            host = parsed.hostname
            port = parsed.port or 80
            if host and port:
                s = socket.create_connection((host, port), timeout=0.3)
                s.close()
                return cand
        except Exception:
            continue
    return None


_active_proxy = _find_working_proxy()
if _active_proxy:
    os.environ["HTTP_PROXY"] = _active_proxy
    os.environ["HTTPS_PROXY"] = _active_proxy

if not hasattr(aiohttp.ClientSession, "_raw_ws_connect"):
    aiohttp.ClientSession._raw_init = aiohttp.ClientSession.__init__
    aiohttp.ClientSession._raw_ws_connect = aiohttp.ClientSession.ws_connect

    def _safe_aiohttp_session_init(self, *args, **kwargs):
        if "trust_env" not in kwargs:
            kwargs["trust_env"] = True
        return aiohttp.ClientSession._raw_init(self, *args, **kwargs)

    def _safe_aiohttp_ws_connect(self, url, *args, **kwargs):
        if _active_proxy and "proxy" not in kwargs:
            kwargs["proxy"] = _active_proxy
        if kwargs.get("max_msg_size") is None:
            kwargs["max_msg_size"] = 4 * 1024 * 1024
        return aiohttp.ClientSession._raw_ws_connect(self, url, *args, **kwargs)

    aiohttp.ClientSession.__init__ = _safe_aiohttp_session_init
    aiohttp.ClientSession.ws_connect = _safe_aiohttp_ws_connect




class V2WithControllersConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    max_global_drawdown_quote: Optional[float] = None
    max_controller_drawdown_quote: Optional[float] = None


class V2WithControllers(StrategyV2Base):
    """
    This script runs a generic strategy with cash out feature. Will also check if the controllers configs have been
    updated and apply the new settings.
    The cash out of the script can be set by the time_to_cash_out parameter in the config file. If set, the script will
    stop the controllers after the specified time has passed, and wait until the active executors finalize their
    execution.
    The controllers will also have a parameter to manually cash out. In that scenario, the main strategy will stop the
    specific controller and wait until the active executors finalize their execution. The rest of the executors will
    wait until the main strategy stops them.
    """
    performance_report_interval: int = 1

    def __init__(self, connectors: Dict[str, ConnectorBase], config: V2WithControllersConfig):
        super().__init__(connectors, config)
        self.config = config
        self.max_pnl_by_controller = {}
        self.max_global_pnl = Decimal("0")
        self.drawdown_exited_controllers = []
        self.closed_executors_buffer: int = 30
        self._last_performance_report_timestamp = 0

    def on_tick(self):
        super().on_tick()
        if not self._is_stop_triggered:
            self.check_manual_kill_switch()
            self.control_max_drawdown()
            self.send_performance_report()

    def control_max_drawdown(self):
        if self.config.max_controller_drawdown_quote:
            self.check_max_controller_drawdown()
        if self.config.max_global_drawdown_quote:
            self.check_max_global_drawdown()

    def check_max_controller_drawdown(self):
        for controller_id, controller in self.controllers.items():
            if controller.status != RunnableStatus.RUNNING:
                continue
            controller_pnl = self.get_performance_report(controller_id).global_pnl_quote
            last_max_pnl = self.max_pnl_by_controller[controller_id]
            if controller_pnl > last_max_pnl:
                self.max_pnl_by_controller[controller_id] = controller_pnl
            else:
                current_drawdown = last_max_pnl - controller_pnl
                if current_drawdown > self.config.max_controller_drawdown_quote:
                    self.logger().info(f"Controller {controller_id} reached max drawdown. Stopping the controller.")
                    controller.stop()
                    executors_order_placed = self.filter_executors(
                        executors=self.get_executors_by_controller(controller_id),
                        filter_func=lambda x: x.is_active and not x.is_trading,
                    )
                    self.executor_orchestrator.execute_actions(
                        actions=[StopExecutorAction(controller_id=controller_id, executor_id=executor.id) for executor in executors_order_placed]
                    )
                    self.drawdown_exited_controllers.append(controller_id)

    def check_max_global_drawdown(self):
        current_global_pnl = sum([self.get_performance_report(controller_id).global_pnl_quote for controller_id in self.controllers.keys()])
        if current_global_pnl > self.max_global_pnl:
            self.max_global_pnl = current_global_pnl
        else:
            current_global_drawdown = self.max_global_pnl - current_global_pnl
            if current_global_drawdown > self.config.max_global_drawdown_quote:
                self.drawdown_exited_controllers.extend(list(self.controllers.keys()))
                self.logger().info("Global drawdown reached. Stopping the strategy.")
                self._is_stop_triggered = True
                HummingbotApplication.main_application().stop()

    def get_controller_report(self, controller_id: str) -> dict:
        """
        Get the full report for a controller including performance and custom info.
        """
        performance_report = self.controller_reports.get(controller_id, {}).get("performance")
        return {
            "performance": performance_report.dict() if performance_report else {},
            "custom_info": self.controllers[controller_id].get_custom_info()
        }

    def send_performance_report(self):
        if self.current_timestamp - self._last_performance_report_timestamp >= self.performance_report_interval and self._pub:
            controller_reports = {controller_id: self.get_controller_report(controller_id) for controller_id in self.controllers.keys()}
            self._pub(controller_reports)
            self._last_performance_report_timestamp = self.current_timestamp

    def check_manual_kill_switch(self):
        for controller_id, controller in self.controllers.items():
            if controller.config.manual_kill_switch and controller.status == RunnableStatus.RUNNING:
                self.logger().info(f"Manual cash out for controller {controller_id}.")
                controller.stop()
                executors_to_stop = self.get_executors_by_controller(controller_id)
                self.executor_orchestrator.execute_actions(
                    [StopExecutorAction(executor_id=executor.id,
                                        controller_id=executor.controller_id) for executor in executors_to_stop])
            if not controller.config.manual_kill_switch and controller.status == RunnableStatus.TERMINATED:
                if controller_id in self.drawdown_exited_controllers:
                    continue
                self.logger().info(f"Restarting controller {controller_id}.")
                controller.start()

    def check_executors_status(self):
        active_executors = self.filter_executors(
            executors=self.get_all_executors(),
            filter_func=lambda executor: executor.status == RunnableStatus.RUNNING
        )
        if not active_executors:
            self.logger().info("All executors have finalized their execution. Stopping the strategy.")
            HummingbotApplication.main_application().stop()
        else:
            non_trading_executors = self.filter_executors(
                executors=active_executors,
                filter_func=lambda executor: not executor.is_trading
            )
            self.executor_orchestrator.execute_actions(
                [StopExecutorAction(executor_id=executor.id,
                                    controller_id=executor.controller_id) for executor in non_trading_executors])

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        return []

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        return []

    def apply_initial_setting(self):
        connectors_position_mode = {}
        for controller_id, controller in self.controllers.items():
            self.max_pnl_by_controller[controller_id] = Decimal("0")
            config_dict = controller.config.model_dump()
            if "connector_name" in config_dict:
                if self.is_perpetual(config_dict["connector_name"]):
                    if "position_mode" in config_dict:
                        connectors_position_mode[config_dict["connector_name"]] = config_dict["position_mode"]
                    if "leverage" in config_dict and "trading_pair" in config_dict:
                        self.connectors[config_dict["connector_name"]].set_leverage(
                            leverage=config_dict["leverage"],
                            trading_pair=config_dict["trading_pair"])
        for connector_name, position_mode in connectors_position_mode.items():
            self.connectors[connector_name].set_position_mode(position_mode)

    def did_fail_order(self, order_failed_event: MarketOrderFailureEvent):
        """
        Handle order failure events by logging the error and stopping the strategy if necessary.
        """
        if order_failed_event.error_message and "position side" in order_failed_event.error_message.lower():
            connectors_position_mode = {}
            for controller_id, controller in self.controllers.items():
                config_dict = controller.config.model_dump()
                if "connector_name" in config_dict:
                    if self.is_perpetual(config_dict["connector_name"]):
                        if "position_mode" in config_dict:
                            connectors_position_mode[config_dict["connector_name"]] = config_dict["position_mode"]
            for connector_name, position_mode in connectors_position_mode.items():
                self.connectors[connector_name].set_position_mode(position_mode)
