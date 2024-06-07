import threading
from queue import PriorityQueue
import time
from typing import Dict
import enum
import datetime
from python.utils.thread_util import IntervalThread, stop_thread
from blueprints.account.controllers import AccountController
from utils.redis_tools import RedisWrapper
from utils.finance_tools import stock_info_local_redis, stock_list_local_redis, stock_ticker_local_redis
from utils.market_tools import get_symbol_list, get_ticker


# ====================== enum and structs ======================#
class Side(enum):
    BUY = 1
    SELL = 2


class Order:
    def __init__(
            self,
            order_id,
            symbol,
            price,
            qty,
            side
    ):
        """
        Constructor
        """
        self.order_id = order_id
        self.symbol = symbol
        self.price = price
        self.qty = qty
        self.cum_qty = 0
        self.leaves_qty = qty
        self.side = side


class Trade:
    def __init__(
            self,
            order_id,
            symbol,
            trade_price,
            trade_qty,
            trade_side,
            trade_id
    ):
        """
        Constructor
        """
        self.order_id = order_id
        self.symbol = symbol
        self.trade_price = trade_price
        self.trade_qty = trade_qty
        self.trade_side = trade_side
        self.trade_id = trade_id


class OrderComparable:
    """
    Price priority and time priority principle
    - Buy orders with higher prices have priority in transactions, and sell orders with lower prices have priority in transactions.
    - Orders with earlier times have priority in transactions.
    """

    def __init__(self, order):
        self.order = order

    def __lt__(self, other):
        other_order = other.order
        if self.order.side != other_order.side:
            raise Exception("can only compare same side order")

        # Higher price priority for buyer side
        if self.order.side == 'buy':
            if self.order.order_price > other_order.order_price:
                return True
            elif self.order.order_price == other_order.order_price:
                if self.order.order_time <= other_order.order_time:
                    return True
                else:
                    return False
            else:
                return False

        # Lower price priority for seller side
        elif self.order.side == 'sell':
            if self.order.order_price < other_order.order_price:
                return False
            elif self.order.order_price == other_order.order_price:
                if self.order.order_time <= other_order.order_time:
                    return False
                else:
                    return True
            else:
                return True


class OrderBook:
    """
    {
        symbol 1: queue,
        symbol 2: queue
    }
    """

    def __init__(self, symbol):
        self.symbol = symbol
        self.asks = PriorityQueue()
        self.bids = PriorityQueue()
        self.order_id_mapping = {}

        self.build_order_book()

    def build_order_book(self):
        """
        Build Order Book
        """

    def add_order(self, order: Order):
        if order.order_id in self.order_id_mapping:
            return False
        self.order_id_mapping[order.order_id] = order

        if order.side == 'buy':
            self.bids.put(OrderComparable(order))
        elif order.side == 'sell':
            self.asks.put(OrderComparable(order))

        return True

    def pop_order(self, side):
        """
        从队列中取出优先级最高的element
        """
        if side == 'buy':
            if self.bids.empty():
                result = None
            else:
                result = self.bids.get().order
        elif side == 'sell':
            if self.asks.empty():
                result = None
            else:
                result = self.asks.get().order
        return result


class TradingEngine:
    """
    初始化及恢复：
    1. 获取所有订单，重建Orderbook

    在每一个tick内：
    1. 从当前买单和卖单中获取最优先的n个进行处理，价格符合才成交
    """

    LISTEN_CHANNEL = 'live_order'
    WRITE_CHANNEL = 'live_order_response'
    TRADE_INTERVAL = 0.5
    MONITOR_INTERVAL = 10
    TRADE_RESTART = 15

    def __init__(self, symbols=None, is_live=True):
        self._subscribe_order = None
        self.subscribe_order_thread = None
        self.orderbook_mapping: Dict[str, OrderBook] = {}
        self.ticker_mapping: Dict[str, float] = {}
        self.is_live = is_live

        if symbols == None:
            symbols = get_symbol_list()
        self.symbols = symbols

        # 从数据库中重建orderbook
        self._init_orderbook(symbols)

        self.rw = RedisWrapper()
        self.p = self.rw.subscribe(self.LISTEN_CHANNEL)
        self.last_trade_ts = time.time()

    def monitor(self):
        """
        监控整个引擎的运行情况
        """
        if time.time() - self.last_trade_ts > self.TRADE_RESTART:
            stop_thread(self.trade_thread)
            self.trade_thread = IntervalThread(
                interval=self.TRADE_INTERVAL, target=self._trade)
            self.trade_thread.start()

    def _init_orderbook(self, symbols):
        for symbol in symbols:
            self.orderbook_mapping[symbol] = OrderBook(symbol)

    def _process_order(self, data: Dict):
        # {
        #   'event': 'make' | 'cancel',
        #   'payload': {
        #     'symbol': '',
        #     'order_id': '',
        #     'order_time': '',
        #     'order_price': '',
        #     'amount': '',
        #     'status': ''
        #   }
        # }
        event = data['event']
        payload = data['payload']
        symbol = payload['symbol']
        order_id = payload['order_id']
        order_type = payload['order_type']

        if event == 'make':
            result = self.orderbook_mapping[symbol].add_order(Order(
                order_id=order_id,
                account_id=payload['account_id'],
                user_id=payload['user_id'],
                symbol=symbol,
                side=payload['side'],
                order_time=datetime.datetime.fromisoformat(
                    payload['order_time']),
                order_price=payload['order_price'],
                amount=payload['amount'],
                status=payload['status'],
                order_type=order_type
            ))
        return result

    def _update_ticker(self, new_ticker={}):
        if new_ticker == {}:
            data = stock_list_local_redis()
            ts = datetime.datetime.now().isoformat()
            new_ticker = {
                x['代码']: {
                    'price': x['最新价'],
                    'last': x['昨收'],
                    'diff_p': x['涨跌幅'],
                    'ts': ts
                }
                for x in data if x['代码'] in self.symbols
            }
        self.ticker_mapping.update(new_ticker)

    def _close_order(self, order_id, actual_price, actual_time):
        order_record: Order = {}

        if order_record.status != 'open':
            return True

        order_record.actual_price = actual_price
        order_record.actual_time = actual_time
        order_record.status = 'close'

        account = AccountController(order_record.account_id)
        order_cash = int(
            round(order_record.order_price * order_record.amount * 100))
        actual_cash = int(round(actual_price * order_record.amount * 100))
        if order_record.side == 'buy':
            res = account._new_position(
                order_record.symbol, order_record.amount, order_cash, actual_cash)
        elif order_record.side == 'sell':
            res = account._sell_position(
                order_record.symbol, order_record.amount, actual_cash)

        # 如果仓位变更失败，close_order也将失败
        if not res:
            return False

        symbol_name = stock_info_local_redis(order_record.symbol)['名称']
        return True

    def _trade(self, new_ticker=None, dismiss_market_time=False):
        """
        依次处理所有orderbook，有open且价格符合的订单则根据当前实际价格进行成交
        这里是整个系统中最可能有性能瓶颈的地方
        如果timecost > interval的话，就要开始优化这一部分了
        """
        if new_ticker is None:
            new_ticker = {}
        start = time.time()
        self.last_trade_ts = start

        # 获取最新价格
        if new_ticker != {}:
            self._update_ticker(new_ticker)

        current = datetime.datetime.now()
        for symbol, orderbook in self.orderbook_mapping.items():
            if symbol not in self.ticker_mapping:
                continue

            current_price = self.ticker_mapping[symbol]['price']
            buy_order: Order = orderbook.pop_order('buy')
            if buy_order:
                if buy_order.order_price >= current_price:
                    if self._close_order(buy_order.order_id, current_price, current):
                        orderbook.order_id_mapping.pop(buy_order.order_id)
                else:
                    orderbook.add_order(buy_order)

            sell_order: Order = orderbook.pop_order('sell')
            if sell_order:
                if sell_order.order_price <= current_price:
                    if self._close_order(sell_order.order_id, current_price, current):
                        orderbook.order_id_mapping.pop(sell_order.order_id)
                else:
                    orderbook.add_order(sell_order)

        end = time.time()

    def run(self):

        self.subscribe_order_thread = threading.Thread(
            target=self._subscribe_order, daemon=True)
        self.subscribe_order_thread.start()

        if self.is_live:
            self.update_ticker_thread = IntervalThread(
                interval=3, target=self._update_ticker, daemon=True
            )
            self.update_ticker_thread.start()

        self.monitor_thread = IntervalThread(
            interval=self.MONITOR_INTERVAL, target=self.monitor, daemon=True)
        self.monitor_thread.start()

        self.trade_thread = IntervalThread(
            interval=self.TRADE_INTERVAL, target=self._trade, daemon=True)
        self.trade_thread.start()

        self.monitor_thread.join()


if __name__ == '__main__':
    trade_system = TradingEngine()
    trade_system.run()
