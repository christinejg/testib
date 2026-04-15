import threading
import time
from flask import Flask, request, jsonify
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order

FUTURES_MAP = {
    "GC1!": {"symbol": "GC", "exchange": "COMEX", "expiry": "202508", "tick": 0.10},
    "ES1!": {"symbol": "ES", "exchange": "CME",   "expiry": "202506", "tick": 0.25},
    "NQ1!": {"symbol": "NQ", "exchange": "CME",   "expiry": "202506", "tick": 0.25},
    "CL1!": {"symbol": "CL", "exchange": "NYMEX", "expiry": "202506", "tick": 0.01},
}

def execDetails(self, reqId, contract, execution):
    print(f"[成交] {execution.side} {execution.shares} {contract.symbol} "
          f"@ {execution.price} 时间:{execution.time}")

def position(self, account, contract, position, avgCost):
    print(f"[持仓] {contract.symbol} {position}股 均价:{avgCost}")

def positionEnd(self):
    print("[持仓] 查询完毕")

def make_contract(symbol):
    contract = Contract()
    if symbol in FUTURES_MAP:
        info = FUTURES_MAP[symbol]
        contract.symbol   = info["symbol"]
        contract.secType  = "FUT"
        contract.exchange = info["exchange"]
        contract.currency = "USD"
        contract.lastTradeDateOrContractMonth = info["expiry"]
    else:
        contract.symbol   = symbol
        contract.secType  = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
    return contract

def round_to_tick(price, tick_size):
    decimals = len(str(tick_size).rstrip('0').split('.')[-1])
    return round(round(price / tick_size) * tick_size, decimals)


class IBApp(EWrapper, EClient):
    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self.order_id    = None
        self.connected   = False
        # order_id → symbol，用来追踪止损单是哪个 symbol 的
        self.sl_order_map = {}

    def nextValidId(self, orderId):
        self.order_id = orderId
        self.connected = True
        print(f"[IB] 已连接，Order ID: {orderId}")

    def orderStatus(self, orderId, status, filled, remaining,
                    avgFillPrice, permId, parentId, lastFillPrice,
                    clientId, whyHeld, mktCapPrice):
        print(f"[IB] 订单状态 - ID:{orderId} {status} 成交:{filled} 均价:{avgFillPrice}")

        # 如果止损单被触发成交，自动清除仓位记录
        if status == "Filled" and orderId in self.sl_order_map:
            symbol = self.sl_order_map[orderId]
            if symbol in open_positions:
                del open_positions[symbol]
                print(f"[仓位] {symbol} 止损触发，仓位已清除")
            del self.sl_order_map[orderId]

    def openOrder(self, orderId, contract, order, orderState):
        print(f"[IB] 订单提交 - {order.action} {order.totalQuantity} {contract.symbol}")

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode not in [2104, 2106, 2158]:
            print(f"[IB] 错误 {errorCode}: {errorString}")


app_ib = IBApp()

def run_ib():
    app_ib.connect("127.0.0.1", 4002, clientId=1)
    app_ib.run()

ib_thread = threading.Thread(target=run_ib, daemon=True)
ib_thread.start()

print("[服务器] 等待 IB Gateway 连接...")
for _ in range(20):
    if app_ib.connected:
        break
    time.sleep(0.5)

if not app_ib.connected:
    print("[警告] IB Gateway 未连接")

# 追踪持仓：{ symbol: { "sl_order_id": int, "quantity": int } }
open_positions = {}

flask_app = Flask(__name__)


@flask_app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    print(f"[Webhook] 收到信号: {data}")

    action = data['action'].upper()
    symbol = data['symbol'].upper()

    # ── 入场 BUY ──────────────────────────────────────────────
    if action == "BUY":

        # 已经有持仓就不重复买
        if symbol in open_positions:
            print(f"[跳过] {symbol} 已有持仓，忽略此信号")
            return jsonify({"status": "skipped", "reason": "already in position"}), 200

        quantity = int(float(data.get('quantity', 1)))
        sl_pct   = float(data.get('stop_loss_pct', 0.02))
        price    = float(data.get('price', 0))

        if not price:
            return jsonify({"error": "缺少 price"}), 400

        tick_size = FUTURES_MAP[symbol]["tick"] if symbol in FUTURES_MAP else 0.01
        sl_price  = round_to_tick(price * (1 - sl_pct), tick_size)

        contract = make_contract(symbol)

        # 市价买入
        app_ib.order_id += 1
        buy_id = app_ib.order_id
        buy_order = Order()
        buy_order.orderId       = buy_id
        buy_order.action        = "BUY"
        buy_order.orderType     = "MKT"
        buy_order.totalQuantity = quantity
        buy_order.transmit      = True
        buy_order.eTradeOnly    = False
        buy_order.firmQuoteOnly = False

        # 独立止损单
        app_ib.order_id += 1
        sl_id = app_ib.order_id
        sl_order = Order()
        sl_order.orderId       = sl_id
        sl_order.action        = "SELL"
        sl_order.orderType     = "STP"
        sl_order.totalQuantity = quantity
        sl_order.auxPrice      = sl_price
        sl_order.transmit      = True
        sl_order.eTradeOnly    = False
        sl_order.firmQuoteOnly = False

        app_ib.placeOrder(buy_id, contract, buy_order)
        app_ib.placeOrder(sl_id,  contract, sl_order)

        # 记录仓位
        open_positions[symbol] = {"sl_order_id": sl_id, "quantity": quantity}
        app_ib.sl_order_map[sl_id] = symbol

        print(f"[入场] {symbol} 买入 {quantity}，止损 ${sl_price} (-{sl_pct*100}%)")

        return jsonify({
            "status":          "ok",
            "action":          "BUY",
            "symbol":          symbol,
            "quantity":        quantity,
            "entry_price":     price,
            "stop_loss_price": sl_price
        }), 200

    # ── 离场 SELL（死叉触发）──────────────────────────────────
    elif action == "SELL":

        if symbol not in open_positions:
            print(f"[跳过] {symbol} 没有持仓，忽略死叉信号")
            return jsonify({"status": "skipped", "reason": "no position"}), 200

        pos = open_positions[symbol]

        # 先取消止损单
        app_ib.cancelOrder(pos["sl_order_id"])
        print(f"[取消] 止损单 ID:{pos['sl_order_id']} 已取消")

        # 市价卖出
        app_ib.order_id += 1
        sell_id = app_ib.order_id
        sell_order = Order()
        sell_order.orderId       = sell_id
        sell_order.action        = "SELL"
        sell_order.orderType     = "MKT"
        sell_order.totalQuantity = pos["quantity"]
        sell_order.transmit      = True
        sell_order.eTradeOnly    = False
        sell_order.firmQuoteOnly = False

        contract = make_contract(symbol)
        app_ib.placeOrder(sell_id, contract, sell_order)

        del open_positions[symbol]
        if pos["sl_order_id"] in app_ib.sl_order_map:
            del app_ib.sl_order_map[pos["sl_order_id"]]

        print(f"[离场] {symbol} 死叉信号，市价卖出 {pos['quantity']}")

        return jsonify({
            "status":   "ok",
            "action":   "SELL",
            "symbol":   symbol,
            "quantity": pos["quantity"]
        }), 200

    else:
        return jsonify({"error": f"未知 action: {action}"}), 400


@flask_app.route('/health', methods=['GET'])
def health():
    return jsonify({
        "ib_connected":   app_ib.connected,
        "order_id":       app_ib.order_id,
        "open_positions": open_positions
    }), 200


@flask_app.route('/positions', methods=['GET'])
def positions():
    app_ib.reqPositions()
    return jsonify({"open_positions": open_positions}), 200


if __name__ == '__main__':
    flask_app.run(host='0.0.0.0', port=5000)
