import threading
import time
from flask import Flask, request, jsonify
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.execution import ExecutionFilter

FUTURES_MAP = {
    "GC1!": {"symbol": "GC", "exchange": "COMEX", "expiry": "202508", "tick": 0.10},
    "ES1!": {"symbol": "ES", "exchange": "CME",   "expiry": "202506", "tick": 0.25},
    "NQ1!": {"symbol": "NQ", "exchange": "CME",   "expiry": "202506", "tick": 0.25},
    "CL1!": {"symbol": "CL", "exchange": "NYMEX", "expiry": "202506", "tick": 0.01},
}

# 全局持仓记录，定义在最顶部避免 NameError
open_positions = {}


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
        self.order_id        = None
        self.connected       = False
        self.sl_order_map    = {}
        self.exec_history    = []
        self.ib_positions    = {}
        self._positions_done = threading.Event()

    def nextValidId(self, orderId):
        self.order_id = orderId
        self.connected = True
        print(f"[IB] 已连接，Order ID: {orderId}")
        self.reqOpenOrders()
        self.reqPositions()
        self.reqExecutions(10001, ExecutionFilter())

    # ── 挂单同步（恢复止损单记录）─────────────────────────────
    def openOrder(self, orderId, contract, order, orderState):
        print(f"[IB] 订单提交 - {order.action} {order.totalQuantity} {contract.symbol}")
        if order.orderType == "STP" and order.action == "SELL":
            symbol = contract.symbol
            for k, v in FUTURES_MAP.items():
                if v["symbol"] == symbol:
                    symbol = k
                    break
            qty = int(order.totalQuantity)
            if symbol not in open_positions:
                open_positions[symbol] = {"sl_order_id": orderId, "quantity": qty}
            else:
                # 补上之前 None 的 sl_order_id
                if open_positions[symbol]["sl_order_id"] is None:
                    open_positions[symbol]["sl_order_id"] = orderId
            self.sl_order_map[orderId] = symbol
            print(f"[恢复] 止损单同步: {symbol} sl_id={orderId} qty={qty}")

    def openOrderEnd(self):
        print(f"[恢复] 挂单同步完成，当前持仓: {open_positions}")

    # ── 订单状态回调 ──────────────────────────────────────────
    def orderStatus(self, orderId, status, filled, remaining,
                    avgFillPrice, permId, parentId, lastFillPrice,
                    clientId, whyHeld, mktCapPrice):
        print(f"[IB] 订单状态 - ID:{orderId} {status} 成交:{filled} 均价:{avgFillPrice}")
        if status == "Filled" and orderId in self.sl_order_map:
            symbol = self.sl_order_map[orderId]
            if symbol in open_positions:
                del open_positions[symbol]
                print(f"[仓位] {symbol} 止损触发，仓位已清除")
            del self.sl_order_map[orderId]

    # ── 历史成交回调 ──────────────────────────────────────────
    def execDetails(self, reqId, contract, execution):
        record = {
            "time":    execution.time,
            "symbol":  contract.symbol,
            "secType": contract.secType,
            "side":    execution.side,
            "shares":  execution.shares,
            "price":   execution.price,
            "orderId": execution.orderId,
            "execId":  execution.execId,
        }
        if not any(e["execId"] == record["execId"] for e in self.exec_history):
            self.exec_history.append(record)
        print(f"[成交] {record['side']} {record['shares']} {record['symbol']} @ {record['price']} {record['time']}")

    def execDetailsEnd(self, reqId):
        print(f"[成交] 历史记录同步完成，共 {len(self.exec_history)} 笔")

    # ── 实际持仓回调 ──────────────────────────────────────────
    def position(self, account, contract, position, avgCost):
        if position != 0:
            self.ib_positions[contract.symbol] = {
                "position": position,
                "avgCost":  avgCost,
                "secType":  contract.secType,
                "exchange": contract.exchange,
                "currency": contract.currency,
            }
            print(f"[持仓] {contract.symbol} {position}股/张 均价:{avgCost}")
        elif contract.symbol in self.ib_positions:
            del self.ib_positions[contract.symbol]

    def positionEnd(self):
        print(f"[持仓] 同步完成，共 {len(self.ib_positions)} 个持仓")
        # 自动补全 open_positions（有IB持仓但本地没记录的，比如重启后）
        for ib_symbol, info in self.ib_positions.items():
            tv_symbol = ib_symbol
            for k, v in FUTURES_MAP.items():
                if v["symbol"] == ib_symbol:
                    tv_symbol = k
                    break
            if tv_symbol not in open_positions and info["position"] > 0:
                open_positions[tv_symbol] = {
                    "sl_order_id": None,
                    "quantity": int(info["position"])
                }
                print(f"[自动恢复] {tv_symbol} qty={int(info['position'])} (等待止损单同步)")
        self._positions_done.set()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode not in [2104, 2106, 2107, 2158]:
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

flask_app = Flask(__name__)


@flask_app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    print(f"[Webhook] 收到信号: {data}")

    action = data['action'].upper()
    symbol = data['symbol'].upper()

    # ── 入场 BUY ──────────────────────────────────────────────
    if action == "BUY":
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
        contract  = make_contract(symbol)

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

        open_positions[symbol] = {"sl_order_id": sl_id, "quantity": quantity}
        app_ib.sl_order_map[sl_id] = symbol

        print(f"[入场] {symbol} 买入 {quantity}，止损 ${sl_price} (-{sl_pct*100}%)")
        return jsonify({
            "status": "ok", "action": "BUY", "symbol": symbol,
            "quantity": quantity, "entry_price": price, "stop_loss_price": sl_price
        }), 200

    # ── 离场 SELL ─────────────────────────────────────────────
    elif action == "SELL":
        if symbol not in open_positions:
            print(f"[跳过] {symbol} 没有持仓，忽略死叉信号")
            return jsonify({"status": "skipped", "reason": "no position"}), 200

        pos = open_positions[symbol]

        # sl_order_id 可能为 None（重启后自动恢复的仓位）
        if pos["sl_order_id"] is not None:
            app_ib.cancelOrder(pos["sl_order_id"])
            print(f"[取消] 止损单 ID:{pos['sl_order_id']} 已取消")
        else:
            print(f"[跳过取消] {symbol} 无止损单记录，直接市价卖出")

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

        sl_id = pos["sl_order_id"]
        del open_positions[symbol]
        if sl_id and sl_id in app_ib.sl_order_map:
            del app_ib.sl_order_map[sl_id]

        print(f"[离场] {symbol} 死叉信号，市价卖出 {pos['quantity']}")
        return jsonify({
            "status": "ok", "action": "SELL",
            "symbol": symbol, "quantity": pos["quantity"]
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
    """从IB实时拉取实际持仓"""
    app_ib._positions_done.clear()
    app_ib.ib_positions = {}
    app_ib.reqPositions()
    app_ib._positions_done.wait(timeout=5)
    return jsonify({
        "ib_positions":   app_ib.ib_positions,
        "open_positions": open_positions
    }), 200


@flask_app.route('/executions', methods=['GET'])
def executions():
    """查看今日成交记录，可加 ?symbol=AAPL 过滤"""
    symbol = request.args.get('symbol', '').upper()
    history = app_ib.exec_history
    if symbol:
        history = [e for e in history if e["symbol"] == symbol]
    return jsonify({
        "count":      len(history),
        "executions": sorted(history, key=lambda x: x["time"], reverse=True)
    }), 200


@flask_app.route('/close', methods=['POST'])
def close_position():
    """平仓指定 symbol。POST /close {"symbol": "AAPL"}"""
    data   = request.json or {}
    symbol = data.get('symbol', '').upper()

    if not symbol:
        return jsonify({"error": "缺少 symbol"}), 400

    # 取消止损单（如果有）
    if symbol in open_positions:
        sl_id = open_positions[symbol]["sl_order_id"]
        if sl_id is not None:
            app_ib.cancelOrder(sl_id)
            print(f"[平仓] 取消止损单 ID:{sl_id}")

    # 从IB查实际持仓
    app_ib._positions_done.clear()
    app_ib.ib_positions = {}
    app_ib.reqPositions()
    app_ib._positions_done.wait(timeout=5)

    ib_sym   = FUTURES_MAP.get(symbol, {}).get("symbol", symbol)
    pos_info = app_ib.ib_positions.get(ib_sym) or app_ib.ib_positions.get(symbol)

    if not pos_info or pos_info["position"] == 0:
        open_positions.pop(symbol, None)
        return jsonify({"status": "no_position", "symbol": symbol}), 200

    qty      = abs(pos_info["position"])
    action   = "BUY" if pos_info["position"] < 0 else "SELL"
    contract = make_contract(symbol)

    app_ib.order_id += 1
    close_order = Order()
    close_order.orderId       = app_ib.order_id
    close_order.action        = action
    close_order.orderType     = "MKT"
    close_order.totalQuantity = qty
    close_order.transmit      = True
    close_order.eTradeOnly    = False
    close_order.firmQuoteOnly = False

    app_ib.placeOrder(app_ib.order_id, contract, close_order)
    open_positions.pop(symbol, None)

    print(f"[平仓] {symbol} {action} {qty}")
    return jsonify({"status": "ok", "symbol": symbol, "action": action, "quantity": qty}), 200


@flask_app.route('/closeall', methods=['POST'])
def close_all():
    """一键平所有仓位（根据IB实际持仓）"""
    for sym, pos in list(open_positions.items()):
        if pos["sl_order_id"] is not None:
            app_ib.cancelOrder(pos["sl_order_id"])
            print(f"[全平] 取消止损单 {sym} ID:{pos['sl_order_id']}")

    app_ib._positions_done.clear()
    app_ib.ib_positions = {}
    app_ib.reqPositions()
    app_ib._positions_done.wait(timeout=5)

    results = []
    for ib_symbol, pos_info in app_ib.ib_positions.items():
        if pos_info["position"] == 0:
            continue

        qty      = abs(pos_info["position"])
        action   = "BUY" if pos_info["position"] < 0 else "SELL"

        tv_symbol = ib_symbol
        for k, v in FUTURES_MAP.items():
            if v["symbol"] == ib_symbol:
                tv_symbol = k
                break

        contract = make_contract(tv_symbol)
        app_ib.order_id += 1
        close_order = Order()
        close_order.orderId       = app_ib.order_id
        close_order.action        = action
        close_order.orderType     = "MKT"
        close_order.totalQuantity = qty
        close_order.transmit      = True
        close_order.eTradeOnly    = False
        close_order.firmQuoteOnly = False

        app_ib.placeOrder(app_ib.order_id, contract, close_order)
        print(f"[全平] {ib_symbol} {action} {qty}")
        results.append({"symbol": ib_symbol, "action": action, "quantity": qty})

    open_positions.clear()
    app_ib.sl_order_map.clear()
    return jsonify({"status": "ok", "closed": results}), 200


@flask_app.route('/restore', methods=['POST'])
def restore_position():
    """手动恢复持仓记录。POST /restore {"symbol":"AAPL","quantity":1,"sl_order_id":61}"""
    data        = request.json or {}
    symbol      = data.get('symbol', '').upper()
    quantity    = int(data.get('quantity', 1))
    sl_order_id = data.get('sl_order_id')  # 可以为 null

    if not symbol:
        return jsonify({"error": "缺少 symbol"}), 400

    open_positions[symbol] = {"sl_order_id": sl_order_id, "quantity": quantity}
    if sl_order_id:
        app_ib.sl_order_map[int(sl_order_id)] = symbol
    print(f"[恢复] 手动恢复: {symbol} qty={quantity} sl_id={sl_order_id}")
    return jsonify({"status": "ok", "open_positions": open_positions}), 200


if __name__ == '__main__':
    flask_app.run(host='0.0.0.0', port=5000)
