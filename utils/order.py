from py_clob_client.clob_types import OrderArgs, OrderType, OrdersScoringParams, OpenOrderParams
from py_clob_client.order_builder.constants import BUY, SELL

from typing import Literal

from py_clob_client.clob_types import  OrderArgs

def create_and_submit_order(client, token_id, side: Literal['BUY'] | Literal['SELL'], price, size):
    # Create and sign a limit order buying 100 YES tokens for 0.0005 each
    order_args = OrderArgs(
        price=price,
        size=size,
        side=side,
        token_id=token_id,
    )
    signed_order = client.create_order(order_args)
    resp = client.post_order(signed_order)
    print(f'Order created. Logs: {resp}')

def cancel_order(client, orderID):
    resp = client.cancel(order_id=orderID)
    if not resp['not_cancelled']:
        print(f'Order cancelled.')
    else:
        print(f'Order could not be cancelled. Logs: {resp['not_canceled']}')

def cancel_orders(client, orderIDs):
    resp = client.cancel_orders(orderIDs)
    if not resp['not_canceled']:
        print(f'All orders have been cancelled.')
    else:
        print(f'Some orders could not be cancelled. Logs: {resp['not_canceled']}')

def cancel_all_orders(client):
    resp = client.cancel_all()
    if not resp['not_canceled']:
        print(f'All orders have been cancelled.')
    else:
        print(f'Some orders could not be cancelled. Logs: {resp['not_canceled']}')

def get_order(client, orderID):
    order = client.get_order(orderID)
    print(f'Order retrieved.')

    return order

def get_orders_scorings(client, orderIDs):
    scorings = client.are_orders_scoring(OrdersScoringParams(orderIds=orderIDs))
    print(f'Order scoring for orders {orderIDs} retrieved.')

    return scorings

def get_market_active_orders(client, market_id):
    resp = client.get_orders(OpenOrderParams(market=market_id,))
    print(f'Orders from Market #{market_id} retrieved.')   

    return resp   
