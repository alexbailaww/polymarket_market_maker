from py_clob_client.clob_types import OrderArgs, OrderType, OrdersScoringParams, OpenOrderParams
from py_clob_client.order_builder.constants import BUY, SELL

from typing import Literal

from py_clob_client.clob_types import  OrderArgs

from ratelimit import limits, sleep_and_retry
from api_usage import track_api_usage

# 80 calls per 10 seconds
CALLS = 80
PERIOD = 10

@track_api_usage
@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def create_and_submit_order(client, token_id, side: Literal['BUY'] | Literal['SELL'], price, size):
    order_args = OrderArgs(
        price=price,
        size=size,
        side=side,
        token_id=token_id,
    )
    signed_order = client.create_order(order_args)
    resp = client.post_order(signed_order)
    print(f'Order created. Logs: {resp}')

@track_api_usage
@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def cancel_order(client, orderID):
    resp = client.cancel(order_id=orderID)
    if not resp['not_canceled']:
        print(f'Order cancelled.')
    else:
        print(f'Order could not be cancelled. Logs: {resp['not_canceled']}')

@track_api_usage
@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def cancel_orders(client, orderIDs):
    resp = client.cancel_orders(orderIDs)
    if not resp['not_canceled']:
        print(f'All orders have been cancelled.')
    else:
        print(f'Some orders could not be cancelled. Logs: {resp['not_canceled']}')

@track_api_usage
@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def cancel_market_orders(client, marketID, tokenID):
    resp = client.cancel_market_orders(market = marketID, asset_id = tokenID)
    if not resp['not_canceled']:
        print(f'All orders from market #{tokenID} have been cancelled.')
    else:
        print(f'Some orders could not be cancelled. Logs: {resp['not_canceled']}')

@track_api_usage
@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def cancel_all_orders(client):
    resp = client.cancel_all()
    if not resp['not_canceled']:
        print(f'All orders have been cancelled.')
    else:
        print(f'Some orders could not be cancelled. Logs: {resp['not_canceled']}')

@track_api_usage
@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_order(client, orderID):
    order = client.get_order(orderID)
    print(f'Order retrieved.')

    return order

@track_api_usage
@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_orders_scorings(client, orderIDs):
    scorings = client.are_orders_scoring(OrdersScoringParams(orderIds=orderIDs))
    print(f'Order scoring for orders {orderIDs} retrieved.')

    return scorings

@track_api_usage
@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_market_active_orders(client, market_id):
    resp = client.get_orders(OpenOrderParams(market=market_id,))

    return resp   
