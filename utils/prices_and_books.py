from py_clob_client.clob_types import BookParams

from ratelimit import limits, sleep_and_retry

from api_usage import track_api_usage

# 80 calls per 10 seconds
CALLS = 80
PERIOD = 10

# util for order books
# ##########################################
def chunker(lst, batch_size):
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]
# ##########################################

@track_api_usage
@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_order_book(client, tokenID):
    order_book = client.get_order_book(tokenID)
    print('Order book retrieved.')

    return order_book

@track_api_usage
@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_order_books(client, tokenIDs):
    order_books = client.get_order_books(
        params= tokenIDs
    )
    print('Order books retrieved.')

    return order_books

@track_api_usage
@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_all_order_books(client, tokenIDs):
    order_books = []
    batch_size = 100

    for batch in chunker(tokenIDs, batch_size):
        order_books.extend(client.get_order_books(params = batch))
    print('All order books retrieved.')

    return order_books

@track_api_usage
@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_market_price(client, tokenID):
    market_price = client.get_prices(
        params = [
            BookParams(token_id = tokenID, side = 'BUY'),
            BookParams(token_id = tokenID, side = 'SELL')
        ]
    )
    print("Market price retrieved.")

    return market_price

@track_api_usage
@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_market_midpoint_price(client, tokenID, side):
    market_midpoint_price = client.get_midpoint(tokenID)
    print("Market midpoint price retrieved.")

    return market_midpoint_price

@track_api_usage
@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_market_spread(client, tokenID):
    market_spread = client.get_spread(tokenID)
    print("Market spread retrieved.")

    return market_spread

@track_api_usage
@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_market_spreads(client, tokenIDs):
    order_books = client.get_spreads(
        params= tokenIDs
    )
    print('Market spreads retrieved.')

    return order_books

@track_api_usage
@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_all_market_spreads(client, tokenIDs):
    order_books = []
    batch_size = 100

    for batch in chunker(tokenIDs, batch_size):
        order_books.extend(client.get_spreads(params = batch))
    print('All market spreads retrieved.')