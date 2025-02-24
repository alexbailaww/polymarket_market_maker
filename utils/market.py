import csv
import json
import os

from ratelimit import limits, sleep_and_retry

# 80 calls per 10 seconds
CALLS = 80
PERIOD = 10

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_all(client):
    markets_list = []
    next_cursor = None

    while True:
        try:
            if next_cursor is None:
                response = client.get_markets()
            else:
                response = client.get_markets(next_cursor=next_cursor)

            if 'data' not in response:
                break
            markets_list.extend(response['data'])
            next_cursor = response.get("next_cursor")

            if not next_cursor:
                break

        except Exception as e:
            # print(f"Exception occurred: {e}")
            # print(f"Exception details: {e.__class__.__name__}")
            # print(f"Error message: {e.args}")
            break

    return markets_list

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_sampling_all(client):
    markets_list = []
    next_cursor = None

    while True:
        try:
            if next_cursor is None:
                response = client.get_sampling_markets()
            else:
                response = client.get_sampling_markets(next_cursor=next_cursor)

            if 'data' not in response:
                break
            markets_list.extend(response['data'])
            next_cursor = response.get("next_cursor")

            if not next_cursor:
                break

        except Exception as e:
            # print(f"Exception occurred: {e}")
            # print(f"Exception details: {e.__class__.__name__}")
            # print(f"Error message: {e.args}")
            break

    return markets_list

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_single_byName(client, market_name):
    all_markets = get_all(client)

    for market in all_markets:
        if market['question'] == market_name:
            return market
    
    return

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_single_byCondition(client, conditionID):
    resp = client.get_market(condition_id = conditionID)
    if 'data' not in resp:
        return 
    else:
        return resp['data']
    
@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_single_byToken(client, tokenID):
    resp = get_sampling_all(client)
    for market in resp:
        if tokenID == market['tokens'][0]['token_id']:
            return {"question": market['question'], "answer": market['tokens'][0]['outcome']}
        if tokenID == market['tokens'][1]['token_id']:
            return {"question": market['question'], "answer": market['tokens'][1]['outcome']}

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def total_possible_rewards(client):
    response = get_sampling_all(client)
    total = 0

    for market in response:
        total += market['rewards']['rates'][0]['rewards_daily_rate']
    
    return total