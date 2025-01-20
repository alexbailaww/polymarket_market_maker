import csv
import json
import os

def fetch_all(client):
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

def fetch_single(client, market_name):
    all_markets = fetch_all(client)

    for market in all_markets:
        if market['question'] == market_name:
            return market
    
    return None

def total_possible_rewards(client):
    response = fetch_all(client)
    total = 0

    for market in response:
        total += market['rewards']['rates'][0]['rewards_daily_rate']
    
    return total