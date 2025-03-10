import requests
import json
import datetime
import time

def get_best_bid_ask(conditionID):
    r = requests.get(f"https://gamma-api.polymarket.com/markets?condition_ids={conditionID}")
    response = r.json()[0]
    result = [
            {
                "outcome": "Yes",
                "timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                "best_bid": round(response['bestBid'], 5),
                "best_ask": round(response['bestAsk'], 5)
            },
            {
                "outcome": "No",
                "timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                "best_bid": 1 - round(response['bestAsk'], 5),
                "best_ask": 1 - round(response['bestBid'], 5)
            }
        ]
    return result

condition_id = '0x0b9f873f001fa5ccf06fb55a21663401a86ba8af20c2452baea618a32c08c88f'

yes_token = '14129606891696045435070810675187301340086644342883712605070431972382394052093'
no_token = '91902514841442955492299313618851884821546725395960468411290135010102100553137'

callIndex = 0

start_time = time.time()
while True:
    get_best_bid_ask(condition_id)
    callIndex += 1
    if callIndex % 10 == 0:
        print(f'Gamma API call index = {callIndex}, time = {round(time.time() - start_time, 2)} seconds')