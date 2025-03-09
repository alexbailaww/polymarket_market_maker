import asyncio
import json
import websockets
import os
from dotenv import load_dotenv
import datetime

# Polymarket WebSocket URL for the market channel.
WEBSOCKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

load_dotenv()

async def get_best_bid_ask(tokenIDs):
    subscription_message = json.dumps({
        "auth":{
            "apiKey": os.getenv('CLOB_API_KEY'),
            "secret": os.getenv('CLOB_SECRET'),
            "passphrase": os.getenv('CLOB_PASS_PHRASE')
        },
        "assets_ids": tokenIDs,
        "type": "Market"
    })

    async with websockets.connect(WEBSOCKET_URL) as websocket:
        # Send the proper JSON-formatted subscription message.
        await websocket.send(subscription_message)

        # Listen continuously for incoming messages. 
        while True:
            try:
                message = await websocket.recv()
            except websockets.ConnectionClosed:
                break

            data = json.loads(message)

            # In case the message is a list.
            if isinstance(data, list):
                for event in data:
                    if event.get("event_type") == "book":
                        yield {
                            "timestamp": datetime.datetime.fromtimestamp(int(event["timestamp"]) / 1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  
                            "best_bid": event["bids"][-1]["price"],
                            "best_ask": event["asks"][-1]["price"],
                        }
            # In case the message is a single dict.
            elif isinstance(data, dict):
                if data.get("event_type") == "book":
                    yield {
                        "timestamp": datetime.datetime.fromtimestamp(int(event["timestamp"]) / 1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                        "best_bid": data["bids"][-1]["price"],
                        "best_ask": data["asks"][-1]["price"],
                    }

async def get_tick_size_change(tokenIDs):
    subscription_message = json.dumps({
        "auth":{
            "apiKey": os.getenv('CLOB_API_KEY'),
            "secret": os.getenv('CLOB_SECRET'),
            "passphrase": os.getenv('CLOB_PASS_PHRASE')
        },
        "assets_ids": tokenIDs,
        "type": "Market"
    })

    async with websockets.connect(WEBSOCKET_URL) as websocket:
        # Send the proper JSON-formatted subscription message.
        await websocket.send(subscription_message)

        # Listen continuously for incoming messages.
        while True:
            try:
                message = await websocket.recv()
            except websockets.ConnectionClosed:
                break

            data = json.loads(message)

            # In case the message is a list.
            if isinstance(data, list):
                for event in data:
                    if event.get("event_type") == "tick_size_change":
                        yield {
                            "old_tick_size": event["old_tick_size"],
                            "new_tick_size": event["new_tick_size"]
                        } 
            # In case the message is a single dict.
            elif isinstance(data, dict):
                if data.get("event_type") == "tick_size_change":
                    yield {
                        "old_tick_size": data["old_tick_size"],
                        "new_tick_size": data["new_tick_size"]
                    }

async def get_trades(marketIDs):
    subscription_message = json.dumps({
        "auth":{
            "apiKey": os.getenv('CLOB_API_KEY'),
            "secret": os.getenv('CLOB_SECRET'),
            "passphrase": os.getenv('CLOB_PASS_PHRASE')
        },
        "markets": marketIDs,
        "type": "User"
    })

    async with websockets.connect(WEBSOCKET_URL) as websocket:
        # Send the proper JSON-formatted subscription message. 
        await websocket.send(subscription_message)

        # Listen continuously for incoming messages.
        while True:
            try:
                message = await websocket.recv()
            except websockets.ConnectionClosed:
                break

            data = json.loads(message)

            # In case the message is a list.
            if isinstance(data, list):
                for event in data:
                    if event.get("event_type") == "order" and event.get("type") == "UPDATE":
                        yield {
                            "market_id": event["market"],
                            "token_id": event["asset_id"],
                            "size": event["size"]
                        }
            # In case the message is a single dict.
            elif isinstance(data, dict):
                if data.get("event_type") == "order" and data.get("type") == "UPDATE":
                    yield {
                            "market_id": event["market"],
                            "token_id": event["asset_id"],
                            "size": event["size"]
                    }

async def market_events(tokenIDs):
    subscription_message = json.dumps({
        "auth":{
            "apiKey": os.getenv('CLOB_API_KEY'),
            "secret": os.getenv('CLOB_SECRET'),
            "passphrase": os.getenv('CLOB_PASS_PHRASE')
        },
        "assets_ids": tokenIDs,
        "type": "Market"
    })

    async with websockets.connect(WEBSOCKET_URL) as websocket:
        # Send the proper JSON-formatted subscription message.
        await websocket.send(subscription_message)

        # Listen continuously for incoming messages.
        while True:
            try:
                message = await websocket.recv()
            except websockets.ConnectionClosed:
                break

            data = json.loads(message)
            yield data

async def main():
    tokenIDs = ['19421936073502749499295956976296494028264193035596788165532289826979364170763', '33245518756710035259466965025019863630995207982345911576600719002024360031950']
    async for event in get_best_bid_ask(tokenIDs):
        print(json.dumps(event, indent = 4))

if __name__ == "__main__":
    asyncio.run(main())
                    