import asyncio
import json
import websockets
import os
from dotenv import load_dotenv

# Polymarket WebSocket URL for the market channel.
WEBSOCKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

load_dotenv()

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
            yield data

            # # In case the message is a list.
            # if isinstance(data, list):
            #     for event in data:
            #         if event.get("event_type") == "order" and event.get("type") == "UPDATE":
            #             yield {
            #                 "market_id": event["market"],
            #                 "token_id": event["asset_id"],
            #                 "size": event["size"]
            #             }
            # # In case the message is a single dict.
            # elif isinstance(data, dict):
            #     if data.get("event_type") == "order" and data.get("type") == "UPDATE":
            #         yield {
            #                 "market_id": event["market"],
            #                 "token_id": event["asset_id"],
            #                 "size": event["size"]
            #         }

async def main():
    tokenIDs = [
        "110222417228270638383974743746762302792556220380554556504458115620557107501861"
    ]
    async for event in get_trades(tokenIDs):
        print(json.dumps(event, indent = 4))

if __name__ == "__main__":
    asyncio.run(main())
                    