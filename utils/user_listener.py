import os
from dotenv import load_dotenv
import asyncio
import json
import websockets

WEBSOCKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

load_dotenv()

async def listen_user_trades(condition_id):
    subscription_message = json.dumps({
        "auth": {
            "apiKey": os.getenv("CLOB_API_KEY"),
            "secret": os.getenv("CLOB_SECRET"),
            "passphrase": os.getenv("CLOB_PASS_PHRASE")
        },
        "markets": [condition_id],
        "type": "User"
    })

    # The websocket URL may be the same as the market endpoint.
    # Adjust this URL if your provider offers a separate endpoint for the user channel.
    async with websockets.connect(WEBSOCKET_URL) as websocket:
        # Send the proper JSON-formatted subscription message.
        await websocket.send(subscription_message)

        # Listen continuously for incoming messages.
        while True:
            try:
                message = await websocket.recv()
            except websockets.ConnectionClosed:
                print("Trade WS Conn closed")
                break

            data = json.loads(message)
            if isinstance(data, list):
                for event in data:
                    if event.get("event_type") == "trade":
                        yield event
            elif isinstance(data, dict):
                if data.get("event_type") == "trade":
                    yield data

async def main():
    condition_id = "0x0d455730a68140b78530bb150ba956f2c4ff79a8b718439d9734b96d07abca14"

    async for event in listen_user_trades(condition_id):
        print(json.dumps(event, indent = 4))

if __name__ == "__main__":
    asyncio.run(main())