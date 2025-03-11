import os
import asyncio
import json
from datetime import datetime
import websockets
import market, clob_client, order

# Set your Polymarket websocket URL here
WEBSOCKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

class OrderBook:
    def __init__(self):
        self.buys = {}   # Dictionary for bid levels: {price: available_size}
        self.sells = {}  # Dictionary for ask levels: {price: available_size}

    def update_from_book(self, data):
        # In the snapshot, the order book levels are under "bids" and "asks"
        self.buys = {float(level["price"]): float(level["size"]) for level in data.get("bids", [])}
        self.sells = {float(level["price"]): float(level["size"]) for level in data.get("asks", [])}

    def update_from_price_change(self, data):
        # Process each change in the "changes" list of a price_change event.
        changes = data.get("changes", [])
        for change in changes:
            side = change.get("side", "").lower()  # e.g., "BUY" becomes "buy"
            price = float(change.get("price", 0))
            new_size = float(change.get("size", 0))
            if side == "buy":
                if new_size == 0:
                    self.buys.pop(price, None)
                else:
                    self.buys[price] = new_size
            elif side == "sell":
                if new_size == 0:
                    self.sells.pop(price, None)
                else:
                    self.sells[price] = new_size

    def best_bid_ask(self):
        best_bid = max(self.buys.keys()) if self.buys else None
        best_ask = min(self.sells.keys()) if self.sells else None
        return best_bid, best_ask

class BinaryOrderBook:
    """
    Tracks a binary market with two outcomes: No and Yes.
    For the No outcome, the order book updates come directly.
    The Yes outcome prices are derived by inversion:
      Yes best bid = 1 - (No best ask)
      Yes best ask = 1 - (No best bid)
    """
    def __init__(self, no_asset_id, yes_asset_id):
        self.orderbooks = {
            no_asset_id: OrderBook(),
            yes_asset_id: OrderBook(),  # In case you receive direct Yes updates.
        }
        self.no_asset_id = no_asset_id
        self.yes_asset_id = yes_asset_id

    def update_from_book(self, data):
        asset_id = data.get("asset_id")
        if asset_id in self.orderbooks:
            self.orderbooks[asset_id].update_from_book(data)

    def update_from_price_change(self, data):
        asset_id = data.get("asset_id")
        if asset_id in self.orderbooks:
            self.orderbooks[asset_id].update_from_price_change(data)

    def best_prices(self):
        best_bid_no, best_ask_no = self.orderbooks[self.no_asset_id].best_bid_ask()
        derived_best_bid_yes = round(1 - best_ask_no, 5) if best_ask_no is not None else None
        derived_best_ask_yes = round(1 - best_bid_no, 5) if best_bid_no is not None else None

        return {
            "No": {
                "best_bid": round(best_bid_no, 5) if best_bid_no is not None else None,
                "best_ask": round(best_ask_no, 5) if best_ask_no is not None else None
            },
            "Yes": {
                "best_bid": derived_best_bid_yes,
                "best_ask": derived_best_ask_yes
            }
        }

async def get_best_bid_ask(tokenIDs):
    subscription_message = json.dumps({
        "auth": {
            "apiKey": os.getenv('CLOB_API_KEY'),
            "secret": os.getenv('CLOB_SECRET'),
            "passphrase": os.getenv('CLOB_PASS_PHRASE')
        },
        "assets_ids": tokenIDs,
        "type": "Market"
    })

    async with websockets.connect(WEBSOCKET_URL) as websocket:
        await websocket.send(subscription_message)
        while True:
            try:
                message = await websocket.recv()
            except websockets.ConnectionClosed:
                break
            data = json.loads(message)
            yield data

async def listen_binary_market(no_asset_id, yes_asset_id):
    """
    Listens to market events for both outcomes and yields results.
    
    For regular order book updates (book / price_change):
      {
          "datetime": <datetime>,
          "No": {"best_bid": ..., "best_ask": ...},
          "Yes": {"best_bid": ..., "best_ask": ...}
      }
    
    For tick size changes:
      {
          "datetime": <datetime>,
          "old_tick_size": <old tick size>,
          "new_tick_size": <new tick size>
      }
    """
    binary_order_book = BinaryOrderBook(no_asset_id, yes_asset_id)
    async for data in get_best_bid_ask([no_asset_id, yes_asset_id]):
        # The websocket may return a list of messages.
        messages = data if isinstance(data, list) else [data]
        timestamps = []
        order_book_updated = False
        tick_results = []
        
        for msg in messages:
            ts = msg.get("timestamp")
            if ts:
                timestamps.append(int(ts))
            event_type = msg.get("event_type")
            if event_type == "book":
                binary_order_book.update_from_book(msg)
                order_book_updated = True
            elif event_type == "price_change":
                binary_order_book.update_from_price_change(msg)
                order_book_updated = True
            elif event_type == "tick_size_change":
                # Build tick size change result for this event.
                dt = datetime.fromtimestamp(int(ts)/1000.0).isoformat() if ts else None
                tick_result = {
                    "datetime": dt,
                    "old_tick_size": round(float(msg.get("old_tick_size")), 5) if msg.get("old_tick_size") is not None else None,
                    "new_tick_size": round(float(msg.get("new_tick_size")), 5) if msg.get("new_tick_size") is not None else None
                }
                tick_results.append(tick_result)
            else:
                print("Unknown event:", msg)
        
        # Determine the update datetime from the highest timestamp in this batch.
        update_datetime = datetime.fromtimestamp(max(timestamps)/1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if timestamps else None
        
        # Yield any tick size change events first.
        for tick_update in tick_results:
            yield tick_update
        
        # Then, if there were order book updates, yield the best bid/ask result.
        if order_book_updated:
            prices = binary_order_book.best_prices()
            result = {
                "datetime": update_datetime,
                "No": {"best_bid": prices["No"]["best_bid"], "best_ask": prices["No"]["best_ask"]},
                "Yes": {"best_bid": prices["Yes"]["best_bid"], "best_ask": prices["Yes"]["best_ask"]}
            }
            yield result

if __name__ == "__main__":
    bot = clob_client.create_client()
    order.cancel_all_orders(bot)

    mkt = market.get_single_byName(bot, "Will Ontario resume electricity surcharge to the U.S. by next Friday?")

    # Replace these with your actual asset IDs for the No and Yes outcomes.
    no_asset_id = "72847832711967062303071270284482950561188218126373806294341642866826184782998"
    yes_asset_id = "23994765257735901643289866175445839985500568899141362167867883906244447758525"

    async def main():
        async for update in listen_binary_market(no_asset_id, yes_asset_id):
            print(json.dumps(update, indent=4))

    asyncio.run(main())
