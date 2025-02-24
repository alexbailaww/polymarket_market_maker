import time
import logging
from rich.logging import RichHandler
# from rich import print
from rich.progress import track

import asyncio

from utils.order import create_and_submit_order, cancel_order, get_market_active_orders, cancel_all_orders, cancel_market_orders
from utils.prices_and_books import get_market_price
from utils.market_listener import market_events, get_best_bid_ask, get_tick_size_change, get_trades

logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])

# async def run_async_bot_bidOnly(client, market):
#     try:
#         # Extract necessary market parameters.
#         event = market['question']
#         market_id = market['condition_id']
#         market_min_order_size = market['rewards']['min_size']
#         market_min_tick_size = market['minimum_tick_size']
#         market_spread = market['rewards']['max_spread']

#         # Choose the token with the lower price.
#         if market['tokens'][0]['price'] < market['tokens'][1]['price']:
#             outcome = market['tokens'][0]['outcome']
#             token_id = market['tokens'][0]['token_id']
#         else:
#             outcome = market['tokens'][1]['outcome']
#             token_id = market['tokens'][1]['token_id']

#         market_name = f"{event} ({outcome})"

#         # Determine how many ticks to offset below the best bid.
#         TOP_BOOK_TICKS = 2 if market_spread <= 1 else 1

#         # Flags and state variables.
#         initial_order_placed = False
#         current_best_bid = None

#         # Listen to market updates for the specific token.
#         async for market_data in get_best_bid_ask([token_id]):
#             new_best_bid = float(market_data['best_bid'])
#             new_order_price = new_best_bid - (TOP_BOOK_TICKS * market_min_tick_size)

#             # First read: place an initial order.
#             if not initial_order_placed:
#                 current_best_bid = new_best_bid
#                 logging.info(f"[bold green]First market read:[/bold green] Best bid = {new_best_bid}")
#                 logging.info(f"({market_name}): Placing initial order under best bid at price {new_order_price} "
#                       f"for {market_min_order_size} shares. Total cost: {market_min_order_size * new_order_price}")
#                 # Uncomment the next line when ready to place an order.
#                 # await create_and_submit_order(client, token_id, 'BUY', new_order_price, market_min_order_size)
#                 initial_order_placed = True

#             # Subsequent market updates.
#             else:
#                 # If the best bid has changed enough, update our order.
#                 if abs(current_best_bid - new_best_bid) > (market_min_tick_size * TOP_BOOK_TICKS):
#                     logging.info(f"({market_name}): [bold yellow]Price change:[/bold yellow] Best bid changed from {current_best_bid} to {new_best_bid}")
                    
#                     # Attempt to cancel any existing buy order.
#                     current_orders = get_market_active_orders(client, market_id)
#                     current_buy_order = None
#                     for order in current_orders:
#                         if order['side'].lower() == 'buy':
#                             current_buy_order = order
#                             break

#                     if current_buy_order:
#                         logging.info(f"({market_name}): Cancelling existing buy order with ID: {current_buy_order['id']}")
#                         # Uncomment the next line when ready to cancel the order.
#                         # cancel_order(client, current_buy_order['id'])
#                     else:
#                         logging.info(f"({market_name}): No active buy order found to cancel.")

#                     # Place a new order under the new best bid.
#                     logging.info(f"({market_name}): Placing new order under best bid at price {new_order_price} "
#                           f"for {market_min_order_size} shares. Total cost: {market_min_order_size * new_order_price}")
#                     # Uncomment the next line when ready to place a new order.
#                     # await create_and_submit_order(client, token_id, 'BUY', new_order_price, market_min_order_size)
                    
#                     # Update the current best bid for subsequent comparisons.
#                     current_best_bid = new_best_bid
#                 else:
#                     logging.info(f"({market_name}): [bold bright_cyan]No price change:[/bold bright_cyan] current best bid {current_best_bid}, new best bid {new_best_bid}. "
#                           f"Keeping order at {new_order_price}")

#     except KeyboardInterrupt:
#         logging.info(f"({market_name}): Shutdown signal received. Cancelling all orders...")
#         cancel_all_orders(client)
#         logging.info(f"({market_name}): Shutting Down...")
#         time.sleep(1)
#         logging.info(f"({market_name}): Cya!")

async def run_async_bot_bidAndTick(client, market):
    try:
        # Extract market parameters.
        event_text = market['question']
        market_id = market['condition_id']
        market_slug = market['market_slug']
        market_min_order_size = market['rewards']['min_size']
        market_spread = market['rewards']['max_spread']

        logging.info(f"[bold green]Bot started.[/bold green]", exc_info=True, extra={"bot_slug": market_slug})

        # Choose the token with the lower price.
        if market['tokens'][0]['price'] < market['tokens'][1]['price']:
            outcome = market['tokens'][0]['outcome']
            token_id = market['tokens'][0]['token_id']
        else:
            outcome = market['tokens'][1]['outcome']
            token_id = market['tokens'][1]['token_id']

        market_name = f"{event_text} ({outcome})"

        # Determine how many ticks to offset below the best bid.
        TOP_BOOK_TICKS = 2 if market_spread <= 1 else 1

        # Initialize state variables.
        current_min_tick_size = float(market.get('minimum_tick_size', 0.01))
        current_best_bid = None
        current_order_price = None
        initial_order_placed = False

        # Create a shared asyncio.Queue to receive both kinds of events.
        event_queue = asyncio.Queue()

        # Listener for best bid events.
        async def best_bid_listener():
            async for data in get_best_bid_ask([token_id]):
                # Mark the event type so we can distinguish later.
                await event_queue.put({"type": "best_bid", **data})

        # Listener for tick size change events.
        async def tick_size_listener():
            async for data in get_tick_size_change([token_id]):
                await event_queue.put({"type": "tick_size", **data})

        # Listener for trade events.
        async def trade_listener():
            async for data in get_trades([market_id]):
                await event_queue.put({"type": "fill", **data})

        # Start both listener tasks.
        best_bid_task = asyncio.create_task(best_bid_listener())
        tick_size_task = asyncio.create_task(tick_size_listener())
        fill_task = asyncio.create_task(trade_listener())

        # Process incoming events as they arrive.
        while True:
            event_data = await event_queue.get()

            # --- Process best bid events ---
            if event_data["type"] == "best_bid":
                new_best_bid = float(event_data["best_bid"])
                if not initial_order_placed:
                    # First market read: place the initial order.
                    current_best_bid = new_best_bid
                    current_order_price = new_best_bid - (TOP_BOOK_TICKS * current_min_tick_size)
                    logging.info(f"[bold green]Initial market read:[/bold green] Best bid = [bold bright_cyan]{new_best_bid}[/bold bright_cyan], Tick size = [bold bright_cyan]{current_min_tick_size}[/bold bright_cyan]", exc_info=True, extra={"bot_slug": market_slug})
                    logging.info(f"[bold green]Placing initial order[/bold green] at price [bold bright_cyan]{current_order_price}[/bold bright_cyan] for [bold bright_cyan]{3 * market_min_order_size}[/bold bright_cyan] shares. Total: [bold bright_cyan]{3 * market_min_order_size * current_order_price:3f}[/bold bright_cyan]", exc_info=True, extra={"bot_slug": market_slug})
                    # Uncomment the following when ready:
                    # await create_and_submit_order(client, token_id, 'BUY', current_order_price, 3 * market_min_order_size)
                    initial_order_placed = True
                else:
                    # For subsequent best bid updates, decide if the order should be updated.
                    threshold = current_min_tick_size * TOP_BOOK_TICKS
                    if abs(new_best_bid - current_best_bid) > threshold:
                        logging.info(f"[bold yellow]Best bid update event:[/bold yellow] from [bold bright_cyan]{current_best_bid}[/bold bright_cyan] to [bold bright_cyan]{new_best_bid}[/bold bright_cyan]", exc_info=True, extra={"bot_slug": market_slug})
                        current_best_bid = new_best_bid
                        new_order_price = new_best_bid - (TOP_BOOK_TICKS * current_min_tick_size)
                        if abs(new_order_price - current_order_price) > threshold:
                            # Cancel the existing order if it exists.
                            current_orders = get_market_active_orders(client, market_id)
                            logging.info(f"Orders retrieved.", exc_info=True, extra={"bot_slug": market_slug})
                            current_buy_order = None
                            for order in current_orders:
                                if order['side'].lower() == 'buy':
                                    current_buy_order = order
                                    break
                            if current_buy_order:
                                logging.info(f"[bold yellow]Cancelling order[/bold yellow] ID {current_buy_order['id']} due to best bid change.", exc_info=True, extra={"bot_slug": market_slug})
                                # Uncomment when ready:
                                # await cancel_order(client, current_buy_order['id'])
                            logging.info(f"[bold yellow]Placing new order[/bold yellow] at price [bold bright_cyan]{new_order_price}[/bold bright_cyan] for [bold bright_cyan]{3 * market_min_order_size}[/bold bright_cyan] shares. Total: [bold bright_cyan]{3 * market_min_order_size * new_order_price:3f}[/bold bright_cyan]", exc_info=True, extra={"bot_slug": market_slug})
                            # Uncomment when ready:
                            # await create_and_submit_order(client, token_id, 'BUY', new_order_price, 3 * market_min_order_size)
                            current_order_price = new_order_price
                        else:
                            logging.info(f"Best bid updated but the order price remains effectively unchanged.", exc_info=True, extra={"bot_slug": market_slug})
                    else:
                        logging.info(f"Received best bid [bold bright_cyan]{new_best_bid}[/bold bright_cyan] but no significant change (threshold [bold bright_cyan]{threshold}[/bold bright_cyan]).", exc_info=True, extra={"bot_slug": market_slug})

            # --- Process tick size change events ---
            elif event_data["type"] == "tick_size":
                new_tick_size = float(event_data["new_tick_size"])
                if new_tick_size != current_min_tick_size:
                    logging.info(f"[bold yellow]Tick size change event:[/bold yellow] Changing tick size from [bold bright_cyan]{current_min_tick_size}[/bold bright_cyan] to [bold bright_cyan]{new_tick_size}[/bold bright_cyan]", exc_info=True, extra={"bot_slug": market_slug})
                    current_min_tick_size = new_tick_size
                    # If an order is active and we know the best bid, recalculate the order price.
                    if current_best_bid is not None and initial_order_placed:
                        new_order_price = current_best_bid - (TOP_BOOK_TICKS * current_min_tick_size)
                        threshold = current_min_tick_size * TOP_BOOK_TICKS
                        if abs(new_order_price - current_order_price) > threshold:
                            current_orders = get_market_active_orders(client, market_id)
                            logging.info(f"Orders retrieved.", exc_info=True, extra={"bot_slug": market_slug})
                            current_buy_order = None
                            for order in current_orders:
                                if order['side'].lower() == 'buy':
                                    current_buy_order = order
                                    break
                            if current_buy_order:
                                logging.info(f"[bold yellow]Cancelling order[/bold yellow] ID {current_buy_order['id']} due to tick size change.", exc_info=True, extra={"bot_slug": market_slug})
                                # Uncomment when ready:
                                # await cancel_order(client, current_buy_order['id'])
                            logging.info(f"[bold yellow]Placing new order[/bold yellow] at price [bold bright_cyan]{new_order_price}[/bold bright_cyan] for [bold bright_cyan]{3 * market_min_order_size}[/bold bright_cyan] shares. Total: [bold bright_cyan]{3 * market_min_order_size * new_order_price:3f}[/bold bright_cyan]", exc_info=True, extra={"bot_slug": market_slug})
                            # Uncomment when ready:
                            # await create_and_submit_order(client, token_id, 'BUY', new_order_price, 3 * market_min_order_size)
                            current_order_price = new_order_price
                        else:
                            logging.info(f"Tick size updated but the order price remains effectively unchanged.", exc_info=True, extra={"bot_slug": market_slug})
                    else:
                        logging.info(f"Tick size updated, but no active order exists yet.", exc_info=True, extra={"bot_slug": market_slug})

            elif event_data["type"] == "fill":
                logging.info(f"Order from '[bold pink]{market_slug}[/bold pink]' has been filled for [bold bright_cyan]{event_data["size"]}[/bold bright_cyan] shares", exc_info=True, extra={"bot_slug": market_slug}) 
                logging.info(f"Shutting down bot for '[bold red]{market_slug}[/bold red]'", exc_info=True, extra={"bot_slug": market_slug})
                # Cancel all listener tasks.
                best_bid_task.cancel()
                tick_size_task.cancel()
                fill_task.cancel()
                break

    except KeyboardInterrupt:
        logging.info(f"Shutdown signal received. Cancelling all orders from {market_slug}'...", exc_info=True, extra={"bot_slug": market_slug})
        cancel_market_orders(client, market_id, token_id)
        logging.info(f"Shutting Down...", exc_info=True, extra={"bot_slug": market_slug})
        time.sleep(1)
        logging.info(f"Cya!", exc_info=True, extra={"bot_slug": market_slug}) 