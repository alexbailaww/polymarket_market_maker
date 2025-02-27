import time
import logging
from rich.logging import RichHandler
# from rich import print
from rich.progress import track

import asyncio

from utils.order import create_and_submit_order, cancel_order, get_market_active_orders, cancel_all_orders, cancel_market_orders
from utils.prices_and_books import get_market_price
from utils.market_listener import market_events, get_best_bid_ask, get_tick_size_change, get_trades

from order_quantities import order_quantities

logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])

async def run_async_bot_bidAndTick(client, market):
    try:
        # Extract market parameters.
        event_text = market['question']
        market_id = market['condition_id']
        market_slug = market['market_slug']
        market_min_order_size = market['rewards']['min_size']
        market_spread = market['rewards']['max_spread']

        logging.info(f"[bold green]Bot started.[/bold green]", extra={"bot_slug": market_slug})

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
        current_order_quantity = None  # The quantity used in the last placed order

        # Helper function to get the current order quantity (in shares).
        # Default is exactly the market_min_order_size if not updated.
        def get_order_quantity():
            return order_quantities.get(market_slug, market_min_order_size)

        # Create a shared asyncio.Queue to receive events.
        event_queue = asyncio.Queue()

        # Listener for best bid events.
        async def best_bid_listener():
            async for data in get_best_bid_ask([token_id]):
                await event_queue.put({"type": "best_bid", **data})

        # Listener for tick size change events.
        async def tick_size_listener():
            async for data in get_tick_size_change([token_id]):
                await event_queue.put({"type": "tick_size", **data})

        # Listener for trade events.
        async def trade_listener():
            async for data in get_trades([market_id]):
                await event_queue.put({"type": "fill", **data})

        # NEW: Listener for quantity update events.
        async def quantity_update_listener():
            while True:
                await asyncio.sleep(0.5)  # Poll every 0.5 seconds
                updated_qty = get_order_quantity()
                if initial_order_placed and updated_qty != current_order_quantity:
                    # Push a quantity update event into the event queue.
                    await event_queue.put({"type": "quantity_update", "updated_qty": updated_qty})

        # Start all listener tasks.
        best_bid_task = asyncio.create_task(best_bid_listener())
        tick_size_task = asyncio.create_task(tick_size_listener())
        fill_task = asyncio.create_task(trade_listener())
        qty_task = asyncio.create_task(quantity_update_listener())

        # Main event loop.
        while True:
            event_data = await event_queue.get()

            if event_data["type"] == "best_bid":
                new_best_bid = float(event_data["best_bid"])
                if not initial_order_placed:
                    # Place the initial order.
                    current_best_bid = new_best_bid
                    current_order_price = new_best_bid - (TOP_BOOK_TICKS * current_min_tick_size)
                    order_qty = get_order_quantity()
                    logging.info(
                        f"[bold green]Initial market read:[/bold green] Best bid = [bold cyan]{new_best_bid:3f}[/bold cyan], Tick size = [bold cyan]{current_min_tick_size:3f}[/bold cyan]",
                        extra={"bot_slug": market_slug})
                    logging.info(
                        f"[bold green]Placing initial order[/bold green] at price [bold cyan]{current_order_price:3f}[/bold cyan] for [bold cyan]{order_qty}[/bold cyan] shares. Total: [bold cyan]{order_qty * current_order_price:3f}[/bold cyan]",
                        extra={"bot_slug": market_slug})
                    # Uncomment when ready:
                    # await create_and_submit_order(client, token_id, 'BUY', current_order_price, order_qty)
                    initial_order_placed = True
                    current_order_quantity = order_qty
                else:
                    threshold = current_min_tick_size * TOP_BOOK_TICKS
                    if abs(new_best_bid - current_best_bid) > threshold:
                        logging.info(
                            f"[bold yellow]Best bid update:[/bold yellow] from [bold cyan]{current_best_bid:3f}[/bold cyan] to [bold cyan]{new_best_bid:3f}[/bold cyan]",
                            extra={"bot_slug": market_slug})
                        current_best_bid = new_best_bid
                        new_order_price = new_best_bid - (TOP_BOOK_TICKS * current_min_tick_size)
                        if abs(new_order_price - current_order_price) > threshold:
                            order_qty = get_order_quantity()
                            current_orders = get_market_active_orders(client, market_id)
                            logging.info("Orders retrieved.", extra={"bot_slug": market_slug})
                            current_buy_order = next((order for order in current_orders if order['side'].lower() == 'buy'), None)
                            if current_buy_order:
                                logging.info(
                                    f"[bold yellow]Cancelling order[/bold yellow] ID {current_buy_order['id']} due to best bid change.",
                                    extra={"bot_slug": market_slug})
                                # Uncomment when ready:
                                # await cancel_order(client, current_buy_order['id'])
                            logging.info(
                                f"[bold yellow]Placing new order[/bold yellow] at price [bold cyan]{new_order_price:3f}[/bold cyan] for [bold cyan]{order_qty}[/bold cyan] shares. Total: [bold cyan]{order_qty * new_order_price:3f}[/bold cyan]",
                                extra={"bot_slug": market_slug})
                            # Uncomment when ready:
                            # await create_and_submit_order(client, token_id, 'BUY', new_order_price, order_qty)
                            current_order_price = new_order_price
                            current_order_quantity = order_qty
                        else:
                            logging.info("Best bid update but order price unchanged.",
                                         extra={"bot_slug": market_slug})
                    else:
                        logging.info(
                            f"Best bid [bold cyan]{new_best_bid}[/bold cyan] received but below threshold (threshold [bold cyan]{threshold}[/bold cyan]).",
                            extra={"bot_slug": market_slug})

            elif event_data["type"] == "tick_size":
                new_tick_size = float(event_data["new_tick_size"])
                if new_tick_size != current_min_tick_size:
                    logging.info(
                        f"[bold yellow]Tick size change:[/bold yellow] from [bold cyan]{current_min_tick_size}[/bold cyan] to [bold cyan]{new_tick_size}[/bold cyan]",
                        extra={"bot_slug": market_slug})
                    current_min_tick_size = new_tick_size
                    if current_best_bid is not None and initial_order_placed:
                        new_order_price = current_best_bid - (TOP_BOOK_TICKS * current_min_tick_size)
                        threshold = current_min_tick_size * TOP_BOOK_TICKS
                        if abs(new_order_price - current_order_price) > threshold:
                            order_qty = get_order_quantity()
                            current_orders = get_market_active_orders(client, market_id)
                            logging.info("Orders retrieved.", extra={"bot_slug": market_slug})
                            current_buy_order = next((order for order in current_orders if order['side'].lower() == 'buy'), None)
                            if current_buy_order:
                                logging.info(
                                    f"[bold yellow]Cancelling order[/bold yellow] ID {current_buy_order['id']} due to tick size change.",
                                    extra={"bot_slug": market_slug})
                                # Uncomment when ready:
                                # await cancel_order(client, current_buy_order['id'])
                            logging.info(
                                f"[bold yellow]Placing new order[/bold yellow] at price [bold cyan]{new_order_price:3f}[/bold cyan] for [bold cyan]{order_qty}[/bold cyan] shares. Total: [bold cyan]{order_qty * new_order_price:3f}[/bold cyan]",
                                extra={"bot_slug": market_slug})
                            # Uncomment when ready:
                            # await create_and_submit_order(client, token_id, 'BUY', new_order_price, order_qty)
                            current_order_price = new_order_price
                            current_order_quantity = order_qty
                        else:
                            logging.info("Tick size update but order price unchanged.",
                                         extra={"bot_slug": market_slug})
                    else:
                        logging.info("Tick size update but no active order.",
                                     extra={"bot_slug": market_slug})

            elif event_data["type"] == "fill":
                logging.info(
                    f"Order from {market_slug} [bold yellow]filled[/bold yellow] for [bold cyan]{event_data['size']}[/bold cyan] shares",
                    extra={"bot_slug": market_slug})
                logging.info(
                    f"[bold red]Shutting down bot[/bold red] for {market_slug}",
                    extra={"bot_slug": market_slug})
                best_bid_task.cancel()
                tick_size_task.cancel()
                fill_task.cancel()
                qty_task.cancel()
                break

            elif event_data["type"] == "quantity_update":
                # Process a quantity update event.
                updated_qty = event_data["updated_qty"]
                logging.info(
                    f"[bold magenta]Order quantity update[/bold magenta] detected: current [bold cyan]{current_order_quantity}[/bold cyan] -> new [bold cyan]{updated_qty}[/bold cyan]",
                    extra={"bot_slug": market_slug})
                current_orders = get_market_active_orders(client, market_id)
                current_buy_order = next((order for order in current_orders if order['side'].lower() == 'buy'), None)
                if current_buy_order:
                    logging.info(
                        f"[bold yellow]Cancelling order[/bold yellow] ID {current_buy_order['id']} due to quantity update.",
                        extra={"bot_slug": market_slug})
                    # Uncomment when ready:
                    # await cancel_order(client, current_buy_order['id'])
                logging.info(
                    f"[bold yellow]Placing updated order[/bold yellow] at price [bold cyan]{current_order_price:3f}[/bold cyan] for [bold cyan]{updated_qty}[/bold cyan] shares. Total: [bold cyan]{updated_qty * current_order_price:3f}[/bold cyan]",
                    extra={"bot_slug": market_slug})
                # Uncomment when ready:
                # await create_and_submit_order(client, token_id, 'BUY', current_order_price, updated_qty)
                current_order_quantity = updated_qty

    except KeyboardInterrupt:
        logging.info(f"Shutdown signal received. Cancelling orders for {market_slug}...",
                     extra={"bot_slug": market_slug})
        cancel_market_orders(client, market_id, token_id)
        logging.info("Shutting Down...", extra={"bot_slug": market_slug})
        time.sleep(1)
        logging.info("Cya!", extra={"bot_slug": market_slug})