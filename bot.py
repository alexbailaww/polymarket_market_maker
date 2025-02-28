import time
import logging
from rich.logging import RichHandler
# from rich import print
from rich.progress import track

import asyncio

from utils.order import create_and_submit_order, cancel_order, get_market_active_orders, cancel_all_orders, cancel_market_orders
from utils.prices_and_books import get_market_price
from utils.market_listener import market_events, get_best_bid_ask, get_tick_size_change, get_trades
from utils.balance import fetch_balance

from order_quantities import order_quantities

from py_clob_client.exceptions import PolyApiException

logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])

async def run_async_bot_bidAndTick(client, market):
    try:
        # At the top of your bot.py (or within an appropriate scope), initialize a variable:
        last_invalid_quantity = None
        # Extract market parameters.
        event_text = market['question']
        market_id = market['condition_id']
        market_slug = market['market_slug']
        market_min_order_size = market['rewards']['min_size']
        market_spread = market['rewards']['max_spread']

        order_quantities[market_slug] = market_min_order_size

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

        def get_order_quantity():
            return order_quantities.get(market_slug, market_min_order_size)

        event_queue = asyncio.Queue()

        async def best_bid_listener():
            async for data in get_best_bid_ask([token_id]):
                await event_queue.put({"type": "best_bid", **data})

        async def tick_size_listener():
            async for data in get_tick_size_change([token_id]):
                await event_queue.put({"type": "tick_size", **data})

        async def trade_listener():
            async for data in get_trades([market_id]):
                await event_queue.put({"type": "fill", **data})

        async def quantity_update_listener():
            while True:
                await asyncio.sleep(0.5)
                updated_qty = get_order_quantity()
                if initial_order_placed and updated_qty != current_order_quantity:
                    await event_queue.put({"type": "quantity_update", "updated_qty": updated_qty})

        best_bid_task = asyncio.create_task(best_bid_listener())
        tick_size_task = asyncio.create_task(tick_size_listener())
        fill_task = asyncio.create_task(trade_listener())
        qty_task = asyncio.create_task(quantity_update_listener())

        while True:
            event_data = await event_queue.get()

            if event_data["type"] == "best_bid":
                new_best_bid = float(event_data["best_bid"])
                if not initial_order_placed:
                    current_best_bid = new_best_bid
                    current_order_price = new_best_bid - (TOP_BOOK_TICKS * current_min_tick_size)
                    order_qty = get_order_quantity()
                    logging.info(
                        f"[bold green]Initial market read:[/bold green] Best bid = [bold cyan]{new_best_bid:3f}[/bold cyan], Tick size = [bold cyan]{current_min_tick_size:3f}[/bold cyan]",
                        extra={"bot_slug": market_slug})
                    logging.info(
                        f"[bold green]Placing initial order[/bold green] at price [bold cyan]{current_order_price:3f}[/bold cyan] for [bold cyan]{order_qty}[/bold cyan] shares. Total: [bold cyan]{order_qty * current_order_price:3f}[/bold cyan]",
                        extra={"bot_slug": market_slug})
                    try:
                        create_and_submit_order(client, token_id, 'BUY', current_order_price, order_qty)
                    except PolyApiException as e:
                        if 'not enough balance' in str(e).lower():
                            logging.info(f"[bold red]Market too expensive![/bold red] Cannot place initial order for {market_slug}.", extra={"bot_slug": market_slug})
                            cancel_market_orders(client, market_id, token_id)
                            best_bid_task.cancel()
                            tick_size_task.cancel()
                            fill_task.cancel()
                            qty_task.cancel()
                            break
                        else:
                            raise
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
                                    f"[bold yellow]Cancelling order[/bold yellow] due to best bid change.",
                                    extra={"bot_slug": market_slug})
                                cancel_order(client, current_buy_order['id'])
                            logging.info(
                                f"[bold yellow]Placing new order[/bold yellow] at price [bold cyan]{new_order_price:3f}[/bold cyan] for [bold cyan]{order_qty}[/bold cyan] shares. Total: [bold cyan]{order_qty * new_order_price:3f}[/bold cyan]",
                                extra={"bot_slug": market_slug})
                            try:
                                create_and_submit_order(client, token_id, 'BUY', new_order_price, order_qty)
                            except PolyApiException as e:
                                if 'not enough balance' in str(e).lower():
                                    logging.info(f"[bold red]Market too expensive![/bold red] Cannot update order on best bid change for {market_slug}.", extra={"bot_slug": market_slug})
                                    cancel_market_orders(client, market_id, token_id)
                                    best_bid_task.cancel()
                                    tick_size_task.cancel()
                                    fill_task.cancel()
                                    qty_task.cancel()
                                    break
                                else:
                                    raise
                            current_order_price = new_order_price
                            current_order_quantity = order_qty
                        else:
                            logging.info("Best bid update but order price unchanged.", extra={"bot_slug": market_slug})
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
                                    f"[bold yellow]Cancelling order[/bold yellow] due to tick size change.",
                                    extra={"bot_slug": market_slug})
                                cancel_order(client, current_buy_order['id'])
                            logging.info(
                                f"[bold yellow]Placing new order[/bold yellow] at price [bold cyan]{new_order_price:3f}[/bold cyan] for [bold cyan]{order_qty}[/bold cyan] shares. Total: [bold cyan]{order_qty * new_order_price:3f}[/bold cyan]",
                                extra={"bot_slug": market_slug})
                            try:
                                create_and_submit_order(client, token_id, 'BUY', new_order_price, order_qty)
                            except PolyApiException as e:
                                if 'not enough balance' in str(e).lower():
                                    logging.info(f"[bold red]Market too expensive![/bold red] Cannot update order on tick size change for {market_slug}.", extra={"bot_slug": market_slug})
                                    cancel_market_orders(client, market_id, token_id)
                                    best_bid_task.cancel()
                                    tick_size_task.cancel()
                                    fill_task.cancel()
                                    qty_task.cancel()
                                    break
                                else:
                                    raise
                            current_order_price = new_order_price
                            current_order_quantity = order_qty
                        else:
                            logging.info("Tick size update but order price unchanged.", extra={"bot_slug": market_slug})
                    else:
                        logging.info("Tick size update but no active order.", extra={"bot_slug": market_slug})

            elif event_data["type"] == "fill":
                logging.info(
                    f"Order from {market_slug} [bold yellow]filled[/bold yellow] for [bold cyan]{event_data['size']}[/bold cyan] shares",
                    extra={"bot_slug": market_slug})
                logging.info(
                    f"[bold red]Shutting down bot[/bold red] for {market_slug}",
                    extra={"bot_slug": market_slug})
                cancel_market_orders(client, market_id, token_id)
                best_bid_task.cancel()
                tick_size_task.cancel()
                fill_task.cancel()
                qty_task.cancel()
                break

            # Within the main event loop inside run_async_bot_bidAndTick:
            elif event_data["type"] == "quantity_update":
                updated_qty = event_data["updated_qty"]
                
                # If no change, skip processing.
                if updated_qty == current_order_quantity:
                    continue

                # If the same invalid quantity was already processed, skip to avoid spamming logs.
                if last_invalid_quantity is not None and updated_qty == last_invalid_quantity:
                    continue

                logging.info(
                    f"[bold magenta]Order quantity update[/bold magenta] detected: current [bold cyan]{current_order_quantity}[/bold cyan] -> new [bold cyan]{updated_qty}[/bold cyan]",
                    extra={"bot_slug": market_slug})
                
                current_balance = fetch_balance()
                # Check if the updated order cost would exceed the balance.
                if (updated_qty * current_order_price) > current_balance:
                    max_qty = int(current_balance // current_order_price)
                    logging.info(
                        f"[bold red]Order quantity update failed![/bold red] The updated order cost ([bold cyan]{updated_qty * current_order_price:.3f}[/bold cyan]) exceeds your balance ([bold cyan]{current_balance}[/bold cyan]). "
                        f"[bold red]Maximum allowable quantity is[/bold red] [bold cyan]{max_qty}[/bold cyan]. Please update the quantity accordingly.",
                        extra={"bot_slug": market_slug})
                    logging.info(
                        f"[bold yellow]Keeping previous order active[/bold yellow] at price [bold cyan]{current_order_price:3f}[/bold cyan] for [bold cyan]{current_order_quantity}[/bold cyan] shares. Total: [bold cyan]{current_order_quantity * current_order_price:3f}[/bold cyan]",
                        extra={"bot_slug": market_slug})
                    # Store this invalid value so that repeated updates with the same value are ignored.
                    last_invalid_quantity = updated_qty
                    continue  # Do not update the order; wait for the UI to send a new value.
                
                # If we get here, the new quantity is valid; clear the invalid flag.
                last_invalid_quantity = None

                current_orders = get_market_active_orders(client, market_id)
                current_buy_order = next((order for order in current_orders if order['side'].lower() == 'buy'), None)
                if current_buy_order:
                    logging.info(
                        f"[bold yellow]Cancelling order[/bold yellow] due to quantity update.",
                        extra={"bot_slug": market_slug})
                    cancel_order(client, current_buy_order['id'])
                logging.info(
                    f"[bold yellow]Placing updated order[/bold yellow] at price [bold cyan]{current_order_price:3f}[/bold cyan] for [bold cyan]{updated_qty}[/bold cyan] shares. Total: [bold cyan]{updated_qty * current_order_price:3f}[/bold cyan]",
                    extra={"bot_slug": market_slug})
                try:
                    create_and_submit_order(client, token_id, 'BUY', current_order_price, updated_qty)
                except PolyApiException as e:
                    if 'not enough balance / allowance' in str(e).lower():
                        logging.info(f"[bold red]Allowance problem![/bold red] Cannot update order quantity for {market_slug} due to insufficient allowance.",
                                    extra={"bot_slug": market_slug})
                        cancel_market_orders(client, market_id, token_id)
                        continue
                    else:
                        raise
                current_order_quantity = updated_qty

    except asyncio.CancelledError:
        logging.info(f"Bot for {market['market_slug']} cancelled.", extra={"bot_slug": market['market_slug']})
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}", extra={"bot_slug": market['market_slug']})
        raise
    finally:
        # This block will run whether the bot stops due to a fill, cancellation, or error.
        logging.info(f"Cancelling orders for {market['market_slug']}.", extra={"bot_slug": market['market_slug']})
        # Make sure you cancel orders on both sides if you go dual–side later.
        cancel_market_orders(client, market_id, market['tokens'][0]['token_id'])
