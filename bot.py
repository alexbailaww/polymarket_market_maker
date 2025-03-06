import time
import logging
from rich.logging import RichHandler
# from rich import print
from rich.progress import track
import json

import asyncio

from utils.order import create_and_submit_order, cancel_order, get_market_active_orders, cancel_all_orders, cancel_market_orders
from utils.prices_and_books import get_market_price
from utils.market_listener import get_best_bid_ask, get_tick_size_change, get_trades
from utils.balance import fetch_balance

from order_quantities import order_quantities

from py_clob_client.exceptions import PolyApiException

logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])

async def run_single_side(client, market, token_index: int):
    """
    Handles the entire bot logic for *one* side (one token_id) of the market,
    in its own task. All log messages are prefixed by "(outcome)" in bold magenta.
    """
    try:
        market_slug = market["market_slug"]
        market_id   = market["condition_id"]
        market_min_order_size = market["rewards"]["min_size"]

        # Assign a default order quantity for this market (shared storage).
        if market_slug not in order_quantities:
            order_quantities[market_slug] = market_min_order_size

        # Extract the specific token/outcome we're going to trade
        token_info = market["tokens"][token_index]
        token_id   = token_info["token_id"]
        outcome    = token_info["outcome"]

        # Prefix for logs
        prefix = f"[bold magenta]({outcome})[/bold magenta]"

        # We'll update TOP_BOOK_TICKS based on the current market spread.
        # Initially, we use the static value, but it will be updated on every event.
        TOP_BOOK_TICKS = 2  # default; will be updated after first bid/ask event

        # Initialize state variables
        current_min_tick_size   = float(market.get("minimum_tick_size", 0.01))
        current_best_bid        = None
        current_order_price     = None
        initial_order_placed    = False
        current_order_quantity  = None
        last_invalid_quantity   = None  # for ignoring repeated invalid qty updates

        def get_order_quantity():
            return order_quantities.get(market_slug, market_min_order_size)

        # We'll queue up events from listeners:
        event_queue = asyncio.Queue()

        # ------------------ Listeners for *this* token_id ------------------

        async def best_bid_listener():
            # Subscribe to best-bid/ask changes for just this token_id.
            async for data in get_best_bid_ask([token_id]):
                # Tag the event with type = "best_bid"
                await event_queue.put({"type": "best_bid", **data})

        async def tick_size_listener():
            # Subscribe to tick-size changes for just this token_id
            async for data in get_tick_size_change([token_id]):
                await event_queue.put({"type": "tick_size", **data})

        async def trade_listener():
            # For trades/fills, we subscribe at market_id level
            # then filter if the fill is for *this* token_id
            async for data in get_trades([market_id]):
                if data.get("token_id") == token_id:
                    await event_queue.put({"type": "fill", **data})

        async def quantity_update_listener():
            # Periodically check if user changed the order quantity
            while True:
                await asyncio.sleep(0.5)
                updated_qty = get_order_quantity()
                if initial_order_placed and updated_qty != current_order_quantity:
                    await event_queue.put({
                        "type": "quantity_update",
                        "updated_qty": updated_qty,
                        "token_id": token_id
                    })

        # ------------------ Spawn these listeners as tasks ------------------
        best_bid_task = asyncio.create_task(best_bid_listener())
        tick_size_task = asyncio.create_task(tick_size_listener())
        fill_task      = asyncio.create_task(trade_listener())
        qty_task       = asyncio.create_task(quantity_update_listener())

        logging.info(
            f"{prefix} [bold green]Bot started[/bold green]",
            extra={"bot_slug": market_slug},
        )

        # ------------------ Main loop: process events ------------------
        while True:
            event_data = await event_queue.get()
            e_type = event_data["type"]

            if e_type == "best_bid":
                # Now we extract both best_bid and best_ask from the event
                new_best_bid = float(event_data["best_bid"])
                new_best_ask = float(event_data["best_ask"])
                # Compute the current market spread dynamically
                new_market_spread = new_best_ask - new_best_bid
                # Update TOP_BOOK_TICKS based on the new spread
                TOP_BOOK_TICKS = 1 if new_market_spread <= 1 else 2

                logging.info(
                    f"{prefix} [bold green]Market spread updated:[/bold green] "
                    f"best_ask - best_bid = [bold cyan]{new_market_spread:.3f}[/bold cyan]",
                    extra={"bot_slug": market_slug},
                )

                if not initial_order_placed:
                    current_best_bid = new_best_bid
                    current_order_price = new_best_bid - (TOP_BOOK_TICKS * current_min_tick_size)
                    order_qty = get_order_quantity()

                    logging.info(
                        f"{prefix} [bold green]Initial read:[/bold green] "
                        f"best_bid=[bold cyan]{new_best_bid:.3f}[/bold cyan], best_ask=[bold cyan]{new_best_ask:.3f}[/bold cyan], "
                        f"tick_size=[bold cyan]{current_min_tick_size:.3f}[/bold cyan]",
                        extra={"bot_slug": market_slug},
                    )
                    logging.info(
                        f"{prefix} [bold green]Placing initial BUY[/bold green] at "
                        f"[bold cyan]{current_order_price:.3f}[/bold cyan], qty=[bold cyan]{order_qty}[/bold cyan]. "
                        f"Total=[bold cyan]{order_qty * current_order_price:.3f}[/bold cyan]",
                        extra={"bot_slug": market_slug},
                    )
                    try:
                        create_and_submit_order(client, token_id, "BUY", current_order_price, order_qty)
                    except PolyApiException as e:
                        if "not enough balance" in str(e).lower():
                            logging.info(
                                f"{prefix} [bold red]Too expensive![/bold red] "
                                f"Cannot place initial order. Shutting down this side.",
                                extra={"bot_slug": market_slug},
                            )
                            cancel_market_orders(client, market_id, token_id)
                            # Cancel tasks associated with this side
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
                    # If best bid changes significantly, we may want to move our order
                    threshold = current_min_tick_size * TOP_BOOK_TICKS
                    if abs(new_best_bid - current_best_bid) > threshold:
                        logging.info(
                            f"{prefix} [bold yellow]Best-bid update:[/bold yellow] "
                            f"{current_best_bid:.3f} -> [bold cyan]{new_best_bid:.3f}[/bold cyan]",
                            extra={"bot_slug": market_slug},
                        )
                        current_best_bid = new_best_bid
                        new_order_price = new_best_bid - (TOP_BOOK_TICKS * current_min_tick_size)

                        if abs(new_order_price - current_order_price) > threshold:
                            order_qty = get_order_quantity()
                            current_orders = get_market_active_orders(client, market_id)
                            logging.info(
                                f"{prefix} Orders retrieved.",
                                extra={"bot_slug": market_slug},
                            )
                            current_buy_order = next(
                                (o for o in current_orders if o["side"].lower() == "buy" and o["asset_id"] == token_id),
                                None,
                            )
                            if current_buy_order:
                                logging.info(
                                    f"{prefix} [bold yellow]Cancelling old BUY[/bold yellow] due to best-bid change.",
                                    extra={"bot_slug": market_slug},
                                )
                                cancel_order(client, current_buy_order["id"])
                            logging.info(
                                f"{prefix} [bold yellow]Placing new BUY[/bold yellow] "
                                f"at [bold cyan]{new_order_price:.3f}[/bold cyan], qty=[bold cyan]{order_qty}[/bold cyan]. "
                                f"Total=[bold cyan]{order_qty * new_order_price:.3f}[/bold cyan]",
                                extra={"bot_slug": market_slug},
                            )
                            try:
                                create_and_submit_order(client, token_id, "BUY", new_order_price, order_qty)
                            except PolyApiException as e:
                                if "not enough balance" in str(e).lower():
                                    logging.info(
                                        f"{prefix} [bold red]Too expensive![/bold red] "
                                        f"Cannot update BUY; shutting down.",
                                        extra={"bot_slug": market_slug},
                                    )
                                    cancel_market_orders(client, market_id, token_id)
                                    break
                                else:
                                    raise
                            current_order_price = new_order_price
                            current_order_quantity = order_qty
                        else:
                            logging.info(
                                f"{prefix} Best-bid changed but below threshold, ignoring.",
                                extra={"bot_slug": market_slug},
                            )
                    else:
                        logging.info(
                            f"{prefix} Best-bid changed but below threshold, ignoring.",
                            extra={"bot_slug": market_slug},
                        )

            elif e_type == "tick_size":
                new_tick_size = float(event_data["new_tick_size"])
                if new_tick_size != current_min_tick_size:
                    logging.info(
                        f"{prefix} [bold yellow]Tick-size change:[/bold yellow] "
                        f"{current_min_tick_size:.3f} -> [bold cyan]{new_tick_size:.3f}[/bold cyan]",
                        extra={"bot_slug": market_slug},
                    )
                    current_min_tick_size = new_tick_size

                    if current_best_bid is not None and initial_order_placed:
                        new_order_price = current_best_bid - (TOP_BOOK_TICKS * current_min_tick_size)
                        threshold       = current_min_tick_size * TOP_BOOK_TICKS
                        if abs(new_order_price - current_order_price) > threshold:
                            order_qty = get_order_quantity()
                            current_orders = get_market_active_orders(client, market_id)
                            logging.info(
                                f"{prefix} Orders retrieved.",
                                extra={"bot_slug": market_slug},
                            )
                            current_buy_order = next(
                                (o for o in current_orders if o["side"].lower() == "buy" and o["asset_id"] == token_id),
                                None,
                            )
                            if current_buy_order:
                                logging.info(
                                    f"{prefix} [bold yellow]Cancelling BUY[/bold yellow] due to tick-size change.",
                                    extra={"bot_slug": market_slug},
                                )
                                cancel_order(client, current_buy_order["id"])
                            logging.info(
                                f"{prefix} [bold yellow]Placing new BUY[/bold yellow] "
                                f"at [bold cyan]{new_order_price:.3f}[/bold cyan], qty=[bold cyan]{order_qty}[/bold cyan]. "
                                f"Total=[bold cyan]{order_qty * new_order_price:.3f}[/bold cyan]",
                                extra={"bot_slug": market_slug},
                            )
                            try:
                                create_and_submit_order(client, token_id, "BUY", new_order_price, order_qty)
                            except PolyApiException as e:
                                if "not enough balance" in str(e).lower():
                                    logging.info(
                                        f"{prefix} [bold red]Too expensive![/bold red] "
                                        f"Cannot update order on tick-size change.",
                                        extra={"bot_slug": market_slug},
                                    )
                                    cancel_market_orders(client, market_id, token_id)
                                    break
                                else:
                                    raise
                            current_order_price = new_order_price
                            current_order_quantity = order_qty
                        else:
                            logging.info(
                                f"{prefix} Tick-size changed but order price unchanged.",
                                extra={"bot_slug": market_slug},
                            )
                    else:
                        logging.info(
                            f"{prefix} Tick-size update but no active order yet.",
                            extra={"bot_slug": market_slug},
                        )

            elif e_type == "fill":
                fill_size = event_data["size"]
                logging.info(
                    f"{prefix} [bold yellow]Fill event[/bold yellow], size=[bold cyan]{fill_size}[/bold cyan]",
                    extra={"bot_slug": market_slug},
                )
                logging.info(
                    f"{prefix} [bold red]Shutting down bot side[/bold red] for {market_slug}",
                    extra={"bot_slug": market_slug},
                )
                cancel_market_orders(client, market_id, token_id)
                # Cancel tasks for this side, then break out of loop.
                best_bid_task.cancel()
                tick_size_task.cancel()
                fill_task.cancel()
                qty_task.cancel()
                break

            elif e_type == "quantity_update":
                updated_qty = event_data["updated_qty"]
                t_id = event_data["token_id"]
                if updated_qty == current_order_quantity:
                    continue
                if last_invalid_quantity is not None and updated_qty == last_invalid_quantity:
                    continue

                logging.info(
                    f"{prefix} [bold magenta]Order quantity update[/bold magenta]: "
                    f"{current_order_quantity} -> [bold cyan]{updated_qty}[/bold cyan]",
                    extra={"bot_slug": market_slug},
                )

                current_balance = fetch_balance()

                if (updated_qty * current_order_price) > current_balance:
                    max_qty = int(current_balance // current_order_price)
                    logging.info(
                        f"{prefix} [bold red]Order qty update failed![/bold red] "
                        f"Cost=[bold cyan]{updated_qty * current_order_price:.3f}[/bold cyan] "
                        f"exceeds balance=[bold cyan]{current_balance}[/bold cyan]. "
                        f"Max allowable=[bold cyan]{max_qty}[/bold cyan]. Keeping old qty.",
                        extra={"bot_slug": market_slug},
                    )
                    last_invalid_quantity = updated_qty
                    continue
                last_invalid_quantity = None

                current_orders = get_market_active_orders(client, market_id)
                for idx, o in enumerate(current_orders):
                    if "token_id" not in o:
                        print("Order at index", idx, "is missing token_id:", o)
                current_buy_order = next(
                    (o for o in current_orders if o["side"].lower() == "buy" and o["asset_id"] == token_id),
                    None,
                )
                if current_buy_order:
                    logging.info(
                        f"{prefix} [bold yellow]Cancelling BUY[/bold yellow] due to qty update.",
                        extra={"bot_slug": market_slug},
                    )
                    cancel_order(client, current_buy_order["id"])
                logging.info(
                    f"{prefix} [bold yellow]Placing updated BUY[/bold yellow] at "
                    f"[bold cyan]{current_order_price:.3f}[/bold cyan], qty=[bold cyan]{updated_qty}[/bold cyan]. "
                    f"Total=[bold cyan]{updated_qty * current_order_price:.3f}[/bold cyan]",
                    extra={"bot_slug": market_slug},
                )
                try:
                    create_and_submit_order(client, token_id, "BUY", current_order_price, updated_qty)
                except PolyApiException as e:
                    if "not enough balance / allowance" in str(e).lower():
                        logging.info(
                            f"{prefix} [bold red]Too expensive![/bold red] "
                            f"Cannot update order on quantity change.",
                            extra={"bot_slug": market_slug},
                        )
                        cancel_market_orders(client, market_id, token_id)
                        continue
                    else:
                        raise
                current_order_quantity = updated_qty

    except asyncio.CancelledError:
        logging.info(
            f"[bold magenta]({outcome})[/bold magenta] [bold red]Bot cancelled[/bold red] for {market['market_slug']}",
            extra={"bot_slug": market["market_slug"]},
        )
        raise
    except Exception as e:
        logging.error(
            f"[bold magenta]({outcome})[/bold magenta] Unexpected error: {e}",
            extra={"bot_slug": market["market_slug"]},
        )
        raise
    except PolyApiException as e:
        if "not enough balance / allowance" in str(e).lower():
            logging.info(
                f"{prefix} [bold red]Balance / Allowance Error[/bold red] ",
                extra={"bot_slug": market_slug},
            )
        else:
            raise
    finally:
        # Runs whether the bot stops due to fill, cancellation, or error.
        logging.info(
            f"[bold magenta]({outcome})[/bold magenta] [bold red]Cancelling any remaining orders.[/bold red]",
            extra={"bot_slug": market["market_slug"]},
        )
        print("Cancelling orders for", market["market_slug"], token_index)
        # Retrieve active orders for this market
        current_orders = get_market_active_orders(client, market["condition_id"])
        # Filter the order for this specific token (side)
        order_to_cancel = next(
            (o for o in current_orders if o["side"].lower() == "buy" and o["asset_id"] == market["tokens"][token_index]["token_id"]),
            None
        )
        if order_to_cancel:
            cancel_order(client, order_to_cancel["id"])
            print(f"Cancelled order {order_to_cancel['id']} for token {market['tokens'][token_index]['token_id']}")
        else:
            print("No active order found for this side.")


async def run_async_bot_bidAndTick(client, market):
    """
    Spawns TWO parallel tasks: one for each outcome/token_id in the market.

    If you want to shut down the entire bot as soon as one side is filled,
    you can do so by:
      - Cancelling all tasks from within one side’s 'fill' event, or
      - Checking the gather result, etc.

    This example simply runs each side until it is filled or fails.
    """

    tasks = []
    for token_index in range(len(market["tokens"])):
        tasks.append(asyncio.create_task(run_single_side(client, market, token_index)))

    # Let them run in parallel
    await asyncio.gather(*tasks)
