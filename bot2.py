# bot2.py
import time
import logging
from rich.logging import RichHandler
from rich.progress import track
import asyncio

from utils.order import (
    create_and_submit_order,
    cancel_order,
    get_market_active_orders,
    cancel_all_orders,
    cancel_market_orders,
)
from utils.prices_and_books import get_market_price
# Use the new binary listener – note that it yields combined events for both outcomes
from utils.market_listener_v2 import listen_binary_market
from utils.user_listener import listen_user_trades
from utils.balance import fetch_balance

from order_quantities import order_quantities

from py_clob_client.exceptions import PolyApiException

logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])

async def run_async_bot_bidAndTick(client, market):
    try:
        last_invalid_quantity = None
        event_text = market["question"]
        market_id = market["condition_id"]
        market_slug = market["market_slug"]
        market_min_order_size = market["rewards"]["min_size"]
        # NOTE: We no longer use a provided market spread; it is computed below

        # Initialize the default order quantity.
        order_quantities[market_slug] = market_min_order_size

        logging.info(f"[bold green]Bot started[/bold green] for {market_slug}.", extra={"bot_slug": market_slug})

        # Determine the token IDs for both outcomes from market['tokens'].
        no_token = None
        yes_token = None
        for token in market["tokens"]:
            if token["outcome"].lower() == "no":
                no_token = token["token_id"]
            elif token["outcome"].lower() == "yes":
                yes_token = token["token_id"]

        if not no_token or not yes_token:
            logging.info("[bold red]Could not determine token IDs for both outcomes.[/bold red]", extra={"bot_slug": market_slug})
            return

        market_name = f"{event_text} (Binary Market)"

        # Create a state dictionary for each market side.
        # --> Added an asyncio.Lock for each side.
        state = {
            "No": {
                "lock": asyncio.Lock(),
                "current_min_tick_size": float(market.get("minimum_tick_size", 0.01)),
                "current_best_bid": None,
                "current_order_price": None,
                "initial_order_placed": False,
                "current_order_quantity": None,
            },
            "Yes": {
                "lock": asyncio.Lock(),
                "current_min_tick_size": float(market.get("minimum_tick_size", 0.01)),
                "current_best_bid": None,
                "current_order_price": None,
                "initial_order_placed": False,
                "current_order_quantity": None,
            },
        }

        # Create an async queue to receive events.
        event_queue = asyncio.Queue()

        # Market listener task – uses the new binary listener.
        async def market_listener():
            async for data in listen_binary_market(no_token, yes_token):
                # Distinguish between tick size events and order book updates.
                if "old_tick_size" in data:
                    await event_queue.put({"type": "tick_size", **data})
                else:
                    await event_queue.put({"type": "best_bid", **data})

        # Trade listener task (for order fills).
        async def trade_listener():
            async for data in listen_user_trades([market_id]):
                await event_queue.put({"type": "fill", **data})

        # Quantity update listener task.
        async def quantity_update_listener():
            while True:
                await asyncio.sleep(0.5)
                updated_qty = order_quantities.get(market_slug, market_min_order_size)
                for side in ["No", "Yes"]:
                    if state[side]["initial_order_placed"] and updated_qty != state[side]["current_order_quantity"]:
                        await event_queue.put({"type": "quantity_update", "side": side, "updated_qty": updated_qty})

        # Create listener tasks.
        market_listener_task = asyncio.create_task(market_listener())
        trade_listener_task = asyncio.create_task(trade_listener())
        qty_listener_task = asyncio.create_task(quantity_update_listener())

        # Main event loop.
        while True:
            event_data = await event_queue.get()

            if event_data["type"] == "best_bid":
                print("BEST BID")
                # --- Debounce high-frequency best bid events ---
                await asyncio.sleep(0.1)

                # Compute the market spread from the No side.
                best_bid_no = float(event_data["No"]["best_bid"])
                best_ask_no = float(event_data["No"]["best_ask"])
                computed_spread = best_ask_no - best_bid_no
                TOP_BOOK_TICKS = 1 if computed_spread <= 1 else 2

                # Check if any side has a changed best bid (or it's the initial read).
                update_required = False
                for side in ["No", "Yes"]:
                    prices = event_data.get(side, {})
                    new_bid_val = prices.get("best_bid")
                    if new_bid_val is None:
                        continue
                    new_bid = float(new_bid_val)
                    if state[side]["current_best_bid"] is None or new_bid != state[side]["current_best_bid"]:
                        update_required = True
                        break

                if update_required:
                    for side in ["No", "Yes"]:
                        prices = event_data.get(side, {})
                        new_bid_val = prices.get("best_bid")
                        if new_bid_val is None:
                            continue
                        new_best_bid = float(new_bid_val)
                        current_tick = state[side]["current_min_tick_size"]

                        # Process within a lock for this side.
                        async with state[side]["lock"]:
                            if not state[side]["initial_order_placed"]:
                                state[side]["current_best_bid"] = new_best_bid
                                new_order_price = new_best_bid - (TOP_BOOK_TICKS * current_tick)
                                state[side]["current_order_price"] = new_order_price
                                order_qty = order_quantities.get(market_slug, market_min_order_size)
                                logging.info(
                                    f"[bold green]({side}) Initial market read: Best bid = [bold cyan]{new_best_bid:.4f}[/bold cyan], Tick size = [bold cyan]{current_tick:.4f}[/bold cyan], Spread = [bold cyan]{computed_spread:.4f}[/bold cyan], TBT = [bold cyan]{TOP_BOOK_TICKS}[/bold cyan].[/bold green]",
                                    extra={"bot_slug": market_slug},
                                )
                                logging.info(
                                    f"[bold green]({side}) Placing initial order at price [bold cyan]{new_order_price:.4f}[/bold cyan] for [bold cyan]{order_qty}[/bold cyan] shares. Total: [bold cyan]{order_qty * new_order_price:.4f}[/bold cyan].[/bold green]",
                                    extra={"bot_slug": market_slug},
                                )
                                try:
                                    await asyncio.to_thread(
                                        create_and_submit_order,
                                        client,
                                        no_token if side == "No" else yes_token,
                                        "BUY",
                                        new_order_price,
                                        order_qty,
                                    )
                                except PolyApiException as e:
                                    if "not enough balance" in str(e).lower():
                                        logging.info(
                                            f"[bold red]({side}) Market too expensive![/bold red] Cannot place initial order for {market_slug}.",
                                            extra={"bot_slug": market_slug},
                                        )
                                        await asyncio.to_thread(
                                            cancel_market_orders,
                                            client,
                                            market_id,
                                            no_token if side == "No" else yes_token,
                                        )
                                        market_listener_task.cancel()
                                        trade_listener_task.cancel()
                                        qty_listener_task.cancel()
                                        return
                                    else:
                                        raise
                                state[side]["initial_order_placed"] = True
                                state[side]["current_order_quantity"] = order_qty
                            else:
                                # Only update if the best bid has changed.
                                if state[side]['current_best_bid'] == new_best_bid:
                                    continue
                                # Retrieve active orders and cancel the existing one.
                                current_orders = await asyncio.to_thread(get_market_active_orders, client, market_id)
                                current_buy_order = next(
                                    (
                                        order
                                        for order in current_orders
                                        if order["side"].lower() == "buy"
                                        and order.get("asset_id") == (no_token if side == "No" else yes_token)
                                    ),
                                    None,
                                )
                                if current_buy_order:
                                    await asyncio.to_thread(cancel_order, client, current_buy_order["id"])
                                    # Wait briefly to allow cancellation to process.
                                    await asyncio.sleep(0.1)
                                logging.info(
                                    f"[bold yellow]({side}) Best bid update: Old = [bold cyan]{state[side]['current_best_bid']:.4f}[/bold cyan], New = [bold cyan]{new_best_bid:.4f}[/bold cyan], Spread = [bold cyan]{computed_spread:.4f}[/bold cyan], TBT = [bold cyan]{TOP_BOOK_TICKS}[/bold cyan]. Order was cancelled due to change.[/bold yellow]",
                                    extra={"bot_slug": market_slug},
                                )
                                state[side]["current_best_bid"] = new_best_bid
                                new_order_price = new_best_bid - (TOP_BOOK_TICKS * current_tick)
                                logging.info(
                                    f"[bold yellow]({side}) New order price recalculation: Old = [bold cyan]{state[side]['current_order_price']:.4f}[/bold cyan], New = [bold cyan]{new_order_price:.4f}[/bold cyan].[/bold yellow]",
                                    extra={"bot_slug": market_slug},
                                )
                                order_qty = order_quantities.get(market_slug, market_min_order_size)
                                logging.info(
                                    f"[bold yellow]({side}) Placing new order at price [bold cyan]{new_order_price:.4f}[/bold cyan] for [bold cyan]{order_qty}[/bold cyan] shares. Total = [bold cyan]{order_qty * new_order_price:.4f}[/bold cyan].[/bold yellow]",
                                    extra={"bot_slug": market_slug},
                                )
                                try:
                                    await asyncio.to_thread(
                                        create_and_submit_order,
                                        client,
                                        no_token if side == "No" else yes_token,
                                        "BUY",
                                        new_order_price,
                                        order_qty,
                                    )
                                except PolyApiException as e:
                                    if "not enough balance" in str(e).lower():
                                        logging.info(
                                            f"[bold red]({side}) Market too expensive![/bold red] Cannot update order on best bid change for {market_slug}.",
                                            extra={"bot_slug": market_slug},
                                        )
                                        await asyncio.to_thread(
                                            cancel_market_orders,
                                            client,
                                            market_id,
                                            no_token if side == "No" else yes_token,
                                        )
                                        market_listener_task.cancel()
                                        trade_listener_task.cancel()
                                        qty_listener_task.cancel()
                                        return
                                    else:
                                        raise
                                state[side]["current_order_price"] = new_order_price
                                state[side]["current_order_quantity"] = order_qty

            elif event_data["type"] == "tick_size":
                print("TICK SIZE")
                new_tick_size = float(event_data["new_tick_size"])
                update_dt = event_data.get("datetime")
                for side in ["No", "Yes"]:
                    async with state[side]["lock"]:
                        old_tick = state[side]["current_min_tick_size"]
                        if new_tick_size != old_tick:
                            logging.info(
                                f"[bold yellow]({side}) Tick size change:[/bold yellow] Old = [bold cyan]{old_tick:.4f}[/bold cyan], New = [bold cyan]{new_tick_size:.4f}[/bold cyan]",
                                extra={"bot_slug": market_slug},
                            )
                            state[side]["current_min_tick_size"] = new_tick_size
                            if state[side]["current_best_bid"] is not None and state[side]["initial_order_placed"]:
                                new_order_price = state[side]["current_best_bid"] - (TOP_BOOK_TICKS * new_tick_size)
                                logging.info(
                                    f"[bold yellow]({side}) Tick update: Old = [bold cyan]{state[side]['current_order_price']:.4f}[/bold cyan], New = [bold cyan]{new_order_price:.4f}[/bold cyan].[/bold yellow]",
                                    extra={"bot_slug": market_slug},
                                )
                                if new_order_price != state[side]["current_order_price"]:
                                    order_qty = order_quantities.get(market_slug, market_min_order_size)
                                    current_orders = await asyncio.to_thread(get_market_active_orders, client, market_id)
                                    logging.info(
                                        f"[bold yellow]({side}) Order retrieved after tick change.[/bold yellow]",
                                        extra={"bot_slug": market_slug},
                                    )
                                    current_buy_order = next(
                                        (
                                            order
                                            for order in current_orders
                                            if order["side"].lower() == "buy"
                                            and order.get("asset_id") == (no_token if side == "No" else yes_token)
                                        ),
                                        None,
                                    )
                                    if current_buy_order:
                                        logging.info(
                                            f"[bold yellow]({side}) Cancelling order due to tick size change.[/bold yellow]",
                                            extra={"bot_slug": market_slug},
                                        )
                                        await asyncio.to_thread(cancel_order, client, current_buy_order["id"])
                                        await asyncio.sleep(0.1)
                                    logging.info(
                                        f"[bold yellow]({side}) Placing new order at price [bold cyan]{new_order_price:.4f}[/bold cyan] for [bold cyan]{order_qty}[/bold cyan] shares. Total = [bold cyan]{order_qty * new_order_price:.4f}[/bold cyan].[/bold yellow]",
                                        extra={"bot_slug": market_slug},
                                    )
                                    try:
                                        await asyncio.to_thread(
                                            create_and_submit_order,
                                            client,
                                            no_token if side == "No" else yes_token,
                                            "BUY",
                                            new_order_price,
                                            order_qty,
                                        )
                                    except PolyApiException as e:
                                        if "not enough balance" in str(e).lower():
                                            logging.info(
                                                f"[bold red]({side}) Market too expensive![/bold red] Cannot update order on tick size change for {market_slug}.",
                                                extra={"bot_slug": market_slug},
                                            )
                                            await asyncio.to_thread(
                                                cancel_market_orders,
                                                client,
                                                market_id,
                                                no_token if side == "No" else yes_token,
                                            )
                                            market_listener_task.cancel()
                                            trade_listener_task.cancel()
                                            qty_listener_task.cancel()
                                            return
                                        else:
                                            raise
                                    state[side]["current_order_price"] = new_order_price
                                    state[side]["current_order_quantity"] = order_qty
                                else:
                                    logging.info(
                                        f"[bold yellow]({side}) Tick size update did not change order price. Current order price remains: [bold cyan]{state[side]['current_order_price']:.4f}[/bold cyan].[/bold yellow]",
                                        extra={"bot_slug": market_slug},
                                    )
                            else:
                                logging.info(
                                    f"[bold yellow]({side}) Tick size update but no active order or best bid available.[/bold yellow]",
                                    extra={"bot_slug": market_slug},
                                )

            elif event_data["type"] == "fill":
                print("FILL")
                logging.info(
                    f"[bold pink]Order matched (not confirmed yet) for {market_slug}.[/bold pink]",
                    extra={"bot_slug": market_slug},
                )
                logging.info(f"[bold red]Shutting down bot for {market_slug}[/bold red]", extra={"bot_slug": market_slug})
                await asyncio.to_thread(cancel_market_orders, client, market_id, no_token)
                await asyncio.to_thread(cancel_market_orders, client, market_id, yes_token)
                market_listener_task.cancel()
                trade_listener_task.cancel()
                qty_listener_task.cancel()
                return

            elif event_data["type"] == "quantity_update":
                print("QTY UPDATE")
                side = event_data.get("side")
                updated_qty = event_data.get("updated_qty")
                if updated_qty is None or side not in ["No", "Yes"]:
                    continue
                if updated_qty == state[side]["current_order_quantity"]:
                    continue
                if last_invalid_quantity is not None and updated_qty == last_invalid_quantity:
                    continue

                async with state[side]["lock"]:
                    logging.info(
                        f"[bold dark_orange3]({side}) Order quantity update detected:[/bold dark_orange3] Old quantity = [bold cyan]{state[side]['current_order_quantity']}[/bold cyan], New quantity = [bold cyan]{updated_qty}[/bold cyan]",
                        extra={"bot_slug": market_slug},
                    )
                    # current_balance = fetch_balance()
                    # if (updated_qty * state[side]["current_order_price"]) > current_balance:
                    #     max_qty = int(current_balance // state[side]["current_order_price"])
                    #     logging.info(
                    #         f"[bold red]({side}) Quantity update failed![/bold red] Updated order cost ([bold cyan]{updated_qty * state[side]['current_order_price']:.4f}[/bold cyan]) exceeds balance ([bold cyan]{current_balance}[/bold cyan]). "
                    #         f"Maximum allowable quantity is [bold cyan]{max_qty}[/bold cyan]. Keeping previous order.",
                    #         extra={"bot_slug": market_slug},
                    #     )
                    #     last_invalid_quantity = updated_qty
                    #     continue

                    last_invalid_quantity = None
                    current_orders = await asyncio.to_thread(get_market_active_orders, client, market_id)
                    current_buy_order = next(
                        (
                            order
                            for order in current_orders
                            if order["side"].lower() == "buy"
                            and order.get("asset_id") == (no_token if side == "No" else yes_token)
                        ),
                        None,
                    )
                    if current_buy_order:
                        logging.info(
                            f"[bold dark_orange3]({side}) Cancelling order due to quantity update.[/bold dark_orange3]",
                            extra={"bot_slug": market_slug},
                        )
                        await asyncio.to_thread(cancel_order, client, current_buy_order["id"])
                        await asyncio.sleep(0.1)
                    logging.info(
                        f"[bold dark_orange3]({side}) Placing updated order at price [bold cyan]{state[side]['current_order_price']:.4f}[/bold cyan] for [bold cyan]{updated_qty}[/bold cyan] shares. Total: [bold cyan]{updated_qty * state[side]['current_order_price']:.4f}[/bold cyan].[/bold dark_orange3]",
                        extra={"bot_slug": market_slug},
                    )
                    try:
                        await asyncio.to_thread(
                            create_and_submit_order,
                            client,
                            no_token if side == "No" else yes_token,
                            "BUY",
                            state[side]["current_order_price"],
                            updated_qty,
                        )
                    except PolyApiException as e:
                        if "not enough balance" in str(e).lower():
                            logging.info(
                                f"[bold red]({side}) Balance problem![/bold red] Cannot update order quantity for {market_slug} on ({side}) due to insufficient balance.",
                                extra={"bot_slug": market_slug},
                            )
                            await asyncio.to_thread(
                                cancel_market_orders,
                                client,
                                market_id,
                                no_token if side == "No" else yes_token,
                            )
                            continue
                        else:
                            raise
                    state[side]["current_order_quantity"] = updated_qty

    except asyncio.CancelledError:
        logging.info(f"[bold red]Bot for {market['market_slug']} cancelled.[/bold red]", extra={"bot_slug": market["market_slug"]})
        raise
    except Exception as e:
        logging.info(f"[bold red]Unexpected error:[/bold red] {e}", extra={"bot_slug": market["market_slug"]})
        raise
    finally:
        logging.info(f"[bold red]Cancelling orders for {market['market_slug']}.[/bold red]", extra={"bot_slug": market["market_slug"]})
        await asyncio.to_thread(cancel_market_orders, client, market_id, market["tokens"][0]["token_id"])
        await asyncio.to_thread(cancel_market_orders, client, market_id, market["tokens"][1]["token_id"])

# Example entry point.
if __name__ == "__main__":
    # Here you would typically initialize your client and market data.
    # Then run your bot with something like:
    #
    #   client = <your_client>
    #   market = <market_data_dict>
    #   asyncio.run(run_async_bot_bidAndTick(client, market))
    #
    # For now, this is just a placeholder.
    pass
