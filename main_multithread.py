from utils.order import cancel_all_orders
from utils.clob_client import create_client
from utils.balance import fetch_allowance, fetch_balance
from utils.market import get_single_byName
from bot import run_async_bot_bidOnly, run_async_bot_bidAndTick
from bot_manager import BotManager

import asyncio
import logging
from rich.logging import RichHandler

from rich import print

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])

async def main():
    # Bot initialization and startup
    polyBot = create_client()

    cancel_all_orders(polyBot)

    botBalance = fetch_balance()
    botAllowance = fetch_allowance()
    logging.info(f'Balance: {botBalance}')
    logging.info(f'Allowance: {botAllowance}\n')  

    market_names = [
        'Will Putin meet with Trump in first 100 days?',
        'Will China invade Taiwan in 2025?',
        'Russia x Ukraine ceasefire in 2025?'
    ]

    available_markets = []

    for name in market_names:
        available_markets.append(get_single_byName(polyBot, name))

    logging.info("[bold blue]Markets retrieved. [/bold blue]")
    
    manager = BotManager(polyBot, available_markets)
    manager.start_all()
    
    # Optionally, start a monitor task.
    monitor_task = asyncio.create_task(manager.monitor_tasks())
    
    try:
        await asyncio.gather(*manager.tasks.values())
    except asyncio.CancelledError:
        pass
    finally:
        monitor_task.cancel()
        await asyncio.gather(monitor_task, return_exceptions=True)
        await manager.stop_all()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutdown signal received. Exiting...")
