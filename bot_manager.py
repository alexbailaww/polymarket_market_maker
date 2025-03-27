import asyncio
import logging
from rich.logging import RichHandler

from bot2 import run_async_bot_bidAndTick
from rich import print

logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])

class BotManager:
    def __init__(self, client, markets):
        self.client = client
        self.markets = markets 
        self.tasks = {}         

    async def start_bot(self, market, bookAway: int):
        market_slug = market['market_slug']
        try:
            while True:
                try:
                    await run_async_bot_bidAndTick(self.client, market, bookAway)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    # Optionally log the exception
                    logging.error(f"Error in bot {market_slug}: {e}", extra={"bot_slug": market_slug})
                    break
                else:
                    break
        finally:
            if market_slug in self.tasks:
                self.tasks.pop(market_slug)
            logging.info(f"Bot for market {market_slug} has stopped.", extra={"bot_slug": market_slug})


    def start_all(self):
        """Starts all bots and stores their tasks."""
        for market in self.markets:
            market_slug = market['market_slug']
            self.tasks[market_slug] = asyncio.create_task(self.start_bot(market))

    async def stop_all(self):
        """Cancels all running bot tasks and waits for their completion."""
        logging.info("[bold red]Stopping all bots...[/bold red]", extra={"bot_slug": "global"})
        for task in self.tasks.values():
            task.cancel()
        self.tasks.clear()
        await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        logging.info("[bold red]All bots have been stopped.[/bold red]", extra={"bot_slug": "global"})

    def stop_bot(self, market_slug):
        """Cancels a specific bot task if it's running."""
        task = self.tasks.get(market_slug)
        if task:
            task.cancel()
            self.tasks.pop(market_slug)
            logging.info(
                f"Bot for market [bold red]{market_slug}[/bold red] stopped.",
                extra={"bot_slug": market_slug}
            )
        else:
            logging.info(
                f"No running bot found for market [bold red]{market_slug}[/bold red].",
                extra={"bot_slug": market_slug}
            )

    async def monitor_tasks(self):
        """
        Periodically check the status of tasks.
        If a task is done unexpectedly, restart it.
        """
        while True:
            for market in self.markets:
                market_slug = market['market_slug']
                task = self.tasks.get(market_slug)
                # If the task is done and not cancelled, restart it.
                if task is not None and task.done():
                    if task.cancelled():
                        logging.info(
                            f"Bot for market [bold red]{market_slug}[/bold red] is cancelled. Skipping restart.",
                            extra={"bot_slug": market_slug}
                        )
                    elif task.exception():
                        logging.info(
                            f"Bot for market [bold red]{market_slug}[/bold red] failed with an exception and will be restarted.",
                            extra={"bot_slug": market_slug}
                        )
                        self.tasks[market_slug] = asyncio.create_task(self.start_bot(market))
                    else:
                        logging.info(
                            f"Bot for market [bold orange]{market_slug}[/bold orange] ended normally. Restarting it.",
                            extra={"bot_slug": market_slug}
                        )
                        self.tasks[market_slug] = asyncio.create_task(self.start_bot(market))
            await asyncio.sleep(10)
