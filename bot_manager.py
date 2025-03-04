import asyncio
import logging
from rich.logging import RichHandler

# Import your single-side function
from bot import run_single_side
from rich import print

logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])

class BotManager:
    def __init__(self, client, markets):
        self.client = client
        self.markets = markets
        # We'll store tasks by (market_slug, token_index) or (market_slug, "both")
        self.tasks = {}

    async def start_bot_side(self, market, token_index: int):
        """
        Starts a bot for one side (token_index) of a market, automatically restarts on failure.
        This loops until canceled or no error occurs.
        """
        market_slug = market["market_slug"]
        task_key = (market_slug, token_index)
        outcome = market['tokens'][token_index]['outcome']

        try:
            while True:
                try:
                    # logging.info(
                    #     f"Starting side '{outcome}' for market [bold green]{market_slug}[/bold green]",
                    #     extra={"bot_slug": market_slug}
                    # )
                    await run_single_side(self.client, market, token_index)
                except asyncio.CancelledError:
                    logging.info(
                        f"[bold magenta]({outcome})[/bold magenta] Side for market [bold red]{market_slug}[/bold red] was cancelled.",
                        extra={"bot_slug": market_slug}
                    )
                    break
                except Exception as e:
                    logging.info(
                        f"[bold magenta]({outcome})[/bold magenta] Bot side for market [bold red]{market_slug}[/bold red] encountered an error: {e}",
                        exc_info=True,
                        extra={"bot_slug": market_slug}
                    )
                    # Wait before restarting to avoid tight error loops
                    await asyncio.sleep(5)
                    logging.info(
                        f"[bold magenta]({outcome})[/bold magenta] Restarting side for market [bold orange]{market_slug}[/bold orange]...",
                        extra={"bot_slug": market_slug}
                    )
                else:
                    # If run_single_side returns without error or fill, break out.
                    break
        finally:
            # Cleanup if we exit the while True
            if task_key in self.tasks:
                self.tasks.pop(task_key)
            # logging.info(
            #     f"[bold magenta]({outcome})[/bold magenta] Bot side for market [bold red]{market_slug}[/bold red] has stopped.",
            #     extra={"bot_slug": market_slug}
            # )

    async def start_bot(self, market):
        """
        Spawns TWO parallel side tasks (token_index=0,1) for the given market
        and waits until both exit.
        """
        market_slug = market['market_slug']

        t0 = asyncio.create_task(self.start_bot_side(market, 0))
        t1 = asyncio.create_task(self.start_bot_side(market, 1))

        self.tasks[(market_slug, 0)] = t0
        self.tasks[(market_slug, 1)] = t1

        # Wait until both sides have stopped or are cancelled
        await asyncio.gather(t0, t1)

        # Once gather returns, both sides are done
        logging.info(
            f"Both sides have stopped for market [bold red]{market_slug}[/bold red].",
            extra={"bot_slug": market_slug}
        )

    def stop_bot_side(self, market_slug: str, token_index: int):
        """
        Cancels a specific side if it's running.
        """
        task_key = (market_slug, token_index)
        task = self.tasks.get(task_key)
        if task:
            task.cancel()
            # logging.info(
            #     f"Cancelled side={token_index} for market [bold red]{market_slug}[/bold red].",
            #     extra={"bot_slug": market_slug}
            # )
        else:
            logging.info(
                f"No running side for market [bold red]{market_slug}[/bold red].",
                extra={"bot_slug": market_slug}
            )

    def stop_bot(self, market_slug: str):
        """
        Cancels BOTH sides for the given market.
        """
        self.stop_bot_side(market_slug, 0)
        self.stop_bot_side(market_slug, 1)

    def start_all(self):
        """
        (Optional) Starts all bots for all markets. Not typically used if you want to pick markets by slug.
        """
        for market in self.markets:
            slug = market['market_slug']
            if (slug, 0) not in self.tasks and (slug, 1) not in self.tasks:
                # create a "master" gather
                master_task = asyncio.create_task(self.start_bot(market))
                self.tasks[(slug, 'both')] = master_task

    async def stop_all(self):  
        """
        Cancels all running bot tasks (both sides) and waits for them to complete.
        """
        logging.info("[bold red]Stopping all bots...[/bold red]", extra={"bot_slug": "global"})
        # Cancel everything
        for key, task in list(self.tasks.items()):
            task.cancel()
        # Clear them out 
        self.tasks.clear()
        logging.info("[bold red]All bots have been stopped.[/bold red]", extra={"bot_slug": "global"})

    async def monitor_tasks(self):
        """
        Periodically check the status of tasks.
        If a task is done unexpectedly, restart it.
        (Optional method if you want auto–restart outside the while–true loop.)
        """
        while True:
            for market in self.markets:
                slug = market['market_slug']
                for token_index in [0, 1]:
                    task_key = (slug, token_index)
                    task = self.tasks.get(task_key)
                    outcome = market['tokens'][token_index]['outcome']
                    if task is not None and task.done():
                        # If it's done, check if it was cancelled or had an exception
                        if task.cancelled():
                            logging.info(
                                f"Side '{outcome}' for market [bold red]{slug}[/bold red] was manually cancelled. Not restarting.",
                                extra={"bot_slug": slug}
                            )
                        elif task.exception():
                            logging.info(
                                f"Side '{outcome}' for market [bold red]{slug}[/bold red] failed unexpectedly, restarting.",
                                extra={"bot_slug": slug}
                            )
                            # Restart that side
                            new_task = asyncio.create_task(self.start_bot_side(market, token_index))
                            self.tasks[task_key] = new_task
                        else:
                            logging.info(
                                f"Side '{outcome}' for market [bold orange]{slug}[/bold orange] ended normally. Restarting it.",
                                extra={"bot_slug": slug}
                            )
                            new_task = asyncio.create_task(self.start_bot_side(market, token_index))
                            self.tasks[task_key] = new_task
            await asyncio.sleep(10)
