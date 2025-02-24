from bot import run_async_bot_bidOnly, run_async_bot_bidAndTick
from utils.clob_client import create_client
from utils.balance import fetch_allowance, fetch_balance
from utils.market import get_single_byName
from utils.market_listener import market_events

import time
import asyncio

from rich import print
from rich.progress import track

# async def main():
    # run whenever needed
    # allowance.set_allowances()
    # api.generate_api_keys()

    # run for session start
    # polyBot = create_client()
    # sampling_market = get_single_byName(polyBot, 'Will the Eagles win Super Bowl 2025?')

    # botBalance = fetch_balance()
    # botAllowance = fetch_allowance()
    # print(f'Balance: {botBalance}')
    # print(f'Allowance: {botAllowance}\n')

    # for _ in track(range(10), description="PolyBot Initializing..."):
    #     time.sleep(1)

    # # sampling_markets = market.get_sampling_all(polyBot)
    # sampling_market = get_single_byName(polyBot, 'Will the Eagles win Super Bowl 2025?')

    # run_async_bot_bidAndTick(polyBot, sampling_market)

if __name__ == "__main__":
    polyBot = create_client()
    sampling_market = get_single_byName(polyBot, 'Will Liverpool win the UEFA Champions League?')

    asyncio.run(run_async_bot_bidAndTick(polyBot, sampling_market))