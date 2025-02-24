# In server.py
import asyncio
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import uvicorn

import difflib

from fastapi import Body

import threading
import sys
import time

# Import your BotManager and utilities
from bot_manager import BotManager
from utils.order import cancel_all_orders
from utils.clob_client import create_client
from utils.balance import fetch_allowance, fetch_balance
from utils.market import get_single_byName, get_sampling_all

from rich.logging import RichHandler
from rich import print

# Basic logging configuration using Rich
logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])

# Import our custom in-memory log handler and shared log_buffers.
from inMemoryLogHandler import InMemoryLogHandler, log_buffers, CustomFormatter

memory_handler = InMemoryLogHandler()
custom_formatter = CustomFormatter("%(asctime)s - %(message)s")
memory_handler.setFormatter(custom_formatter)

# Attach our in-memory handler to the root logger.
root_logger = logging.getLogger()
root_logger.addHandler(memory_handler)

# Initialize bot environment
polyBot = create_client()
cancel_all_orders(polyBot)
botBalance = fetch_balance()
botAllowance = fetch_allowance()
logging.info(f'Balance: {botBalance}', extra={"bot_slug": "global"})
logging.info(f'Allowance: {botAllowance}\n', extra={"bot_slug": "global"})

# market_names = [
#     'Will Putin meet with Trump in first 100 days?',
#     'Will China invade Taiwan in 2025?',
#     'Russia x Ukraine ceasefire in 2025?'
# ]
available_markets = get_sampling_all(polyBot)
# for name in market_names:
#     available_markets.append(get_single_byName(polyBot, name))
logging.info("[bold blue]Markets retrieved.[/bold blue]", extra={"bot_slug": "global"})

manager = BotManager(polyBot, available_markets)
# Bots are started/stopped via API calls

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="frontend"), name="static")

@app.get("/")
async def read_index():
    return FileResponse("frontend/index.html")

@app.post("/start_bot/{market_slug}")
async def start_bot(market_slug: str):
    market = None
    for m in manager.markets:
        if m['market_slug'] == market_slug:
            market = m
            break
    if market is None:
        raise HTTPException(status_code=404, detail="Market not found")
    # Create the task and store it in the tasks dictionary
    task = asyncio.create_task(manager.start_bot(market))
    manager.tasks[market_slug] = task
    return {"status": f"Bot {market_slug} started."}

@app.post("/stop_bot/{market_slug}")
async def stop_bot(market_slug: str):
    task = manager.tasks.get(market_slug)
    if not task:
        # If there is no running task, inform the user.
        return {"status": f"Bot {market_slug} is not running."}
    task.cancel()
    manager.tasks.pop(market_slug)
    return {"status": f"Bot {market_slug} stopped."}

@app.post("/start_all")
async def start_all(confirmed: list = Body(...)):
    started = []
    for slug in confirmed:
        # Find the market with the matching slug.
        market = next((m for m in manager.markets if m["market_slug"] == slug), None)
        if market and slug not in manager.tasks:
            task = asyncio.create_task(manager.start_bot(market))
            manager.tasks[slug] = task
            started.append(slug)
    return {"status": "Bots started for confirmed markets: " + ", ".join(started)}

@app.post("/stop_all")
async def stop_all(confirmed: list = Body(...)):
    stopped = []
    for slug in confirmed:
        task = manager.tasks.get(slug)
        if task:
            task.cancel()
            manager.tasks.pop(slug, None)
            stopped.append(slug)
    return {"status": "Bots stopped for confirmed markets: " + ", ".join(stopped)}

@app.get("/markets")
async def get_markets():
    market_list = []
    for m in manager.markets:
        # Derive market side based on token price
        if m['tokens'][0]['price'] < m['tokens'][1]['price']:
            outcome = m['tokens'][0]['outcome']
        else:
            outcome = m['tokens'][1]['outcome']
        market_list.append({
            "market_slug": m["market_slug"],
            "question": m.get("question", ""),
            "side": outcome  # Market side field
        })
    return {"markets": market_list}

@app.get("/tasks")
async def get_tasks():
    # Return the keys (market slugs) of the running tasks.
    return {"tasks": list(manager.tasks.keys())}

@app.get("/suggest_markets")
async def suggest_markets(query: str):
    if not query:
        return {"suggestions": []}
    # Use the market "question" field for matching.
    market_questions = [m["question"] for m in manager.markets]
    # Adjust cutoff as needed for closer matches (e.g., 0.7)
    matches = difflib.get_close_matches(query, market_questions, n=5, cutoff=0.3)
    suggestions = [m for m in manager.markets if m["question"] in matches]
    return {"suggestions": suggestions}

@app.post("/clear_logs/{market_slug}")
async def clear_logs(market_slug: str):
    if market_slug in log_buffers:
        log_buffers[market_slug].clear()
        return {"status": f"Logs for market {market_slug} cleared."}
    else:
        raise HTTPException(status_code=404, detail=f"No logs found for market {market_slug}.")

@app.websocket("/ws/logs/{market_slug}")
async def websocket_logs(websocket: WebSocket, market_slug: str):
    await websocket.accept()
    try:
        while True:
            logs = "\n".join(list(log_buffers[market_slug]))
            await websocket.send_text(logs)
            await asyncio.sleep(1)
    except WebSocketDisconnect as e:
        # e.code typically is 1000 for a normal closure (e.g., page refresh)
        if e.code != 1006:
            logging.info(
                f"WebSocket disconnected for bot {market_slug} with code {e.code}",
                extra={"bot_slug": market_slug},
            )

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
