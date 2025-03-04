import asyncio
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import uvicorn
import difflib
import time

# Bot modules
from bot_manager import BotManager
from utils.order import cancel_all_orders, cancel_order, get_market_active_orders
from utils.clob_client import create_client
from utils.balance import fetch_balance
from utils.allowance import fetch_allowance
from utils.market import get_single_byName, get_sampling_all

# Logging and other
from rich.logging import RichHandler
from rich import print
from inMemoryLogHandler import InMemoryLogHandler, log_buffers, CustomFormatter
from pydantic import BaseModel

# Utility for tracking usage
from api_usage import get_api_usage

# helper function
def find_outcome_name(markets, market_slug, side_index):
    for m in markets:
        if m["market_slug"] == market_slug:
            if "tokens" in m and len(m["tokens"]) > side_index:
                return m["tokens"][side_index].get("outcome", f"Side{side_index}")
    return f"Side{side_index}"

# Basic logging configuration using Rich
logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])
memory_handler = InMemoryLogHandler()
custom_formatter = CustomFormatter("%(asctime)s - %(message)s")
memory_handler.setFormatter(custom_formatter)
root_logger = logging.getLogger()
root_logger.addHandler(memory_handler)

class OrderQuantityUpdate(BaseModel):
    quantity: float

# 1) Initialize CLOB client, cancel all orders, fetch balance/allowance
polyBot = create_client()
cancel_all_orders(polyBot)
botBalance = fetch_balance()
botAllowance = fetch_allowance()

logging.info(f'Balance: {botBalance}', extra={"bot_slug": "global"})
logging.info(f'Allowance: {botAllowance}\n', extra={"bot_slug": "global"})

# 2) Get a list of markets, build the BotManager
available_markets = get_sampling_all(polyBot)
logging.info("[bold blue]Markets retrieved.[/bold blue]", extra={"bot_slug": "global"})

manager = BotManager(polyBot, available_markets)

# 3) Create FastAPI app, mount static files
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory="frontend"), name="static")

# 4) Endpoints

@app.get("/api_usage")
async def api_usage():
    usage = get_api_usage()
    return {"api_usage": usage, "rate_limit": "80 calls/10 seconds"}

@app.get("/balance")
async def get_balance():
    balance = fetch_balance()
    return {"balance": balance}

@app.get("/")
async def read_index():
    return FileResponse("frontend/index.html")

@app.post("/start_bot/{market_slug}")
async def start_bot(market_slug: str):
    """
    Start BOTH sides for the given market_slug.
    Will spawn two side tasks (0 and 1).
    """
    market = None
    for m in manager.markets:
        if m['market_slug'] == market_slug:
            market = m
            break
    if market is None:
        raise HTTPException(status_code=404, detail="Market not found")

    # If either side is already running, we can choose to skip or forcibly restart:
    # For now, let's skip if both sides are already running.
    task0 = manager.tasks.get((market_slug, 0))
    task1 = manager.tasks.get((market_slug, 1))
    if task0 and not task0.done() and task1 and not task1.done():
        return {"status": f"Both sides for {market_slug} are already running."}

    # Launch both sides in a single "master" task
    master_task = asyncio.create_task(manager.start_bot(market))
    # Optionally store it under a separate key if you like:
    manager.tasks[(market_slug, "both")] = master_task
    return {"status": f"Bot {market_slug} (both sides) started."}

@app.post("/stop_bot/{market_slug}")
async def stop_bot(market_slug: str):
    """
    Stop BOTH sides for the given market_slug.
    """
    manager.stop_bot(market_slug)
    return {"status": f"Requested stop for both sides of {market_slug}."}

@app.post("/start_side/{market_slug}/{token_index}")
async def start_side(market_slug: str, token_index: int):
    """
    Start only one side (0 or 1) of the given market.
    """
    market = next((m for m in manager.markets if m["market_slug"] == market_slug), None)
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")

    task_key = (market_slug, token_index)
    if task_key in manager.tasks and not manager.tasks[task_key].done():
        return {"status": f"Side={token_index} for {market_slug} is already running."}

    # Start that single side
    side_task = asyncio.create_task(manager.start_bot_side(market, token_index))
    manager.tasks[task_key] = side_task
    return {"status": f"Started side for market {market_slug}."}

@app.post("/stop_side/{market_slug}/{token_index}")
async def stop_side(market_slug: str, token_index: int):
    """
    Stop only one side (0 or 1) of the given market.
    """
    manager.stop_bot_side(market_slug, token_index)
    return {"status": f"Requested stop for side of {market_slug}."}

@app.post("/start_all")
async def start_all(confirmed: list = Body(...)):
    """
    Start BOTH sides for each market in the 'confirmed' list.
    """
    started = []
    for slug in confirmed:
        market = next((m for m in manager.markets if m["market_slug"] == slug), None)
        if market:
            # If not already running both sides, start them
            t0 = manager.tasks.get((slug, 0))
            t1 = manager.tasks.get((slug, 1))
            if not (t0 and not t0.done() and t1 and not t1.done()):
                master_task = asyncio.create_task(manager.start_bot(market))
                manager.tasks[(slug, "both")] = master_task
                started.append(slug)
    return {"status": "Bots started for: " + ", ".join(started)}

@app.post("/stop_all")
async def stop_all(confirmed: list = Body(...)):
    """
    Stop BOTH sides for each market in the 'confirmed' list.
    """
    stopped = []
    for slug in confirmed:
        task0 = manager.tasks.get((slug, 0))
        task1 = manager.tasks.get((slug, 1))
        if task0 or task1:
            manager.stop_bot(slug)
            stopped.append(slug)
    return {"status": "Bots stopped for: " + ", ".join(stopped)}

@app.get("/markets")
async def get_markets():
    """
    Return a minimal list of markets for the frontend.
    """
    market_list = []
    for m in manager.markets:
        # If one token is cheaper, the "side" might be that outcome, etc.
        if m['tokens'][0]['price'] < m['tokens'][1]['price']:
            outcome = m['tokens'][0]['outcome']
        else:
            outcome = m['tokens'][1]['outcome']

        market_list.append({
            "market_slug": m["market_slug"],
            "question": m.get("question", ""),
            "side": outcome
        })
    return {"markets": market_list}

@app.get("/tasks")
async def get_tasks():
    """
    Return a consolidated status per market using only individual side tasks
    that are still active (i.e. not done). The "both" key is ignored.
    """
    result = []
    # Iterate over keys that are individual sides and are not done.
    for (slug, side) in manager.tasks.keys():
        if isinstance(side, int):
            task = manager.tasks[(slug, side)]
            # Only include tasks that are not finished.
            if not task.done():
                outcome = find_outcome_name(manager.markets, slug, side)
                result.append({
                    "market_slug": slug,
                    "side_index": side,
                    "outcome": outcome
                })
    # Consolidate by market_slug
    consolidated = {}
    for task in result:
        slug = task["market_slug"]
        if slug not in consolidated:
            consolidated[slug] = []
        consolidated[slug].append(task["side_index"])
    
    final = []
    for slug, sides in consolidated.items():
        if 0 in sides and 1 in sides:
            status = "Running (Both)"
        elif 0 in sides:
            outcome = find_outcome_name(manager.markets, slug, 0)
            status = f"Running ({outcome})"
        elif 1 in sides:
            outcome = find_outcome_name(manager.markets, slug, 1)
            status = f"Running ({outcome})"
        else:
            status = "Not Running"
        final.append({
            "market_slug": slug,
            "status": status
        })
    return {"tasks": final}

@app.get("/suggest_markets")
async def suggest_markets(query: str):
    if not query:
        return {"suggestions": []}
    market_questions = [m["question"] for m in manager.markets]
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
        if e.code != 1006:
            logging.info(f"WebSocket disconnected for bot {market_slug} with code {e.code}", extra={"bot_slug": market_slug})

# Endpoint to update order quantity
from order_quantities import order_quantities

@app.post("/update_order_quantity/{market_slug}")
async def update_order_quantity(market_slug: str, update: OrderQuantityUpdate):
    quantity = update.quantity
    # Find the market
    market = next((m for m in manager.markets if m["market_slug"] == market_slug), None)
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")

    min_shares = market['rewards']['min_size']
    if quantity < min_shares:
        raise HTTPException(status_code=400, detail=f"Quantity must be at least {min_shares}")
    order_quantities[market_slug] = quantity
    return {"status": f"Market {market_slug} order quantity updated to {quantity}"}

@app.get("/order_quantities")
async def get_order_quantities():
    # Since our order quantities are stored using market_slug as key,
    # simply return the order_quantities dictionary.
    return order_quantities

# Optional: A “shutdown” endpoint if you want the EMERGENCY SHUTDOWN button to do something
@app.post("/shutdown")
async def shutdown_server():
    # This is optional; you can define how you want to handle a server shutdown
    logging.info("[bold red]EMERGENCY SHUTDOWN requested![/bold red]", extra={"bot_slug": "global"})
    # Force all tasks to stop:
    for key, t in list(manager.tasks.items()):
        t.cancel()
    manager.tasks.clear()
    return {"status": "Server is shutting down... (not truly stopping uvicorn, but tasks canceled)"}


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
