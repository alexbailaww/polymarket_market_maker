import asyncio
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import uvicorn
import difflib
import time
from bot_manager import BotManager
from utils.order import cancel_all_orders, cancel_order, get_market_active_orders
from utils.clob_client import create_client
from utils.balance import fetch_balance
from utils.allowance import fetch_allowance
from utils.market import get_single_byName, get_sampling_all
from rich.logging import RichHandler
from rich import print
from inMemoryLogHandler import InMemoryLogHandler, log_buffers, CustomFormatter
from pydantic import BaseModel
from api_usage import get_api_usage

class OrderQuantityUpdate(BaseModel):
    quantity: float

# Basic logging configuration using Rich.
logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])
memory_handler = InMemoryLogHandler()
custom_formatter = CustomFormatter("%(asctime)s - %(message)s")
memory_handler.setFormatter(custom_formatter)
root_logger = logging.getLogger()
root_logger.addHandler(memory_handler)

# Initialize bot environment.
polyBot = create_client()
cancel_all_orders(polyBot)
botBalance = fetch_balance()
botAllowance = fetch_allowance()
logging.info(f'Balance: {botBalance}', extra={"bot_slug": "global"})
logging.info(f'Allowance: {botAllowance}\n', extra={"bot_slug": "global"})

available_markets = get_sampling_all(polyBot)
logging.info("[bold blue]Markets retrieved.[/bold blue]", extra={"bot_slug": "global"})

manager = BotManager(polyBot, available_markets)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory="frontend"), name="static")

@app.get("/api_usage")
async def api_usage():
    usage = get_api_usage()
    return {"api_usage": usage, "rate_limit": "80 calls/10 seconds"}

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
    task = asyncio.create_task(manager.start_bot(market))
    manager.tasks[market_slug] = task
    return {"status": f"Bot {market_slug} started."}

@app.post("/stop_bot/{market_slug}")
async def stop_bot(market_slug: str):
    task = manager.tasks.get(market_slug)
    if not task:
        return {"status": f"Bot {market_slug} is not running."}
    task.cancel()
    manager.tasks.pop(market_slug)
    return {"status": f"Bot {market_slug} stopped."}

@app.post("/start_all")
async def start_all(confirmed: list = Body(...)):
    started = []
    for slug in confirmed:
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
    return {"tasks": list(manager.tasks.keys())}

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

# New endpoint to update the order quantity.
from order_quantities import order_quantities

@app.post("/update_order_quantity/{market_slug}")
async def update_order_quantity(market_slug: str, update: OrderQuantityUpdate):
    quantity = update.quantity
    # Find the market.
    market = next((m for m in manager.markets if m["market_slug"] == market_slug), None)
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")
    min_shares = market['rewards']['min_size']
    if quantity < min_shares:
        raise HTTPException(status_code=400, detail=f"Quantity must be at least {min_shares}")
    order_quantities[market_slug] = quantity
    return {"status": f"Market {market_slug} order quantity updated to {quantity}"}

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
