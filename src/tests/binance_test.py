import asyncio
from websockets import connect
import aiofiles
import sys
import json
import httpx
import time
from datetime import datetime
from pathlib import Path
import random


DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "processed"


async def orderbook_stream(pair: str, client: httpx.AsyncClient):
    pair_upper = pair.upper()
    pair_lower = pair.lower()
    websocket_url = f"wss://stream.binance.com:9443/ws/{pair_lower}@depth"

    def _extract_levels_from_obj(o):
        if not isinstance(o, dict):
            return None, None
        if "b" in o and "a" in o:
            return o.get("b"), o.get("a")
        if "bids" in o and "asks" in o:
            return o.get("bids"), o.get("asks")
        if "data" in o and isinstance(o.get("data"), dict):
            return _extract_levels_from_obj(o.get("data"))
        return None, None

    def add_total_per_level(bids, asks):
        def compute_level_total(level):
            try:
                price = float(level[0])
                qty = float(level[1])
                total = price * qty
                return total
            except Exception:
                return None

        new_bids = None
        new_asks = None
        if bids:
            new_bids = [[lvl[0], lvl[1], compute_level_total(lvl)] for lvl in bids]
        if asks:
            new_asks = [[lvl[0], lvl[1], compute_level_total(lvl)] for lvl in asks]
        return new_bids, new_asks

    today = datetime.now().date()
    orderbook_path = Path(f"{pair_lower}-orderbook-{today}.jsonl")

    backoff = 1
    while True:
        try:
            print(f"[{pair_upper}] connecting to {websocket_url}")
            async with connect(websocket_url) as websocket:
                print(f"[{pair_upper}] connected")
                backoff = 1
                while True:
                    data = await websocket.recv()
                    if isinstance(data, (bytes, bytearray, memoryview)):
                        try:
                            data = bytes(data).decode()
                        except Exception:
                            print(
                                f"[{pair_upper}] received non-decodable binary message, skipping"
                            )
                            continue

                    try:
                        obj = json.loads(data)
                        bids, asks = _extract_levels_from_obj(obj)
                        if bids is not None or asks is not None:
                            new_bids, new_asks = add_total_per_level(bids, asks)
                            if "b" in obj:
                                obj["b"] = new_bids
                            elif "bids" in obj:
                                obj["bids"] = new_bids
                            if "a" in obj:
                                obj["a"] = new_asks
                            elif "asks" in obj:
                                obj["asks"] = new_asks
                        record = {"type": "update", "pair": pair_upper, "data": obj}
                    except Exception:
                        # not JSON, store raw text in data
                        record = {"type": "update", "pair": pair_upper, "data": data}

                    async with aiofiles.open(
                        orderbook_path, mode="a", encoding="utf-8"
                    ) as f:
                        await f.write(json.dumps(record))
                        await f.write("\n")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[{pair_upper}] stream error: {e}; reconnecting in {backoff}s")
            await asyncio.sleep(backoff + random.random())
            backoff = min(backoff * 2, 60)


async def main():
    # load top-100 symbols
    top100_file = DATA_DIR / "top100_symbol.json"
    if not top100_file.exists():
        print(f"Top100 file not found at {top100_file}")
        return

    with open(top100_file, "r") as f:
        symbols = json.load(f)

        preferred_quotes = ("USDT", "USDC", "BUSD")
        pairs = []
        skipped = []
        exchange_info_url = "https://api.binance.com/api/v3/exchangeInfo"

        async with httpx.AsyncClient() as client:
            try:
                r = await client.get(exchange_info_url, timeout=20.0)
                r.raise_for_status()
                info = r.json()
                available = {s["symbol"]: s for s in info.get("symbols", [])}
            except Exception as e:
                print(f"Failed to load exchangeInfo: {e}")
                available = {}

            for sym in symbols:
                if sym.upper() in preferred_quotes:
                    skipped.append((sym, "symbol equals preferred quote"))
                    continue

                found = False
                for quote in preferred_quotes:
                    pair = f"{sym}{quote}".upper()
                    if pair in available:
                        pairs.append(pair)
                        found = True
                        break

                if not found:
                    for pair_sym, meta in available.items():
                        if (
                            pair_sym.startswith(sym.upper() + "")
                            and pair_sym not in pairs
                        ):
                            pairs.append(pair_sym)
                            found = True
                            break

                if not found:
                    skipped.append((sym, "no available pair found"))

    if not pairs:
        print("No valid pairs to stream")
        return

    print(f"Selected {len(pairs)} pairs to stream; skipped {len(skipped)} symbols")
    if skipped:
        for s, reason in skipped[:20]:
            print(f"Skipped: {s} -> {reason}")

    async with httpx.AsyncClient() as client:
        tasks = [asyncio.create_task(orderbook_stream(pair, client)) for pair in pairs]

        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            print("Shutting down... cancelling tasks")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted")
