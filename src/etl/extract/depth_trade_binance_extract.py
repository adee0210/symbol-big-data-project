import asyncio
import datetime
import json
import os
from pdb import run
import sys

import websockets

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
)
from config.logger_config import LoggerConfig
from config.variable_config import DEPTH_TRADE_BINANCE_CONFIG


class DepthTradeBinanceExtract:
    def __init__(self):
        try:
            self.logger = LoggerConfig.logger_config(
                "Extract depth - trade from top 100 coin in Binance"
            )
            # Initialize depth_trade_binance_url first
            self.depth_trade_binance_url = DEPTH_TRADE_BINANCE_CONFIG[
                "depth_trade_binance_url"
            ]
            
            # Get the correct path relative to the project root
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
            symbol_file_path = os.path.join(project_root, "data", "processed", "top100_symbol.json")
            
            with open(symbol_file_path, "r") as f:
                self.symbol_list = json.load(f)
            
            self.trade_symbol_list = [
                f"{symbol.lower()}usdt@trade" for symbol in self.symbol_list
            ]
            self.depth_symbol_list = [
                f"{symbol.lower()}usdt@depth" for symbol in self.symbol_list
            ]

        except Exception as e:
            self.logger.error(f"Error to config: {str(e)}")

    def trade_extract_and_clean(self):

        # connect binance trade websocket
        async def listen():
            self.logger.info("Start extract 100 symbol trade ... ")
            url_trade = self.depth_trade_binance_url + "/".join(self.trade_symbol_list)
            self.logger.info("Connect to trade binance websocket")

            backoff = 1

            async with websockets.connect(
                url_trade, ping_timeout=60, ping_interval=20
            ) as ws:
                while True:
                    try:
                        message = await ws.recv()
                        data = json.loads(message)
                        clean_data = {
                            "type": "trade",
                            "symbol": str(data.get("s")),
                            "price": float(data.get("p")),
                            "quantity": float(data.get("q")),
                            "trade_time": str(
                                datetime.datetime.fromtimestamp(data.get("T") / 1000)
                            ),
                        }
                        yield clean_data

                    except websockets.ConnectionClosed as e:
                        self.logger.error(f"Error to connect trade binance: {str(e)}")
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 60)
                    except Exception as e:
                        self.logger.error(f"Unexpected error: {str(e)}")
                        await asyncio.sleep(5)

        return listen()

    def depth_extract(self):
        async def listen():
            self.logger.info("Start extract 100 symbol depth ... ")
            url_depth = self.depth_trade_binance_url + "/".join(self.depth_symbol_list)
            self.logger.info("Connect to depth binance websocket")

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
                def process_level(level):
                    try:
                        price = float(level[0])
                        qty = float(level[1])
                        total = price * qty
                        return [price, qty, total]  # Trả về tất cả là float
                    except Exception:
                        return None

                new_bids = None
                new_asks = None
                if bids:
                    new_bids = []
                    for lvl in bids:
                        processed = process_level(lvl)
                        if processed:
                            new_bids.append(processed)
                if asks:
                    new_asks = []
                    for lvl in asks:
                        processed = process_level(lvl)
                        if processed:
                            new_asks.append(processed)
                return new_bids, new_asks

            backoff = 1

            while True:
                try:
                    async with websockets.connect(
                        url_depth, ping_timeout=60, ping_interval=20
                    ) as ws:
                        backoff = 1
                        while True:
                            try:
                                message = await ws.recv()

                                # Handle binary data
                                if isinstance(message, (bytes, bytearray, memoryview)):
                                    try:
                                        message = bytes(message).decode()
                                    except Exception:
                                        self.logger.warning(
                                            "Received non-decodable binary message, skipping"
                                        )
                                        continue

                                try:
                                    data = json.loads(message)
                                    bids, asks = _extract_levels_from_obj(data)

                                    if bids is not None or asks is not None:
                                        new_bids, new_asks = add_total_per_level(
                                            bids, asks
                                        )

                                        # Clean data - chỉ lấy các trường cần thiết
                                        clean_data = {
                                            "type": "depth",
                                            "symbol": data.get("s", "").upper(),
                                            "bids": new_bids if new_bids else [],
                                            "asks": new_asks if new_asks else [],
                                            "time": str(datetime.datetime.now()),
                                        }
                                    else:
                                        # Nếu không có bids/asks data thì skip
                                        continue

                                except json.JSONDecodeError:
                                    # Skip non-JSON messages
                                    self.logger.warning(
                                        "Received non-JSON message, skipping"
                                    )
                                    continue

                                yield clean_data

                            except websockets.ConnectionClosed as e:
                                self.logger.error(
                                    f"Depth websocket connection closed: {str(e)}"
                                )
                                break
                            except Exception as e:
                                self.logger.error(
                                    f"Error processing depth message: {str(e)}"
                                )
                                await asyncio.sleep(1)

                except (websockets.ConnectionClosed, OSError) as e:
                    self.logger.error(
                        f"Error to connect depth binance: {str(e)}. Reconnect in {backoff}s"
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                except Exception as e:
                    self.logger.error(f"Unexpected error in depth extract: {str(e)}")
                    await asyncio.sleep(5)

        return listen()
