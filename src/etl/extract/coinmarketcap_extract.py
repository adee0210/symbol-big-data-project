import json
import os
import sys

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
)
from time import sleep
import requests
from config.logger_config import LoggerConfig
from config.mongo_config import MongoConfig
from config.variable_config import COINMARKETCAP_CONFIG


class CMCExtract:
    def __init__(self):
        try:

            self.logger = LoggerConfig.logger_config(
                "Extract top 100 symbol data from CoinMarketCap"
            )
            self.mongo_config = MongoConfig()
            self.mongo_client = self.mongo_config.get_client()
            self.clean_cmc_collection = self.mongo_client[
                COINMARKETCAP_CONFIG["clean_database"]
            ][COINMARKETCAP_CONFIG["clean_cmc_collection"]]
            self.cmc_url = COINMARKETCAP_CONFIG["cmc_url"]
            self.cmc_api = COINMARKETCAP_CONFIG["cmc_api"]
            self.cmc_interval_send_request = COINMARKETCAP_CONFIG[
                "cmc_interval_send_request"
            ]
            self.header = {
                "Accept": "application/json",
                "X-CMC_PRO_API_KEY": self.cmc_api,
            }
            self.logger.info("Successfully retrieved CMC configurations")
        except Exception as e:
            self.logger.error(f"Error initializing crawler: {str(e)}")
            raise

    def cmc_extract(self):

        try:
            self.logger.info("Sending API request...")
            response = requests.get(url=self.cmc_url, headers=self.header)
            data = response.json()
            if data.get("status", {}).get("error_code", {}) != 0:
                error_message = data.get("status", {}).get("error_message")
                self.logger.error(f"API error: {error_message}")
                return None
            self.logger.info("Successfully retrieved the data.")

            return data
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error in cmc_extract: {str(e)}")
            return None

    def cmc_transform(self, cmc_extract_data):
        self.logger.info("Start transform cmc data")
        r = []
        for d in cmc_extract_data["data"]:
            try:
                data = {
                    "name": d["name"],
                    "symbol": d["symbol"],
                    "price": d["quote"]["USD"]["price"],
                    "volume_24h": d["quote"]["USD"]["volume_24h"],
                    "percent_change_1h": d["quote"]["USD"]["percent_change_1h"],
                    "percent_change_24h": d["quote"]["USD"]["percent_change_24h"],
                    "percent_change_7d": d["quote"]["USD"]["percent_change_7d"],
                    "market_cap_dominance": d["quote"]["USD"]["market_cap_dominance"],
                    "circulating_supply": d["circulating_supply"],
                }
                r.append(data)
            except Exception as e:
                self.logger.error(f"Error processing symbol {d['symbol']}: {str(e)}")

        return r

    def get_symbol(self, cmc_transform_data):
        symbol_list = []
        for data in cmc_transform_data:
            symbol_list.append(data["symbol"])

        # Get the correct path relative to the project root
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
        symbol_file_path = os.path.join(project_root, "data", "processed", "top100_symbol.json")
        
        with open(symbol_file_path, "w") as f:
            json.dump(symbol_list, f)

    def cmc_load(self, cmc_transform_data):
        self.logger.info("Start load cmc transform data ...")
        try:
            for data in cmc_transform_data:
                self.clean_cmc_collection.update_one(
                    {"symbol": data["symbol"]}, {"$set": data}, upsert=True
                )
            self.logger.info(
                f"Successfully loaded/updated data into {self.clean_cmc_collection.name}."
            )
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")

    def start(self, running: bool):
        if running is True:
            self.logger.info("Running extract CMC ....")
            while True:
                cmc_extract_data = self.cmc_extract()
                cmc_transform_data = self.cmc_transform(
                    cmc_extract_data=cmc_extract_data
                )
                self.get_symbol(cmc_transform_data=cmc_transform_data)

                self.logger.info("Successfully to transform 100 symbol data")
                self.cmc_load(cmc_transform_data=cmc_transform_data)
                self.logger.info(
                    f"Wait for {self.cmc_interval_send_request} second to update new data"
                )
                sleep(self.cmc_interval_send_request)
        else:
            self.logger.warning(f"Not running extract CMC. Running is {running}")


if __name__ == "__main__":
    test = CMCExtract()
    test.start(running=True)
