import time
import logging
import bs4
import re
import numpy as np

from transformers.utils import save
from common.external_resources import SteamWebApi, OpenExRatesApi
from typing import NamedTuple
from pandas import DataFrame

"""TODO:
5. make the remaining method docs using google syntax"""

class SteamPricesETLSourceConfig(NamedTuple):
    src_videogames_list_link: str
    src_videogame_usd_prices_link: str
    base_currency: str
    ex_currencies: str
    videogames_appids: str

class SteamPricesETLTargetConfig(NamedTuple):
    trg_filename_available_tags: str
    trg_filename_usd_ex_rates: str

class SteamPricesETL:

    def __init__(self,
                 steam_api: SteamWebApi,
                 ex_rates_api: OpenExRatesApi,
                 src_conf: SteamPricesETLSourceConfig,
                 trg_conf: SteamPricesETLTargetConfig,):
        self._logger = logging.getLogger(__name__)
        self.steam_api = steam_api
        self.ex_rates_api = ex_rates_api
        self.src_conf = src_conf
        self.trg_conf = trg_conf

    # Extract
    def get_currency_rates(self, base_currency: str, ex_currencies: str) -> dict:
        ex_rates = self.ex_rates_api.get_ex_rates(base_currency,
                                                   ex_currencies)
        self._logger.debug(f"ex rates from api {ex_rates}")
        return ex_rates

    # Transform
    def parse_app_price(self,
                        price_str: str,
                        ex_rate: float,
                        currency_name: str) -> tuple:
        try:
            price_formatted = re.findall(r"[0-9]+[,\. ]?[0-9]*", price_str)
            if price_formatted:
                usd_price_formatted_str = price_formatted[0].replace(" ", "").replace(".", "").replace(",", ".")
                # if the country uses . for thousands this is ok
                usd_price = float(usd_price_formatted_str) / ex_rate
                if usd_price > 100 or usd_price < 0.1:
                    usd_price_formatted_str = price_formatted[0].replace(" ", "").replace(",", "")
                    # use this for countries that use . for floating point numbers
                    usd_price = float(usd_price_formatted_str) / ex_rate
                return (currency_name, usd_price)
        except Exception:
            self._logger.debug(f"execution for {currency_name} failed. Sending NaN...")
        return (currency_name, np.nan)  # if the price couldn't be parsed, then send NaN

    def get_app_prices_per_app(self, ex_rates, wait_time=3) -> list:
        prices = list()
        for app in self.src_conf.videogames_appids:
            self._logger.info(f"started processing {app} prices")
            for (cc, ex_rate) in ex_rates.items():
                country_code = [country for country, currency in self.src_conf.ex_currencies.items() if currency.lower() == cc.lower()]
                country_code = country_code[0] if len(country_code) > 0 else "us"
                try:
                    app_price_str, currency_name = self.steam_api.get_app_price(app, country_code)
                except Exception:
                    self._logger.error(f"{country_code} for {app} could not be processed... skipping...")
                    continue
                rate = 1
                currency = "usd"
                if currency_name.lower() != "usd":
                    rate = ex_rate
                    currency = cc
                _, usd_price = self.parse_app_price(app_price_str, rate, currency)
                prices.append((app, country_code, currency, usd_price))
                time.sleep(wait_time)
            self._logger.info(f"finished processing {app} prices")
        return prices

    # Load
    def generate_games_data(self):
        """ Use a dataframe for better formatting and save it """
        ex_rates = self.get_currency_rates(self.src_conf.base_currency,
                                           list(self.src_conf.ex_currencies.values()))
        prices = self.get_app_prices_per_app(ex_rates)
        df = DataFrame(data=prices, columns=["app", "country_iso", "currency_steam", "usd_price"])
        print(df)
