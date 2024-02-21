import time
import logging
import re
import numpy as np

from common.external_resources import SteamWebApi, OpenExRatesApi, S3Bucket
from typing import NamedTuple
from pandas import DataFrame

"""TODO:
1. make the remaining method docs using google syntax
2. build save function
"""


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
                 s3_bucket: S3Bucket,
                 src_conf: SteamPricesETLSourceConfig,
                 trg_conf: SteamPricesETLTargetConfig,):
        self._logger = logging.getLogger(__name__)
        self.steam_api = steam_api
        self.ex_rates_api = ex_rates_api
        self.src_conf = src_conf
        self.trg_conf = trg_conf
        self.s3 = s3_bucket

    # Extract
    def get_currency_rates(self, base_currency: str,
                           ex_currencies: str) -> dict:
        ex_rates = self.ex_rates_api.get_ex_rates(base_currency,
                                                  ex_currencies)
        # rate is 1 because all prices are converted to usd
        ex_rates.update({"USD": 1})
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

    def get_prices_per_app(self,
                           app_ids: list,
                           currencies: dict,
                           ex_rates: dict,
                           wait_time=3) -> list:
        prices = list()
        for app in app_ids:
            self._logger.info(f"started processing {app} prices")
            for (cc, currency_name) in currencies.items():
                try:
                    app_price_str, steam_currency = self.steam_api.get_app_price(app_id=app,
                                                                                 country_code=cc)
                    # if country uses usd, then rate is 1
                    rate = ex_rates.get(steam_currency.upper())
                    # parse API price to get float value
                    _, usd_price = self.parse_app_price(app_price_str, rate, steam_currency)
                    prices.append((app, cc.lower(), steam_currency.lower(), usd_price))
                    # wait X time to prevent DDoS
                    time.sleep(wait_time)
                except Exception as e:
                    self._logger.error(f"{cc} for {app} could not be processed... skipping...")
                    self._logger.error(f"{e}")
                    continue

            self._logger.info(f"finished processing {app} prices")
        return prices

    # Load
    def generate_games_data(self):
        """ Use a dataframe for better formatting and save it """
        ex_rates = self.get_currency_rates(self.src_conf.base_currency,
                                           list(self.src_conf.ex_currencies.values()))
        prices = self.get_prices_per_app(app_ids=self.src_conf.videogames_appids,
                                         currencies=self.src_conf.ex_currencies,
                                         ex_rates=ex_rates)
        df = DataFrame(data=prices, columns=["app", "country_iso", "currency_steam", "usd_price"])
        print(df.to_parquet())
        todays_date = datetime.now().strftime("%m/%d/%Y-%H:%M:%S")
        self.s3.save_df_to_parquet(df, f'steam_etl/steam_etl_run_{todays_date}.parquet')
