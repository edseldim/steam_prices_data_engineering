import time
import logging
import re
import numpy as np
import pandas as pd

from common.external_resources import SteamWebApi, OpenExRatesApi, S3Bucket
from datetime import datetime
from typing import NamedTuple
from pandas import DataFrame

"""TODO:
1. make the remaining method docs using google syntax
2. Check the target conf in yaml (the target now is the bucket the etl points to)
"""


class SteamPricesETLSourceConfig(NamedTuple):
    """
    Object that represents the source configuration for
    SteamPricesETL


    :param base_currency: base currency for exchange rates to be relative to
    :param ex_currencies: currencies to get the exchange rates (for base currency)
    :param videogames_appids: videogames to get the prices of in the different exchange
                              currencies
    """
    base_currency: str
    ex_currencies: str
    videogames_appids: str


class SteamPricesETLTargetConfig(NamedTuple):
    """
    Represents the target configuration for
    SteamPricesETL

    :param trg_key: the folder inside the target storage service
    :param trg_key_filename: the filename for the data to be stored
    :param trg_key_date_format: the filename date format in target storage service
                            this one is appended to the filename at the end
    :param trg_format: the filename format for the data to be stored
    :param trg_cols: columns included in the filename to be stored
    """
    trg_key: str
    trg_key_date_format: str
    trg_key_filename: str
    trg_format: str
    trg_cols: list


class SteamPricesETL:
    """
    Extracts, transforms and loads steam games price data
    for different countries
    """

    def __init__(self,
                 steam_api: SteamWebApi,
                 ex_rates_api: OpenExRatesApi,
                 s3_bucket: S3Bucket,
                 src_conf: SteamPricesETLSourceConfig,
                 trg_conf: SteamPricesETLTargetConfig,):
        """
        Constructor for SteamPricesETL

        :param steam_api: connection to steam market api
        :param ex_rates_api: connection to exchange rates api
        :param s3_bucket: connection to s3 bucket api
        :param src_conf: NamedTuple class with source configuration data
        :param trg_conf: NamedTuple class with target configuration data
        """
        self._logger = logging.getLogger(__name__)
        self.steam_api = steam_api
        self.ex_rates_api = ex_rates_api
        self.src_conf = src_conf
        self.trg_conf = trg_conf
        self.s3 = s3_bucket

    # Extract
    def get_currency_rates(self, base_currency: str,
                           ex_currencies: str) -> dict:
        """
        Reads currency exchange rates relative to base currency

        :param base_currency: currency (ALPHA-3) exchange rates will be relative to
        :param ex_currencies: currencies (ALPHA-3) to get exchange rate for (relative to base currency)

        :returns:
            dict: containing currency names (ALPHA-3) as key and exchange rates as values
        """
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
        """
        Parses steam market api app price data to float type and calculates the
        exchange rate for currency

        :param price_str: steam market api app price (raw data from api)
        :param ex_rate: exchange rate to convert price from local currency (currency_name)
                        to whatever the exchange rate is relative to
        :param currency_name: currency name (ALPHA-2) for price_str

        :returns:
            tuple: first position as currency name (the same as the param), and
                   second position as price in the currency the exchange rate is
                   relative to
        """
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
        """
        Gets prices for apps in the currencies the exchange rates are relative to

        :param app_ids: list with the steam games id to get the price of in different currencies
        :param currencies: dict containing the country code (ALPHA-2) and their currency
                            names (ALPHA-3) as values
        :param ex_rates: dict containing currency names (ALPHA-3) as key and exchange rates as values
        :wait_time: time to wait between price extraction (to prevent DDoS attacks)

        :returns:
            list: items as tuples each containing the app_id, country code (ALPHA-2),
                    currency steam uses for app_id in country and price in usd for
                    country
        """
        prices = list()
        for app in app_ids:
            self._logger.info(f"started processing {app} prices")
            for (cc, _) in currencies.items():
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

    def save_as_parquet_to_s3(self,
                              df: pd.DataFrame,
                              filename: str) -> bool:
        """
        Saves df to external storage service

        :param df: Dataframe with the data to be stored
        :param filename: string with the filename the DataFrame will be stored in.
                         DO NOT ADD THE DATA TYPE SUFFIX (.parquet)

        :returns:
            bool: True if data was loaded correctly
        """

        self.s3.save_df_to_parquet(df, filename+".parquet")
        return True

    # Load
    def generate_games_data(self):
        """
        Builds steam app data for different countries and saves it
        to external storage service
        """
        ex_rates = self.get_currency_rates(self.src_conf.base_currency,
                                           list(self.src_conf.ex_currencies.values()))
        prices = self.get_prices_per_app(app_ids=self.src_conf.videogames_appids,
                                         currencies=self.src_conf.ex_currencies,
                                         ex_rates=ex_rates)
        # fit data into dataframe
        df = DataFrame(data=prices, columns=self.trg_conf.trg_cols)
        print(df.to_parquet())
        todays_date = datetime.now().strftime(self.trg_conf.trg_key_date_format)
        filename = f'{self.trg_conf.trg_key}{self.trg_conf.trg_key_filename}{todays_date}'
        # save it
        self.save_as_parquet_to_s3(df=df,
                                   filename=filename)