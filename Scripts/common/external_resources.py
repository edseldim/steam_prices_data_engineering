import json
import requests
import logging

class SteamWebApi:

    def __init__(self,
                 endpoint: str):
        self._logger = logging.getLogger(__name__)
        self.endpoint = endpoint

    def get_app_price(self, app_id: int, base_currency: str = "us") -> tuple:
        params = {"cc": base_currency, "appids": app_id}
        self._logger.debug(f"Processing {self.endpoint} with params cc={params['cc']}&appids={params['appids']}")
        req = requests.get(self.endpoint, params=params)
        assert req.ok
        json_data = json.loads(req.text)
        # check if there's price data
        assert json_data[f"{app_id}"]["data"].get("price_overview", None) is not None
        # check if there's price formatting data
        assert json_data[f"{app_id}"]["data"]["price_overview"].get("final_formatted", None) is not None
        price_str = json_data[f"{app_id}"]["data"]["price_overview"]["final_formatted"]
        currency_name = json_data[f"{app_id}"]["data"]["price_overview"]["currency"]
        return price_str, currency_name

class OpenExRatesApi:

    def __init__(self,
                 endpoint: str,
                 app_token: str):
        self._logger = logging.getLogger(__name__)
        self.endpoint = endpoint
        self.app_token = app_token

    def get_ex_rates(self,
                     base_currency: str,
                     other_currencies: list) -> dict:
        params = {"base": base_currency,
                  "app_id": self.app_token,
                  "symbols": ",".join(other_currencies)}
        self._logger.debug(f"Processing {self.endpoint} with params base={params['base']}&symbols={params['symbols']}")
        req = requests.get(self.endpoint, params=params)
        assert req.ok
        return json.loads(req.text).get("rates", None)
