import json
import requests
import logging
import boto3
import io
import pandas as pd

from matplotlib.figure import Figure

class S3Bucket:

    """
    Represents connection to S3 Bucket
    """

    def __init__(self,
                 endpoint_url: str,
                 region_name: str,
                 aws_access_key_id: str,
                 aws_secret_access_key: str,
                 bucket_name: str):
        """
        Constructor for S3Bucket

        :param endpoint_url: endpoint url (where bucket is located)
        :param region_name: region bucket is in
        :param aws_access_key_id: secret key for session initialization
        :param aws_secret_access_key: secret access key for session initialization
        :param bucket_name: S3 bucket name
        """
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.bucket_name = bucket_name
        self.session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
                                             aws_secret_access_key=aws_secret_access_key)
        self.s3_session = self.session.client('s3', endpoint_url=endpoint_url, region_name=region_name)

    def save_df_to_parquet(self, df: pd.DataFrame, key: str) -> bool:
        """
        Handles S3 bucket connection to save dataframes

        :param df: DataFrame with the data to be saved
        :param key: string path in bucket where data is going to be located in (containing filename too)

        :returns:
            bool: True if data was loaded successfully
        """
        df_buffer = io.BytesIO()
        df.to_parquet(df_buffer, engine='auto', compression='snappy')
        df_buffer.seek(0)
        self.s3_session.upload_fileobj(df_buffer, self.bucket_name, key)
        return True

    def save_fig_to_png(self, fig: Figure, key: str) -> bool:
        """
        Handles S3 bucket connection to save dataframes

        :param df: DataFrame with the data to be saved
        :param key: string path in bucket where data is going to be located in (containing filename too)

        :returns:
            bool: True if data was loaded successfully
        """
        buffer = io.BytesIO()
        fig.savefig(buffer, format="png")
        buffer.seek(0)
        self.s3_session.upload_fileobj(buffer, self.bucket_name, key)
        return True

    def get_bucket_filenames(self,
                             bucket_name: str,
                             prefix: str,
                             same_level=True,
                             delimiter="/") -> list:
        filenames = []
        api_res = self.s3_session.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter=delimiter)
        result = api_res['Contents']
        result = sorted(result, key=lambda d: d['LastModified'], reverse=True)
        filenames = [file_content["Key"] for file_content in result]
        if same_level:
            sub_prefixes = api_res["CommonPrefixes"]
            sub_prefixes = [sub_prefix.get("Prefix") for sub_prefix in sub_prefixes]
            is_file_in_sub_prefix = lambda file: any(sub_prefix in file for sub_prefix in sub_prefixes)
            filenames = list(filter(lambda file: not is_file_in_sub_prefix(file), filenames))
        return filenames

class SteamWebApi:

    """
    Represents connection to Steam Market API
    """

    def __init__(self,
                 endpoint: str):
        """
        Constructor for SteamWebAPI

        :param endpoint: API's endpoint
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint = endpoint

    def get_app_price(self, app_id: int, country_code: str = "us") -> tuple:
        """
        Gets app price from API

        :param app_id: int representing the app id
        :param country_code: string representing the country code (ALPHA-2) for app price info

        :returns:
            tuple: price as string and currency name (ALPHA-3)
        """
        params = {"cc": country_code, "appids": app_id}
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
    """
    Represents a connection to OpenExchangeRates API
    """

    def __init__(self,
                 endpoint: str,
                 app_token: str):
        """
        Constructor for OpenExRatesAPI

        :param endpoint: exchange rate endpoint
        :param app_token: api token for auth
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint = endpoint
        self.app_token = app_token

    def get_ex_rates(self,
                     base_currency: str,
                     other_currencies: list) -> dict:
        """
        Gets exchange rates from API

        :param base_currency: currency (ALPHA-3) exchange rates will be relative to
        :param other_currencies: currencies (ALPHA-3) to get the exchange rates of

        :returns:
            dict: currencies (ALPHA-3) as keys and exchange rates float as values
        """
        params = {"base": base_currency,
                  "app_id": self.app_token,
                  "symbols": ",".join(other_currencies)}
        self._logger.debug(f"Processing {self.endpoint} with params base={params['base']}&symbols={params['symbols']}")
        req = requests.get(self.endpoint, params=params)
        assert req.ok
        return json.loads(req.text).get("rates", None)
