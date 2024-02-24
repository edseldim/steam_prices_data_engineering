import io
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.colors as colors

from babel.numbers import get_territory_currencies
from common.external_resources import S3Bucket
from Scripts.transformers.steam_prices_transformer import SteamPricesETLTargetConfig
from matplotlib.figure import Figure

class WorldMapETLSourceConfig(NamedTuple):
    """
    Object that represents the source configuration for
    WorldMapETL

    """


class WorldMapETLTargetConfig(NamedTuple):
    """
    Represents the target configuration for
    WorldMapETL

    """

class WorldMapETL:

    def __init__(self):
        pass

    def calculate_countries_averages(df: pd.DataFrame):
        pass

    def get_geospatial_df(geo_data_url: str):
        pass

    def merge_geodata_with_prices(country_price_df: pd.DataFrame,
                                  geospatial_df: pd.DataFrame):
        pass

    def generate_world_map(merged_df: pd.DataFrame):
        pass