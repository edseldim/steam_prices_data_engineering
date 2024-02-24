import io
import logging
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.colors as colors

from babel.numbers import get_territory_currencies
from common.external_resources import S3Bucket
from matplotlib.figure import Figure
from datetime import datetime
from typing import NamedTuple

"""
TODO:
    1. Make sure the ETL executes
    2. Parametrize all the necessary details (add to conf). Like image params or url
"""

class WorldMapETLSourceConfig(NamedTuple):
    """
    Object that represents the source configuration for
    WorldMapETL
    """
    parquet_key: str
    world_map_geopandas: str
    iso_code_map: str
    iso_code_map_cols: str
    plot_title: str
    missing_countries_plot_args: dict
    world_map_prices_plot_args: dict


class WorldMapETLTargetConfig(NamedTuple):
    """
    Represents the target configuration for
    WorldMapETL
    """
    trg_key: str
    trg_key_date_format: str
    trg_key_filename: str
    trg_format: str

class WorldMapETL:

    def __init__(self,
                 src_conf: WorldMapETLSourceConfig,
                 trg_conf: WorldMapETLTargetConfig,
                 s3_bucket: S3Bucket):
        self.src_conf = src_conf
        self.trg_conf = trg_conf
        self.s3_bucket = s3_bucket
        self._logger = logging.getLogger(__name__)

    def calculate_countries_averages(df: pd.DataFrame):
        country_means_df = df[["country_iso", "usd_price"]] \
                        .groupby("country_iso") \
                        .agg({"usd_price": "mean"})
        worlds_average_price = df["usd_price"].mean()
        country_means_df["perc_dif"] = (country_means_df/worlds_average_price) - 1
        country_means_df["usd_dif"] = country_means_df["perc_dif"] * worlds_average_price
        country_means_df.reset_index(inplace=True)
        country_means_df["country_iso"].replace("uk", "gb", inplace=True)
        country_means_df["country_iso"] = country_means_df["country_iso"].str.upper()
        return country_means_df.copy()

    def get_geospatial_df(geo_data_url: str):
        world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        # create a lookup table to map iso_2_codes and iso_3_codes
        iso_lookup = pd.read_csv(geo_data_url, usecols=['alpha-3', 'alpha-2'])
        iso_lookup = iso_lookup.set_index('alpha-3')

        # add a new column with iso_a2 codes to the world GeoDataFrame
        world['iso_a2'] = world['iso_a3'] \
                          .apply(lambda x: iso_lookup.loc[x]['alpha-2'] if x in iso_lookup.index else None)

        # map european countries under the same iso code for ease of data processing
        european_countries_iso_codes = world[world["continent"] == "Europe"]["iso_a2"]
        euro_countries = []
        for i, country_iso_code in european_countries_iso_codes.items():
            if country_iso_code:
                euro_country = get_territory_currencies(country_iso_code)
                if euro_country[0].lower() == "eur":
                    euro_countries.append(country_iso_code.lower())

        # change euro countries' iso_2 code to EU so that they can be joined with the world_prices_df
        world["iso_a2"] = world["iso_a2"][~world["iso_a2"].isna()] \
                        .apply(lambda country_code: "EU" if country_code.lower() in euro_countries else country_code)

        return world.copy()

    def merge_geodata_with_prices(country_price_df: pd.DataFrame,
                                  geospatial_df):
        country_price_df["country_iso"] = country_price_df["country_iso"].str.upper()
        merged_df = geospatial_df.merge(country_price_df, left_on='iso_a2', right_on='country_iso')
        return merged_df.copy()

    def get_world_map(merged_df: pd.DataFrame):
        missing_countries = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        missing_countries["steam_value"] = 0
        # Create a custom colormap that ranges from green to red
        cmap = colors.LinearSegmentedColormap.from_list('green_to_red', [(0, 'green'), (1, 'red')])

        # Normalize the data using vmin and vmax
        norm = colors.Normalize(vmin=merged_df["usd_dif"].min(), vmax=merged_df["usd_dif"].max())
        # Create a large plot
        fig, ax = plt.subplots(figsize=(10, 8))

        from mpl_toolkits.axes_grid1 import make_axes_locatable

        # We make the legend as small and thin as possible
        divider = make_axes_locatable(ax)
        cax = divider.append_axes("right", size="10%", pad=0.1)

        # we display the missing country data and the countries with data will be overriden by the merged_df plot
        missing_countries.plot(column="steam_value",
                               ax=ax, color="gray",
                               linestyle='-',
                               edgecolor='black',
                               alpha=0.5)
        merged_df.plot(column='usd_dif',
                       ax=ax,
                       cmap=cmap,
                       norm=norm,
                       cax=cax,
                       linewidth=0.5,
                       linestyle='-',
                       edgecolor='black',
                       legend=True)
        ax.set_axis_off()
        ax.set_title("Steam prices relative to world average in USD")
        return ax.get_figure()

    def save_current_fig(self,
                         fig: Figure,
                         filename: str):
        self.s3_bucket.save_fig_to_png(fig, filename)

    def generate_world_map_image(self):
        self._logger.info(f"Looking up last file in {self.src_conf.parquet_key}")
        last_processed_file = self.s3_bucket.get_bucket_filenames(bucket_name=self.s3_bucket.bucket_name,
                                                                  prefix=self.src_conf.parquet_key)[0]
        self._logger.info(f"Downloading {last_processed_file}...")
        f = io.BytesIO()
        self.s3_bucket.s3_session.download_fileobj(self.s3_bucket.bucket_name,
                                                   last_processed_file,
                                                   f)
        df = pd.read_parquet(f)
        prices_df = self.calculate_countries_averages(df=df.copy())
        world_map_df = self.get_geospatial_df(geo_data_url=self.src_conf.iso_codes_map_url)
        merged_df = self.merge_geodata_with_prices(country_price_df=prices_df.copy(),
                                                   geospatial_df=world_map_df.copy())
        fig = self.get_world_map(merged_df=merged_df.copy())

        todays_date = datetime.now().strftime(self.trg_conf.trg_key_date_format)
        filename = f'{self.trg_conf.trg_key}{self.trg_conf.trg_key_filename}{todays_date}'
        self.save_current_fig(fig=fig,
                              filename=filename)
