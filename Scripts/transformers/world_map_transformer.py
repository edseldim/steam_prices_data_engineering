import io
import logging
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.colors as colors

from babel.numbers import get_territory_currencies
from Scripts.common.external_resources import S3Bucket
from matplotlib.figure import Figure
from datetime import datetime
from typing import NamedTuple
from pandas import DataFrame

class WorldMapETLSourceConfig(NamedTuple):
    """
    Object that represents the source configuration for
    WorldMapETL

    :param parquet_key: the folder inside the target storage service
    :param country_prices_cols: cols with the country price info
    :param country_prices_perc_dif_col: col name for the percentual price difference from world average
    :param country_prices_usd_dif_col: col name for the price difference from world average in usd
    :param country_prices_usd_price_col: col name for the world's average
    :param world_map_geopandas: geopandas built-in map name
    :param country_prices_alpha_2_col: col name for countries code in ALPHA-2
    :param country_prices_alpha_3_col: col name for countries code in ALPHA-3 (for geopandas built-in map)
    :param iso_code_map_url: url for map that maps between ALPHA-2 and ALPHA-3 country codes
    :param iso_code_map_cols: cols with ALPHA-2 and ALPHA-3 country codes for iso_code_map
    :param iso_code_map_alpha_2_col: col with ALPHA-2 country codes
    :param iso_code_map_alpha_3_col: col with ALPHA-3 country codes
    :param plot_title: plot title name
    :param color_bar_args: args from color bar
    :param color_bar_min_max: col, from the df with countries and price difference from world average,
                            that contains the price difference
    :param missing_countries_plot_args: args to create the countries with no information geopandas plot
    :param world_map_prices_plot_args: args to create the countries with price information geopandas plot
    :param divider_plot_args: args to create the price divider plot
    :param plot_args: args to create the empty canvas (plot) where all the maps are going to be drawn upon

    """
    parquet_key: str
    country_prices_cols: str
    country_prices_perc_dif_col: str
    country_prices_usd_dif_col: str
    country_prices_usd_price_col: str
    country_prices_alpha_2_col: str
    country_prices_alpha_3_col: str
    world_map_geopandas: str
    world_map_alpha_2_col: str
    world_map_alpha_3_col: str
    iso_code_map_url: str
    iso_code_map_cols: str
    iso_code_map_alpha_2_col: str
    iso_code_map_alpha_3_col: str
    plot_title: str
    color_bar_args: dict
    color_bar_min_max: str
    missing_countries_plot_args: dict
    world_map_prices_plot_args: dict
    divider_plot_args: dict
    plot_args: dict


class WorldMapETLTargetConfig(NamedTuple):
    """
    Represents the target configuration for
    WorldMapETL

    :param trg_key: the folder inside the target storage service
    :param trg_key_filename: the filename for the data to be stored
    :param trg_key_date_format: the filename date format in target storage service
                            this one is appended to the filename at the end
    :param trg_format: the filename format for the data to be stored
    """
    trg_key: str
    trg_key_date_format: str
    trg_key_filename: str
    trg_format: str

class WorldMapETL:

    """
    Generates the world map using currency
    data
    """

    def __init__(self,
                 src_conf: WorldMapETLSourceConfig,
                 trg_conf: WorldMapETLTargetConfig,
                 s3_bucket: S3Bucket):
        """
        Constructor for WorldMapETL

        :param s3_bucket: connection to s3 bucket api
        :param src_conf: NamedTuple class with source configuration data
        :param trg_conf: NamedTuple class with target configuration data
        """
        self.src_conf = src_conf
        self.trg_conf = trg_conf
        self.s3_bucket = s3_bucket
        self._logger = logging.getLogger(__name__)

    def calculate_countries_averages(self, df: pd.DataFrame) -> DataFrame:
        """
        Calculates countries price deviation from world average

        :param df: dataframe containing price data in usd.

        :returns:
            DataFrame: df containing country codes in ALPHA-2 and
                       usd difference from world average.
        """
        # load conf args to prevent repetition
        country_prices_alpha_2 = self.src_conf.country_prices_alpha_2_col
        usd_price_col = self.src_conf.country_prices_usd_price_col
        perc_dif_col = self.src_conf.country_prices_perc_dif_col
        usd_dif_col = self.src_conf.country_prices_usd_dif_col

        country_means_df = df[self.src_conf.country_prices_cols] \
                                .groupby(country_prices_alpha_2) \
                                .agg({usd_price_col: "mean"})
        worlds_average_price = df[usd_price_col].mean()
        country_means_df[perc_dif_col] = (country_means_df/worlds_average_price) - 1
        country_means_df[usd_dif_col] = country_means_df[perc_dif_col] * worlds_average_price
        country_means_df.reset_index(inplace=True)
        country_means_df[country_prices_alpha_2].replace("uk", "gb", inplace=True)
        country_means_df[country_prices_alpha_2] = country_means_df[country_prices_alpha_2].str.upper()
        return country_means_df.copy()

    def _get_alpha_3_from_2(self,
                            df: pd.DataFrame) -> pd.DataFrame:
        iso_map_iso_alpha_2 = self.src_conf.iso_code_map_alpha_2_col
        iso_map_iso_alpha_3 = self.src_conf.iso_code_map_alpha_3_col
        country_prices_alpha_2_col = self.src_conf.country_prices_alpha_2_col
        country_prices_alpha_3_col = self.src_conf.country_prices_alpha_3_col
        # create a lookup table to map iso_2_codes and iso_3_codes
        iso_lookup = pd.read_csv(self.src_conf.iso_code_map_url,
                                 usecols=self.src_conf.iso_code_map_cols)
        iso_lookup = iso_lookup.set_index(iso_map_iso_alpha_2)
        # add a new column with iso_a2 codes to the world GeoDataFrame
        df[country_prices_alpha_3_col] = df[country_prices_alpha_2_col] \
                                         .apply(lambda x: iso_lookup.loc[x][iso_map_iso_alpha_3] if x in iso_lookup.index else None)
        return df

    def get_geospatial_df(self):
        """
        Creates DataFrame containing geospatial info for each country.

        :returns:
            DataFrame: df containing geospatial data, for instance: Coordinates,
                       countries ALPHA-2 and ALPHA-3, etc.
        """
        # load conf args to prevent repetition
        world_iso_alpha_2 = self.src_conf.world_map_alpha_2_col
        world_iso_alpha_3 = self.src_conf.world_map_alpha_3_col
        iso_map_iso_alpha_2 = self.src_conf.iso_code_map_alpha_2_col
        iso_map_iso_alpha_3 = self.src_conf.iso_code_map_alpha_3_col

        # load world map
        world = gpd.read_file(gpd.datasets.get_path(self.src_conf.world_map_geopandas))
        # create a lookup table to map iso_2_codes and iso_3_codes
        iso_lookup = pd.read_csv(self.src_conf.iso_code_map_url,
                                 usecols=self.src_conf.iso_code_map_cols)
        iso_lookup = iso_lookup.set_index(iso_map_iso_alpha_3)

        # add a new column with iso_a2 codes to the world GeoDataFrame
        world[world_iso_alpha_2] = world[world_iso_alpha_3] \
                                   .apply(lambda x: iso_lookup.loc[x][iso_map_iso_alpha_2] if x in iso_lookup.index else None)

        # map european countries under the same iso code for ease of data processing
        european_countries_iso_codes = world[world["continent"] == "Europe"][world_iso_alpha_2]
        euro_countries = []
        for i, country_iso_code in european_countries_iso_codes.items():
            if country_iso_code:
                euro_country = get_territory_currencies(country_iso_code)
                if euro_country[0].lower() == "eur":
                    euro_countries.append(country_iso_code.lower())

        # change euro countries' iso_2 code to EU so that they can be joined with the world_prices_df
        world[world_iso_alpha_2] = world[world_iso_alpha_2][~world[world_iso_alpha_2].isna()] \
                                   .apply(lambda country_code: "EU" if country_code.lower() in euro_countries else country_code)

        return world.copy()

    def _merge_geodata_with_prices(self,
                                   country_price_df: DataFrame,
                                   geospatial_df: DataFrame):
        """
        Joins country prices with geospatial data using ALPHA-2 iso codes.

        :param geospatial_df: dataframe containing geospatial data for each country.
        :param country_price_df: dataframe containing price data in usd.

        :returns:
            DataFrame: df containing geospatial data and
                       usd difference from world average.
        """
        # load conf args to prevent repetition
        usd_dif_col = self.src_conf.country_prices_usd_dif_col
        world_iso_alpha_2 = self.src_conf.world_map_alpha_2_col
        world_iso_alpha_3 = self.src_conf.world_map_alpha_3_col
        country_prices_alpha_3 = self.src_conf.country_prices_alpha_3_col

        merged_df = geospatial_df.merge(country_price_df,
                                        how="left",
                                        left_on=world_iso_alpha_3,
                                        right_on=country_prices_alpha_3)
        # propagate price across EU countries
        eu_price = merged_df[merged_df[world_iso_alpha_2] == "EU"][usd_dif_col].max()
        merged_df.loc[merged_df[world_iso_alpha_2] == "EU", usd_dif_col] = eu_price
        return merged_df.copy()

    def get_world_map(self, merged_df: pd.DataFrame) -> Figure:
        """
        Generates world map. ETL step, this is not the builder
        method. Using it directly is not recommended.

        :param merged_df: df containing geospatial data and
                          usd difference from world average.

        :returns:
            Figure: Matplotlib object representation of a world map
                    representing price data.
        """
        # load conf args to prevent repetition
        usd_dif_col = self.src_conf.color_bar_min_max

        missing_countries = gpd.read_file(gpd.datasets.get_path(self.src_conf.world_map_geopandas))
        missing_countries["steam_value"] = 0
        # Create a custom colormap that ranges from green to red
        cmap = colors.LinearSegmentedColormap.from_list(**self.src_conf.color_bar_args)

        # Normalize the data using vmin and vmax
        norm = colors.Normalize(vmin=merged_df[usd_dif_col].min(), vmax=merged_df[usd_dif_col].max())
        # Create a large plot
        fig, ax = plt.subplots(**self.src_conf.plot_args)

        from mpl_toolkits.axes_grid1 import make_axes_locatable

        # We make the legend as small and thin as possible
        divider = make_axes_locatable(ax)
        cax = divider.append_axes(**self.src_conf.divider_plot_args)

        # we display the missing country data and the countries with data will be overriden by the merged_df plot
        missing_countries.plot(ax=ax, **self.src_conf.missing_countries_plot_args)
        merged_df.plot(ax=ax,
                       cmap=cmap,
                       norm=norm,
                       cax=cax,
                       **self.src_conf.world_map_prices_plot_args)
        ax.set_axis_off()
        ax.set_title(self.src_conf.plot_title)
        return ax.get_figure()

    def save_current_fig(self,
                         fig: Figure,
                         filename: str,
                         format="png") -> bool:
        """
        Saves figure to external storage service

        :param fig: matplotlib Figure representing world map.
        :param filename: string with the filename the file will be stored in.
                         DO NOT ADD THE DATA TYPE SUFFIX (.png)
        :param format: file format. Supported formats: PNG


        :returns:
            bool: returns true if code was executed successfully
        """
        self.s3_bucket.save_fig_to_png(fig, filename+"."+format)
        return True

    def generate_world_map_image(self):
        """
        Builds world map image for different countries and saves it to
        external storage service
        """
        self._logger.info(f"Looking up last file in {self.src_conf.parquet_key}")
        last_processed_file = self.s3_bucket.get_bucket_filenames(bucket_name=self.s3_bucket.bucket_name,
                                                                  prefix=self.src_conf.parquet_key)[0]
        self._logger.info(f"Downloading {last_processed_file}...")
        f = io.BytesIO()
        self.s3_bucket.s3_session.download_fileobj(self.s3_bucket.bucket_name,
                                                   last_processed_file,
                                                   f)
        df = pd.read_parquet(f)
        prices_df = self.calculate_countries_averages(df.copy())
        prices_df = self._get_alpha_3_from_2(prices_df.copy())
        world_map_df = self.get_geospatial_df()
        merged_df = self._merge_geodata_with_prices(country_price_df=prices_df.copy(),
                                                    geospatial_df=world_map_df.copy())
        fig = self.get_world_map(merged_df=merged_df.copy())

        todays_date = datetime.now().strftime(self.trg_conf.trg_key_date_format)
        filename = f'{self.trg_conf.trg_key}{self.trg_conf.trg_key_filename}{todays_date}'
        self.save_current_fig(fig=fig,
                              filename=filename,
                              format=self.trg_conf.trg_format)
