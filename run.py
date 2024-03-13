import logging
import logging.config
import yaml
import argparse

from Scripts.transformers.steam_prices_transformer import (SteamPricesETL,
                                                           SteamPricesETLSourceConfig,
                                                           SteamPricesETLTargetConfig)
from Scripts.transformers.world_map_transformer import (WorldMapETL,
                                                        WorldMapETLSourceConfig,
                                                        WorldMapETLTargetConfig)
from Scripts.common.external_resources import (SteamWebApi,
                                               OpenExRatesApi,
                                               S3Bucket)


def main():

    """
    Executes the main pipeline flow
    """

    # Parsing YAML file
    parser = argparse.ArgumentParser(description='Run the Xetra ETL job.')
    parser.add_argument('config', help='A configuration file in YAML format.')
    args = parser.parse_args()
    config = yaml.safe_load(open(args.config))
    # configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)

    # api interfaces instances for extracting external data
    steam_api = SteamWebApi(**config["steam_web_api"])
    ex_rates_api = OpenExRatesApi(**config["currency_ex_api"])
    # external storage
    s3_bucket = S3Bucket(**config["s3_bucket"])
    # reading source configuration
    steam_etl_src_config = SteamPricesETLSourceConfig(**config["steam_prices_etl"]["source"])
    world_map_etl_src_config = WorldMapETLSourceConfig(**config["world_map_etl"]["source"])
    # reading target configuration
    steam_etl_trg_config = SteamPricesETLTargetConfig(**config["steam_prices_etl"]["target"])
    world_map_etl_trg_config = WorldMapETLTargetConfig(**config["world_map_etl"]["target"])

    # Steam ETL Execution
    logger.info("SteamPricesETL has started...")
    steam_prices_etl = SteamPricesETL(steam_api=steam_api,
                                      ex_rates_api=ex_rates_api,
                                      s3_bucket=s3_bucket,
                                      src_conf=steam_etl_src_config,
                                      trg_conf=steam_etl_trg_config)
    # running ETL job for the required ETL Steam
    steam_prices_etl.generate_games_data()
    logger.info("SteamPricesETL has finished...")

    # World Map ETL Execution
    logger.info("WorldMapETL has started...")
    world_map_etl = WorldMapETL(s3_bucket=s3_bucket,
                                src_conf=world_map_etl_src_config,
                                trg_conf=world_map_etl_trg_config)
    # running ETL job for the required ETL Steam
    world_map_etl.generate_world_map_image()
    logger.info("WorldMapETL has finished...")


if __name__ == "__main__":
    main()
