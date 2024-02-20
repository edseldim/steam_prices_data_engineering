import logging
import logging.config
import yaml
import argparse

from transformers.etl_transformer import (SteamPricesETL,
                                          SteamPricesETLSourceConfig,
                                          SteamPricesETLTargetConfig)
from common.external_resources import SteamWebApi, OpenExRatesApi


def main():

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
    # reading source configuration
    source_config = SteamPricesETLSourceConfig(**config["steam_prices_etl"]["source"])
    # reading target configuration
    target_config = SteamPricesETLTargetConfig(**config["steam_prices_etl"]["target"])
    logger.info("Resources ETL has started...")
    steam_prices_etl = SteamPricesETL(steam_api=steam_api,
                                      ex_rates_api=ex_rates_api,
                                      src_conf=source_config,
                                      trg_conf=target_config)
    # running ETL job for the required ETL resources
    steam_prices_etl.generate_games_data()
    logger.info("Resources ETL has finished...")


if __name__ == "__main__":
    main()
