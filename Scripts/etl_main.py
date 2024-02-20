import logging
import logging.config
# import json
import yaml
import argparse
# import pandas as pd

# from pathlib import Path
# from load import save_data_checkpoint
# from extract import (get_videogames_urls,
#                      get_price_data,
#                      checkpoint_data_extraction,
#                      get_already_processed_data)
# from transform import (obtain_usd_rates,
#                        retrieve_steam_data,
#                        process_country_prices)
from transformers.etl_transformer import (SteamPricesETL,
                                          SteamPricesETLSourceConfig,
                                          SteamPricesETLTargetConfig)
from common.external_resources import SteamWebApi, OpenExRatesApi

# def set_up_logger():
#     logger = logging.getLogger("etl_main")
#     handler1 = logging.FileHandler('Scripts/Logs/logger1.log')
#     formatter1 = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#     handler1.setFormatter(formatter1)
#     logger.addHandler(handler1)
#     if logger.hasHandlers():
#         logger.handlers.clear()
#         logger.addHandler(handler1)
#     logger.setLevel(logging.DEBUG)
#     return logger


def main():

    # logger = set_up_logger()
    # Parsing YAML file
    parser = argparse.ArgumentParser(description='Run the Xetra ETL job.')
    parser.add_argument('config', help='A configuration file in YAML format.')
    args = parser.parse_args()
    config = yaml.safe_load(open(args.config))
    # configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    # if (not Path("Data\\usd_rates_reference.txt").exists()) \
    #         or (not Path("Data\\videogames_id_reference.txt").exists()):
    # if checkpoint has already been set, then skip initial setup
    # cs = ChromeSession(*config["resources_etl"]["chrome"]["args"],
    #                    **config["resources_etl"]["chrome"]["kwargs"])
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
    # videogame_urls_processed = get_videogames_urls()
    # price_data = process_country_prices(get_price_data())
    # save_data_checkpoint("usd_rates_reference.txt", price_data)
    # save_data_checkpoint("videogames_id_reference.txt",
    #                      videogame_urls_processed)

    # price_data = get_already_processed_data("Data\\usd_rates_reference.txt")
    # usd_rates = obtain_usd_rates(price_data)
    # videogame_urls_processed = get_already_processed_data("Data\\videogames_id_reference.txt")
    # checkpoint_game_id, checkpoint_index = checkpoint_data_extraction(logger=logger)
    # game_data = retrieve_steam_data(videogame_urls_processed,
    #                                 usd_rates,
    #                                 checkpoint_game_id,
    #                                 checkpoint_index,
    #                                 logger=logger)
    # steam_prices_df = pd.read_json(json.dumps(game_data)).T
    # steam_prices_df.to_excel("Data/list_processed.xlsx")


if __name__ == "__main__":
    main()
