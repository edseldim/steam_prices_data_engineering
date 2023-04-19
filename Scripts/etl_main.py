import pandas as pd
import logging
import json

from extract import get_list_reference, get_price_data, checkpoint_data_extraction
from load import save_data_checkpoint
from transform import obtain_usd_rates, retrieve_steam_data, process_country_prices

def set_up_logger():
    logger = logging.getLogger("etl_main")
    handler1 = logging.FileHandler('Scripts/Logs/logger1.log')
    formatter1 = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler1.setFormatter(formatter1)
    logger.addHandler(handler1)
    if logger.hasHandlers():
        logger.handlers.clear()
        logger.addHandler(handler1)
    logger.setLevel(logging.DEBUG)
    return logger

def main():
    logger = set_up_logger()
    videogame_urls_processed = get_list_reference()
    price_data = process_country_prices(get_price_data())
    save_data_checkpoint("usd_rates_reference.txt", price_data)
    save_data_checkpoint("videogames_id_reference.txt", videogame_urls_processed)
    usd_rates = obtain_usd_rates(price_data)
    checkpoint_game_id, checkpoint_index = checkpoint_data_extraction(logger=logger)
    game_data = retrieve_steam_data(videogame_urls_processed, usd_rates, checkpoint_game_id, checkpoint_index, logger=logger)
    steam_prices_df = pd.read_json(json.dumps(game_data)).T
    steam_prices_df.to_excel("Data/list_processed.xlsx")

if __name__ == "__main__":
    main()
