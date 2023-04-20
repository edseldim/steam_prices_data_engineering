import numpy as np
import re
import logging
import concurrent.futures
import json
import time

from extract import send_request, get_country_prices

# set up logger for file
logger = logging.getLogger("transform")
handler1 = logging.FileHandler('Scripts/Logs/logger1.log')
formatter1 = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler1.setFormatter(formatter1)
logger.addHandler(handler1)
if logger.hasHandlers():
    logger.handlers.clear()
    logger.addHandler(handler1)
logger.setLevel(logging.DEBUG)

def process_country_prices(soup):

    """extracts the raw value and country tags and processes them into clean values
    
        Parameters:
        soup: soup object that belongs to the beautifulsoup4 library
        
        Returns:
        A list with the country name in iso code, currency name, game's price in local currency and game's price in USD"""

    country_tags_raw = soup.find("div",class_="table-responsive").find_all("td",attrs={"data-cc":True})
    countries_row_data_raw = soup.find("div",class_="table-responsive").find_all("tr")
    price_tags_raw = soup.find("div",class_="table-responsive").find_all("td",class_="table-prices-converted")
    usd_prices_processed = []
    countries_local_price_processed = []
    for tag in price_tags_raw:
        if "%" not in tag.text:
            if tag.text == "N/A":
                usd_prices_processed.append(np.nan)
            else:
                usd_prices_processed.append(float(re.findall(r"[0-9]+\.?[0-9]*",tag.text)[0]))

    for tag in countries_row_data_raw:
        if tag.find_all("td"):
            price_match = re.findall(r"[0-9]+[,\.]?[0-9]*",tag.find_all("td")[1].text)
            if price_match:
                countries_local_price_processed.append(float(price_match[0].replace(",",".")))
            else:
                countries_local_price_processed.append(np.nan)

    iso_country_code_processed = [tag["data-cc"].strip("\n").strip(" ") for tag in country_tags_raw]
    countries_name_processed = [tag.text.strip("\n").strip(" ") for tag in country_tags_raw]
    countries_prices_processed = list(zip(iso_country_code_processed, countries_name_processed, countries_local_price_processed, usd_prices_processed))
    return countries_prices_processed

def obtain_usd_rates(price_data):

    usd_rates = {}
    for country_data in price_data:
        country_iso_code = country_data[0] 
        local_price = country_data[-2]
        usd_price = country_data[-1]
        usd_ex_rate = local_price/usd_price
        usd_rates[country_iso_code] = round(usd_ex_rate,2)

    return usd_rates

def retrieve_steam_data(videogame_urls_processed, usd_rates, checkpoint_game_id, checkpoint_index, logger=logger, batch = 10, wait_time = 10):
    game_data = {}
    start = 0
    end = len(usd_rates.keys()) - 1
    #batch = 10
    country_iso_codes = list(usd_rates.keys())
    #wait_time = 10
    videogame_urls_processed = sorted([int(re.findall(r"[0-9]+",game_id)[0]) for game_id in videogame_urls_processed]) # for reproducibility purposes when loading the checkpoint
    for game_id in videogame_urls_processed:
        if int(game_id) < int(checkpoint_game_id):
            continue # if game_id has already been processed, then ignore
        params = {
                        "appids":f"{game_id}",
                        "cc":"us"
                }
        json_data = send_request(params)
        if not json_data.get("is_free"):
            game_data[game_id] = list()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for i in range(start, end, batch):
                    if i < checkpoint_index and int(game_id) == int(checkpoint_game_id):
                        continue

                    for j in range(batch):

                        params = {
                            "appids":f"{game_id}",
                            "cc":country_iso_codes[i+j]
                        }
                        logger.debug(f"Country {country_iso_codes[i+j]} for game {game_id} and batch {i} started processing...")
                        futures.append(executor.submit(get_country_prices,params=params, usd_rates=usd_rates))
                    
                    for future in concurrent.futures.as_completed(futures):
                        game_data[game_id].append(future.result())

                        # add checkpoint save
                        with open("Data/game_processing_game_data.txt","w") as f:
                            json.dump(game_data,f)

                        with open("Data/game_processing_checkpoint_data.txt","w") as f:
                            json.dump({
                                "index":i,
                                "game_id":game_id
                            },f)

                    time.sleep(wait_time)
            logger.debug(f"game {game_id} has been processed...")
            game_data[game_id] = dict(game_data[game_id])
    return game_data