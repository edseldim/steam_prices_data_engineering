import numpy as np
import time
import bs4
import logging
import json
import requests
import re

from undetected_chromedriver import Chrome, ChromeOptions
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By


def extract_list_videogames(browser, logger=logging.getLogger("extract"), list_number=1):

    """extracts games' ids from a website with a games id ordered list
        Parameters:
        browser: driver selenium object
        
        Returns:
        A soup object that belongs to the beautifulsoup4 library """

    try:
        browser.get('https://steamdb.info/charts/')
        time.sleep(10)
        html = browser.page_source
        soup = bs4.BeautifulSoup(html, "html.parser")
    except Exception as e:
        browser.quit()
        logger.debug("Hubo un problema",e)
    else:
        browser.quit()
        return soup

def get_game_prices(game_url,browser,keep_browser):

    """extracts a game's cost per country
        Parameters:
        game_url: a string with a game's url from steamdb site
        browser: driver selenium object
        keep_browser: boolean (this opens the game_url on a different tab in case many games' urls are being opened on browser)
        
        Returns:
        A soup object that belongs to the beautifulsoup4 library"""

    if keep_browser:
        browser.execute_script(f"window.open('https://steamdb.info{game_url}', '_blank');")
        browser.switch_to.window(browser.window_handles[-1])
    else:
        browser.get('https://steamdb.info'+game_url)
    time.sleep(30+np.random.random()*60)
    us_cookie = {
        "name":"__Host-cc",
        "value":"us"
    }
    browser.add_cookie(us_cookie)
    browser.refresh()
    button = browser.find_element(By.ID,'js-currency-selector')
    button.click()
    time.sleep(5)
    button = browser.find_element(By.CSS_SELECTOR,'button[data-cc="us"]')
    button.click()
    time.sleep(5)
    html = browser.page_source
    soup = bs4.BeautifulSoup(html, "html.parser")
    return soup

def get_list_reference():
    options = ChromeOptions()
    options.add_argument("--disable-popup-blocking") # disables popup blocking for being able to open a new tab
    browser = Chrome(options=options)
    videogame_list_soup = extract_list_videogames(browser)
    videogame_urls_tags = videogame_list_soup.find_all("a", href=lambda href: href and href.startswith("/app/"))
    videogame_urls_processed = list(set([tag["href"].strip(" ").rstrip("/charts").lstrip("/app/") for tag in videogame_urls_tags])) # removes /charts/app and whitespaces from urls
    return videogame_urls_processed

def get_price_data():
    options = ChromeOptions()
    options.add_argument("--disable-popup-blocking") # disables popup blocking for being able to open a new tab
    browser = Chrome(use_subprocess=True,options=options) # use_subprocess is necessary for bypassing anti-scrapping measurements
    for i,url in enumerate(["/app/221100"]): 
        try:
            if i == 0:
                videogame_website_soup = get_game_prices(url,browser,keep_browser=False) # don't open a new tab for first game
            else:
                videogame_website_soup = get_game_prices(url,browser,keep_browser=True) # open a new tab for subsequent games not to be asked for cloudfare human validation
        except NoSuchElementException as e: # if there's a free videogame, this error will propagate which causes the script to close the browser and open another
            logging.debug(e)
            logging.debug("restarting webbrowser...")
            
            browser.quit()
            options = ChromeOptions()
            options.add_argument("--disable-popup-blocking")
            browser = Chrome(use_subprocess=True,options=options)
        if videogame_website_soup is not None: # if the videogame is not free (there's an available price), then add it to the dict 
            price_data = videogame_website_soup

    browser.close()
    return price_data

def checkpoint_data_extraction(logger=logging.getLogger("extract")):

    checkpoint_data = {}
    try:
        with open("Data/game_processing_checkpoint_data.txt","r") as f:
            checkpoint_data = json.load(f)
    except FileNotFoundError:
        logger.debug("checkpoint file hasn't been created...")

    checkpoint_game_id = checkpoint_data.get("game_id") or 0
    checkpoint_index = checkpoint_data.get("index") or 0

    return checkpoint_game_id, checkpoint_index

def send_request(params, logger=logging.getLogger("extract")):

    """ sends a request to steam store API with some params and sends back the retrieved data 
        Parameters:
        params => dict-like object
        Returns: dict-like object containing game's data"""

    req = requests.get("https://store.steampowered.com/api/appdetails/", params=params)
    try:
        json_data = json.loads(req.text)
        if json_data[params["appids"]]["success"]: # if the game has regional restrictions, this won't meet
            return json_data[params["appids"]]["data"]
    except Exception as e: # in case the get request fails
        logger.debug(f"request to {params.get('appids')} failed... {e}")
    return {} #send_request(params)

def get_country_prices(usd_rates, params, logger=logging.getLogger("extract")):

    """ processes the data from the steam store API to obtain a game's price per country
        Parameters:
        params => dict-like object
        Returns:
        A tuple containing the currency's name as first item, and the game's usd price for that country in the second position"""

    json_data = send_request(params=params)
    try:
        if json_data:
            price_formatted = re.findall(r"[0-9]+[,\. ]?[0-9]*",json_data.get("price_overview").get("final_formatted"))
            if price_formatted:
                usd_price = float(price_formatted[0].replace(" ","").replace(".","").replace(",",".")) / usd_rates[params["cc"]] # if the country uses . for thousands this is ok
                if usd_price > 100 or usd_price < 0.1:
                    usd_price = float(price_formatted[0].replace(" ","").replace(",","")) / usd_rates[params["cc"]] # use this for countries that use . for floating point numbers
                return (params["cc"],usd_price)
    except Exception as e:
        logger.debug(f"execution for {params['cc']} and {params['appids']} failed. Sending NaN...")
    return (params["cc"],np.nan) # if the price couldn't be parsed, then send NaN