import time
import logging
import bs4
import re
import numpy as np

from transformers.utils import ChromeSession, save
from typing import NamedTuple

"""TODO:
2. open a local process that mocks the webpages this uses
3. add config files to etl_main.py
4. Splitting ResourcesETL into 2 classes: one for external processing with selenium and the other for business logic
5. make the remaining method docs using google syntax

SOLVED:

-- fix chrome session and make sure that it's safe not to handle the session closure
    It required Chrome 121 to work (both the webdriver and chrome have to be the same version)

-- check how to handle the whole chrome session reusability issue:
    Do I want it to be part of the classes  or having a function that does all the initializing

    I used a class because I needed common pieces of code to be reused (for readability)"""

class ResourcesETLSourceConfig(NamedTuple):
    src_videogames_list_link: str
    src_videogame_usd_prices_link: str

class ResourcesETLTargetConfig(NamedTuple):
    trg_filename_available_tags: str
    trg_filename_usd_ex_rates: str

class ResourcesETL:

    def __init__(self, chrome_conn: ChromeSession,
                 src_conf: ResourcesETLSourceConfig,
                 trg_conf: ResourcesETLTargetConfig,):
        self._logger = logging.getLogger(__name__)
        self._chrome_conn = chrome_conn
        self.src_conf = src_conf
        self.trg_conf = trg_conf

    # external layer processing
    def _get_videogames_tags(self):
        """extracts games' ids from a website with a games id ordered list
            Parameters:
            browser: driver selenium object

            Returns:
            A soup object that belongs to the beautifulsoup4 library """

        # try:
        source_html = self._chrome_conn.get_html_from_url(self.src_conf.src_videogames_list_link)
        soup = bs4.BeautifulSoup(source_html, "html.parser")
        # except Exception as e:
        #     self._logger.debug("Hubo un problema", e)
        #     raise
        # else:
        return soup

    def _get_game_prices(self):
        """extracts a game's cost per country using a sample videogame
            Parameters:
            browser: driver selenium object

            Returns:
            A soup object that belongs to the beautifulsoup4 library"""

        browser = self._chrome_conn.open_session()
        browser.get(self.src_conf.src_videogame_usd_prices_link)
        time.sleep(30+np.random.random()*60)
        # set the referrence prices in usd with cookie
        us_cookie = {
            "name": "__Host-cc",
            "value": "us"
        }
        browser.add_cookie(us_cookie)
        browser.refresh()
        html = browser.page_source
        soup = bs4.BeautifulSoup(html, "html.parser")
        return soup

    def generate_resources(self):
        save(self.trg_conf.trg_filename_available_tags, self.get_available_videogames())
        save(self.trg_conf.trg_filename_usd_ex_rates, self.get_usd_ex_rates())
        return True

    # business layer processing

    def get_available_videogames(self):
        tags_html = self._get_videogames_tags()
        return self._parse_videogames_ids_from_html_tags(tags_html)

    def get_usd_ex_rates(self):
        game_prices_usd_html = self._get_game_prices()
        return self._consolidate_usd_rates_data(game_prices_usd_html)

    def _parse_videogames_ids_from_html_tags(self, soup):
        """parses a soup with videogames id to a list
            Parameters:
            browser: driver selenium object

            Returns:
            List with videogames ids """
        videogame_ids_html_tags = soup.find_all("a", href=lambda href: href and href.startswith("/app/"))
        videogames_ids = [tag["href"].strip(" ").rstrip("/charts").lstrip("/app/") for tag in videogame_ids_html_tags]
        return list(set(videogames_ids))  # removes /charts/app and whitespaces from urls

    def _consolidate_usd_rates_data(self, soup):
        """extracts the raw value and country tags and processes them into clean values

            Parameters:
            soup: soup object that belongs to the beautifulsoup4 library

            Returns:
            A list with the country name in iso code,
            currency name, game's price in local currency
            and game's price in USD"""

        iso_country_code_processed = self._parse_iso_country_code(soup)
        countries_name_processed = self._parse_country_names(soup)
        countries_local_price_processed = self._parse_local_prices(soup)
        usd_prices_processed = self._parse_price_tags(soup)
        countries_prices_processed = list(zip(iso_country_code_processed,
                                              countries_name_processed,
                                              countries_local_price_processed,
                                              usd_prices_processed))
        return countries_prices_processed

    def _parse_country_names(self, soup):
        country_tags_raw = soup.find("div", class_="table-responsive") \
                               .find_all("td", attrs={"data-cc": True})
        return [tag.text.strip("\n").strip(" ") for tag in country_tags_raw]

    def _parse_iso_country_code(self, soup):
        country_tags_raw = soup.find("div", class_="table-responsive") \
                               .find_all("td", attrs={"data-cc": True})
        return [tag["data-cc"].strip("\n").strip(" ") for tag in country_tags_raw]

    def _parse_price_tags(self, soup):
        price_tags_raw = soup.find("div", class_="table-responsive") \
                             .find_all("td", class_="table-prices-converted")
        usd_prices_processed = []
        for tag in price_tags_raw:
            if "%" not in tag.text:
                if tag.text == "N/A":
                    usd_prices_processed.append(np.nan)
                else:
                    usd_prices_processed.append(float(re.findall(r"[0-9]+\.?[0-9]*", tag.text)[0]))

        return usd_prices_processed

    def _parse_local_prices(self, soup):
        countries_row_data_raw = soup.find("div", class_="table-responsive") \
                                     .find_all("tr")
        countries_local_price_processed = []
        for tag in countries_row_data_raw:
            if tag.find_all("td"):
                price_match = re.findall(r"[0-9]+[,\.]?[0-9]*", tag.find_all("td")[1].text)
                if price_match:
                    countries_local_price_processed.append(float(price_match[0].replace(",", ".")))
                else:
                    countries_local_price_processed.append(np.nan)

        return countries_local_price_processed
