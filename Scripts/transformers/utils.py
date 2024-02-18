import json
import time
import numpy as np

from undetected_chromedriver import Chrome, ChromeOptions

class ChromeSession:
    def __init__(self, *chrome_args, **kwargs):

        self._chrome_args = chrome_args
        self._browser_kwargs = kwargs
        self._browser_kwargs.update({"version_main": 121})

    def _build_chrome_options(self):
        options = ChromeOptions()
        for arg in self._chrome_args:
            options.add_argument(arg)  # disables popup blocking for being able to open a new tab
        return options

    def open_session(self):
        return Chrome(self._build_chrome_options(), **self._browser_kwargs)

    def get_html_from_url(self, url: str, wait=30+np.random.random()*60) -> str:
        browser = self.open_session()
        time.sleep(wait)
        browser.get(url)
        return browser.page_source

def save(filename, obj):
    # save the data to prevent having to look for it again
    with open(f"{filename}", "w") as f:
        json.dump(obj, f)
