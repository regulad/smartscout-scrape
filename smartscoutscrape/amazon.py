import os
import time
from abc import ABCMeta
from threading import Lock
from typing import Self

from bs4 import BeautifulSoup
from requests import Session
from requests.adapters import HTTPAdapter
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.webdriver import WebDriver as ChromeDriver
from selenium.webdriver.support.wait import WebDriverWait
from undetected_chromedriver import Chrome as UndetectedChromeDriver
from undetected_chromedriver import ChromeOptions as UndetectedChromeOptions

from smartscoutscrape.utils import THREADING_SAFE_MAX_WORKERS


class AmazonBaseSession(metaclass=ABCMeta):
    def get_asin_html(self, asin: str) -> BeautifulSoup:
        """
        Get the HTML for an ASIN.
        """

        raise NotImplementedError()

    @staticmethod
    def get_url_asin(asin: str) -> str:
        return f"https://www.amazon.com/dp/{asin}"

    @staticmethod
    def get_product_info(soup: BeautifulSoup) -> tuple[str, ...]:
        """
        Get description, about, and aplus info from the soup.
        """

        # column_names.append("Description")
        description_text: str | None = None
        description = soup.find("div", id="productDescription")
        if description is not None:
            description_text = description.text.strip()

        # column_names.append("About this item")
        about_text: str | None = None
        about = soup.find("div", id="feature-bullets")
        if about is not None:
            about_text = about.text.strip()

        # column_names.append("From the manufacturer")
        manufacturer_text: str | None = None
        manufacturer = soup.find("div", id="aplus")
        if manufacturer is not None:
            manufacturer_text = manufacturer.text.strip()

        return description_text, about_text, manufacturer_text

    def __enter__(self) -> Self:
        """
        Enter the context manager.
        """

        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """
        Exit the context manager.
        """

        self.close()

    def close(self) -> None:
        """
        Close the session.
        """
        pass


class AmazonBrowserSession(AmazonBaseSession):
    """
    Scrape data from Amazon using a Chrome driver.
    """

    options: ChromeOptions | UndetectedChromeOptions
    driver: ChromeDriver

    driver_operation_lock: Lock
    wait: WebDriverWait

    def __init__(self, headless: bool = True, proxy: str | None = None, timeout: int = 10) -> None:
        """
        Initialize the session.
        """
        self.driver_operation_lock = Lock()

        self.options = UndetectedChromeOptions()

        if proxy is not None:
            self.options.add_argument(f"--proxy-server={proxy}")

        self.driver = UndetectedChromeDriver(
            options=self.options,
            use_subprocess=False,
            headless=headless,
        )
        self.driver.implicitly_wait(timeout)
        self.wait = WebDriverWait(self.driver, timeout)

        if self._window_handle_count() < 1:
            self.driver.switch_to.new_window("tab")

        # initial get just to secure cookies and whatelse
        # TODO: if we use a proxy we may get IP blocked, do we need to pass a captcha here
        self.driver.get("https://www.amazon.com")

    def _window_handle_count(self) -> int:
        """
        Get the number of open windows.
        """

        return len(self.driver.window_handles)

    def close(self) -> None:
        """
        Close the session.
        """

        pid = self.driver.service.process.pid
        self.driver.quit()

        # wait for pid to die
        while True:
            try:
                os.kill(pid, 0)
            except OSError:
                # pid is dead
                break
            else:
                time.sleep(0.1)

    def get_asin_html(self, asin: str) -> BeautifulSoup:
        """
        Get the HTML for an ASIN.
        """

        with self.driver_operation_lock:
            before_window = self.driver.current_window_handle
            self.driver.switch_to.new_window("tab")
            self.driver.get(self.get_url_asin(asin))
            self.wait.until(lambda _: self.driver.execute_script("return document.readyState") == "complete")
            data = self.driver.page_source
            self.driver.close()
            self.driver.switch_to.window(before_window)

        soup = BeautifulSoup(data, "html.parser")
        return soup


class AmazonScrapeSession(AmazonBaseSession):
    """
    Use requests to scrape data from Amazon.
    While this is more efficient than the browser session, it is also more likely to get you IP blocked.
    """

    req: Session

    def __init__(self, proxy: str | None = None) -> None:
        """
        Initialize the session.
        """
        self.req = Session()

        self.req.headers["Sec-Ch-Device-Memory"] = "8"
        self.req.headers["Sec-Ch-Dpr"] = "1"
        self.req.headers["Sec-Ch-Ua"] = """Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"""
        self.req.headers["Sec-Ch-Ua-Mobile"] = "?0"
        self.req.headers["Sec-Ch-Ua-Platform"] = """"Windows""" ""
        self.req.headers["Sec-Ch-Ua-Platform-Version"] = """"15.0.0""" ""

        self.req.headers[
            "Accept"
        ] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
        self.req.headers["Accept-Encoding"] = "gzip, deflate, br"
        self.req.headers["Accept-Language"] = "en-US,en;q=0.9"
        self.req.headers["Cache-Control"] = "max-age=0"

        self.req.headers["Upgrade-Insecure-Requests"] = "1"
        self.req.headers[
            "User-Agent"
        ] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        self.req.headers["Viewport-Width"] = "1920"

        adapter = HTTPAdapter(
            max_retries=3, pool_connections=THREADING_SAFE_MAX_WORKERS, pool_maxsize=THREADING_SAFE_MAX_WORKERS
        )
        self.req.mount("https://", adapter)
        self.req.mount("http://", adapter)

        if proxy is not None:
            proxies = {"http": proxy, "https": proxy}
            self.req.proxies.update(proxies)

    def get_asin_html(self, asin: str) -> BeautifulSoup:
        with self.req.get(self.get_url_asin(asin)) as resp:
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")


__all__ = ("AmazonBrowserSession", "AmazonBaseSession", "AmazonScrapeSession")
