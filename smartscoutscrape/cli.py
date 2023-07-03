"""
Command-line interface for smartscout-scrape.

Copyright 2023 Parker Wahle <regulad@regulad.xyz>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
or implied. See the License for the specific language governing
permissions and limitations under the License.

"""

# fmt: off

from __future__ import annotations

import logging
import sqlite3
import time
import zlib
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from threading import Lock
from typing import Optional, cast

import minify_html
import typer
from requests import HTTPError
from rich.logging import RichHandler
from rich.progress import Progress, TextColumn

from smartscoutscrape import AmazonScrapeSession, SmartScoutSession, __copyright__, __title__, __version__, metadata
from smartscoutscrape.utils import THREADING_SAFE_MAX_WORKERS, top_level_dict

using_python_profiler = False
try:
    import cProfile as profile
except ImportError:
    using_python_profiler = True
    import profile

import pstats

# fmt: on

logger = logging.getLogger(__package__)
cli = typer.Typer()


def info(n_seconds: float = 0.01, verbose: bool = False) -> None:
    """
    Get info about smartscout-scrape.

    Args:
        n_seconds: Number of seconds to wait between processing.
        verbose: Output more info

    Example:
        To call this, run: ::

            from testme import info
            info(0.02)
    """
    typer.echo(f"{__title__} version {__version__}, {__copyright__}")
    if verbose:
        typer.echo(str(metadata.__dict__))
    total = 0
    with typer.progressbar(range(100)) as progress:
        for value in progress:
            time.sleep(n_seconds)
            total += 1
    typer.echo(f"Processed {total} things.")


@cli.command()
def generate(
    username: str,
    password: Optional[str] = None,
    database_path: Path = Path("smartscout.db"),
    threads: int = THREADING_SAFE_MAX_WORKERS,
    proxy: Optional[str] = None,
    dump: bool = True,
    extend: bool = False,  # impossibly slow
    images: bool = True,
    dump_html: bool = False,  # impossibly space-consuming
    timeout: float = 5.0,
    log_level: str = "WARNING",
) -> None:
    """
    Generate a CSV file of all products and their data on SmartScout.

    Args:
        username: Your SmartScout username.
        password: Your SmartScout password. If not provided, you will be prompted.
        database_path: Path to the database file.
        threads: Number of threads to use.
        dump: If products should be dumped. ⚠️ only disable this on your second run or you will experience integrity errors
        proxy: Proxy to use, i.e. socks5://localhost:1055
        extend: Extend the database with extra columns like amazon description & images. WARNING: This is impossibly slow, and is likely to get your IP banned by Amazon.
        images: Fetch images.
        log_level: Log level.
        dump_html: If HTML data should be dumped along with the extension data. Enabling this also enables extend.
        timeout: Amount of time to wait if data does not return instantly
    """
    if dump_html:
        extend = True

    if password is None:
        password = typer.prompt("Please enter your password", hide_input=True)

    log_level_int = cast(int, logging.getLevelName(log_level.upper()))
    log_level_info_or_higher = log_level_int <= logging.INFO  # check to see if safe to print
    log_level_debug_or_higher = log_level_int <= logging.DEBUG  # check to see if safe to print
    logging.basicConfig(level=log_level_int, handlers=[RichHandler()])

    if sqlite3.threadsafety < 2:
        raise RuntimeError("sqlite3 is not threadsafe. Please use a different Python version.")

    startup_lock = Lock()
    con_lock = Lock()

    # fmt: off
    with profile.Profile() as pr, \
            SmartScoutSession(proxy=proxy, threads=threads) as smartscout_session, \
            AmazonScrapeSession(proxy=proxy, threads=threads) as amazon_session, \
            sqlite3.Connection(database_path, timeout=timeout, check_same_thread=False) as conn, \
            Progress(
                TextColumn("[bold cyan]{task.completed}/{task.total}[/bold cyan]"),
                *Progress.get_default_columns(),
            ) as progress, \
            ThreadPoolExecutor(thread_name_prefix="CategoryDownloader", max_workers=threads) as category_downloader:
        # fmt: on

        # calibrate profiler
        if using_python_profiler:
            calibration_task = progress.add_task("Calibrating...", total=5)
            for _ in progress.track(range(5), 5, task_id=calibration_task):
                pr.calibrate(10000)
            progress.remove_task(calibration_task)

        # We do our own SQLite synchronization, so we don't need it to exclusively be on the same thread

        # login
        login_task = progress.add_task("Logging in...", total=None)
        smartscout_session.login(username, password)
        progress.update(login_task, total=1, completed=1)
        progress.remove_task(login_task)

        # categories
        category_task = progress.add_task("Getting categories...", total=None)
        categories = smartscout_session.categories()
        progress.update(category_task, total=len(categories))
        conn.execute("CREATE TABLE IF NOT EXISTS categories (id INTEGER PRIMARY KEY, name TEXT)")
        for category in categories:
            try:
                conn.execute(
                    "INSERT INTO categories VALUES (:id, :name) ON CONFLICT (id) DO UPDATE SET (name) = (:name);",
                    category
                )
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                progress.advance(category_task)
        conn.commit()

        progress.remove_task(category_task)

        # subcategories
        subcategory_task = progress.add_task("Getting subcategories...", total=None)
        subcategories = smartscout_session.subcategories()
        progress.update(subcategory_task, total=len(subcategories))
        conn.execute(
            "CREATE TABLE IF NOT EXISTS subcategories ("
            "   'analyticsSearchable' BOOLEAN NULL,"
            "   'pathById' TEXT NULL,"
            "   'parentId' UNSIGNED BIG INT NULL,"
            "   'level' INTEGER NULL,"
            "   'momGrowth' REAL NULL,"
            "   'momGrowth12' REAL NULL,"
            "   'numAsins80PctRev' REAL NULL,"
            "   'rootSubcategoryNodeId' INTEGER NULL,"
            "   id UNSIGNED BIG INT PRIMARY KEY, "
            "   'subcategoryName' TEXT,"
            "   'totalMonthlyRevenue' REAL NULL,"
            "   'totalBrands' INTEGER NULL,"
            "   'totalAsins' INTEGER NULL,"
            "   'avgPrice' REAL NULL,"
            "   'avgReviews' REAL NULL,"
            "   'avgRating' REAL NULL,"
            "   'avgNumberSellers' REAL NULL,"
            "   'avgPageScore' REAL NULL,"
            "   'avgVolume' REAL NULL,"
            "   'totalNumberUnitsSold' INTEGER NULL,"
            "   'totalReviews' INTEGER NULL,"
            "   'subcategoryContextName' TEXT NULL,"
            "   'sellerRevenuePct' REAL NULL,"
            "   'azRevenuePct' REAL NULL,"
            "   'avgListedSinceDays' REAL NULL,"  # INTEGER
            "   'ttm' REAL NULL,"
            "   'isParent' BOOLEAN NULL"
            ");"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS subcategories_parentId ON subcategories(parentId);")
        for subcategory in subcategories:
            try:
                conn.execute(
                    "INSERT INTO subcategories VALUES ("
                    "   :analyticsSearchable,"
                    "   :pathById,"
                    "   :parentId,"
                    "   :level,"
                    "   :momGrowth,"
                    "   :momGrowth12,"
                    "   :numAsins80PctRev,"
                    "   :rootSubcategoryNodeId,"
                    "   :id,"
                    "   :subcategoryName,"
                    "   :totalMonthlyRevenue,"
                    "   :totalBrands,"
                    "   :totalAsins,"
                    "   :avgPrice,"
                    "   :avgReviews,"
                    "   :avgRating,"
                    "   :avgNumberSellers,"
                    "   :avgPageScore,"
                    "   :avgVolume,"
                    "   :totalNumberUnitsSold,"
                    "   :totalReviews,"
                    "   :subcategoryContextName,"
                    "   :sellerRevenuePct,"
                    "   :azRevenuePct,"
                    "   :avgListedSinceDays,"
                    "   :ttm,"
                    "   :isParent"
                    ") ON CONFLICT (id) DO UPDATE SET ("
                    "   'analyticsSearchable',"
                    "   'pathById',"
                    "   'parentId',"
                    "   'level',"
                    "   'momGrowth',"
                    "   'momGrowth12',"
                    "   'numAsins80PctRev',"
                    "   'rootSubcategoryNodeId',"
                    "   'subcategoryName',"
                    "   'totalMonthlyRevenue',"
                    "   'totalBrands',"
                    "   'totalAsins',"
                    "   'avgPrice',"
                    "   'avgReviews',"
                    "   'avgRating',"
                    "   'avgNumberSellers',"
                    "   'avgPageScore',"
                    "   'avgVolume',"
                    "   'totalNumberUnitsSold',"
                    "   'totalReviews',"
                    "   'subcategoryContextName',"
                    "   'sellerRevenuePct',"
                    "   'azRevenuePct',"
                    "   'avgListedSinceDays',"
                    "   'ttm',"
                    "   'isParent'"
                    ") = ("
                    "   :analyticsSearchable,"
                    "   :pathById,"
                    "   :parentId,"
                    "   :level,"
                    "   :momGrowth,"
                    "   :momGrowth12,"
                    "   :numAsins80PctRev,"
                    "   :rootSubcategoryNodeId,"
                    "   :subcategoryName,"
                    "   :totalMonthlyRevenue,"
                    "   :totalBrands,"
                    "   :totalAsins,"
                    "   :avgPrice,"
                    "   :avgReviews,"
                    "   :avgRating,"
                    "   :avgNumberSellers,"
                    "   :avgPageScore,"
                    "   :avgVolume,"
                    "   :totalNumberUnitsSold,"
                    "   :totalReviews,"
                    "   :subcategoryContextName,"
                    "   :sellerRevenuePct,"
                    "   :azRevenuePct,"
                    "   :avgListedSinceDays,"
                    "   :ttm,"
                    "   :isParent"
                    ");",
                    top_level_dict(subcategory),
                )
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                progress.advance(subcategory_task)
        conn.commit()

        progress.remove_task(subcategory_task)

        product_task = progress.add_task("Downloading products...", total=None, start=False)

        product_count_task = progress.add_task("Getting product count...", total=None, visible=False)
        total_products = smartscout_session.get_total_number_of_products()
        progress.update(product_count_task, total=1, completed=1)
        progress.update(product_task, total=total_products)
        progress.remove_task(product_count_task)
        total_products_seen_so_far = 0

        # create the table
        # products
        conn.execute(
            "CREATE TABLE IF NOT EXISTS products ("
            "   'brandId' UNSIGNED BIG INT NULL,"
            "   'subcategoryId' UNSIGNED BIG INT NULL,"
            "   'imageUrl' TEXT NULL,"
            "   'asin' TEXT PRIMARY KEY,"
            "   'title' TEXT NULL,"
            "   'brandName' TEXT NULL,"
            "   'categoryId' UNSIGNED BIG INT NULL,"
            "   'subcategoryName' TEXT NULL,"
            "   'rank' INTEGER NULL,"
            "   'amazonIsr' REAL NULL,"
            "   'numberOfSellers' INTEGER NULL,"
            "   'isVariation' BOOLEAN NULL,"
            "   'monthlyRevenueEstimate' REAL NULL,"
            "   'ttm' REAL NULL,"
            "   'monthlyUnitsSold' INTEGER NULL,"
            "   'listedSince' TEXT NULL,"
            "   'reviewCount' INTEGER NULL,"
            "   'reviewRating' REAL NULL,"
            "   'numberFbaSellers' INTEGER NULL,"
            "   'buyBoxPrice' REAL NULL,"
            "   'averageBuyBoxPrice' REAL NULL,"
            "   'buyBoxEquity' REAL NULL,"
            "   'revenueEquity' REAL NULL,"
            "   'marginEquity' REAL NULL,"
            "   'outOfStockNow' BOOLEAN NULL,"
            "   'productPageScore' REAL NULL,"
            "   'manufacturer' TEXT NULL,"
            "   'upc' TEXT NULL,"
            "   'partNumber' INTEGER NULL,"
            "   'model' TEXT NULL,"
            "   'numberOfItems' INTEGER NULL,"
            "   'totalRatings' INTEGER NULL,"
            "   'momGrowth' REAL NULL,"
            "   'momGrowth12' REAL NULL,"
            "   'note' TEXT NULL,"
            "   FOREIGN KEY (subcategoryId) REFERENCES subcategories(id),"
            "   FOREIGN KEY (categoryId) REFERENCES categories(id)"
            ");",
        )
        # we do a lot of queries on the categoryId and subcategoryId, so we index them
        conn.execute("CREATE INDEX IF NOT EXISTS products_categoryId ON products(categoryId);")
        conn.execute("CREATE INDEX IF NOT EXISTS products_subcategoryId ON products(subcategoryId);")
        # images
        conn.execute(
            "CREATE TABLE IF NOT EXISTS product_images ("
            "   'asin' TEXT PRIMARY KEY,"
            "   'content_type' TEXT NULL,"
            "   'image' BLOB NULL,"
            "   FOREIGN KEY (asin) REFERENCES products(asin)"
            ");",
        )
        # extensions
        conn.execute(
            "CREATE TABLE IF NOT EXISTS product_extensions ("
            "   'asin' TEXT PRIMARY KEY,"
            "   'description' TEXT NULL,"
            "   'about' TEXT NULL,"
            "   'aplus' TEXT NULL,"
            "   'html_zlib' BLOB NULL,"  # compressed html with zlib
            "   FOREIGN KEY (asin) REFERENCES products(asin)"
            ");"
        )
        conn.commit()

        category_task = progress.add_task("Getting categories...", total=len(categories), visible=False)

        def _scrape_category(category_id: int) -> None:
            nonlocal total_products_seen_so_far

            category_dict: dict | None = None
            for category in categories:
                if category["id"] == category_id:
                    category_dict = category
                    break
            if category_dict is None:
                raise ValueError(f"Category ID {category_id} not found.")

            category_name = category_dict["name"]

            category_local_product_task = progress.add_task(
                f"Downloading category {category_name!r}...",
                total=None,
                start=False,
                visible=log_level_info_or_higher,
            )
            number_of_products = smartscout_session.get_number_of_products(category_id=category_id)
            progress.update(category_local_product_task, total=number_of_products)

            with startup_lock:
                total_products_seen_so_far += number_of_products
                if total_products_seen_so_far > total_products:
                    progress.update(product_task, total=total_products_seen_so_far)

            # write in the headers for this file
            progress.start_task(category_local_product_task)
            try:
                for i, product in enumerate(smartscout_session.search_products(category_id=category_id)):
                    if log_level_debug_or_higher and i > 1:
                        break  # only do 10 products in debug mode
                    try:
                        asin = product["asin"]

                        # image
                        content_type: str | None = None
                        image_data: bytes | None = None

                        image_url: str | None = product["imageUrl"]
                        if images and image_url is not None:
                            try:
                                content_type, image_data = smartscout_session.get_product_image(product)
                            except HTTPError as e:
                                if e.response.status_code == 404 or e.response.status_code == 400:
                                    pass
                                else:
                                    raise e

                        # previously smartscout-scrape extend
                        extend_this_row: bool = extend

                        description: str | None = None
                        about: str | None = None
                        aplus: str | None = None
                        html_zlib: bytes | None = None
                        if extend:
                            try:
                                soup = amazon_session.get_asin_html(asin)
                                description, about, aplus = amazon_session.get_product_info(soup)

                                if dump_html:
                                    beautiful = soup.prettify(encoding="utf-8").decode("utf-8")
                                    minified_html = minify_html.minify(
                                        beautiful,
                                        minify_js=True,
                                        minify_css=True,
                                    )
                                    html_zlib = zlib.compress(minified_html.encode("utf-8"))
                                else:
                                    html_zlib = None
                            except HTTPError as e:
                                if e.response.status_code != 404 and e.response.status_code != 400:
                                    logger.warning(f"Failed to get HTML for ASIN {asin}: {e}")
                                extend_this_row = False
                                pass

                        db_friendly_copy_of_product = product.copy()
                        db_friendly_copy_of_product["subcategoryName"] = product["subcategory"]["subcategoryName"]

                        with con_lock:
                            # There is no way to have this many connections open at once into an SQLite database,
                            # so we have to synchronize access to the database.
                            # It's unfortunate for performance, but it's a neccessary evil.
                            try:
                                if dump:
                                    conn.execute(
                                        "INSERT INTO products VALUES ("
                                        "    :brandId,"
                                        "    :subcategoryId,"
                                        "    :imageUrl,"
                                        "    :asin,"
                                        "    :title,"
                                        "    :brandName,"
                                        "    :categoryId,"
                                        "    :subcategoryName,"
                                        "    :rank,"
                                        "    :amazonIsr,"
                                        "    :numberOfSellers,"
                                        "    :isVariation,"
                                        "    :monthlyRevenueEstimate,"
                                        "    :ttm,"
                                        "    :monthlyUnitsSold,"
                                        "    :listedSince,"
                                        "    :reviewCount,"
                                        "    :reviewRating,"
                                        "    :numberFbaSellers,"
                                        "    :buyBoxPrice,"
                                        "    :averageBuyBoxPrice,"
                                        "    :buyBoxEquity,"
                                        "    :revenueEquity,"
                                        "    :marginEquity,"
                                        "    :outOfStockNow,"
                                        "    :productPageScore,"
                                        "    :manufacturer,"
                                        "    :upc,"
                                        "    :partNumber,"
                                        "    :model,"
                                        "    :numberOfItems,"
                                        "    :totalRatings,"
                                        "    :momGrowth,"
                                        "    :momGrowth12,"
                                        "    :note"
                                        ") ON CONFLICT (asin) DO UPDATE SET ("
                                        "    brandId,"
                                        "    subcategoryId,"
                                        "    imageUrl,"
                                        "    title,"
                                        "    brandName,"
                                        "    categoryId,"
                                        "    subcategoryName,"
                                        "    rank,"
                                        "    amazonIsr,"
                                        "    numberOfSellers,"
                                        "    isVariation,"
                                        "    monthlyRevenueEstimate,"
                                        "    ttm,"
                                        "    monthlyUnitsSold,"
                                        "    listedSince,"
                                        "    reviewCount,"
                                        "    reviewRating,"
                                        "    numberFbaSellers,"
                                        "    buyBoxPrice,"
                                        "    averageBuyBoxPrice,"
                                        "    buyBoxEquity,"
                                        "    revenueEquity,"
                                        "    marginEquity,"
                                        "    outOfStockNow,"
                                        "    productPageScore,"
                                        "    manufacturer,"
                                        "    upc,"
                                        "    partNumber,"
                                        "    model,"
                                        "    numberOfItems,"
                                        "    totalRatings,"
                                        "    momGrowth,"
                                        "    momGrowth12,"
                                        "    note"
                                        ") = ("
                                        "    :brandId,"
                                        "    :subcategoryId,"
                                        "    :imageUrl,"
                                        "    :title,"
                                        "    :brandName,"
                                        "    :categoryId,"
                                        "    :subcategoryName,"
                                        "    :rank,"
                                        "    :amazonIsr,"
                                        "    :numberOfSellers,"
                                        "    :isVariation,"
                                        "    :monthlyRevenueEstimate,"
                                        "    :ttm,"
                                        "    :monthlyUnitsSold,"
                                        "    :listedSince,"
                                        "    :reviewCount,"
                                        "    :reviewRating,"
                                        "    :numberFbaSellers,"
                                        "    :buyBoxPrice,"
                                        "    :averageBuyBoxPrice,"
                                        "    :buyBoxEquity,"
                                        "    :revenueEquity,"
                                        "    :marginEquity,"
                                        "    :outOfStockNow,"
                                        "    :productPageScore,"
                                        "    :manufacturer,"
                                        "    :upc,"
                                        "    :partNumber,"
                                        "    :model,"
                                        "    :numberOfItems,"
                                        "    :totalRatings,"
                                        "    :momGrowth,"
                                        "    :momGrowth12,"
                                        "    :note"
                                        ")",
                                        top_level_dict(db_friendly_copy_of_product)
                                    )
                                # image
                                if images:
                                    conn.execute(
                                        "INSERT INTO product_images VALUES ("
                                        "    :asin,"
                                        "    :content_type,"
                                        "    :image"
                                        ") ON CONFLICT (asin) DO UPDATE SET ("
                                        "   content_type,"
                                        "   image"
                                        ") = ("
                                        "   :content_type,"
                                        "   :image"
                                        ")",
                                        {
                                            "asin": asin,
                                            "content_type": content_type,
                                            "image": image_data,
                                        }
                                    )
                                # extension
                                if extend_this_row:
                                    extension_dict = {
                                        "asin": asin,
                                        "description": description,
                                        "about": about,
                                        "aplus": aplus,
                                        "html_zlib": html_zlib,
                                    }
                                    if not dump_html:
                                        # we aren't dumping HTML this run. Don't touch the HTML if it exists.
                                        conn.execute(
                                            "INSERT INTO product_extensions VALUES ("
                                            "    :asin,"
                                            "    :description,"
                                            "    :about,"
                                            "    :aplus,"
                                            "    :html_zlib"
                                            ") ON CONFLICT (asin) DO UPDATE SET ("
                                            "   description,"
                                            "   about,"
                                            "   aplus"
                                            ") = ("
                                            "   :description,"
                                            "   :about,"
                                            "   :aplus"
                                            ")",
                                            extension_dict
                                        )
                                    else:
                                        conn.execute(
                                            "INSERT INTO product_extensions VALUES ("
                                            "    :asin,"
                                            "    :description,"
                                            "    :about,"
                                            "    :aplus,"
                                            "    :html_zlib"
                                            ") ON CONFLICT (asin) DO UPDATE SET ("
                                            "   description,"
                                            "   about,"
                                            "   aplus,"
                                            "   html_zlib"
                                            ") = ("
                                            "   :description,"
                                            "   :about,"
                                            "   :aplus,"
                                            "   :html_zlib"
                                            ")",
                                            extension_dict
                                        )
                            except Exception as e:
                                conn.rollback()
                                raise e
                            else:
                                conn.commit()
                            finally:
                                progress.advance(category_local_product_task)
                                progress.advance(product_task)
                    except Exception as e:
                        logger.warning(f"Could not get product {product}: {e}")
            finally:
                progress.remove_task(category_local_product_task)
                progress.advance(category_task)

        for category in categories:
            category_downloader.submit(partial(_scrape_category, category["id"]))

        progress.start_task(category_task)
        progress.start_task(product_task)
        try:
            category_downloader.shutdown(wait=True)
        finally:
            if log_level_debug_or_higher:
                pr.create_stats()
                stats = pstats.Stats(pr)
                stats.sort_stats(pstats.SortKey.CUMULATIVE)
                stats.print_stats(.05)
        progress.remove_task(category_task)

        logger.info(f"Done!")


if __name__ == "__main__":
    cli()
