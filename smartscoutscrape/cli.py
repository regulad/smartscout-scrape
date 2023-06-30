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
from typing import Optional, cast

import minify_html
import typer
from rich.logging import RichHandler
from rich.progress import Progress, TextColumn

from smartscoutscrape import AmazonScrapeSession, SmartScoutSession, __copyright__, __title__, __version__, metadata
from smartscoutscrape.utils import THREADING_SAFE_MAX_WORKERS

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
    log_level: str = "WARNING",
) -> None:
    """
    Generate a CSV file of all products and their data on SmartScout.

    Args:
        username: Your SmartScout username.
        password: Your SmartScout password. If not provided, you will be prompted.
        folder: The folder to save the CSV file to.
        log_level: The log level to use. Defaults to WARNING.
    """
    timeout: float = 30.0

    if password is None:
        password = typer.prompt("Please enter your password", hide_input=True)

    log_level_int = cast(int, logging.getLevelName(log_level.upper()))
    logging.basicConfig(level=log_level_int, handlers=[RichHandler()])

    if sqlite3.threadsafety < 2:
        raise RuntimeError("sqlite3 is not threadsafe. Please use a different Python version.")

    # fmt: off
    with SmartScoutSession(proxy=proxy, threads=threads) as smartscout_session, \
            AmazonScrapeSession(proxy=proxy, threads=threads) as amazon_session, \
            sqlite3.Connection(database_path, timeout=timeout) as conn, \
            Progress(
                TextColumn("[bold cyan]{task.completed}/{task.total}[/bold cyan]"),
                *Progress.get_default_columns(),
            ) as progress, \
            ThreadPoolExecutor(thread_name_prefix="CategoryDownloader", max_workers=threads) as category_downloader:
        # fmt: on

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
                conn.execute("INSERT INTO categories VALUES (?, ?);", (category["id"], category["name"]))
                conn.commit()
            except sqlite3.IntegrityError:
                continue
            finally:
                progress.advance(category_task)
        progress.remove_task(category_task)

        # subcategories
        subcategory_task = progress.add_task("Getting subcategories...", total=None)
        subcategories = smartscout_session.subcategories()
        progress.update(subcategory_task, total=len(subcategories))
        conn.execute(
            "CREATE TABLE IF NOT EXISTS subcategories ("
            "   'analyticsSearchable' BOOLEAN NULL,"
            "   'pathById' TEXT NULL,"
            "   'parentId' INTEGER NULL,"
            "   'level' INTEGER NULL,"
            "   'momGrowth' REAL NULL,"
            "   'momGrowth12' REAL NULL,"
            "   'numAsins80PctRev' REAL NULL,"
            "   'rootSubcategoryNodeId' INTEGER NULL,"
            "   id INTEGER PRIMARY KEY, "
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
                    ");",
                    (
                        subcategory["analyticsSearchable"],
                        subcategory["pathById"],
                        subcategory["parentId"],
                        subcategory["level"],
                        subcategory["momGrowth"],
                        subcategory["momGrowth12"],
                        subcategory["numAsins80PctRev"],
                        subcategory["rootSubcategoryNodeId"],
                        subcategory["id"],
                        subcategory["subcategoryName"],
                        subcategory["totalMonthlyRevenue"],
                        subcategory["totalBrands"],
                        subcategory["totalAsins"],
                        subcategory["avgPrice"],
                        subcategory["avgReviews"],
                        subcategory["avgRating"],
                        subcategory["avgNumberSellers"],
                        subcategory["avgPageScore"],
                        subcategory["avgVolume"],
                        subcategory["totalNumberUnitsSold"],
                        subcategory["totalReviews"],
                        subcategory["subcategoryContextName"],
                        subcategory["sellerRevenuePct"],
                        subcategory["azRevenuePct"],
                        subcategory["avgListedSinceDays"],
                        subcategory["ttm"],
                        subcategory["isParent"],
                    ),
                )
                conn.commit()
            except sqlite3.IntegrityError:
                continue
            finally:
                progress.advance(subcategory_task)
        progress.remove_task(subcategory_task)

        product_count_task = progress.add_task("Getting product count...", total=None)
        total_products = smartscout_session.get_total_number_of_products()
        progress.update(product_count_task, total=1, completed=1)
        progress.remove_task(product_count_task)

        product_task = progress.add_task("Downloading products...", total=total_products, start=False)

        # create the table
        # products
        conn.execute(
            "CREATE TABLE IF NOT EXISTS products ("
            "   'brandId' INTEGER NULL,"
            "   'subcategoryId' INTEGER NULL,"
            "   'asin' TEXT PRIMARY KEY,"
            "   'brandName' TEXT NULL,"
            "   'categoryId' INTEGER NULL,"
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
            "   FOREIGN KEY (subcategoryId) REFERENCES subcategories(id),"
            "   FOREIGN KEY (categoryId) REFERENCES categories(id)"
            ");",
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS product_images ("
            "   'asin' TEXT PRIMARY KEY,"
            "   'content_type' TEXT NOT NULL,"
            "   'image' BLOB NOT NULL,"
            "   FOREIGN KEY (asin) REFERENCES products(asin)"
            ");",
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS product_extensions ("
            "   'asin' TEXT PRIMARY KEY,"
            "   'description' TEXT NULL,"
            "   'about' TEXT NULL,"
            "   'aplus' TEXT NULL,"
            "   'html_zlib' BLOB NOT NULL,"  # compressed html with zlib
            "   FOREIGN KEY (asin) REFERENCES products(asin)"
            ");"
        )

        category_task = progress.add_task("Getting categories...", total=len(categories), visible=False)
        categories_done = 0

        def _scrape_category(category_id: int) -> None:
            nonlocal categories_done

            category_dict: dict | None = None
            for category in categories:
                if category["id"] == category_id:
                    category_dict = category
                    break
            if category_dict is None:
                raise ValueError(f"Category ID {category_id} not found.")

            category_name = category_dict["name"]

            log_level_info_or_higher = log_level_int <= logging.INFO  # check to see if safe to print
            category_local_product_task = progress.add_task(
                f"Downloading category {category_name!r}...",
                total=None,
                start=False,
                visible=log_level_info_or_higher,
            )
            number_of_products = smartscout_session.get_number_of_products(category_id=category_id)
            progress.update(category_local_product_task, total=number_of_products)

            # write in the headers for this file
            progress.start_task(category_local_product_task)
            with sqlite3.Connection(database_path, timeout=timeout) as threadlocal_conn:
                try:
                    for product in smartscout_session.search_products(category_id=category_id):
                        try:
                            asin = product["asin"]
                            content_type, image_data = smartscout_session.get_product_image(product)

                            soup = amazon_session.get_asin_html(asin)
                            description, about, aplus = amazon_session.get_product_info(soup)
                            minified_html = minify_html.minify(
                                soup.prettify(encoding="utf-8").decode("utf-8"),
                                minify_js=True,
                                minify_css=True,
                            )
                            html_zlib = zlib.compress(minified_html.encode("utf-8"))

                            try:
                                threadlocal_conn.execute(
                                    "INSERT INTO products VALUES ("
                                    "    :brandId,"
                                    "    :subcategoryId,"
                                    "    :asin,"
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
                                    "    :upc"
                                    ")",
                                    (
                                        product["brandId"],
                                        product["subcategoryId"],
                                        product["asin"],
                                        product["brandName"],
                                        product["categoryId"],
                                        product["subcategory"]["subcategoryName"],
                                        product["rank"],
                                        product["amazonIsr"],
                                        product["numberOfSellers"],
                                        product["isVariation"],
                                        product["monthlyRevenueEstimate"],
                                        product["ttm"],
                                        product["monthlyUnitsSold"],
                                        product["listedSince"],
                                        product["reviewCount"],
                                        product["reviewRating"],
                                        product["numberFbaSellers"],
                                        product["buyBoxPrice"],
                                        product["averageBuyBoxPrice"],
                                        product["buyBoxEquity"],
                                        product["revenueEquity"],
                                        product["marginEquity"],
                                        product["outOfStockNow"],
                                        product["productPageScore"],
                                        product["manufacturer"],
                                        product["upc"],
                                    )
                                )
                                # image
                                threadlocal_conn.execute(
                                    "INSERT INTO product_images VALUES ("
                                    "    :asin,"
                                    "    :content_type,"
                                    "    :image"
                                    ")",
                                    (
                                        asin,
                                        content_type,
                                        image_data,
                                    )
                                )
                                # extension
                                threadlocal_conn.execute(
                                    "INSERT INTO product_extensions VALUES ("
                                    "    :asin,"
                                    "    :description,"
                                    "    :about,"
                                    "    :aplus,"
                                    "    :html_zlib"
                                    ")",
                                    (
                                        asin,
                                        description,
                                        about,
                                        aplus,
                                        html_zlib,
                                    )
                                )
                            except Exception as e:
                                threadlocal_conn.rollback()
                                raise e
                            else:
                                threadlocal_conn.commit()
                            finally:
                                progress.advance(category_local_product_task)
                                progress.advance(product_task)
                        except Exception as e:
                            logger.warning(f"Could not get product {product}: {e}")
                finally:
                    progress.remove_task(category_local_product_task)

                    progress.advance(category_task)
                    categories_done += 1

        for category in categories:
            category_downloader.submit(partial(_scrape_category, category["id"]))

        progress.start_task(category_task)
        progress.start_task(product_task)
        while categories_done < len(categories):
            time.sleep(0.1)

        progress.remove_task(category_task)

        logger.info(f"Done!")


if __name__ == "__main__":
    cli()
