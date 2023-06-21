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

from __future__ import annotations

import csv
import logging
import time
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from queue import SimpleQueue
from threading import Lock
from typing import Optional, cast

import typer
from rich.logging import RichHandler
from rich.progress import Progress

from smartscoutscrape import SmartScoutSession, __copyright__, __title__, __version__, metadata
from smartscoutscrape.utils import dot_access

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
    filename: Path = Path.cwd().joinpath("smartscout.csv"),
    log_level: str = "WARNING",
) -> None:
    """
    Generate a CSV file of all products and their data on SmartScout.
    """
    if password is None:
        password = typer.prompt("Please enter your password:", hide_input=True)

    log_level_int = cast(int, logging.getLevelName(log_level.upper()))
    logging.basicConfig(level=log_level_int, handlers=[RichHandler()])

    if filename.exists() and "test" not in filename.stem:
        raise FileExistsError(f"File {filename} already exists.")
    elif filename.exists() and "test" in filename.stem:
        logger.warning(f"File {filename} already exists. Overwriting.")

    # fmt: off
    with filename.open("w", newline="", encoding="utf-8") as fp, \
            SmartScoutSession() as session, \
            Progress() as progress, \
            ThreadPoolExecutor() as executor:
        # fmt: on
        writer = csv.writer(fp, dialect="excel")  # no close required

        login_task = progress.add_task("Logging in...", total=None)
        session.login(username, password)
        progress.update(login_task, total=1, completed=1)
        progress.remove_task(login_task)

        category_task = progress.add_task("Getting categories...", total=None)
        categories = session.categories()
        progress.update(category_task, total=1, completed=1)
        progress.remove_task(category_task)

        product_count_task = progress.add_task("Getting product count...", total=None)
        total_products = session.get_total_number_of_products()
        progress.update(product_count_task, total=1, completed=1)
        progress.remove_task(product_count_task)

        database_construction_task = progress.add_task("Writing headers...", total=None)
        fields = session.ALL_FIELDS
        writer.writerow(fields)
        progress.update(database_construction_task, total=1, completed=1)
        logger.info(f"Using {len(fields)} fields.")
        progress.remove_task(database_construction_task)

        product_task = progress.add_task("Downloading products...", total=total_products, start=False)

        # parallelism to improve performance
        seen_products = set()
        product_lock_lock = Lock()
        line_queue = SimpleQueue()
        categories_done = 0

        def _scrape_category(category_id: int) -> None:
            nonlocal categories_done

            try:
                for i, product in enumerate(session.search_products(category_id=category_id)):
                    try:
                        row_data = [dot_access(product, field) for field in fields]

                        asin = row_data[fields.index("asin")]

                        # make sure we don't double-write
                        with product_lock_lock:
                            if asin in seen_products:
                                # this isn't an error because some products are in multiple categories
                                continue

                            seen_products.add(asin)

                        # special handling for images
                        if "imageUrl" in fields:
                            image_url: str | None = row_data[fields.index("imageUrl")]
                            if image_url is not None:
                                row_data[fields.index("imageUrl")] = session.get_b64_image_from_product(
                                    product
                                )

                        # write it and move on
                        line_queue.put(row_data)
                    except Exception as e:
                        logger.warning(f"Could not get product {product}: {e}")
            finally:
                categories_done += 1

        thread_startup_task = progress.add_task("Spawning downloaders...", total=len(categories))
        for category in categories:
            executor.submit(partial(_scrape_category, category["id"]))
            progress.advance(thread_startup_task)
        progress.remove_task(thread_startup_task)

        progress.start_task(product_task)
        while categories_done < len(categories) or not line_queue.empty():
            row_data = line_queue.get()
            writer.writerow(row_data)
            progress.advance(product_task)

        progress.update(product_task, total=len(seen_products), completed=len(seen_products))

        logger.info(f"Done! Downloaded {len(seen_products)} products.")


if __name__ == "__main__":
    cli()
