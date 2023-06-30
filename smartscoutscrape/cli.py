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

import csv
import logging
import math
import time
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from queue import Empty, Queue
from threading import Event, Lock, Semaphore, Thread, current_thread
from typing import Optional, cast

import minify_html
import typer
from rich.logging import RichHandler
from rich.progress import Progress

from smartscoutscrape import (AmazonBaseSession, AmazonBrowserSession, AmazonScrapeSession, SmartScoutSession,
                              __copyright__, __title__, __version__, metadata)
from smartscoutscrape.utils import THREADING_SAFE_MAX_WORKERS, dot_access

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
    folder: Path = Path.cwd().joinpath("smartscout"),
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
    if password is None:
        password = typer.prompt("Please enter your password:", hide_input=True)

    log_level_int = cast(int, logging.getLevelName(log_level.upper()))
    logging.basicConfig(level=log_level_int, handlers=[RichHandler()])

    if folder.exists() and "test" not in folder.stem:
        raise FileExistsError(f"File {folder} already exists.")
    elif folder.exists() and "test" in folder.stem:
        logger.warning(f"File {folder} already exists. Recreating.")

        # clean out the folder to remove it
        for file in folder.iterdir():
            file.unlink()
        folder.rmdir()

    folder.mkdir(parents=True, exist_ok=False)

    # fmt: off
    with SmartScoutSession() as session, \
        Progress() as progress, \
        ThreadPoolExecutor(thread_name_prefix="CategoryDownloader") as executor:
        # fmt: on

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

        product_task = progress.add_task("Downloading products...", total=total_products, start=False)

        # parallelism to improve performance
        seen_products = set()
        product_lock_lock = Lock()
        product_semaphore = Semaphore(THREADING_SAFE_MAX_WORKERS)

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
            windows_friendly_filename = category_name.replace("/", "-").replace("\\", "-").replace("&", "and")
            friendly_filename = f"{category_id}-{windows_friendly_filename}.csv"

            log_level_info_or_higher = log_level_int <= logging.INFO  # check to see if safe to print
            local_task = progress.add_task(
                f"Downloading category {category_name!r}...",
                total=None,
                start=False,
                visible=log_level_info_or_higher,
            )
            number_of_products = session.get_number_of_products(category_id=category_id)
            progress.update(local_task, total=number_of_products)

            # fmt: off
            with open(folder.joinpath(friendly_filename), "w", newline="", encoding="utf-8") as fp, \
                product_semaphore:
                # fmt: on
                writer = csv.writer(fp, dialect="excel")  # no close required

                # write in the headers for this file
                fields = session.ALL_FIELDS
                writer.writerow(fields)

                line_queue = Queue(maxsize=1000)

                category_done = Event()

                # we spawn the disk write IO in a separate thread to increase performance
                def _sister_runnable() -> None:
                    nonlocal category_done, line_queue

                    while not category_done.is_set() or not line_queue.empty():
                        try:
                            row_data = line_queue.get(timeout=1)
                        except (Empty, TimeoutError):
                            continue
                        try:
                            writer.writerow(row_data)
                            progress.advance(product_task)
                            progress.advance(local_task)
                        finally:
                            line_queue.task_done()

                current_thread_name = current_thread().name  # CategoryDownloader_30  or whatever

                sister_thread = Thread(target=_sister_runnable, name=f"{current_thread_name}_io_sister")

                sister_thread.start()
                progress.start_task(local_task)
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
                    category_done.set()
                    sister_thread.join()
                    line_queue.join()
                    progress.remove_task(local_task)

                    progress.advance(category_task)
                    categories_done += 1

        thread_startup_task = progress.add_task("Spawning downloaders...", total=len(categories))
        for category in categories:
            executor.submit(partial(_scrape_category, category["id"]))
            progress.advance(thread_startup_task)
        progress.remove_task(thread_startup_task)

        progress.start_task(category_task)
        progress.start_task(product_task)
        while categories_done < len(categories):
            time.sleep(0.1)

        progress.update(product_task, total=len(seen_products), completed=len(seen_products))

        logger.info(f"Done! Downloaded {len(seen_products)} products.")


@cli.command()
def extend(
    in_folder: Path = Path.cwd().joinpath("smartscout"),
    out_folder: Path = Path.cwd().joinpath("smartscout-extended"),
    use_browser: bool = False,
    proxy: Optional[str] = None,
    threads: Optional[int] = None,
    log_level: str = "WARNING",
    dump_html: bool = False,
) -> None:
    """
    Extend an existing SmartScout dump with additional fields like descriptions and raw HTML.
    """
    max_workers = threads or THREADING_SAFE_MAX_WORKERS

    log_level_int = cast(int, logging.getLevelName(log_level.upper()))
    logging.basicConfig(level=log_level_int, handlers=[RichHandler()])

    log_level_info_or_higher = log_level_int <= logging.INFO  # check to see if safe to print

    if not in_folder.exists():
        raise ValueError(f"Folder {in_folder} does not exist.")

    if not out_folder.exists():
        out_folder.mkdir(parents=True)
    else:
        logger.warning(f"Folder {out_folder} already exists. Continuing anyway.")

    if not in_folder.is_dir():
        raise ValueError(f"Folder {in_folder} is not a directory.")

    if not in_folder.is_absolute():
        in_folder = in_folder.resolve()

    with Progress() as progress, ThreadPoolExecutor(max_workers=max_workers) as executor:
        startup_task = progress.add_task("Starting up...", total=None)

        session: AmazonBaseSession
        if use_browser:
            session = AmazonBrowserSession(proxy=proxy)
        else:
            session = AmazonScrapeSession(proxy=proxy, threads=threads)

        progress.update(startup_task, total=1, completed=1)
        progress.remove_task(startup_task)

        with session:
            globs: list[Path] = list(in_folder.glob("*.csv"))
            total_line_estimate = 0

            row_extension_task = progress.add_task("Extending rows...", total=None, start=False)
            category_extension_task = progress.add_task(
                "Extending categories...", total=len(globs), start=False, visible=log_level_info_or_higher
            )

            # estimate lines
            for file in globs:
                with file.open("r", newline="", encoding="utf-8") as source_fp:
                    # lets get an estimate of how many lines we have to process
                    total_length_in_bytes = file.stat().st_size
                    source_fp.readline()  # skip the header
                    line1 = source_fp.readline()
                    line2 = source_fp.readline()
                    line1_length_in_bytes = len(line1.encode("utf-8"))
                    line2_length_in_bytes = len(line2.encode("utf-8"))
                    avg_line_length_in_bytes = (line1_length_in_bytes + line2_length_in_bytes) / 2
                    if avg_line_length_in_bytes < 1:
                        # this is a stub
                        continue
                    file_line_estimate = math.ceil(total_length_in_bytes / avg_line_length_in_bytes)

                    total_line_estimate += file_line_estimate

                    # go back to the start for the next file
                    source_fp.seek(0)
            progress.update(row_extension_task, total=total_line_estimate, completed=0)

            for file in globs:
                # this is not io bound so we do not need a sister thread
                def _scrape_one(file: Path) -> None:
                    single_file_task = progress.add_task(
                        f"Extending {file.name!r}...",
                        total=None,
                        start=False,
                        visible=log_level_info_or_higher,
                    )
                    try:
                        if "extended" in file.stem:
                            # we already went to work on this file
                            return

                        # fmt: off
                        with file.open("r", newline="", encoding="utf-8", buffering=1, errors="replace") as source_fp, \
                                (
                                    out_folder
                                    .joinpath(f"{file.stem}-extended.csv")
                                    .open(
                                        "w",
                                        newline="",
                                        encoding="utf-8",
                                        buffering=1,
                                        errors="replace"
                                    )
                                ) as dest_fp:
                            # fmt: on

                            # lets get an estimate of how many lines we have to process
                            total_length_in_bytes = file.stat().st_size

                            source_fp.readline()  # skip the header
                            line1 = source_fp.readline()
                            line2 = source_fp.readline()
                            line1_length_in_bytes = len(line1.encode("utf-8"))
                            line2_length_in_bytes = len(line2.encode("utf-8"))
                            avg_line_length_in_bytes = (line1_length_in_bytes + line2_length_in_bytes) / 2
                            if avg_line_length_in_bytes < 1:
                                # this is a stub
                                progress.update(single_file_task, total=1, completed=1)
                                return
                            total_lines_estimate = math.ceil(total_length_in_bytes / avg_line_length_in_bytes)
                            progress.update(single_file_task, total=total_lines_estimate, completed=0)
                            # go back to the start
                            source_fp.seek(0)

                            reader = csv.reader(source_fp, dialect="excel")
                            writer = csv.writer(dest_fp, dialect="excel")

                            parent_headers = next(reader)
                            dest_headers = ["asin", "description", "about", "aplus"]

                            if dump_html:
                                dest_headers.append("html")

                            writer.writerow(dest_headers)

                            progress.start_task(single_file_task)
                            for data_row in reader:
                                try:
                                    asin = data_row[parent_headers.index("asin")]
                                    soup = session.get_asin_html(asin)

                                    description, about, aplus = session.get_product_info(soup)

                                    data = [asin, description, about, aplus]

                                    if dump_html:
                                        html = minify_html.minify(
                                            soup.prettify(),
                                            minify_js=True,
                                            remove_processing_instructions=True
                                        )
                                        data.append(html)

                                    writer.writerow(data)
                                except Exception as e:
                                    logger.warning(f"Could not extend row in {file}: {e}")
                                    continue
                                finally:
                                    progress.advance(row_extension_task)
                                    progress.advance(single_file_task)
                        progress.update(single_file_task, total=1, completed=1)
                    except Exception as e:
                        logger.warning(f"Could not extend {file}: {e}")
                    finally:
                        progress.advance(category_extension_task)
                        progress.remove_task(single_file_task)

                executor.submit(partial(_scrape_one, file))

            progress.start_task(row_extension_task)
            progress.start_task(category_extension_task)
            executor.shutdown(wait=True)
            progress.remove_task(category_extension_task)
            progress.remove_task(row_extension_task)

            logger.info(f"Done! Extended {len(globs)} files.")


if __name__ == "__main__":
    cli()
