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
from pathlib import Path
from threading import Lock
from typing import Optional, cast

import typer
from rich.logging import RichHandler
from rich.progress import Progress, TextColumn

from smartscoutscrape import (AmazonScrapeSession, SmartScoutSession, SmartScoutSQLiteHelper, __copyright__, __title__,
                              __version__, metadata)
from smartscoutscrape.utils import THREADING_SAFE_MAX_WORKERS

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
            ) as progress:
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

        setup_task = progress.add_task("Setting up...", total=None)
        with SmartScoutSQLiteHelper(
            database_connection=conn,
            amazon_session=amazon_session,
            smartscout_session=smartscout_session
        ) as helper:
            progress.remove_task(setup_task)
            # Download all products
            download_task = progress.add_task("Downloading products...", total=None, start=False)
            progress.update(download_task, total=smartscout_session.get_total_number_of_products())
            try:
                for _ in helper.dump_all(yield_rows=True, dump_default=dump, dump_html=dump_html, extend=extend, images=images):
                    progress.advance(download_task)
            finally:
                if log_level_debug_or_higher:
                    pr.create_stats()
                    stats = pstats.Stats(pr)
                    stats.sort_stats(pstats.SortKey.CUMULATIVE)
                    stats.print_stats(.05)
            progress.remove_task(download_task)

        logger.info(f"Done!")


if __name__ == "__main__":
    cli()
