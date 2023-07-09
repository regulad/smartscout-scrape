"""
A set of utilities for working with smartscout data in pandas.
"""
import sqlite3
import tempfile
from os import PathLike
from pathlib import Path
from sqlite3 import Connection
from typing import Generator

import pandas as pd
from tqdm import tqdm_notebook

from .amazon import AmazonScrapeSession
from .smartscout import SmartScoutSession
from .sqlite import SmartScoutSQLiteHelper


class SmartScoutPandas:
    """
    A wrapper that helps you get SmartScout data into pandas dataframes using SQLite in-memory databases.
    """

    TRAINING_THRESHOLD = 50000  # should be more than enough to train a model

    _dumped_categories: set[int]
    conn: Connection
    amazon_session: AmazonScrapeSession
    smartscout_session: SmartScoutSession
    helper: SmartScoutSQLiteHelper

    def __init__(self, database: str | PathLike[str]) -> None:
        self._dumped_categories = set()

        self.conn = sqlite3.connect(database, check_same_thread=False)
        self.amazon_session = AmazonScrapeSession()
        self.smartscout_session = SmartScoutSession()
        self.smartscout_session.login()
        self.helper = SmartScoutSQLiteHelper(
            database_connection=self.conn,
            amazon_session=self.amazon_session,
            smartscout_session=self.smartscout_session,
        )
        self.helper.setup()

        for known_category in self.conn.execute("SELECT DISTINCT categoryId FROM products"):
            self._dumped_categories.add(known_category[0])

    @classmethod
    def in_memory(cls) -> "SmartScoutPandas":
        """
        Create an in-memory SmartScoutPandas instance.
        This is the fastest option, but data will not persist after the instance is closed.
        Additionally, you may need to wait a while for data to download.
        """
        return cls(":memory:")

    @classmethod
    def from_temp_dir(cls) -> "SmartScoutPandas":
        """
        Create a temporary SmartScoutPandas instance.
         This offers more persistent storage than :memory:, good for development.
        """
        return cls(Path(tempfile.gettempdir()).joinpath("smartscout.db"))

    def close(self) -> None:
        self.helper.close()
        self.smartscout_session.close()
        self.amazon_session.close()
        self.conn.close()

    def _is_parent(self, subcategory_id: int) -> bool:
        return (
            self.conn.execute("SELECT isParent FROM subcategories WHERE ABS(id) = ?", (subcategory_id,)).fetchone()[0]
            or 0
        ) > 0

    def _get_children(self, subcategory_id: int) -> Generator[int, None, None]:
        for row in self.conn.execute("SELECT ABS(id) FROM subcategories WHERE parentId = ?", (subcategory_id,)):
            child_id = row[0]
            if self._is_parent(child_id) and child_id != subcategory_id:
                yield from self._get_children(child_id)
            if child_id != subcategory_id:
                yield child_id
        yield subcategory_id

    def get_df_of_subcategory(self, subcategory_id: int) -> pd.DataFrame:
        parents_youngest_to_oldest = self.smartscout_session.find_category_parent(subcategory_id)
        parents_oldest_to_youngest = reversed(list(parents_youngest_to_oldest))
        oldest_subcategory_id = next(parents_oldest_to_youngest)["id"]

        category_id: int = self.conn.execute(
            """
            SELECT categories.id
            FROM subcategories
            JOIN categories ON categories.name == subcategories.subcategoryName
            WHERE subcategories.id == ?
            LIMIT 1;
        """,
            (oldest_subcategory_id,),
        ).fetchone()[0]

        if category_id not in self._dumped_categories:
            item_count = self.smartscout_session.get_number_of_products(category_id)
            row_gen = self.helper.dump_category(category_id, yield_rows=True)
            for _ in tqdm_notebook(row_gen, total=item_count, desc="Dumping SmartScout data"):
                pass  # we don't care that much, and by side effect they are added to the database
            self._dumped_categories.add(category_id)

        subcategory_ids = f"({', '.join(map(str, self._get_children(subcategory_id)))})"

        return pd.read_sql_query(
            f"""
            SELECT
                --index
                products.asin,
                --independent variables
                product_images.image, --only jpeg, png, and gif; so PIL can identify it without the content-type
                products.title,
                --product_extensions.description,
                --product_extensions.about,
                --product_extensions.aplus,
                products.manufacturer,
                products.brandName,
                products.averageBuyBoxPrice,
                products.numberOfItems,
                products.numberOfSellers,
                --dependent variables
                products.monthlyRevenueEstimate,
                products.monthlyUnitsSold,
                products.reviewRating,
                products.reviewCount
            FROM products
            JOIN product_images ON products.asin == product_images.asin
            --JOIN product_extensions ON products.asin == product_extensions.asin --not currently using this data nor is it collected
            WHERE
                subcategoryId IN {subcategory_ids} --no injection risk because subcategory_ids is a tuple of ints and not a string
                --nulls
                AND image IS NOT NULL
                AND title IS NOT NULL
                AND manufacturer IS NOT NULL
                AND brandName IS NOT NULL
                AND reviewRating IS NOT NULL
                --outliers
                AND monthlyRevenueEstimate > 0
                AND monthlyUnitsSold > 0
                AND NOT outOfStockNow
                AND DATE(listedSince) < DATE('now', '-1 month') --make sure item has been listed long enough to have accurate data about it
            --randomization needs to be done before limit so that it will be representative of the whole dataset and not the first n subcategories that get sorted to the top of the sqlite index
            ORDER BY RANDOM() --will be better for the model to not have the same subcategory in a row which would reduce the opportunity that both the model would have to overfit a category and that the testing data wouldn't be representative of the training data
            LIMIT {self.TRAINING_THRESHOLD}; --do not waste memory past the point of diminishing returns
            """,
            self.conn,
            index_col="asin",
        )


__all__ = ("SmartScoutPandas",)
