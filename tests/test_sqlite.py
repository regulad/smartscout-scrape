import logging
import sqlite3
import time
from sqlite3 import Connection

import pytest

from smartscoutscrape import AmazonScrapeSession, SmartScoutSession, SmartScoutSQLiteHelper


class TestSQLiteHelper:
    APPLIANCES = 21

    @pytest.fixture(scope="session")  # we need this for persistence across tests
    def sqlite(self) -> Connection:
        conn = sqlite3.connect(":memory:", check_same_thread=False)
        yield conn
        conn.close()

    @pytest.fixture(scope="session")  # only because setup takes FOREVER
    def helper(
        self, sqlite: Connection, smartscout_session: SmartScoutSession, amazon_session: AmazonScrapeSession
    ) -> SmartScoutSQLiteHelper:
        helper = SmartScoutSQLiteHelper(
            database_connection=sqlite,
            smartscout_session=smartscout_session,
            amazon_session=amazon_session,
        )
        helper.setup()
        yield helper
        helper.close()

    def test_created_tables(self, helper: SmartScoutSQLiteHelper, sqlite: Connection) -> None:
        cursor = sqlite.cursor()

        try:
            category_count = cursor.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
            assert category_count > 0
            subcategory_count = cursor.execute("SELECT COUNT(*) FROM subcategories").fetchone()[0]
            assert subcategory_count > 0
        finally:
            cursor.close()

    def test_dump_products_from_category(
        self, helper: SmartScoutSQLiteHelper, sqlite: Connection, smartscout_session: SmartScoutSession
    ) -> None:
        appliance_count = smartscout_session.get_number_of_products(self.APPLIANCES)
        before_time = time.time()
        for i, product in enumerate(helper.dump_category(self.APPLIANCES, yield_rows=True)):
            if i > 1000:
                break
            assert product is not None
            logging.info(f"Got product {product['asin']}, {i + 1}/{appliance_count}")
        after_time = time.time()
        time_taken = after_time - before_time

        cursor = sqlite.cursor()

        product_count = cursor.execute("SELECT COUNT(*) FROM products").fetchone()[0]

        estimated_time_entire_category = time_taken * (appliance_count / product_count)

        logging.info(
            f"Dumped 100 products in {time_taken} seconds, "
            f"est. {estimated_time_entire_category} for {appliance_count} products"
        )

    def test_dump_all_products(
        self, helper: SmartScoutSQLiteHelper, sqlite: Connection, smartscout_session: SmartScoutSession
    ) -> None:
        total_product_count = smartscout_session.get_total_number_of_products()
        before_time = time.time()
        for i, product in enumerate(helper.dump_category(self.APPLIANCES, yield_rows=True)):
            if i > 1000:
                break
            assert product is not None
            logging.info(f"Got product {product['asin']}, {i + 1}/{total_product_count}")
        after_time = time.time()
        time_taken = after_time - before_time

        cursor = sqlite.cursor()

        product_count = cursor.execute("SELECT COUNT(*) FROM products").fetchone()[0]

        estimated_time_entire_category = time_taken * (total_product_count / product_count)

        logging.info(
            f"Dumped 100 products in {time_taken} seconds, "
            f"est. {estimated_time_entire_category} for {total_product_count} products"
        )


__all__ = ("TestSQLiteHelper",)
