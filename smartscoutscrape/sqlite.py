import logging
import warnings
import zlib
from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed
from queue import Empty, Queue
from sqlite3 import Connection
from threading import Event, Lock, Thread
from typing import Generator, Self

import minify_html
from requests import HTTPError

from .amazon import AmazonBaseSession
from .smartscout import SmartScoutSession
from .utils import top_level_dict

logger = logging.getLogger(__package__)


class SmartScoutSQLiteHelper:
    """Helps dump data from SmartScout & Amazon into a SQLite database."""

    # initialization arguments
    database_connection: Connection
    _owns_database_connection: bool

    smartscout_session: SmartScoutSession
    _owns_smartscout_session: bool

    amazon_session: AmazonBaseSession
    _owns_amazon_session: bool

    # variables
    connection_lock: Lock
    closed: bool

    # class vars (cache)
    _total_products_cache: int | None = None

    def __init__(
        self,
        *,
        database_connection: Connection,
        owns_connection: bool = False,
        smartscout_session: SmartScoutSession,
        owns_smartscout_session: bool = False,
        amazon_session: AmazonBaseSession,
        owns_amazon_session: bool = False,
    ) -> None:
        """Initialize the helper. The setup function must be called prior to using the helper."""
        self.database_connection = database_connection
        self._owns_database_connection = owns_connection
        self.smartscout_session = smartscout_session
        self._owns_smartscout_session = owns_smartscout_session
        self.amazon_session = amazon_session
        self._owns_amazon_session = owns_amazon_session

        self.connection_lock = Lock()
        self.closed = False

    def close(self) -> None:
        """Close the helper."""
        if self._owns_database_connection:
            self.database_connection.close()
        if self._owns_smartscout_session:
            self.smartscout_session.close()
        if self._owns_amazon_session:
            self.amazon_session.close()
        self.closed = True

    def __del__(self) -> None:
        """Close the helper."""
        if not self.closed:
            warnings.warn(
                "Unclosed SmartScoutSQLiteHelper. Call close() on the helper to close it.",
                ResourceWarning,
                stacklevel=2,
            )
            self.close()

    def __enter__(self) -> Self:
        """Enter the context manager."""
        self.setup()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Exit the context manager."""
        self.close()

    def setup(self) -> None:
        # categories
        categories = self.smartscout_session.categories()
        self.database_connection.execute("CREATE TABLE IF NOT EXISTS categories (id INTEGER PRIMARY KEY, name TEXT)")
        for category in categories:
            try:
                self.database_connection.execute(
                    "INSERT INTO categories VALUES (:id, :name) ON CONFLICT (id) DO UPDATE SET (name) = (:name);",
                    category,
                )
            except Exception as e:
                self.database_connection.rollback()
                raise e
        self.database_connection.commit()

        # subcategories
        subcategories = self.smartscout_session.subcategories()
        self.database_connection.execute(
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
        self.database_connection.execute(
            "CREATE INDEX IF NOT EXISTS subcategories_parentId ON subcategories(parentId);"
        )
        for subcategory in subcategories:
            try:
                self.database_connection.execute(
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
                self.database_connection.rollback()
                raise e
        self.database_connection.commit()

        self._total_products_cache = self.smartscout_session.get_total_number_of_products()

        # create the table
        # products
        self.database_connection.execute(
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
        self.database_connection.execute("CREATE INDEX IF NOT EXISTS products_categoryId ON products(categoryId);")
        self.database_connection.execute(
            "CREATE INDEX IF NOT EXISTS products_subcategoryId ON products(subcategoryId);"
        )
        # images
        self.database_connection.execute(
            "CREATE TABLE IF NOT EXISTS product_images ("
            "   'asin' TEXT PRIMARY KEY,"
            "   'content_type' TEXT NULL,"
            "   'image' BLOB NULL,"
            "   FOREIGN KEY (asin) REFERENCES products(asin)"
            ");",
        )
        # extensions
        self.database_connection.execute(
            "CREATE TABLE IF NOT EXISTS product_extensions ("
            "   'asin' TEXT PRIMARY KEY,"
            "   'description' TEXT NULL,"
            "   'about' TEXT NULL,"
            "   'aplus' TEXT NULL,"
            "   'html_zlib' BLOB NULL,"  # compressed html with zlib
            "   FOREIGN KEY (asin) REFERENCES products(asin)"
            ");"
        )
        self.database_connection.commit()

    @property
    def total_products(self) -> int:
        """Returns the total number of products on Amazon."""

        return self._total_products_cache

    def _write_smartscout_product(
        self,
        product: dict,
        dump_default: bool = True,
        extend: bool = False,
        dump_html: bool = False,
        images: bool = True,
    ) -> dict:
        """Writes a product to the database and returns the product after it has been processed."""

        asin = product["asin"]

        # image
        content_type: str | None = None
        image_data: bytes | None = None

        image_url: str | None = product["imageUrl"]
        if images and image_url is not None:
            try:
                content_type, image_data = self.smartscout_session.get_product_image(product)
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
                soup = self.amazon_session.get_asin_html(asin)
                description, about, aplus = self.amazon_session.get_product_info(soup)

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

        db_friendly_copy_of_product = top_level_dict(product).copy()
        db_friendly_copy_of_product["subcategoryName"] = product["subcategory"]["subcategoryName"]

        with self.connection_lock:
            # There is no way to have this many connections open at once into an SQLite database,
            # so we have to synchronize access to the database.
            # It's unfortunate for performance, but it's a neccessary evil.
            try:
                if dump_default:
                    self.database_connection.execute(
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
                        db_friendly_copy_of_product,
                    )
                # image
                if images:
                    self.database_connection.execute(
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
                        },
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
                        self.database_connection.execute(
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
                            extension_dict,
                        )
                    else:
                        self.database_connection.execute(
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
                            extension_dict,
                        )
            except Exception as e:
                self.database_connection.rollback()
                raise e
            else:
                self.database_connection.commit()
                return db_friendly_copy_of_product

    def dump_all(
        self,
        *,
        yield_rows: bool = True,
        dump_default: bool = True,
        extend: bool = False,
        dump_html: bool = False,
        images: bool = True,
    ) -> Generator[dict[str, str | int | float | None], None, None]:
        with ThreadPoolExecutor(thread_name_prefix="SmartScoutDumper") as executor:
            finished_rows = Queue(maxsize=1000)
            gen_exit_event = Event()

            def _process_category(category_id: int) -> None:
                for product in self.dump_category(
                    category_id=category_id,
                    yield_rows=True,
                    dump_default=dump_default,
                    extend=extend,
                    dump_html=dump_html,
                    images=images,
                ):
                    if gen_exit_event.is_set():
                        break
                    finished_rows.put(product)

            def _run_spawner():
                spawned = list()

                for category in self.smartscout_session.categories():
                    if gen_exit_event.is_set():
                        break
                    spawned.append(executor.submit(_process_category, category["id"]))

                for future in as_completed(spawned):
                    if gen_exit_event.is_set():
                        break
                    future.result()

            spawner = Thread(target=_run_spawner)
            spawner.start()

            while not (finished_rows.empty() and not spawner.is_alive()):
                try:
                    row = finished_rows.get(timeout=1)
                except Empty:
                    continue
                if yield_rows:
                    try:
                        yield row
                    except GeneratorExit:
                        gen_exit_event.set()
                        executor.shutdown(wait=True, cancel_futures=True)
                        break
                finished_rows.task_done()  # don't hold it up, let it continue

            executor.shutdown(wait=True)  # wait for all the threads to finish writing/fetching

    def dump_category(
        self,
        category_id: int,
        *,
        yield_rows: bool = True,
        dump_default: bool = True,
        extend: bool = False,
        dump_html: bool = False,
        images: bool = True,
    ) -> Generator[dict[str, str | int | float | None], None, None]:
        """
        Scrape a category and write it to the database.

        Args:
            category_id (int): The category ID to scrape.
            yield_rows (bool): Whether to yield rows or not.
            dump_default (bool, optional): Whether to dump the default data. Defaults to True.
            extend (bool, optional): Whether to extend the data. Defaults to True.
            dump_html (bool, optional): Whether to dump the HTML. Defaults to False.
            images (bool, optional): Whether to dump the images. Defaults to True.
        """

        categories = self.smartscout_session.categories()

        category_dict: dict | None = None
        for category in categories:
            if category["id"] == category_id:
                category_dict = category
                break
        if category_dict is None:
            raise ValueError(f"Category ID {category_id} not found.")

        # write in the headers for this file
        with ThreadPoolExecutor(thread_name_prefix=f"Category{category_id}Writer") as writer:
            finished_rows = Queue(maxsize=1000)
            gen_exit_event = Event()

            def _process_product(product: dict) -> dict:
                processed_product = self._write_smartscout_product(product, dump_default, extend, dump_html, images)
                finished_rows.put(processed_product)
                return processed_product

            def _run_spawner():
                spawned = list()

                for i, product in enumerate(self.smartscout_session.search_products(category_id=category_id)):
                    if gen_exit_event.is_set():
                        break
                    try:
                        spawned.append(writer.submit(_process_product, product))
                    except Exception as e:
                        logger.warning(f"Could not get product {product}: {e}")

                for future in as_completed(spawned):
                    if gen_exit_event.is_set():
                        break
                    future.result()

            spawner = Thread(target=_run_spawner)
            spawner.start()

            try:
                while not (finished_rows.empty() and not spawner.is_alive()):
                    # Fun fact: the executor will hold ITSELF up if we don't get the results out of the queue,
                    # so we have to do this to help it self-regulate. Besides that, it's just good practice.
                    try:
                        row = finished_rows.get(timeout=1)
                    except Empty:
                        continue
                    if yield_rows:
                        try:
                            yield row
                        except GeneratorExit:
                            gen_exit_event.set()
                            writer.shutdown(wait=True, cancel_futures=True)
                            break
                    finished_rows.task_done()  # don't hold it up, let it continue
            except CancelledError:
                gen_exit_event.set()
                writer.shutdown(wait=True, cancel_futures=True)
                raise

            writer.shutdown(wait=True)  # wait for all the threads to finish writing/fetching


__all__ = ("SmartScoutSQLiteHelper",)
