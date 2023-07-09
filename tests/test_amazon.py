import pytest

from smartscoutscrape import AmazonScrapeSession


@pytest.fixture(scope="session")  # takes FOREVER to login
def amazon_session():
    session = AmazonScrapeSession()
    yield session
    session.close()


class TestAmazon:
    def test_amazon_product(self, amazon_session) -> None:
        PRODUCT_ASIN = "B09ZRNP19N"
        soup = amazon_session.get_asin_html(PRODUCT_ASIN)
        product_info = amazon_session.get_product_info(soup)

        print()
        print(product_info)


__all__ = ("TestAmazon",)

if __name__ == "__main__":
    pytest.main()
