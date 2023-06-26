import pytest

from smartscoutscrape import AmazonBaseSession, AmazonScrapeSession


class TestAmazon:
    @pytest.fixture()
    def session(self):
        session = AmazonScrapeSession()
        yield session
        session.close()

    def test_amazon_product(self, session: AmazonBaseSession) -> None:
        PRODUCT_ASIN = "B09ZRNP19N"
        soup = session.get_asin_html(PRODUCT_ASIN)
        product_info = session.get_product_info(soup)

        print()
        print(product_info)


__all__ = ("TestAmazon",)

if __name__ == "__main__":
    pytest.main()
