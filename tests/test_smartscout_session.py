import pytest

from smartscoutscrape import SmartScoutSession

# These secrets are not valuable, but there are scrapers that go on GitHub and email me when they find secrets,
# and I'm getting sick of them


@pytest.fixture(scope="session")  # this also takes FOREVER to login
def smartscout_session():
    session = SmartScoutSession()
    session.login()
    yield session
    session.close()


class TestSession:
    # def test_register_new_account(self):
    #     session = SmartScoutSession()
    #
    #     random_email = session.random_characters(16) + "@gmail.com"
    #     random_password = session.random_characters(16)
    #     random_first_name = session.random_characters(16)
    #     random_last_name = session.random_characters(16)
    #
    #     session.register(
    #         email=random_email,
    #         password=random_password,
    #         first_name=random_first_name,
    #         last_name=random_last_name,
    #     )
    #
    #     session.login(
    #         email=random_email,
    #         password=random_password,
    #     )
    #
    #     assert session.logged_in

    def test_search_product(self, smartscout_session) -> None:
        for i, product in enumerate(smartscout_session.search_products()):
            if i > 100:
                break
            assert product is not None
            print(f"{product['brandName']} {product['title']} ({product['asin']})")

    def test_search_recursive_product(self, smartscout_session) -> None:
        for i, product in enumerate(smartscout_session.search_products_recursive()):
            if i > 100:
                break  # Don't want to spam the API
            assert product is not None
            print(f"{product['brandName']} {product['title']} ({product['asin']})")

    def test_image(self, smartscout_session) -> None:
        for i, product in enumerate(smartscout_session.search_products()):
            if i > 10:
                break
            assert product is not None
            print(product["asin"])
            print(smartscout_session.get_b64_image_from_product(product))

    def test_categories(self, smartscout_session) -> None:
        for category in smartscout_session.categories():
            assert category is not None
            print(f"{category['name']} ({category['id']})")

    def test_subcategories(self, smartscout_session) -> None:
        for subcategory in smartscout_session.subcategories():
            assert subcategory is not None
            print(f"{subcategory['subcategoryName']} ({subcategory['id']})")

    def test_count(self, smartscout_session) -> None:
        whole = smartscout_session.get_total_number_of_products()
        print(f"all: {whole}")


__all__ = ("TestSession",)

if __name__ == "__main__":
    pytest.main()
