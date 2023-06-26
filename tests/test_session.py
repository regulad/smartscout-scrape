from base64 import b64decode

import pytest

from smartscoutscrape import SmartScoutSession

# These secrets are not valuable, but there are scrapers that go on GitHub and email me when they find secrets,
# and I'm getting sick of them
KNOWN_EMAIL = b64decode(b"amVuc0ByZWd1bGFkLnh5eg==").decode("utf-8")
KNOWN_PASSWORD = b64decode(b"TXFzRlM3UFpQWVVvaUw=").decode("utf-8")


@pytest.fixture(scope="session")
def session():
    session = SmartScoutSession()

    session.login(
        email=KNOWN_EMAIL,
        password=KNOWN_PASSWORD,
    )

    yield session

    session.close()


class TestSession:
    def test_register_new_account(self):
        session = SmartScoutSession()

        random_email = session.random_characters(16) + "@gmail.com"
        random_password = session.random_characters(16)
        random_first_name = session.random_characters(16)
        random_last_name = session.random_characters(16)

        session.register(
            email=random_email,
            password=random_password,
            first_name=random_first_name,
            last_name=random_last_name,
        )

        session.login(
            email=random_email,
            password=random_password,
        )

        assert session.logged_in

    def test_login_existing_account(self):
        session = SmartScoutSession()

        session.login(
            email=KNOWN_EMAIL,
            password=KNOWN_PASSWORD,
        )

        assert session.logged_in

    def test_search_product(self, session: SmartScoutSession) -> None:
        for i, product in enumerate(session.search_products()):
            if i > 100:
                break
            assert product is not None
            print(f"{product['brandName']} {product['title']} ({product['asin']})")

    def test_search_recursive_product(self, session: SmartScoutSession) -> None:
        for i, product in enumerate(session.search_products_recursive()):
            if i > 100:
                break  # Don't want to spam the API
            assert product is not None
            print(f"{product['brandName']} {product['title']} ({product['asin']})")

    def test_image(self, session: SmartScoutSession) -> None:
        for i, product in enumerate(session.search_products()):
            if i > 10:
                break
            assert product is not None
            print(product["asin"])
            print(session.get_b64_image_from_product(product))

    def test_categories(self, session: SmartScoutSession) -> None:
        for category in session.categories():
            assert category is not None
            print(f"{category['name']} ({category['id']})")

    def test_subcategories(self, session: SmartScoutSession) -> None:
        for subcategory in session.subcategories():
            assert subcategory is not None
            print(f"{subcategory['subcategoryName']} ({subcategory['id']})")

    def test_find_category_parent(self, session: SmartScoutSession) -> None:
        TOOLS = -24051790011
        SHOP_NFL = -24071665011

        for parent in session.find_category_parent(TOOLS):
            assert parent is not None
            print(f"{parent['subcategoryName']} ({parent['id']})")
            print("is a child of")

        print()

        for parent in session.find_category_parent(SHOP_NFL):
            assert parent is not None
            print(f"{parent['subcategoryName']} ({parent['id']})")
            print("is a child of")

    def test_count(self, session: SmartScoutSession) -> None:
        whole = session.get_total_number_of_products()
        print(f"all: {whole}")


__all__ = ("TestSession",)

if __name__ == "__main__":
    pytest.main()
