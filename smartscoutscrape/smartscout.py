"""
Sesssion class for SmartScout API

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
import base64
import json
import random
import string
import time
from typing import Generator, Literal, Self, Sequence, TypeAlias

from requests import Session
from requests.adapters import HTTPAdapter
from requests.structures import CaseInsensitiveDict

from .utils import THREADING_SAFE_MAX_WORKERS

Marketplace: TypeAlias = Literal[
    "US",  # Amazon US
    "UK",  # Amazon UK
    "IT",  # Amazon Italy
    "DE",  # Amazon Germany
    "CA",  # Amazon Canada
    "FR",  # Amazon France
    "MX",  # Amazon Mexico
    "IN",  # Amazon India
    "ES",  # Amazon Spain
]


class SmartScoutSession:
    # Class variables

    KNOWN_EMAIL = base64.b64decode(b"amVuc0ByZWd1bGFkLnh5eg==").decode("utf-8")
    KNOWN_PASSWORD = base64.b64decode(b"TXFzRlM3UFpQWVVvaUw=").decode("utf-8")
    ALL_FIELDS = [
        "brandId",
        "subcategoryId",
        "imageUrl",
        "asin",
        "title",
        "brandName",
        "categoryId",
        "subcategory.subcategoryName",
        "rank",
        "amazonIsr",
        "numberOfSellers",
        "isVariation",
        "monthlyRevenueEstimate",
        "ttm",
        "monthlyUnitsSold",
        "listedSince",
        "reviewCount",
        "reviewRating",
        "numberFbaSellers",
        "buyBoxPrice",
        "averageBuyBoxPrice",
        "buyBoxEquity",
        "revenueEquity",
        "marginEquity",
        "outOfStockNow",
        "productPageScore",
        "manufacturer",
        "upc",
        "partNumber",
        "model",
        "numberOfItems",
        "totalRatings",
        "momGrowth",
        "momGrowth12",
        "note",
    ]

    # Instance variables

    req: Session

    setup_account: bool | None
    refresh_token: str | None
    access_jwt: str | None
    user_profile: dict | None

    _req_id_header: str
    marketplace: Marketplace

    # Caches

    _category_cache: list[dict] | None
    _sub_category_cache: list[dict] | None

    def __init__(
        self, marketplace: Marketplace = "US", proxy: str | None = None, threads: int = THREADING_SAFE_MAX_WORKERS
    ) -> None:
        self.setup_account = False
        self.refresh_token = None
        self.access_jwt = None
        self.user_profile = None

        self.marketplace = marketplace
        self._req_id_header = self.random_characters(32)

        self.req = Session()
        adapter = HTTPAdapter(max_retries=3, pool_connections=threads, pool_maxsize=threads)
        self.req.mount("https://", adapter)
        self.req.mount("http://", adapter)
        self.req.headers.update(self._headers())
        if proxy is not None:
            self.req.proxies.update({"https": proxy, "http": proxy})

        self._category_cache = None
        self._sub_category_cache = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.req.close()

    def refresh_jwt(self) -> None:
        pass  # TODO: implement

    @property
    def authorization(self) -> str | None:
        if self.access_jwt is None:
            return None
        if self.expired:
            self.refresh_jwt()
        return f"Bearer {self.access_jwt}"

    @property
    def expired(self) -> bool:
        if self.access_jwt is None:
            return True
        decoded_jwt = self.access_jwt.split(".")[1]
        decoded_jwt += "=" * ((4 - len(decoded_jwt) % 4) % 4)
        decoded_jwt_plaintext = base64.b64decode(decoded_jwt).decode("utf-8")
        decoded_jwt_data = json.loads(decoded_jwt_plaintext)
        return decoded_jwt_data["exp"] < time.time()

    @property
    def logged_in(self) -> bool:
        return self.access_jwt is not None

    @staticmethod
    def random_characters(length: int) -> str:
        return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))  # nosec

    def _request_id(self) -> str:
        return f"{self._req_id_header}.{self.random_characters(16)}"

    def _headers(self) -> CaseInsensitiveDict[str]:
        req_id = self._request_id()  # only for this request

        req_headers = CaseInsensitiveDict(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
                # "Accept": "text/plain",  # idk why the website doesn't use application/json
                # "Accept-Language": "en-US,en;q=0.9",
                # "Accept-Encoding": "gzip, deflate, br",
                # "Connection": "keep-alive",
                "Referer": "https://app.smartscout.com/",
                "Origin": "https://app.smartscout.com",
                "X-Smartscout-Marketplace": self.marketplace,
                "Request-ID": f"|{req_id}",  # not sure if needed
                "Traceparent": f"00-{req_id}-0",  # not sure if needed
            }
        )

        if self.authorization is not None:
            req_headers["Authorization"] = self.authorization

        return req_headers

    def register(
        self,
        email: str,
        password: str,
        first_name: str,
        last_name: str,
        phone_number: str | None = None,
    ) -> bool:
        payload = {
            "email": email,
            "password": password,
            "passwordConfirm": password,
            "firstName": first_name,
            "lastName": last_name,
            "phoneNumber": phone_number,
        }
        with self.req.post(
            "https://smartscoutapi-east.azurewebsites.net/api/authentication/register",
            headers=self._headers(),
            json=payload,
        ) as resp:
            resp.raise_for_status()

            status_dict = resp.json()

            if status_dict.get("failed", False):
                raise Exception(status_dict["errors"])
            else:
                return status_dict.get("succeeded", False)

    def login(self, email: str = KNOWN_EMAIL, password: str = KNOWN_PASSWORD) -> None:
        """
        Login to SmartScout with the given email and password. If you omit the email and password, it will use my account.
        Hey, I told you it's a flawed model.
        """
        payload = {"userName": email, "password": password}
        with self.req.post(
            "https://smartscoutapi-east.azurewebsites.net/api/authentication/login",
            headers=self._headers(),
            json=payload,
        ) as resp:
            resp.raise_for_status()

            data = resp.json()

            self.setup_account = data.get("setupAccount", None)
            self.refresh_token = data.get("refreshToken", None)
            self.access_jwt = data.get("token", None)
            self.user_profile = data.get("userProfile", None)

    def subscribe(self) -> None:
        """
        Subscribe to SmartScout (not implemented)
        """
        "https://smartscoutapi-east.azurewebsites.net/api/subscription"
        raise NotImplementedError

    def enrollments(self) -> None:
        """
        Enroll in SmartScout (not implemented)
        """
        "https://smartscoutapi-east.azurewebsites.net/api/enrollments"
        raise NotImplementedError

    def session(self) -> None:
        """
        Get session information (not implemented)
        """
        "https://smartscoutapi-east.azurewebsites.net/api/sessions"
        raise NotImplementedError

    def get_active_subscription(self) -> None:
        """
        Get active subscription (not implemented)
        """
        "https://smartscoutapi-east.azurewebsites.net/api/plans/plans-active-subscription"
        raise NotImplementedError

    # Key Tools

    # # Brands

    # # Traffic Graph

    # # Sellers

    # # Search Terms (ASINs)

    # # FBA Calculator

    # # UPC Scanner

    # # Suppliers

    # # "AI" Listing Architect

    # # Popping Topics (not free)

    # Other Tools

    # # Subcategories

    def categories(self) -> list[dict]:
        if self._category_cache is not None:
            return self._category_cache

        with self.req.get(
            "https://smartscoutapi-east.azurewebsites.net/api/categories", headers=self._headers()
        ) as resp:
            resp.raise_for_status()

            data = resp.json()

            self._category_cache = data["categories"]

            return self._category_cache

    def subcategories(self) -> list[dict]:
        if self._sub_category_cache is not None:
            return self._sub_category_cache

        payload = {
            "filter": {},
            "loadDefaultData": True,
            "parentId": None,
        }
        with self.req.post(
            "https://smartscoutapi-east.azurewebsites.net/api/subcategories/search",
            headers=self._headers(),
            json=payload,
        ) as resp:
            resp.raise_for_status()

            data = resp.json()

            total_row_count = data["pageInfo"]["totalRowCount"]
            payload = data["payload"]

            self._sub_category_cache = payload

            return payload

    def find_category_parent(self, category_id: int) -> Generator[dict, None, None]:
        subcategories = self.subcategories()

        parent_category: dict | None = None

        while True:
            for category in subcategories:
                if category["id"] != category_id:
                    continue

                if category["isParent"]:
                    parent_category = category
                    yield category

                if category["parentId"] is None:
                    return

                category_id = category["parentId"]
                break
            else:
                raise Exception("Category not found")

    # # Products

    def get_product_image(self, product: dict) -> tuple[str, bytes]:
        image_url_id = product["imageUrl"]

        if image_url_id.startswith("https://") or image_url_id.startswith("http://"):
            image_url = image_url_id
        else:
            image_url = f"https://images-na.ssl-images-amazon.com/images/I/{image_url_id}"

        with self.req.get(image_url, headers=self._headers()) as resp:
            resp.raise_for_status()

            image_bytes = resp.content
            content_type = resp.headers["Content-Type"]

            return content_type, image_bytes

    def get_b64_image_from_product(self, product: dict) -> str:
        content_type, image_bytes = self.get_product_image(product)

        base64_bytes = base64.b64encode(image_bytes)
        base64_string = base64_bytes.decode("utf-8")

        return f"data:{content_type};base64,{base64_string}"

    def _search_products_one_page(
        self,
        category_id: int | None = None,
        fields: Sequence[str] | None = None,
        load_default_data: bool | None = None,
        *,
        start_row: int,
        end_row: int,
    ) -> tuple[int | None, list[dict]]:
        if category_id is not None:
            # TODO: Add other filters if needed
            filters = {
                "categoryId": {"values": [str(category_id)], "filterType": "set"},
                "rank": {"min": None, "max": None},
                "numberOfSellers": {"min": None, "max": None},
                "numberFbaSellers": {"min": None, "max": None},
                "amazonIsr": {"min": None, "max": None},
                "buyBoxPrice": {"min": None, "max": None},
                "monthlyRevenueEstimate": {"min": None, "max": None},
                "reviewRating": {"min": None, "max": None},
                "reviewCount": {"min": None, "max": None},
            }
        else:
            filters = {}

        if fields is None:
            fields = self.ALL_FIELDS

        if load_default_data is None:
            load_default_data = not bool(filters)

        payload = {
            "filter": filters,
            "loadDefaultData": load_default_data,
            "pageFilter": {
                "startRow": start_row,
                "endRow": end_row,
                "sortModel": [],
                "includeTotalRowCount": True,
                "fields": list(fields),
            },
        }

        with self.req.post(
            "https://smartscoutapi-east.azurewebsites.net/api/products/search",
            headers=self._headers(),
            json=payload,
        ) as resp:
            resp.raise_for_status()

            data = resp.json()

            return data["pageInfo"]["totalRowCount"], data["payload"]

    def get_number_of_products(self, category_id: int | None) -> int:
        """
        Get the number of products that match the search criteria
        """
        total_row_count, _ = self._search_products_one_page(
            category_id=category_id, load_default_data=False, fields=[], start_row=0, end_row=1
        )
        return total_row_count

    def get_total_number_of_products(self) -> int:
        """
        Get the total number of all products in the database.
        """
        return self.get_number_of_products(category_id=None)

    def search_products(
        self,
        *args,
        **kwargs,
    ) -> Generator[dict, None, None]:
        """
        Search products generator
        """
        row_step = 1000  # doesn't work with below

        start_row = 0
        end_row = row_step
        total_row_count = row_step + 1

        while start_row < total_row_count:
            total_row_count, products = self._search_products_one_page(
                *args, **kwargs, start_row=start_row, end_row=end_row
            )
            start_row += row_step
            end_row += row_step

            yield from products

    def search_products_recursive(self, fields: Sequence[str] | None = None) -> Generator[dict, None, None]:
        """
        Search all products in all categories recursively
        """
        for category in self.categories():
            yield from self.search_products(
                category_id=int(category["id"]),
                fields=fields,
            )

    # # Top New Products

    # # Seller Map

    # # Product-Seller View

    # # Ad Spy

    # # Sales Estimator

    # Account

    # # Settings

    # # Subscription

    # # Billing

    # # User

    # # Sign Out

    def sign_out(self) -> None:
        self.access_jwt = None

    def close(self) -> None:
        self.sign_out()
        self.req.close()


__all__ = ("SmartScoutSession",)
