"""
Utilities for smartscoutscrape.

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
import csv
import os
import sys
from typing import Any, Mapping


def dot_access(mapping: Mapping[str, str | Any], locator: str) -> Any | None:
    """
    Access a mapping using dot notation.
    """
    item: Any | None = None
    for i, location in enumerate(locator.split(".")):
        local_mapping = mapping if i == 0 else item
        if location in local_mapping:
            item = local_mapping[location]
        else:
            return None
    return item


THREADING_SAFE_MAX_WORKERS = min(32, (os.cpu_count() or 1) + 4)


def increase_csv_maxlen() -> int:
    largest_possible_int = sys.maxsize

    while True:
        # decrease the maxInt value by factor 10
        # as long as the OverflowError occurs.

        try:
            csv.field_size_limit(largest_possible_int)
            break
        except OverflowError:
            largest_possible_int = int(largest_possible_int / 10)

    return largest_possible_int


increase_csv_maxlen()


def top_level_dict(input_dict: dict) -> dict:
    output_dict = dict()

    for key, value in input_dict.items():
        if not isinstance(value, dict):
            output_dict[key] = value

    return output_dict


__all__ = ("dot_access", "THREADING_SAFE_MAX_WORKERS", "increase_csv_maxlen", "top_level_dict")
