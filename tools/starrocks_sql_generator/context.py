# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generation context with recursion depth tracking."""

from __future__ import annotations

import random
from contextlib import contextmanager
from dataclasses import dataclass, field

from .schema import Schema


@dataclass
class GenContext:
    schema: Schema
    max_depth: int = 3
    max_list_items: int = 2
    seed: int = 0
    rng: random.Random = field(default_factory=random.Random)
    depths: dict[str, int] = field(default_factory=dict)
    path_tag: str = ""

    def __post_init__(self):
        if self.seed:
            self.rng = random.Random(self.seed)

    def can_recurse(self, rule: str) -> bool:
        return self.depths.get(rule, 0) < self.max_depth

    @contextmanager
    def recurse(self, rule: str):
        if not self.can_recurse(rule):
            yield False
            return
        self.depths[rule] = self.depths.get(rule, 0) + 1
        try:
            yield True
        finally:
            self.depths[rule] = self.depths.get(rule, 0) - 1

    def clone(self, *, path_tag: str | None = None, seed: int | None = None) -> GenContext:
        return GenContext(
            schema=self.schema,
            max_depth=self.max_depth,
            max_list_items=self.max_list_items,
            seed=seed if seed is not None else self.seed,
            depths=dict(self.depths),
            path_tag=path_tag if path_tag is not None else self.path_tag,
        )
