# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_

from starrocks.requirements import Requirements


class RequirementReasonsTest(fixtures.TestBase):
    def test_every_requirement_the_dialect_sets_gives_its_reason(self):
        """A requirement the dialect opens or closes says why in its docstring, since the suites report only that."""
        undocumented = [
            name
            for name, attribute in vars(Requirements).items()
            if isinstance(attribute, property) and not (attribute.fget.__doc__ or "").strip()
        ]
        eq_(undocumented, [])
