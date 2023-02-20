// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "column/datum_tuple.h"

#include "column/schema.h"

namespace starrocks {

int DatumTuple::compare(const Schema& schema, const DatumTuple& rhs) const {
    CHECK_EQ(_datums.size(), schema.num_fields());
    CHECK_EQ(_datums.size(), rhs._datums.size());
    for (size_t i = 0; i < _datums.size(); i++) {
        int r = schema.field(i)->type()->cmp(_datums[i], rhs[i]);
        if (r != 0) {
            return r;
        }
    }
    return 0;
}

} // namespace starrocks
