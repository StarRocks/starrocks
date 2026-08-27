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

#include "exprs/agg/factory/aggregate_resolver_sum_map.hpp"

namespace starrocks {

// Floating-point and decimal key types (TYPE_DECIMAL256 is intentionally excluded, matching the
// original register_avg() which skipped it for sum_map).
void AggregateFuncResolver::register_sum_map2() {
    register_sum_map_for_keys<TYPE_FLOAT, TYPE_DOUBLE, TYPE_DECIMALV2, TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128>(
            this);
}

} // namespace starrocks
