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

#pragma once

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_hash.h"
#include "column/column_viewer.h"
#include "column/map_column.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"
#include "util/orlp/pdqsort.h"
#include "util/phmap/phmap.h"

namespace starrocks {

class MapFunctions {
public:
    DEFINE_VECTORIZED_FN(map_from_arrays);

    DEFINE_VECTORIZED_FN(map_size);

    DEFINE_VECTORIZED_FN(map_keys);

    DEFINE_VECTORIZED_FN(map_values);

    DEFINE_VECTORIZED_FN(map_filter);

    DEFINE_VECTORIZED_FN(distinct_map_keys);

    DEFINE_VECTORIZED_FN(map_concat);

private:
    static void _filter_map_items(const MapColumn* src_column, const ColumnPtr& raw_filter, MapColumn* dest_column,
                                  NullColumn* dest_null_map);
};

} // namespace starrocks
