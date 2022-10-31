// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_hash.h"
#include "column/column_viewer.h"
#include "column/map_column.h"
#include "exprs/vectorized/function_helper.h"
#include "udf/udf.h"
#include "util/orlp/pdqsort.h"
#include "util/phmap/phmap.h"

namespace starrocks::vectorized {

class MapFunctions {
public:
    DEFINE_VECTORIZED_FN(map_size);

    DEFINE_VECTORIZED_FN(map_keys);

    DEFINE_VECTORIZED_FN(map_values);
};

} // namespace starrocks::vectorized
