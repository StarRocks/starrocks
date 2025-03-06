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

#include "exprs/hyperloglog_functions.h"

#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "column/object_column.h"
#include "exprs/function_context.h"
#include "exprs/unary_function.h"
#include "types/hll.h"
#include "util/phmap/phmap.h"

namespace starrocks {

// hll_cardinality_from_string
DEFINE_UNARY_FN_WITH_IMPL(hllCardinalityFromStringImpl, str) {
    HyperLogLog hll(str);
    return hll.estimate_cardinality();
}

StatusOr<ColumnPtr> HyperloglogFunctions::hll_cardinality_from_string(FunctionContext* context,
                                                                      const starrocks::Columns& columns) {
    return VectorizedStrictUnaryFunction<hllCardinalityFromStringImpl>::evaluate<TYPE_VARCHAR, TYPE_BIGINT>(columns[0]);
}

// hll_cardinality
DEFINE_UNARY_FN_WITH_IMPL(hllCardinalityImpl, hll_ptr) {
    return hll_ptr->estimate_cardinality();
}

StatusOr<ColumnPtr> HyperloglogFunctions::hll_cardinality(FunctionContext* context, const starrocks::Columns& columns) {
    return VectorizedStrictUnaryFunction<hllCardinalityImpl>::evaluate<TYPE_HLL, TYPE_BIGINT>(columns[0]);
}

// hll_hash
StatusOr<ColumnPtr> HyperloglogFunctions::hll_hash(FunctionContext* context, const Columns& columns) {
    ColumnViewer<TYPE_VARCHAR> str_viewer(columns[0]);

    auto hll_column = HyperLogLogColumn::create();

    size_t size = columns[0]->size();
    for (int row = 0; row < size; ++row) {
        HyperLogLog hll;
        if (!str_viewer.is_null(row)) {
            Slice s = str_viewer.value(row);
            uint64_t hash = HashUtil::murmur_hash64A(s.data, s.size, HashUtil::MURMUR_SEED);
            hll.update(hash);
        }

        hll_column->append(&hll);
    }

    if (ColumnHelper::is_all_const(columns)) {
        return ConstColumn::create(std::move(hll_column), columns[0]->size());
    } else {
        return hll_column;
    }
}

// hll_empty
StatusOr<ColumnPtr> HyperloglogFunctions::hll_empty(FunctionContext* context, const Columns& columns) {
    auto p = HyperLogLogColumn::create();

    p->append_default();
    return ConstColumn::create(std::move(p), 1);
}

// hll_serialize
DEFINE_UNARY_FN_WITH_IMPL(HllSerializeImpl, hll) {
    size_t size = hll->serialize_size();
    char data[size];
    size = hll->serialize((uint8_t*)data);
    return std::string(data, size);
}

StatusOr<ColumnPtr> HyperloglogFunctions::hll_serialize(FunctionContext* context, const Columns& columns) {
    return VectorizedStringStrictUnaryFunction<HllSerializeImpl>::evaluate<TYPE_HLL, TYPE_VARCHAR>(columns[0]);
}

// hll_deserialize
StatusOr<ColumnPtr> HyperloglogFunctions::hll_deserialize(FunctionContext* context, const Columns& columns) {
    ColumnViewer<TYPE_VARCHAR> str_viewer(columns[0]);
    auto hll_column = HyperLogLogColumn::create();
    size_t size = columns[0]->size();
    for (int row = 0; row < size; ++row) {
        HyperLogLog hll;
        if (!str_viewer.is_null(row)) {
            Slice s = str_viewer.value(row);
            hll.deserialize(s);
        }

        hll_column->append(&hll);
    }

    if (ColumnHelper::is_all_const(columns)) {
        return ConstColumn::create(std::move(hll_column), columns[0]->size());
    } else {
        return hll_column;
    }
}

} // namespace starrocks
