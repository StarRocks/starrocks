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

#include "exprs/ds_theta_functions.h"

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/agg/data_sketch/ds_theta.h"
#include "gutil/strings/substitute.h"

#pragma push_macro("IS_BIG_ENDIAN")
#undef IS_BIG_ENDIAN
#include "datasketches/theta_a_not_b.hpp"
#include "datasketches/theta_intersection.hpp"
#include "datasketches/theta_sketch.hpp"
#include "datasketches/theta_union.hpp"
#pragma pop_macro("IS_BIG_ENDIAN")

namespace starrocks {

using alloc_type = DataSketchesTheta::alloc_type;
using wrapped_compact_theta_sketch = DataSketchesTheta::wrapped_compact_theta_sketch;
using theta_union_type = DataSketchesTheta::theta_union_type;
using theta_intersection_type = datasketches::theta_intersection_alloc<alloc_type>;
using theta_a_not_b_type = datasketches::theta_a_not_b_alloc<alloc_type>;
using theta_compact_sketch = DataSketchesTheta::theta_compact_sketch;

namespace {

// Serializes a compact theta sketch to the row buffer.
template <typename CompactSketch>
void append_compact(CompactSketch&& sk, ColumnBuilder<TYPE_VARBINARY>& builder) {
    auto bytes = sk.serialize();
    builder.append(Slice(reinterpret_cast<const char*>(bytes.data()), bytes.size()));
}

} // namespace

StatusOr<ColumnPtr> DsThetaFunctions::ds_theta_estimate(FunctionContext* context, const Columns& columns) {
    ColumnViewer<TYPE_VARBINARY> viewer(columns[0]);
    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_BIGINT> builder(size);

    for (size_t row = 0; row < size; ++row) {
        if (viewer.is_null(row)) {
            builder.append_null();
            continue;
        }
        auto slice = viewer.value(row);
        if (slice.size == 0) {
            builder.append(0);
            continue;
        }
        try {
            auto sk = wrapped_compact_theta_sketch::wrap(slice.data, slice.size);
            builder.append(static_cast<int64_t>(sk.get_estimate()));
        } catch (const std::exception& e) {
            return Status::InternalError(strings::Substitute("ds_theta_estimate failed: $0", e.what()));
        }
    }
    return builder.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> DsThetaFunctions::ds_theta_union(FunctionContext* context, const Columns& columns) {
    ColumnViewer<TYPE_VARBINARY> lhs(columns[0]);
    ColumnViewer<TYPE_VARBINARY> rhs(columns[1]);
    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_VARBINARY> builder(size);

    for (size_t row = 0; row < size; ++row) {
        if (lhs.is_null(row) || rhs.is_null(row)) {
            builder.append_null();
            continue;
        }
        int64_t mem = 0;
        try {
            theta_union_type u = theta_union_type::builder(alloc_type(&mem)).build();
            auto a_slice = lhs.value(row);
            auto b_slice = rhs.value(row);
            if (a_slice.size > 0) {
                u.update(wrapped_compact_theta_sketch::wrap(a_slice.data, a_slice.size));
            }
            if (b_slice.size > 0) {
                u.update(wrapped_compact_theta_sketch::wrap(b_slice.data, b_slice.size));
            }
            append_compact(u.get_result(), builder);
        } catch (const std::exception& e) {
            return Status::InternalError(strings::Substitute("ds_theta_union failed: $0", e.what()));
        }
    }
    return builder.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> DsThetaFunctions::ds_theta_intersect(FunctionContext* context, const Columns& columns) {
    ColumnViewer<TYPE_VARBINARY> lhs(columns[0]);
    ColumnViewer<TYPE_VARBINARY> rhs(columns[1]);
    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_VARBINARY> builder(size);

    for (size_t row = 0; row < size; ++row) {
        if (lhs.is_null(row) || rhs.is_null(row)) {
            builder.append_null();
            continue;
        }
        int64_t mem = 0;
        try {
            theta_intersection_type inter(datasketches::DEFAULT_SEED, alloc_type(&mem));
            auto a_slice = lhs.value(row);
            auto b_slice = rhs.value(row);
            // intersection: feeding an empty sketch yields empty result, matching set semantics
            inter.update(wrapped_compact_theta_sketch::wrap(a_slice.data, a_slice.size));
            inter.update(wrapped_compact_theta_sketch::wrap(b_slice.data, b_slice.size));
            append_compact(inter.get_result(), builder);
        } catch (const std::exception& e) {
            return Status::InternalError(strings::Substitute("ds_theta_intersect failed: $0", e.what()));
        }
    }
    return builder.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> DsThetaFunctions::ds_theta_a_not_b(FunctionContext* context, const Columns& columns) {
    ColumnViewer<TYPE_VARBINARY> lhs(columns[0]);
    ColumnViewer<TYPE_VARBINARY> rhs(columns[1]);
    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_VARBINARY> builder(size);

    for (size_t row = 0; row < size; ++row) {
        if (lhs.is_null(row) || rhs.is_null(row)) {
            builder.append_null();
            continue;
        }
        int64_t mem = 0;
        try {
            theta_a_not_b_type anb(datasketches::DEFAULT_SEED, alloc_type(&mem));
            auto a_slice = lhs.value(row);
            auto b_slice = rhs.value(row);
            auto a = wrapped_compact_theta_sketch::wrap(a_slice.data, a_slice.size);
            auto b = wrapped_compact_theta_sketch::wrap(b_slice.data, b_slice.size);
            append_compact(anb.compute(a, b), builder);
        } catch (const std::exception& e) {
            return Status::InternalError(strings::Substitute("ds_theta_a_not_b failed: $0", e.what()));
        }
    }
    return builder.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks

#include "gen_cpp/opcode/DsThetaFunctions.inc"
