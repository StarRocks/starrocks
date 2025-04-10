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

#include "formats/parquet/statistics_helper.h"

#include <string>

#include "column/column_helper.h"
#include "column/datum.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/predicate.h"
#include "formats/parquet/column_converter.h"
#include "formats/parquet/encoding_plain.h"
#include "formats/parquet/schema.h"
#include "gutil/casts.h"
#include "runtime/large_int_value.h"
#include "runtime/types.h"
#include "simd/simd.h"
#include "storage/column_predicate.h"
#include "storage/types.h"
#include "storage/uint24.h"
#include "types/date_value.h"
#include "types/logical_type.h"

namespace starrocks::parquet {

Status StatisticsHelper::decode_value_into_column(ColumnPtr column, const std::vector<std::string>& values,
                                                  const TypeDescriptor& type, const ParquetField* field,
                                                  const std::string& timezone) {
    std::unique_ptr<ColumnConverter> converter;
    RETURN_IF_ERROR(ColumnConverterFactory::create_converter(*field, type, timezone, &converter));
    bool ret = true;
    switch (field->physical_type) {
    case tparquet::Type::type::INT32: {
        int32_t decode_value = 0;
        if (!converter->need_convert) {
            for (size_t i = 0; i < values.size(); i++) {
                RETURN_IF_ERROR(PlainDecoder<int32_t>::decode(values[i], &decode_value));
                ret &= (column->append_numbers(&decode_value, sizeof(int32_t)) > 0);
            }
        } else {
            ColumnPtr src_column = converter->create_src_column();
            for (size_t i = 0; i < values.size(); i++) {
                RETURN_IF_ERROR(PlainDecoder<int32_t>::decode(values[i], &decode_value));
                ret &= (src_column->append_numbers(&decode_value, sizeof(int32_t)) > 0);
            }
            RETURN_IF_ERROR(converter->convert(src_column, column.get()));
        }
        break;
    }
    case tparquet::Type::type::INT64: {
        int64_t decode_value = 0;
        if (!converter->need_convert) {
            for (size_t i = 0; i < values.size(); i++) {
                RETURN_IF_ERROR(PlainDecoder<int64_t>::decode(values[i], &decode_value));
                ret &= (column->append_numbers(&decode_value, sizeof(int64_t)) > 0);
            }
        } else {
            ColumnPtr src_column = converter->create_src_column();
            for (size_t i = 0; i < values.size(); i++) {
                RETURN_IF_ERROR(PlainDecoder<int64_t>::decode(values[i], &decode_value));
                ret &= (src_column->append_numbers(&decode_value, sizeof(int64_t)) > 0);
            }
            RETURN_IF_ERROR(converter->convert(src_column, column.get()));
        }
        break;
    }
    case tparquet::Type::type::BYTE_ARRAY:
    // todo: FLBA need more test
    case tparquet::Type::type::FIXED_LEN_BYTE_ARRAY: {
        Slice decode_value;
        if (!converter->need_convert) {
            for (size_t i = 0; i < values.size(); i++) {
                RETURN_IF_ERROR(PlainDecoder<Slice>::decode(values[i], &decode_value));
                ret &= column->append_strings(std::vector<Slice>{decode_value});
            }
        } else {
            ColumnPtr src_column = converter->create_src_column();
            for (size_t i = 0; i < values.size(); i++) {
                RETURN_IF_ERROR(PlainDecoder<Slice>::decode(values[i], &decode_value));
                ret &= src_column->append_strings(std::vector<Slice>{decode_value});
            }
            RETURN_IF_ERROR(converter->convert(src_column, column.get()));
        }
        break;
    }
    default:
        return Status::Aborted("Not Supported min/max value type");
    }

    if (UNLIKELY(!ret)) {
        return Status::InternalError("Decode min-max column failed");
    }
    return Status::OK();
}

bool StatisticsHelper::can_be_used_for_statistics_filter(ExprContext* ctx,
                                                         StatisticsHelper::StatSupportedFilter& filter_type) {
    const Expr* root_expr = ctx->root();
    if (root_expr->node_type() == TExprNodeType::IN_PRED && root_expr->op() == TExprOpcode::FILTER_IN) {
        const Expr* c = root_expr->get_child(0);
        if (c->node_type() != TExprNodeType::type::SLOT_REF) {
            return false;
        }

        const TypeDescriptor& td = c->type();
        // TODO: support more data type
        if (!td.is_integer_type() && !td.is_string_type() && !td.is_date_type()) {
            return false;
        }

        LogicalType ltype = c->type().type;

        switch (ltype) {
#define M(NAME)                                                                                         \
    case LogicalType::NAME: {                                                                           \
        if (dynamic_cast<const VectorizedInConstPredicate<LogicalType::NAME>*>(root_expr) != nullptr) { \
            filter_type = StatisticsHelper::StatSupportedFilter::FILTER_IN;                             \
            return true;                                                                                \
        } else {                                                                                        \
            return false;                                                                               \
        }                                                                                               \
    }
            APPLY_FOR_ALL_SCALAR_TYPE(M);
#undef M
        default:
            return false;
        }
    } else if (root_expr->node_type() == TExprNodeType::FUNCTION_CALL) {
        std::string null_function_name;
        if (root_expr->is_null_scalar_function(null_function_name)) {
            const Expr* c = root_expr->get_child(0);
            if (c->node_type() != TExprNodeType::type::SLOT_REF) {
                return false;
            } else {
                if (null_function_name == "null") {
                    filter_type = StatisticsHelper::StatSupportedFilter::IS_NULL;
                } else {
                    filter_type = StatisticsHelper::StatSupportedFilter::IS_NOT_NULL;
                }
                return true;
            }
        } else {
            return false;
        }
    } else {
        return false;
    }
}

void translate_to_string_value(ColumnPtr col, size_t i, std::string& value) {
    if (col->is_date()) {
        value = col->get(i).get_date().to_string();
        return;
    } else if (col->is_timestamp()) {
        value = col->get(i).get_timestamp().to_string();
        return;
    }

    auto v = col->get(i);
    v.visit([&](auto& variant) {
        std::visit(
                [&](auto&& arg) {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_same_v<T, Slice>) {
                        value = arg.to_string();
                    } else if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
                                         std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>) {
                        value = std::to_string(arg);
                    } else if constexpr (std::is_same_v<T, int128_t>) {
                        value = LargeIntValue::to_string(arg);
                    } else {
                        // not supported, and should be denied in can_be_used_for_statistics_filter
                        DCHECK(false) << "Unsupported type";
                    }
                },
                variant);
    });
}

Status StatisticsHelper::in_filter_on_min_max_stat(const std::vector<std::string>& min_values,
                                                   const std::vector<std::string>& max_values, ExprContext* ctx,
                                                   const ParquetField* field, const std::string& timezone,
                                                   std::vector<bool>& selected) {
    const Expr* root_expr = ctx->root();
    DCHECK(root_expr->node_type() == TExprNodeType::IN_PRED && root_expr->op() == TExprOpcode::FILTER_IN);
    const Expr* c = root_expr->get_child(0);
    LogicalType ltype = c->type().type;
    ColumnPtr values;
    switch (ltype) {
#define M(NAME)                                                                                                \
    case LogicalType::NAME: {                                                                                  \
        const auto* in_filter = dynamic_cast<const VectorizedInConstPredicate<LogicalType::NAME>*>(root_expr); \
        if (in_filter != nullptr) {                                                                            \
            values = in_filter->get_all_values();                                                              \
            break;                                                                                             \
        } else {                                                                                               \
            return Status::OK();                                                                               \
        }                                                                                                      \
    }
        APPLY_FOR_ALL_SCALAR_TYPE(M);
#undef M
    default:
        return Status::OK();
    }

    // TODO: there is no need to use nullable column,
    //  but there are many places in our reader just treat column as nullable, and use down_cast<NullableColumn>
    ColumnPtr min_col = ColumnHelper::create_column(c->type(), true);
    min_col->reserve(min_values.size());
    RETURN_IF_ERROR(decode_value_into_column(min_col, min_values, c->type(), field, timezone));
    DCHECK(!min_col->has_null());
    min_col = down_cast<NullableColumn*>(min_col.get())->data_column();
    ColumnPtr max_col = ColumnHelper::create_column(c->type(), true);
    max_col->reserve(max_values.size());
    RETURN_IF_ERROR(decode_value_into_column(max_col, max_values, c->type(), field, timezone));
    DCHECK(!max_col->has_null());
    max_col = down_cast<NullableColumn*>(max_col.get())->data_column();

    // logic and example:
    // there are two pairs of min/max value like [1, 4] (which means min_value is 1 and max value is 4), [4, 6]
    // the in_const_predicate get values as [2, 3, 7] (which means we have a predicate like `in [2, 3, 7]`)
    // so the step to decide if there is value in in-filter locate in min->max:
    // 1. treat values in in-filter as a col, [2,3,7]
    // 2. for each pair min/max value create predicate col >= min and col =< max
    //    [2,3,7] >= 1 & [2, 3, 7] <= 4;
    // 3. evaluate the predicate, if there is nonzero in the result,
    //    we know there is at least one value locate in min->max
    //    [2,3,7] >= 1 -> [1, 1, 1] [2, 3, 7] <= 4 -> [1, 1, 0]
    //    so the result is [1, 1, 0], which means [2, 3] locate in 1->4
    // for [4, 6], [2, 3, 7] >= 4 -> [0, 0, 1], [2, 3, 7] =< 6 -> [1, 1, 0]
    // [0, 0, 1] & [1, 1, 0] -> [0, 0, 0], so there is no value locate in 4->6

    for (size_t i = 0; i < min_values.size(); i++) {
        ObjectPool pool;
        std::string min_value;
        std::string max_value;

        translate_to_string_value(min_col, i, min_value);
        translate_to_string_value(max_col, i, max_value);

        Filter filter(values->size(), 1);

        ColumnPredicate* pred_ge = pool.add(new_column_ge_predicate(get_type_info(ltype), 0, min_value));
        RETURN_IF_ERROR(pred_ge->evaluate_and(values.get(), filter.data()));
        if (!SIMD::contain_nonzero(filter)) {
            selected[i] = false;
            continue;
        }
        ColumnPredicate* pred_le = pool.add(new_column_le_predicate(get_type_info(ltype), 0, max_value));
        RETURN_IF_ERROR(pred_le->evaluate_and(values.get(), filter.data()));
        selected[i] = SIMD::contain_nonzero(filter);
    }

    return Status::OK();
}

} // namespace starrocks::parquet