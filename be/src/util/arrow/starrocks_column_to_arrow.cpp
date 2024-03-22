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

#include "util/arrow/starrocks_column_to_arrow.h"

#include "column//map_column.h"
#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "common/statusor.h"
#include "exec/arrow_type_traits.h"
#include "exprs/expr.h"
#include "runtime/large_int_value.h"
#include "util/raw_container.h"

namespace starrocks {

class ColumnContext;

// Function to convert rows of the column in the range [start_idx, end_idx] to arrow
typedef arrow::Status (*StarRocksToArrowConvertFunc)(const ColumnPtr& column, int start_idx, int end_idx,
                                                     ColumnContext* column_context, arrow::ArrayBuilder* array_builder);

// The context information for a column
struct ColumnContext {
    ColumnContext(const TypeDescriptor& desc, const std::shared_ptr<arrow::DataType>& at,
                  StarRocksToArrowConvertFunc func)
            : type_desc(desc), arrow_type(at), convert_func(func) {}

    // StarRocks type descriptor for this column
    const TypeDescriptor& type_desc;

    // Arrow data type for this column
    const std::shared_ptr<arrow::DataType>& arrow_type;

    // Function used to convert column to arrow
    StarRocksToArrowConvertFunc convert_func;

    // Child column contexts which are only available for Array/Struct/Map column.
    // The vector is lazily initialized, and should check whether it's empty before
    // using it. And initialize it first if it's empty.
    // The size of the vector depends on the type of the column
    //    Array:  one context for ArrayColumn::_elements
    //    Struct: contexts for each field of StructColumn::_fields
    //    Map:    two contexts for MapColumn::_keys and MapColumn::_values respectively
    std::vector<ColumnContext> child_columns;
};

template <LogicalType LT, ArrowTypeId AT, bool is_nullable, typename = guard::Guard>
struct ColumnToArrowConverter;

DEF_PRED_GUARD(ConvFloatAndIntegerGuard, is_conv_float_integer, LogicalType, LT, ArrowTypeId, AT)
#define IS_CONV_FLOAT_INTEGER_CTOR(LT, AT) DEF_PRED_CASE_CTOR(is_conv_float_integer, LT, AT)
#define IS_CONV_FLOAT_INTEGER(LT, ...) \
    DEF_BINARY_RELATION_ENTRY_SEP_SEMICOLON(IS_CONV_FLOAT_INTEGER_CTOR, LT, ##__VA_ARGS__)

IS_CONV_FLOAT_INTEGER(TYPE_BOOLEAN, ArrowTypeId::BOOL)
IS_CONV_FLOAT_INTEGER(TYPE_TINYINT, ArrowTypeId::INT8)
IS_CONV_FLOAT_INTEGER(TYPE_SMALLINT, ArrowTypeId::INT16)
IS_CONV_FLOAT_INTEGER(TYPE_INT, ArrowTypeId::INT32)
IS_CONV_FLOAT_INTEGER(TYPE_BIGINT, ArrowTypeId::INT64)
IS_CONV_FLOAT_INTEGER(TYPE_FLOAT, ArrowTypeId::FLOAT)
IS_CONV_FLOAT_INTEGER(TYPE_DOUBLE, ArrowTypeId::DOUBLE)
IS_CONV_FLOAT_INTEGER(TYPE_TIME, ArrowTypeId::DOUBLE)

template <LogicalType LT, ArrowTypeId AT, bool is_nullable>
struct ColumnToArrowConverter<LT, AT, is_nullable, ConvFloatAndIntegerGuard<LT, AT>> {
    using StarRocksCppType = RunTimeCppType<LT>;
    using StarRocksColumnType = RunTimeColumnType<LT>;
    using ArrowType = ArrowTypeIdToType<AT>;
    using ArrowBuilderType = typename arrow::TypeTraits<ArrowType>::BuilderType;

    static inline arrow::Status convert(const ColumnPtr& column, int start_idx, int end_idx,
                                        [[maybe_unused]] ColumnContext* column_context,
                                        arrow::ArrayBuilder* array_builder) {
        ArrowBuilderType* builder = down_cast<ArrowBuilderType*>(array_builder);
        if constexpr (is_nullable) {
            const auto* nullable_column = down_cast<NullableColumn*>(column.get());
            const auto* data_column = down_cast<StarRocksColumnType*>(nullable_column->data_column().get());
            const auto& data = data_column->get_data();
            for (auto i = start_idx; i <= end_idx; ++i) {
                if (nullable_column->is_null(i)) {
                    ARROW_RETURN_NOT_OK(builder->AppendNull());
                } else {
                    ARROW_RETURN_NOT_OK(builder->Append(data[i]));
                }
            }
        } else {
            const auto* data_column = down_cast<StarRocksColumnType*>(column.get());
            const auto* values = data_column->get_data().data() + start_idx;
            ARROW_RETURN_NOT_OK(builder->AppendValues(values, end_idx - start_idx + 1));
        }
        return arrow::Status::OK();
    }
};

DEF_PRED_GUARD(ConvDecimalGuard, is_conv_decimal, LogicalType, LT, ArrowTypeId, AT)
#define IS_CONV_DECIMAL_CTOR(LT, AT) DEF_PRED_CASE_CTOR(is_conv_decimal, LT, AT)
#define IS_CONV_DECIMAL_R(AT, ...) DEF_BINARY_RELATION_ENTRY_SEP_SEMICOLON_R(IS_CONV_DECIMAL_CTOR, AT, ##__VA_ARGS__)

IS_CONV_DECIMAL_R(ArrowTypeId::DECIMAL, TYPE_DECIMALV2, TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128)

template <LogicalType LT, ArrowTypeId AT, bool is_nullable>
struct ColumnToArrowConverter<LT, AT, is_nullable, ConvDecimalGuard<LT, AT>> {
    using StarRocksCppType = RunTimeCppType<LT>;
    using StarRocksColumnType = RunTimeColumnType<LT>;
    using ArrowType = ArrowTypeIdToType<AT>;
    using ArrowBuilderType = typename arrow::TypeTraits<ArrowType>::BuilderType;

    static inline arrow::Decimal128 convert_datum(const StarRocksCppType& datum) {
        int128_t value;
        if constexpr (lt_is_decimalv2<LT>) {
            value = datum.value();
        } else if constexpr (lt_is_decimal<LT>) {
            value = datum;
        } else {
            static_assert(lt_is_decimalv2<LT> || lt_is_decimal<LT>, "Illegal LogicalType");
        }
        int64_t high = value >> 64;
        uint64_t low = value;
        return {high, low};
    }

    static inline arrow::Status convert(const ColumnPtr& column, int start_idx, int end_idx,
                                        [[maybe_unused]] ColumnContext* column_context,
                                        arrow::ArrayBuilder* array_builder) {
        ArrowBuilderType* builder = down_cast<ArrowBuilderType*>(array_builder);
        if constexpr (is_nullable) {
            const auto* nullable_column = down_cast<NullableColumn*>(column.get());
            const auto* data_column = down_cast<StarRocksColumnType*>(nullable_column->data_column().get());
            const auto& data = data_column->get_data();
            for (auto i = start_idx; i <= end_idx; ++i) {
                if (nullable_column->is_null(i)) {
                    ARROW_RETURN_NOT_OK(builder->AppendNull());
                } else {
                    ARROW_RETURN_NOT_OK(builder->Append(convert_datum(data[i])));
                }
            }
        } else {
            const auto* data_column = down_cast<StarRocksColumnType*>(column.get());
            const auto& data = data_column->get_data();
            for (auto i = start_idx; i <= end_idx; ++i) {
                ARROW_RETURN_NOT_OK(builder->Append(convert_datum(data[i])));
            }
        }
        return arrow::Status::OK();
    }
};

DEF_PRED_GUARD(ConvBinaryGuard, is_conv_binary, LogicalType, LT, ArrowTypeId, AT)
#define IS_CONV_BINARY_CTOR(LT, AT) DEF_PRED_CASE_CTOR(is_conv_binary, LT, AT)
#define IS_CONV_BINARY_R(AT, ...) DEF_BINARY_RELATION_ENTRY_SEP_SEMICOLON_R(IS_CONV_BINARY_CTOR, AT, ##__VA_ARGS__)

IS_CONV_BINARY_R(ArrowTypeId::STRING, TYPE_VARCHAR, TYPE_HLL, TYPE_CHAR, TYPE_DATE, TYPE_DATETIME, TYPE_LARGEINT)
IS_CONV_BINARY_R(ArrowTypeId::STRING, TYPE_DECIMALV2, TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128)
//TODO(by satanson): one day, we must support TYPE_STRUCT/TYPE_MAP/TYPE_ARRAY
IS_CONV_BINARY_R(ArrowTypeId::STRING, TYPE_JSON)

template <LogicalType LT, ArrowTypeId AT, bool is_nullable>
struct ColumnToArrowConverter<LT, AT, is_nullable, ConvBinaryGuard<LT, AT>> {
    using StarRocksCppType = RunTimeCppType<LT>;
    using StarRocksColumnType = RunTimeColumnType<LT>;
    using ArrowType = ArrowTypeIdToType<AT>;
    using ArrowBuilderType = typename arrow::TypeTraits<ArrowType>::BuilderType;

    static inline std::string convert_datum(const StarRocksCppType& datum, [[maybe_unused]] int precision,
                                            [[maybe_unused]] int scale) {
        if constexpr (lt_is_string<LT> || lt_is_decimalv2<LT> || lt_is_date_or_datetime<LT>) {
            return datum.to_string();
        } else if constexpr (lt_is_hll<LT>) {
            std::string s;
            raw::make_room(&s, datum->max_serialized_size());
            auto n = datum->serialize((uint8_t*)(&s.front()));
            s.resize(n);
            return s;
        } else if constexpr (lt_is_largeint<LT>) {
            return LargeIntValue::to_string(datum);
        } else if constexpr (lt_is_decimal<LT>) {
            return DecimalV3Cast::to_string<StarRocksCppType>(datum, precision, scale);
        } else if constexpr (lt_is_json<LT>) {
            return datum->to_string_uncheck();
        } else {
            static_assert(is_conv_binary<LT, AT>, "Illegal LogicalType");
            return "";
        }
    }

    static inline arrow::Status convert(const ColumnPtr& column, int start_idx, int end_idx,
                                        [[maybe_unused]] ColumnContext* column_context,
                                        arrow::ArrayBuilder* array_builder) {
        ArrowBuilderType* builder = down_cast<ArrowBuilderType*>(array_builder);
        if constexpr (is_nullable) {
            const auto* nullable_column = down_cast<NullableColumn*>(column.get());
            const auto* data_column = down_cast<StarRocksColumnType*>(nullable_column->data_column().get());
            if constexpr (lt_is_string<LT>) {
                const auto& data = data_column->get_proxy_data();
                for (auto i = start_idx; i <= end_idx; ++i) {
                    if (nullable_column->is_null(i)) {
                        ARROW_RETURN_NOT_OK(builder->AppendNull());
                    } else {
                        ARROW_RETURN_NOT_OK(builder->Append(convert_datum(data[i], -1, -1)));
                    }
                }
            } else {
                const auto& data = data_column->get_data();
                [[maybe_unused]] int precision = -1;
                [[maybe_unused]] int scale = -1;
                if constexpr (lt_is_decimal<LT>) {
                    precision = data_column->precision();
                    scale = data_column->scale();
                }
                for (auto i = start_idx; i <= end_idx; ++i) {
                    if (nullable_column->is_null(i)) {
                        ARROW_RETURN_NOT_OK(builder->AppendNull());
                    } else {
                        ARROW_RETURN_NOT_OK(builder->Append(convert_datum(data[i], precision, scale)));
                    }
                }
            }
        } else {
            const auto* data_column = down_cast<StarRocksColumnType*>(column.get());
            if constexpr (lt_is_string<LT>) {
                const auto& data = data_column->get_proxy_data();
                for (auto i = start_idx; i <= end_idx; ++i) {
                    ARROW_RETURN_NOT_OK(builder->Append(convert_datum(data[i], -1, -1)));
                }
            } else {
                const auto& data = data_column->get_data();
                [[maybe_unused]] int precision = -1;
                [[maybe_unused]] int scale = -1;
                if constexpr (lt_is_decimal<LT>) {
                    precision = data_column->precision();
                    scale = data_column->scale();
                }
                for (auto i = start_idx; i <= end_idx; ++i) {
                    ARROW_RETURN_NOT_OK(builder->Append(convert_datum(data[i], precision, scale)));
                }
            }
        }
        return arrow::Status::OK();
    }
};

static inline StarRocksToArrowConvertFunc resolve_convert_func(LogicalType lt, ArrowTypeId at, bool is_nullable);

DEF_PRED_GUARD(ConvArrayGuard, is_conv_array, LogicalType, LT, ArrowTypeId, AT)
#define IS_CONV_ARRAY_CTOR(LT, AT) DEF_PRED_CASE_CTOR(is_conv_array, LT, AT)
#define IS_CONV_ARRAY_R(AT, ...) DEF_BINARY_RELATION_ENTRY_SEP_SEMICOLON_R(IS_CONV_ARRAY_CTOR, AT, ##__VA_ARGS__)

IS_CONV_ARRAY_R(ArrowTypeId::LIST, TYPE_ARRAY)

template <LogicalType LT, ArrowTypeId AT, bool is_nullable>
struct ColumnToArrowConverter<LT, AT, is_nullable, ConvArrayGuard<LT, AT>> {
    static inline arrow::Status initialize_child_column_context(const ColumnPtr& column,
                                                                ColumnContext* column_context) {
        if (!column_context->child_columns.empty()) {
            return arrow::Status::OK();
        }

        bool child_is_nullable;
        if constexpr (is_nullable) {
            const auto* nullable_column = down_cast<NullableColumn*>(column.get());
            const auto* data_column = down_cast<ArrayColumn*>(nullable_column->data_column().get());
            child_is_nullable = data_column->elements_column()->is_nullable();
        } else {
            const auto* data_column = down_cast<ArrayColumn*>(column.get());
            child_is_nullable = data_column->elements_column()->is_nullable();
        }

        auto& child_type_desc = column_context->type_desc.children[0];
        auto& child_arrow_type = down_cast<arrow::ListType*>(column_context->arrow_type.get())->field(0)->type();
        auto func = resolve_convert_func(child_type_desc.type, child_arrow_type->id(), child_is_nullable);
        if (func == nullptr) {
            return arrow::Status::NotImplemented("");
        }

        column_context->child_columns.push_back({child_type_desc, child_arrow_type, func});
        return arrow::Status::OK();
    }

    static inline arrow::Status convert(const ColumnPtr& column, int start_idx, int end_idx,
                                        [[maybe_unused]] ColumnContext* column_context,
                                        arrow::ArrayBuilder* array_builder) {
        ARROW_RETURN_NOT_OK(initialize_child_column_context(column, column_context));
        arrow::ListBuilder* builder = down_cast<arrow::ListBuilder*>(array_builder);
        auto child_builder = builder->value_builder();
        auto& child_column_context = column_context->child_columns[0];
        if constexpr (is_nullable) {
            const auto* nullable_column = down_cast<NullableColumn*>(column.get());
            const auto* data_column = down_cast<ArrayColumn*>(nullable_column->data_column().get());
            auto& offsets = data_column->offsets().get_data();
            const auto& child_column = data_column->elements_column();
            for (auto i = start_idx; i <= end_idx; ++i) {
                if (nullable_column->is_null(i)) {
                    ARROW_RETURN_NOT_OK(builder->AppendNull());
                } else {
                    ARROW_RETURN_NOT_OK(builder->Append());
                    ARROW_RETURN_NOT_OK(child_column_context.convert_func(child_column, offsets[i], offsets[i + 1] - 1,
                                                                          &child_column_context, child_builder));
                }
            }
        } else {
            const auto* data_column = down_cast<ArrayColumn*>(column.get());
            auto& offsets = data_column->offsets().get_data();
            const auto& child_column = data_column->elements_column();
            for (auto i = start_idx; i <= end_idx; ++i) {
                ARROW_RETURN_NOT_OK(builder->Append());
                ARROW_RETURN_NOT_OK(child_column_context.convert_func(child_column, offsets[i], offsets[i + 1] - 1,
                                                                      &child_column_context, child_builder));
            }
        }
        return arrow::Status::OK();
    }
};

DEF_PRED_GUARD(ConvStructGuard, is_conv_struct, LogicalType, LT, ArrowTypeId, AT)
#define IS_CONV_STRUCT_CTOR(LT, AT) DEF_PRED_CASE_CTOR(is_conv_struct, LT, AT)
#define IS_CONV_STRUCT_R(AT, ...) DEF_BINARY_RELATION_ENTRY_SEP_SEMICOLON_R(IS_CONV_STRUCT_CTOR, AT, ##__VA_ARGS__)

IS_CONV_STRUCT_R(ArrowTypeId::STRUCT, TYPE_STRUCT)

template <LogicalType LT, ArrowTypeId AT, bool is_nullable>
struct ColumnToArrowConverter<LT, AT, is_nullable, ConvStructGuard<LT, AT>> {
    static inline arrow::Status initialize_child_column_context(const ColumnPtr& column,
                                                                ColumnContext* column_context) {
        if (!column_context->child_columns.empty()) {
            return arrow::Status::OK();
        }

        const StructColumn* data_column;
        if constexpr (is_nullable) {
            const auto* nullable_column = down_cast<NullableColumn*>(column.get());
            data_column = down_cast<StructColumn*>(nullable_column->data_column().get());
        } else {
            data_column = down_cast<StructColumn*>(column.get());
        }

        const auto& type_desc = column_context->type_desc;
        arrow::StructType* arrow_type = down_cast<arrow::StructType*>(column_context->arrow_type.get());
        for (int i = 0; i < type_desc.children.size(); i++) {
            auto& child_type_desc = type_desc.children[i];
            auto& child_arrow_type = arrow_type->field(i)->type();
            auto func = resolve_convert_func(child_type_desc.type, child_arrow_type->id(),
                                             data_column->fields()[i]->is_nullable());
            column_context->child_columns.push_back({child_type_desc, child_arrow_type, func});
        }
        return arrow::Status::OK();
    }

    static inline arrow::Status convert(const ColumnPtr& column, int start_idx, int end_idx,
                                        [[maybe_unused]] ColumnContext* column_context,
                                        arrow::ArrayBuilder* array_builder) {
        ARROW_RETURN_NOT_OK(initialize_child_column_context(column, column_context));
        arrow::StructBuilder* builder = down_cast<arrow::StructBuilder*>(array_builder);
        if constexpr (is_nullable) {
            const auto* nullable_column = down_cast<NullableColumn*>(column.get());
            const auto* data_column = down_cast<StructColumn*>(nullable_column->data_column().get());
            for (auto i = start_idx; i <= end_idx; ++i) {
                if (nullable_column->is_null(i)) {
                    ARROW_RETURN_NOT_OK(builder->AppendNull());
                } else {
                    ARROW_RETURN_NOT_OK(builder->Append());
                    for (int field = 0; field < data_column->fields().size(); field++) {
                        auto child_builder = builder->field_builder(field);
                        auto& child_column_context = column_context->child_columns[field];
                        ARROW_RETURN_NOT_OK(child_column_context.convert_func(data_column->fields()[field], i, i,
                                                                              &child_column_context, child_builder));
                    }
                }
            }
        } else {
            const auto* data_column = down_cast<StructColumn*>(column.get());
            for (auto i = start_idx; i <= end_idx; ++i) {
                ARROW_RETURN_NOT_OK(builder->Append());
                for (int field = 0; field < data_column->fields().size(); field++) {
                    auto child_builder = builder->field_builder(field);
                    auto& child_column_context = column_context->child_columns[field];
                    ARROW_RETURN_NOT_OK(child_column_context.convert_func(data_column->fields()[field], i, i,
                                                                          &child_column_context, child_builder));
                }
            }
        }
        return arrow::Status::OK();
    }
};

DEF_PRED_GUARD(ConvMapGuard, is_conv_map, LogicalType, LT, ArrowTypeId, AT)
#define IS_CONV_MAP_CTOR(LT, AT) DEF_PRED_CASE_CTOR(is_conv_map, LT, AT)
#define IS_CONV_MAP_R(AT, ...) DEF_BINARY_RELATION_ENTRY_SEP_SEMICOLON_R(IS_CONV_MAP_CTOR, AT, ##__VA_ARGS__)

IS_CONV_MAP_R(ArrowTypeId::MAP, TYPE_MAP)

template <LogicalType LT, ArrowTypeId AT, bool is_nullable>
struct ColumnToArrowConverter<LT, AT, is_nullable, ConvMapGuard<LT, AT>> {
    static inline arrow::Status initialize_child_column_context(const ColumnPtr& column,
                                                                ColumnContext* column_context) {
        if (!column_context->child_columns.empty()) {
            return arrow::Status::OK();
        }

        const MapColumn* data_column;
        if constexpr (is_nullable) {
            const auto* nullable_column = down_cast<NullableColumn*>(column.get());
            data_column = down_cast<MapColumn*>(nullable_column->data_column().get());
        } else {
            data_column = down_cast<MapColumn*>(column.get());
        }

        const auto& type_desc = column_context->type_desc;
        arrow::MapType* arrow_type = down_cast<arrow::MapType*>(column_context->arrow_type.get());

        auto& key_type_desc = type_desc.children[0];
        auto& key_arrow_type = arrow_type->field(0)->type();
        auto key_func = resolve_convert_func(key_type_desc.type, key_arrow_type->id(),
                                             data_column->keys_column()->is_nullable());
        column_context->child_columns.push_back({key_type_desc, key_arrow_type, key_func});

        auto& value_type_desc = type_desc.children[1];
        auto& value_arrow_type = arrow_type->field(1)->type();
        auto value_func = resolve_convert_func(value_type_desc.type, value_arrow_type->id(),
                                               data_column->values_column()->is_nullable());
        column_context->child_columns.push_back({value_type_desc, value_arrow_type, value_func});

        return arrow::Status::OK();
    }

    static inline arrow::Status convert(const ColumnPtr& column, int start_idx, int end_idx,
                                        [[maybe_unused]] ColumnContext* column_context,
                                        arrow::ArrayBuilder* array_builder) {
        ARROW_RETURN_NOT_OK(initialize_child_column_context(column, column_context));
        arrow::MapBuilder* builder = down_cast<arrow::MapBuilder*>(array_builder);
        auto& key_context = column_context->child_columns[0];
        auto key_convert_func = column_context->child_columns[0].convert_func;
        auto key_builder = builder->key_builder();
        auto& value_context = column_context->child_columns[1];
        auto value_convert_func = column_context->child_columns[1].convert_func;
        auto value_builder = builder->value_builder();
        if constexpr (is_nullable) {
            const auto* nullable_column = down_cast<NullableColumn*>(column.get());
            const auto* data_column = down_cast<MapColumn*>(nullable_column->data_column().get());
            const auto& key_column = data_column->keys_column();
            const auto& value_column = data_column->values_column();
            auto& offsets = data_column->offsets().get_data();
            for (auto i = start_idx; i <= end_idx; ++i) {
                if (nullable_column->is_null(i)) {
                    ARROW_RETURN_NOT_OK(builder->AppendNull());
                } else {
                    ARROW_RETURN_NOT_OK(builder->Append());
                    ARROW_RETURN_NOT_OK(
                            key_convert_func(key_column, offsets[i], offsets[i + 1] - 1, &key_context, key_builder));
                    ARROW_RETURN_NOT_OK(value_convert_func(value_column, offsets[i], offsets[i + 1] - 1, &value_context,
                                                           value_builder));
                }
            }
        } else {
            const auto* data_column = down_cast<MapColumn*>(column.get());
            const auto& key_column = data_column->keys_column();
            const auto& value_column = data_column->values_column();
            auto& offsets = data_column->offsets().get_data();
            for (auto i = start_idx; i <= end_idx; ++i) {
                ARROW_RETURN_NOT_OK(builder->Append());
                ARROW_RETURN_NOT_OK(
                        key_convert_func(key_column, offsets[i], offsets[i + 1] - 1, &key_context, key_builder));
                ARROW_RETURN_NOT_OK(value_convert_func(value_column, offsets[i], offsets[i + 1] - 1, &value_context,
                                                       value_builder));
            }
        }
        return arrow::Status::OK();
    }
};

constexpr int32_t starrocks_to_arrow_convert_idx(LogicalType lt, ArrowTypeId at, bool is_nullable) {
    return (at << 17) | (lt << 2) | (is_nullable ? 2 : 0);
}

#define STARROCKS_TO_ARROW_CONV_SINGLE_ENTRY_CTOR(lt, at, is_nullable) \
    { starrocks_to_arrow_convert_idx(lt, at, is_nullable), &ColumnToArrowConverter<lt, at, is_nullable>::convert }

#define STARROCKS_TO_ARROW_CONV_ENTRY_CTOR(lt, at) \
    STARROCKS_TO_ARROW_CONV_SINGLE_ENTRY_CTOR(lt, at, false), STARROCKS_TO_ARROW_CONV_SINGLE_ENTRY_CTOR(lt, at, true)

#define STARROCKS_TO_ARROW_CONV_ENTRY_R(at, ...) \
    DEF_BINARY_RELATION_ENTRY_SEP_COMMA_R(STARROCKS_TO_ARROW_CONV_ENTRY_CTOR, at, ##__VA_ARGS__)

static const std::unordered_map<int32_t, StarRocksToArrowConvertFunc> global_starrocks_to_arrow_conv_table{
        STARROCKS_TO_ARROW_CONV_ENTRY_R(ArrowTypeId::BOOL, TYPE_BOOLEAN),
        STARROCKS_TO_ARROW_CONV_ENTRY_R(ArrowTypeId::INT8, TYPE_TINYINT),
        STARROCKS_TO_ARROW_CONV_ENTRY_R(ArrowTypeId::INT16, TYPE_SMALLINT),
        STARROCKS_TO_ARROW_CONV_ENTRY_R(ArrowTypeId::INT32, TYPE_INT),
        STARROCKS_TO_ARROW_CONV_ENTRY_R(ArrowTypeId::INT64, TYPE_BIGINT),
        STARROCKS_TO_ARROW_CONV_ENTRY_R(ArrowTypeId::FLOAT, TYPE_FLOAT),
        STARROCKS_TO_ARROW_CONV_ENTRY_R(ArrowTypeId::DOUBLE, TYPE_DOUBLE, TYPE_TIME),
        STARROCKS_TO_ARROW_CONV_ENTRY_R(ArrowTypeId::DECIMAL, TYPE_DECIMALV2),
        STARROCKS_TO_ARROW_CONV_ENTRY_R(ArrowTypeId::DECIMAL, TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128),
        STARROCKS_TO_ARROW_CONV_ENTRY_R(ArrowTypeId::STRING, TYPE_VARCHAR, TYPE_CHAR, TYPE_HLL),
        STARROCKS_TO_ARROW_CONV_ENTRY_R(ArrowTypeId::STRING, TYPE_LARGEINT, TYPE_DATE, TYPE_DATETIME),
        STARROCKS_TO_ARROW_CONV_ENTRY_R(ArrowTypeId::STRING, TYPE_JSON),
        STARROCKS_TO_ARROW_CONV_ENTRY_R(ArrowTypeId::LIST, TYPE_ARRAY),
        STARROCKS_TO_ARROW_CONV_ENTRY_R(ArrowTypeId::STRUCT, TYPE_STRUCT),
        STARROCKS_TO_ARROW_CONV_ENTRY_R(ArrowTypeId::MAP, TYPE_MAP)};

static inline StarRocksToArrowConvertFunc resolve_convert_func(LogicalType lt, ArrowTypeId at, bool is_nullable) {
    const auto func_id = starrocks_to_arrow_convert_idx(lt, at, is_nullable);
    const auto end = global_starrocks_to_arrow_conv_table.end();
    auto it = global_starrocks_to_arrow_conv_table.find(func_id);
    return it != end ? it->second : nullptr;
}

class ColumnToArrowArrayConverter : public arrow::TypeVisitor {
public:
    ColumnToArrowArrayConverter(const ColumnPtr& column, arrow::MemoryPool* pool, const TypeDescriptor& type_desc,
                                const std::shared_ptr<arrow::DataType>& arrow_type,
                                std::shared_ptr<arrow::Array>& array)
            : _column(column), _pool(pool), _type_desc(type_desc), _arrow_type(arrow_type), _array(array) {}

    using arrow::TypeVisitor::Visit;

#define DEF_VISIT_METHOD(Type)                                                                      \
    arrow::Status Visit(const arrow::Type& type) override {                                         \
        auto func = resolve_convert_func(_type_desc.type, type.type_id, _column->is_nullable());    \
        if (func == nullptr) {                                                                      \
            return arrow::Status::NotImplemented("");                                               \
        }                                                                                           \
        ColumnContext column_context(_type_desc, _arrow_type, func);                                \
        std::unique_ptr<arrow::ArrayBuilder> builder;                                               \
        ARROW_RETURN_NOT_OK(arrow::MakeBuilder(_pool, _arrow_type, &builder));                      \
        ARROW_RETURN_NOT_OK(func(_column, 0, _column->size() - 1, &column_context, builder.get())); \
        return builder->Finish(&_array);                                                            \
    }

    DEF_VISIT_METHOD(Decimal128Type);
    DEF_VISIT_METHOD(DoubleType);
    DEF_VISIT_METHOD(FloatType);
    DEF_VISIT_METHOD(BooleanType);
    DEF_VISIT_METHOD(Int8Type);
    DEF_VISIT_METHOD(Int16Type);
    DEF_VISIT_METHOD(Int32Type);
    DEF_VISIT_METHOD(Int64Type);
    DEF_VISIT_METHOD(StringType);
    DEF_VISIT_METHOD(ListType);
    DEF_VISIT_METHOD(StructType);
    DEF_VISIT_METHOD(MapType);

#undef DEF_VISIT_METHOD

private:
    const ColumnPtr& _column;
    arrow::MemoryPool* _pool;
    const TypeDescriptor& _type_desc;
    const std::shared_ptr<arrow::DataType>& _arrow_type;
    std::shared_ptr<arrow::Array>& _array;
}; // namespace starrocks

Status convert_chunk_to_arrow_batch(Chunk* chunk, std::vector<ExprContext*>& _output_expr_ctxs,
                                    const std::shared_ptr<arrow::Schema>& schema, arrow::MemoryPool* pool,
                                    std::shared_ptr<arrow::RecordBatch>* result) {
    if (chunk->num_columns() != schema->num_fields()) {
        return Status::InvalidArgument("number fields not match");
    }

    int result_num_column = _output_expr_ctxs.size();
    std::vector<std::shared_ptr<arrow::Array>> arrays(result_num_column);

    size_t num_rows = chunk->num_rows();
    for (auto i = 0; i < result_num_column; ++i) {
        ASSIGN_OR_RETURN(ColumnPtr column, _output_expr_ctxs[i]->evaluate(chunk))
        Expr* expr = _output_expr_ctxs[i]->root();
        if (column->is_constant()) {
            // Don't modify the column of src chunk, otherwise the memory statistics of query is invalid.
            column = ColumnHelper::copy_and_unfold_const_column(expr->type(), column->is_nullable(), column, num_rows);
        }
        auto& array = arrays[i];
        DCHECK(!column->is_constant() && !column->only_null());
        ColumnToArrowArrayConverter converter(column, pool, expr->type(), schema->field(i)->type(), array);
        auto arrow_st = arrow::VisitTypeInline(*schema->field(i)->type(), &converter);
        if (!arrow_st.ok()) {
            return Status::InvalidArgument(arrow_st.ToString());
        }
    }
    *result = arrow::RecordBatch::Make(schema, num_rows, std::move(arrays));
    return Status::OK();
}

// only used for UT test
Status convert_chunk_to_arrow_batch(Chunk* chunk, const std::vector<const TypeDescriptor*>& slot_types,
                                    const std::vector<SlotId>& slot_ids, const std::shared_ptr<arrow::Schema>& schema,
                                    arrow::MemoryPool* pool, std::shared_ptr<arrow::RecordBatch>* result) {
    if (chunk->num_columns() != schema->num_fields()) {
        return Status::InvalidArgument("number fields not match");
    }

    std::vector<std::shared_ptr<arrow::Array>> arrays(slot_types.size());

    size_t num_rows = chunk->num_rows();
    for (auto i = 0; i < slot_types.size(); ++i) {
        auto column = chunk->get_column_by_slot_id(slot_ids[i]);
        if (column->is_constant()) {
            // Don't modify the column of src chunk, otherwise the memory statistics of query is invalid.
            column =
                    ColumnHelper::copy_and_unfold_const_column(*slot_types[i], column->is_nullable(), column, num_rows);
        }
        auto& array = arrays[i];
        DCHECK(!column->is_constant() && !column->only_null());
        ColumnToArrowArrayConverter converter(column, pool, *slot_types[i], schema->field(i)->type(), array);
        auto arrow_st = arrow::VisitTypeInline(*schema->field(i)->type(), &converter);
        if (!arrow_st.ok()) {
            return Status::InvalidArgument(arrow_st.ToString());
        }
    }
    *result = arrow::RecordBatch::Make(schema, num_rows, std::move(arrays));
    return Status::OK();
}
} // namespace starrocks
