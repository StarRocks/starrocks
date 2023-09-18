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

#include "column/column_helper.h"
#include "column/type_traits.h"
#include "common/statusor.h"
#include "exec/arrow_type_traits.h"
#include "exprs/expr.h"
#include "runtime/large_int_value.h"
#include "util/raw_container.h"

namespace starrocks {

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
    static inline arrow::Status convert(const ColumnPtr& column, arrow::MemoryPool* pool,
                                        std::shared_ptr<arrow::Array>& array) {
        DCHECK(!column->is_constant() && !column->only_null());
        ArrowBuilderType builder(pool);
        if constexpr (is_nullable) {
            const auto* nullable_column = down_cast<NullableColumn*>(column.get());
            const auto* data_column = down_cast<StarRocksColumnType*>(nullable_column->data_column().get());
            const auto* null_column = down_cast<NullColumn*>(nullable_column->null_column().get());
            const auto& data = data_column->get_data();
            const auto num_rows = null_column->size();
            for (auto i = 0; i < num_rows; ++i) {
                if (nullable_column->is_null(i)) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                } else {
                    ARROW_RETURN_NOT_OK(builder.Append(data[i]));
                }
            }
        } else {
            const auto* data_column = down_cast<StarRocksColumnType*>(column.get());
            const auto& data = data_column->get_data();
            ARROW_RETURN_NOT_OK(builder.AppendValues(data));
        }
        return builder.Finish(&array);
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

    static inline arrow::Status convert(const ColumnPtr& column, arrow::MemoryPool* pool,
                                        std::shared_ptr<arrow::Array>& array) {
        DCHECK(!column->is_constant() && !column->only_null());
        std::unique_ptr<ArrowBuilderType> builder;
        if constexpr (is_nullable) {
            const auto* nullable_column = down_cast<NullableColumn*>(column.get());
            const auto* data_column = down_cast<StarRocksColumnType*>(nullable_column->data_column().get());
            const auto* null_column = down_cast<NullColumn*>(nullable_column->null_column().get());
            const auto& data = data_column->get_data();
            const auto num_rows = null_column->size();

            if constexpr (lt_is_decimalv2<LT>) {
                auto arrow_type = std::make_shared<ArrowType>(27, 9);
                builder = std::make_unique<ArrowBuilderType>(std::move(arrow_type), pool);
            } else if constexpr (lt_is_decimal<LT>) {
                auto arrow_type = std::make_shared<ArrowType>(data_column->precision(), data_column->scale());
                builder = std::make_unique<ArrowBuilderType>(std::move(arrow_type), pool);
            } else {
                static_assert(lt_is_decimalv2<LT> || lt_is_decimal<LT>, "Illegal LogicalType");
            }
            for (auto i = 0; i < num_rows; ++i) {
                if (nullable_column->is_null(i)) {
                    ARROW_RETURN_NOT_OK(builder->AppendNull());
                } else {
                    ARROW_RETURN_NOT_OK(builder->Append(convert_datum(data[i])));
                }
            }
        } else {
            const auto* data_column = down_cast<StarRocksColumnType*>(column.get());
            if constexpr (lt_is_decimalv2<LT>) {
                auto arrow_type = std::make_shared<ArrowType>(27, 9);
                builder = std::make_unique<ArrowBuilderType>(std::move(arrow_type), pool);
            } else if constexpr (lt_is_decimal<LT>) {
                auto arrow_type = std::make_shared<ArrowType>(data_column->precision(), data_column->scale());
                builder = std::make_unique<ArrowBuilderType>(std::move(arrow_type), pool);
            } else {
                static_assert(lt_is_decimalv2<LT> || lt_is_decimal<LT>, "Illegal LogicalType");
            }
            const auto& data = data_column->get_data();
            const auto num_rows = column->size();
            for (auto i = 0; i < num_rows; ++i) {
                ARROW_RETURN_NOT_OK(builder->Append(convert_datum(data[i])));
            }
        }
        return builder->Finish(&array);
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

    static inline arrow::Status convert(const ColumnPtr& column, arrow::MemoryPool* pool,
                                        std::shared_ptr<arrow::Array>& array) {
        DCHECK(!column->is_constant() && !column->only_null());
        std::unique_ptr<ArrowBuilderType> builder = std::make_unique<ArrowBuilderType>(pool);
        if constexpr (is_nullable) {
            const auto* nullable_column = down_cast<NullableColumn*>(column.get());
            const auto* data_column = down_cast<StarRocksColumnType*>(nullable_column->data_column().get());
            const auto* null_column = down_cast<NullColumn*>(nullable_column->null_column().get());
            const auto num_rows = null_column->size();
            if constexpr (lt_is_string<LT>) {
                const auto& data = data_column->get_proxy_data();
                for (auto i = 0; i < num_rows; ++i) {
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
                for (auto i = 0; i < num_rows; ++i) {
                    if (nullable_column->is_null(i)) {
                        ARROW_RETURN_NOT_OK(builder->AppendNull());
                    } else {
                        ARROW_RETURN_NOT_OK(builder->Append(convert_datum(data[i], precision, scale)));
                    }
                }
            }
        } else {
            const auto* data_column = down_cast<StarRocksColumnType*>(column.get());
            const auto num_rows = column->size();
            if constexpr (lt_is_string<LT>) {
                const auto& data = data_column->get_proxy_data();
                for (auto i = 0; i < num_rows; ++i) {
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
                for (auto i = 0; i < num_rows; ++i) {
                    ARROW_RETURN_NOT_OK(builder->Append(convert_datum(data[i], precision, scale)));
                }
            }
        }
        return builder->Finish(&array);
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
typedef arrow::Status (*StarRocksToArrowConvertFunc)(const ColumnPtr&, arrow::MemoryPool*,
                                                     std::shared_ptr<arrow::Array>&);

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
};
static inline StarRocksToArrowConvertFunc resolve_convert_func(LogicalType lt, ArrowTypeId at, bool is_nullable) {
    const auto func_id = starrocks_to_arrow_convert_idx(lt, at, is_nullable);
    const auto end = global_starrocks_to_arrow_conv_table.end();
    auto it = global_starrocks_to_arrow_conv_table.find(func_id);
    return it != end ? it->second : nullptr;
}

class ColumnToArrowArrayConverter : public arrow::TypeVisitor {
public:
    ColumnToArrowArrayConverter(const ColumnPtr& column, arrow::MemoryPool* pool, LogicalType lt,
                                std::shared_ptr<arrow::Array>& array)
            : _column(column), _pool(pool), _pt(lt), _array(array) {}

    using arrow::TypeVisitor::Visit;

#define DEF_VISIT_METHOD(Type)                                                       \
    arrow::Status Visit(const arrow::Type& type) override {                          \
        auto func = resolve_convert_func(_pt, type.type_id, _column->is_nullable()); \
        if (func == nullptr) {                                                       \
            return arrow::Status::NotImplemented("");                                \
        }                                                                            \
        return func(_column, _pool, _array);                                         \
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

#undef DEF_VISIT_METHOD

private:
    const ColumnPtr& _column;
    arrow::MemoryPool* _pool;
    LogicalType _pt;
    std::shared_ptr<arrow::Array>& _array;
};

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
        ColumnToArrowArrayConverter converter(column, pool, expr->type().type, array);
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
        ColumnToArrowArrayConverter converter(column, pool, slot_types[i]->type, array);
        auto arrow_st = arrow::VisitTypeInline(*schema->field(i)->type(), &converter);
        if (!arrow_st.ok()) {
            return Status::InvalidArgument(arrow_st.ToString());
        }
    }
    *result = arrow::RecordBatch::Make(schema, num_rows, std::move(arrays));
    return Status::OK();
}
} // namespace starrocks
