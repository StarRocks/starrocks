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

#include "exprs/literal.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exprs/jit/ir_helper.h"
#include "gutil/port.h"
#include "gutil/strings/fastmem.h"
#include "types/constexpr.h"

namespace starrocks {

#define CASE_TYPE_COLUMN(NODE_TYPE, CHECK_TYPE, LITERAL_VALUE)                              \
    case NODE_TYPE: {                                                                       \
        DCHECK_EQ(node.node_type, TExprNodeType::CHECK_TYPE);                               \
        DCHECK(node.__isset.LITERAL_VALUE);                                                 \
        _value = ColumnHelper::create_const_column<NODE_TYPE>(node.LITERAL_VALUE.value, 1); \
        break;                                                                              \
    }

template <LogicalType LT>
static RunTimeCppType<LT> unpack_decimal(const std::string& s) {
    static_assert(lt_is_decimal<LT>);
    RunTimeCppType<LT> value;
#ifdef IS_LITTLE_ENDIAN
    strings::memcpy_inlined(&value, &s.front(), sizeof(value));
#else
    std::copy(s.rbegin(), s.rend(), (char*)&value);
#endif
    return value;
}

template <LogicalType DecimalType, typename = DecimalLTGuard<DecimalType>>
static ColumnPtr const_column_from_literal(const TExprNode& node, int precision, int scale) {
    using CppType = RunTimeCppType<DecimalType>;
    using ColumnType = RunTimeColumnType<DecimalType>;
    CppType datum;
    DCHECK(node.__isset.decimal_literal);
    // using TDecimalLiteral::integer_value take precedence over using TDecimalLiteral::value
    if (node.decimal_literal.__isset.integer_value) {
        const std::string& s = node.decimal_literal.integer_value;
        datum = unpack_decimal<DecimalType>(s);
        return ColumnHelper::create_const_decimal_column<DecimalType>(datum, precision, scale, 1);
    }
    auto& literal_value = node.decimal_literal.value;
    auto fail =
            DecimalV3Cast::from_string<CppType>(&datum, precision, scale, literal_value.c_str(), literal_value.size());
    if (fail) {
        return ColumnHelper::create_const_null_column(1);
    } else {
        return ColumnHelper::create_const_decimal_column<DecimalType>(datum, precision, scale, 1);
    }
}

VectorizedLiteral::VectorizedLiteral(const TExprNode& node) : Expr(node) {
    if (node.node_type == TExprNodeType::NULL_LITERAL) {
        _value = ColumnHelper::create_const_null_column(1);
        return;
    }

    switch (_type.type) {
        CASE_TYPE_COLUMN(TYPE_BOOLEAN, BOOL_LITERAL, bool_literal)
        CASE_TYPE_COLUMN(TYPE_TINYINT, INT_LITERAL, int_literal);
        CASE_TYPE_COLUMN(TYPE_SMALLINT, INT_LITERAL, int_literal);
        CASE_TYPE_COLUMN(TYPE_INT, INT_LITERAL, int_literal);
        CASE_TYPE_COLUMN(TYPE_BIGINT, INT_LITERAL, int_literal);
        CASE_TYPE_COLUMN(TYPE_FLOAT, FLOAT_LITERAL, float_literal);
        CASE_TYPE_COLUMN(TYPE_DOUBLE, FLOAT_LITERAL, float_literal);
    case TYPE_LARGEINT: {
        DCHECK_EQ(node.node_type, TExprNodeType::LARGE_INT_LITERAL);

        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        auto data = StringParser::string_to_int<__int128>(node.large_int_literal.value.c_str(),
                                                          node.large_int_literal.value.size(), &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            data = MAX_INT128;
        }
        _value = ColumnHelper::create_const_column<TYPE_LARGEINT>(data, 1);
        break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        // @IMPORTANT: build slice though get_data, else maybe will cause multi-thread crash in scanner
        _value = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(node.string_literal.value), 1);
        break;
    }
    case TYPE_TIME: {
        _value = ColumnHelper::create_const_column<TYPE_TIME>(node.float_literal.value, 1);
        break;
    }
    case TYPE_DATE: {
        DateValue v;
        if (v.from_string(node.date_literal.value.c_str(), node.date_literal.value.size())) {
            _value = ColumnHelper::create_const_column<TYPE_DATE>(v, 1);
        } else {
            _value = ColumnHelper::create_const_null_column(1);
        }
        break;
    }
    case TYPE_DATETIME: {
        TimestampValue v;
        if (v.from_string(node.date_literal.value.c_str(), node.date_literal.value.size())) {
            _value = ColumnHelper::create_const_column<TYPE_DATETIME>(v, 1);
        } else {
            _value = ColumnHelper::create_const_null_column(1);
        }
        break;
    }
    case TYPE_DECIMALV2: {
        _value = ColumnHelper::create_const_column<TYPE_DECIMALV2>(DecimalV2Value(node.decimal_literal.value), 1);
        break;
    }
    case TYPE_DECIMAL32: {
        _value = const_column_from_literal<TYPE_DECIMAL32>(node, this->type().precision, this->type().scale);
        break;
    }
    case TYPE_DECIMAL64: {
        _value = const_column_from_literal<TYPE_DECIMAL64>(node, this->type().precision, this->type().scale);
        break;
    }
    case TYPE_DECIMAL128: {
        _value = const_column_from_literal<TYPE_DECIMAL128>(node, this->type().precision, this->type().scale);
        break;
    }
    case TYPE_VARBINARY: {
        // @IMPORTANT: build slice though get_data, else maybe will cause multi-thread crash in scanner
        _value = ColumnHelper::create_const_column<TYPE_VARBINARY>(Slice(node.binary_literal.value), 1);
        break;
    }
    default:
        DCHECK(false) << "Vectorized engine not implement type: " << _type.type;
        break;
    }
}

VectorizedLiteral::VectorizedLiteral(ColumnPtr&& value, const TypeDescriptor& type)
        : Expr(type, false), _value(std::move(value)) {
    DCHECK(_value->is_constant());
}

#undef CASE_TYPE_COLUMN

StatusOr<ColumnPtr> VectorizedLiteral::evaluate_checked(ExprContext* context, Chunk* ptr) {
    ColumnPtr column = _value->clone_empty();
    column->append(*_value, 0, 1);
    if (ptr != nullptr) {
        column->resize(ptr->num_rows());
    }
    return column;
}

bool VectorizedLiteral::is_compilable() const {
    return IRHelper::support_jit(_type.type);
}

std::string VectorizedLiteral::jit_func_name() const {
    return "{" + type().debug_string() + "[" + _value->debug_string() + "]}";
}

StatusOr<LLVMDatum> VectorizedLiteral::generate_ir_impl(ExprContext* context, JITContext* jit_ctx) {
    LLVMDatum datum(jit_ctx->builder, _value->only_null());
    ASSIGN_OR_RETURN(datum.value, IRHelper::create_ir_number(jit_ctx->builder, _type.type, _value->raw_data()));
    return datum;
}


std::string VectorizedLiteral::debug_string() const {
    std::stringstream out;
    out << "Literal("
        << "type=" << this->type().debug_string() << ", value=" << _value->debug_string() << ")";
    return out.str();
}

VectorizedLiteral::~VectorizedLiteral() = default;

} // namespace starrocks
