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

#include "exprs/binary_predicate.h"

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "column/type_traits.h"
#include "exprs/binary_function.h"
#include "storage/column_predicate.h"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"

namespace starrocks {

template <LogicalType ltype>
struct PredicateCmpType {
    using CmpType = RunTimeCppType<ltype>;
};

template <>
struct PredicateCmpType<TYPE_JSON> {
    using CmpType = JsonValue;
};

// The evaluator for LogicalType
template <LogicalType ltype>
using EvalEq = std::equal_to<typename PredicateCmpType<ltype>::CmpType>;
template <LogicalType ltype>
using EvalNe = std::not_equal_to<typename PredicateCmpType<ltype>::CmpType>;
template <LogicalType ltype>
using EvalLt = std::less<typename PredicateCmpType<ltype>::CmpType>;
template <LogicalType ltype>
using EvalLe = std::less_equal<typename PredicateCmpType<ltype>::CmpType>;
template <LogicalType ltype>
using EvalGt = std::greater<typename PredicateCmpType<ltype>::CmpType>;
template <LogicalType ltype>
using EvalGe = std::greater_equal<typename PredicateCmpType<ltype>::CmpType>;

struct EvalCmpZero {
    TExprOpcode::type op;

    EvalCmpZero(TExprOpcode::type in_op) : op(in_op) {}

    void eval(const std::vector<int8_t>& cmp_values, ColumnBuilder<TYPE_BOOLEAN>* output) {
        auto cmp = build_comparator();
        for (int8_t x : cmp_values) {
            output->append(cmp(x));
        }
    }

    std::function<bool(int)> build_comparator() {
        switch (op) {
        case TExprOpcode::EQ:
            return [](int x) { return x == 0; };
        case TExprOpcode::NE:
            return [](int x) { return x != 0; };
        case TExprOpcode::LE:
            return [](int x) { return x <= 0; };
        case TExprOpcode::LT:
            return [](int x) { return x < 0; };
        case TExprOpcode::GE:
            return [](int x) { return x >= 0; };
        case TExprOpcode::GT:
            return [](int x) { return x > 0; };
        case TExprOpcode::EQ_FOR_NULL:
            return [](int x) { return x == 0; };
        default:
            CHECK(false) << "illegal operation: " << op;
        }
    }
};

// A wrapper for evaluator, to fit in the Expression framework
template <typename CMP>
struct BinaryPredFunc {
    template <typename LType, typename RType, typename ResultType>
    static inline ResultType apply(const LType& l, const RType& r) {
        return CMP()(l, r);
    }
};

template <LogicalType Type, typename OP>
class VectorizedBinaryPredicate final : public Predicate {
public:
    explicit VectorizedBinaryPredicate(const TExprNode& node) : Predicate(node) {}
    ~VectorizedBinaryPredicate() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedBinaryPredicate(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {
        ASSIGN_OR_RETURN(auto l, _children[0]->evaluate_checked(context, ptr));
        ASSIGN_OR_RETURN(auto r, _children[1]->evaluate_checked(context, ptr));
        return VectorizedStrictBinaryFunction<OP>::template evaluate<Type, TYPE_BOOLEAN>(l, r);
    }
};

class ArrayPredicate final : public Predicate {
public:
    explicit ArrayPredicate(const TExprNode& node) : Predicate(node), _comparator(node.opcode) {}
    ~ArrayPredicate() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new ArrayPredicate(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {
        ASSIGN_OR_RETURN(auto l, _children[0]->evaluate_checked(context, ptr));
        ASSIGN_OR_RETURN(auto r, _children[1]->evaluate_checked(context, ptr));
        auto lhs_arr = std::static_pointer_cast<ArrayColumn>(l);
        auto rhs_arr = std::static_pointer_cast<ArrayColumn>(r);

        ColumnBuilder<TYPE_BOOLEAN> builder(ptr->num_rows());
        std::vector<int8_t> cmp_result;
        lhs_arr->compare_column(*rhs_arr, &cmp_result);

        // Convert the compare result (-1, 0, 1) to the predicate result (true/false)
        _comparator.eval(cmp_result, &builder);

        return builder.build(ColumnHelper::is_all_const(ptr->columns()));
    }

private:
    EvalCmpZero _comparator;
};

template <LogicalType Type, typename OP>
class VectorizedNullSafeEqPredicate final : public Predicate {
public:
    explicit VectorizedNullSafeEqPredicate(const TExprNode& node) : Predicate(node) {}
    ~VectorizedNullSafeEqPredicate() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedNullSafeEqPredicate(*this)); }

    // if v1 null and v2 null = true
    // if v1 null and v2 not null = false
    // if v1 not null and v2 null = false
    // if v1 not null and v2 not null = v1 OP v2
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {
        ASSIGN_OR_RETURN(auto l, _children[0]->evaluate_checked(context, ptr));
        ASSIGN_OR_RETURN(auto r, _children[1]->evaluate_checked(context, ptr));

        ColumnViewer<Type> v1(l);
        ColumnViewer<Type> v2(r);
        Columns list = {l, r};

        size_t size = list[0]->size();
        ColumnBuilder<TYPE_BOOLEAN> builder(size);
        for (int row = 0; row < size; ++row) {
            auto null1 = v1.is_null(row);
            auto null2 = v2.is_null(row);

            if (null1 & null2) {
                // all null = true
                builder.append(true);
            } else if (null1 ^ null2) {
                // one null = false
                builder.append(false);
            } else {
                // all not null = value eq
                using CppType = RunTimeCppType<Type>;
                builder.append(OP::template apply<CppType, CppType, bool>(v1.value(row), v2.value(row)));
            }
        }

        return builder.build(ColumnHelper::is_all_const(list));
    }
};

struct BinaryPredicateBuilder {
    template <LogicalType data_type>
    Expr* operator()(const TExprNode& node) {
        switch (node.opcode) {
        case TExprOpcode::EQ:
            return new VectorizedBinaryPredicate<data_type, BinaryPredFunc<EvalEq<data_type>>>(node);
        case TExprOpcode::NE:
            return new VectorizedBinaryPredicate<data_type, BinaryPredFunc<EvalNe<data_type>>>(node);
        case TExprOpcode::LT:
            return new VectorizedBinaryPredicate<data_type, BinaryPredFunc<EvalLt<data_type>>>(node);
        case TExprOpcode::LE:
            return new VectorizedBinaryPredicate<data_type, BinaryPredFunc<EvalLe<data_type>>>(node);
        case TExprOpcode::GT:
            return new VectorizedBinaryPredicate<data_type, BinaryPredFunc<EvalGt<data_type>>>(node);
        case TExprOpcode::GE:
            return new VectorizedBinaryPredicate<data_type, BinaryPredFunc<EvalGe<data_type>>>(node);
        case TExprOpcode::EQ_FOR_NULL:
            return new VectorizedNullSafeEqPredicate<data_type, BinaryPredFunc<EvalEq<data_type>>>(node);
        default:
            break;
        }
        return nullptr;
    }
};

Expr* VectorizedBinaryPredicateFactory::from_thrift(const TExprNode& node) {
    LogicalType type;
    if (node.__isset.child_type_desc) {
        type = TypeDescriptor::from_thrift(node.child_type_desc).type;
    } else {
        type = thrift_to_type(node.child_type);
    }

    if (type == TYPE_ARRAY) {
        return new ArrayPredicate(node);
    } else {
        return type_dispatch_predicate<Expr*>(type, true, BinaryPredicateBuilder(), node);
    }
}

} // namespace starrocks
