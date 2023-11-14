// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/binary_predicate.h"

#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "column/type_traits.h"
#include "exprs/vectorized/binary_function.h"
#include "runtime/primitive_type.h"
#include "runtime/primitive_type_infra.h"
#include "storage/vectorized_column_predicate.h"

namespace starrocks::vectorized {

template <PrimitiveType ptype>
struct PredicateCmpType {
    using CmpType = RunTimeCppType<ptype>;
};

template <>
struct PredicateCmpType<TYPE_JSON> {
    using CmpType = JsonValue;
};

// The evaluator for PrimitiveType
template <PrimitiveType ptype>
using EvalEq = std::equal_to<typename PredicateCmpType<ptype>::CmpType>;
template <PrimitiveType ptype>
using EvalNe = std::not_equal_to<typename PredicateCmpType<ptype>::CmpType>;
template <PrimitiveType ptype>
using EvalLt = std::less<typename PredicateCmpType<ptype>::CmpType>;
template <PrimitiveType ptype>
using EvalLe = std::less_equal<typename PredicateCmpType<ptype>::CmpType>;
template <PrimitiveType ptype>
using EvalGt = std::greater<typename PredicateCmpType<ptype>::CmpType>;
template <PrimitiveType ptype>
using EvalGe = std::greater_equal<typename PredicateCmpType<ptype>::CmpType>;

// A wrapper for evaluator, to fit in the Expression framework
template <typename CMP>
struct BinaryPredFunc {
    template <typename LType, typename RType, typename ResultType>
    static inline ResultType apply(const LType& l, const RType& r) {
        return CMP()(l, r);
    }
};

template <PrimitiveType Type, typename OP>
class VectorizedBinaryPredicate final : public Predicate {
public:
    explicit VectorizedBinaryPredicate(const TExprNode& node) : Predicate(node) {}
    ~VectorizedBinaryPredicate() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedBinaryPredicate(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override {
        ASSIGN_OR_RETURN(auto l, _children[0]->evaluate_checked(context, ptr));
        ASSIGN_OR_RETURN(auto r, _children[1]->evaluate_checked(context, ptr));
        return VectorizedStrictBinaryFunction<OP>::template evaluate<Type, TYPE_BOOLEAN>(l, r);
    }
};

template <PrimitiveType Type, typename OP>
class VectorizedNullSafeEqPredicate final : public Predicate {
public:
    explicit VectorizedNullSafeEqPredicate(const TExprNode& node) : Predicate(node) {}
    ~VectorizedNullSafeEqPredicate() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedNullSafeEqPredicate(*this)); }

    // if v1 null and v2 null = true
    // if v1 null and v2 not null = false
    // if v1 not null and v2 null = false
    // if v1 not null and v2 not null = v1 OP v2
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override {
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
    template <PrimitiveType data_type>
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
    PrimitiveType type = thrift_to_type(node.child_type);

    return type_dispatch_predicate<Expr*>(type, true, BinaryPredicateBuilder(), node);
}

} // namespace starrocks::vectorized
