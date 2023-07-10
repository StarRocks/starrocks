// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/arithmetic_expr.h"

#include <optional>

#include "common/object_pool.h"
#include "common/statusor.h"
#include "exprs/vectorized/arithmetic_operation.h"
#include "exprs/vectorized/binary_function.h"
#include "exprs/vectorized/decimal_binary_function.h"
#include "exprs/vectorized/decimal_cast_expr.h"
#include "exprs/vectorized/unary_function.h"
#include "runtime/decimalv3.h"
#include "util/pred_guard.h"

namespace starrocks::vectorized {

#define DEFINE_CLASS_CONSTRUCTOR(CLASS_NAME)          \
    CLASS_NAME(const TExprNode& node) : Expr(node) {} \
    virtual ~CLASS_NAME() {}                          \
                                                      \
    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new CLASS_NAME(*this)); }

static std::optional<PrimitiveType> eliminate_trivial_cast_for_decimal_mul(const Expr* e) {
    DIAGNOSTIC_PUSH
#if defined(__GNUC__) && !defined(__clang__)
    DIAGNOSTIC_IGNORE("-Wmaybe-uninitialized")
#endif
    if (!e->is_cast_expr()) {
        return {};
    }
    const auto* e_child = e->get_child(0);
    const auto& e_type = e->type();
    const auto& e_child_type = e_child->type();
    if (e_type.is_decimalv3_type() && e_child_type.is_decimalv3_type() && e_type.scale == e_child_type.scale) {
        return {e_child->type().type};
    } else {
        return {};
    }
    DIAGNOSTIC_POP
}

template <PrimitiveType Type, typename OP>
class VectorizedArithmeticExpr final : public Expr {
public:
    DEFINE_CLASS_CONSTRUCTOR(VectorizedArithmeticExpr);

    StatusOr<ColumnPtr> evaluate_decimal_fast_mul(ExprContext* context, vectorized::Chunk* chunk) {
        auto lhs_pt_opt = eliminate_trivial_cast_for_decimal_mul(_children[0]);
        auto rhs_pt_opt = eliminate_trivial_cast_for_decimal_mul(_children[1]);
        if (lhs_pt_opt.has_value() && rhs_pt_opt.has_value()) {
            auto lhs_pt = lhs_pt_opt.value();
            auto rhs_pt = rhs_pt_opt.value();
            if (lhs_pt == TYPE_DECIMAL64 && rhs_pt == TYPE_DECIMAL64 && Type == TYPE_DECIMAL128) {
                ASSIGN_OR_RETURN(auto l, _children[0]->get_child(0)->evaluate_checked(context, chunk));
                ASSIGN_OR_RETURN(auto r, _children[1]->get_child(0)->evaluate_checked(context, chunk));
                return VectorizedStrictDecimalBinaryFunction<MulOp64x64_128, false>::template evaluate<
                        TYPE_DECIMAL64, TYPE_DECIMAL64, Type>(l, r);
            }
            if (lhs_pt == TYPE_DECIMAL32 && rhs_pt == TYPE_DECIMAL64 && Type == TYPE_DECIMAL128) {
                ASSIGN_OR_RETURN(auto l, _children[0]->get_child(0)->evaluate_checked(context, chunk));
                ASSIGN_OR_RETURN(auto r, _children[1]->get_child(0)->evaluate_checked(context, chunk));
                return VectorizedStrictDecimalBinaryFunction<MulOp32x64_128, false>::template evaluate<
                        TYPE_DECIMAL32, TYPE_DECIMAL64, Type>(l, r);
            }
            if (lhs_pt == TYPE_DECIMAL64 && rhs_pt == TYPE_DECIMAL32 && Type == TYPE_DECIMAL128) {
                ASSIGN_OR_RETURN(auto l, _children[0]->get_child(0)->evaluate_checked(context, chunk));
                ASSIGN_OR_RETURN(auto r, _children[1]->get_child(0)->evaluate_checked(context, chunk));
                return VectorizedStrictDecimalBinaryFunction<MulOp32x64_128, false>::template evaluate<
                        TYPE_DECIMAL32, TYPE_DECIMAL64, Type>(r, l);
            }
            if (lhs_pt == TYPE_DECIMAL32 && rhs_pt == TYPE_DECIMAL32 && Type == TYPE_DECIMAL128) {
                ASSIGN_OR_RETURN(auto l, _children[0]->get_child(0)->evaluate_checked(context, chunk));
                ASSIGN_OR_RETURN(auto r, _children[1]->get_child(0)->evaluate_checked(context, chunk));
                return VectorizedStrictDecimalBinaryFunction<MulOp32x32_128, false>::template evaluate<
                        TYPE_DECIMAL32, TYPE_DECIMAL32, Type>(r, l);
            }
        }
        return nullptr;
    }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override {
#if defined(__x86_64__) && defined(__GNUC__)
        if constexpr (is_mul_op<OP> && pt_is_decimal<Type>) {
            ASSIGN_OR_RETURN(auto opt_result, evaluate_decimal_fast_mul(context, ptr));
            if (opt_result != nullptr) {
                return opt_result;
            }
        }
#endif
        ASSIGN_OR_RETURN(auto l, _children[0]->evaluate_checked(context, ptr));
        ASSIGN_OR_RETURN(auto r, _children[1]->evaluate_checked(context, ptr));
        if constexpr (pt_is_decimal<Type>) {
            // Enable overflow checking in decimal arithmetic
            return VectorizedStrictDecimalBinaryFunction<OP, true>::template evaluate<Type>(l, r);
        } else {
            using ArithmeticOp = ArithmeticBinaryOperator<OP, Type>;
            return VectorizedStrictBinaryFunction<ArithmeticOp>::template evaluate<Type>(l, r);
        }
    }
    std::string debug_string() const override {
        std::stringstream out;
        auto expr_debug_string = Expr::debug_string();
        out << "VectorizedArithmeticExpr ("
            << "lhs=" << _children[0]->type().debug_string() << ", rhs=" << _children[1]->type().debug_string()
            << ", result=" << this->type().debug_string() << ", lhs_is_constant=" << _children[0]->is_constant()
            << ", rhs_is_constant=" << _children[1]->is_constant() << ", expr (" << expr_debug_string << ") )";
        return out.str();
    }
};

template <PrimitiveType Type, typename Op>
class VectorizedDivArithmeticExpr final : public Expr {
public:
    DEFINE_CLASS_CONSTRUCTOR(VectorizedDivArithmeticExpr);
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override {
        if constexpr (is_intdiv_op<Op> && pt_is_bigint<Type>) {
            using CastFunction = VectorizedUnaryFunction<DecimalTo<true>>;
            switch (_children[0]->type().type) {
            case TYPE_DECIMAL32: {
                ASSIGN_OR_RETURN(auto column, evaluate_internal<TYPE_DECIMAL32>(context, ptr));
                return CastFunction::evaluate<TYPE_DECIMAL32, PrimitiveType::TYPE_BIGINT>(column);
            }
            case TYPE_DECIMAL64: {
                ASSIGN_OR_RETURN(auto column, evaluate_internal<TYPE_DECIMAL64>(context, ptr));
                return CastFunction::evaluate<TYPE_DECIMAL64, PrimitiveType::TYPE_BIGINT>(column);
            }
            case TYPE_DECIMAL128: {
                ASSIGN_OR_RETURN(auto column, evaluate_internal<TYPE_DECIMAL128>(context, ptr));
                return CastFunction::evaluate<TYPE_DECIMAL128, PrimitiveType::TYPE_BIGINT>(column);
            }
            default:
                return evaluate_internal<Type>(context, ptr);
            }
        } else {
            return evaluate_internal<Type>(context, ptr);
        }
    }

private:
    template <PrimitiveType LType>
    StatusOr<ColumnPtr> evaluate_internal(ExprContext* context, vectorized::Chunk* ptr) {
        ASSIGN_OR_RETURN(auto l, _children[0]->evaluate_checked(context, ptr));
        ASSIGN_OR_RETURN(auto r, _children[1]->evaluate_checked(context, ptr));
        if constexpr (pt_is_decimal<LType>) {
            using VectorizedDiv = VectorizedUnstrictDecimalBinaryFunction<LType, DivOp, true>;
            return VectorizedDiv::template evaluate<LType>(l, r);
        } else {
            using RightZeroCheck = ArithmeticRightZeroCheck<LType>;
            using ArithmeticDiv = ArithmeticBinaryOperator<DivOp, LType>;
            using VectorizedDiv = VectorizedUnstrictBinaryFunction<RightZeroCheck, ArithmeticDiv>;
            return VectorizedDiv::template evaluate<LType>(l, r);
        }
    }
};

template <PrimitiveType Type>
class VectorizedModArithmeticExpr final : public Expr {
public:
    DEFINE_CLASS_CONSTRUCTOR(VectorizedModArithmeticExpr);
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override {
        ASSIGN_OR_RETURN(auto l, _children[0]->evaluate_checked(context, ptr));
        ASSIGN_OR_RETURN(auto r, _children[1]->evaluate_checked(context, ptr));

        if constexpr (pt_is_decimal<Type>) {
            using VectorizedDiv = VectorizedUnstrictDecimalBinaryFunction<Type, ModOp, true>;
            return VectorizedDiv::template evaluate<Type>(l, r);
        } else {
            using RightZeroCheck = ArithmeticRightZeroCheck<Type>;
            using ArithmeticMod = ArithmeticBinaryOperator<ModOp, Type>;
            using VectorizedMod = VectorizedUnstrictBinaryFunction<RightZeroCheck, ArithmeticMod>;
            return VectorizedMod::template evaluate<Type>(l, r);
        }
    }
};

template <PrimitiveType Type>
class VectorizedBitNotArithmeticExpr final : public Expr {
public:
    DEFINE_CLASS_CONSTRUCTOR(VectorizedBitNotArithmeticExpr);
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override {
        ASSIGN_OR_RETURN(auto l, _children[0]->evaluate_checked(context, ptr));
        using ArithmeticBitNot = ArithmeticUnaryOperator<BitNotOp, Type>;
        return VectorizedStrictUnaryFunction<ArithmeticBitNot>::template evaluate<Type>(l);
    }
};

template <PrimitiveType Type, typename OP>
class VectorizedBitShiftArithmeticExpr final : public Expr {
public:
    DEFINE_CLASS_CONSTRUCTOR(VectorizedBitShiftArithmeticExpr);
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override {
        ASSIGN_OR_RETURN(auto l, _children[0]->evaluate_checked(context, ptr));
        ASSIGN_OR_RETURN(auto r, _children[1]->evaluate_checked(context, ptr));

        using ArithmeticOp = ArithmeticBinaryOperator<OP, Type>;
        return VectorizedStrictBinaryFunction<ArithmeticOp>::template evaluate<Type, TYPE_BIGINT, Type>(l, r);
    }
};

#undef DEFINE_CLASS_CONSTRUCTOR

#define CASE_TYPE(TYPE, OP) \
    case TYPE: {            \
        CASE_FN(TYPE, OP);  \
    }

#define CASE_INT_TYPE(OP)         \
    CASE_TYPE(TYPE_TINYINT, OP);  \
    CASE_TYPE(TYPE_SMALLINT, OP); \
    CASE_TYPE(TYPE_INT, OP);      \
    CASE_TYPE(TYPE_BIGINT, OP);   \
    CASE_TYPE(TYPE_LARGEINT, OP);

#define CASE_FLOAT_TYPE(OP)    \
    CASE_TYPE(TYPE_FLOAT, OP); \
    CASE_TYPE(TYPE_DOUBLE, OP);

#define CASE_DECIMAL_TYPE(OP) CASE_TYPE(TYPE_DECIMALV2, OP);

#define CASE_DECIMAL_V3_TYPE(OP)  \
    CASE_TYPE(TYPE_DECIMAL32, OP) \
    CASE_TYPE(TYPE_DECIMAL64, OP) \
    CASE_TYPE(TYPE_DECIMAL128, OP)

#define SWITCH_INT_TYPE(OP)                                                   \
    switch (resultType) {                                                     \
        CASE_INT_TYPE(OP);                                                    \
    default:                                                                  \
        LOG(WARNING) << "vectorized engine not support type: " << resultType; \
        return nullptr;                                                       \
    }

#define SWITCH_NUMBER_TYPE(OP)                                                \
    switch (resultType) {                                                     \
        CASE_INT_TYPE(OP);                                                    \
        CASE_FLOAT_TYPE(OP);                                                  \
    default:                                                                  \
        LOG(WARNING) << "vectorized engine not support type: " << resultType; \
        return nullptr;                                                       \
    }

#define SWITCH_ALL_TYPE(OP)                                                   \
    switch (resultType) {                                                     \
        CASE_INT_TYPE(OP);                                                    \
        CASE_FLOAT_TYPE(OP);                                                  \
        CASE_DECIMAL_TYPE(OP);                                                \
        CASE_DECIMAL_V3_TYPE(OP);                                             \
    default:                                                                  \
        LOG(WARNING) << "vectorized engine not support type: " << resultType; \
        return nullptr;                                                       \
    }

Expr* VectorizedArithmeticExprFactory::from_thrift(const starrocks::TExprNode& node) {
    PrimitiveType resultType = TypeDescriptor::from_thrift(node.type).type;
    switch (node.opcode) {
#define CASE_FN(TYPE, OP) return new VectorizedArithmeticExpr<TYPE, OP>(node);
    case TExprOpcode::ADD:
        SWITCH_ALL_TYPE(AddOp);
    case TExprOpcode::SUBTRACT:
        SWITCH_ALL_TYPE(SubOp);
    case TExprOpcode::MULTIPLY:
        SWITCH_ALL_TYPE(MulOp);
    case TExprOpcode::BITAND:
        SWITCH_INT_TYPE(BitAndOp);
    case TExprOpcode::BITOR:
        SWITCH_INT_TYPE(BitOrOp);
    case TExprOpcode::BITXOR:
        SWITCH_INT_TYPE(BitXorOp);

#undef CASE_FN

#define CASE_FN(TYPE, OP) return new VectorizedDivArithmeticExpr<TYPE, OP>(node);
    case TExprOpcode::DIVIDE:
        SWITCH_ALL_TYPE(DivOp)
    case TExprOpcode::INT_DIVIDE:
        SWITCH_ALL_TYPE(IntDivOp)
#undef CASE_FN

#define CASE_FN(TYPE, OP) return new VectorizedModArithmeticExpr<TYPE>(node);
    case TExprOpcode::MOD:
        SWITCH_ALL_TYPE(TYPE_NULL);
#undef CASE_FN

#define CASE_FN(TYPE, OP) return new VectorizedBitNotArithmeticExpr<TYPE>(node);
    case TExprOpcode::BITNOT:
        SWITCH_INT_TYPE(TYPE_NULL);
#undef CASE_FN

#define CASE_FN(TYPE, OP) return new VectorizedBitShiftArithmeticExpr<TYPE, OP>(node);
    case TExprOpcode::BIT_SHIFT_LEFT:
        SWITCH_INT_TYPE(BitShiftLeftOp);
    case TExprOpcode::BIT_SHIFT_RIGHT:
        SWITCH_INT_TYPE(BitShiftRightOp);
    case TExprOpcode::BIT_SHIFT_RIGHT_LOGICAL:
        SWITCH_INT_TYPE(BitShiftRightLogicalOp);
#undef CASE_FN

    default:
        LOG(WARNING) << "vectorized engine not support arithmetic operation: " << node.opcode;
        return nullptr;
    }
}

#undef CASE_TYPE
#undef CASE_INT_TYPE
#undef CASE_FLOAT_TYPE
#undef CASE_DECIMAL_TYPE
#undef SWITCH_INT_TYPE
#undef SWITCH_NUMBER_TYPE
#undef SWITCH_ALL_TYPE

} // namespace starrocks::vectorized
