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
#include "exprs/unary_function.h"
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

    bool is_compilable() const override { return IRHelper::support_jit(Type); }

    StatusOr<LLVMDatum> generate_ir_impl(ExprContext* context, const llvm::Module& module, llvm::IRBuilder<>& b,
                                         const std::vector<LLVMDatum>& datums) const override {
        if constexpr (lt_is_decimal<Type>) {
            // TODO(yueyang): Implement decimal cmp in LLVM IR.
            return Status::NotSupported("JIT of decimal cmp not support");
        } else {
            auto* l = datums[0].value;
            auto* r = datums[1].value;
            LLVMDatum result(b);
            if constexpr (std::is_same_v<OP, BinaryPredFunc<EvalEq<Type>>>) {
                if constexpr (lt_is_float<Type>) {
                    result.value = b.CreateFCmpOEQ(l, r);
                } else {
                    result.value = b.CreateICmpEQ(l, r);
                }
            } else if constexpr (std::is_same_v<OP, BinaryPredFunc<EvalNe<Type>>>) {
                if constexpr (lt_is_float<Type>) {
                    result.value = b.CreateFCmpUNE(l, r);
                } else {
                    result.value = b.CreateICmpNE(l, r);
                }
            } else if constexpr (std::is_same_v<OP, BinaryPredFunc<EvalLt<Type>>>) {
                if constexpr (lt_is_float<Type>) {
                    result.value = b.CreateFCmpOLT(l, r);
                } else {
                    result.value = b.CreateICmpSLT(l, r);
                }
            } else if constexpr (std::is_same_v<OP, BinaryPredFunc<EvalLe<Type>>>) {
                if constexpr (lt_is_float<Type>) {
                    result.value = b.CreateFCmpOLE(l, r);
                } else {
                    result.value = b.CreateICmpSLE(l, r);
                }
            } else if constexpr (std::is_same_v<OP, BinaryPredFunc<EvalGt<Type>>>) {
                if constexpr (lt_is_float<Type>) {
                    result.value = b.CreateFCmpOGT(l, r);
                } else {
                    result.value = b.CreateICmpSGT(l, r);
                }
            } else if constexpr (std::is_same_v<OP, BinaryPredFunc<EvalGe<Type>>>) {
                if constexpr (lt_is_float<Type>) {
                    result.value = b.CreateFCmpOGE(l, r);
                } else {
                    result.value = b.CreateICmpSGE(l, r);
                }
            } else {
                LOG(WARNING) << "unsupported cmp op";
                return Status::InternalError("unsupported cmp op");
            }
            return result;
        }
    }

    std::string debug_string() const override {
        std::stringstream out;
        auto expr_debug_string = Expr::debug_string();
        out << "VectorizedBinaryPredicate ("
            << "lhs=" << _children[0]->type().debug_string() << ", rhs=" << _children[1]->type().debug_string()
            << ", result=" << this->type().debug_string() << ", lhs_is_constant=" << _children[0]->is_constant()
            << ", rhs_is_constant=" << _children[1]->is_constant() << ", expr (" << expr_debug_string << ") )";
        return out.str();
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

        if (l->only_null() || r->only_null()) {
            return ColumnHelper::create_const_null_column(l->size());
        }

        auto* data1 =
                ColumnHelper::get_data_column(ColumnHelper::unpack_and_duplicate_const_column(l->size(), l).get());
        auto* data2 =
                ColumnHelper::get_data_column(ColumnHelper::unpack_and_duplicate_const_column(r->size(), r).get());

        DCHECK(data1->is_array());
        DCHECK(data2->is_array());
        auto lhs_arr = down_cast<ArrayColumn&>(*data1);
        auto rhs_arr = down_cast<ArrayColumn&>(*data2);

        ColumnBuilder<TYPE_BOOLEAN> builder(l->size());
        std::vector<int8_t> cmp_result;
        lhs_arr.compare_column(rhs_arr, &cmp_result);

        // Convert the compare result (-1, 0, 1) to the predicate result (true/false)
        _comparator.eval(cmp_result, &builder);

        ColumnPtr data_result = builder.build(false); // non-const columns as unfolded earlier

        if (l->has_null() || r->has_null()) {
            NullColumnPtr null_flags = FunctionHelper::union_nullable_column(l, r);
            return FunctionHelper::merge_column_and_null_column(std::move(data_result), std::move(null_flags));
        } else {
            return data_result;
        }
    }

private:
    EvalCmpZero _comparator;
};

template <bool is_equal>
class CommonEqualsPredicate final : public Predicate {
public:
    explicit CommonEqualsPredicate(const TExprNode& node) : Predicate(node) {}
    ~CommonEqualsPredicate() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new CommonEqualsPredicate(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* chunk) override {
        ASSIGN_OR_RETURN(auto l, _children[0]->evaluate_checked(context, chunk));
        ASSIGN_OR_RETURN(auto r, _children[1]->evaluate_checked(context, chunk));

        if (l->only_null() || r->only_null()) {
            return ColumnHelper::create_const_null_column(l->size());
        }
        // a nullable column must not contain const columns
        size_t lstep = l->is_constant() ? 0 : 1;
        size_t rstep = r->is_constant() ? 0 : 1;

        auto& const1 = FunctionHelper::get_data_column_of_const(l);
        auto& const2 = FunctionHelper::get_data_column_of_const(r);

        auto& data1 = FunctionHelper::get_data_column_of_nullable(const1);
        auto& data2 = FunctionHelper::get_data_column_of_nullable(const2);

        size_t size = l->size();
        ColumnBuilder<TYPE_BOOLEAN> builder(size);
        for (size_t i = 0, loff = 0, roff = 0; i < size; i++) {
            if (l->is_null(loff) || r->is_null(roff)) {
                builder.append_null();
            } else {
                auto res = data1->equals(loff, *(data2.get()), roff, false);
                if (res == -1) {
                    builder.append_null();
                } else {
                    builder.append(!(res ^ is_equal));
                }
            }

            loff += lstep;
            roff += rstep;
        }
        return builder.build(ColumnHelper::is_all_const({l, r}));
    }
};

DEFINE_UNARY_FN_WITH_IMPL(isNullImpl, v) {
    return v;
}

class CommonNullSafeEqualsPredicate final : public Predicate {
public:
    explicit CommonNullSafeEqualsPredicate(const TExprNode& node) : Predicate(node) {}
    ~CommonNullSafeEqualsPredicate() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new CommonNullSafeEqualsPredicate(*this)); }

    // if v1 null and v2 null = true
    // if v1 null and v2 not null = false
    // if v1 not null and v2 null = false
    // if v1 not null and v2 not null = v1 OP v2
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* chunk) override {
        ASSIGN_OR_RETURN(auto l, _children[0]->evaluate_checked(context, chunk));
        ASSIGN_OR_RETURN(auto r, _children[1]->evaluate_checked(context, chunk));

        if (l->only_null() && r->only_null()) {
            return ColumnHelper::create_const_column<TYPE_BOOLEAN>(true, l->size());
        }

        auto is_null_predicate = [&](const ColumnPtr& column) {
            if (!column->is_nullable() || !column->has_null()) {
                return ColumnHelper::create_const_column<TYPE_BOOLEAN>(false, column->size());
            }
            auto col = ColumnHelper::as_raw_column<NullableColumn>(column)->null_column();
            return VectorizedStrictUnaryFunction<isNullImpl>::evaluate<TYPE_NULL, TYPE_BOOLEAN>(col);
        };

        if (l->only_null()) {
            return is_null_predicate(r);
        } else if (r->only_null()) {
            return is_null_predicate(l);
        }

        auto& const1 = FunctionHelper::get_data_column_of_nullable(l);
        auto& const2 = FunctionHelper::get_data_column_of_nullable(r);

        size_t lstep = const1->is_constant() ? 0 : 1;
        size_t rstep = const2->is_constant() ? 0 : 1;

        auto& data1 = FunctionHelper::get_data_column_of_const(const1);
        auto& data2 = FunctionHelper::get_data_column_of_const(const2);

        size_t size = l->size();
        ColumnBuilder<TYPE_BOOLEAN> builder(size);
        for (size_t i = 0, loff = 0, roff = 0; i < size; i++) {
            auto ln = l->is_null(loff);
            auto rn = r->is_null(roff);
            if (ln & rn) {
                builder.append(true);
            } else if (ln ^ rn) {
                builder.append(false);
            } else {
                builder.append(data1->equals(loff, *(data2.get()), roff));
            }

            loff += lstep;
            roff += rstep;
        }
        return builder.build(ColumnHelper::is_all_const({l, r}));
    }
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
        if (node.opcode == TExprOpcode::EQ) {
            return new CommonEqualsPredicate<true>(node);
        } else if (node.opcode == TExprOpcode::EQ_FOR_NULL) {
            return new CommonNullSafeEqualsPredicate(node);
        } else {
            return new ArrayPredicate(node);
        }
    } else if (type == TYPE_MAP || type == TYPE_STRUCT) {
        if (node.opcode == TExprOpcode::EQ) {
            return new CommonEqualsPredicate<true>(node);
        } else if (node.opcode == TExprOpcode::EQ_FOR_NULL) {
            return new CommonNullSafeEqualsPredicate(node);
        } else if (node.opcode == TExprOpcode::NE) {
            return new CommonEqualsPredicate<false>(node);
        } else {
            return nullptr;
        }
    } else {
        return type_dispatch_predicate<Expr*>(type, true, BinaryPredicateBuilder(), node);
    }
}

} // namespace starrocks
