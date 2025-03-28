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

#include "exprs/compound_predicate.h"

#include "common/object_pool.h"
#include "exprs/binary_function.h"
#include "exprs/predicate.h"
#include "exprs/unary_function.h"
#include "runtime/runtime_state.h"

#ifdef STARROCKS_JIT_ENABLE
#include "exprs/jit/ir_helper.h"
#endif

namespace starrocks {

#define DEFINE_COMPOUND_CONSTRUCT(CLASS)              \
    CLASS(const TExprNode& node) : Predicate(node) {} \
    virtual ~CLASS() {}                               \
    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new CLASS(*this)); }

/**
 * IS NULL AND IS NULL = IS NULL
 * IS NOT NULL AND IS NOT NULL = IS NOT NULL
 * TRUE AND IS NULL = IS NULL
 * FALSE AND IS NULL = IS NOT NULL(FALSE)
 */
DEFINE_LOGIC_NULL_BINARY_FUNCTION_WITH_IMPL(AndNullImpl, l_value, l_null, r_value, r_null) {
    return (l_null & r_null) | (r_null & (l_null ^ l_value)) | (l_null & (r_null ^ r_value));
}

DEFINE_BINARY_FUNCTION_WITH_IMPL(AndImpl, l_value, r_value) {
    return l_value & r_value;
}

class VectorizedAndCompoundPredicate final : public Predicate {
public:
    DEFINE_COMPOUND_CONSTRUCT(VectorizedAndCompoundPredicate);

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {
        ASSIGN_OR_RETURN(auto l, _children[0]->evaluate_checked(context, ptr));
        int l_falses = ColumnHelper::count_false_with_notnull(l);

        // left all false and not null
        if (l_falses == l->size()) {
            return Column::mutate(std::move(l));
        }

        ASSIGN_OR_RETURN(auto r, _children[1]->evaluate_checked(context, ptr));

        return VectorizedLogicPredicateBinaryFunction<AndNullImpl, AndImpl>::template evaluate<TYPE_BOOLEAN>(l, r);
    }

#ifdef STARROCKS_JIT_ENABLE
    bool is_compilable(RuntimeState* state) const override { return state->can_jit_expr(CompilableExprType::LOGICAL); }

    JitScore compute_jit_score(RuntimeState* state) const override {
        JitScore jit_score = {0, 0};
        if (!is_compilable(state)) {
            return jit_score;
        }
        for (auto child : _children) {
            auto tmp = child->compute_jit_score(state);
            jit_score.score += tmp.score;
            jit_score.num += tmp.num;
        }
        jit_score.num++;
        jit_score.score += 0; // no benefit
        return jit_score;
    }

    StatusOr<LLVMDatum> generate_ir_impl(ExprContext* context, JITContext* jit_ctx) override {
        std::vector<LLVMDatum> datums(2);
        ASSIGN_OR_RETURN(datums[0], _children[0]->generate_ir(context, jit_ctx))
        ASSIGN_OR_RETURN(datums[1], _children[1]->generate_ir(context, jit_ctx))
        auto& b = jit_ctx->builder;
        LLVMDatum result(b);
        result.value = b.CreateAnd(datums[0].value, datums[1].value);
        result.null_flag = b.CreateOr(b.CreateAnd(datums[0].null_flag, datums[1].null_flag),
                                      b.CreateOr(b.CreateAnd(datums[0].null_flag, datums[1].value),
                                                 b.CreateAnd(datums[1].null_flag, datums[0].value)));
        return result;
    }

    std::string jit_func_name_impl(RuntimeState* state) const override {
        return "{" + _children[0]->jit_func_name(state) + " & " + _children[1]->jit_func_name(state) + "}" +
               (is_constant() ? "c:" : "") + (is_nullable() ? "n:" : "") + type().debug_string();
    }
#endif

    std::string debug_string() const override {
        std::stringstream out;
        auto expr_debug_string = Expr::debug_string();
        out << "VectorizedAndCompoundPredicate ("
            << "lhs=" << _children[0]->type().debug_string() << ", rhs=" << _children[1]->type().debug_string()
            << ", result=" << this->type().debug_string() << ", lhs_is_constant=" << _children[0]->is_constant()
            << ", rhs_is_constant=" << _children[1]->is_constant() << ", expr (" << expr_debug_string << ") )";
        return out.str();
    }
};

/**
 * IS NULL OR IS NULL = IS NULL
 * IS NOT NULL OR IS NOT NULL = IS NOT NULL
 * TRUE OR NULL = IS NOT NULL(TRUE)
 * FALSE OR IS NULL = IS NULL
 */
DEFINE_LOGIC_NULL_BINARY_FUNCTION_WITH_IMPL(OrNullImpl, l_value, l_null, r_value, r_null) {
    return (l_null & r_null) | (r_null & (r_null ^ l_value)) | (l_null & (l_null ^ r_value));
}

DEFINE_BINARY_FUNCTION_WITH_IMPL(OrImpl, l_value, r_value) {
    return l_value | r_value;
}

class VectorizedOrCompoundPredicate final : public Predicate {
public:
    DEFINE_COMPOUND_CONSTRUCT(VectorizedOrCompoundPredicate);
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {
        ASSIGN_OR_RETURN(auto l, _children[0]->evaluate_checked(context, ptr));

        int l_trues = ColumnHelper::count_true_with_notnull(l);
        // left all true and not null
        if (l_trues == l->size()) {
            return Column::mutate(std::move(l));
        }

        ASSIGN_OR_RETURN(auto r, _children[1]->evaluate_checked(context, ptr));

        return VectorizedLogicPredicateBinaryFunction<OrNullImpl, OrImpl>::template evaluate<TYPE_BOOLEAN>(l, r);
    }

#ifdef STARROCKS_JIT_ENABLE

    bool is_compilable(RuntimeState* state) const override { return state->can_jit_expr(CompilableExprType::LOGICAL); }

    JitScore compute_jit_score(RuntimeState* state) const override {
        JitScore jit_score = {0, 0};
        if (!is_compilable(state)) {
            return jit_score;
        }
        for (auto child : _children) {
            auto tmp = child->compute_jit_score(state);
            jit_score.score += tmp.score;
            jit_score.num += tmp.num;
        }
        jit_score.num++;
        jit_score.score += 0; // no benefit
        return jit_score;
    }

    StatusOr<LLVMDatum> generate_ir_impl(ExprContext* context, JITContext* jit_ctx) override {
        std::vector<LLVMDatum> datums(2);
        ASSIGN_OR_RETURN(datums[0], _children[0]->generate_ir(context, jit_ctx))
        ASSIGN_OR_RETURN(datums[1], _children[1]->generate_ir(context, jit_ctx))
        auto& b = jit_ctx->builder;
        LLVMDatum result(b);
        result.value = b.CreateOr(datums[0].value, datums[1].value);
        result.null_flag = b.CreateOr(b.CreateAnd(datums[0].null_flag, datums[1].null_flag),
                                      b.CreateOr(b.CreateAnd(datums[0].null_flag, b.CreateNot(datums[1].value)),
                                                 b.CreateAnd(datums[1].null_flag, b.CreateNot(datums[0].value))));
        return result;
    }

    std::string jit_func_name_impl(RuntimeState* state) const override {
        return "{" + _children[0]->jit_func_name(state) + " | " + _children[1]->jit_func_name(state) + "}" +
               (is_constant() ? "c:" : "") + (is_nullable() ? "n:" : "") + type().debug_string();
    }
#endif

    std::string debug_string() const override {
        std::stringstream out;
        auto expr_debug_string = Expr::debug_string();
        out << "VectorizedOrCompoundPredicate ("
            << "lhs=" << _children[0]->type().debug_string() << ", rhs=" << _children[1]->type().debug_string()
            << ", result=" << this->type().debug_string() << ", lhs_is_constant=" << _children[0]->is_constant()
            << ", rhs_is_constant=" << _children[1]->is_constant() << ", expr (" << expr_debug_string << ") )";
        return out.str();
    }
};

DEFINE_UNARY_FN_WITH_IMPL(CompoundPredNot, l) {
    return !l;
}

class VectorizedNotCompoundPredicate final : public Predicate {
public:
    DEFINE_COMPOUND_CONSTRUCT(VectorizedNotCompoundPredicate);
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {
        ASSIGN_OR_RETURN(auto l, _children[0]->evaluate_checked(context, ptr));

        return VectorizedStrictUnaryFunction<CompoundPredNot>::template evaluate<TYPE_BOOLEAN>(l);
    }
#ifdef STARROCKS_JIT_ENABLE

    bool is_compilable(RuntimeState* state) const override { return state->can_jit_expr(CompilableExprType::LOGICAL); }

    JitScore compute_jit_score(RuntimeState* state) const override {
        JitScore jit_score = {0, 0};
        if (!is_compilable(state)) {
            return jit_score;
        }
        for (auto child : _children) {
            auto tmp = child->compute_jit_score(state);
            jit_score.score += tmp.score;
            jit_score.num += tmp.num;
        }
        jit_score.num++;
        jit_score.score += 0; // no benefit
        return jit_score;
    }

    StatusOr<LLVMDatum> generate_ir_impl(ExprContext* context, JITContext* jit_ctx) override {
        ASSIGN_OR_RETURN(LLVMDatum datum, _children[0]->generate_ir(context, jit_ctx))
        auto& b = jit_ctx->builder;
        LLVMDatum result(b);
        result.value = b.CreateSelect(IRHelper::bool_to_cond(b, datum.value), b.getInt8(0), b.getInt8(1));
        result.null_flag = datum.null_flag;
        return result;
    }

    std::string jit_func_name_impl(RuntimeState* state) const override {
        return "{!" + _children[0]->jit_func_name(state) + "}" + (is_constant() ? "c:" : "") +
               (is_nullable() ? "n:" : "") + type().debug_string();
    }
#endif

    std::string debug_string() const override {
        std::stringstream out;
        auto expr_debug_string = Expr::debug_string();
        out << "VectorizedNotCompoundPredicate (" << _children[0]->type().debug_string()
            << ", result=" << this->type().debug_string() << ", child_is_constant=" << _children[0]->is_constant()
            << ", expr (" << expr_debug_string << ") )";
        return out.str();
    }
};

#undef DEFINE_COMPOUND_CONSTRUCT

Expr* VectorizedCompoundPredicateFactory::from_thrift(const TExprNode& node) {
    switch (node.opcode) {
    case TExprOpcode::COMPOUND_AND:
        return new VectorizedAndCompoundPredicate(node);
    case TExprOpcode::COMPOUND_OR:
        return new VectorizedOrCompoundPredicate(node);
    case TExprOpcode::COMPOUND_NOT:
        return new VectorizedNotCompoundPredicate(node);
    default:
        DCHECK(false) << "Not support compound predicate: " << node.opcode;
        return nullptr;
    }
}

} // namespace starrocks
