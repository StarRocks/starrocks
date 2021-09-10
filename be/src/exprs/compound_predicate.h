// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/compound_predicate.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef STARROCKS_BE_SRC_QUERY_EXPRS_COMPOUND_PREDICATE_H
#define STARROCKS_BE_SRC_QUERY_EXPRS_COMPOUND_PREDICATE_H

#include <string>

#include "common/object_pool.h"
#include "exprs/predicate.h"
#include "gen_cpp/Exprs_types.h"

namespace starrocks {

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30

class CompoundPredicate : public Predicate {
public:
    static void init();
    static BooleanVal compound_not(FunctionContext* context, const BooleanVal&);

protected:
    friend class Expr;

    explicit CompoundPredicate(const TExprNode& node);

    // virtual Status prepare(RuntimeState* state, const RowDescriptor& desc);
    std::string debug_string() const override;

    bool is_vectorized() const override { return false; }

private:
    friend class OpcodeRegistry;
};

/// Expr for evaluating and (&&) operators
class AndPredicate : public CompoundPredicate {
public:
    Expr* clone(ObjectPool* pool) const override { return pool->add(new AndPredicate(*this)); }

    starrocks_udf::BooleanVal get_boolean_val(ExprContext* context, TupleRow*) override;

protected:
    friend class Expr;

    explicit AndPredicate(const TExprNode& node) : CompoundPredicate(node) {}

    std::string debug_string() const override {
        std::stringstream out;
        out << "AndPredicate(" << Expr::debug_string() << ")";
        return out.str();
    }

private:
    friend class OpcodeRegistry;
};

/// Expr for evaluating or (||) operators
class OrPredicate : public CompoundPredicate {
public:
    Expr* clone(ObjectPool* pool) const override { return pool->add(new OrPredicate(*this)); }

    starrocks_udf::BooleanVal get_boolean_val(ExprContext* context, TupleRow*) override;

protected:
    friend class Expr;

    explicit OrPredicate(const TExprNode& node) : CompoundPredicate(node) {}

    std::string debug_string() const override {
        std::stringstream out;
        out << "OrPredicate(" << Expr::debug_string() << ")";
        return out.str();
    }

private:
    friend class OpcodeRegistry;
};

/// Expr for evaluating or (||) operators
class NotPredicate : public CompoundPredicate {
public:
    Expr* clone(ObjectPool* pool) const override { return pool->add(new NotPredicate(*this)); }

    starrocks_udf::BooleanVal get_boolean_val(ExprContext* context, TupleRow*) override;

protected:
    friend class Expr;

    explicit NotPredicate(const TExprNode& node) : CompoundPredicate(node) {}

    std::string debug_string() const override {
        std::stringstream out;
        out << "NotPredicate(" << Expr::debug_string() << ")";
        return out.str();
    }

private:
    friend class OpcodeRegistry;
};
} // namespace starrocks

#endif
