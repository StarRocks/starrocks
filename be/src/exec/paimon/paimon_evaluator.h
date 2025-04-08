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

#pragma once

#include "exprs/expr.h"
#include "paimon/predicate/predicate_builder.h"

namespace starrocks {
class PaimonEvaluator {
public:
    PaimonEvaluator(const std::vector<SlotDescriptor*>& slots);
    ~PaimonEvaluator() = default;
    std::shared_ptr<paimon::Predicate> evaluate(const std::vector<Expr*>* conjuncts);

private:
    std::shared_ptr<paimon::Predicate> evaluate(Expr* conjunct, bool neg);
    std::shared_ptr<paimon::Predicate> evaluate_compound(TExprOpcode::type op_type, const std::vector<Expr*>* children,
                                                         bool neg);
    std::shared_ptr<paimon::Predicate> evaluate_null(int32_t field_index, const std::string& field_name,
                                                     const paimon::FieldType& fieldType, bool neg);
    std::shared_ptr<paimon::Predicate> evaluate_equal(int32_t field_index, const std::string& field_name,
                                                      const paimon::FieldType& fieldType,
                                                      const paimon::Literal& literal, bool neg);
    std::shared_ptr<paimon::Predicate> evaluate_le(int32_t field_index, const std::string& field_name,
                                                   const paimon::FieldType& fieldType, const paimon::Literal& literal,
                                                   bool neg);
    std::shared_ptr<paimon::Predicate> evaluate_lt(int32_t field_index, const std::string& field_name,
                                                   const paimon::FieldType& fieldType, const paimon::Literal& literal,
                                                   bool neg);
    std::shared_ptr<paimon::Predicate> evaluate_ge(int32_t field_index, const std::string& field_name,
                                                   const paimon::FieldType& fieldType, const paimon::Literal& literal,
                                                   bool neg);
    std::shared_ptr<paimon::Predicate> evaluate_gt(int32_t field_index, const std::string& field_name,
                                                   const paimon::FieldType& fieldType, const paimon::Literal& literal,
                                                   bool neg);
    std::shared_ptr<paimon::Predicate> evaluate_in(int32_t field_index, const std::string& field_name,
                                                   const paimon::FieldType& fieldType,
                                                   const std::vector<paimon::Literal>& literals, bool neg);
    bool _ok_to_paimon_literal(Expr* lit);
    bool _ok_to_paimon_type(const TypeDescriptor& type);
    paimon::Literal translate_to_paimon_literal(Expr* lit);
    paimon::FieldType translate_to_paimon_type(const TypeDescriptor& type);
    void translate_to_paimon_in_list_literals(Expr* in_list_expr, std::vector<paimon::Literal>& ret);

    std::vector<SlotDescriptor*> slots;
};
} // namespace starrocks