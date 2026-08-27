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

#include <paimon/predicate/predicate_builder.h>

#include "exprs/expr.h"

namespace starrocks {
class SlotDescriptor;

class PaimonPredicateConverter {
public:
    explicit PaimonPredicateConverter(const std::vector<SlotDescriptor*>& slots);
    ~PaimonPredicateConverter() = default;
    std::shared_ptr<paimon::Predicate> convert(const std::vector<Expr*>* conjuncts);

private:
    std::shared_ptr<paimon::Predicate> convert(Expr* conjunct, bool neg);
    std::shared_ptr<paimon::Predicate> convert_compound(TExprOpcode::type op_type, const std::vector<Expr*>* children,
                                                         bool neg);
    std::shared_ptr<paimon::Predicate> convert_null(int32_t field_index, const std::string& field_name,
                                                     const paimon::FieldType& fieldType, bool neg);
    std::shared_ptr<paimon::Predicate> convert_equal(int32_t field_index, const std::string& field_name,
                                                      const paimon::FieldType& fieldType,
                                                      const paimon::Literal& literal, bool neg);
    std::shared_ptr<paimon::Predicate> convert_le(int32_t field_index, const std::string& field_name,
                                                   const paimon::FieldType& fieldType, const paimon::Literal& literal,
                                                   bool neg);
    std::shared_ptr<paimon::Predicate> convert_lt(int32_t field_index, const std::string& field_name,
                                                   const paimon::FieldType& fieldType, const paimon::Literal& literal,
                                                   bool neg);
    std::shared_ptr<paimon::Predicate> convert_ge(int32_t field_index, const std::string& field_name,
                                                   const paimon::FieldType& fieldType, const paimon::Literal& literal,
                                                   bool neg);
    std::shared_ptr<paimon::Predicate> convert_gt(int32_t field_index, const std::string& field_name,
                                                   const paimon::FieldType& fieldType, const paimon::Literal& literal,
                                                   bool neg);
    std::shared_ptr<paimon::Predicate> convert_in(int32_t field_index, const std::string& field_name,
                                                   const paimon::FieldType& fieldType,
                                                   const std::vector<paimon::Literal>& literals, bool neg);
    bool _ok_to_paimon_literal(Expr* lit);
    bool _ok_to_paimon_type(const TypeDescriptor& type);
    paimon::Literal translate_to_paimon_literal(Expr* lit);
    paimon::FieldType translate_to_paimon_type(const TypeDescriptor& type);
    void translate_to_paimon_in_list_literals(Expr* in_list_expr, std::vector<paimon::Literal>& ret);
    std::vector<SlotDescriptor*> _slots;
};
} // namespace starrocks
