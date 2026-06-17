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

#include <string>
#include <utility>

#include "storage/primitive/predicate_parser.h"
#include "tablet_schema.h"

namespace starrocks {

class TabletSchema;
class OlapPredicateParser final : public PredicateParser {
public:
    explicit OlapPredicateParser(TabletSchemaCSPtr schema) : _schema(std::move(schema)) {}
    // explicit PredicateParser(const std::vector<SlotDescriptor*>* slot_descriptors) : _slot_desc(slot_descriptors) {}

    // check if an expression can be pushed down to the storage level
    bool can_pushdown(const ColumnPredicate* predicate) const override;

    bool can_pushdown(const ConstPredicateNodePtr& pred_tree) const override;

    bool can_pushdown(const SlotDescriptor* slot_desc) const override;

    // Parse |condition| into a predicate that can be pushed down.
    // return nullptr if parse failed.
    StatusOr<ColumnPredicate*> parse_thrift_cond(const TCondition& condition) const override;
    StatusOr<ColumnPredicate*> parse_thrift_cond(const GeneralCondition& condition) const override;

    StatusOr<ColumnPredicate*> parse_expr_ctx(const SlotDescriptor& slot_desc, RuntimeState*,
                                              ExprContext* expr_ctx) const override;

    uint32_t column_id(const SlotDescriptor& slot_desc) const override;

private:
    template <typename ConditionType>
    StatusOr<ColumnPredicate*> t_parse_thrift_cond(const ConditionType& condition) const;

    const TabletSchemaCSPtr _schema = nullptr;
    // const std::vector<SlotDescriptor*>* _slot_desc = nullptr;
};

} // namespace starrocks
