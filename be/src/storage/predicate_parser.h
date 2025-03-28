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

#include "common/statusor.h"
#include "storage/predicate_tree/predicate_tree_fwd.h"
#include "tablet_schema.h"

namespace starrocks {

class TabletSchema;
class TCondition;
class ExprContext;
class SlotDescriptor;
class RuntimeState;

class ColumnPredicate;

class PredicateParser {
public:
    virtual ~PredicateParser() = default;
    // check if an expression can be pushed down to the storage level
    virtual bool can_pushdown(const ColumnPredicate* predicate) const = 0;

    virtual bool can_pushdown(const ConstPredicateNodePtr& pred_tree) const = 0;

    virtual bool can_pushdown(const SlotDescriptor* slot_desc) const = 0;

    // Parse |condition| into a predicate that can be pushed down.
    // return nullptr if parse failed.
    virtual ColumnPredicate* parse_thrift_cond(const TCondition& condition) const = 0;

    virtual StatusOr<ColumnPredicate*> parse_expr_ctx(const SlotDescriptor& slot_desc, RuntimeState*,
                                                      ExprContext* expr_ctx) const = 0;

    virtual uint32_t column_id(const SlotDescriptor& slot_desc) const = 0;

protected:
    static ColumnPredicate* create_column_predicate(const TCondition& condition, TypeInfoPtr& type_info,
                                                    ColumnId index);
};

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
    ColumnPredicate* parse_thrift_cond(const TCondition& condition) const override;

    StatusOr<ColumnPredicate*> parse_expr_ctx(const SlotDescriptor& slot_desc, RuntimeState*,
                                              ExprContext* expr_ctx) const override;

    uint32_t column_id(const SlotDescriptor& slot_desc) const override;

private:
    const TabletSchemaCSPtr _schema = nullptr;
    // const std::vector<SlotDescriptor*>* _slot_desc = nullptr;
};

class ConnectorPredicateParser final : public PredicateParser {
public:
    explicit ConnectorPredicateParser(const std::vector<SlotDescriptor*>* slot_descriptors)
            : _slot_desc(slot_descriptors) {}

    bool can_pushdown(const ColumnPredicate* predicate) const override;

    bool can_pushdown(const ConstPredicateNodePtr& pred_tree) const override;

    bool can_pushdown(const SlotDescriptor* slot_desc) const override;

    ColumnPredicate* parse_thrift_cond(const TCondition& condition) const override;

    StatusOr<ColumnPredicate*> parse_expr_ctx(const SlotDescriptor& slot_desc, RuntimeState*,
                                              ExprContext* expr_ctx) const override;

    uint32_t column_id(const SlotDescriptor& slot_desc) const override;

private:
    const std::vector<SlotDescriptor*>* _slot_desc = nullptr;
};

} // namespace starrocks
