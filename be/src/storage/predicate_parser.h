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

#include "common/statusor.h"

namespace starrocks {

class TabletSchema;
class TCondition;
class ExprContext;
class SlotDescriptor;
class RuntimeState;

class ColumnPredicate;

class PredicateParser {
public:
    explicit PredicateParser(const TabletSchema& schema) : _schema(schema) {}

    // check if an expression can be pushed down to the storage level
    bool can_pushdown(const ColumnPredicate* predicate) const;

    bool can_pushdown(const SlotDescriptor* slot_desc) const;

    // Parse |condition| into a predicate that can be pushed down.
    // return nullptr if parse failed.
    ColumnPredicate* parse_thrift_cond(const TCondition& condition) const;

    StatusOr<ColumnPredicate*> parse_expr_ctx(const SlotDescriptor& slot_desc, RuntimeState*,
                                              ExprContext* expr_ctx) const;

    uint32_t column_id(const SlotDescriptor& slot_desc);

private:
    const TabletSchema& _schema;
};

} // namespace starrocks
