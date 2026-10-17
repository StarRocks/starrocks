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

#include "exprs/table_function/table_function.h"

namespace starrocks {

// Name: json_each
// Usage: expands the outermost JSON object into a set of key/value pairs.
// Signature: table(Varchar key, Json value) json_each(Jsonj)
// Example:
//  json_each(parse_json('{"a": 1, "b": 2'))
// | key | value |
// | a   | 1 .   |
// | b . | 2 .   |
class JsonEach final : public TableFunction {
public:
    // Expansion is unbounded (one row per JSON key/element) and every local is a COW MutablePtr or a
    // non-owning vpack view, so a std::bad_alloc can unwind out of process() without leaking. Wrapping
    // it turns an oversized expansion into a failed query instead of a terminated BE.
    bool is_exception_safe() const override { return true; }

    std::pair<Columns, UInt32Column::Ptr> process(RuntimeState* runtime_state,
                                                  TableFunctionState* state) const override;

    Status init(const TFunction& fn, TableFunctionState** state) const override {
        *state = new TableFunctionState();
        return Status::OK();
    }

    Status prepare(TableFunctionState* state) const override { return Status::OK(); }

    Status open(RuntimeState* runtime_state, TableFunctionState* state) const override { return Status::OK(); };

    Status close(RuntimeState* runtime_state, TableFunctionState* state) const override {
        delete state;
        return Status::OK();
    }
};

} // namespace starrocks
