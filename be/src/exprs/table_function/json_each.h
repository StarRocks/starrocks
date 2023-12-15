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

#include "exprs/table_function/table_function.h"
#include "runtime/runtime_state.h"

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
    std::pair<Columns, UInt32Column::Ptr> process(TableFunctionState* state) const override;

    [[nodiscard]] Status init(const TFunction& fn, TableFunctionState** state) const override {
        *state = new TableFunctionState();
        return Status::OK();
    }

    [[nodiscard]] Status prepare(TableFunctionState* state) const override { return Status::OK(); }

    [[nodiscard]] Status open(RuntimeState* runtime_state, TableFunctionState* state) const override {
        return Status::OK();
    };

    [[nodiscard]] Status close(RuntimeState* runtime_state, TableFunctionState* state) const override {
        delete state;
        return Status::OK();
    }
};

} // namespace starrocks
