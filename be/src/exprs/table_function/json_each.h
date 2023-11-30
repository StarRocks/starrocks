// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/table_function/table_function.h"
#include "runtime/runtime_state.h"

namespace starrocks::vectorized {

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
    std::pair<Columns, UInt32Column::Ptr> process(TableFunctionState* state, bool* eos) const override;

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

} // namespace starrocks::vectorized
