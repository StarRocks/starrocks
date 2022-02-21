// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exprs/table_function/table_function.h"
#include "runtime/primitive_type.h"

namespace starrocks::vectorized {

class TableFunctionFactory {
public:
};

extern const TableFunction* get_table_function(const std::string& name, const std::vector<PrimitiveType>& arg_type,
                                               const std::vector<PrimitiveType>& return_type,
                                               TFunctionBinaryType::type binary_type = TFunctionBinaryType::BUILTIN);

// List of all table functions

// Name: json_each
// Usage: expands the outermost JSON object into a set of key/value pairs.
// Signature: table(key, value) json_each(JsonColumn)
class JsonEach final : public TableFunction {
public:
    std::pair<Columns, ColumnPtr> process(TableFunctionState* state, bool* eos) const override;

    Status init(const TFunction& fn, TableFunctionState** state) const override {
        *state = new TableFunctionState();
        return Status::OK();
    }

    Status prepare(TableFunctionState* state) const override { return Status::OK(); }

    Status open(TableFunctionState* state) const override { return Status::OK(); };

    Status close(TableFunctionState* state) const override {
        delete state;
        return Status::OK();
    }
};

} // namespace starrocks::vectorized