// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <utility>

#include "exprs/table_function/table_function.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"

namespace starrocks {
class TFunction;
}  // namespace starrocks

namespace starrocks::vectorized {

// Now UDTF only support one column return
class JavaUDTFFunction final : public TableFunction {
public:
    JavaUDTFFunction() = default;
    ~JavaUDTFFunction() = default;

    Status init(const TFunction& fn, TableFunctionState** state) const override;
    Status prepare(TableFunctionState* state) const override;
    Status open(TableFunctionState* state) const override;
    std::pair<Columns, ColumnPtr> process(TableFunctionState* state, bool* eos) const override;
    Status close(TableFunctionState* state) const override;
};
} // namespace starrocks::vectorized