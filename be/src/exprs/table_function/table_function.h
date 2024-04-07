// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <utility>

#include "column/fixed_length_column.h"
#include "exprs/vectorized/function_helper.h"
#include "runtime/runtime_state.h"

namespace starrocks::vectorized {

class TableFunctionState {
public:
    TableFunctionState() = default;
    virtual ~TableFunctionState() = default;

    void set_params(starrocks::vectorized::Columns columns) { this->_columns = std::move(columns); }

    void set_offset(int offset) { this->_offset = offset; }

    int get_offset() { return _offset; }

    starrocks::vectorized::Columns& get_columns() { return _columns; }

    void set_status(Status status) { _status = std::move(status); }

    const Status& status() const { return _status; }

private:
    //Params of table function
    starrocks::vectorized::Columns _columns;
    /**
     * _offset is used to record the return value offset of the currently processed columns parameter,
     * if the table function needs to return too many results.
     * In order to avoid occupying a large amount of memory,
     * the result can be returned multiple times according to this offset
     */
    int _offset;
    Status _status;
};

class TableFunction {
public:
    virtual ~TableFunction() = default;

    //Initialize TableFunctionState
    virtual Status init(const TFunction& fn, TableFunctionState** state) const = 0;

    //Some preparations are made in prepare, such as establishing a connection or initializing initial values
    virtual Status prepare(TableFunctionState* state) const = 0;

    virtual Status open(RuntimeState* runtime_state, TableFunctionState* state) const = 0;

    //Table function processing logic
    virtual std::pair<Columns, UInt32Column::Ptr> process(TableFunctionState* state, bool* eos) const = 0;

    //Release the resources constructed in init and prepare
    virtual Status close(RuntimeState* runtime_state, TableFunctionState* context) const = 0;
};

using TableFunctionPtr = std::shared_ptr<TableFunction>;

} // namespace starrocks::vectorized
