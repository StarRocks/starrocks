// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <utility>

#include "column/column.h"
#include "exprs/vectorized/function_helper.h"

namespace starrocks::vectorized {

class TableFunctionState {
public:
    TableFunctionState() = default;

    void set_params(starrocks::vectorized::Columns columns) { this->_columns = std::move(columns); }

    void set_offset(int offset) { this->_offset = offset; }

    int get_offset() { return _offset; }

    starrocks::vectorized::Columns get_columns() { return _columns; }

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
};

class TableFunction {
public:
    virtual ~TableFunction() = default;

    //Initialize TableFunctionState
    virtual Status init(TableFunctionState** state) const = 0;

    //Some preparations are made in prepare, such as establishing a connection or initializing initial values
    virtual Status prepare(TableFunctionState* state) const = 0;

    //Table function processing logic
    virtual std::pair<Columns, ColumnPtr> process(TableFunctionState* state, bool* eos) const = 0;

    //Release the resources constructed in init and prepare
    virtual Status close(TableFunctionState* context) const = 0;
};

using TableFunctionPtr = std::shared_ptr<TableFunction>;

} // namespace starrocks::vectorized