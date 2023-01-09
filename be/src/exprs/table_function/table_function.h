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

#include <utility>

#include "column/column.h"
#include "exprs/function_helper.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class TableFunctionState {
public:
    TableFunctionState() = default;
    virtual ~TableFunctionState() = default;

    void set_params(starrocks::Columns columns) { this->_columns = std::move(columns); }

    void set_offset(int offset) { this->_offset = offset; }

    int get_offset() { return _offset; }

    starrocks::Columns& get_columns() { return _columns; }

private:
    //Params of table function
    starrocks::Columns _columns;
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
    virtual Status init(const TFunction& fn, TableFunctionState** state) const = 0;

    //Some preparations are made in prepare, such as establishing a connection or initializing initial values
    virtual Status prepare(TableFunctionState* state) const = 0;

    virtual Status open(RuntimeState* runtime_state, TableFunctionState* state) const = 0;

    //Table function processing logic
    virtual std::pair<Columns, ColumnPtr> process(TableFunctionState* state, bool* eos) const = 0;

    //Release the resources constructed in init and prepare
    virtual Status close(RuntimeState* runtime_state, TableFunctionState* context) const = 0;
};

using TableFunctionPtr = std::shared_ptr<TableFunction>;

} // namespace starrocks
