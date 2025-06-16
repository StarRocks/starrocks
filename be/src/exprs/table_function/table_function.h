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

#include "column/fixed_length_column.h"
#include "exprs/function_helper.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class TableFunctionState {
public:
    TableFunctionState() = default;
    virtual ~TableFunctionState() = default;

    void set_params(Columns columns) {
        this->_columns = std::move(columns);
        set_processed_rows(0);
        on_new_params();
    }

    Columns& get_columns() { return _columns; }

    virtual void set_offset(int64_t offset) { this->_offset = offset; }

    int64_t get_offset() { return _offset; }

    void set_is_left_join(bool is_left_join) { this->_is_left_join = is_left_join; }

    bool get_is_left_join() { return _is_left_join; }

    // How many rows of `get_columns()` have been processed/consumed by the table function.
    //
    // If `processed_rows()` < `input_rows()`, the table function will be invoked again with the same parameter columns.
    // If `processed_rows()` >= `input_rows()`, the table function will be invoked with new parameter columns next time.
    //
    // The processed rows will be reset to zero in `set_params()`.
    size_t processed_rows() const { return _processed_rows; }

    void set_processed_rows(size_t value) { _processed_rows = value; }

    size_t input_rows() const { return _columns.empty() ? 0 : _columns[0]->size(); }

    void set_status(Status status) { _status = std::move(status); }

    [[nodiscard]] const Status& status() const { return _status; }

    void set_is_required(bool is_required) { _is_required = is_required; }

    bool is_required() { return _is_required; }

private:
    virtual void on_new_params(){};

    //Params of table function
    Columns _columns;

    size_t _processed_rows = 0;

    /**
     * _offset is used to record the return value offset of the currently processed columns parameter,
     * if the table function needs to return too many results.
     * In order to avoid occupying a large amount of memory,
     * the result can be returned multiple times according to this offset
     */
    int64_t _offset = 0;

    Status _status;

    // used to identify left join for table function
    bool _is_left_join = false;
    bool _is_required = true;
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
    virtual std::pair<Columns, UInt32Column::Ptr> process(RuntimeState* runtime_state,
                                                          TableFunctionState* state) const = 0;

    //Release the resources constructed in init and prepare
    virtual Status close(RuntimeState* runtime_state, TableFunctionState* context) const = 0;
};

using TableFunctionPtr = std::shared_ptr<TableFunction>;

} // namespace starrocks
