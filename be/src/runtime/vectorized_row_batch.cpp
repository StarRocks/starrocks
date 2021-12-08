// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/vectorized_row_batch.h"

#include <memory>

#include "common/logging.h"

namespace starrocks {

VectorizedRowBatch::VectorizedRowBatch(const TabletSchema* schema, const std::vector<uint32_t>& cols, int capacity)
        : _schema(schema), _cols(cols), _capacity(capacity), _limit(capacity) {
    _selected_in_use = false;
    _size = 0;

    _mem_pool = std::make_unique<MemPool>();

    _selected = reinterpret_cast<uint16_t*>(new char[sizeof(uint16_t) * _capacity]);

    _col_vectors.resize(schema->num_columns(), nullptr);
    for (ColumnId column_id : cols) {
        _col_vectors[column_id] = new ColumnVector();
    }
}

} // namespace starrocks
