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

#include "column/array_column.h"
#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "fs/fs.h"
#include "storage/index/vector/vector_index_builder_factory.h"
#include "storage/rowset/common.h"
#include "storage/tablet_schema.h"
#include "types/bitmap_value.h"

namespace starrocks {

class ArrayColumn;

class VectorIndexWriter {
public:
    static void create(const std::shared_ptr<TabletIndex>& tablet_index, const std::string& vector_index_file_path,
                       bool is_element_nullable, std::unique_ptr<VectorIndexWriter>* res);

    VectorIndexWriter(const std::shared_ptr<TabletIndex>& tablet_index, std::string vector_index_file_path,
                      bool is_element_nullable)
            : _tablet_index(tablet_index),
              _vector_index_file_path(std::move(vector_index_file_path)),
              _is_element_nullable(is_element_nullable) {
        // Element of array column must be nullable.
        DCHECK(_is_element_nullable);
    }

    Status init();

    Status append(const Column& src);

    Status finish(uint64_t* index_size);

    uint64_t size() const;

    uint64_t estimate_buffer_size() const;

    uint64_t total_mem_footprint() const { return estimate_buffer_size(); }

private:
    std::shared_ptr<TabletIndex> _tablet_index;
    std::string _vector_index_file_path;
    std::unique_ptr<VectorIndexBuilder> _index_builder;

    uint32_t _start_vector_index_build_threshold = config::config_vector_index_default_build_threshold;

    // buffer data for tiny data size
    ColumnPtr _buffer_column;

    // size of null_bit column is the same size with buffer_column
    // e.g. buffer_column: [1, NULL, 3, NULL, 4], null_column: [0, 1, 0, 1, 0]
    const bool _is_element_nullable;
    size_t _next_row_id = 0;
    size_t _row_size = 0;
    size_t _buffer_size = 0;

    Status _prepare_index_builder();

    Status _append_data(const Column& src, size_t offset);
};

} // namespace starrocks