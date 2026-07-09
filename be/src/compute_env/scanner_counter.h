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

#include <cstdint>

namespace starrocks {

const int64_t MAX_ERROR_LINES_IN_FILE = 50;

// Diagram of row counters relationship:
//
// num_raw_rows_read (Total Rows from Storage)
// |
// +-- num_rows_filtered (Invalid Format)
// |
// +-- filtered_rows_read (Valid Format)
//     |
//     +-- num_rows_unselected (Filtered by Predicates)
//     |
//     +-- num_rows_read (Rows Returned)
//
// Equations:
// 1. filtered_rows_read = num_raw_rows_read - num_rows_filtered
// 2. num_rows_read = filtered_rows_read - num_rows_unselected
struct ScannerCounter {
    // num of rows filtered by invalid data format
    int64_t num_rows_filtered = 0;
    // num of rows filtered by predicates
    int64_t num_rows_unselected = 0;
    // num of rows with valid format (after format validation, before predicate filtering)
    // filtered_rows_read = num_rows_read + num_rows_unselected
    int64_t filtered_rows_read = 0;
    // num of rows returned (after predicate filtering)
    // num_rows_read = filtered_rows_read - num_rows_unselected
    int64_t num_rows_read = 0;
    int64_t num_bytes_read = 0;

    // total time cost in scanner
    int64_t total_ns = 0;
    // time cost in fill chunk
    int64_t fill_ns = 0;
    // time cost in read batch from file
    int64_t read_batch_ns = 0;
    // time cost in cast chunk
    int64_t cast_chunk_ns = 0;
    // time cost in materialize
    int64_t materialize_ns = 0;

    // time cost in init chunk
    int64_t init_chunk_ns = 0;

    // time cost in read file
    int64_t file_read_ns = 0;
    // count of file read io
    int64_t file_read_count = 0;
    // count of files opened for reading
    int64_t num_files_read = 0;
};

} // namespace starrocks
