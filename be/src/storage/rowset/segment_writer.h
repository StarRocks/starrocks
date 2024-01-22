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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/segment_writer.h

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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/macros.h"
#include "runtime/global_dict/types.h"

namespace starrocks {

class RowBlock;
class TabletSchema;
class TabletColumn;
class ShortKeyIndexBuilder;
class MemTracker;
class WritableFile;
class Chunk;
class ColumnWriter;

extern const char* const k_segment_magic;
extern const uint32_t k_segment_magic_length;

struct SegmentWriterOptions {
#ifdef BE_TEST
    uint32_t num_rows_per_block = 100;
#else
    uint32_t num_rows_per_block = 1024;
#endif
    GlobalDictByNameMaps* global_dicts = nullptr;
    std::vector<int32_t> referenced_column_ids;
};

// SegmentWriter is responsible for writing data into single segment by all or partital columns.
//
// Usage Example:
//      // create writer
//      SegmentWriter writer;
//
//      // 1. all columns
//      writer.init();
//      writer.append_chunk(chunk1);
//      writer.append_chunk(chunk2);
//      ...
//      writer.finalize(file_size, index_size);
//
//      // 2. partital columns each time
//      for (column_group : column_groups) {
//          writer.init(column_group, has_key);
//          writer.append_chunk(chunk1);
//          writer.append_chunk(chunk2);
//          ...
//          writer.finalize_columns(index_size);
//      }
//      writer->finalize_footer(file_size);
//
class SegmentWriter {
public:
    SegmentWriter(std::unique_ptr<WritableFile> block, uint32_t segment_id, const TabletSchema* tablet_schema,
                  SegmentWriterOptions opts);
    ~SegmentWriter();

    SegmentWriter(const SegmentWriter&) = delete;
    void operator=(const SegmentWriter&) = delete;

    Status init();

    Status init(bool has_key);

    // Used for vertical compaction
    // footer is used for partial update
    Status init(const std::vector<uint32_t>& column_indexes, bool has_key, SegmentFooterPB* footer = nullptr);

    // |chunk| contains partial or all columns data corresponding to _column_writers.
    Status append_chunk(const Chunk& chunk);

    uint64_t estimate_segment_size();

    uint32_t num_rows_written() const { return _num_rows_written; }
    uint32_t num_rows() const { return _num_rows; }

    // finalize columns data, index and footer
    Status finalize(uint64_t* segment_file_size, uint64_t* index_size, uint64_t* footer_position);

    // Used for vertical compaction
    // finalize columns data and index
    Status finalize_columns(uint64_t* index_size);
    // finalize footer
    Status finalize_footer(uint64_t* segment_file_size, uint64_t* footer_position = nullptr);

    uint32_t segment_id() const { return _segment_id; }

    const DictColumnsValidMap& global_dict_columns_valid_info() { return _global_dict_columns_valid_info; }

    const std::string& segment_path() const;

    uint64_t current_filesz() const;

private:
    Status _write_short_key_index();
    Status _write_footer();
    Status _write_raw_data(const std::vector<Slice>& slices);
    void _init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column);

    uint32_t _segment_id;
    const TabletSchema* _tablet_schema;
    SegmentWriterOptions _opts;

    std::unique_ptr<WritableFile> _wfile;

    SegmentFooterPB _footer;
    std::unique_ptr<ShortKeyIndexBuilder> _index_builder;
    std::vector<std::unique_ptr<ColumnWriter>> _column_writers;
    std::vector<uint32_t> _column_indexes;
    bool _has_key = true;
    std::vector<uint32_t> _sort_column_indexes;

    // num rows written when appending [partial] columns
    uint32_t _num_rows_written = 0;
    // segment total num rows
    uint32_t _num_rows = 0;

    DictColumnsValidMap _global_dict_columns_valid_info;
};

} // namespace starrocks
