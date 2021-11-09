// This file is made available under Elastic License 2.0.
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
#include <memory> // unique_ptr
#include <string>
#include <vector>

#include "common/status.h" // Status
#include "gen_cpp/segment_v2.pb.h"
#include "gutil/macros.h"
#include "runtime/global_dicts.h"

namespace starrocks {

class RowBlock;
class RowCursor;
class TabletSchema;
class TabletColumn;
class ShortKeyIndexBuilder;
class MemTracker;

namespace fs {
class WritableBlock;
}

namespace vectorized {
class Chunk;
}

namespace segment_v2 {

class ColumnWriter;

extern const char* const k_segment_magic;
extern const uint32_t k_segment_magic_length;

struct SegmentWriterOptions {
    uint32_t storage_format_version = 1;
    uint32_t num_rows_per_block = 1024;
    vectorized::GlobalDictByNameMaps* global_dicts = nullptr;
};

class SegmentWriter {
public:
    SegmentWriter(std::unique_ptr<fs::WritableBlock> block, uint32_t segment_id, const TabletSchema* tablet_schema,
                  const SegmentWriterOptions& opts);
    ~SegmentWriter();

    SegmentWriter(const SegmentWriter&) = delete;
    void operator=(const SegmentWriter&) = delete;

    Status init(uint32_t write_mbytes_per_sec);

    template <typename RowType>
    Status append_row(const RowType& row);

    Status append_chunk(const vectorized::Chunk& chunk);

    uint64_t estimate_segment_size();

    uint32_t num_rows_written() const { return _row_count; }

    Status finalize(uint64_t* segment_file_size, uint64_t* index_size);

    uint32_t segment_id() const { return _segment_id; }

    const vectorized::DictColumnsValidMap& global_dict_columns_valid_info() { return _global_dict_columns_valid_info; }

private:
    Status _write_data();
    Status _write_ordinal_index();
    Status _write_zone_map();
    Status _write_bitmap_index();
    Status _write_bloom_filter_index();
    Status _write_short_key_index();
    Status _write_footer();
    Status _write_raw_data(const std::vector<Slice>& slices);
    void _init_column_meta(ColumnMetaPB* meta, uint32_t* column_id, const TabletColumn& column);

    uint32_t _segment_id;
    const TabletSchema* _tablet_schema;
    SegmentWriterOptions _opts;

    std::unique_ptr<fs::WritableBlock> _wblock;

    SegmentFooterPB _footer;
    std::unique_ptr<ShortKeyIndexBuilder> _index_builder;
    std::vector<std::unique_ptr<ColumnWriter>> _column_writers;
    uint32_t _row_count = 0;

    vectorized::DictColumnsValidMap _global_dict_columns_valid_info;
};

} // namespace segment_v2
} // namespace starrocks
