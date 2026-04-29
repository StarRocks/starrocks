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

#include <storage/flat_json_config.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/macros.h"
#include "io/core/input_stream.h"
#include "runtime/global_dict/types.h"
#include "runtime/global_dict/types_fwd_decl.h"
#include "storage/row_store_encoder_factory.h"
#include "storage/tablet_schema.h"
#include "storage/variant_tuple.h"

namespace starrocks {

class RowBlock;
class TabletSchema;
class TabletColumn;
class ShortKeyIndexBuilder;
class MemTracker;
class WritableFile;
class Chunk;
class ColumnWriter;
class Schema;
struct SegmentFileInfo;
class SegmentMetadataPB;

extern const char* const k_segment_magic;
extern const uint32_t k_segment_magic_length;

class SegmentFileMark {
public:
    std::string rowset_path_prefix;
    std::string rowset_id;
};

struct SegmentWriterOptions {
#ifdef BE_TEST
    uint32_t num_rows_per_block = 100;
#else
    uint32_t num_rows_per_block = 1024;
#endif
    GlobalDictByNameMaps* global_dicts = nullptr;
    std::vector<int32_t> referenced_column_ids;
    SegmentFileMark segment_file_mark;
    // Full paths (including location scheme) for vector index files, keyed by index_id.
    // Populated by the tablet writer for shared-data mode where the location provider
    // resolves object-storage paths; in shared-nothing mode this map is empty and the
    // segment writer falls back to IndexDescriptor-based path construction.
    std::map<int64_t, std::string> vector_index_file_paths;
    std::string encryption_meta;
    bool is_compaction = false;
    bool skip_vector_index = false;
    std::shared_ptr<FlatJsonConfig> flat_json_config = nullptr;
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
    SegmentWriter(std::unique_ptr<WritableFile> block, uint32_t segment_id, TabletSchemaCSPtr tablet_schema,
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

    const std::string& encryption_meta() const { return _opts.encryption_meta; }

    const std::map<int64_t, std::string>& vector_index_file_paths() const { return _opts.vector_index_file_paths; }

    bool has_vector_index_written() const { return _has_vector_index_written; }

    int64_t bundle_file_offset() const;

    StatusOr<std::unique_ptr<io::NumericStatistics>> get_numeric_statistics();

    bool has_key() { return _has_key; }

    const VariantTuple& get_sort_key_min() { return _sort_key_min; }
    const VariantTuple& get_sort_key_max() { return _sort_key_max; }

    // Transfer sort-key min, max, samples, and interval into a SegmentFileInfo.
    // Moves _sort_key_samples out; preserves the carrier invariant
    // (samples.empty() <=> interval == 0).
    void write_sort_key_fields_to(SegmentFileInfo& file_info);

    // Copy sort-key min, max, samples, and interval into a proto.
    // Used by update_manager.cpp which reads from SegmentWriter directly
    // instead of going through a SegmentFileInfo carrier.
    void write_sort_key_fields_to(SegmentMetadataPB* segment_meta) const;

    // Accessors for sort-key samples (used in unit tests).
    int64_t get_sort_key_sample_row_interval() const { return _sort_key_sample_row_interval; }
    const std::vector<VariantTuple>& get_sort_key_samples() const { return _sort_key_samples; }

private:
    Status _write_short_key_index();
    Status _write_footer();
    Status _write_raw_data(const std::vector<Slice>& slices);
    void _init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column);
    void _verify_footer();

    // Check global dictionary validity for a single column writer
    void _check_column_global_dict_valid(ColumnWriter* column_writer, uint32_t column_index);

    uint32_t _segment_id;
    TabletSchemaCSPtr _tablet_schema;
    SegmentWriterOptions _opts;

    std::unique_ptr<WritableFile> _wfile;

    SegmentFooterPB _footer;
    std::unique_ptr<ShortKeyIndexBuilder> _index_builder;
    std::vector<std::unique_ptr<ColumnWriter>> _column_writers;
    std::vector<uint32_t> _column_indexes;
    bool _has_key = true;
    std::vector<uint32_t> _sort_column_indexes;
    VariantTuple _sort_key_min;
    VariantTuple _sort_key_max;

    // Sort-key sampler state. Armed at most once, on the first init() call
    // with has_key=true and non-empty _sort_column_indexes. Preserved across
    // vertical-writer non-key column-group re-init calls.
    std::vector<VariantTuple> _sort_key_samples;
    int64_t _next_sort_key_sample_row_index = 0;
    int64_t _sort_key_sample_row_interval = 0; // 0 = disabled / not yet armed
    std::unique_ptr<Schema> _schema_without_full_row_column;

    // num rows written when appending [partial] columns
    uint32_t _num_rows_written = 0;
    // segment total num rows
    uint32_t _num_rows = 0;

    DictColumnsValidMap _global_dict_columns_valid_info;

    bool _has_vector_index_written = false;
};

} // namespace starrocks
