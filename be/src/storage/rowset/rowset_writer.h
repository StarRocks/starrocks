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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/rowset_writer.h

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

#include <mutex>
#include <vector>

#include "common/statusor.h"
#include "gen_cpp/data.pb.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/types.pb.h"
#include "gutil/macros.h"
#include "runtime/global_dict/types.h"
#include "runtime/global_dict/types_fwd_decl.h"
#include "storage/column_mapping.h"
#include "storage/compaction_utils.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/segment_writer.h"

namespace butil {
class IOBuf;
}

namespace starrocks {

class SegmentWriter;
class WritableFile;

enum class FlushChunkState { UNKNOWN, UPSERT, DELETE, MIXED };

class Chunk;
class Column;

// RowsetWriter is responsible for writing data into segment by row or chunk.
// Usage Example:
//      // create writer
//      std::unique_ptr<RowsetWriter> writer;
//      RowsetFactory::create_rowset_writer(writer_context, &writer);
//
//      // write data
//      // 1. serial add chunk
//      // should ensure the order of data between chunks
//      // flush segment when size or number of rows reaches certain condition
//      writer->add_chunk(chunk1);
//      writer->add_chunk(chunk2);
//      ...
//      writer->flush();
//
//      // 2. parallel add chunk
//      // each chunk generates a segment
//      writer->flush_chunk(chunk);
//
//      // 3. add chunk by columns
//      for (column_group : column_groups) {
//          writer->add_columns(chunk1, column_group, is_key);
//          writer->add_columns(chunk2, column_group, is_key);
//          ...
//          writer->flush_columns();
//      }
//      writer->final_flush();
//
//      // finish
//      writer->build();
//
class RowsetWriter {
public:
    RowsetWriter() = default;
    explicit RowsetWriter(const RowsetWriterContext& context);
    virtual ~RowsetWriter() = default;

    RowsetWriter(const RowsetWriter&) = delete;
    const RowsetWriter& operator=(const RowsetWriter&) = delete;

    virtual Status init();

    virtual Status add_chunk(const Chunk& chunk) { return Status::NotSupported("RowsetWriter::add_chunk"); }

    // Used for vertical compaction
    // |Chunk| contains partial columns data corresponding to |column_indexes|.
    virtual Status add_columns(const Chunk& chunk, const std::vector<uint32_t>& column_indexes, bool is_key) {
        return Status::NotSupported("RowsetWriter::add_columns");
    }

    virtual Status flush_chunk(const Chunk& chunk, SegmentPB* seg_info = nullptr) {
        return Status::NotSupported("RowsetWriter::flush_chunk");
    }

    virtual Status flush_chunk_with_deletes(const Chunk& upserts, const Column& deletes,
                                            SegmentPB* seg_info = nullptr) {
        return Status::NotSupported("RowsetWriter::flush_chunk_with_deletes");
    }

    // Precondition: the input `rowset` should have the same type of the rowset we're building
    virtual Status add_rowset(RowsetSharedPtr rowset) { return Status::NotSupported("RowsetWriter::add_rowset"); }

    // Precondition: the input `rowset` should have the same type of the rowset we're building
    virtual Status add_rowset_for_linked_schema_change(RowsetSharedPtr rowset, const SchemaMapping& schema_mapping) {
        return Status::NotSupported("RowsetWriter::add_rowset_for_linked_schema_change");
    }

    // explicit flush all buffered rows into segment file.
    virtual Status flush() { return Status::NotSupported("RowsetWriter::flush"); }

    // Used for vertical compaction
    // flush columns data and index
    virtual Status flush_columns() { return Status::NotSupported("RowsetWriter::flush_columns"); }

    // flush segments footer
    virtual Status final_flush() { return Status::NotSupported("RowsetWriter::final_flush"); }

    // finish building and return pointer to the built rowset (guaranteed to be inited).
    // return nullptr when failed
    virtual StatusOr<RowsetSharedPtr> build();

    Status flush_segment(const SegmentPB& segment_pb, butil::IOBuf& data);

    virtual Version version() { return _context.version; }

    virtual int64_t num_rows() { return _num_rows_written; }

    virtual int64_t total_data_size() { return _total_data_size; }

    virtual RowsetId rowset_id() { return _context.rowset_id; }

    virtual const DictColumnsValidMap& global_dict_columns_valid_info() const {
        return _global_dict_columns_valid_info;
    }

protected:
    RowsetWriterContext _context;
    std::shared_ptr<FileSystem> _fs;
    std::unique_ptr<RowsetMetaPB> _rowset_meta_pb;
    std::unique_ptr<RowsetTxnMetaPB> _rowset_txn_meta_pb;
    SegmentWriterOptions _writer_options;

    int _num_segment{0};
    int _num_delfile{0};
    vector<uint32> _delfile_idxes;
    vector<std::string> _tmp_segment_files;
    // mutex lock for vectorized add chunk and flush
    std::mutex _lock;

    // counters and statistics maintained during data write
    int64_t _num_rows_written;
    int64_t _num_rows_flushed = 0;
    std::vector<int64_t> _num_rows_of_tmp_segment_files;
    int64_t _num_rows_del;
    int64_t _total_row_size;
    int64_t _total_data_size;
    int64_t _total_index_size;

    bool _is_pending = false;
    bool _already_built = false;

    FlushChunkState _flush_chunk_state = FlushChunkState::UNKNOWN;

    DictColumnsValidMap _global_dict_columns_valid_info;
};

class VerticalRowsetWriter;

// Chunk contains all schema columns data.
class HorizontalRowsetWriter final : public RowsetWriter {
public:
    explicit HorizontalRowsetWriter(const RowsetWriterContext& context);
    ~HorizontalRowsetWriter() override;

    Status add_chunk(const Chunk& chunk) override;

    Status flush_chunk(const Chunk& chunk, SegmentPB* seg_info = nullptr) override;
    Status flush_chunk_with_deletes(const Chunk& upserts, const Column& deletes, SegmentPB* seg_info) override;

    // add rowset by create hard link
    Status add_rowset(RowsetSharedPtr rowset) override;
    Status add_rowset_for_linked_schema_change(RowsetSharedPtr rowset, const SchemaMapping& schema_mapping) override;

    Status flush() override;

    StatusOr<RowsetSharedPtr> build() override;

private:
    StatusOr<std::unique_ptr<SegmentWriter>> _create_segment_writer();

    Status _flush_segment_writer(std::unique_ptr<SegmentWriter>* segment_writer, SegmentPB* seg_info = nullptr);

    Status _final_merge();

    Status _flush_chunk(const Chunk& chunk, SegmentPB* seg_info = nullptr);

    std::string _flush_state_to_string();

    std::string _error_msg();

    std::unique_ptr<SegmentWriter> _segment_writer;
    std::unique_ptr<VerticalRowsetWriter> _vertical_rowset_writer;
};

// Chunk contains partial columns data corresponding to column_indexes.
class VerticalRowsetWriter final : public RowsetWriter {
public:
    explicit VerticalRowsetWriter(const RowsetWriterContext& context);
    ~VerticalRowsetWriter() override;

    Status add_columns(const Chunk& chunk, const std::vector<uint32_t>& column_indexes, bool is_key) override;

    Status flush_columns() override;

    Status final_flush() override;

private:
    StatusOr<std::unique_ptr<SegmentWriter>> _create_segment_writer(const std::vector<uint32_t>& column_indexes,
                                                                    bool is_key);

    Status _flush_columns(std::unique_ptr<SegmentWriter>* segment_writer);

    std::vector<std::unique_ptr<SegmentWriter>> _segment_writers;
    size_t _current_writer_index = 0;
};

} // namespace starrocks
