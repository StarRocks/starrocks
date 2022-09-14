// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/beta_rowset_writer.h

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
#include "gen_cpp/olap_file.pb.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/segment_writer.h"

namespace starrocks {

namespace fs {
class WritableBlock;
}

class SegmentWriter;

class BetaRowsetWriter : public RowsetWriter {
public:
    explicit BetaRowsetWriter(const RowsetWriterContext& context);
    ~BetaRowsetWriter() override = default;

    Status init() override;

    StatusOr<RowsetSharedPtr> build() override;

    Version version() override { return _context.version; }
    int64_t num_rows() override { return _num_rows_written; }
    int64_t total_data_size() override { return _total_data_size; }
    RowsetId rowset_id() override { return _context.rowset_id; }

protected:
    Status flush_src_rssids(uint32_t segment_id);

    RowsetWriterContext _context;
    std::unique_ptr<RowsetMetaPB> _rowset_meta_pb;
    std::unique_ptr<TabletSchema> _rowset_schema;
    std::unique_ptr<RowsetTxnMetaPB> _rowset_txn_meta_pb;
    SegmentWriterOptions _writer_options;

    int _num_segment;
    vector<bool> _segment_has_deletes;
    vector<std::string> _tmp_segment_files;
    // mutex lock for vectorized add chunk and flush
    std::mutex _lock;

    // counters and statistics maintained during data write
    int64_t _num_rows_written;
    int64_t _total_row_size;
    int64_t _total_data_size;
    int64_t _total_index_size;

    // used for updatable tablet's compaction
    std::unique_ptr<vector<uint32_t>> _src_rssids;

    bool _is_pending = false;
    bool _already_built = false;
};

// Chunk contains all schema columns data.
class HorizontalBetaRowsetWriter final : public BetaRowsetWriter {
public:
    explicit HorizontalBetaRowsetWriter(const RowsetWriterContext& context);
    ~HorizontalBetaRowsetWriter() override;

    Status add_chunk(const vectorized::Chunk& chunk) override;
    Status add_chunk_with_rssid(const vectorized::Chunk& chunk, const vector<uint32_t>& rssid) override;

    Status flush_chunk(const vectorized::Chunk& chunk) override;
    Status flush_chunk_with_deletes(const vectorized::Chunk& upserts, const vectorized::Column& deletes) override;

    // add rowset by create hard link
    Status add_rowset(RowsetSharedPtr rowset) override;
    Status add_rowset_for_linked_schema_change(RowsetSharedPtr rowset, const SchemaMapping& schema_mapping) override;

    Status flush() override;

    StatusOr<RowsetSharedPtr> build() override;

private:
    StatusOr<std::unique_ptr<SegmentWriter>> _create_segment_writer();

    Status _flush_segment_writer(std::unique_ptr<SegmentWriter>* segment_writer);

    Status _final_merge();

    std::unique_ptr<SegmentWriter> _segment_writer;
};

// Chunk contains partial columns data corresponding to column_indexes.
class VerticalBetaRowsetWriter final : public BetaRowsetWriter {
public:
    explicit VerticalBetaRowsetWriter(const RowsetWriterContext& context);
    ~VerticalBetaRowsetWriter() override;

    Status add_columns(const vectorized::Chunk& chunk, const std::vector<uint32_t>& column_indexes,
                       bool is_key) override;

    Status add_columns_with_rssid(const vectorized::Chunk& chunk, const std::vector<uint32_t>& column_indexes,
                                  const std::vector<uint32_t>& rssid) override;

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
