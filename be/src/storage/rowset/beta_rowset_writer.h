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

#ifndef STARROCKS_BE_SRC_OLAP_ROWSET_BETA_ROWSET_WRITER_H
#define STARROCKS_BE_SRC_OLAP_ROWSET_BETA_ROWSET_WRITER_H

#include <mutex>
#include <vector>

#include "storage/rowset/rowset_writer.h"

namespace starrocks {

namespace fs {
class WritableBlock;
}

namespace segment_v2 {
class SegmentWriter;
} // namespace segment_v2

class BetaRowsetWriter : public RowsetWriter {
public:
    explicit BetaRowsetWriter(const RowsetWriterContext& context);

    ~BetaRowsetWriter() override;

    OLAPStatus init() override;

    OLAPStatus add_row(const RowCursor& row) override { return _add_row(row); }

    // For Memtable::flush()
    OLAPStatus add_row(const ContiguousRow& row) override { return _add_row(row); }

    OLAPStatus add_chunk(const vectorized::Chunk& chunk) override;

    OLAPStatus add_chunk_with_rssid(const vectorized::Chunk& chunk, const vector<uint32_t>& rssid) override;

    OLAPStatus flush_chunk(const vectorized::Chunk& chunk) override;

    OLAPStatus flush_chunk_with_deletes(const vectorized::Chunk& upserts, const vectorized::Column& deletes) override;

    // add rowset by create hard link
    OLAPStatus add_rowset(RowsetSharedPtr rowset) override;

    OLAPStatus add_rowset_for_linked_schema_change(RowsetSharedPtr rowset,
                                                   const SchemaMapping& schema_mapping) override;

    OLAPStatus flush() override;

    RowsetSharedPtr build() override;

    Version version() override { return _context.version; }

    int64_t num_rows() override { return _num_rows_written; }

    int64_t total_data_size() override { return _total_data_size; }

    RowsetId rowset_id() override { return _context.rowset_id; }

private:
    template <typename RowType>
    OLAPStatus _add_row(const RowType& row);

    std::unique_ptr<segment_v2::SegmentWriter> _create_segment_writer();

    OLAPStatus _flush_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>* segment_writer);
    Status _flush_src_rssids();

    Status _final_merge();

    RowsetWriterContext _context;
    std::shared_ptr<RowsetMeta> _rowset_meta;
    std::unique_ptr<TabletSchema> _rowset_schema;

    int _num_segment;
    vector<bool> _segment_has_deletes;
    vector<std::string> _tmp_segment_files;
    std::unique_ptr<segment_v2::SegmentWriter> _segment_writer;
    // mutex lock for vectorized add chunk and flush
    std::mutex _lock;

    // counters and statistics maintained during data write
    int64_t _num_rows_written;
    int64_t _total_row_size;
    int64_t _total_data_size;
    int64_t _total_index_size;
    // TODO rowset's Zonemap

    // used for updatable tablet's compaction
    std::unique_ptr<vector<uint32_t>> _src_rssids;

    bool _is_pending = false;
    bool _already_built = false;
};

} // namespace starrocks

#endif //STARROCKS_BE_SRC_OLAP_ROWSET_BETA_ROWSET_WRITER_H
