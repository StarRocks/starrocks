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

#include <string>
#include <unordered_map>

#include "storage/olap_common.h"
#include "storage/primary_index.h"
#include "storage/rowset/column_iterator.h"
#include "storage/tablet_updates.h"

namespace starrocks {

class Tablet;
class DeltaColumnGroup;
class FileSystem;
class Segment;
class RandomAccessFile;
class ColumnIterator;

struct RowsetSegmentId {
    RowsetId rowset_id;
    uint32_t rowset_seg_id;
    uint32_t segment_id;
};

class ColumnIteratorWrapper {
public:
    ColumnIteratorWrapper() {}
    ~ColumnIteratorWrapper() {}
    ColumnIteratorWrapper(ColumnIteratorWrapper&&) = default;
    Status create(std::shared_ptr<FileSystem> fs, const std::string& path, std::shared_ptr<Segment> segment,
                  uint32_t cid);

    ColumnIterator* operator->() const { return _column_iter.get(); }

    ColumnIterator& operator*() const { return *_column_iter; }

    size_t row_nums() const { return _row_nums; }

private:
    ColumnIteratorOptions _iter_opts;
    OlapReaderStatistics _stats;
    std::unique_ptr<RandomAccessFile> _read_file;
    std::unique_ptr<ColumnIterator> _column_iter;
    std::shared_ptr<Segment> _segment;
    size_t _row_nums = 0;
};

struct ColumnPartialUpdateState {
    // Maintains the mapping of each row of data in the update segment to source row
    std::vector<uint64_t> src_rss_rowids;
    bool inited = false;
    EditVersion read_version;
    // Maintains the mapping of source row to update segment's row
    std::map<uint64_t, uint32_t> rss_rowid_to_update_rowid;

    void release() {
        src_rss_rowids.clear();
        rss_rowid_to_update_rowid.clear();
        inited = false;
    }

    // build `rss_rowid_to_update_rowid` from `src_rss_rowids`
    void build_rss_rowid_to_update_rowid() {
        for (uint32_t upt_id = 0; upt_id < src_rss_rowids.size(); upt_id++) {
            uint64_t each_rss_rowid = src_rss_rowids[upt_id];
            // build rssid & rowid -> update file's rowid
            // each_rss_rowid == UINT64_MAX means that key not exist in pk index
            if (each_rss_rowid < UINT64_MAX) {
                rss_rowid_to_update_rowid[each_rss_rowid] = upt_id;
            }
        }
    }
};

class RowsetColumnUpdateState {
public:
    using ColumnUniquePtr = std::unique_ptr<Column>;
    using DeltaColumnGroupPtr = std::shared_ptr<DeltaColumnGroup>;

    RowsetColumnUpdateState();
    ~RowsetColumnUpdateState();

    RowsetColumnUpdateState(const RowsetColumnUpdateState&) = delete;
    const RowsetColumnUpdateState& operator=(const RowsetColumnUpdateState&) = delete;

    Status load(Tablet* tablet, Rowset* rowset);

    Status finalize_partial_update_state(Tablet* tablet, Rowset* rowset, uint32_t rowset_id,
                                         EditVersion latest_applied_version, const PrimaryIndex& index);

    // Generate delta columns by partial update states and rowset,
    // And distribute partial update column data to different `.col` files
    Status finalize(Rowset* rowset, Tablet* tablet, int64_t version);

    const std::vector<ColumnUniquePtr>& upserts() const { return _upserts; }

    std::size_t memory_usage() const { return _memory_usage; }

    std::string to_string() const;

    const std::vector<ColumnPartialUpdateState>& parital_update_states() { return _partial_update_states; }

    const std::map<uint32_t, DeltaColumnGroupPtr>& delta_column_groups() { return _rssid_to_delta_column_group; }

private:
    Status _load_upserts(Rowset* rowset, uint32_t upsert_id);

    Status _do_load(Tablet* tablet, Rowset* rowset);

    Status _check_and_resolve_conflict(Tablet* tablet, uint32_t rowset_id, uint32_t segment_id,
                                       EditVersion latest_applied_version, const PrimaryIndex& index);

    StatusOr<std::unique_ptr<SegmentWriter>> _prepare_delta_column_group_writer(Rowset* rowset,
                                                                                std::shared_ptr<TabletSchema> tschema,
                                                                                uint32_t rssid, int64_t ver);

    // to build `_partial_update_states`
    Status _prepare_partial_update_states(Tablet* tablet, Rowset* rowset, uint32_t idx, bool need_lock);

    // rebuild src_rss_rowids and rss_rowid_to_update_rowid
    Status _resolve_conflict(Tablet* tablet, uint32_t rowset_id, uint32_t segment_id,
                             EditVersion latest_applied_version, const PrimaryIndex& index);

    /// To reduce memory usage in primary index, we use 32-bit rssid as value.
    /// But we use <RowsetId, segment id> to spell out segment file names,
    /// so we need `_find_rowset_seg_id` and `_init_rowset_seg_id` to build this connection between them.
    // find <RowsetId, segment id> by rssid
    StatusOr<RowsetSegmentId> _find_rowset_seg_id(uint32_t rssid);
    // build the map from rssid to <RowsetId, segment id>
    Status _init_rowset_seg_id(Tablet* tablet, int64_t version);

    Status _read_chunk_from_update(const std::map<uint32_t, std::pair<uint32_t, uint32_t>>& rowid_to_update_rowid,
                                   std::vector<ChunkIteratorPtr>& update_iterators, std::vector<uint32_t>& rowids,
                                   Chunk* result_chunk);

private:
    std::once_flag _load_once_flag;
    Status _status;
    // it contains primary key seriable column for each update segment file
    std::vector<ColumnUniquePtr> _upserts;
    // cache chunks read from update segment files
    std::vector<ChunkPtr> _update_chunk_cache;

    size_t _memory_usage = 0;
    int64_t _tablet_id = 0;

    std::vector<ColumnPartialUpdateState> _partial_update_states;

    // maintain from rssid to rowsetid & segid
    std::map<uint32_t, RowsetSegmentId> _rssid_to_rowsetid_segid;

    // when generate delta column group finish, these fields will be filled
    bool _finalize_finished = false;
    std::map<uint32_t, DeltaColumnGroupPtr> _rssid_to_delta_column_group;

    // stats, only used for performence debug, remove later
    int64_t _total_write_chunk_bytes = 0;
    int64_t _total_read_chunk_bytes = 0;
    int64_t _write_io_cnt = 0;
    int64_t _read_io_cnt = 0;
};

inline std::ostream& operator<<(std::ostream& os, const RowsetColumnUpdateState& o) {
    os << o.to_string();
    return os;
}

} // namespace starrocks
