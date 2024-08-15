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

#include "exec/pipeline/scan/morsel.h"
#include "runtime/mem_pool.h"
#include "storage/chunk_iterator.h"
#include "storage/delete_predicates.h"
#include "storage/tablet_reader_params.h"
#include "types_fwd.h"

namespace starrocks {
class OlapTuple;

class Chunk;
class ChunkIterator;
class ColumnPredicate;
struct RowSourceMask;
class RowSourceMaskBuffer;
class SeekRange;
class SeekTuple;
class Segment;
class TabletSchema;
class TabletMetadataPB;

namespace lake {

class Rowset;
class TabletManager;

class TabletReader final : public ChunkIterator {
    using Chunk = starrocks::Chunk;
    using ChunkIteratorPtr = starrocks::ChunkIteratorPtr;
    using ColumnPredicate = starrocks::ColumnPredicate;
    using DeletePredicates = starrocks::DeletePredicates;
    using RowsetPtr = std::shared_ptr<Rowset>;
    using RowSourceMask = starrocks::RowSourceMask;
    using RowSourceMaskBuffer = starrocks::RowSourceMaskBuffer;
    using Schema = starrocks::Schema;
    using SeekRange = starrocks::SeekRange;
    using SeekTuple = starrocks::SeekTuple;
    using TabletReaderParams = starrocks::TabletReaderParams;

public:
    TabletReader(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> metadata, Schema schema);
    TabletReader(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> metadata, Schema schema,
                 bool need_split, bool could_split_physically);
    TabletReader(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> metadata, Schema schema,
                 std::vector<RowsetPtr> rowsets, std::shared_ptr<const TabletSchema> tablet_schema);
    TabletReader(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> metadata, Schema schema,
                 std::vector<RowsetPtr> rowsets, bool is_key, RowSourceMaskBuffer* mask_buffer,
                 std::shared_ptr<const TabletSchema> tablet_schema);
    ~TabletReader() override;

    DISALLOW_COPY_AND_MOVE(TabletReader);

    void set_is_asc_hint(bool is_asc) { _is_asc_hint = is_asc; }

    Status prepare();

    // Precondition: the last method called must have been `prepare()`.
    Status open(const TabletReaderParams& read_params);

    void close() override;

    const OlapReaderStatistics& stats() const { return _stats; }
    OlapReaderStatistics* mutable_stats() { return &_stats; }

    size_t merged_rows() const override { return _collect_iter->merged_rows(); }

    void get_split_tasks(std::vector<pipeline::ScanSplitContextPtr>* split_tasks) { split_tasks->swap(_split_tasks); }

protected:
    Status do_get_next(Chunk* chunk) override;
    Status do_get_next(Chunk* chunk, std::vector<uint64_t>* rssid_rowids) override;
    Status do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) override;
    Status do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks,
                       std::vector<uint64_t>* rssid_rowids) override;

private:
    using PredicateList = std::vector<const ColumnPredicate*>;
    using PredicateMap = std::unordered_map<ColumnId, PredicateList>;

    Status get_segment_iterators(const TabletReaderParams& params, std::vector<ChunkIteratorPtr>* iters);

    Status init_predicates(const TabletReaderParams& read_params);
    Status init_delete_predicates(const TabletReaderParams& read_params, DeletePredicates* dels);

    Status init_collector(const TabletReaderParams& read_params);
    Status init_compaction_column_paths(const TabletReaderParams& read_params);

    static Status to_seek_tuple(const TabletSchema& tablet_schema, const OlapTuple& input, SeekTuple* tuple,
                                MemPool* mempool);

    static Status parse_seek_range(const TabletSchema& tablet_schema,
                                   TabletReaderParams::RangeStartOperation range_start_op,
                                   TabletReaderParams::RangeEndOperation range_end_op,
                                   const std::vector<OlapTuple>& range_start_key,
                                   const std::vector<OlapTuple>& range_end_key, std::vector<SeekRange>* ranges,
                                   MemPool* mempool);

    TabletManager* _tablet_mgr;
    std::shared_ptr<const TabletMetadataPB> _tablet_metadata;
    std::shared_ptr<const TabletSchema> _tablet_schema;

    // _rowsets is specified in the constructor when compaction
    bool _rowsets_inited = false;
    std::vector<RowsetPtr> _rowsets;
    std::vector<SegmentSharedPtr> _segments;
    std::shared_ptr<ChunkIterator> _collect_iter;

    PredicateMap _pushdown_predicates;
    DeletePredicates _delete_predicates;
    PredicateList _predicate_free_list;

    OlapReaderStatistics _stats;

    MemPool _mempool;
    ObjectPool _obj_pool;

    bool _is_asc_hint = true;

    // used for vertical compaction
    bool _is_vertical_merge = false;
    bool _is_key = false;
    RowSourceMaskBuffer* _mask_buffer = nullptr;

    // used for table internal parallel
    bool _need_split = false;
    bool _could_split_physically = false;
    std::vector<pipeline::ScanSplitContextPtr> _split_tasks;
};

} // namespace lake
} // namespace starrocks
