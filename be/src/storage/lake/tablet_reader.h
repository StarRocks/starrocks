// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "runtime/mem_pool.h"
#include "storage/chunk_iterator.h"
#include "storage/delete_predicates.h"
#include "storage/lake/tablet.h"
#include "storage/tablet_reader_params.h"

namespace starrocks {
class OlapTuple;

namespace vectorized {
class Chunk;
class ChunkIterator;
class ColumnPredicate;
class RowSourceMask;
class RowSourceMaskBuffer;
class SeekRange;
class SeekTuple;
} // namespace vectorized

namespace lake {

class Rowset;

class TabletReader final : public vectorized::ChunkIterator {
    using Chunk = starrocks::vectorized::Chunk;
    using ChunkIteratorPtr = starrocks::vectorized::ChunkIteratorPtr;
    using ColumnPredicate = starrocks::vectorized::ColumnPredicate;
    using DeletePredicates = starrocks::vectorized::DeletePredicates;
    using RowsetPtr = std::shared_ptr<Rowset>;
    using RowSourceMask = starrocks::vectorized::RowSourceMask;
    using RowSourceMaskBuffer = starrocks::vectorized::RowSourceMaskBuffer;
    using Schema = starrocks::vectorized::Schema;
    using SeekRange = starrocks::vectorized::SeekRange;
    using SeekTuple = starrocks::vectorized::SeekTuple;
    using TabletReaderParams = starrocks::vectorized::TabletReaderParams;

public:
    TabletReader(Tablet tablet, int64_t version, Schema schema);
    TabletReader(Tablet tablet, int64_t version, Schema schema, const std::vector<RowsetPtr>& rowsets);
    TabletReader(Tablet tablet, int64_t version, Schema schema, bool is_key, RowSourceMaskBuffer* mask_buffer);
    ~TabletReader() override;

    DISALLOW_COPY_AND_MOVE(TabletReader);

    Status prepare();

    // Precondition: the last method called must have been `prepare()`.
    Status open(const TabletReaderParams& read_params);

    void close() override;

    const OlapReaderStatistics& stats() const { return _stats; }
    OlapReaderStatistics* mutable_stats() { return &_stats; }

    size_t merged_rows() const override { return _collect_iter->merged_rows(); }

protected:
    Status do_get_next(Chunk* chunk) override;
    Status do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) override;

private:
    using PredicateList = std::vector<const ColumnPredicate*>;
    using PredicateMap = std::unordered_map<ColumnId, PredicateList>;

    Status get_segment_iterators(const TabletReaderParams& params, std::vector<ChunkIteratorPtr>* iters);

    Status init_predicates(const TabletReaderParams& read_params);
    Status init_delete_predicates(const TabletReaderParams& read_params, DeletePredicates* dels);

    Status init_collector(const TabletReaderParams& read_params);

    static Status to_seek_tuple(const TabletSchema& tablet_schema, const OlapTuple& input, SeekTuple* tuple,
                                MemPool* mempool);

    static Status parse_seek_range(const TabletSchema& tablet_schema,
                                   TabletReaderParams::RangeStartOperation range_start_op,
                                   TabletReaderParams::RangeEndOperation range_end_op,
                                   const std::vector<OlapTuple>& range_start_key,
                                   const std::vector<OlapTuple>& range_end_key, std::vector<SeekRange>* ranges,
                                   MemPool* mempool);

    Tablet _tablet;
    std::shared_ptr<const TabletSchema> _tablet_schema;
    int64_t _version;

    // _rowsets is specified in the constructor when compaction
    bool _rowsets_inited = false;
    std::vector<RowsetPtr> _rowsets;
    std::shared_ptr<ChunkIterator> _collect_iter;

    PredicateMap _pushdown_predicates;
    DeletePredicates _delete_predicates;
    PredicateList _predicate_free_list;

    OlapReaderStatistics _stats;

    MemPool _mempool;
    ObjectPool _obj_pool;

    // used for vertical compaction
    bool _is_vertical_merge = false;
    bool _is_key = false;
    RowSourceMaskBuffer* _mask_buffer = nullptr;
};

} // namespace lake
} // namespace starrocks
