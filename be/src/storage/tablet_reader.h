// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include <memory>
#include <vector>

#include "column/chunk.h"
#include "runtime/mem_pool.h"
#include "storage/delete_predicates.h"
#include "storage/row_source_mask.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_options.h"
#include "storage/seek_range.h"
#include "storage/tablet.h"
#include "storage/tablet_reader_params.h"

namespace starrocks::vectorized {

class ColumnPredicate;

class TabletReader final : public ChunkIterator {
public:
    TabletReader(TabletSharedPtr tablet, const Version& version, Schema schema);
    // *captured_rowsets* is captured forward before creating TabletReader.
    TabletReader(TabletSharedPtr tablet, const Version& version, Schema schema,
                 const std::vector<RowsetSharedPtr>& captured_rowsets);
    TabletReader(TabletSharedPtr tablet, const Version& version, Schema schema, bool is_key,
                 RowSourceMaskBuffer* mask_buffer);
    ~TabletReader() override { close(); }

    Status prepare();

    // Precondition: the last method called must have been `prepare()`.
    Status open(const TabletReaderParams& read_params);

    void close() override;

    const OlapReaderStatistics& stats() const { return _stats; }
    OlapReaderStatistics* mutable_stats() { return &_stats; }

    size_t merged_rows() const override { return _collect_iter->merged_rows(); }

    void set_delete_predicates_version(Version version) { _delete_predicates_version = version; }

    Status get_segment_iterators(const TabletReaderParams& params, std::vector<ChunkIteratorPtr>* iters);

    static Status parse_seek_range(const TabletSharedPtr& tablet,
                                   TabletReaderParams::RangeStartOperation range_start_op,
                                   TabletReaderParams::RangeEndOperation range_end_op,
                                   const std::vector<OlapTuple>& range_start_key,
                                   const std::vector<OlapTuple>& range_end_key, std::vector<SeekRange>* ranges,
                                   MemPool* mempool);

public:
    Status do_get_next(Chunk* chunk) override;
    Status do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) override;

private:
    using PredicateList = std::vector<const ColumnPredicate*>;
    using PredicateMap = std::unordered_map<ColumnId, PredicateList>;

    Status _init_predicates(const TabletReaderParams& read_params);
    Status _init_delete_predicates(const TabletReaderParams& read_params, DeletePredicates* dels);
    Status _init_collector(const TabletReaderParams& read_params);

    static Status _to_seek_tuple(const TabletSchema& tablet_schema, const OlapTuple& input, SeekTuple* tuple,
                                 MemPool* mempool);

    TabletSharedPtr _tablet;
    Version _version;
    // version of delete predicates, equal as _version by default
    // _delete_predicates_version will be set as max_version of tablet in schema change vectorized
    Version _delete_predicates_version;

    MemPool _mempool;
    ObjectPool _obj_pool;

    PredicateMap _pushdown_predicates;
    DeletePredicates _delete_predicates;
    PredicateList _predicate_free_list;

    std::vector<RowsetSharedPtr> _rowsets;
    std::shared_ptr<ChunkIterator> _collect_iter;

    OlapReaderStatistics _stats;

    // used for vertical compaction
    bool _is_vertical_merge = false;
    bool _is_key = false;
    RowSourceMaskBuffer* _mask_buffer = nullptr;
};

} // namespace starrocks::vectorized
