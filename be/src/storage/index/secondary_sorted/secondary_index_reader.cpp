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

#include "storage/index/secondary_sorted/secondary_index_reader.h"

#include <list>
#include <mutex>

#include "base/concurrency/stopwatch.hpp"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/column.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/object_pool.h"
#include "fmt/format.h"
#include "fs/fs.h"
#include "runtime/chunk_helper.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate.h"
#include "storage/del_vector.h"
#include "storage/index/secondary_sorted/predicate_remap.h"
#include "storage/index/secondary_sorted/types.h"
#include "storage/lake/tablet_manager.h"
#include "storage/olap_common.h"
#include "storage/predicate_tree/predicate_tree.h"
#include "storage/primitive/chunk_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"

namespace starrocks::secondary_sorted {

namespace {

constexpr size_t kReadChunkSize = 4096;

// Process-wide LRU cache of opened SecondaryIndexReader instances keyed
// by (tablet_id, file_name). A 100 M-row/1-tablet `.idx` file is 355 MB
// on OSS; re-downloading footer + column-reader init + zone-map index
// for every query costs ~100 ms. Caching the reader collapses that to
// a hash lookup on subsequent queries.
class ReaderCache {
public:
    static ReaderCache& instance() {
        static ReaderCache c;
        return c;
    }

    std::shared_ptr<SecondaryIndexReader> get(const std::string& key) {
        std::lock_guard<std::mutex> l(_mu);
        auto it = _index.find(key);
        if (it == _index.end()) return nullptr;
        // Move to front of LRU list.
        _entries.splice(_entries.begin(), _entries, it->second);
        return it->second->second;
    }

    void put(const std::string& key, std::shared_ptr<SecondaryIndexReader> reader) {
        std::lock_guard<std::mutex> l(_mu);
        if (auto it = _index.find(key); it != _index.end()) {
            // Already inserted concurrently; refresh position and keep first.
            _entries.splice(_entries.begin(), _entries, it->second);
            return;
        }
        _entries.emplace_front(key, std::move(reader));
        _index[key] = _entries.begin();
        const size_t cap = static_cast<size_t>(std::max<int64_t>(1, config::secondary_index_reader_cache_capacity));
        while (_entries.size() > cap) {
            _index.erase(_entries.back().first);
            _entries.pop_back();
        }
    }

private:
    std::mutex _mu;
    std::list<std::pair<std::string, std::shared_ptr<SecondaryIndexReader>>> _entries;
    std::unordered_map<std::string, decltype(_entries)::iterator> _index;
};

std::string make_cache_key(int64_t tablet_id, const std::string& file_name) {
    return fmt::format("{}|{}", tablet_id, file_name);
}

// Process-wide cache of LOOKUP RESULTS (not just opened readers). Keyed by
// (.idx file_name | predicate signature). A single tablet's scan fans out
// into many morsels, each with its own TabletReader running the identical
// lookup; this memoizes the per-segment candidate bitmap so the .idx scan
// runs once instead of once-per-morsel.
//
// Each entry carries a std::once_flag so that the first morsel computes the
// bitmap while the rest block on it, rather than all racing to compute.
struct LookupCacheEntry {
    std::once_flag once;
    StatusOr<std::shared_ptr<const PerSegmentRowidBitmap>> result{nullptr};
};

class LookupResultCache {
public:
    static LookupResultCache& instance() {
        static LookupResultCache c;
        return c;
    }

    // Returns the entry for |key|, creating an empty one if absent. The
    // caller drives computation via std::call_once on the returned entry.
    std::shared_ptr<LookupCacheEntry> get_or_create(const std::string& key) {
        std::lock_guard<std::mutex> l(_mu);
        if (auto it = _index.find(key); it != _index.end()) {
            _entries.splice(_entries.begin(), _entries, it->second);
            return it->second->second;
        }
        auto entry = std::make_shared<LookupCacheEntry>();
        _entries.emplace_front(key, entry);
        _index[key] = _entries.begin();
        const size_t cap = static_cast<size_t>(std::max<int64_t>(1, config::secondary_index_reader_cache_capacity));
        while (_entries.size() > cap) {
            _index.erase(_entries.back().first);
            _entries.pop_back();
        }
        return entry;
    }

private:
    std::mutex _mu;
    std::list<std::pair<std::string, std::shared_ptr<LookupCacheEntry>>> _entries;
    std::unordered_map<std::string, decltype(_entries)::iterator> _index;
};

// Answers a covered query entirely from the .idx file: scans the index
// segment (predicate already pushed down into |idx_iter|), decodes
// __sidx_pos__ -> (seg_id, rowid), drops rows that DelVec marks deleted, and
// emits only the requested output columns -- no base-table readback.
class CoveringIndexIterator final : public ChunkIterator {
public:
    CoveringIndexIterator(Schema output_schema, ChunkIteratorPtr idx_iter, Schema idx_read_schema,
                          std::vector<int> out_to_inner, uint32_t pos_col_idx, int64_t tablet_id,
                          int64_t rowset_id_base, int64_t version, std::shared_ptr<DelvecLoader> delvec_loader,
                          int chunk_size)
            : ChunkIterator(std::move(output_schema), chunk_size),
              _idx_iter(std::move(idx_iter)),
              _idx_read_schema(std::move(idx_read_schema)),
              _out_to_inner(std::move(out_to_inner)),
              _pos_col_idx(pos_col_idx),
              _tablet_id(tablet_id),
              _rowset_id_base(rowset_id_base),
              _version(version),
              _delvec_loader(std::move(delvec_loader)) {}

    void close() override {
        if (_idx_iter != nullptr) _idx_iter->close();
    }

protected:
    Status do_get_next(Chunk* chunk) override {
        if (_idx_iter == nullptr) return Status::EndOfFile("covering: no index iterator");
        if (_idx_chunk == nullptr) {
            ASSIGN_OR_RETURN(_idx_chunk, RuntimeChunkHelper::new_chunk_checked(_idx_read_schema, chunk_size()));
        }
        std::vector<uint32_t> survivors;
        while (true) {
            _idx_chunk->reset();
            Status s = _idx_iter->get_next(_idx_chunk.get());
            if (s.is_end_of_file()) break;
            RETURN_IF_ERROR(s);
            const size_t n = _idx_chunk->num_rows();
            if (n == 0) continue;

            const auto* pos_col = down_cast<const Int64Column*>(_idx_chunk->get_column_by_index(_pos_col_idx).get());
            const auto& pos = pos_col->get_data();
            survivors.clear();
            survivors.reserve(n);
            for (size_t r = 0; r < n; ++r) {
                uint32_t seg = 0, rowid = 0;
                decode_position(pos[r], &seg, &rowid);
                // The inner .idx scan is already restricted to this morsel's
                // index-row slice, so every row read here belongs to exactly
                // this morsel -- no per-row morsel filter needed.
                bool deleted = false;
                RETURN_IF_ERROR(_is_deleted(seg, rowid, &deleted));
                if (!deleted) survivors.push_back(static_cast<uint32_t>(r));
            }
            if (survivors.empty()) continue;
            for (size_t of = 0; of < _out_to_inner.size(); ++of) {
                chunk->get_column_raw_ptr_by_index(of)->append_selective(
                        *_idx_chunk->get_column_raw_ptr_by_index(_out_to_inner[of]), survivors.data(), 0,
                        static_cast<uint32_t>(survivors.size()));
            }
            if (chunk->num_rows() > 0) return Status::OK();
        }
        return chunk->num_rows() > 0 ? Status::OK() : Status::EndOfFile("covering: eof");
    }

private:
    Status _is_deleted(uint32_t seg, uint32_t rowid, bool* deleted) {
        *deleted = false;
        if (_delvec_loader == nullptr) return Status::OK();
        const uint32_t gseg = static_cast<uint32_t>(_rowset_id_base) + seg;
        auto it = _delvec_cache.find(gseg);
        if (it == _delvec_cache.end()) {
            DelVectorPtr dv;
            TabletSegmentId tsid;
            tsid.tablet_id = _tablet_id;
            tsid.segment_id = gseg;
            RETURN_IF_ERROR(_delvec_loader->load(tsid, _version, &dv));
            it = _delvec_cache.emplace(gseg, std::move(dv)).first;
        }
        if (it->second != nullptr && !it->second->empty()) {
            *deleted = it->second->roaring()->contains(rowid);
        }
        return Status::OK();
    }

    ChunkIteratorPtr _idx_iter;
    Schema _idx_read_schema;
    std::vector<int> _out_to_inner;
    uint32_t _pos_col_idx;
    int64_t _tablet_id;
    int64_t _rowset_id_base;
    int64_t _version;
    std::shared_ptr<DelvecLoader> _delvec_loader;
    std::unordered_map<uint32_t, DelVectorPtr> _delvec_cache;
    ChunkPtr _idx_chunk;
};

} // namespace

SecondaryIndexReader::SecondaryIndexReader(std::shared_ptr<FileSystem> fs, lake::TabletManager* tablet_mgr,
                                           int64_t tablet_id, SecondaryIndexFilePB file_pb,
                                           TabletSchemaCSPtr source_schema)
        : _fs(std::move(fs)),
          _tablet_mgr(tablet_mgr),
          _tablet_id(tablet_id),
          _file_pb(std::move(file_pb)),
          _source_schema(std::move(source_schema)) {}

StatusOr<std::shared_ptr<SecondaryIndexReader>> SecondaryIndexReader::open(const OpenInput& input) {
    if (input.fs == nullptr || input.tablet_mgr == nullptr || input.source_schema == nullptr) {
        return Status::InvalidArgument("SecondaryIndexReader::open: missing fs/tablet_mgr/source_schema");
    }
    auto reader = std::shared_ptr<SecondaryIndexReader>(
            new SecondaryIndexReader(input.fs, input.tablet_mgr, input.tablet_id, input.file_pb, input.source_schema));
    RETURN_IF_ERROR(reader->_init());
    return reader;
}

StatusOr<std::shared_ptr<SecondaryIndexReader>> SecondaryIndexReader::open_cached(const OpenInput& input,
                                                                                  OlapReaderStatistics* stats) {
    if (input.file_pb.file_name().empty()) {
        // No filename to key on -- fall through to a non-cached open which
        // will surface a clearer error from _init().
        return open(input);
    }
    const std::string key = make_cache_key(input.tablet_id, input.file_pb.file_name());
    if (auto hit = ReaderCache::instance().get(key); hit != nullptr) {
        if (stats != nullptr) stats->secondary_index_cache_hits++;
        return hit;
    }
    MonotonicStopWatch watch;
    if (stats != nullptr) watch.start();
    ASSIGN_OR_RETURN(auto reader, open(input));
    if (stats != nullptr) {
        stats->secondary_index_open_ns += watch.elapsed_time();
        stats->secondary_index_cache_misses++;
        stats->secondary_index_files_opened++;
    }
    ReaderCache::instance().put(key, reader);
    return reader;
}

Status SecondaryIndexReader::_init() {
    // Resolve index column ids from the source schema.
    _source_index_col_ids.clear();
    _source_index_col_ids.reserve(_file_pb.index_col_names_size());
    for (int i = 0; i < _file_pb.index_col_names_size(); ++i) {
        const std::string& name = _file_pb.index_col_names(i);
        size_t idx = _source_schema->field_index(name);
        if (idx == static_cast<size_t>(-1)) {
            return Status::NotFound(fmt::format("secondary index references unknown source column: '{}'", name));
        }
        _source_index_col_ids.push_back(static_cast<uint32_t>(idx));
    }

    _index_schema = build_index_tablet_schema(*_source_schema, _source_index_col_ids, /*enable_bloom_filter=*/true);
    _encoded_pos_col_idx = static_cast<uint32_t>(_index_schema->num_columns() - 1);

    // Resolve full path and open the file as a Segment.
    const std::string full_path = _tablet_mgr->segment_location(_tablet_id, _file_pb.file_name());
    FileInfo info;
    info.path = full_path;
    info.size = _file_pb.file_size();
    ASSIGN_OR_RETURN(_segment, Segment::open(_fs, info, /*segment_id=*/0, _index_schema, /*footer_length_hint=*/nullptr,
                                             /*partial_rowset_footer=*/nullptr, LakeIOOptions{}, _tablet_mgr));
    return Status::OK();
}

StatusOr<PerSegmentRowidBitmap> SecondaryIndexReader::lookup(const PredicateTree& source_pred_tree,
                                                             ObjectPool* obj_pool, OlapReaderStatistics* stats) {
    PerSegmentRowidBitmap result;
    if (_segment == nullptr) return result;

    MonotonicStopWatch lookup_watch;
    if (stats != nullptr) lookup_watch.start();
    DeferOp lookup_timer([&] {
        if (stats != nullptr) stats->secondary_index_lookup_ns += lookup_watch.elapsed_time();
    });

    // Build a read schema covering every column in the index file: the
    // index columns get the user's predicates pushed down; the encoded
    // position column is read raw.
    Schema read_schema = ChunkHelper::convert_schema(_index_schema);

    // Inner stats for the .idx segment scan itself; kept separate from the
    // caller's |stats| (which aggregates the secondary-index path totals).
    OlapReaderStatistics idx_scan_stats;
    SegmentReadOptions read_opts;
    read_opts.fs = _fs;
    read_opts.stats = &idx_scan_stats;
    // Remap source-schema predicates into the synthetic index-file column
    // id space and push them into the inner segment iterator's pred_tree.
    // When obj_pool is null (e.g. tests) we fall back to an empty tree.
    if (obj_pool != nullptr && !source_pred_tree.empty()) {
        read_opts.pred_tree = build_remapped_predicate_tree(source_pred_tree, _source_index_col_ids, obj_pool);
        // Share the same remapped tree with SegmentZoneMapPruner so an .idx
        // segment whose min/max doesn't cover the predicate value returns
        // EndOfFile directly from Segment::_new_iterator -- saves footer
        // page loads and column-reader init for non-overlapping parts.
        read_opts.pred_tree_for_zone_map = read_opts.pred_tree;
    }

    ASSIGN_OR_RETURN(auto iter, _segment->new_iterator(read_schema, read_opts));
    if (iter == nullptr) return result;

    ASSIGN_OR_RETURN(auto chunk, RuntimeChunkHelper::new_chunk_checked(read_schema, kReadChunkSize));
    while (true) {
        chunk->reset();
        Status s = iter->get_next(chunk.get());
        if (s.is_end_of_file()) break;
        RETURN_IF_ERROR(s);
        const size_t n = chunk->num_rows();
        if (n == 0) continue;
        if (stats != nullptr) stats->secondary_index_rows_scanned += static_cast<int64_t>(n);

        auto pos_col_any = chunk->get_column_by_index(_encoded_pos_col_idx);
        // We constructed __sidx_pos__ as a non-nullable BIGINT, so the
        // resulting column should always be Int64Column. Use the const
        // overload because the column is held via the COW-immutable Ptr.
        const auto* pos_col = down_cast<const Int64Column*>(pos_col_any.get());
        const auto& data = pos_col->get_data();
        for (size_t r = 0; r < n; ++r) {
            uint32_t seg_id = 0;
            uint32_t rowid = 0;
            decode_position(data[r], &seg_id, &rowid);
            result[seg_id].add(rowid);
        }
    }
    return result;
}

StatusOr<std::shared_ptr<const PerSegmentRowidBitmap>> SecondaryIndexReader::lookup_cached(
        const std::string& cache_key, const PredicateTree& source_pred_tree, ObjectPool* obj_pool,
        OlapReaderStatistics* stats) {
    auto entry = LookupResultCache::instance().get_or_create(cache_key);
    // Exactly one caller computes; the rest block here and then share the
    // result. The computing caller pays the lookup cost in |stats|; waiters
    // record nothing, which correctly reflects that they did no .idx work.
    std::call_once(entry->once, [&] {
        auto res = lookup(source_pred_tree, obj_pool, stats);
        if (res.ok()) {
            entry->result = std::make_shared<const PerSegmentRowidBitmap>(std::move(res).value());
        } else {
            entry->result = res.status();
        }
    });
    return entry->result;
}

StatusOr<ChunkIteratorPtr> SecondaryIndexReader::make_covering_iterator(
        const Schema& output_schema, const PredicateTree& source_pred_tree, ObjectPool* obj_pool,
        int64_t rowset_id_base, int64_t version, std::shared_ptr<DelvecLoader> delvec_loader, int chunk_size,
        SparseRangePtr idx_rowid_range, OlapReaderStatistics* stats) {
    if (_segment == nullptr) {
        return Status::InternalError("make_covering_iterator: index segment not opened");
    }
    // Inner read schema covers all index columns + the encoded position; the
    // predicate is pushed down exactly as in lookup() so the .idx scan only
    // touches the matching range.
    Schema read_schema = ChunkHelper::convert_schema(_index_schema);
    SegmentReadOptions read_opts;
    read_opts.fs = _fs;
    read_opts.stats = stats;
    if (obj_pool != nullptr && !source_pred_tree.empty()) {
        read_opts.pred_tree = build_remapped_predicate_tree(source_pred_tree, _source_index_col_ids, obj_pool);
        read_opts.pred_tree_for_zone_map = read_opts.pred_tree;
    }
    // Restrict this morsel's scan to its slice of the .idx row positions so the
    // covering scan is parallel across morsels with no redundant rescans.
    if (idx_rowid_range != nullptr && !idx_rowid_range->empty()) {
        read_opts.rowid_range_option = std::move(idx_rowid_range);
    }
    ASSIGN_OR_RETURN(auto inner_iter, _segment->new_iterator(read_schema, read_opts));

    // Map each output field (by name) to its column index in the inner
    // (.idx) read schema. Caller guarantees every output column is an index
    // column, so a miss is a programming error.
    std::vector<int> out_to_inner;
    out_to_inner.reserve(output_schema.num_fields());
    for (size_t f = 0; f < static_cast<size_t>(output_schema.num_fields()); ++f) {
        const std::string fname(output_schema.field(f)->name());
        int found = -1;
        for (int j = 0; j < _file_pb.index_col_names_size(); ++j) {
            if (_file_pb.index_col_names(j) == fname) {
                found = j;
                break;
            }
        }
        if (found < 0) {
            return Status::InternalError(fmt::format("make_covering_iterator: output column '{}' not in index", fname));
        }
        out_to_inner.push_back(found);
    }

    if (inner_iter == nullptr) {
        // Zone-map pruned the whole .idx: nothing matches in this rowset.
        return Status::EndOfFile("covering: index segment pruned");
    }
    return std::make_shared<CoveringIndexIterator>(
            output_schema, std::move(inner_iter), std::move(read_schema), std::move(out_to_inner), _encoded_pos_col_idx,
            _tablet_id, rowset_id_base, version, std::move(delvec_loader), chunk_size);
}

} // namespace starrocks::secondary_sorted
