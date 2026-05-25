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

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/object_pool.h"
#include "fmt/format.h"
#include "fs/fs.h"
#include "runtime/chunk_helper.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/column_predicate.h"
#include "storage/index/secondary_sorted/predicate_remap.h"
#include "storage/index/secondary_sorted/types.h"
#include "storage/lake/tablet_manager.h"
#include "storage/olap_common.h"
#include "storage/predicate_tree/predicate_tree.h"
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

StatusOr<std::shared_ptr<SecondaryIndexReader>> SecondaryIndexReader::open_cached(const OpenInput& input) {
    if (input.file_pb.file_name().empty()) {
        // No filename to key on -- fall through to a non-cached open which
        // will surface a clearer error from _init().
        return open(input);
    }
    const std::string key = make_cache_key(input.tablet_id, input.file_pb.file_name());
    if (auto hit = ReaderCache::instance().get(key); hit != nullptr) {
        return hit;
    }
    ASSIGN_OR_RETURN(auto reader, open(input));
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
                                                             ObjectPool* obj_pool) {
    PerSegmentRowidBitmap result;
    if (_segment == nullptr) return result;

    // Build a read schema covering every column in the index file: the
    // index columns get the user's predicates pushed down; the encoded
    // position column is read raw.
    Schema read_schema = ChunkHelper::convert_schema(_index_schema);

    OlapReaderStatistics stats;
    SegmentReadOptions read_opts;
    read_opts.fs = _fs;
    read_opts.stats = &stats;
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

} // namespace starrocks::secondary_sorted
