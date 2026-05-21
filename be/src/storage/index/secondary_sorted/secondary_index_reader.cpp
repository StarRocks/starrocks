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

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/config.h"
#include "common/logging.h"
#include "fs/fs.h"
#include "runtime/chunk_helper.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/column_predicate.h"
#include "storage/index/secondary_sorted/types.h"
#include "storage/lake/tablet_manager.h"
#include "storage/olap_common.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"

namespace starrocks::secondary_sorted {

namespace {

constexpr size_t kReadChunkSize = 4096;

// Reuse the same synthetic-schema construction as SecondaryIndexWriter so
// reader and writer agree on the on-disk layout. Kept in sync via a single
// helper here.
TabletSchemaSPtr build_index_schema_from_source(const TabletSchema& source_schema,
                                                const std::vector<uint32_t>& index_col_ids) {
    auto schema = std::make_shared<TabletSchema>();
    schema->set_id(TabletSchema::invalid_id());
    std::vector<ColumnId> sort_key_idxes;
    int32_t next_unique_id = 1;
    for (size_t i = 0; i < index_col_ids.size(); ++i) {
        TabletColumn col(source_schema.column(index_col_ids[i]));
        col.set_unique_id(next_unique_id++);
        col.set_is_key(true);
        col.set_is_sort_key(true);
        col.set_aggregation(STORAGE_AGGREGATE_NONE);
        col.set_is_bf_column(false);
        col.set_has_bitmap_index(false);
        schema->append_column(std::move(col));
        sort_key_idxes.push_back(static_cast<ColumnId>(i));
    }
    TabletColumn pos_col(STORAGE_AGGREGATE_NONE, TYPE_BIGINT, /*is_nullable=*/false, next_unique_id++,
                        sizeof(int64_t));
    pos_col.set_name(kEncodedPositionColumnName);
    pos_col.set_is_key(false);
    pos_col.set_is_sort_key(false);
    schema->append_column(std::move(pos_col));
    schema->set_sort_key_idxes(std::move(sort_key_idxes));
    schema->set_num_short_key_columns(static_cast<uint16_t>(index_col_ids.size()));
    return schema;
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
    auto reader = std::shared_ptr<SecondaryIndexReader>(new SecondaryIndexReader(
            input.fs, input.tablet_mgr, input.tablet_id, input.file_pb, input.source_schema));
    RETURN_IF_ERROR(reader->_init());
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
            return Status::NotFound(
                    fmt::format("secondary index references unknown source column: '{}'", name));
        }
        _source_index_col_ids.push_back(static_cast<uint32_t>(idx));
    }

    _index_schema = build_index_schema_from_source(*_source_schema, _source_index_col_ids);
    _encoded_pos_col_idx = static_cast<uint32_t>(_index_schema->num_columns() - 1);

    // Resolve full path and open the file as a Segment.
    const std::string full_path = _tablet_mgr->segment_location(_tablet_id, _file_pb.file_name());
    FileInfo info;
    info.path = full_path;
    info.size = _file_pb.file_size();
    ASSIGN_OR_RETURN(_segment,
                     Segment::open(_fs, info, /*segment_id=*/0, _index_schema, /*footer_length_hint=*/nullptr,
                                    /*partial_rowset_footer=*/nullptr, LakeIOOptions{}, _tablet_mgr));
    return Status::OK();
}

StatusOr<PerSegmentRowidBitmap> SecondaryIndexReader::lookup(
        const std::vector<const ColumnPredicate*>& index_col_predicates) {
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
    // PoC v1: predicate translation from source-schema column ids to the
    // synthetic index-file column ids is not implemented; leave pred_tree
    // empty so the lookup scans the entire index file. v2 will fill this
    // in to actually narrow the candidate set.
    (void)index_col_predicates;

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
