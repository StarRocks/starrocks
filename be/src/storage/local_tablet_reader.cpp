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

#include "storage/local_tablet_reader.h"

#include "gen_cpp/internal_service.pb.h"
#include "serde/protobuf_serde.h"
#include "storage/chunk_helper.h"
#include "storage/primary_index.h"
#include "storage/primary_key_encoder.h"
#include "storage/projection_iterator.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_updates.h"

namespace starrocks {

LocalTabletReader::LocalTabletReader() = default;

Status LocalTabletReader::init(TabletSharedPtr& tablet, int64_t version) {
    if (tablet->updates() == nullptr) {
        return Status::NotSupported("LocalTabletReader only support PK tablet");
    }
    _tablet = tablet;
    _version = version;
    return Status::OK();
}

Status LocalTabletReader::close() {
    return Status::OK();
}

static void plan_read_by_rssid(const vector<uint64_t>& rowids, vector<bool>& found,
                               std::map<uint32_t, std::vector<uint32_t>>& rowids_by_rssid, vector<uint32_t>& idxes) {
    struct RowidSortEntry {
        uint32_t rowid;
        uint32_t idx;
        RowidSortEntry(uint32_t rowid, uint32_t idx) : rowid(rowid), idx(idx) {}
        bool operator<(const RowidSortEntry& rhs) const { return rowid < rhs.rowid; }
    };

    size_t n = rowids.size();
    found.resize(n);
    phmap::node_hash_map<uint32_t, std::vector<RowidSortEntry>> sort_entry_by_rssid;
    uint32_t idx = 0;
    for (uint32_t i = 0; i < n; i++) {
        uint64_t v = rowids[i];
        uint32_t rssid = v >> 32;
        if (rssid == (uint32_t)-1) {
            found[i] = false;
        } else {
            found[i] = true;
            uint32_t rowid = v & ROWID_MASK;
            sort_entry_by_rssid[rssid].emplace_back(rowid, idx++);
        }
    }
    // construct rowids_by_rssid
    for (auto& e : sort_entry_by_rssid) {
        std::sort(e.second.begin(), e.second.end());
        rowids_by_rssid.emplace(e.first, std::vector<uint32_t>(e.second.size()));
    }
    idxes.resize(idx);
    // iterate rowids_by_rssid by rssid order
    uint32_t ridx = 0;
    for (auto& e : rowids_by_rssid) {
        auto& sort_entries = sort_entry_by_rssid[e.first];
        for (uint32_t i = 0; i < sort_entries.size(); i++) {
            e.second[i] = sort_entries[i].rowid;
            idxes[sort_entries[i].idx] = ridx;
            ridx++;
        }
    }
}

Status LocalTabletReader::multi_get(const Chunk& keys, const std::vector<std::string>& value_columns,
                                    std::vector<bool>& found, Chunk& values) {
    // get read column ids by values_columns
    const auto& tablet_schema = _tablet->tablet_schema();
    std::vector<uint32_t> value_column_ids;
    for (const auto& name : value_columns) {
        auto cid = tablet_schema->field_index(name);
        if (cid == -1) {
            return Status::InvalidArgument(strings::Substitute("multi_get value_column $0 not found", name));
        }
        value_column_ids.push_back(cid);
    }
    return multi_get(keys, value_column_ids, found, values);
}

Status LocalTabletReader::multi_get(const Chunk& keys, const std::vector<uint32_t>& value_column_ids,
                                    std::vector<bool>& found, Chunk& values) {
    int64_t t_start = MonotonicMillis();
    size_t n = keys.num_rows();
    if (n > UINT32_MAX) {
        return Status::InvalidArgument(
                strings::Substitute("multi_get number of keys exceed limit $0 > $1", n, UINT32_MAX));
    }

    // convert keys to pk single column format
    const auto& tablet_schema = _tablet->tablet_schema();
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    std::unique_ptr<Column> pk_column;
    if (!PrimaryKeyEncoder::create_column(*tablet_schema->schema(), &pk_column).ok()) {
        CHECK(false) << "create column for primary key encoder failed";
    }
    PrimaryKeyEncoder::encode(*tablet_schema->schema(), keys, 0, keys.num_rows(), pk_column.get());

    // search pks in pk index to get rowids
    EditVersion edit_version;
    std::vector<uint64_t> rowids(n);
    RETURN_IF_ERROR(_tablet->updates()->get_rss_rowids_by_pk(_tablet.get(), *pk_column, &edit_version, &rowids));
    if (edit_version.major_number() != _version) {
        return Status::InternalError(
                strings::Substitute("multi_get version not match tablet:$0 current_version:$1 read_version:$2",
                                    _tablet->tablet_id(), edit_version.to_string(), _version));
    }
    if (rowids.size() != n) {
        return Status::InternalError(strings::Substitute("multi_get rowid size not match tablet:$0 $1 != $2",
                                                         _tablet->tablet_id(), rowids.size(), n));
    }

    // sort rowids by rssid, so we can plan&perform read operations by rowset/segment
    std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
    vector<uint32_t> idxes;
    plan_read_by_rssid(rowids, found, rowids_by_rssid, idxes);

    auto read_column_schema = ChunkHelper::convert_schema(tablet_schema, value_column_ids);
    std::vector<std::unique_ptr<Column>> read_columns(value_column_ids.size());
    for (uint32_t i = 0; i < read_columns.size(); ++i) {
        read_columns[i] = ChunkHelper::column_from_field(*read_column_schema.field(i).get())->clone_empty();
    }
    RETURN_IF_ERROR(_tablet->updates()->get_column_values(value_column_ids, _version, false, rowids_by_rssid,
                                                          &read_columns, nullptr, tablet_schema));

    // reorder read values to input keys' order and put into values output parameter
    values.reset();
    for (size_t col_idx = 0; col_idx < value_column_ids.size(); col_idx++) {
        values.get_column_by_index(col_idx)->append_selective(*read_columns[col_idx], idxes.data(), 0, idxes.size());
    }
    int64_t t_end = MonotonicMillis();
    LOG(INFO) << strings::Substitute("multi_get tablet:$0 version:$1 #columns:$2 #rows:$3 found:$4 time:$5ms",
                                     _tablet->tablet_id(), _version, value_column_ids.size(), n, idxes.size(),
                                     t_end - t_start);
    return Status::OK();
}

StatusOr<ChunkIteratorPtr> LocalTabletReader::scan(const std::vector<std::string>& value_columns,
                                                   const std::vector<const ColumnPredicate*>& predicates) {
    TabletReaderParams tablet_reader_params;
    tablet_reader_params.predicates = predicates;
    auto& full_schema = *_tablet->tablet_schema()->schema();
    vector<ColumnId> column_ids;
    for (auto& cname : value_columns) {
        auto idx = full_schema.get_field_index_by_name(cname);
        if (idx == -1) {
            return Status::InvalidArgument(strings::Substitute("column $0 not found", cname));
        }
        column_ids.push_back(idx);
    }
    auto values_schema = Schema(&full_schema, column_ids);
    std::shared_ptr<TabletReader> reader = std::make_shared<TabletReader>(_tablet, Version(0, _version), full_schema);
    RETURN_IF_ERROR(reader->prepare());
    RETURN_IF_ERROR(reader->open(tablet_reader_params));
    // TODO: remove projection
    return new_projection_iterator(values_schema, reader);
}

Status handle_tablet_multi_get_rpc(const PTabletReaderMultiGetRequest& request, PTabletReaderMultiGetResult& result) {
    int64_t tablet_id = request.tablet_id();
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        return Status::InternalError(strings::Substitute("mult-get rpc failed, tablet $0 not found", tablet_id));
    }
    int64_t version = request.version();
    vector<string> value_columns(request.values_columns().begin(), request.values_columns().end());
    auto local_tablet_reader = std::make_unique<LocalTabletReader>();
    RETURN_IF_ERROR(local_tablet_reader->init(tablet, version));

    const auto& tablet_schema = tablet->tablet_schema();
    const auto& keys_pb = request.keys();
    vector<ColumnId> key_column_ids;
    for (size_t i = 0; i < tablet_schema->num_key_columns(); i++) {
        key_column_ids.push_back(i);
    }
    Schema key_schema(tablet_schema->schema(), key_column_ids);
    std::vector<uint32_t> value_column_ids;
    for (const auto& name : value_columns) {
        auto cid = tablet_schema->field_index(name);
        if (cid == -1) {
            return Status::InvalidArgument(strings::Substitute("multi_get value_column $0 not found", name));
        }
        value_column_ids.push_back(cid);
    }
    Schema values_schema(tablet_schema->schema(), value_column_ids);
    auto keys_st = serde::deserialize_chunk_pb_with_schema(key_schema, keys_pb.data());
    if (!keys_st.ok()) {
        return keys_st.status();
    }
    vector<bool> found;
    ChunkPtr values = ChunkHelper::new_chunk(values_schema, keys_st.value().num_rows());
    RETURN_IF_ERROR(local_tablet_reader->multi_get(keys_st.value(), value_column_ids, found, *values));
    auto* found_pb = result.mutable_found();
    found_pb->Reserve(found.size());
    for (auto f : found) {
        found_pb->Add(f);
    }
    StatusOr<ChunkPB> values_pb;
    TRY_CATCH_BAD_ALLOC(values_pb = serde::ProtobufChunkSerde::serialize_without_meta(*values, nullptr));
    if (!values_pb.ok()) {
        return values_pb.status();
    }
    result.mutable_values()->Swap(&values_pb.value());
    return Status::OK();
}

} // namespace starrocks