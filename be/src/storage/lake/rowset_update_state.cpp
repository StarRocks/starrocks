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

#include "rowset_update_state.h"

#include "common/tracer.h"
#include "gutil/strings/substitute.h"
#include "serde/column_array_serde.h"
#include "storage/chunk_helper.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/rowset.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/segment_rewriter.h"
#include "storage/tablet_schema.h"
#include "util/defer_op.h"
#include "util/phmap/phmap.h"
#include "util/stack_util.h"
#include "util/time.h"

namespace starrocks {

namespace lake {

RowsetUpdateState::RowsetUpdateState() = default;

RowsetUpdateState::~RowsetUpdateState() {
    if (!_status.ok()) {
        LOG(WARNING) << "bad RowsetUpdateState released tablet:" << _tablet_id;
    }
}

Status RowsetUpdateState::load(const TxnLogPB_OpWrite& op_write, const TabletMetadata& metadata, int64_t base_version,
                               Tablet* tablet, const MetaFileBuilder* builder) {
    if (UNLIKELY(!_status.ok())) {
        return _status;
    }
    std::call_once(_load_once_flag, [&] {
        _base_version = base_version;
        _builder = builder;
        _status = _do_load(op_write, metadata, tablet);
        if (!_status.ok()) {
            LOG(WARNING) << "load RowsetUpdateState error: " << _status << " tablet:" << _tablet_id << " stack:\n"
                         << get_stack_trace();
            if (_status.is_mem_limit_exceeded()) {
                LOG(WARNING) << CurrentThread::mem_tracker()->debug_string();
            }
        }
    });
    return _status;
}

Status RowsetUpdateState::_do_load(const TxnLogPB_OpWrite& op_write, const TabletMetadata& metadata, Tablet* tablet) {
    std::stringstream cost_str;
    MonotonicStopWatch watch;
    watch.start();

    std::unique_ptr<TabletSchema> tablet_schema = std::make_unique<TabletSchema>(metadata.schema());

    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(*tablet_schema, pk_columns);
    std::unique_ptr<Column> pk_column;
    if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
        CHECK(false) << "create column for primary key encoder failed";
    }

    auto root_path = tablet->metadata_root_location();
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_path));
    // always one file for now.
    for (const std::string& path : op_write.dels()) {
        ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(tablet->del_location(path)));
        ASSIGN_OR_RETURN(auto file_size, read_file->get_size());
        std::vector<uint8_t> read_buffer(file_size);
        RETURN_IF_ERROR(read_file->read_at_fully(0, read_buffer.data(), read_buffer.size()));
        auto col = pk_column->clone();
        if (serde::ColumnArraySerde::deserialize(read_buffer.data(), col.get()) == nullptr) {
            return Status::InternalError("column deserialization failed");
        }
        _deletes.emplace_back(std::move(col));
    }
    cost_str << " [read deletes] " << watch.elapsed_time();
    watch.reset();

    std::unique_ptr<Rowset> rowset_ptr =
            std::make_unique<Rowset>(tablet, std::make_shared<RowsetMetadataPB>(op_write.rowset()));
    OlapReaderStatistics stats;
    auto res = rowset_ptr->get_each_segment_iterator(pkey_schema, &stats);
    if (!res.ok()) {
        return res.status();
    }
    auto& itrs = res.value();
    CHECK(itrs.size() == rowset_ptr->num_segments())
            << "itrs.size != num_segments " << itrs.size() << ", " << rowset_ptr->num_segments();
    _upserts.resize(rowset_ptr->num_segments());
    // only hold pkey, so can use larger chunk size
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, 4096);
    auto chunk = chunk_shared_ptr.get();
    for (size_t i = 0; i < itrs.size(); i++) {
        auto& dest = _upserts[i];
        auto col = pk_column->clone();
        auto itr = itrs[i].get();
        if (itr != nullptr) {
            auto num_rows = rowset_ptr->num_rows();
            col->reserve(num_rows);
            while (true) {
                chunk->reset();
                auto st = itr->get_next(chunk);
                if (st.is_end_of_file()) {
                    break;
                } else if (!st.ok()) {
                    return st;
                } else {
                    PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(), col.get());
                }
            }
            itr->close();
        }
        dest = std::move(col);
    }
    cost_str << " [read upserts] " << watch.elapsed_time();
    LOG(INFO) << "RowsetUpdateState do_load cost: " << cost_str.str();

    for (const auto& upsert : _upserts) {
        _memory_usage += upsert != nullptr ? upsert->memory_usage() : 0;
    }
    for (const auto& one_delete : _deletes) {
        _memory_usage += one_delete != nullptr ? one_delete->memory_usage() : 0;
    }
    if (!op_write.has_txn_meta() || rowset_ptr->num_segments() == 0 || op_write.txn_meta().has_merge_condition()) {
        return Status::OK();
    }
    return _prepare_partial_update_states(op_write, metadata, tablet, *tablet_schema);
}

struct RowidSortEntry {
    uint32_t rowid;
    uint32_t idx;
    RowidSortEntry(uint32_t rowid, uint32_t idx) : rowid(rowid), idx(idx) {}
    bool operator<(const RowidSortEntry& rhs) const { return rowid < rhs.rowid; }
};

// group rowids by rssid, and for each group sort by rowid, return as `rowids_by_rssid`
// num_default: return the number of rows that need to fill in default values
// idxes: reverse indexes to restore values from (rssid,rowid) order to rowid order
// i.e.
// input rowids: [
//    (0, 3),
//    (-1,-1),
//    (1, 3),
//    (0, 1),
//    (-1,-1),
//    (1, 2),
// ]
// output:
//   num_default: 2
//   rowids_by_rssid: {
//     0: [
//        1,
//        3
//     ],
//     1: [
//        2,
//        3
//	   ]
//   }
//   the read column values will be in this order: [default_value, (0,1), (0,3), (1,2), (1,3)]
//   the indexes used to convert read columns values to write order will be: [2, 0, 4, 1, 0, 3]
void RowsetUpdateState::plan_read_by_rssid(const std::vector<uint64_t>& rowids, size_t* num_default,
                                           std::map<uint32_t, std::vector<uint32_t>>* rowids_by_rssid,
                                           std::vector<uint32_t>* idxes) {
    uint32_t n = rowids.size();
    phmap::node_hash_map<uint32_t, vector<RowidSortEntry>> sort_entry_by_rssid;
    std::vector<uint32_t> defaults;
    for (uint32_t i = 0; i < n; i++) {
        uint64_t v = rowids[i];
        uint32_t rssid = v >> 32;
        if (rssid == (uint32_t)-1) {
            defaults.push_back(i);
        } else {
            uint32_t rowid = v & ROWID_MASK;
            sort_entry_by_rssid[rssid].emplace_back(rowid, i);
        }
    }
    *num_default = defaults.size();
    idxes->resize(rowids.size());
    size_t ridx = 0;
    if (defaults.size() > 0) {
        // set defaults idxes to 0
        for (uint32_t e : defaults) {
            (*idxes)[e] = ridx;
        }
        ridx++;
    }
    // construct rowids_by_rssid
    for (auto& e : sort_entry_by_rssid) {
        std::sort(e.second.begin(), e.second.end());
        rowids_by_rssid->emplace(e.first, vector<uint32_t>(e.second.size()));
    }
    // iterate rowids_by_rssid by rssid order
    for (auto& e : *rowids_by_rssid) {
        auto& sort_entries = sort_entry_by_rssid[e.first];
        for (uint32_t i = 0; i < sort_entries.size(); i++) {
            e.second[i] = sort_entries[i].rowid;
            (*idxes)[sort_entries[i].idx] = ridx;
            ridx++;
        }
    }
}

Status RowsetUpdateState::_prepare_partial_update_states(const TxnLogPB_OpWrite& op_write,
                                                         const TabletMetadata& metadata, Tablet* tablet,
                                                         const TabletSchema& tablet_schema) {
    int64_t t_start = MonotonicMillis();
    const auto& txn_meta = op_write.txn_meta();

    std::vector<uint32_t> update_column_ids(txn_meta.partial_update_column_ids().begin(),
                                            txn_meta.partial_update_column_ids().end());
    std::set<uint32_t> update_columns_set(update_column_ids.begin(), update_column_ids.end());

    std::vector<uint32_t> read_column_ids;
    for (uint32_t i = 0; i < tablet_schema.num_columns(); i++) {
        if (update_columns_set.find(i) == update_columns_set.end()) {
            read_column_ids.push_back(i);
        }
    }

    auto read_column_schema = ChunkHelper::convert_schema(tablet_schema, read_column_ids);
    size_t num_segments = op_write.rowset().segments_size();
    // segment id -> column list
    std::vector<std::vector<std::unique_ptr<Column>>> read_columns;
    read_columns.resize(num_segments);
    _partial_update_states.resize(num_segments);
    for (size_t i = 0; i < num_segments; i++) {
        read_columns[i].resize(read_column_ids.size());
        _partial_update_states[i].write_columns.resize(read_columns[i].size());
        _partial_update_states[i].src_rss_rowids.resize(_upserts[i]->size());
        for (uint32_t j = 0; j < read_columns[i].size(); ++j) {
            auto column = ChunkHelper::column_from_field(*read_column_schema.field(j).get());
            read_columns[i][j] = column->clone_empty();
            _partial_update_states[i].write_columns[j] = column->clone_empty();
        }
    }

    int64_t t_read_index = MonotonicMillis();
    // segment id -> [rowids list]
    std::vector<std::vector<uint64_t>*> rss_rowids;
    rss_rowids.resize(num_segments);
    for (size_t i = 0; i < num_segments; ++i) {
        rss_rowids[i] = &(_partial_update_states[i].src_rss_rowids);
    }
    DCHECK_EQ(_upserts.size(), num_segments);
    // use upserts to get rowids in each segment
    RETURN_IF_ERROR(tablet->update_mgr()->get_rowids_from_pkindex(tablet, metadata, _upserts, _base_version, _builder,
                                                                  &rss_rowids));

    int64_t t_read_values = MonotonicMillis();
    size_t total_rows = 0;
    // rows actually needed to be read, excluding rows with default values
    size_t total_nondefault_rows = 0;
    for (size_t i = 0; i < num_segments; i++) {
        size_t num_default = 0;
        std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
        vector<uint32_t> idxes;
        plan_read_by_rssid(_partial_update_states[i].src_rss_rowids, &num_default, &rowids_by_rssid, &idxes);
        total_rows += _partial_update_states[i].src_rss_rowids.size();
        total_nondefault_rows += _partial_update_states[i].src_rss_rowids.size() - num_default;
        // get column values by rowid, also get default values if needed
        RETURN_IF_ERROR(tablet->update_mgr()->get_column_values(tablet, metadata, op_write, tablet_schema,
                                                                read_column_ids, num_default > 0, rowids_by_rssid,
                                                                &read_columns[i]));
        for (size_t col_idx = 0; col_idx < read_column_ids.size(); col_idx++) {
            _partial_update_states[i].write_columns[col_idx]->append_selective(*read_columns[i][col_idx], idxes.data(),
                                                                               0, idxes.size());
        }
        // release read column memory
        read_columns[i].clear();
    }
    int64_t t_end = MonotonicMillis();

    LOG(INFO) << strings::Substitute(
            "prepare lake PartialUpdateState tablet:$0 #segment:$1 #row:$2(#non-default:$3) #column:$4 "
            "time:$5ms(index:$6/value:$7)",
            _tablet_id, num_segments, total_rows, total_nondefault_rows, read_columns.size(), t_end - t_start,
            t_read_values - t_read_index, t_end - t_read_values);
    return Status::OK();
}

Status RowsetUpdateState::rewrite_segment(const TxnLogPB_OpWrite& op_write, const TabletMetadata& metadata,
                                          Tablet* tablet) {
    MonotonicStopWatch watch;
    watch.start();
    // const_cast for paritial update to rewrite segment file in op_write
    RowsetMetadata* rowset_meta = const_cast<TxnLogPB_OpWrite*>(&op_write)->mutable_rowset();
    auto root_path = tablet->metadata_root_location();
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_path));
    std::unique_ptr<TabletSchema> tablet_schema = std::make_unique<TabletSchema>(metadata.schema());
    // get rowset schema
    if (!op_write.has_txn_meta() || op_write.rewrite_segments_size() == 0 || rowset_meta->segments_size() == 0 ||
        op_write.txn_meta().has_merge_condition()) {
        return Status::OK();
    }
    CHECK(op_write.rewrite_segments_size() == rowset_meta->segments_size());
    // currently assume it's a partial update
    const auto& txn_meta = op_write.txn_meta();
    // columns supplied in rowset
    std::vector<uint32_t> update_colum_ids(txn_meta.partial_update_column_ids().begin(),
                                           txn_meta.partial_update_column_ids().end());
    std::set<uint32_t> update_columns_set(update_colum_ids.begin(), update_colum_ids.end());
    // columns needs to be read from tablet's data
    std::vector<uint32_t> read_column_ids;
    for (uint32_t i = 0; i < tablet_schema->num_columns(); i++) {
        if (update_columns_set.find(i) == update_columns_set.end()) {
            read_column_ids.push_back(i);
        }
    }

    for (int i = 0; i < rowset_meta->segments_size(); i++) {
        auto src_path = rowset_meta->segments(i);
        auto dest_path = op_write.rewrite_segments(i);

        int64_t t_rewrite_start = MonotonicMillis();
        FooterPointerPB partial_rowset_footer = txn_meta.partial_rowset_footers(i);
        // if rewrite fail, let segment gc to clean dest segment file
        RETURN_IF_ERROR(SegmentRewriter::rewrite(tablet->segment_location(src_path),
                                                 tablet->segment_location(dest_path), *tablet_schema, read_column_ids,
                                                 _partial_update_states[i].write_columns, i, partial_rowset_footer));
        int64_t t_rewrite_end = MonotonicMillis();
        LOG(INFO) << strings::Substitute(
                "lake apply partial segment tablet:$0 rowset:$1 seg:$2 #column:$3 #rewrite:$4ms [$5 -> $6]",
                tablet->id(), rowset_meta->id(), i, read_column_ids.size(), t_rewrite_end - t_rewrite_start, src_path,
                dest_path);
    }

    // rename segment file
    for (int i = 0; i < rowset_meta->segments_size(); i++) {
        rowset_meta->set_segments(i, op_write.rewrite_segments(i));
    }

    if (watch.elapsed_time() > /*100ms=*/100 * 1000 * 1000) {
        LOG(INFO) << "RowsetUpdateState rewrite_segment cost(ms): " << watch.elapsed_time() / 1000000;
    }
    return Status::OK();
}

std::string RowsetUpdateState::to_string() const {
    return strings::Substitute("RowsetUpdateState tablet:$0", _tablet_id);
}

} // namespace lake

} // namespace starrocks
