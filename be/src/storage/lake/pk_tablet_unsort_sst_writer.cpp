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

#include "storage/lake/pk_tablet_unsort_sst_writer.h"

#include <fmt/format.h>

#include "column/chunk.h"
#include "common/config_cache_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_rowset_fwd.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "platform/key_cache.h"
#include "runtime/mem_tracker.h"
#include "storage/lake/filenames.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/vacuum.h"
#include "storage/sstable/comparator.h"
#include "storage/sstable/filter_policy.h"
#include "storage/sstable/iterator.h"
#include "storage/sstable/merger.h"
#include "storage/sstable/options.h"
#include "storage/sstable/table.h"
#include "storage/sstable/table_builder.h"
#include "storage/storage_metrics.h"
#include "types/datum.h"

namespace starrocks::lake {

Status PkTabletUnsortSSTWriter::reset_sst_writer(const std::shared_ptr<LocationProvider>& location_provider,
                                                 const std::shared_ptr<FileSystem>& fs) {
    _location_provider = location_provider;
    _fs = fs;
    WritableFileOptions wopts;
    _encryption_meta.clear();
    if (config::enable_transparent_data_encryption) {
        ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
        wopts.encryption_info = pair.info;
        _encryption_meta = std::move(pair.encryption_meta);
    }
    if (location_provider && fs) {
        ASSIGN_OR_RETURN(_wf,
                         fs->new_writable_file(wopts, location_provider->sst_location(_tablet_id, gen_sst_filename())));
    } else {
        ASSIGN_OR_RETURN(_wf, fs::new_writable_file(wopts, _tablet_mgr->sst_location(_tablet_id, gen_sst_filename())));
    }
    _map.clear();
    _deleted_rowids.clear();
    _delete_keys.reset();
    _intermediate_ssts.clear();
    _map_mem_usage = 0;
    _next_rowid = 0;
    return Status::OK();
}

void PkTabletUnsortSSTWriter::collect_delete_key(const Slice& key) {
    // A map/merge key is already an encoded-PK slice, i.e. exactly one del-file binary cell, so append
    // it straight into the del-file column (lazily built in the same encoded-PK format as the SST keys).
    if (_delete_keys == nullptr) {
        _delete_keys = clone_empty_pk_column();
    }
    _delete_keys->append_datum(Datum(key));
}

Status PkTabletUnsortSSTWriter::append_sst_record(const Chunk& data, const std::vector<uint64_t>* rssid_rowids,
                                                  const std::vector<uint32_t>* column_indexes) {
    return append_records(data, rssid_rowids, column_indexes, /*is_delete=*/false);
}

Status PkTabletUnsortSSTWriter::append_delete_records(const Chunk& data, const std::vector<uint64_t>* rssid_rowids,
                                                      const std::vector<uint32_t>* column_indexes) {
    return append_records(data, rssid_rowids, column_indexes, /*is_delete=*/true);
}

void PkTabletUnsortSSTWriter::reconcile_entry(std::string_view key, uint64_t order, uint32_t rowid) {
    // Keep the row with the largest order (last op wins). When an UPSERT loses (its rowid is a real
    // segment position, not kDeleteRowid), record it so publish masks it with a delete vector. A
    // DELETE has no segment row (kDeleteRowid), so a losing/superseded DELETE contributes nothing.
    // The find is heterogeneous (std::less<> transparent comparator), so a duplicate primary key does
    // not allocate; only a first-seen key materializes the owning std::string on the emplace branch.
    auto it = _map.find(key);
    if (it == _map.end()) {
        // Rough per-entry footprint: encoded key bytes + value + btree node overhead.
        static constexpr size_t kBtreeEntryOverhead = 24;
        _map_mem_usage += key.size() + sizeof(Entry) + kBtreeEntryOverhead;
        _map.emplace(std::string(key), Entry{order, rowid});
    } else if (order > it->second.order) {
        if (it->second.rowid != kDeleteRowid) {
            _deleted_rowids.push_back(it->second.rowid);
        }
        it->second = Entry{order, rowid};
    } else if (rowid != kDeleteRowid) {
        _deleted_rowids.push_back(rowid);
    }
}

Status PkTabletUnsortSSTWriter::append_records(const Chunk& data, const std::vector<uint64_t>* rssid_rowids,
                                               const std::vector<uint32_t>* column_indexes, bool is_delete) {
    if (_wf == nullptr) {
        return Status::InternalError("unsort pk sst writer not initialized");
    }
    const size_t num_rows = data.num_rows();
    if (num_rows == 0) {
        return Status::OK();
    }
    if (rssid_rowids == nullptr || rssid_rowids->size() != num_rows) {
        return Status::InternalError(fmt::format("unsort pk sst writer requires per-row order keys, expected {} got {}",
                                                 num_rows, rssid_rowids == nullptr ? 0 : rssid_rowids->size()));
    }
    // Vertical compaction's key group carries [sort-key columns, then PK columns], so the PK columns
    // are NOT at chunk positions [0, num_key). Project them out (in primary-key order) using
    // `column_indexes` -- the tablet column id of each chunk column -- so encode_pk_keys, which
    // assumes the chunk's first num_key columns are the PK, sees the right columns. The load path
    // passes the full row without column_indexes and is used directly.
    Chunk pk_chunk;
    const Chunk* pk_data = &data;
    if (column_indexes != nullptr) {
        RETURN_IF_ERROR(project_pk_columns(data, *column_indexes, &pk_chunk));
        pk_data = &pk_chunk;
    }
    Buffer<Slice> keys;
    MutableColumnPtr owned_column;
    ASSIGN_OR_RETURN(const Slice* vkeys, encode_pk_keys(*pk_data, &keys, &owned_column));
    for (size_t i = 0; i < num_rows; i++) {
        const uint64_t order = (*rssid_rowids)[i];
        // DELETE rows occupy no segment position; they only carry order for the per-PK reconciliation.
        const uint32_t rowid = is_delete ? kDeleteRowid : (_next_rowid + static_cast<uint32_t>(i));
        reconcile_entry(vkeys[i], order, rowid);
    }
    if (!is_delete) {
        _next_rowid += static_cast<uint32_t>(num_rows);
    }
    // Bound memory: once the map reaches the memtable threshold, spill it to an intermediate SST.
    // The residual map plus all intermediates are k-way merged into the final SST at flush time.
    if (is_map_full()) {
        RETURN_IF_ERROR(flush_map_to_intermediate_sst());
    }
    return Status::OK();
}

Status PkTabletUnsortSSTWriter::project_pk_columns(const Chunk& data, const std::vector<uint32_t>& column_indexes,
                                                   Chunk* out) const {
    const size_t num_key = _tablet_schema_ptr->num_key_columns();
    for (uint32_t pk_col = 0; pk_col < num_key; ++pk_col) {
        // Tablet column ids [0, num_key) are the primary-key columns; find which chunk column carries
        // this one. The scan is over the (small) key-group width, once per chunk.
        size_t pos = column_indexes.size();
        for (size_t i = 0; i < column_indexes.size(); ++i) {
            if (column_indexes[i] == pk_col) {
                pos = i;
                break;
            }
        }
        if (pos == column_indexes.size()) {
            return Status::InternalError(
                    fmt::format("unsort pk sst writer: primary-key column {} missing from key group", pk_col));
        }
        out->append_column(data.get_column_by_index(pos), static_cast<SlotId>(pk_col));
    }
    return Status::OK();
}

bool PkTabletUnsortSSTWriter::is_map_full() const {
    // Count the loser rowids too, not just the map: a dup-heavy batch can hold a lot of memory in
    // _deleted_rowids (which a map spill does not shrink) while _map itself stays small, so spilling the
    // map keeps the writer's combined footprint near the bound instead of letting _map independently
    // pile another l0_max_mem_usage on top of the loser vector. (_delete_keys is filled only at flush,
    // never during append, so it is not part of the footprint at this spill check.)
    const size_t mem_usage = _map_mem_usage + _deleted_rowids.size() * sizeof(uint32_t);
    if (mem_usage >= static_cast<size_t>(config::l0_max_mem_usage)) {
        return true;
    }
    // Under memory pressure, spill at a lower bound, mirroring LakePersistentIndex::is_memtable_full.
    auto* update_mgr = _tablet_mgr->update_mgr();
    if (update_mgr != nullptr && update_mgr->mem_tracker() != nullptr &&
        update_mgr->mem_tracker()->limit_exceeded_by_ratio(config::memory_urgent_level) &&
        mem_usage >= static_cast<size_t>(config::l0_min_mem_usage)) {
        return true;
    }
    return false;
}

Status PkTabletUnsortSSTWriter::flush_map_to_intermediate_sst() {
    if (_map.empty()) {
        return Status::OK();
    }
    WritableFileOptions wopts;
    std::string encryption_meta;
    if (config::enable_transparent_data_encryption) {
        ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
        wopts.encryption_info = pair.info;
        encryption_meta = std::move(pair.encryption_meta);
    }
    const std::string location = (_location_provider && _fs)
                                         ? _location_provider->sst_location(_tablet_id, gen_sst_filename())
                                         : _tablet_mgr->sst_location(_tablet_id, gen_sst_filename());
    std::unique_ptr<WritableFile> wf;
    if (_location_provider && _fs) {
        ASSIGN_OR_RETURN(wf, _fs->new_writable_file(wopts, location));
    } else {
        ASSIGN_OR_RETURN(wf, fs::new_writable_file(wopts, location));
    }
    std::unique_ptr<sstable::FilterPolicy> filter_policy;
    filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
    sstable::Options options;
    options.filter_policy = filter_policy.get();
    sstable::TableBuilder builder(options, wf.get());
    // Intermediate SST stores both rowid and the dedup order (in the value's version field) so the
    // final merge can pick the winner across intermediate SSTs.
    for (const auto& [k, v] : _map) {
        IndexValuesWithVerPB index_value_pb;
        auto* value = index_value_pb.add_values();
        value->set_rowid(v.rowid);
        value->set_version(static_cast<int64_t>(v.order));
        RETURN_IF_ERROR(builder.Add(Slice(k), Slice(index_value_pb.SerializeAsString())));
    }
    RETURN_IF_ERROR(builder.Finish());
    const uint64_t size = builder.FileSize();
    RETURN_IF_ERROR(wf->close());
    _intermediate_ssts.push_back({location, size, std::move(encryption_meta)});
    _map.clear();
    _map_mem_usage = 0;
    return Status::OK();
}

Status PkTabletUnsortSSTWriter::merge_intermediates_into(sstable::TableBuilder* builder) {
    // Open every intermediate SST for reading. `rfs`/`tables` own the files/tables for the whole
    // merge. `iter_holders` owns the child iterators ONLY until the merging iterator is built: the
    // MergingIterator returned by NewMergingIterator takes ownership of the children and deletes them
    // in its destructor (via IteratorWrapper), so we release the holders once it is created to avoid
    // freeing each child iterator twice (a double-free that crashes under real allocators). The
    // holders exist solely so a failed Table::Open before that point still cleans up.
    std::vector<std::unique_ptr<RandomAccessFile>> rfs;
    std::vector<std::unique_ptr<sstable::Table>> tables;
    std::vector<std::unique_ptr<sstable::Iterator>> iter_holders;
    std::vector<sstable::Iterator*> child_iters;
    for (const auto& sst : _intermediate_ssts) {
        RandomAccessFileOptions opts;
        if (!sst.encryption_meta.empty()) {
            ASSIGN_OR_RETURN(opts.encryption_info, KeyCache::instance().unwrap_encryption_meta(sst.encryption_meta));
        }
        std::unique_ptr<RandomAccessFile> rf;
        if (_fs) {
            ASSIGN_OR_RETURN(rf, _fs->new_random_access_file(opts, sst.location));
        } else {
            ASSIGN_OR_RETURN(rf, fs::new_random_access_file(opts, sst.location));
        }
        std::unique_ptr<sstable::Table> table;
        RETURN_IF_ERROR(sstable::Table::Open(sstable::Options{}, rf.get(), sst.size, table));
        auto* iter = table->NewIterator(sstable::ReadOptions{});
        iter_holders.emplace_back(iter);
        child_iters.push_back(iter);
        rfs.push_back(std::move(rf));
        tables.push_back(std::move(table));
    }
    std::unique_ptr<sstable::Iterator> merged(sstable::NewMergingIterator(
            sstable::BytewiseComparator(), child_iters.data(), static_cast<int>(child_iters.size())));
    // Ownership of the child iterators has transferred to `merged` (see comment above); drop our
    // holders so they are not deleted a second time when this function returns.
    for (auto& holder : iter_holders) {
        (void)holder.release();
    }
    for (merged->SeekToFirst(); merged->Valid();) {
        const std::string key = merged->key().to_string();
        bool has_best = false;
        uint64_t best_order = 0;
        uint32_t best_rowid = 0;
        // All entries sharing this key are adjacent in the merged stream. Keep the one with the
        // largest order (last flushed wins); the rest are dedup losers -> delete vector.
        while (merged->Valid() && merged->key() == Slice(key)) {
            const Slice value = merged->value();
            IndexValuesWithVerPB index_value_pb;
            if (!index_value_pb.ParseFromArray(value.data, static_cast<int>(value.size)) ||
                index_value_pb.values_size() == 0) {
                return Status::InternalError("failed to parse intermediate unsort sst value");
            }
            const uint64_t order = static_cast<uint64_t>(index_value_pb.values(0).version());
            const uint32_t rowid = index_value_pb.values(0).rowid();
            if (!has_best || order > best_order) {
                // A losing UPSERT (real rowid) is masked by the delete vector; a losing DELETE
                // (kDeleteRowid) has no segment row and is simply dropped.
                if (has_best && best_rowid != kDeleteRowid) {
                    _deleted_rowids.push_back(best_rowid);
                }
                has_best = true;
                best_order = order;
                best_rowid = rowid;
            } else if (rowid != kDeleteRowid) {
                _deleted_rowids.push_back(rowid);
            }
            merged->Next();
        }
        if (best_rowid == kDeleteRowid) {
            // Latest op for this key is a DELETE: collect it into the del file (see collect_delete_key)
            // rather than the SST, so publish erases the key -- including a row from an earlier txn.
            collect_delete_key(Slice(key));
        } else {
            IndexValuesWithVerPB out;
            out.add_values()->set_rowid(best_rowid);
            RETURN_IF_ERROR(builder->Add(Slice(key), Slice(out.SerializeAsString())));
        }
    }
    return merged->status();
}

StatusOr<std::pair<FileInfo, PersistentIndexSstableRangePB>> PkTabletUnsortSSTWriter::flush_sst_writer() {
    if (_wf == nullptr) {
        return Status::InternalError("unsort pk sst writer not initialized");
    }
    std::unique_ptr<sstable::FilterPolicy> filter_policy;
    filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
    sstable::Options options;
    options.filter_policy = filter_policy.get();
    sstable::TableBuilder builder(options, _wf.get());
    if (_intermediate_ssts.empty()) {
        // Whole segment fit in memory: write the sorted map directly. Keys are added in encoded-PK
        // order as required by TableBuilder. Only the rowid is stored; rssid/version are projected
        // from shared_rssid/shared_version at ingest time (single-segment SST).
        for (const auto& [k, v] : _map) {
            if (v.rowid == kDeleteRowid) {
                // Latest op for this key is a DELETE: collect it into the del file (see collect_delete_key)
                // rather than the SST, so publish erases the key -- including a row from an earlier txn.
                collect_delete_key(Slice(k));
                continue;
            }
            IndexValuesWithVerPB index_value_pb;
            index_value_pb.add_values()->set_rowid(v.rowid);
            RETURN_IF_ERROR(builder.Add(Slice(k), Slice(index_value_pb.SerializeAsString())));
        }
    } else {
        // Spilled: flush the residual map to an intermediate SST too, then k-way merge all
        // intermediates into this final SST (resolving cross-SST duplicate PKs + delvec). This
        // round-trips the already-sorted residual through one remote PUT+GET instead of merging it
        // straight from memory; that keeps peak RAM bounded (the residual is freed before the merge) at
        // the cost of one extra I/O -- an intentional I/O-vs-memory tradeoff, not an oversight.
        RETURN_IF_ERROR(flush_map_to_intermediate_sst());
        RETURN_IF_ERROR(merge_intermediates_into(&builder));
    }
    if (auto st = builder.Finish(); !st.ok()) {
        StorageMetrics::instance()->pk_index_sst_write_error_total.increment(1);
        LOG(WARNING) << "Failed to finish unsort PersistentIndex SST, error: " << st;
        return st;
    }
    RETURN_IF_ERROR(_wf->close());
    FileInfo file_info;
    file_info.path = file_name(_wf->filename());
    file_info.size = builder.FileSize();
    file_info.encryption_meta = _encryption_meta;
    PersistentIndexSstableRangePB range_pb;
    auto [key_start, key_end] = builder.KeyRange();
    range_pb.set_start_key(key_start.to_string());
    range_pb.set_end_key(key_end.to_string());
    // Best-effort async cleanup of intermediate spill SSTs (non-blocking, off the write path). The
    // success path removes them here. Orphans left by a crash/error are NOT reclaimed by the scheduled
    // vacuum daemons -- those skip UUID-named, txn_id==0 persistent-index SSTs (true of any PK load's
    // SSTs, spill or not) -- so they are reclaimed only by the manual lake_datafile_gc tool.
    if (!_intermediate_ssts.empty()) {
        std::vector<std::string> intermediate_paths;
        intermediate_paths.reserve(_intermediate_ssts.size());
        for (const auto& sst : _intermediate_ssts) {
            intermediate_paths.emplace_back(sst.location);
        }
        delete_files_async(std::move(intermediate_paths));
    }
    _wf.reset();
    _map.clear();
    _intermediate_ssts.clear();
    _map_mem_usage = 0;
    _next_rowid = 0;
    return std::make_pair(file_info, range_pb);
}

} // namespace starrocks::lake
