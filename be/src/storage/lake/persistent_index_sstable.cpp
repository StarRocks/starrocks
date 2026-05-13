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

#include "storage/lake/persistent_index_sstable.h"

#include <butil/time.h> // NOLINT

#include <algorithm>

#include "base/debug/trace.h"
#include "base/testutil/sync_point.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_starlet_fwd.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "fs/key_cache.h"
#include "gen_cpp/types.pb.h"
#include "io/core/input_stream.h"
#include "storage/lake/lake_delvec_loader.h"
#include "storage/lake/utils.h"
#include "storage/sstable/table_builder.h"
#include "storage/storage_metrics.h"

namespace starrocks::lake {

namespace {

// Cheap drop-in for `get_numeric_statistics()` on the publish hot path: a single
// virtual call returning a 13×int64 struct, no heap alloc / no strings / no vector.
// Plain POSIX (shared-nothing UT) returns all zero via the base default. Callers
// always pass non-null `_rf.get()` (set in `init()`) or non-null fresh `rf.get()`
// (from `ASSIGN_OR_RETURN`), so the only guard we need is against the rare case
// where the underlying file's stream pointer comes back null.
io::IoStatsSnapshot take_sstable_io_snapshot(RandomAccessFile* rf) {
    auto stream = rf->stream();
    return stream ? stream->get_io_stats_snapshot() : io::IoStatsSnapshot{};
}

Status drop_corrupted_sstable_cache(const std::string& path) {
#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
    if (!config::lake_clear_corrupted_cache_data) {
        return Status::NotSupported("lake_clear_corrupted_cache_data is turned off");
    }
    auto fs_or = FileSystemFactory::CreateSharedFromString(path);
    if (!fs_or.ok()) {
        LOG(INFO) << "clear corrupted cache for " << path << ", error:" << fs_or.status();
        return fs_or.status();
    }
    auto drop_status = (*fs_or)->drop_local_cache(path);
    TEST_SYNC_POINT_CALLBACK("PersistentIndexSstable::drop_corrupted_cache", &drop_status);
    LOG(INFO) << "clear corrupted cache for " << path << ", error:" << drop_status;
    return drop_status;
#else
    return Status::NotSupported("clear corrupted cache is only supported in shared-data mode");
#endif
}

} // namespace

Status PersistentIndexSstable::init(std::unique_ptr<RandomAccessFile> rf, const PersistentIndexSstablePB& sstable_pb,
                                    Cache* cache, bool need_filter, DelVectorPtr delvec,
                                    const TabletMetadataPtr& metadata, TabletManager* tablet_mgr) {
    sstable::Options options;
    if (need_filter) {
        _filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
        options.filter_policy = _filter_policy.get();
    }
    options.block_cache = cache;
    sstable::Table* table;
    auto open_st = sstable::Table::Open(options, rf.get(), sstable_pb.filesize(), &table);
    TEST_SYNC_POINT_CALLBACK("PersistentIndexSstable::init:table_open_error", &open_st);
    if (open_st.is_corruption()) {
        auto drop_status = drop_corrupted_sstable_cache(rf->filename());
        if (drop_status.ok()) {
            delete table;
            if (tablet_mgr == nullptr) {
                return Status::InvalidArgument("tablet_mgr is null when loading sst file");
            }
            RandomAccessFileOptions opts;
            if (!sstable_pb.encryption_meta().empty()) {
                ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(sstable_pb.encryption_meta()));
                opts.encryption_info = std::move(info);
            }
            ASSIGN_OR_RETURN(rf, fs::new_random_access_file(
                                         opts, tablet_mgr->sst_location(metadata->id(), sstable_pb.filename())));
            open_st = sstable::Table::Open(options, rf.get(), sstable_pb.filesize(), &table);
            TEST_SYNC_POINT_CALLBACK("PersistentIndexSstable::init:table_open_retry_error", &open_st);
        }
    }
    if (!open_st.ok()) {
        StorageMetrics::instance()->pk_index_sst_read_error_total.increment(1);
        LOG(WARNING) << "Failed to open PersistentIndex SST file: " << sstable_pb.filename() << ", error: " << open_st;
        return open_st;
    }
    _sst.reset(table);
    _rf = std::move(rf);
    _sstable_pb.CopyFrom(sstable_pb);
    // load delvec
    if (_sstable_pb.has_delvec() && _sstable_pb.delvec().size() > 0) {
        if (delvec) {
            // If delvec is already provided, use it directly.
            _delvec = std::move(delvec);
        } else {
            if (metadata == nullptr) {
                return Status::InvalidArgument("metadata is null when loading delvec from file");
            }
            if (tablet_mgr == nullptr) {
                return Status::InvalidArgument("tablet_mgr is null when loading delvec from file");
            }
            // otherwise, load delvec from file
            LakeIOOptions lake_io_opts{.fill_data_cache = true, .skip_disk_cache = false};
            auto delvec_loader =
                    std::make_unique<LakeDelvecLoader>(tablet_mgr, nullptr, true /* fill cache */, lake_io_opts);
            RETURN_IF_ERROR(delvec_loader->load_from_meta(metadata, _sstable_pb.delvec(), &_delvec));
        }
    }
    return Status::OK();
}

Status PersistentIndexSstable::build_sstable(const phmap::btree_map<std::string, IndexValueWithVer, std::less<>>& map,
                                             WritableFile* wf, uint64_t* filesz,
                                             PersistentIndexSstableRangePB* range_pb) {
    std::unique_ptr<sstable::FilterPolicy> filter_policy;
    filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
    sstable::Options options;
    options.filter_policy = filter_policy.get();
    sstable::TableBuilder builder(options, wf);
    for (const auto& [k, v] : map) {
        IndexValuesWithVerPB index_value_pb;
        auto* value = index_value_pb.add_values();
        value->set_version(v.first);
        value->set_rssid(v.second.get_rssid());
        value->set_rowid(v.second.get_rowid());
        RETURN_IF_ERROR(builder.Add(Slice(k), Slice(index_value_pb.SerializeAsString())));
    }
    if (auto st = builder.Finish(); !st.ok()) {
        StorageMetrics::instance()->pk_index_sst_write_error_total.increment(1);
        LOG(WARNING) << "Failed to finish PersistentIndex SST, error: " << st;
        return st;
    }
    *filesz = builder.FileSize();
    if (range_pb != nullptr) {
        auto [key_start, key_end] = builder.KeyRange();
        range_pb->set_start_key(key_start.to_string());
        range_pb->set_end_key(key_end.to_string());
    }
    return Status::OK();
}

Status PersistentIndexSstable::multi_get(const Slice* keys, const KeyIndexSet& key_indexes, int64_t version,
                                         IndexValue* values, KeyIndexSet* found_key_indexes) const {
    std::vector<std::string> index_value_with_vers(key_indexes.size());
    sstable::ReadIOStat stat;
    sstable::ReadOptions options;
    options.stat = &stat;
    std::unique_ptr<RandomAccessFile> rf;
    if (config::enable_pk_index_parallel_execution) {
        RandomAccessFileOptions opts;
        if (!_sstable_pb.encryption_meta().empty()) {
            ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(_sstable_pb.encryption_meta()));
            opts.encryption_info = std::move(info);
        }
        ASSIGN_OR_RETURN(rf, fs::new_random_access_file(opts, _rf->filename()));
    }
    options.file = rf.get();
    // When parallel execution opens a fresh `rf`, its IO counters start at 0 so the delta below
    // equals the absolute IO done by this multi_get. Otherwise `_rf` is reused across calls and
    // we measure the delta against its running totals.
    RandomAccessFile* active_rf = (rf != nullptr) ? rf.get() : _rf.get();
    io::IoStatsSnapshot io_snap_before = take_sstable_io_snapshot(active_rf);
    // Currently, there is no need to set predicate for MultiGet of persistent index sstable. Because predicate
    // only used for sstable compaction to filter out some keys for tablet split purpose and such keys can not
    // be read by the persistent index by designed. So even we provide a predicate, all keys read by multi_get
    // will always meet the condition.
    auto start_ts = butil::gettimeofday_us();
    auto multiget_st = _sst->MultiGet(options, keys, key_indexes.begin(), key_indexes.end(), &index_value_with_vers);
    TEST_SYNC_POINT_CALLBACK("PersistentIndexSstable::multi_get:error", &multiget_st);
    if (multiget_st.is_corruption()) {
        auto drop_status = drop_corrupted_sstable_cache(_rf->filename());
        if (drop_status.ok()) {
            multiget_st = _sst->MultiGet(options, keys, key_indexes.begin(), key_indexes.end(), &index_value_with_vers);
            TEST_SYNC_POINT_CALLBACK("PersistentIndexSstable::multi_get:retry_error", &multiget_st);
        }
    }
    if (!multiget_st.ok()) {
        StorageMetrics::instance()->pk_index_sst_read_error_total.increment(1);
        LOG(WARNING) << "Failed to multi_get from PersistentIndex SST file: " << _sstable_pb.filename()
                     << ", error: " << multiget_st;
        return multiget_st;
    }
    auto end_ts = butil::gettimeofday_us();
    io::IoStatsSnapshot io_snap_after = take_sstable_io_snapshot(active_rf);
    TRACE_COUNTER_INCREMENT("multi_get_us", end_ts - start_ts);
    TRACE_COUNTER_INCREMENT("read_block_hit_cache_cnt", stat.block_cnt_from_cache);
    TRACE_COUNTER_INCREMENT("read_block_miss_cache_cnt", stat.block_cnt_from_file);
    // Break down the misses into reads served by the local data cache vs. reads that went out to
    // the remote object store (S3/OSS/etc.). Deltas are clamped at 0 because cumulative counters
    // should never decrease, but we guard against the file being swapped underneath us in retries.
    TRACE_COUNTER_INCREMENT(
            "sstable_io_local_disk_bytes",
            std::max<int64_t>(0, io_snap_after.bytes_read_local_disk - io_snap_before.bytes_read_local_disk));
    TRACE_COUNTER_INCREMENT("sstable_io_remote_bytes",
                            std::max<int64_t>(0, io_snap_after.bytes_read_remote - io_snap_before.bytes_read_remote));
    TRACE_COUNTER_INCREMENT(
            "sstable_io_count_local_disk",
            std::max<int64_t>(0, io_snap_after.io_count_local_disk - io_snap_before.io_count_local_disk));
    TRACE_COUNTER_INCREMENT("sstable_io_count_remote",
                            std::max<int64_t>(0, io_snap_after.io_count_remote - io_snap_before.io_count_remote));
    size_t i = 0;
    for (auto& key_index : key_indexes) {
        // Index_value_with_vers is empty means key is not found in sst.
        // Value in sst can not be empty.
        if (index_value_with_vers[i].empty()) {
            ++i;
            continue;
        }
        IndexValuesWithVerPB index_value_with_ver_pb;
        if (!index_value_with_ver_pb.ParseFromString(index_value_with_vers[i])) {
            return Status::InternalError("parse index value info failed");
        }
        // Check if this rowid is already filtered by delvec
        if (_delvec) {
            if (_delvec->roaring()->contains(index_value_with_ver_pb.values(0).rowid())) {
                ++i;
                continue;
            }
        }
        // Tombstone-aware projection: see is_index_tombstone() in storage/lake/utils.h
        // for why the rssid/rowid sentinel must be preserved through both the
        // shared_rssid overwrite and the rssid_offset shift. Version is independent of
        // the NullIndexValue encoding and is projected onto every entry (including
        // tombstones) so that the version-equality lookup below matches them.
        if (_sstable_pb.has_shared_version() && _sstable_pb.shared_version() > 0) {
            DCHECK(_sstable_pb.has_shared_rssid());
            for (size_t j = 0; j < index_value_with_ver_pb.values_size(); ++j) {
                index_value_with_ver_pb.mutable_values(j)->set_version(_sstable_pb.shared_version());
                if (is_index_tombstone(index_value_with_ver_pb.values(j))) continue;
                index_value_with_ver_pb.mutable_values(j)->set_rssid(_sstable_pb.shared_rssid());
            }
        }
        if (_sstable_pb.rssid_offset() != 0) {
            for (size_t j = 0; j < index_value_with_ver_pb.values_size(); ++j) {
                if (is_index_tombstone(index_value_with_ver_pb.values(j))) continue;
                const int64_t rssid =
                        static_cast<int64_t>(index_value_with_ver_pb.values(j).rssid()) + _sstable_pb.rssid_offset();
                index_value_with_ver_pb.mutable_values(j)->set_rssid(static_cast<uint32_t>(rssid));
            }
        }

        if (index_value_with_ver_pb.values_size() > 0) {
            if (version < 0) {
                values[key_index] = build_index_value(index_value_with_ver_pb.values(0));
                found_key_indexes->insert(key_index);
            } else {
                for (size_t j = 0; j < index_value_with_ver_pb.values_size(); ++j) {
                    if (index_value_with_ver_pb.values(j).version() == version) {
                        values[key_index] = build_index_value(index_value_with_ver_pb.values(j));
                        found_key_indexes->insert(key_index);
                        break;
                    }
                }
            }
        }
        ++i;
    }
    return Status::OK();
}

size_t PersistentIndexSstable::memory_usage() const {
    return (_sst != nullptr) ? _sst->memory_usage() : 0;
}

Status PersistentIndexSstable::sample_keys(std::vector<std::string>* keys, size_t sample_interval_bytes) const {
    if (_sst == nullptr) {
        return Status::InvalidArgument("SSTable is not initialized");
    }
    return _sst->sample_keys(keys, sample_interval_bytes);
}

StatusOr<PersistentIndexSstableUniquePtr> PersistentIndexSstable::new_sstable(
        const PersistentIndexSstablePB& sstable_pb, const std::string& location, Cache* cache, bool need_filter,
        const DelVectorPtr& delvec, const TabletMetadataPtr& metadata, TabletManager* tablet_mgr) {
    auto sstable = std::make_unique<PersistentIndexSstable>();
    RandomAccessFileOptions opts;
    if (!sstable_pb.encryption_meta().empty()) {
        ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(sstable_pb.encryption_meta()));
        opts.encryption_info = std::move(info);
    }
    ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(opts, location));
    RETURN_IF_ERROR(sstable->init(std::move(rf), sstable_pb, cache, need_filter, delvec, metadata, tablet_mgr));
    return std::move(sstable);
}

PersistentIndexSstableStreamBuilder::PersistentIndexSstableStreamBuilder(std::unique_ptr<WritableFile> wf,
                                                                         std::string encryption_meta)
        : _wf(std::move(wf)), _encryption_meta(std::move(encryption_meta)) {
    _filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
    sstable::Options options;
    options.filter_policy = _filter_policy.get();
    _table_builder = std::make_unique<sstable::TableBuilder>(options, _wf.get());
}

Status PersistentIndexSstableStreamBuilder::add(const Slice& key) {
    if (_finished) {
        return Status::InvalidArgument("Builder already finished");
    }

    IndexValuesWithVerPB index_value_pb;
    auto* val = index_value_pb.add_values();
    val->set_rowid(_sst_rowid++);

    RETURN_IF_ERROR(_table_builder->Add(key, Slice(index_value_pb.SerializeAsString())));
    return _table_builder->status();
}

Status PersistentIndexSstableStreamBuilder::finish(uint64_t* file_size) {
    if (_finished) {
        return Status::InvalidArgument("Builder already finished");
    }

    RETURN_IF_ERROR(_table_builder->Finish());
    _finished = true;
    if (file_size != nullptr) {
        *file_size = _table_builder->FileSize();
    }
    return _table_builder->status();
}

uint64_t PersistentIndexSstableStreamBuilder::num_entries() const {
    return _table_builder ? _table_builder->NumEntries() : 0;
}

FileInfo PersistentIndexSstableStreamBuilder::file_info() const {
    FileInfo file_info;
    file_info.path = file_name(_wf->filename());
    file_info.size = _table_builder ? _table_builder->FileSize() : 0;
    file_info.encryption_meta = _encryption_meta;
    return file_info;
}

std::pair<Slice, Slice> PersistentIndexSstableStreamBuilder::key_range() const {
    DCHECK(_table_builder != nullptr);
    return _table_builder->KeyRange();
}

} // namespace starrocks::lake
