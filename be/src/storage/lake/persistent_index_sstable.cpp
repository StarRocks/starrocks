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

#include <atomic>
#include <mutex>

#include "fs/fs.h"
#include "fs/key_cache.h"
#include "gen_cpp/types.pb.h"
#include "runtime/exec_env.h"
#include "storage/lake/lake_delvec_loader.h"
#include "storage/lake/utils.h"
#include "storage/sstable/table_builder.h"
#include "testutil/sync_point.h"
#include "util/monotime.h"
#include "util/starrocks_metrics.h"
#include "util/threadpool.h"
#include "util/trace.h"

namespace starrocks::lake {

namespace {

Status drop_corrupted_sstable_cache(const std::string& path) {
#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
    if (!config::lake_clear_corrupted_cache_data) {
        return Status::NotSupported("lake_clear_corrupted_cache_data is turned off");
    }
    auto fs_or = FileSystem::CreateSharedFromString(path);
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

    // Open the footer / index / metaindex / filter blocks via a temporary file
    // handle that bypasses the local disk cache. These small blocks are kept
    // resident in Table::Rep for the lifetime of the sstable, so disk-caching
    // them only consumes vdb bandwidth without serving any later read. Cold
    // start on this run reads them across ~17 PK-index SSTs/tablet × 1000
    // tablets and saturates the local SSD bandwidth ceiling. Runtime data
    // block reads continue to use the long-lived `rf` (or the per-call file
    // built in PersistentIndexSstable::multi_get when parallel execution is
    // enabled) and so still populate the local disk cache.
    auto make_init_rf = [&](const std::string& path) -> StatusOr<std::unique_ptr<RandomAccessFile>> {
        RandomAccessFileOptions init_opts;
        init_opts.skip_fill_local_cache = true;
        init_opts.skip_disk_cache = true;
        // Route through the isolated PK-index sst_open S3 client (separate connection
        // pool + shorter requestTimeoutMs). Caps cold-start OSS tail latency by failing
        // a stuck GetObject so the SDK retries on a fresh connection.
        init_opts.s3_operation_type = S3ClientOpType::kPkIndexSstOpen;
        if (!sstable_pb.encryption_meta().empty()) {
            ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(sstable_pb.encryption_meta()));
            init_opts.encryption_info = std::move(info);
        }
        return fs::new_random_access_file(init_opts, path);
    };
    ASSIGN_OR_RETURN(auto init_rf, make_init_rf(rf->filename()));

    sstable::Table* table;
    auto open_st = sstable::Table::Open(options, init_rf.get(), sstable_pb.filesize(), &table);
    TEST_SYNC_POINT_CALLBACK("PersistentIndexSstable::init:table_open_error", &open_st);
    if (open_st.is_corruption()) {
        auto drop_status = drop_corrupted_sstable_cache(rf->filename());
        if (drop_status.ok()) {
            delete table;
            if (tablet_mgr == nullptr) {
                return Status::InvalidArgument("tablet_mgr is null when loading sst file");
            }
            const std::string sst_path = tablet_mgr->sst_location(metadata->id(), sstable_pb.filename());
            RandomAccessFileOptions opts;
            opts.s3_operation_type = S3ClientOpType::kPkIndexSstOpen;
            if (!sstable_pb.encryption_meta().empty()) {
                ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(sstable_pb.encryption_meta()));
                opts.encryption_info = std::move(info);
            }
            ASSIGN_OR_RETURN(rf, fs::new_random_access_file(opts, sst_path));
            ASSIGN_OR_RETURN(init_rf, make_init_rf(sst_path));
            open_st = sstable::Table::Open(options, init_rf.get(), sstable_pb.filesize(), &table);
            TEST_SYNC_POINT_CALLBACK("PersistentIndexSstable::init:table_open_retry_error", &open_st);
        }
    }
    if (!open_st.ok()) {
        StarRocksMetrics::instance()->pk_index_sst_read_error_total.increment(1);
        LOG(WARNING) << "Failed to open PersistentIndex SST file: " << sstable_pb.filename() << ", error: " << open_st;
        return open_st;
    }
    _sst.reset(table);
    // Swap the long-lived file in for runtime block reads (compaction
    // iterators, non-parallel multi_get) so they keep populating the local
    // disk cache. The init_rf is dropped at the end of this scope.
    _sst->set_file(rf.get());
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
        StarRocksMetrics::instance()->pk_index_sst_write_error_total.increment(1);
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
        opts.s3_operation_type = S3ClientOpType::kPkIndexSstOpen;
        if (!_sstable_pb.encryption_meta().empty()) {
            ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(_sstable_pb.encryption_meta()));
            opts.encryption_info = std::move(info);
        }
        ASSIGN_OR_RETURN(rf, fs::new_random_access_file(opts, _rf->filename()));
    }
    options.file = rf.get();
    // Currently, there is no need to set predicate for MultiGet of persistent index sstable. Because predicate
    // only used for sstable compaction to filter out some keys for tablet split purpose and such keys can not
    // be read by the persistent index by designed. So even we provide a predicate, all keys read by multi_get
    // will always meet the condition.
    auto start_ts = butil::gettimeofday_us();
    Status multiget_st;
    bool used_chunk_parallel = false;
    int64_t multiget_chunks = 0;
    if (config::enable_pk_index_parallel_chunk_multi_get && config::enable_pk_index_parallel_execution &&
        static_cast<int64_t>(key_indexes.size()) >= config::pk_index_parallel_chunk_min_keys &&
        ExecEnv::GetInstance() != nullptr && ExecEnv::GetInstance()->pk_index_chunk_io_thread_pool() != nullptr) {
        const int target_keys = std::max(1, static_cast<int>(config::pk_index_parallel_chunk_target_keys));
        const int max_chunks = std::max(1, static_cast<int>(config::pk_index_parallel_chunk_max_chunks));
        int chunks = static_cast<int>((key_indexes.size() + target_keys - 1) / target_keys);
        chunks = std::min(chunks, max_chunks);
        if (chunks > 1) {
            // Chunk the key_indexes set into `chunks` contiguous, balanced ranges. Each chunk
            // gets its own RandomAccessFile (the underlying SeekableInputStream is not safe
            // for concurrent use; per-task files share the AWS S3Client which IS thread-safe).
            //
            // The chunk_io ThreadPool is dedicated and distinct from the inner_io pool that
            // dispatches per-fileset multi_gets in LakePersistentIndex::get_from_sstables —
            // re-entering inner_io from inside one of its workers would deadlock via
            // ThreadPool::wait()'s check_not_pool_thread_unlocked() (the same trap PR #72579
            // / iter-031 fell into for pk_index_execution_thread_pool).
            std::vector<KeyIndexSet::const_iterator> bounds;
            bounds.reserve(static_cast<size_t>(chunks) + 1);
            bounds.push_back(key_indexes.begin());
            const size_t per = key_indexes.size() / static_cast<size_t>(chunks);
            const size_t rem = key_indexes.size() % static_cast<size_t>(chunks);
            auto cur = key_indexes.begin();
            for (int c = 0; c < chunks; ++c) {
                size_t step = per + (static_cast<size_t>(c) < rem ? 1 : 0);
                std::advance(cur, step);
                bounds.push_back(cur);
            }
            std::vector<std::vector<std::string>> chunk_outs(chunks);
            std::vector<sstable::ReadIOStat> chunk_stats(chunks);
            std::vector<std::atomic<int>> chunk_done(chunks);
            for (int c = 0; c < chunks; ++c) {
                const size_t sz = static_cast<size_t>(std::distance(bounds[c], bounds[c + 1]));
                chunk_outs[c].assign(sz, std::string());
                chunk_done[c].store(0, std::memory_order_relaxed);
            }

            std::mutex status_mu;
            Status shared_status;
            auto* pool = ExecEnv::GetInstance()->pk_index_chunk_io_thread_pool();
            auto token = pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
            Trace* trace = Trace::CurrentTrace();
            // Each task creates its OWN RandomAccessFile so primary and hedged tasks
            // don't share an underlying connection. The first task to complete wins
            // (compare_exchange_strong on chunk_done[c]) and writes its results into
            // chunk_outs[c]/chunk_stats[c]; the loser's local_outs go out of scope.
            auto run_chunk = [&](int c) {
                const size_t sz = static_cast<size_t>(std::distance(bounds[c], bounds[c + 1]));
                std::vector<std::string> local_outs(sz);
                sstable::ReadIOStat local_stat;
                std::unique_ptr<RandomAccessFile> local_rf;
                {
                    RandomAccessFileOptions rf_opts;
                    rf_opts.s3_operation_type = S3ClientOpType::kPkIndexSstOpen;
                    if (!_sstable_pb.encryption_meta().empty()) {
                        auto info_or = KeyCache::instance().unwrap_encryption_meta(_sstable_pb.encryption_meta());
                        if (!info_or.ok()) {
                            std::lock_guard<std::mutex> lg(status_mu);
                            shared_status.update(info_or.status());
                            return;
                        }
                        rf_opts.encryption_info = std::move(info_or.value());
                    }
                    auto rf_or = fs::new_random_access_file(rf_opts, _rf->filename());
                    if (!rf_or.ok()) {
                        std::lock_guard<std::mutex> lg(status_mu);
                        shared_status.update(rf_or.status());
                        return;
                    }
                    local_rf = std::move(rf_or.value());
                }
                sstable::ReadOptions local_opts;
                local_opts.file = local_rf.get();
                local_opts.stat = &local_stat;
                auto s = _sst->MultiGet(local_opts, keys, bounds[c], bounds[c + 1], &local_outs);
                int expected = 0;
                if (chunk_done[c].compare_exchange_strong(expected, 1, std::memory_order_acq_rel)) {
                    chunk_outs[c] = std::move(local_outs);
                    chunk_stats[c] = local_stat;
                    if (!s.ok()) {
                        std::lock_guard<std::mutex> lg(status_mu);
                        shared_status.update(s);
                    }
                }
                // else: another task already won; discard our work.
            };
            // Dispatch primary chunks.
            for (int c = 0; c < chunks; ++c) {
                auto submit_st = token->submit_func([c, &run_chunk, trace]() {
                    ADOPT_TRACE(trace);
                    run_chunk(c);
                });
                if (!submit_st.ok()) {
                    // Pool busy / queue full — execute inline so we still produce a result for
                    // this chunk. We don't ADOPT_TRACE here because we are already on the
                    // calling thread which has the trace context.
                    run_chunk(c);
                }
            }
            // Hedged chunk reads (iter-056, Lever I). When a primary chunk task hasn't
            // completed within `pk_index_chunk_hedge_after_ms`, dispatch ONE duplicate
            // task with a fresh RandomAccessFile (= fresh underlying S3 connection).
            // First completer atomically claims chunk_outs[c]; the loser is discarded.
            // Targets cold-start OSS-tail latency: post-#72735 fan-out is ~205 SSTs
            // (iter-055 pindex_unique_sstables_touched_cnt p99) → ~1500 OSS reads in
            // flight per cold-start publish, dominated by individual OSS p99 (~5 s) tails.
            // A fresh RAF spawns a fresh S3 client connection → likely lands on a
            // different server / connection and completes in p50 (~150 ms).
            //
            // Hedges run on the SAME chunk_io pool as primaries: the primary's worker
            // is sleeping on OSS (idle CPU) so the slot is fungible. No re-entrance
            // trap (hedge runs _sst->MultiGet just like the primary, no descent into
            // chunk_io from a chunk_io worker). The dispatcher (this thread, on
            // pk_index_sst_walk pool) sleeps in Token::wait_for() — that does NOT
            // occupy a chunk_io worker.
            const int hedge_after_ms = static_cast<int>(config::pk_index_chunk_hedge_after_ms);
            const bool hedge_enabled = config::enable_pk_index_chunk_hedge && hedge_after_ms > 0;
            int64_t hedged_cnt = 0;
            if (hedge_enabled) {
                if (!token->wait_for(MonoDelta::FromMilliseconds(hedge_after_ms))) {
                    const int hedge_max = std::max(0, static_cast<int>(config::pk_index_chunk_hedge_max_per_multi_get));
                    for (int c = 0; c < chunks && hedged_cnt < hedge_max; ++c) {
                        if (chunk_done[c].load(std::memory_order_acquire) != 0) {
                            continue;
                        }
                        auto submit_st = token->submit_func([c, &run_chunk, trace]() {
                            ADOPT_TRACE(trace);
                            run_chunk(c);
                        });
                        if (!submit_st.ok()) {
                            // Pool busy: skip hedge for this chunk. Don't run inline —
                            // the primary is still in flight; an inline duplicate would
                            // re-do the same OSS wait we are trying to short-circuit.
                            continue;
                        }
                        ++hedged_cnt;
                    }
                }
                token->wait();
            } else {
                token->wait();
            }
            multiget_st = shared_status;
            if (hedged_cnt > 0) {
                TRACE_COUNTER_INCREMENT("multi_get_hedged_chunks", hedged_cnt);
            }

            // Concatenate per-chunk outputs back into index_value_with_vers in key_indexes
            // iteration order (the chunks are contiguous slices of key_indexes, so simple
            // concat preserves the alignment with the post-loop below that iterates
            // key_indexes and indexes into index_value_with_vers by sequential i).
            size_t out_pos = 0;
            for (int c = 0; c < chunks; ++c) {
                for (auto& v : chunk_outs[c]) {
                    index_value_with_vers[out_pos++] = std::move(v);
                }
            }
            for (const auto& s : chunk_stats) {
                stat.bytes_from_file += s.bytes_from_file;
                stat.bytes_from_cache += s.bytes_from_cache;
                stat.block_cnt_from_file += s.block_cnt_from_file;
                stat.block_cnt_from_cache += s.block_cnt_from_cache;
            }
            used_chunk_parallel = true;
            multiget_chunks = chunks;
        }
    }
    if (!used_chunk_parallel) {
        multiget_st = _sst->MultiGet(options, keys, key_indexes.begin(), key_indexes.end(), &index_value_with_vers);
    }
    TEST_SYNC_POINT_CALLBACK("PersistentIndexSstable::multi_get:error", &multiget_st);
    if (multiget_st.is_corruption()) {
        auto drop_status = drop_corrupted_sstable_cache(_rf->filename());
        if (drop_status.ok()) {
            // Retry on the simple sequential path so retry semantics stay identical to the
            // pre-iter-034 behaviour on transient corruption.
            multiget_st = _sst->MultiGet(options, keys, key_indexes.begin(), key_indexes.end(), &index_value_with_vers);
            TEST_SYNC_POINT_CALLBACK("PersistentIndexSstable::multi_get:retry_error", &multiget_st);
        }
    }
    if (!multiget_st.ok()) {
        StarRocksMetrics::instance()->pk_index_sst_read_error_total.increment(1);
        LOG(WARNING) << "Failed to multi_get from PersistentIndex SST file: " << _sstable_pb.filename()
                     << ", error: " << multiget_st;
        return multiget_st;
    }
    auto end_ts = butil::gettimeofday_us();
    TRACE_COUNTER_INCREMENT("multi_get_us", end_ts - start_ts);
    TRACE_COUNTER_INCREMENT("read_block_hit_cache_cnt", stat.block_cnt_from_cache);
    TRACE_COUNTER_INCREMENT("read_block_miss_cache_cnt", stat.block_cnt_from_file);
    if (multiget_chunks > 0) {
        TRACE_COUNTER_INCREMENT("multi_get_chunk_count", multiget_chunks);
    }
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
    opts.s3_operation_type = S3ClientOpType::kPkIndexSstOpen;
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
        : _wf(std::move(wf)), _finished(false), _encryption_meta(std::move(encryption_meta)) {
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
