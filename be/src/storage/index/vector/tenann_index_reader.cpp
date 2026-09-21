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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/tablet.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifdef WITH_TENANN
#include "tenann_index_reader.h"

#include <algorithm>
#include <new>
#include <stdexcept>

#include "base/utility/defer_op.h"
#include "common/config_vector_index_fwd.h"
#include "common/runtime_profile.h"
#include "common/status.h"
#include "common/statusor.h"
#include "fs/fs.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_env.h"
#include "storage/index/vector/tenann/tenann_index_utils.h"
#include "storage/index/vector/vector_index_cache.h"
#include "storage/index/vector/vector_index_file_reader.h"
#include "storage_primitive/storage_stats.h"
#include "tenann/common/error.h"
#include "tenann/common/seq_view.h"
#include "tenann/factory/index_factory.h"
#include "tenann/index/index_cache.h"
#include "tenann/index/index_reader.h"
#include "tenann/searcher/id_filter.h"

namespace starrocks {

namespace {

void apply_index_reader_cache_options(tenann::IndexMeta* meta) {
    if (meta->index_type() == tenann::IndexType::kFaissIvfPq) {
        if (config::enable_vector_index_block_cache) {
            meta->index_reader_options()[tenann::IndexReaderOptions::cache_index_file_key] = false;
            meta->index_reader_options()[tenann::IndexReaderOptions::cache_index_block_key] = true;
        } else {
            meta->index_reader_options()[tenann::IndexReaderOptions::cache_index_file_key] = true;
            meta->index_reader_options()[tenann::IndexReaderOptions::cache_index_block_key] = false;
        }
    } else {
        meta->index_reader_options()[tenann::IndexReaderOptions::cache_index_file_key] = true;
    }
}

// Fold the IO done by one index read into the query stats. Deltas are clamped at 0:
// cumulative counters never decrease, but the guard keeps a swapped stream from
// producing negative numbers.
void accumulate_vi_io_stats(OlapReaderStatistics& stats, const io::IoStatsSnapshot& before,
                            const io::IoStatsSnapshot& after) {
    auto delta = [](int64_t a, int64_t b) { return std::max<int64_t>(0, a - b); };
    stats.vector_index_io_local_disk_bytes += delta(after.bytes_read_local_disk, before.bytes_read_local_disk);
    stats.vector_index_io_remote_bytes += delta(after.bytes_read_remote, before.bytes_read_remote);
    stats.vector_index_io_local_disk_ns += delta(after.io_ns_read_local_disk, before.io_ns_read_local_disk);
    stats.vector_index_io_remote_ns += delta(after.io_ns_read_remote, before.io_ns_read_remote);
}

} // namespace

// Where this load gets its bytes. Only HNSW wants the whole file up front: IVF-PQ reads
// per-list blocks on demand, so pulling the file ahead of it is pure waste. The size gate
// lives in VectorIndexFileReader::open(), which is where the size is certain.
VectorIndexFetchMode pick_vector_index_fetch_mode(const tenann::IndexMeta& meta, const FileInfo& vi_file) {
    // Nothing to parallelise unless all three hold: shared-nothing hands tenann a path
    // rather than a FileSystem and the kernel already does the right thing with a local
    // file; one reader is the plain streamed read; and IVF-PQ reads per-list blocks on
    // demand, so pulling its arrays in parallel buys nothing.
    if (vi_file.fs == nullptr || config::vector_index_load_parallel_threads <= 1 ||
        meta.index_type() != tenann::IndexType::kFaissHnsw) {
        return VectorIndexFetchMode::kStreamed;
    }
    return VectorIndexFetchMode::kParallel;
}

namespace {

StatusOr<tenann::IndexRef> load_vector_index(const tenann::IndexMeta& meta, const FileInfo& vi_file,
                                             VectorIndexCache& vector_index_cache,
                                             const std::shared_ptr<MemTracker>& tracker, OlapReaderStatistics& stats) {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(tracker.get());
    try {
        std::shared_ptr<VectorIndexFileReader> external_file_reader;
        io::IoStatsSnapshot io_before;
        if (vi_file.fs != nullptr) {
            auto opened_or = [&]() {
                SCOPED_RAW_TIMER(&stats.vector_index_file_open_ns);
                return VectorIndexFileReader::open(vi_file, pick_vector_index_fetch_mode(meta, vi_file));
            }();
            if (!opened_or.ok()) {
                return opened_or.status();
            }
            external_file_reader = std::shared_ptr<VectorIndexFileReader>(opened_or.value().release());
            // The mappings this reader hands faiss outlive the load, so they charge the
            // tracker themselves; give it to the reader before anything is read.
            external_file_reader->set_mem_tracker(tracker);

            io_before = external_file_reader->load_file_io_stats();
        }

        // The size gate in open() can downgrade the mode, so read it back rather than
        // recording what was asked for.
        switch (external_file_reader != nullptr ? external_file_reader->fetch_mode()
                                                : VectorIndexFetchMode::kStreamed) {
        case VectorIndexFetchMode::kParallel:
            ++stats.vector_index_parallel_load_count;
            break;
        case VectorIndexFetchMode::kStreamed:
            ++stats.vector_index_streamed_load_count;
            break;
        }

        auto reader = tenann::IndexFactory::CreateReaderFromMeta(meta);
        reader->SetIndexCache(&vector_index_cache);
        if (external_file_reader != nullptr) {
            reader->SetFileReader(external_file_reader);
        }
        DeferOp collect_read_stats([&] {
            const auto& read_stats = reader->read_timing_stats();
            stats.vector_index_read_file_ns += read_stats.read_file_ns;
            stats.vector_index_init_index_ns += read_stats.init_index_ns;
        });
        auto index_ref = reader->ReadIndexFile(vi_file.path);
        if (external_file_reader != nullptr) {
            // Snapshot before the release below: a released reader reports all-zero. A read
            // that threw skips this and contributes no IO attribution, which is fine -- the
            // load failed and the query does not use the index anyway.
            accumulate_vi_io_stats(stats, io_before, external_file_reader->load_file_io_stats());
            // Only the initial load needs the sequential stream; every later block read
            // opens its own file. Do not pin the remote stream in the cached index.
            external_file_reader->release_load_file();
        }
        if (index_ref == nullptr) {
            return Status::InternalError("vector index loader returned a null IndexRef");
        }
        return index_ref;
    } catch (const tenann::Error& e) {
        return tenann_error_to_status(e);
    } catch (const std::bad_alloc& e) {
        return Status::MemoryLimitExceeded(e.what());
    } catch (const std::exception& e) {
        return Status::InternalError(e.what());
    } catch (...) {
        return Status::InternalError("unknown vector index loader exception");
    }
}

VectorIndexCache::AsyncIndexLoader make_owned_index_loader(std::shared_ptr<const tenann::IndexMeta> meta,
                                                           FileInfo vi_file, VectorIndexCache& vector_index_cache,
                                                           std::shared_ptr<MemTracker> tracker) {
    return [meta = std::move(meta), vi_file = std::move(vi_file), vector_index_cache = &vector_index_cache,
            tracker = std::move(tracker)]() -> StatusOr<tenann::IndexRef> {
        OlapReaderStatistics background_stats;
        return load_vector_index(*meta, vi_file, *vector_index_cache, tracker, background_stats);
    };
}

} // namespace

StatusOr<VectorIndexReaderInitResult> TenANNReader::init_searcher(tenann::IndexMeta meta, const FileInfo& vi_file,
                                                                  OlapReaderStatistics& stats) {
    const std::string& index_path = vi_file.path;

    apply_index_reader_cache_options(&meta);

    // Charge the load to the process tracker (not the vector_index tracker): the
    // index lives in the heap, so the allocator hook already accounts these bytes
    // to process exactly once here. The VectorIndexCache then labels the same bytes
    // on the vector_index tracker via consume_without_root (see its ctor), which
    // does NOT re-add to process. Pointing the hook at the vector_index tracker
    // instead would double the vector_index label (hook + cache) and leak it on
    // eviction (the eventual tenann free runs on whatever thread drops the last
    // IndexRef, not necessarily under this tracker). Routing through process keeps
    // the load off the originating query's mem limit while leaving the deterministic
    // cache consume/release as the sole source of the vector_index label.
    auto tracker = RuntimeEnv::GetInstance()->process_mem_tracker_shared();
    std::shared_ptr<const tenann::IndexMeta> async_meta;

    if (!_cache_handle.valid()) {
        if (_async_load_on_miss) {
            // The background task owns every captured value. In particular it
            // never keeps query statistics or SegmentIterator stack objects.
            async_meta = std::make_shared<tenann::IndexMeta>(std::move(meta));
            auto owned_loader = make_owned_index_loader(async_meta, vi_file, _vector_index_cache, tracker);
            VectorIndexCacheProbeResult probe;
            {
                SCOPED_RAW_TIMER(&stats.vector_index_cache_lookup_ns);
                probe = _vector_index_cache.TryGetOrSchedule(tenann::CacheKey(index_path), std::move(owned_loader));
            }
            if (probe.state != VectorIndexCacheProbeState::kReady) {
                ++stats.vector_index_cache_miss_count;
                return VectorIndexReaderInitResult::kFallback;
            }
            _cache_handle = std::move(probe.handle);
            ++stats.vector_index_cache_hit_count;
        } else {
            Status load_status = Status::OK();
            bool loader_invoked = false;
            int64_t loader_ns = 0;
            auto loader = [&]() -> tenann::IndexRef {
                loader_invoked = true;
                SCOPED_RAW_TIMER(&loader_ns);
                auto loaded_or = load_vector_index(meta, vi_file, _vector_index_cache, tracker, stats);
                if (!loaded_or.ok()) {
                    load_status = loaded_or.status();
                    return nullptr;
                }
                return std::move(loaded_or).value();
            };

            int64_t get_or_create_ns = 0;
            bool cache_ok = false;
            bool wait_timed_out = false;
            {
                SCOPED_RAW_TIMER(&get_or_create_ns);
                auto result = _vector_index_cache.GetOrCreateForQuery(tenann::CacheKey(index_path), loader,
                                                                      /*wait_for_loading=*/true);
                wait_timed_out = result.state == VectorIndexCacheProbeState::kWaitTimeout;
                cache_ok = result.state == VectorIndexCacheProbeState::kReady;
                _cache_handle = std::move(result.handle);
            }
            if (wait_timed_out) {
                stats.vector_index_cache_lookup_ns += get_or_create_ns;
                ++stats.vector_index_cache_miss_count;
                return VectorIndexReaderInitResult::kFallback;
            }
            // Exclude this caller's loader time so a cold leader reports cache bookkeeping,
            // while a concurrent follower reports its singleflight wait.
            stats.vector_index_cache_lookup_ns += std::max<int64_t>(0, get_or_create_ns - loader_ns);
            if (loader_invoked) {
                ++stats.vector_index_cache_miss_count;
            } else {
                ++stats.vector_index_cache_hit_count;
            }
            if (!cache_ok) {
                return !load_status.ok() ? load_status
                                         : Status::InternalError("failed to load vector index: " + index_path);
            }
        }
    } else {
        ++stats.vector_index_cache_hit_count;
    }

    {
        SCOPED_RAW_TIMER(&stats.vector_index_searcher_init_ns);
        try {
            const auto& searcher_meta = async_meta != nullptr ? *async_meta : meta;
            _searcher = tenann::AnnSearcherFactory::CreateSearcherFromMeta(searcher_meta);
            // AttachIndexRef skips the second cache lookup Searcher::ReadIndex
            // would otherwise do — we already hold the ref from GetOrCreate.
            _searcher->AttachIndexRef(_cache_handle.index_ref());

            // Hard-check in addition to tenann's internal DCHECK: a silent
            // AttachIndex failure would yield wrong search results downstream.
            if (!_searcher->is_index_loaded()) {
                return Status::InternalError("vector index searcher did not finish loading: " + index_path);
            }
        } catch (const tenann::Error& e) {
            return tenann_error_to_status(e);
        } catch (const std::exception& e) {
            return Status::InternalError(e.what());
        }
    }
    return VectorIndexReaderInitResult::kReady;
}

Status TenANNReader::search(tenann::PrimitiveSeqView query_vector, int k, int64_t* result_ids,
                            uint8_t* result_distances, tenann::IdFilter* id_filter) {
    try {
        _searcher->AnnSearch(query_vector, k, result_ids, result_distances, id_filter);
    } catch (tenann::Error& e) {
        return Status::InternalError(e.what());
    }
    return Status::OK();
};

Status TenANNReader::range_search(tenann::PrimitiveSeqView query_vector, int k, std::vector<int64_t>* result_ids,
                                  std::vector<float>* result_distances, tenann::IdFilter* id_filter, float range,
                                  int order) {
    try {
        _searcher->RangeSearch(query_vector, range, k, tenann::AnnSearcher::ResultOrder(order), result_ids,
                               result_distances, id_filter);
    } catch (tenann::Error& e) {
        return Status::InternalError(e.what());
    }
    return Status::OK();
};

} // namespace starrocks
#endif
