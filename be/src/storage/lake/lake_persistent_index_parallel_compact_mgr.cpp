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

#include "storage/lake/lake_persistent_index_parallel_compact_mgr.h"

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <algorithm>
#include <memory>
#include <utility>

#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "gutil/strings/substitute.h"
#include "storage/lake/filenames.h"
#include "storage/lake/lake_persistent_index.h"
#include "storage/lake/persistent_index_sstable.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/sstable/comparator.h"
#include "storage/sstable/concatenating_iterator.h"
#include "storage/sstable/iterator.h"
#include "storage/sstable/merger.h"
#include "storage/sstable/options.h"
#include "storage/sstable/table_builder.h"
#include "util/countdown_latch.h"
#include "util/cpu_info.h"
#include "util/defer_op.h"
#include "util/starrocks_metrics.h"
#include "util/trace.h"

namespace starrocks::lake {

using BThreadCountDownLatch = GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>;
const sstable::Comparator* comparator = sstable::BytewiseComparator();

// return true when [seek_key, stop_key) and [range.start_key(), range.end_key()] overlap
bool SeekRange::has_overlap(const PersistentIndexSstableRangePB& range) const {
    // range is on the right side of [seek_key, stop_key)
    if (!stop_key.empty() && comparator->Compare(Slice(stop_key), Slice(range.start_key())) <= 0) {
        return false;
    }
    // range is on the left side of [seek_key, stop_key)
    if (comparator->Compare(Slice(range.end_key()), Slice(seek_key)) < 0) {
        return false;
    }
    return true;
}

// Return true when [seek_key, stop_key) fully contains [range.start_key(), range.end_key()]
bool SeekRange::full_contains(const PersistentIndexSstableRangePB& range) const {
    if (comparator->Compare(Slice(seek_key), Slice(range.start_key())) > 0) {
        return false;
    }
    if (!stop_key.empty() && comparator->Compare(Slice(stop_key), Slice(range.end_key())) <= 0) {
        return false;
    }
    return true;
}

size_t LakePersistentIndexParallelCompactTask::input_sstable_file_cnt() const {
    size_t cnt = 0;
    for (const auto& fileset : _input_sstables) {
        cnt += fileset.size();
    }
    return cnt;
}

void LakePersistentIndexParallelCompactTask::run() {
    DCHECK(_cb != nullptr);
    Status status = do_run();
    _cb->update_status(status);
    if (status.ok()) {
        _cb->add_result(_output_sstables);
    }
}

void LakePersistentIndexParallelCompactTask::cancel() {
    // When canceling, just update the status to cancelled.
    DCHECK(_cb != nullptr);
    _cb->update_status(Status::Cancelled("LakePersistentIndexParallelCompactTask cancelled"));
}

Status LakePersistentIndexParallelCompactTask::do_run() {
    Trace* trace = _cb->trace();
    scoped_refptr<Trace> child_trace(new Trace);
    Trace* sub_trace = child_trace.get();
    trace->AddChildTrace("SubCompactionTask", sub_trace);
    ADOPT_TRACE(sub_trace);
    TRACE_COUNTER_INCREMENT("queuing_latency_us", butil::gettimeofday_us() - _cb->create_us());
    TRACE_COUNTER_SCOPE_LATENCY_US("task_latency_us");
    const size_t file_cnt = input_sstable_file_cnt();
    if (file_cnt == 0 || _tablet_mgr == nullptr) {
        return Status::InternalError("Invalid task parameters");
    }

    // Collect all iterators from input sstables
    std::vector<sstable::Iterator*> concat_iters;
    std::vector<std::unique_ptr<PersistentIndexSstable>> sstables;
    DeferOp free_concat_iters([&] {
        for (sstable::Iterator* iter : concat_iters) {
            delete iter;
        }
    });

    sstable::ReadOptions read_options;
    read_options.fill_cache = false;

    // Open each sstable and create iterator
    for (const auto& fileset : _input_sstables) {
        std::vector<sstable::Iterator*> sst_iters;
        DeferOp free_iters([&] {
            for (sstable::Iterator* iter : sst_iters) {
                delete iter;
            }
        });
        for (const auto& sstable_pb : fileset) {
            if (sstable_pb.filesize() == 0) {
                continue;
            }
            TRACE_COUNTER_INCREMENT("compact_input_bytes", sstable_pb.filesize());
            TRACE_COUNTER_INCREMENT("compact_input_sst_cnt", 1);

            // Open sstable file
            RandomAccessFileOptions opts;
            if (!sstable_pb.encryption_meta().empty()) {
                ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(sstable_pb.encryption_meta()));
                opts.encryption_info = std::move(info);
            }

            ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(
                                              opts, _tablet_mgr->sst_location(_metadata->id(), sstable_pb.filename())));

            // Create PersistentIndexSstable
            auto sst = std::make_unique<PersistentIndexSstable>();
            RETURN_IF_ERROR(sst->init(std::move(rf), sstable_pb, nullptr, false, nullptr, _metadata, _tablet_mgr));

            // Create iterator
            read_options.max_rss_rowid = sstable_pb.max_rss_rowid();
            read_options.shared_rssid = sstable_pb.shared_rssid();
            read_options.shared_version = sstable_pb.shared_version();
            read_options.delvec = sst->delvec();

            sstable::Iterator* iter = sst->new_iterator(read_options);
            sst_iters.push_back(iter);
            sstables.push_back(std::move(sst));
        }
        // Create concatenating iterator for each fileset
        if (!sst_iters.empty()) {
            concat_iters.push_back(sstable::NewConcatenatingIterator(&sst_iters[0], sst_iters.size()));
            sst_iters.clear(); // Clear vector without deleting iterators (managed by concat_iters)
        }
    }

    if (concat_iters.empty()) {
        return Status::OK();
    }

    if (concat_iters.size() == 1) {
        // Only one fileset, only do move.
        bool full_contain = true;
        for (const auto& sst : sstables) {
            if (!_seek_range.full_contains(sst->sstable_pb().range())) {
                full_contain = false;
                break;
            }
        }
        if (full_contain) {
            for (const auto& sst : sstables) {
                _output_sstables.push_back(sst->sstable_pb());
                _output_sstables.back().mutable_fileset_id()->CopyFrom(_output_fileset_id.to_proto());
            }
            return Status::OK();
        }
    }

    // Create merging iterator for merging all filesets
    sstable::Options options;
    std::unique_ptr<sstable::Iterator> merging_iter(
            sstable::NewMergingIterator(options.comparator, &concat_iters[0], concat_iters.size()));
    concat_iters.clear(); // Clear vector without deleting iterators (managed by merging_iter)
    if (!_seek_range.seek_key.empty()) {
        merging_iter->Seek(_seek_range.seek_key);
    } else {
        merging_iter->SeekToFirst();
    }

    if (!merging_iter->Valid()) {
        return merging_iter->status();
    }

    auto merger = std::make_unique<KeyValueMerger>(merging_iter->key().to_string(), merging_iter->max_rss_rowid(),
                                                   _merge_base_level, _tablet_mgr, _metadata->id(),
                                                   true /* generate multi outputs*/);
    while (merging_iter->Valid()) {
        const std::string& cur_key = merging_iter->key().to_string();
        if (!_seek_range.stop_key.empty() &&
            options.comparator->Compare(Slice(cur_key), Slice(_seek_range.stop_key)) >= 0) {
            // meet the scan range boundary, quit.
            break;
        }
        RETURN_IF_ERROR(merger->merge(merging_iter.get()));
        merging_iter->Next();
    }
    RETURN_IF_ERROR(merging_iter->status());
    ASSIGN_OR_RETURN(auto merge_results, merger->finish());

    // Set output sstable metadata
    for (const auto& merge_result : merge_results) {
        PersistentIndexSstablePB output_sstable;
        output_sstable.set_filename(merge_result.filename);
        output_sstable.set_filesize(merge_result.filesize);
        output_sstable.set_encryption_meta(merge_result.encryption_meta);
        // Set key range
        output_sstable.mutable_range()->set_start_key(merge_result.start_key);
        output_sstable.mutable_range()->set_end_key(merge_result.end_key);
        // set fileset id
        output_sstable.mutable_fileset_id()->CopyFrom(_output_fileset_id.to_proto());

        _output_sstables.push_back(std::move(output_sstable));
        TRACE_COUNTER_INCREMENT("compact_output_bytes", merge_result.filesize);
    }

    return Status::OK();
}

AsyncCompactCB::AsyncCompactCB(std::unique_ptr<ThreadPoolToken> token,
                               std::function<Status(const std::vector<PersistentIndexSstablePB>&)> callback) {
    _thread_pool_token = std::move(token);
    _callback = std::move(callback);
    _trace_guard = scoped_refptr<Trace>(new Trace());
    _create_us = butil::gettimeofday_us();
}

Trace* AsyncCompactCB::trace() {
    return _trace_guard.get();
}

StatusOr<bool> AsyncCompactCB::wait_for(int timeout_ms) {
    if (_thread_pool_token == nullptr) {
        return true;
    }
    bool succ = true;
    if (timeout_ms < 0) {
        _thread_pool_token->wait();
    } else {
        succ = _thread_pool_token->wait_for(MonoDelta::FromMilliseconds(timeout_ms));
    }
    if (succ) {
        ADOPT_TRACE(_trace_guard.get());
        RETURN_IF_ERROR(_status);
        std::sort(_output_sstables.begin(), _output_sstables.end(),
                  [&](const PersistentIndexSstablePB& a, const PersistentIndexSstablePB& b) {
                      return comparator->Compare(Slice(a.range().start_key()), Slice(b.range().start_key())) < 0;
                  });
        RETURN_IF_ERROR(_callback(_output_sstables));
        _thread_pool_token = nullptr;
        TRACE_COUNTER_INCREMENT("total_latency_us", butil::gettimeofday_us() - _create_us);
    }
    return succ;
}

void AsyncCompactCB::add_result(const std::vector<PersistentIndexSstablePB>& ssts) {
    std::lock_guard<std::mutex> lg(_mutex);
    for (const auto& sst : ssts) {
        _output_sstables.push_back(sst);
    }
}

void AsyncCompactCB::update_status(const Status& status) {
    std::lock_guard<std::mutex> lg(_mutex);
    _status.update(status);
}

LakePersistentIndexParallelCompactMgr::~LakePersistentIndexParallelCompactMgr() {
    shutdown();
}

Status LakePersistentIndexParallelCompactMgr::init() {
    ThreadPoolBuilder builder("cloud_native_pk_index_compact");
    builder.set_min_threads(1);
    builder.set_max_threads(calc_max_threads());
    builder.set_max_queue_size(config::pk_index_parallel_compaction_threadpool_size);
    auto st = builder.build(&_thread_pool);
    if (st.ok()) {
        REGISTER_THREAD_POOL_METRICS(cloud_native_pk_index_compact, _thread_pool);
    }
    return st;
}

void LakePersistentIndexParallelCompactMgr::shutdown() {
    if (_thread_pool) {
        _thread_pool->shutdown();
        _thread_pool.reset();
    }
}

Status LakePersistentIndexParallelCompactMgr::update_max_threads(int max_threads) {
    if (_thread_pool) {
        return _thread_pool->update_max_threads(max_threads);
    }
    return Status::OK();
}

int32_t LakePersistentIndexParallelCompactMgr::calc_max_threads() const {
    int32_t max_threads = config::pk_index_parallel_compaction_threadpool_max_threads;
    if (max_threads <= 0) {
        max_threads = CpuInfo::num_cores() / 2;
    }
    return std::max(1, max_threads);
}

StatusOr<AsyncCompactCBPtr> LakePersistentIndexParallelCompactMgr::async_compact(
        const std::vector<std::vector<PersistentIndexSstablePB>>& candidates, const TabletMetadataPtr& metadata,
        bool merge_base_level, const std::function<Status(const std::vector<PersistentIndexSstablePB>&)>& callback) {
    std::vector<std::shared_ptr<LakePersistentIndexParallelCompactTask>> tasks;
    generate_compaction_tasks(candidates, metadata, merge_base_level, &tasks);
    AsyncCompactCBPtr cb;
    if (tasks.empty()) {
        // do nothing
    } else {
        // run via thread pool
        cb = std::make_unique<AsyncCompactCB>(_thread_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT), callback);
        for (auto& task : tasks) {
            task->set_cb(cb.get());
            auto submit_st = cb->thread_pool_token()->submit(task);
            if (!submit_st.ok()) {
                LOG(ERROR) << "Failed to submit persistent index parallel compaction task to thread pool: "
                           << submit_st;
                cb->update_status(submit_st);
                break;
            }
        }
    }
    return cb;
}

Status LakePersistentIndexParallelCompactMgr::compact(
        const std::vector<std::vector<PersistentIndexSstablePB>>& candidates, const TabletMetadataPtr& metadata,
        bool merge_base_level, std::vector<PersistentIndexSstablePB>* output_sstables) {
    ASSIGN_OR_RETURN(auto cb, async_compact(candidates, metadata, merge_base_level,
                                            [output_sstables](const std::vector<PersistentIndexSstablePB>& sstables) {
                                                output_sstables->insert(output_sstables->end(), sstables.begin(),
                                                                        sstables.end());
                                                return Status::OK();
                                            }));
    if (cb == nullptr) return Status::OK();
    RETURN_IF_ERROR(cb->wait_for());
    LOG(INFO) << strings::Substitute(
            "Lake persistent index parallel compaction completed for tablet $0, merge_base_level=$1, "
            "input filesets=$2, output sstables=$3, trace=$4",
            metadata->id(), merge_base_level, candidates.size(), output_sstables->size(), cb->trace()->MetricsAsJSON());
    return Status::OK();
}

bool LakePersistentIndexParallelCompactMgr::key_ranges_overlap(const std::string& start1, const std::string& end1,
                                                               const std::string& start2, const std::string& end2) {
    // Two ranges [start1, end1] and [start2, end2] (both inclusive) are non-overlapping if:
    bool cond1 = comparator->Compare(Slice(end1), Slice(start2)) < 0;
    bool cond2 = comparator->Compare(Slice(end2), Slice(start1)) < 0;

    return !(cond1 || cond2);
}

Status LakePersistentIndexParallelCompactMgr::sample_keys_from_sstable(const PersistentIndexSstablePB& sstable_pb,
                                                                       const TabletMetadataPtr& metadata,
                                                                       std::vector<std::string>* sample_keys) {
    if (sstable_pb.filesize() <= config::pk_index_sstable_sample_interval_bytes) {
        // use start key as boundary key only for small sstables
        sample_keys->push_back(sstable_pb.range().start_key());
    } else {
        // get sample keys from large sstables
        auto* block_cache = _tablet_mgr->update_mgr()->block_cache();
        ASSIGN_OR_RETURN(auto sstable,
                         PersistentIndexSstable::new_sstable(
                                 sstable_pb, _tablet_mgr->sst_location(metadata->id(), sstable_pb.filename()),
                                 block_cache ? block_cache->cache() : nullptr, false));
        RETURN_IF_ERROR(sstable->sample_keys(sample_keys, config::pk_index_sstable_sample_interval_bytes));
    }
    return Status::OK();
}

void LakePersistentIndexParallelCompactMgr::generate_compaction_tasks(
        const std::vector<std::vector<PersistentIndexSstablePB>>& candidates, const TabletMetadataPtr& metadata,
        bool merge_base_level, std::vector<std::shared_ptr<LakePersistentIndexParallelCompactTask>>* tasks) {
    TRACE_COUNTER_SCOPE_LATENCY_US("generate_sst_compact_tasks_latency_us");
    const int64_t start_us = butil::gettimeofday_us();
    if (candidates.empty()) {
        return;
    }

    // Check if any sstable has no key range (infinite boundary)
    // If found, create a single task without parallel splitting
    for (const auto& fileset : candidates) {
        for (const auto& sst : fileset) {
            if (!sst.has_range()) {
                // Found an sstable with infinite boundary, no parallel splitting
                tasks->push_back(std::make_shared<LakePersistentIndexParallelCompactTask>(
                        candidates, _tablet_mgr, metadata, merge_base_level, UniqueId::gen_uid(), SeekRange()));
                return;
            }
        }
    }

    // Collect all sstables with their fileset index
    struct SstableWithFileset {
        PersistentIndexSstablePB sstable;
        size_t fileset_index;

        bool operator<(const SstableWithFileset& other) const {
            return comparator->Compare(Slice(sstable.range().start_key()), Slice(other.sstable.range().start_key())) <
                   0;
        }
    };

    std::vector<SstableWithFileset> all_sstables;
    std::set<std::string, std::function<bool(const std::string&, const std::string&)>> boundary_keys(
            [](const std::string& a, const std::string& b) { return comparator->Compare(Slice(a), Slice(b)) < 0; });
    std::vector<std::string> boundary_keys_vec;
    size_t total_size = 0;
    for (size_t i = 0; i < candidates.size(); ++i) {
        for (const auto& sst : candidates[i]) {
            all_sstables.push_back({sst, i});
            total_size += sst.filesize();
            std::vector<std::string> sample_keys;
            auto st = sample_keys_from_sstable(sst, metadata, &sample_keys);
            if (st.ok()) {
                for (const auto& key : sample_keys) {
                    boundary_keys.insert(key);
                }
            } else {
                LOG(WARNING) << "Failed to sample keys from sstable " << sst.filename()
                             << ", use start_key as boundary key only: " << st;
                boundary_keys.insert(sst.range().start_key());
            }
        }
    }
    if (all_sstables.empty()) {
        return;
    }

    // prepare boundary keys vector
    boundary_keys_vec.assign(boundary_keys.begin(), boundary_keys.end());
    // Sort by start_key to ensure sstables are processed in order
    std::sort(all_sstables.begin(), all_sstables.end());

    // Calculate segment number based on total size, threshold and parallelism config
    size_t segment_num = std::max<size_t>(
            1, total_size / std::max<size_t>(1, config::pk_index_parallel_compaction_task_split_threshold_bytes));
    segment_num = std::min<size_t>(segment_num, calc_max_threads() * 2);

    struct Segment {
        // [seek_key, stop_key)
        SeekRange seek_range;
        std::vector<std::vector<PersistentIndexSstablePB>> filesets; // organized by fileset
        size_t file_cnt = 0;
    };

    // We use start_key of input sstable files as segment boundary candidates
    // E.g. if we have 4 input sstables with key ranges:
    //   sst1: [0, 100]
    //   sst2: [50, 150]
    //   sst3: [120, 200]
    //   sst4: [180, 250]
    // The candidate boundaries range are: [0, 50), [50, 120), [120, 180), [180, 250)
    // If we want to split into 2 segments, we can use [0, 120) and [120, 250)
    // Next step, we need to find out Step-X.
    // Range count <= segment_num * Step-X
    // And range count = sst count
    // So Step-X = sst count / segment_num
    size_t step_x = std::max((size_t)std::ceil((double)boundary_keys_vec.size() / (double)segment_num), (size_t)1);
    std::vector<Segment> segments;
    for (size_t i = 0; i < boundary_keys_vec.size();) {
        size_t end_idx = i + step_x;
        // Create a segment from i to end_idx
        Segment segment;
        segment.seek_range.seek_key = boundary_keys_vec[i];
        do {
            if (end_idx >= boundary_keys_vec.size()) {
                // last segment with infinite boundary
                segment.seek_range.stop_key = "";
            } else {
                segment.seek_range.stop_key = boundary_keys_vec[end_idx];
            }
            if (!segment.seek_range.stop_key.empty() &&
                comparator->Compare(Slice(segment.seek_range.seek_key), Slice(segment.seek_range.stop_key)) >= 0) {
                // invalid segment, move end_idx forward
                end_idx++;
            } else {
                break;
            }
        } while (true);
        segment.filesets.resize(candidates.size());
        segments.push_back(std::move(segment));
        i = end_idx;
    }
    // Assign sstables to segments
    for (const auto& sst_with_fileset : all_sstables) {
        const auto& sst = sst_with_fileset.sstable;
        // Find the first segment that has overlap with sst
        for (auto& segment : segments) {
            if (segment.seek_range.has_overlap(sst.range())) {
                segment.filesets[sst_with_fileset.fileset_index].push_back(sst);
                segment.file_cnt++;
            }
        }
    }

    VLOG(1) << fmt::format("LakePersistentIndexParallelCompactMgr: generated {} compaction segments for tablet {}",
                           segments.size(), metadata->id());

    // Create tasks from segments
    UniqueId fileset_id = UniqueId::gen_uid();
    for (auto& seg : segments) {
        if (seg.file_cnt > 0) {
            tasks->push_back(std::make_shared<LakePersistentIndexParallelCompactTask>(
                    std::move(seg.filesets), _tablet_mgr, metadata, merge_base_level, fileset_id, seg.seek_range));
        }
    }

    LOG(INFO) << fmt::format(
            "LakePersistentIndexParallelCompactMgr: generated {} compaction segments for tablet {}, cost {} us",
            segments.size(), metadata->id(), butil::gettimeofday_us() - start_us);
}

void LakePersistentIndexParallelCompactMgr::TEST_generate_compaction_tasks(
        const std::vector<std::vector<PersistentIndexSstablePB>>& candidates, const TabletMetadataPtr& metadata,
        bool merge_base_level, std::vector<std::shared_ptr<LakePersistentIndexParallelCompactTask>>* tasks) {
    generate_compaction_tasks(candidates, metadata, merge_base_level, tasks);
}

Status LakePersistentIndexParallelCompactMgr::TEST_sample_keys_from_sstable(const PersistentIndexSstablePB& sstable_pb,
                                                                            const TabletMetadataPtr& metadata,
                                                                            std::vector<std::string>* sample_keys) {
    return sample_keys_from_sstable(sstable_pb, metadata, sample_keys);
}

} // namespace starrocks::lake
