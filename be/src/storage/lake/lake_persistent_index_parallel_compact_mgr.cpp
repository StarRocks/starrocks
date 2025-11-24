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

#include <algorithm>
#include <memory>
#include <utility>

#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "gutil/strings/substitute.h"
#include "storage/lake/filenames.h"
#include "storage/lake/persistent_index_sstable.h"
#include "storage/lake/tablet_manager.h"
#include "storage/sstable/comparator.h"
#include "storage/sstable/iterator.h"
#include "storage/sstable/merger.h"
#include "storage/sstable/options.h"
#include "storage/sstable/table_builder.h"
#include "util/defer_op.h"

namespace starrocks::lake {

Status LakePersistentIndexParallelCompactTask::run() {
    if (_input_sstables.empty() || _tablet_mgr == nullptr) {
        return Status::InternalError("Invalid task parameters");
    }

    // Count total number of sstables
    int total_sstable_count = 0;
    const PersistentIndexSstablePB* single_sstable = nullptr;
    for (const auto& fileset : _input_sstables) {
        for (const auto& sst : fileset) {
            if (sst.filesize() > 0) {
                total_sstable_count++;
                single_sstable = &sst;
            }
        }
    }

    // Optimization: if only one sstable, directly reuse it without merge
    if (total_sstable_count == 1 && single_sstable != nullptr) {
        _output_sstable.CopyFrom(*single_sstable);
        return Status::OK();
    }

    // Collect all iterators from input sstables
    std::vector<sstable::Iterator*> iters;
    DeferOp free_iters([&] {
        for (sstable::Iterator* iter : iters) {
            delete iter;
        }
    });

    sstable::ReadOptions read_options;
    read_options.fill_cache = false;

    // Open each sstable and create iterator
    for (const auto& fileset : _input_sstables) {
        for (const auto& sstable_pb : fileset) {
            if (sstable_pb.filesize() == 0) {
                continue;
            }

            // Open sstable file
            RandomAccessFileOptions opts;
            if (!sstable_pb.encryption_meta().empty()) {
                ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(sstable_pb.encryption_meta()));
                opts.encryption_info = std::move(info);
            }

            ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(
                                              opts, _tablet_mgr->sst_location(_tablet_id, sstable_pb.filename())));

            // Create PersistentIndexSstable
            auto sst = std::make_shared<PersistentIndexSstable>();
            RETURN_IF_ERROR(sst->init(std::move(rf), sstable_pb, nullptr, false));

            // Create iterator
            read_options.max_rss_rowid = sstable_pb.max_rss_rowid();
            read_options.shared_rssid = sstable_pb.shared_rssid();
            read_options.shared_version = sstable_pb.shared_version();

            sstable::Iterator* iter = sst->new_iterator(read_options);
            iters.push_back(iter);
        }
    }

    if (iters.empty()) {
        return Status::OK();
    }

    // Create merging iterator
    sstable::Options options;
    std::unique_ptr<sstable::Iterator> merging_iter(
            sstable::NewMergingIterator(options.comparator, &iters[0], iters.size()));
    merging_iter->SeekToFirst();
    iters.clear(); // Clear vector without deleting iterators (managed by merging_iter)

    if (!merging_iter->Valid()) {
        return merging_iter->status();
    }

    // Generate output filename
    auto filename = gen_sst_filename();
    auto location = _tablet_mgr->sst_location(_tablet_id, filename);

    // Create writable file
    WritableFileOptions wopts;
    std::string encryption_meta;
    if (config::enable_transparent_data_encryption) {
        ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
        wopts.encryption_info = pair.info;
        encryption_meta.swap(pair.encryption_meta);
    }

    ASSIGN_OR_RETURN(auto wf, fs::new_writable_file(wopts, location));

    // Create table builder
    std::unique_ptr<sstable::FilterPolicy> filter_policy;
    filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
    options.filter_policy = filter_policy.get();
    sstable::TableBuilder builder(options, wf.get());

    // Merge sstables using KeyValueMerger
    auto merger = std::make_unique<lake::KeyValueMerger>(merging_iter->key().to_string(), merging_iter->max_rss_rowid(),
                                                         &builder, merge_base_level);
    while (merging_iter->Valid()) {
        RETURN_IF_ERROR(merger->merge(merging_iter.get()));
        merging_iter->Next();
    }
    RETURN_IF_ERROR(merging_iter->status());
    RETURN_IF_ERROR(merger->finish());
    RETURN_IF_ERROR(builder.Finish());
    RETURN_IF_ERROR(wf->close());

    // Set output sstable metadata
    _output_sstable.set_filename(filename);
    _output_sstable.set_filesize(builder.FileSize());
    _output_sstable.set_encryption_meta(encryption_meta);

    // Set key range
    auto [key_start, key_end] = builder.KeyRange();
    _output_sstable.mutable_range()->set_start_key(key_start.to_string());
    _output_sstable.mutable_range()->set_end_key(key_end.to_string());

    return Status::OK();
}

LakePersistentIndexParallelCompactMgr::~LakePersistentIndexParallelCompactMgr() {
    if (_thread_pool) {
        _thread_pool->shutdown();
    }
}

Status LakePersistentIndexParallelCompactMgr::init() {
    ThreadPoolBuilder builder("lake_persistent_index_compact");
    builder.set_min_threads(1);
    builder.set_max_threads(std::max(1, config::pk_parallel_compaction_threadpool_max_threads));
    return builder.build(&_thread_pool);
}

Status LakePersistentIndexParallelCompactMgr::compact(
        const std::vector<std::vector<PersistentIndexSstablePB>>& candidates, int64_t tablet_id, bool merge_base_level,
        std::vector<PersistentIndexSstablePB>* output_sstables) {
    std::vector<LakePersistentIndexParallelCompactTask> tasks;
    generate_compaction_tasks(candidates, tablet_id, merge_base_level, &tasks);
    // if only one task in tasks, run in current thread
    if (tasks.size() <= 1) {
        RETURN_IF_ERROR(tasks[0].run());
        output_sstables->push_back(*tasks[0].get_output_sstable());
    } else {
        // run via thread pool
        auto latch = BThreadCountDownLatch(tasks.size());
        Status final_status = Status::OK();
        std::mutex output_mutex;
        for (int i = 0; i < tasks.size(); ++i) {
            RETURN_IF_ERROR(_thread_pool->submit_func([&, i]() {
                Status st = tasks[i].run();
                if (!st.ok()) {
                    LOG(ERROR) << "Persistent index parallel compaction task failed: " << st;
                    std::lock_guard<std::mutex> lg(output_mutex);
                    final_status = st;
                } else {
                    // protect output_sstables via mutex
                    std::lock_guard<std::mutex> lg(output_mutex);
                    output_sstables->push_back(*tasks[i].get_output_sstable());
                }
                latch.count_down();
            }));
        }
        latch.wait();
        RETURN_IF_ERROR(final_status);
        // add output_sstables in order of start_key
        sstable::Comparator* comparator = sstable::BytewiseComparator();
        std::sort(output_sstables->begin(), output_sstables->end(),
                  [comparator](const PersistentIndexSstablePB& a, const PersistentIndexSstablePB& b) {
                      return comparator->Compare(Slice(a.start_key()), Slice(b.start_key())) < 0;
                  });
    }
    return Status::OK();
}

bool LakePersistentIndexParallelCompactMgr::key_ranges_overlap(const std::string& start1, const std::string& end1,
                                                               const std::string& start2, const std::string& end2) {
    sstable::Comparator* comparator = sstable::BytewiseComparator();

    // Two ranges [start1, end1) and [start2, end2) are non-overlap if:
    // end1 < start2 OR end2 < start1
    bool cond1 = comparator->Compare(Slice(end1), Slice(start2)) < 0;
    bool cond2 = comparator->Compare(Slice(end2), Slice(start1)) < 0;

    return !(cond1 || cond2);
}

void LakePersistentIndexParallelCompactMgr::generate_compaction_tasks(
        const std::vector<std::vector<PersistentIndexSstablePB>>& candidates, int64_t tablet_id, bool merge_base_level,
        std::vector<LakePersistentIndexParallelCompactTask>* tasks) {
    if (candidates.empty()) {
        return;
    }

    // Check if any sstable has no key range (infinite boundary)
    // If found, create a single task without parallel splitting
    for (const auto& fileset : candidates) {
        for (const auto& sst : fileset) {
            if (!sst.has_range() || !sst.range().has_start_key() || !sst.range().has_end_key()) {
                // Found an sstable with infinite boundary, no parallel splitting
                LakePersistentIndexParallelCompactTask task;
                task.input_sstables = candidates;
                tasks->push_back(std::move(task));
                return;
            }
        }
    }

    sstable::Comparator* comparator = sstable::BytewiseComparator();

    // Collect all sstables with their fileset index
    struct SstableWithFileset {
        PersistentIndexSstablePB sstable;
        size_t fileset_index;

        bool operator<(const SstableWithFileset& other) const {
            return sstable::BytewiseComparator()->Compare(Slice(sstable.range().start_key()),
                                                          Slice(other.sstable.range().start_key())) < 0;
        }
    };

    std::vector<SstableWithFileset> all_sstables;
    for (size_t i = 0; i < candidates.size(); ++i) {
        for (const auto& sst : candidates[i]) {
            all_sstables.push_back({sst, i});
        }
    }

    // Sort by start_key to ensure sstables are processed in order
    std::sort(all_sstables.begin(), all_sstables.end());

    // Group sstables into segments based on threshold
    // Each sstable belongs to exactly one segment
    int64_t threshold = config::pk_parallel_compaction_task_split_threshold_bytes;

    struct Segment {
        std::string start_key;
        std::string end_key;
        int64_t total_size = 0;
        std::vector<std::vector<PersistentIndexSstablePB>> sstables; // organized by fileset
    };

    std::vector<Segment> segments;
    if (all_sstables.empty()) {
        return;
    }

    // Initialize first segment with the first sstable
    Segment current_segment;
    current_segment.sstables.resize(candidates.size());
    current_segment.start_key = all_sstables[0].sstable.range().start_key();
    current_segment.end_key = all_sstables[0].sstable.range().end_key();
    current_segment.sstables[all_sstables[0].fileset_index].push_back(all_sstables[0].sstable);
    current_segment.total_size = all_sstables[0].sstable.filesize();

    // Process remaining sstables starting from the second one
    for (size_t i = 1; i < all_sstables.size(); ++i) {
        const auto& item = all_sstables[i];
        const auto& sst = item.sstable;
        size_t fileset_idx = item.fileset_index;

        // Check if this sstable overlaps with current segment
        bool has_overlap = false;
        if (current_segment.total_size > 0) {
            // Overlap exists if sst.start_key < current_segment.end_key
            has_overlap = comparator->Compare(Slice(sst.range().start_key()), Slice(current_segment.end_key)) < 0;
        }

        // Check if adding this sstable would exceed threshold
        // BUT if there's overlap, we must merge even if exceeding threshold
        if (current_segment.total_size > 0 && !has_overlap && current_segment.total_size + sst.filesize() > threshold) {
            // No overlap and exceeds threshold, start a new segment
            segments.push_back(std::move(current_segment));
            current_segment = Segment();
            current_segment.sstables.resize(candidates.size());
            current_segment.start_key = sst.range().start_key();
            current_segment.end_key = sst.range().end_key();
        }

        // Add sstable to current segment
        current_segment.sstables[fileset_idx].push_back(sst);
        current_segment.total_size += sst.filesize();

        // Update end_key to the maximum end_key in this segment
        if (comparator->Compare(Slice(sst.range().end_key()), Slice(current_segment.end_key)) > 0) {
            current_segment.end_key = sst.range().end_key();
        }
    }

    // Don't forget the last segment
    if (current_segment.total_size > 0) {
        segments.push_back(std::move(current_segment));
    }

    // Create tasks from segments
    for (auto& seg : segments) {
        LakePersistentIndexParallelCompactTask task(std::move(seg.sstables), std::move(seg.start_key),
                                                    std::move(seg.end_key), _tablet_mgr, tablet_id, merge_base_level);
        tasks->push_back(std::move(task));
    }
}

} // namespace starrocks::lake
