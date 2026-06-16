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

#include "exec/pipeline/scan/morsel_queue_factory.h"

#include "exec/pipeline/scan/bucket_sequence_morsel_queue.h"
#include "exec/pipeline/scan/olap_dynamic_morsel_queue.h"
#include "gutil/casts.h"

namespace starrocks::pipeline {

SharedMorselQueueFactory::SharedMorselQueueFactory(MorselQueuePtr queue, int size)
        : _queue(std::move(queue)), _size(size) {}

size_t SharedMorselQueueFactory::num_original_morsels() const {
    return _queue->num_original_morsels();
}

Status SharedMorselQueueFactory::append_morsels([[maybe_unused]] int driver_seq, Morsels&& morsels) {
    RETURN_IF_ERROR(_queue->append_morsels(std::move(morsels)));
    return Status::OK();
}

void SharedMorselQueueFactory::set_has_more_scan_ranges(bool v) {
    _queue->set_has_more_scan_ranges(v);
}

bool SharedMorselQueueFactory::reach_limit() const {
    return _queue->reach_limit();
}

size_t IndividualMorselQueueFactory::num_original_morsels() const {
    size_t total = 0;
    for (const auto& queue : _queue_per_driver_seq) {
        total += queue->num_original_morsels();
    }
    return total;
}

Status MorselQueueFactory::append_morsels(int driver_seq, Morsels&& morsels) {
    return Status::NotSupported("MorselQueueFactory::append_morsels not supported");
}

StatusOr<int> MorselQueueFactory::next_driver_seq() {
    return Status::NotSupported("MorselQueueFactory::next_driver_seq not supported");
}

Status MorselQueueFactory::mark_split_source_morsel_finished() {
    return Status::NotSupported("MorselQueueFactory::mark_split_source_morsel_finished not supported");
}

IndividualMorselQueueFactory::IndividualMorselQueueFactory(std::map<int, MorselQueuePtr>&& queue_per_driver_seq,
                                                           bool could_local_shuffle,
                                                           bool enable_random_append_split_morsel)
        : _could_local_shuffle(could_local_shuffle),
          _enable_random_append_split_morsel(enable_random_append_split_morsel) {
    if (queue_per_driver_seq.empty()) {
        _queue_per_driver_seq.emplace_back(pipeline::create_empty_morsel_queue());
        return;
    }

    int max_dop = queue_per_driver_seq.rbegin()->first;
    _queue_per_driver_seq.reserve(max_dop + 1);
    for (int i = 0; i <= max_dop; ++i) {
        auto it = queue_per_driver_seq.find(i);
        if (it == queue_per_driver_seq.end()) {
            _queue_per_driver_seq.emplace_back(create_empty_morsel_queue());
        } else {
            _queue_per_driver_seq.emplace_back(std::move(it->second));
        }
    }

    if (_enable_random_append_split_morsel) {
        int64_t total = static_cast<int64_t>(num_original_morsels());
        _remaining_split_source_morsels.store(total, std::memory_order_relaxed);
    }
}

// The reason why we want to expand size of this vector is support of incremental scan ranges delivery
// Think about a case that in the initial round, there is 4 drivers assigned scan ranges, so vector size is 4
// but in the next round, if there is 5 drivers assigned scan ranges, then we have to expane vector to 5.
static void ensure_size_of_queue_per_drive_seq(std::vector<MorselQueuePtr>& _queue_per_driver_seq, int driver_seq) {
    int size = _queue_per_driver_seq.size();
    if (driver_seq >= size) {
        for (int i = 0; i < (driver_seq - size) + 1; i++) {
            _queue_per_driver_seq.emplace_back(create_empty_morsel_queue());
        }
    }
}

Status IndividualMorselQueueFactory::append_morsels(int driver_seq, Morsels&& morsels) {
    ensure_size_of_queue_per_drive_seq(_queue_per_driver_seq, driver_seq);

#ifndef NDEBUG
    // only append splitted morsel
    if (_enable_random_append_split_morsel) {
        for (const auto& morsel : morsels) {
            auto* scan_morsel = down_cast<ScanMorsel*>(morsel.get());
            DCHECK(scan_morsel != nullptr);
            DCHECK(scan_morsel->get_split_context() != nullptr);
        }
    }
#endif

    RETURN_IF_ERROR(_queue_per_driver_seq[driver_seq]->append_morsels(std::move(morsels)));
    return Status::OK();
}

StatusOr<int> IndividualMorselQueueFactory::next_driver_seq() {
    int size = _queue_per_driver_seq.size();
    if (size == 0) {
        return Status::NotSupported("IndividualMorselQueueFactory::next_driver_seq empty");
    }
    int seq = _random_cursor.fetch_add(1, std::memory_order_relaxed);
    return seq % size;
}

void IndividualMorselQueueFactory::set_has_more_scan_ranges(bool v) {
    for (auto& q : _queue_per_driver_seq) {
        q->set_has_more_scan_ranges(v);
    }
}

Status IndividualMorselQueueFactory::mark_split_source_morsel_finished() {
    DCHECK(_enable_random_append_split_morsel);
    int64_t remain = _remaining_split_source_morsels.load(std::memory_order_relaxed);
    while (remain > 0) {
        if (_remaining_split_source_morsels.compare_exchange_weak(remain, remain - 1, std::memory_order_relaxed,
                                                                  std::memory_order_relaxed)) {
            if (remain == 1) {
                for (auto& q : _queue_per_driver_seq) {
                    q->set_has_more_from_split(false);
                }
            }
            return Status::OK();
        }
    }
    return Status::OK();
}

bool IndividualMorselQueueFactory::reach_limit() const {
    for (const auto& p : _queue_per_driver_seq) {
        if (p->reach_limit()) {
            return true;
        }
    }
    return false;
}

BucketSequenceMorselQueueFactory::BucketSequenceMorselQueueFactory(std::map<int, MorselQueuePtr>&& queue_per_driver_seq,
                                                                   bool could_local_shuffle)
        : _could_local_shuffle(could_local_shuffle) {
    if (queue_per_driver_seq.empty()) {
        _queue_per_driver_seq.emplace_back(pipeline::create_empty_morsel_queue());
        return;
    }

    _queue_per_driver_seq.reserve(queue_per_driver_seq.size());
    int max_dop = queue_per_driver_seq.rbegin()->first;
    for (int i = 0; i <= max_dop; ++i) {
        auto it = queue_per_driver_seq.find(i);
        if (it == queue_per_driver_seq.end()) {
            _queue_per_driver_seq.emplace_back(create_empty_morsel_queue());
        } else {
            _queue_per_driver_seq.emplace_back(std::make_unique<BucketSequenceMorselQueue>(std::move(it->second)));
        }
    }
}

Status BucketSequenceMorselQueueFactory::append_morsels(int driver_seq, Morsels&& morsels) {
    ensure_size_of_queue_per_drive_seq(_queue_per_driver_seq, driver_seq);
    RETURN_IF_ERROR(_queue_per_driver_seq[driver_seq]->append_morsels(std::move(morsels)));
    return Status::OK();
}

void BucketSequenceMorselQueueFactory::set_has_more_scan_ranges(bool v) {
    for (auto& q : _queue_per_driver_seq) {
        q->set_has_more_scan_ranges(v);
    }
}

size_t BucketSequenceMorselQueueFactory::num_original_morsels() const {
    size_t total = 0;
    for (const auto& queue : _queue_per_driver_seq) {
        total += queue->num_original_morsels();
    }
    return total;
}

} // namespace starrocks::pipeline
