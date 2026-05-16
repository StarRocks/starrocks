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

#pragma once

#include <atomic>
#include <map>
#include <vector>

#include "exec/pipeline/scan/morsel_queue.h"
#include "glog/logging.h"

namespace starrocks::pipeline {

class MorselQueueFactory {
public:
    virtual ~MorselQueueFactory() = default;

    virtual MorselQueue* create(int driver_sequence) = 0;
    virtual size_t size() const = 0;
    virtual size_t num_original_morsels() const = 0;

    virtual bool is_shared() const = 0;
    virtual bool could_local_shuffle() const = 0;

    virtual Status append_morsels(int driver_seq, Morsels&& morsels);
    virtual StatusOr<int> next_driver_seq();
    virtual bool enable_random_append_split_morsel() const { return false; }
    virtual void set_has_more_scan_ranges(bool v) {}
    virtual Status mark_split_source_morsel_finished();
    virtual bool reach_limit() const { return false; }
};

class SharedMorselQueueFactory final : public MorselQueueFactory {
public:
    SharedMorselQueueFactory(MorselQueuePtr queue, int size);
    ~SharedMorselQueueFactory() override = default;

    MorselQueue* create(int driver_sequence) override { return _queue.get(); }
    size_t size() const override { return _size; }
    size_t num_original_morsels() const override;

    bool is_shared() const override { return true; }
    bool could_local_shuffle() const override { return true; }

    Status append_morsels(int driver_seq, Morsels&& morsels) override;
    void set_has_more_scan_ranges(bool v) override;
    bool reach_limit() const override;

private:
    MorselQueuePtr _queue;
    const int _size;
};

class IndividualMorselQueueFactory final : public MorselQueueFactory {
public:
    IndividualMorselQueueFactory(std::map<int, MorselQueuePtr>&& queue_per_driver_seq, bool could_local_shuffle,
                                 bool enable_random_append_split_morsel);
    ~IndividualMorselQueueFactory() override = default;

    MorselQueue* create(int driver_sequence) override {
        DCHECK_LT(driver_sequence, _queue_per_driver_seq.size());
        return _queue_per_driver_seq[driver_sequence].get();
    }

    size_t size() const override { return _queue_per_driver_seq.size(); }

    size_t num_original_morsels() const override;

    bool is_shared() const override { return false; }
    bool could_local_shuffle() const override { return _could_local_shuffle; }

    Status append_morsels(int driver_seq, Morsels&& morsels) override;
    StatusOr<int> next_driver_seq() override;
    bool enable_random_append_split_morsel() const override {
        DCHECK(_could_local_shuffle);
        return _enable_random_append_split_morsel;
    }
    void set_has_more_scan_ranges(bool v) override;
    Status mark_split_source_morsel_finished() override;
    bool reach_limit() const override;

private:
    std::vector<MorselQueuePtr> _queue_per_driver_seq;
    std::atomic<int> _random_cursor{0};
    std::atomic<int64_t> _remaining_split_source_morsels{0};
    const bool _could_local_shuffle;
    const bool _enable_random_append_split_morsel;
};

class BucketSequenceMorselQueueFactory final : public MorselQueueFactory {
public:
    BucketSequenceMorselQueueFactory(std::map<int, MorselQueuePtr>&& queue_per_driver_seq, bool could_local_shuffle);
    ~BucketSequenceMorselQueueFactory() override = default;

    MorselQueue* create(int driver_sequence) override {
        DCHECK_LT(driver_sequence, _queue_per_driver_seq.size());
        return _queue_per_driver_seq[driver_sequence].get();
    }

    size_t size() const override { return _queue_per_driver_seq.size(); }

    size_t num_original_morsels() const override;

    bool is_shared() const override { return false; }

    bool could_local_shuffle() const override { return _could_local_shuffle; }

    Status append_morsels(int driver_seq, Morsels&& morsels) override;
    void set_has_more_scan_ranges(bool v) override;

private:
    std::vector<MorselQueuePtr> _queue_per_driver_seq;
    const bool _could_local_shuffle;
};

} // namespace starrocks::pipeline
