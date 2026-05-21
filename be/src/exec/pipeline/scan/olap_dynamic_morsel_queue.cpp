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

#include "exec/pipeline/scan/olap_dynamic_morsel_queue.h"

#include <iterator>

#include "exec/pipeline/scan/olap_dynamic_morsel_queue_builder.h"

namespace starrocks::pipeline {

namespace {

class OlapDynamicMorselQueueBuilder final : public MorselQueueBuilder {
public:
    OlapDynamicMorselQueueBuilder(Morsels&& morsels, bool has_more_scan_ranges, size_t max_dop)
            : _morsels(std::move(morsels)), _has_more_scan_ranges(has_more_scan_ranges), _max_dop(max_dop) {}
    ~OlapDynamicMorselQueueBuilder() override = default;

    size_t num_original_morsels() const override { return _morsels.size(); }
    size_t max_degree_of_parallelism() const override { return _max_dop > 0 ? _max_dop : _morsels.size(); }
    bool can_uniform_distribute() const override { return true; }

    bool has_more_scan_ranges() const override { return _has_more_scan_ranges; }
    void set_has_more_scan_ranges(bool value) override { _has_more_scan_ranges = value; }
    bool has_more_from_split() const override { return _has_more_from_split; }
    void set_has_more_from_split(bool value) override { _has_more_from_split = value; }

    StatusOr<MorselQueuePtr> build() override {
        auto dynamic_queue = std::make_unique<OlapDynamicMorselQueue>(take_morsels(), _has_more_scan_ranges);
        if (_max_dop > 0) {
            dynamic_queue->set_max_degree_of_parallelism(_max_dop);
        }
        MorselQueuePtr queue = std::move(dynamic_queue);
        queue->set_has_more_from_split(_has_more_from_split);
        return queue;
    }

    Morsels take_morsels() override {
        Morsels morsels;
        morsels.swap(_morsels);
        return morsels;
    }

    StatusOr<MorselQueuePtr> build_from_morsels(Morsels&& morsels) const override {
        MorselQueuePtr queue = std::make_unique<OlapDynamicMorselQueue>(std::move(morsels), _has_more_scan_ranges);
        queue->set_has_more_from_split(_has_more_from_split);
        return queue;
    }

private:
    Morsels _morsels;
    bool _has_more_scan_ranges = false;
    bool _has_more_from_split = false;
    size_t _max_dop = 0;
};

} // namespace

MorselQueuePtr create_empty_morsel_queue() {
    // instead of creating OlapFixedMorselQueue, OlapDynamicMorselQueue permits to add scan ranges dynamically
    // because if we have incremental scan ranges delivery, some driver maybe does not have any scan ranges at first
    // but in the next round, it will have scan ranges to process.
    return std::make_unique<OlapDynamicMorselQueue>(std::vector<MorselPtr>{}, true);
}

StatusOr<MorselPtr> OlapDynamicMorselQueue::try_get() {
    std::lock_guard<std::mutex> _l(_mutex);
    if (_size == 0) return nullptr;
    _size -= 1;
    MorselPtr ret = std::move(_queue.front());
    _queue.pop_front();
    if (_ticket_checker != nullptr && ret->has_owner_id() && !ret->is_ticket_checker_entered()) {
        ret->set_ticket_checker_entered(true);
        _ticket_checker->enter(ret->owner_id(), ret->is_last_split());
    }
    return std::move(ret);
}

void OlapDynamicMorselQueue::unget(MorselPtr&& morsel) {
    std::lock_guard<std::mutex> _l(_mutex);
    _size += 1;
    _queue.emplace_front(std::move(morsel));
}

Status OlapDynamicMorselQueue::append_morsels(std::vector<MorselPtr>&& morsels) {
    std::lock_guard<std::mutex> _l(_mutex);
    _size += morsels.size();
    // add split morsels to front of this queue.
    // so this new morsels share same owner_id with recently processed morsel.
    _queue.insert(_queue.begin(), std::make_move_iterator(morsels.begin()), std::make_move_iterator(morsels.end()));
    return Status::OK();
}

MorselQueueBuilderPtr make_olap_dynamic_morsel_queue_builder(Morsels&& morsels, bool has_more_scan_ranges,
                                                             size_t max_dop) {
    return std::make_unique<OlapDynamicMorselQueueBuilder>(std::move(morsels), has_more_scan_ranges, max_dop);
}

} // namespace starrocks::pipeline
