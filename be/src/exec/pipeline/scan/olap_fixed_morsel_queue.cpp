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

#include "exec/pipeline/scan/olap_fixed_morsel_queue.h"

#include "exec/pipeline/scan/olap_fixed_morsel_queue_builder.h"

namespace starrocks::pipeline {

namespace {

class OlapFixedMorselQueueBuilder final : public MorselQueueBuilder {
public:
    explicit OlapFixedMorselQueueBuilder(Morsels&& morsels) : _morsels(std::move(morsels)) {}
    ~OlapFixedMorselQueueBuilder() override = default;

    size_t num_original_morsels() const override { return _morsels.size(); }
    size_t max_degree_of_parallelism() const override { return _morsels.size(); }
    bool can_uniform_distribute() const override { return true; }

    bool has_more_scan_ranges() const override { return _has_more_scan_ranges; }
    void set_has_more_scan_ranges(bool value) override { _has_more_scan_ranges = value; }
    bool has_more_from_split() const override { return _has_more_from_split; }
    void set_has_more_from_split(bool value) override { _has_more_from_split = value; }

    StatusOr<MorselQueuePtr> build() override { return build_from_morsels(take_morsels()); }

    Morsels take_morsels() override {
        Morsels morsels;
        morsels.swap(_morsels);
        return morsels;
    }

    StatusOr<MorselQueuePtr> build_from_morsels(Morsels&& morsels) const override {
        MorselQueuePtr queue = std::make_unique<OlapFixedMorselQueue>(std::move(morsels));
        queue->set_has_more_scan_ranges(_has_more_scan_ranges);
        queue->set_has_more_from_split(_has_more_from_split);
        return queue;
    }

private:
    Morsels _morsels;
    bool _has_more_scan_ranges = false;
    bool _has_more_from_split = false;
};

} // namespace

StatusOr<MorselPtr> OlapFixedMorselQueue::try_get() {
    if (_unget_morsel != nullptr) {
        return std::move(_unget_morsel);
    }
    auto idx = _pop_index.load();
    // prevent _num_morsels from superfluous addition
    if (idx >= _num_morsels) {
        return nullptr;
    }
    idx = _pop_index.fetch_add(1);
    if (idx < _num_morsels) {
        if (!_tablet_rowsets.empty()) {
            _morsels[idx]->set_rowsets(_tablet_rowsets[idx]);
        }
        return std::move(_morsels[idx]);
    } else {
        return nullptr;
    }
}

MorselQueueBuilderPtr make_olap_fixed_morsel_queue_builder(Morsels&& morsels) {
    return std::make_unique<OlapFixedMorselQueueBuilder>(std::move(morsels));
}

} // namespace starrocks::pipeline
