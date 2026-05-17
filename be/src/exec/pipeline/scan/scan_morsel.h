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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "gen_cpp/InternalService_types.h"

namespace starrocks {

class BaseRowset;
using BaseRowsetSharedPtr = std::shared_ptr<BaseRowset>;
struct TabletReaderParams;

namespace pipeline {

/// Morsel.
class ScanMorselX {
public:
    explicit ScanMorselX(int32_t plan_node_id) : _plan_node_id(plan_node_id) {}
    virtual ~ScanMorselX() = default;

    int32_t get_plan_node_id() const { return _plan_node_id; }

    virtual void init_tablet_reader_params(TabletReaderParams* params) {}

    virtual std::tuple<int64_t, int64_t> get_lane_owner_and_version() const {
        return std::tuple<int64_t, int64_t>{0L, 0L};
    }

    // from_version is used when reading incremental rowsets. in default, from_version = 0 means all of the rowsets
    // will be read out. In multi-version cache mechanism, when probing the cache and finding that cached result has
    // stale version, then incremental rowsets in the version range from the cached version till required version
    // should be read out and merged with the cache result, here from_version is cached version.
    void set_from_version(int64_t from_version) { _from_version = from_version; }
    int64_t from_version() { return _from_version; }

    void set_rowsets(const std::vector<BaseRowsetSharedPtr>& rowsets) { _rowsets = &rowsets; }
    void set_delta_rowsets(std::vector<BaseRowsetSharedPtr>&& delta_rowsets) {
        _delta_rowsets = std::move(delta_rowsets);
    }
    const std::vector<BaseRowsetSharedPtr>& rowsets() const {
        if (_delta_rowsets.has_value()) {
            return _delta_rowsets.value();
        } else {
            return *_rowsets;
        }
    }

    virtual const std::unordered_set<std::string>& skip_min_max_metrics() const {
        static const std::unordered_set<std::string> metrics;
        return metrics;
    }

private:
    int32_t _plan_node_id;
    int64_t _from_version = 0;

    static const std::vector<BaseRowsetSharedPtr> kEmptyRowsets;
    // _rowsets is owned by MorselQueue, whose lifecycle is longer than that of Morsel.
    const std::vector<BaseRowsetSharedPtr>* _rowsets = &kEmptyRowsets;
    std::optional<std::vector<BaseRowsetSharedPtr>> _delta_rowsets;
};

class ScanSplitContext {
public:
    virtual ~ScanSplitContext() = default;
    void set_last_split(bool v) { _is_last_split = v; }
    bool is_last_split() const { return _is_last_split; }

private:
    bool _is_last_split = false;
};

using ScanSplitContextPtr = std::unique_ptr<ScanSplitContext>;

class ScanMorsel;
using Morsel = ScanMorsel;
using MorselPtr = std::unique_ptr<Morsel>;
using Morsels = std::vector<MorselPtr>;

class ScanMorsel : public ScanMorselX {
public:
    ScanMorsel(int32_t plan_node_id, const TScanRange& scan_range);
    ScanMorsel(int32_t plan_node_id, const TScanRangeParams& scan_range);
    ~ScanMorsel() override;

    TScanRange* get_scan_range() { return _scan_range.get(); }

    TInternalScanRange* get_olap_scan_range() { return &(_scan_range->internal_scan_range); }

    std::tuple<int64_t, int64_t> get_lane_owner_and_version() const override {
        return std::tuple<int64_t, int64_t>{_owner_id, _version};
    }

    void set_split_context(ScanSplitContextPtr&& split_context) {
        if (split_context == nullptr) return;
        _split_context = std::move(split_context);
        _is_last_split = _split_context->is_last_split();
    }
    ScanSplitContext* get_split_context() { return _split_context.get(); }

    bool has_owner_id() const { return _has_owner_id; }
    int32_t owner_id() const { return _owner_id; }
    int32_t partition_id() const { return _partition_id; }

    bool is_last_split() const { return _is_last_split; }
    void set_last_split(bool v) { _is_last_split = v; }

    bool is_ticket_checker_entered() const { return _ticket_checker_entered; }
    void set_ticket_checker_entered(bool v) { _ticket_checker_entered = v; }

    static void build_scan_morsels(int node_id, const std::vector<TScanRangeParams>& scan_ranges,
                                   bool accept_empty_scan_ranges, pipeline::Morsels* morsels, bool* has_more_morsel);
    static bool has_more_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges);

private:
    std::unique_ptr<TScanRange> _scan_range;
    ScanSplitContextPtr _split_context = nullptr;
    bool _has_owner_id = false;
    int64_t _owner_id = 0;
    int64_t _version = 0;
    int64_t _partition_id = 0;
    bool _is_last_split = true;
    bool _ticket_checker_entered = false;
};

} // namespace pipeline
} // namespace starrocks
