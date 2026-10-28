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

#include "exec_primitive/pipeline/scan/scan_morsel.h"

#include <cstdlib>
#include <memory>
#include <utility>

namespace starrocks::pipeline {

const std::vector<BaseRowsetSharedPtr> ScanMorselX::kEmptyRowsets;

ScanMorsel::ScanMorsel(int32_t plan_node_id, const TScanRange& scan_range)
        : ScanMorselX(plan_node_id), _scan_range(std::make_unique<TScanRange>(scan_range)) {
    _init_owner();
}

// Thrift structs are generated without move constructors, so std::move would deep-copy; the
// generated swap() exchanges the members instead.
ScanMorsel::ScanMorsel(int32_t plan_node_id, TScanRange&& scan_range)
        : ScanMorselX(plan_node_id), _scan_range(std::make_unique<TScanRange>()) {
    using std::swap;
    swap(*_scan_range, scan_range);
    _init_owner();
}

void ScanMorsel::_init_owner() {
    if (_scan_range->__isset.internal_scan_range) {
        _owner_id = _scan_range->internal_scan_range.tablet_id;
        auto str_version = _scan_range->internal_scan_range.version;
        _version = strtol(str_version.c_str(), nullptr, 10);
        _owner_id = _scan_range->internal_scan_range.__isset.bucket_sequence
                            ? _scan_range->internal_scan_range.bucket_sequence
                            : _owner_id;
        _partition_id = _scan_range->internal_scan_range.partition_id;
        _has_owner_id = true;
    }
    if (_scan_range->__isset.binlog_scan_range) {
        _owner_id = _scan_range->binlog_scan_range.tablet_id;
        _has_owner_id = true;
    }
}

ScanMorsel::ScanMorsel(int32_t plan_node_id, const TScanRangeParams& scan_range)
        : ScanMorsel(plan_node_id, scan_range.scan_range) {}

ScanMorsel::~ScanMorsel() = default;

// ScanRanges is a const vector& (copy each scan range) or a vector&& (move it).
template <typename ScanRanges>
static void build_scan_morsels_impl(int node_id, ScanRanges&& scan_ranges, bool accept_empty_scan_ranges,
                                    pipeline::Morsels* ptr_morsels, bool* has_more_morsel) {
    pipeline::Morsels& morsels = *ptr_morsels;
    *has_more_morsel = false;
    for (auto& scan_range : scan_ranges) {
        if (scan_range.__isset.empty && scan_range.empty) {
            if (scan_range.__isset.has_more) {
                *has_more_morsel = scan_range.has_more;
            }
            continue;
        }
        morsels.emplace_back(
                std::make_unique<pipeline::ScanMorsel>(node_id, std::forward_like<ScanRanges>(scan_range.scan_range)));
    }

    if (morsels.empty() && !accept_empty_scan_ranges) {
        morsels.emplace_back(std::make_unique<pipeline::ScanMorsel>(node_id, TScanRangeParams()));
    }
}

void ScanMorsel::build_scan_morsels(int node_id, const std::vector<TScanRangeParams>& scan_ranges,
                                    bool accept_empty_scan_ranges, pipeline::Morsels* morsels, bool* has_more_morsel) {
    build_scan_morsels_impl(node_id, scan_ranges, accept_empty_scan_ranges, morsels, has_more_morsel);
}

void ScanMorsel::build_scan_morsels(int node_id, std::vector<TScanRangeParams>&& scan_ranges,
                                    bool accept_empty_scan_ranges, pipeline::Morsels* morsels, bool* has_more_morsel) {
    build_scan_morsels_impl(node_id, std::move(scan_ranges), accept_empty_scan_ranges, morsels, has_more_morsel);
}

bool ScanMorsel::has_more_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    bool has_more = false;
    for (const auto& scan_range : scan_ranges) {
        if (scan_range.__isset.empty && scan_range.empty) {
            if (scan_range.__isset.has_more) {
                has_more = scan_range.has_more;
            }
        }
    }
    return has_more;
}

} // namespace starrocks::pipeline
