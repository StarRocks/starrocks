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

#include "exec/pipeline/scan/scan_morsel.h"

#include <cstdlib>

namespace starrocks::pipeline {

const std::vector<BaseRowsetSharedPtr> ScanMorselX::kEmptyRowsets;

ScanMorsel::ScanMorsel(int32_t plan_node_id, const TScanRange& scan_range)
        : ScanMorselX(plan_node_id), _scan_range(std::make_unique<TScanRange>(scan_range)) {
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

void ScanMorsel::build_scan_morsels(int node_id, const std::vector<TScanRangeParams>& scan_ranges,
                                    bool accept_empty_scan_ranges, pipeline::Morsels* ptr_morsels,
                                    bool* has_more_morsel) {
    pipeline::Morsels& morsels = *ptr_morsels;
    *has_more_morsel = false;
    for (const auto& scan_range : scan_ranges) {
        if (scan_range.__isset.empty && scan_range.empty) {
            if (scan_range.__isset.has_more) {
                *has_more_morsel = scan_range.has_more;
            }
            continue;
        }
        morsels.emplace_back(std::make_unique<pipeline::ScanMorsel>(node_id, scan_range));
    }

    if (morsels.empty() && !accept_empty_scan_ranges) {
        morsels.emplace_back(std::make_unique<pipeline::ScanMorsel>(node_id, TScanRangeParams()));
    }
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
