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

#include <string>
#include <unordered_set>

#include "exec/pipeline/scan/scan_morsel.h"
#include "storage/tablet_reader_params.h"

namespace starrocks::pipeline {

class PhysicalSplitScanMorsel final : public ScanMorsel {
public:
    PhysicalSplitScanMorsel(int32_t plan_node_id, const TScanRange& scan_range, RowidRangeOptionPtr rowid_range_option)
            : ScanMorsel(plan_node_id, scan_range), _rowid_range_option(std::move(rowid_range_option)) {}

    ~PhysicalSplitScanMorsel() override = default;

    void init_tablet_reader_params(TabletReaderParams* params) override;

    const std::unordered_set<std::string>& skip_min_max_metrics() const override {
        static const std::unordered_set<std::string> metrics{"ShortKeyFilterRows", "SegmentZoneMapFilterRows"};
        return metrics;
    }

    RowidRangeOptionPtr get_rowid_range_option() { return _rowid_range_option; }

private:
    RowidRangeOptionPtr _rowid_range_option;
};

class LogicalSplitScanMorsel final : public ScanMorsel {
public:
    LogicalSplitScanMorsel(int32_t plan_node_id, const TScanRange& scan_range,
                           ShortKeyRangesOptionPtr short_key_ranges_option)
            : ScanMorsel(plan_node_id, scan_range), _short_key_ranges_option(std::move(short_key_ranges_option)) {}

    ~LogicalSplitScanMorsel() override = default;

    void init_tablet_reader_params(TabletReaderParams* params) override;

    const std::unordered_set<std::string>& skip_min_max_metrics() const override {
        static const std::unordered_set<std::string> metrics{"ShortKeyFilterRows", "SegmentZoneMapFilterRows"};
        return metrics;
    }

    ShortKeyRangesOptionPtr get_short_key_ranges_option() { return _short_key_ranges_option; }

private:
    ShortKeyRangesOptionPtr _short_key_ranges_option;
};

} // namespace starrocks::pipeline
