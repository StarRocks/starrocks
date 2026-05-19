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

#include "exec/pipeline/scan/morsel_queue.h"

namespace starrocks::pipeline {

static std::vector<TInternalScanRange*> convert_morsels_to_olap_scan_ranges(const Morsels& morsels) {
    std::vector<TInternalScanRange*> scan_ranges;
    scan_ranges.reserve(morsels.size());
    for (const auto& morsel : morsels) {
        auto* scan_morsel = down_cast<ScanMorsel*>(morsel.get());
        auto* scan_range = scan_morsel->get_olap_scan_range();
        scan_ranges.emplace_back(scan_range);
    }
    return scan_ranges;
}

std::vector<TInternalScanRange*> MorselQueue::prepare_olap_scan_ranges() const {
    return convert_morsels_to_olap_scan_ranges(_morsels);
}

void MorselQueue::unget(MorselPtr&& morsel) {
    _unget_morsel = std::move(morsel);
}

Status MorselQueue::append_morsels(Morsels&& morsels) {
    return Status::NotSupported("MorselQueue::append_morsels not supported");
}

} // namespace starrocks::pipeline
