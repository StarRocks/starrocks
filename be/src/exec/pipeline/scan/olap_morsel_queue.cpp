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

#include "exec/pipeline/scan/olap_morsel_queue.h"

namespace starrocks::pipeline {

std::vector<TInternalScanRange*> OlapMorselQueue::prepare_olap_scan_ranges() const {
    std::vector<TInternalScanRange*> scan_ranges;
    scan_ranges.reserve(_morsels.size());
    for (const auto& morsel : _morsels) {
        scan_ranges.emplace_back(morsel->get_olap_scan_range());
    }
    return scan_ranges;
}

} // namespace starrocks::pipeline
