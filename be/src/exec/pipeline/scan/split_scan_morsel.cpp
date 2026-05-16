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

#include "exec/pipeline/scan/split_scan_morsel.h"

namespace starrocks::pipeline {

void PhysicalSplitScanMorsel::init_tablet_reader_params(TabletReaderParams* params) {
    params->rowid_range_option = _rowid_range_option;
}

void LogicalSplitScanMorsel::init_tablet_reader_params(TabletReaderParams* params) {
    params->short_key_ranges_option = _short_key_ranges_option;
}

} // namespace starrocks::pipeline
