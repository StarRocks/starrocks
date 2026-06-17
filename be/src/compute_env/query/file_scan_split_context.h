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

#include <cstddef>
#include <memory>
#include <vector>

#include "exec/pipeline/scan/scan_morsel.h"

namespace starrocks {

struct FileScanSplitContext : public pipeline::ScanSplitContext {
    size_t start_offset = 0;
    size_t end_offset = 0;

    virtual std::unique_ptr<FileScanSplitContext> clone() = 0;
};

using FileScanSplitContextPtr = std::unique_ptr<FileScanSplitContext>;

void merge_file_scan_split_tasks(std::vector<FileScanSplitContextPtr>* split_tasks, size_t max_split_size);

} // namespace starrocks
