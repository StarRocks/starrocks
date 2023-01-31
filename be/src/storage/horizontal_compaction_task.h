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

#include <vector>

#include "common/status.h"
#include "storage/compaction_task.h"
#include "storage/compaction_utils.h"
#include "storage/olap_common.h"

namespace starrocks {

class RowsetWriter;

class TabletReader;
class Schema;
class HorizontalCompactionTask : public CompactionTask {
public:
    HorizontalCompactionTask() : CompactionTask(HORIZONTAL_COMPACTION) {}
    ~HorizontalCompactionTask() override = default;
    Status run_impl() override;

private:
    Status _horizontal_compact_data(Statistics* statistics);
    StatusOr<size_t> _compact_data(int32_t chunk_size, TabletReader& reader, const Schema& schema,
                                   RowsetWriter* output_rs_writer);
    void _failure_callback();
    void _success_callback();
};

} // namespace starrocks
