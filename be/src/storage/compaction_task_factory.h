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

#include <utility>
#include <vector>

#include "common/statusor.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet.h"

namespace starrocks {

class CompactionTask;

class CompactionTaskFactory {
public:
    CompactionTaskFactory(Version output_version, TabletSharedPtr tablet, std::vector<RowsetSharedPtr>&& input_rowsets,
                          double compaction_score, CompactionType compaction_type)
            : _output_version(output_version),
              _tablet(std::move(tablet)),
              _input_rowsets(std::move(input_rowsets)),
              _compaction_score(compaction_score),
              _compaction_type(compaction_type) {}
    ~CompactionTaskFactory() = default;

    std::shared_ptr<CompactionTask> create_compaction_task();

private:
    Version _output_version;
    TabletSharedPtr _tablet;
    std::vector<RowsetSharedPtr> _input_rowsets;
    double _compaction_score;
    CompactionType _compaction_type;
};

} // namespace starrocks
