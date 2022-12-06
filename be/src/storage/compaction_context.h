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

#include <memory>
#include <set>
#include <vector>

#include "storage/compaction_policy.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet.h"

namespace starrocks {

class CompactionPolicy;

struct CompactionContext {
    double score = 0;
    CompactionType type = INVALID_COMPACTION;
    std::unique_ptr<CompactionPolicy> policy;
};

} // namespace starrocks
