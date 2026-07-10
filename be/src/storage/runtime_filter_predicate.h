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
#include <vector>

#include "base/status.h"
#include "storage_primitive/runtime_filter_predicate.h"

namespace starrocks {

class ColumnIterator;
class ObjectPool;
class Schema;

class RuntimeFilterPredicatesRewriter {
public:
    static Status rewrite(ObjectPool* obj_pool, RuntimeFilterPredicates& preds,
                          const std::vector<std::unique_ptr<ColumnIterator>>& column_iterators, const Schema& schema);
};

} // namespace starrocks
