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

#include "storage/index/vector/vector_search_executor.h"

namespace starrocks {

Status DefaultVectorSearchExecutor::search(const VectorQuery& query, const RowIdFilter* filter,
                                           VectorSearchOutput* output) {
    output->ann_result.clear();
    output->profile.clear();
    return _strategy->execute(_ann_index.get(), query, filter, &output->ann_result);
}

Status DefaultVectorSearchExecutor::close() {
    if (_ann_index) {
        RETURN_IF_ERROR(_ann_index->close());
    }
    return Status::OK();
}

} // namespace starrocks
