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

#include "storage/index/vector/vector_index_builder_factory.h"

#include "column/array_column.h"
#include "storage/index/vector/tenann/tenann_index_builder.h"

namespace starrocks {
// =============== IndexBuilderFactory =============
StatusOr<std::unique_ptr<VectorIndexBuilder>> VectorIndexBuilderFactory::create_index_builder(
        const std::shared_ptr<TabletIndex>& tablet_index, const std::string& segment_index_path,
        const IndexBuilderType index_builder_type, const bool is_element_nullable) {
    switch (index_builder_type) {
    case TEN_ANN:
#ifdef WITH_TENANN
        return std::make_unique<TenAnnIndexBuilderProxy>(tablet_index, segment_index_path, is_element_nullable);
#else
        return std::make_unique<EmptyVectorIndexBuilder>(tablet_index, segment_index_path);
#endif
    default:
        return Status::InternalError("Unknown index builder type");
    }
}

} // namespace starrocks