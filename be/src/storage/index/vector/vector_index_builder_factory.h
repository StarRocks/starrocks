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

#include "common/config.h"
#include "common/status.h"
#include "fs/fs_util.h"
#include "storage/index/vector/vector_index_builder.h"
#include "storage/tablet_schema.h"

namespace starrocks {

class VectorIndexBuilderFactory {
public:
    static StatusOr<std::unique_ptr<VectorIndexBuilder>> create_index_builder(
            const std::shared_ptr<TabletIndex>& tablet_index, const std::string& segment_index_path,
            const IndexBuilderType index_builder_type, const bool is_element_nullable);

    static StatusOr<IndexBuilderType> get_index_builder_type_from_config(std::shared_ptr<TabletIndex> _tablet_index) {
        return IndexBuilderType::TEN_ANN;
    }
};

} // namespace starrocks