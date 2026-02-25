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

#ifdef WITH_TENANN

#include <memory>

#include "fs/fs.h"
#include "storage/index/vector/vector_index_builder.h"
#include "storage/tablet_schema.h"
#include "tenann/builder/index_builder.h"

namespace starrocks {

// A proxy to real Ten ANN index builder
class TenAnnIndexBuilderProxy final : public VectorIndexBuilder {
public:
    TenAnnIndexBuilderProxy(std::shared_ptr<TabletIndex> tablet_index, std::string segment_index_path,
                            bool is_element_nullable)
            : VectorIndexBuilder(std::move(tablet_index), std::move(segment_index_path)),
              _is_element_nullable(is_element_nullable) {}

    // proxy should not clean index builder resource
    ~TenAnnIndexBuilderProxy() override { close(); };

    Status init() override;

    Status add(const Column& array_column, const size_t offset) override;

    Status flush() override;

    void close() const;

private:
    OnceFlag _init_once;
    std::shared_ptr<tenann::IndexBuilder> _index_builder = nullptr;
    uint32_t _dim = 0;
    // This will be true when `metric_type` is cosine_similarity and `is_vector_normed` is true.
    // When it is true, the vector (a row of the array column) is either null or the sum of the squares of all elements
    // equals 1.
    bool _is_input_normalized = false;

    const bool _is_element_nullable;
};

} // namespace starrocks

#endif