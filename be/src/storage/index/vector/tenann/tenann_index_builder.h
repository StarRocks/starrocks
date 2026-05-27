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
#include "storage/index/vector/vector_index_file_writer.h"
#include "storage/tablet_schema.h"
#include "tenann/builder/index_builder.h"

namespace starrocks {

// A proxy to real Ten ANN index builder
class TenAnnIndexBuilderProxy final : public VectorIndexBuilder {
public:
    TenAnnIndexBuilderProxy(std::shared_ptr<TabletIndex> tablet_index, std::string segment_index_path,
                            bool is_element_nullable, int omp_threads)
            : VectorIndexBuilder(std::move(tablet_index), std::move(segment_index_path)),
              _is_element_nullable(is_element_nullable),
              _omp_threads(omp_threads),
              _file_writer(nullptr) {}

    TenAnnIndexBuilderProxy(std::shared_ptr<TabletIndex> tablet_index, std::string segment_index_path,
                            bool is_element_nullable, int omp_threads, VectorIndexFileWriter* file_writer)
            : VectorIndexBuilder(std::move(tablet_index), std::move(segment_index_path)),
              _is_element_nullable(is_element_nullable),
              _omp_threads(omp_threads),
              _file_writer(file_writer) {}

    // proxy should not clean index builder resource
    ~TenAnnIndexBuilderProxy() override { (void)close(); };

    Status init() override;

    Status add(const Column& array_column, const size_t offset) override;

    Status flush() override;

    Status close() const override;

private:
    std::shared_ptr<tenann::IndexBuilder> _index_builder = nullptr;
    uint32_t _dim = 0;

    const bool _is_element_nullable;
    const int _omp_threads;
    VectorIndexFileWriter* _file_writer = nullptr;
};

} // namespace starrocks

#endif