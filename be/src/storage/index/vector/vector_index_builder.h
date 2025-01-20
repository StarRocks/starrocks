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

#include "storage/tablet_schema.h"

namespace starrocks {

enum IndexBuilderType { TEN_ANN = 0, UNKNOWN = 100 };

class VectorIndexBuilder {
public:
    VectorIndexBuilder(std::shared_ptr<TabletIndex> tablet_index, std::string segment_index_path)
            : _tablet_index(std::move(tablet_index)), _index_path(std::move(segment_index_path)){};
    virtual ~VectorIndexBuilder() = default;

    // init from builder meta
    virtual Status init() = 0;

    virtual Status add(const Column& array_column, const size_t offset) = 0;

    // flush data into disk
    virtual Status flush() = 0;

    // we should make sure the independence of TenAnn index, include data and metadata, to make [[IndexScanNode]] simple
    // enough in the future other than to read the meta both in StarRocks and TenAnn.
    // Furthermore, TenAnn index within tablet level should decouple with segment, therefore we should do the empty mark
    // by marking the index file.
    static Status flush_empty(const std::string& segment_index_path);

protected:
    std::shared_ptr<TabletIndex> _tablet_index;
    std::unique_ptr<VectorIndexBuilder> _debug_writer;
    std::string _index_path;
};

class EmptyVectorIndexBuilder final : public VectorIndexBuilder {
public:
    EmptyVectorIndexBuilder(std::shared_ptr<TabletIndex> tablet_index, std::string segment_index_path)
            : VectorIndexBuilder(std::move(tablet_index), std::move(segment_index_path)){};

    Status init() override { return Status::OK(); }

    Status add(const Column& array_column, const size_t offset) override { return Status::OK(); }

    Status flush() override {
        RETURN_IF_ERROR(VectorIndexBuilder::flush_empty(_index_path));
        return Status::OK();
    }
};

} // namespace starrocks