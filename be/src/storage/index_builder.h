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

#include "storage/tablet_schema.h"
#include "fs/fs.h"
#include "tenann/builder/index_builder.h"

namespace starrocks {

enum IndexBuilderType { TEXT = 0, TEN_ANN = 1 };

class IndexBuilder {
public:
    IndexBuilder(std::shared_ptr<TabletIndex> tablet_index, std::string segment_index_path)
            : _tablet_index(std::move(tablet_index)), _index_path(std::move(segment_index_path)){};
    virtual ~IndexBuilder() = default;

    // init from builder meta
    virtual Status init() = 0;

    // write not null data
    virtual Status write(const Column& data) = 0;

    // write data contains nulls
    virtual Status write(const Column& data, const Column& invalid_row_id_column) = 0;

    // flush data into dist
    virtual Status flush() = 0;

protected:
    std::shared_ptr<TabletIndex> _tablet_index;
    std::string _index_path;
};

class TextIndexBuilder : public IndexBuilder {
public:
    TextIndexBuilder(std::shared_ptr<TabletIndex> tablet_index, std::string segment_index_path)
            : IndexBuilder(std::move(tablet_index), std::move(segment_index_path)){};
    ~TextIndexBuilder() override {
        auto close_status = close();
        if (!close_status.ok()) {
            LOG(FATAL) << "Close resource error with path writer: " << _index_path;
        }
    }
    Status init() override;

    Status write(const Column& data) override;

    Status write(const Column& data, const Column& invalid_row_id_column) override;

    Status flush() override;

    Status close();

private:
    std::unique_ptr<WritableFile> _output_index_file;
};

// A proxy to real Ten ANN index builder
class TenAnnIndexBuilderProxy : public IndexBuilder {
public:
    TenAnnIndexBuilderProxy(std::shared_ptr<TabletIndex> tablet_index, std::string segment_index_path)
            : IndexBuilder(std::move(tablet_index), std::move(segment_index_path)){};

    // proxy should not clean index builder resource
    ~TenAnnIndexBuilderProxy() override = default;

    Status init() override;

    Status write(const Column& data) override;

    Status write(const Column& data, const Column& invalid_row_id_column) override;

    Status flush() override;

private:
    std::shared_ptr<tenann::IndexBuilder> _index_builder;
    uint32_t _dim = 0;
};

} // namespace starrocks