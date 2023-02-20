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

#include "common/status.h"
#include "formats/parquet/schema.h"
#include "gen_cpp/parquet_types.h"

namespace starrocks::parquet {

// Class corresponding to FileMetaData in thrift
class FileMetaData {
public:
    FileMetaData() = default;
    ~FileMetaData() = default;

    Status init(const tparquet::FileMetaData& t_metadata, bool case_sensitive);

    uint64_t num_rows() const { return _num_rows; }

    std::string debug_string() const;

    const tparquet::FileMetaData& t_metadata() const { return _t_metadata; }

    const SchemaDescriptor& schema() const { return _schema; }

private:
    tparquet::FileMetaData _t_metadata;
    uint64_t _num_rows{0};
    SchemaDescriptor _schema;
};

} // namespace starrocks::parquet
