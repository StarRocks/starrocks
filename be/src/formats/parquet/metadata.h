// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "common/status.h"
#include "formats/parquet/schema.h"
#include "gen_cpp/parquet_types.h"

namespace starrocks::parquet {

// Class corresponding to FileMetaData in thrift
class FileMetaData {
public:
    FileMetaData() {}
    ~FileMetaData() = default;

    Status init(const tparquet::FileMetaData& t_metadata);

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
