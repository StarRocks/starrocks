// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "formats/parquet/metadata.h"

#include <sstream>

#include "formats/parquet/schema.h"

namespace starrocks::parquet {

Status FileMetaData::init(const tparquet::FileMetaData& t_metadata) {
    // construct schema from thrift
    RETURN_IF_ERROR(_schema.from_thrift(t_metadata.schema));
    _num_rows = t_metadata.num_rows;

    _t_metadata = t_metadata;
    return Status::OK();
}

std::string FileMetaData::debug_string() const {
    std::stringstream ss;
    ss << "schema=" << _schema.debug_string();
    return ss.str();
}

} // namespace starrocks::parquet
