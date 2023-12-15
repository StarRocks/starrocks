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

#include "formats/parquet/metadata.h"

#include <sstream>

#include "formats/parquet/schema.h"

namespace starrocks::parquet {

Status FileMetaData::init(tparquet::FileMetaData& t_metadata, bool case_sensitive) {
    // construct schema from thrift
    RETURN_IF_ERROR(_schema.from_thrift(t_metadata.schema, case_sensitive));
    _num_rows = t_metadata.num_rows;
<<<<<<< Updated upstream
    _t_metadata = std::move(t_metadata);
    if (_t_metadata.__isset.created_by) {
        _writer_version = ApplicationVersion(_t_metadata.created_by);
    } else {
        _writer_version = ApplicationVersion("unknown 0.0.0");
    }
=======

    _t_metadata = t_metadata;
>>>>>>> Stashed changes
    return Status::OK();
}

std::string FileMetaData::debug_string() const {
    std::stringstream ss;
    ss << "schema=" << _schema.debug_string();
    return ss.str();
}

} // namespace starrocks::parquet
