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

#include <cstdint>

#include "common/status.h"
#include "fs/fs.h"
#include "storage/tablet_schema.h"

namespace starrocks {

// Rebuilds every Tantivy GIN index declared by |tablet_schema| from the final
// logical contents of one segment. Primary Key row-mode partial update uses
// this after missing columns have been materialized so the resulting compound
// .idx contains all indexed columns, not just the columns written in one phase.
class TantivyIndexRebuilder {
public:
    static Status rebuild(const FileInfo& segment_file_info, uint32_t segment_id,
                          const TabletSchemaCSPtr& tablet_schema);

private:
    Status _rebuild(const FileInfo& segment_file_info, uint32_t segment_id, const TabletSchemaCSPtr& tablet_schema);
};

} // namespace starrocks
