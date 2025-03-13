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

namespace starrocks::lake::format {

constexpr const char* SR_FORMAT_WRITER_TYPE = "starrocks.format.writer_type";

constexpr const char* SR_FORMAT_ROWS_PER_SEGMENT = "starrocks.format.rows_per_segment";

constexpr const char* SR_FORMAT_CHUNK_SIZE = "starrocks.format.chunk_size";

constexpr const char* SR_FORMAT_USING_COLUMN_UID = "starrocks.format.using_column_uid";

constexpr const char* SR_FORMAT_QUERY_PLAN = "starrocks.format.query_plan";

constexpr const char* SR_FORMAT_COLUMN_ID = "starrocks.format.column.id";

} // namespace starrocks::lake::format
