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

#include <arrow/array.h>

#include "column/column.h"
#include "common/status.h"

namespace starrocks {

// Appends [start, start + num_rows) of a Paimon BLOB arrow column into a TYPE_FILE column
// (a NullableColumn wrapping a FileColumn).
//
// paimon-cpp exposes BLOB as arrow large_binary. Each value is either the blob payload itself
// or, for columns stored as descriptors (blob-descriptor-field / blob-as-descriptor), a
// serialized paimon BlobDescriptor:
//   version:int8 | magic:int64 (0x424C4F4244455343 "BLOBDESC") | uri_len:int32 | uri |
//   offset:int64 | length:int64          (little endian, version 2)
// A descriptor becomes a reference row (uri/offset/size); anything else becomes an inline row.
Status append_paimon_blob_to_file_column(const arrow::Array* array, size_t start, size_t num_rows, Column* dst);

} // namespace starrocks
