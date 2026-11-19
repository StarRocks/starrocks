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

#include <vector>

#include "column/column.h"
#include "gen_cpp/Partitions_types.h"

namespace starrocks {

struct BucketAwarePartitionCtx {
    BucketAwarePartitionCtx(const std::vector<TBucketProperty>& bucket_properties, std::vector<uint32_t>& hash_values,
                            std::vector<uint32_t>& round_hashes, std::vector<uint32_t>& bucket_ids,
                            std::vector<uint32_t>& round_ids)
            : bucket_properties(bucket_properties),
              hash_values(hash_values),
              round_hashes(round_hashes),
              bucket_ids(bucket_ids),
              round_ids(round_ids) {}
    const std::vector<TBucketProperty>& bucket_properties;
    std::vector<uint32_t>& hash_values;
    std::vector<uint32_t>& round_hashes;
    std::vector<uint32_t>& bucket_ids;
    std::vector<uint32_t>& round_ids;
};

// Iceberg bucket hash of one column, written into hashes[0, num_rows). NULL rows are left untouched.
// MURMUR3_X86_32_UUID parses each 36-char uuid string to its 16 bytes before hashing, as Iceberg does.
void bucket_hash_column(const Column& column, TBucketFunction::type func, uint32_t* hashes, size_t num_rows);

void compute_hash_values_and_bucket_ids(const std::vector<const Column*>& partitions_columns,
                                        BucketAwarePartitionCtx ctx);

} // namespace starrocks