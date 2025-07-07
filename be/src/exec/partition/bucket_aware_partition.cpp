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

#include "exec/partition/bucket_aware_partition.h"

#include "column/nullable_column.h"
#include "gutil/casts.h"

namespace starrocks {

void calc_hash_values_and_bucket_ids(const std::vector<const Column*>& partitions_columns,
                                     BucketAwarePartitionCtx ctx) {
    size_t num_rows = partitions_columns[0]->size();
    const auto& bucket_properties = ctx.bucket_properties;
    auto& hash_values = ctx.hash_values;
    auto& bucket_ids = ctx.bucket_ids;
    auto& round_hashes = ctx.round_hashes;
    auto& round_ids = ctx.round_ids;

    hash_values.assign(num_rows, 0);
    bucket_ids.assign(num_rows, 0);
    for (int i = 0; i < partitions_columns.size(); ++i) {
        // TODO, enhance it if we try to support more bucket functions.
        DCHECK(bucket_properties[i].bucket_func == TBucketFunction::MURMUR3_X86_32);
        round_hashes.assign(num_rows, 0);
        round_ids.assign(num_rows, 0);
        partitions_columns[i]->murmur_hash3_x86_32(&round_hashes[0], 0, num_rows);
        for (int j = 0; j < num_rows; j++) {
            hash_values[j] ^= round_hashes[j];
            round_ids[j] = (round_hashes[j] & std::numeric_limits<int>::max()) % bucket_properties[i].bucket_num;
        }
        if (partitions_columns[i]->has_null()) {
            const auto& null_data =
                    down_cast<const NullableColumn*>(partitions_columns[i])->null_column()->immutable_data();
            for (int j = 0; j < num_rows; j++) {
                round_ids[j] = null_data[j] ? bucket_properties[i].bucket_num : round_ids[j];
            }
        }

        if (i == partitions_columns.size() - 1) {
            for (int j = 0; j < num_rows; j++) {
                bucket_ids[j] += round_ids[j];
            }
        } else {
            for (int j = 0; j < num_rows; j++) {
                // bucket mapping, same behavior as FE
                bucket_ids[j] = (round_ids[j] + bucket_ids[j]) * (bucket_properties[i + 1].bucket_num + 1);
            }
        }
    }
}

} // namespace starrocks