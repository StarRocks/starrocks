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

#include "runtime/bucket_aware_partition.h"

#include "base/hash/hash_util.hpp"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/nullable_column.h"
#include "gutil/casts.h"

namespace starrocks {

namespace {

int hex_value(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

// Parses "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" into its 16 bytes in string order (msb then lsb, big-endian).
bool parse_uuid(const Slice& s, uint8_t* bytes) {
    if (s.size != 36) return false;
    int k = 0;
    for (int i = 0; i < 36;) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (s.data[i] != '-') return false;
            ++i;
            continue;
        }
        int hi = hex_value(s.data[i]);
        int lo = hex_value(s.data[i + 1]);
        if (hi < 0 || lo < 0) return false;
        bytes[k++] = static_cast<uint8_t>((hi << 4) | lo);
        i += 2;
    }
    return true;
}

// A string that is not a uuid equals no value of a uuid column, so any bucket is correct for it;
// hashing its raw bytes keeps the routing deterministic.
uint32_t uuid_murmur_hash3_x86_32(const Slice& s) {
    uint8_t bytes[16];
    if (parse_uuid(s, bytes)) {
        return HashUtil::murmur_hash3_32(bytes, sizeof(bytes), 0);
    }
    return HashUtil::murmur_hash3_32(s.data, s.size, 0);
}

void uuid_murmur_hash3_x86_32(const Column& column, uint32_t* hashes, size_t num_rows) {
    if (num_rows == 0) return;
    if (column.is_constant()) {
        const Column* inner = down_cast<const ConstColumn&>(column).data_column().get();
        if (inner->is_null(0)) return;
        const auto* binary = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(inner));
        std::fill(hashes, hashes + num_rows, uuid_murmur_hash3_x86_32(binary->get_slice(0)));
        return;
    }
    const auto* binary = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(&column));
    const uint8_t* null_data = nullptr;
    if (column.is_nullable() && column.has_null()) {
        null_data = down_cast<const NullableColumn&>(column).immutable_null_column_data().data();
    }
    for (size_t i = 0; i < num_rows; i++) {
        if (null_data != nullptr && null_data[i]) continue;
        hashes[i] = uuid_murmur_hash3_x86_32(binary->get_slice(i));
    }
}

} // namespace

void bucket_hash_column(const Column& column, TBucketFunction::type func, uint32_t* hashes, size_t num_rows) {
    switch (func) {
    case TBucketFunction::MURMUR3_X86_32_UUID:
        uuid_murmur_hash3_x86_32(column, hashes, num_rows);
        break;
    default:
        DCHECK(func == TBucketFunction::MURMUR3_X86_32);
        column.murmur_hash3_x86_32(hashes, 0, num_rows);
        break;
    }
}

void compute_hash_values_and_bucket_ids(const std::vector<const Column*>& partitions_columns,
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
        round_hashes.assign(num_rows, 0);
        round_ids.assign(num_rows, 0);
        bucket_hash_column(*partitions_columns[i], bucket_properties[i].bucket_func, round_hashes.data(), num_rows);
        for (int j = 0; j < num_rows; j++) {
            hash_values[j] ^= round_hashes[j];
        }
        // Modulo cannot be easily vectorized, keep it in a separate scalar loop
        for (int j = 0; j < num_rows; j++) {
            round_ids[j] = (round_hashes[j] & std::numeric_limits<int>::max()) % bucket_properties[i].bucket_num;
        }
        if (partitions_columns[i]->has_null()) {
            const auto& null_data =
                    down_cast<const NullableColumn*>(partitions_columns[i])->immutable_null_column_data();
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