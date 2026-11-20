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

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/const_column.h"
#include "column/nullable_column.h"

namespace starrocks {
namespace {

// Hashes from the Iceberg spec (Appendix B) and Iceberg Java
// Transforms.bucket(N).bind(Types.UUIDType.get()), i.e. murmur3_x86_32 (seed 0) of the 16 uuid bytes.
constexpr const char* kUuidSpec = "f79c3e09-677c-4bbd-a479-3f349cb785e7"; // 1488055340
constexpr const char* kUuidZero = "00000000-0000-0000-0000-000000000000"; // 2167721464
constexpr const char* kUuidOnes = "ffffffff-ffff-ffff-ffff-ffffffffffff"; // 3843634286

ColumnPtr nullable_strings(const std::vector<std::string>& values, const std::vector<uint8_t>& nulls) {
    auto data = BinaryColumn::create();
    auto null_col = NullColumn::create();
    for (size_t i = 0; i < values.size(); i++) {
        data->append(Slice(values[i]));
        null_col->append(nulls[i]);
    }
    return NullableColumn::create(std::move(data), std::move(null_col));
}

std::vector<uint32_t> bucket_ids(const Column* column, TBucketFunction::type func, int32_t bucket_num) {
    TBucketProperty prop;
    prop.__set_bucket_func(func);
    prop.__set_bucket_num(bucket_num);
    std::vector<TBucketProperty> props{prop};
    std::vector<uint32_t> hash_values, round_hashes, ids, round_ids;
    BucketAwarePartitionCtx ctx(props, hash_values, round_hashes, ids, round_ids);
    compute_hash_values_and_bucket_ids({column}, ctx);
    return ids;
}

TEST(BucketAwarePartitionTest, uuid_hash_matches_iceberg) {
    auto col = BinaryColumn::create();
    for (const char* u : {kUuidSpec, kUuidZero, kUuidOnes}) col->append(Slice(u));
    std::vector<uint32_t> hashes(3, 0);
    bucket_hash_column(*col, TBucketFunction::MURMUR3_X86_32_UUID, hashes.data(), 3);
    EXPECT_EQ(std::vector<uint32_t>({1488055340u, 2167721464u, 3843634286u}), hashes);
}

TEST(BucketAwarePartitionTest, uuid_bucket_ids_with_null) {
    auto col = nullable_strings({kUuidSpec, kUuidZero, kUuidOnes, ""}, {0, 0, 0, 1});
    // (h & INT_MAX) % 16; NULL goes to bucket N.
    EXPECT_EQ(std::vector<uint32_t>({12, 8, 14, 16}), bucket_ids(col.get(), TBucketFunction::MURMUR3_X86_32_UUID, 16));
    // Hashing the 36-char string instead routes the same uuid elsewhere: the bug this function fixes.
    EXPECT_NE(bucket_ids(col.get(), TBucketFunction::MURMUR3_X86_32_UUID, 16),
              bucket_ids(col.get(), TBucketFunction::MURMUR3_X86_32, 16));
}

TEST(BucketAwarePartitionTest, uuid_const_column) {
    auto inner = BinaryColumn::create();
    inner->append(Slice(kUuidSpec));
    auto col = ConstColumn::create(std::move(inner), 3);
    EXPECT_EQ(std::vector<uint32_t>({340, 340, 340}),
              bucket_ids(col.get(), TBucketFunction::MURMUR3_X86_32_UUID, 1000));
}

TEST(BucketAwarePartitionTest, uuid_malformed_is_deterministic) {
    auto col = BinaryColumn::create();
    for (const char* s : {"not-a-uuid", "f79c3e09-677c-4bbd-a479-3f349cb785eZ", "not-a-uuid"}) col->append(Slice(s));
    std::vector<uint32_t> hashes(3, 0);
    bucket_hash_column(*col, TBucketFunction::MURMUR3_X86_32_UUID, hashes.data(), 3);
    EXPECT_EQ(hashes[0], hashes[2]);
    EXPECT_NE(1488055340u, hashes[1]);
}

} // namespace
} // namespace starrocks
