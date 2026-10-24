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

#include "formats/delta/deletion_vector.h"

#include <gtest/gtest.h>
#include <roaring/roaring64.h>
#include <roaring/roaring_version.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "base/simd/simd.h"
#include "base/string/base85.h"
#include "column/vectorized_fwd.h"
#include "formats/deletion_bitmap.h"

namespace starrocks::formats {

using Roaring64Guard = std::unique_ptr<roaring64_bitmap_t, decltype(&roaring::api::roaring64_bitmap_free)>;

class DeletionVectorTest : public testing::Test {
public:
    void SetUp() override { dv = std::make_shared<DeletionVector>(options); }

    DeletionVectorOptions options;
    std::shared_ptr<DeletionVector> dv;
};

TEST_F(DeletionVectorTest, inlineDeletionVectorTest) {
    std::string encoded_inline_dv = "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L";
    SkipRowsContextPtr skipRowsContext = std::make_shared<SkipRowsContext>();
    Status status = dv->deserialized_inline_dv(encoded_inline_dv, skipRowsContext);
    ASSERT_TRUE(status.ok());

    uint64_t cardinality = skipRowsContext->deletion_bitmap->get_cardinality();
    vector<uint64_t> bitmap_vector(cardinality);
    skipRowsContext->deletion_bitmap->to_array(bitmap_vector);
    ASSERT_EQ(6, bitmap_vector.size());
    std::stringstream ss;
    for (int64_t rowid : bitmap_vector) {
        ss << rowid << " ";
    }
    ASSERT_EQ("3 4 7 11 18 29 ", ss.str());
}

TEST_F(DeletionVectorTest, absolutePathTest) {
    // test relative path
    options.descriptor.__set_storageType("u");
    options.descriptor.__set_pathOrInlineDv("ab^-aqEH.-t@S}K{vb[*k^");
    dv = std::make_shared<DeletionVector>(options);

    std::string table_location = "s3://mytable";
    StatusOr<std::string> absolute_path = dv->get_absolute_path(table_location);
    ASSERT_TRUE(absolute_path.ok());
    ASSERT_EQ("s3://mytable/ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin", absolute_path.value());
    // test absolute path
    options.descriptor.__set_storageType("p");
    options.descriptor.__set_pathOrInlineDv("s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin");
    dv = std::make_shared<DeletionVector>(options);
    absolute_path = dv->get_absolute_path(table_location);
    ASSERT_TRUE(absolute_path.ok());
    ASSERT_EQ("s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin", absolute_path.value());
}

TEST_F(DeletionVectorTest, croaring_5_3_2_cardinality_and_unaligned_deserialize) {
    ASSERT_STREQ("5.3.2", ROARING_VERSION);

    Roaring64Guard raw(roaring::api::roaring64_bitmap_create(), &roaring::api::roaring64_bitmap_free);
    ASSERT_NE(nullptr, raw.get());
    // Array container (high 48-bit key 0)
    for (uint64_t i = 0; i < 300; ++i) {
        roaring::api::roaring64_bitmap_add(raw.get(), i * 3);
    }
    // Bitset container (high 48-bit key 1)
    for (uint64_t i = 0; i < 65536; i += 2) {
        roaring::api::roaring64_bitmap_add(raw.get(), 65536ULL + i);
    }
    // Run container (high 48-bit key 2)
    roaring::api::roaring64_bitmap_add_range_closed(raw.get(), 131072ULL, 150000ULL);
    roaring::api::roaring64_bitmap_run_optimize(raw.get());

    size_t ser_size = roaring::api::roaring64_bitmap_portable_size_in_bytes(raw.get());
    std::vector<char> unaligned_buf(ser_size + 8, 0);
    char* odd_ptr = unaligned_buf.data() + 1;
    ASSERT_EQ(ser_size, roaring::api::roaring64_bitmap_portable_serialize(raw.get(), odd_ptr));
    raw.reset();

    roaring64_bitmap_t* deserialized = roaring::api::roaring64_bitmap_portable_deserialize_safe(odd_ptr, ser_size);
    ASSERT_NE(nullptr, deserialized);

    DeletionBitmap del_bitmap(deserialized);
    const uint64_t expected_total = 300 + 32768 + (150000 - 131072 + 1);
    ASSERT_EQ(expected_total, del_bitmap.get_cardinality());
    ASSERT_EQ(300ULL, del_bitmap.get_range_cardinality(0, 65536));
    ASSERT_EQ(32768ULL, del_bitmap.get_range_cardinality(65536, 131072));

    Filter filter(1024, 1);
    auto res = del_bitmap.fill_filter(65536, 65536 + 1024, filter);
    ASSERT_TRUE(res.ok());
    ASSERT_TRUE(res.value());
    ASSERT_EQ(512U, SIMD::count_nonzero(filter));
}

TEST_F(DeletionVectorTest, croaring_5_3_2_portable_deserialize_safe_and_corrupt_rejection) {
    ASSERT_STREQ("5.3.2", ROARING_VERSION);

    Roaring64Guard raw(roaring::api::roaring64_bitmap_create(), &roaring::api::roaring64_bitmap_free);
    ASSERT_NE(nullptr, raw.get());
    roaring::api::roaring64_bitmap_add(raw.get(), 42);
    roaring::api::roaring64_bitmap_add(raw.get(), 1000);
    size_t ser_size = roaring::api::roaring64_bitmap_portable_size_in_bytes(raw.get());
    std::vector<char> buf(ser_size + 8, 0);
    char* odd_ptr = buf.data() + 1;
    ASSERT_EQ(ser_size, roaring::api::roaring64_bitmap_portable_serialize(raw.get(), odd_ptr));
    raw.reset();

    // Safe deserialize succeeds on valid unaligned buffer and preserves exact elements
    Roaring64Guard ok_bm(roaring::api::roaring64_bitmap_portable_deserialize_safe(odd_ptr, ser_size),
                         &roaring::api::roaring64_bitmap_free);
    ASSERT_NE(nullptr, ok_bm.get());
    ASSERT_EQ(2ULL, roaring::api::roaring64_bitmap_get_cardinality(ok_bm.get()));
    ASSERT_TRUE(roaring::api::roaring64_bitmap_contains(ok_bm.get(), 42));
    ASSERT_TRUE(roaring::api::roaring64_bitmap_contains(ok_bm.get(), 1000));
    ASSERT_FALSE(roaring::api::roaring64_bitmap_contains(ok_bm.get(), 43));

    // Boundary truncations (0 bytes, 1 byte, mid-stream, off-by-one tail) are safely rejected
    for (size_t bad_len : {size_t(0), size_t(1), ser_size / 2, ser_size - 1}) {
        ASSERT_EQ(nullptr, roaring::api::roaring64_bitmap_portable_deserialize_safe(odd_ptr, bad_len));
    }

    // Corrupted high-key group count header (0xFFFFFFFFFFFFFFFFULL) within a bounded buffer is safely rejected
    std::vector<char> corrupt_header(odd_ptr, odd_ptr + ser_size);
    const uint64_t bogus_group_count = ~uint64_t(0);
    std::memcpy(corrupt_header.data(), &bogus_group_count, sizeof(bogus_group_count));
    ASSERT_EQ(nullptr,
              roaring::api::roaring64_bitmap_portable_deserialize_safe(corrupt_header.data(), corrupt_header.size()));
}
} // namespace starrocks::formats
