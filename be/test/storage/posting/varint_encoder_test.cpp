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

#include "storage/posting/varint_encoder.h"

#include <limits>

#include "gtest/gtest.h"
#include "testutil/assert.h"

namespace starrocks {

class VarIntEncoderTest : public ::testing::Test {
protected:
    void SetUp() override { encoder_ = std::make_shared<VarIntEncoder>(); }
    void TearDown() override {}

    std::shared_ptr<VarIntEncoder> encoder_;
};

// Test encoding and decoding empty roaring
TEST_F(VarIntEncoderTest, test_encode_decode_empty) {
    roaring::Roaring empty;
    std::vector<uint8_t> encoded;
    EXPECT_OK(encoder_->encode(empty, &encoded));
    EXPECT_TRUE(encoded.empty());

    roaring::Roaring decoded;
    EXPECT_OK(encoder_->decode(encoded, &decoded));
    EXPECT_TRUE(decoded.isEmpty());
}

// Test encoding and decoding single value
TEST_F(VarIntEncoderTest, test_encode_decode_single_value) {
    roaring::Roaring original;
    original.add(42);

    std::vector<uint8_t> encoded;
    EXPECT_OK(encoder_->encode(original, &encoded));
    EXPECT_FALSE(encoded.empty());

    roaring::Roaring decoded;
    EXPECT_OK(encoder_->decode(encoded, &decoded));
    EXPECT_EQ(1, decoded.cardinality());
    EXPECT_TRUE(decoded.contains(42));
}

// Test encoding and decoding multiple values
TEST_F(VarIntEncoderTest, test_encode_decode_multiple_values) {
    roaring::Roaring original;
    original.add(1);
    original.add(10);
    original.add(100);
    original.add(1000);
    original.add(10000);

    std::vector<uint8_t> encoded;
    EXPECT_OK(encoder_->encode(original, &encoded));
    EXPECT_FALSE(encoded.empty());

    roaring::Roaring decoded;
    EXPECT_OK(encoder_->decode(encoded, &decoded));
    EXPECT_EQ(5, decoded.cardinality());
    EXPECT_TRUE(decoded.contains(1));
    EXPECT_TRUE(decoded.contains(10));
    EXPECT_TRUE(decoded.contains(100));
    EXPECT_TRUE(decoded.contains(1000));
    EXPECT_TRUE(decoded.contains(10000));
}

// Test encoding and decoding consecutive values
TEST_F(VarIntEncoderTest, test_encode_decode_consecutive_values) {
    roaring::Roaring original;
    for (uint32_t i = 0; i < 100; i++) {
        original.add(i);
    }

    std::vector<uint8_t> encoded;
    EXPECT_OK(encoder_->encode(original, & encoded));
    EXPECT_FALSE(encoded.empty());

    roaring::Roaring decoded;
    EXPECT_OK(encoder_->decode(encoded, &decoded));
    EXPECT_EQ(100, decoded.cardinality());
    for (uint32_t i = 0; i < 100; i++) {
        EXPECT_TRUE(decoded.contains(i));
    }
}

// Test encoding and decoding sparse values
TEST_F(VarIntEncoderTest, test_encode_decode_sparse_values) {
    roaring::Roaring original;
    original.add(0);
    original.add(1000000);
    original.add(2000000);
    original.add(3000000);

    std::vector<uint8_t> encoded;
    EXPECT_OK(encoder_->encode(original, &encoded));
    EXPECT_FALSE(encoded.empty());

    roaring::Roaring decoded;
    EXPECT_OK(encoder_->decode(encoded, &decoded));
    EXPECT_EQ(4, decoded.cardinality());
    EXPECT_TRUE(decoded.contains(0));
    EXPECT_TRUE(decoded.contains(1000000));
    EXPECT_TRUE(decoded.contains(2000000));
    EXPECT_TRUE(decoded.contains(3000000));
}

// Test encoding and decoding boundary values
TEST_F(VarIntEncoderTest, test_encode_decode_boundary_values) {
    roaring::Roaring original;
    original.add(0);
    original.add(127);       // Max 1-byte varint
    original.add(128);       // Min 2-byte varint
    original.add(16383);     // Max 2-byte varint
    original.add(16384);     // Min 3-byte varint
    original.add(2097151);   // Max 3-byte varint
    original.add(2097152);   // Min 4-byte varint
    original.add(268435455); // Max 4-byte varint

    std::vector<uint8_t> encoded;
    roaring::Roaring decoded;
    EXPECT_OK(encoder_->encode(original, &encoded));
    EXPECT_OK(encoder_->decode(encoded, &decoded));

    EXPECT_EQ(original.cardinality(), decoded.cardinality());
    EXPECT_TRUE(decoded.contains(0));
    EXPECT_TRUE(decoded.contains(127));
    EXPECT_TRUE(decoded.contains(128));
    EXPECT_TRUE(decoded.contains(16383));
    EXPECT_TRUE(decoded.contains(16384));
    EXPECT_TRUE(decoded.contains(2097151));
    EXPECT_TRUE(decoded.contains(2097152));
    EXPECT_TRUE(decoded.contains(268435455));
}

// Test encoding and decoding max uint32 value
TEST_F(VarIntEncoderTest, test_encode_decode_max_value) {
    roaring::Roaring original;
    original.add(std::numeric_limits<uint32_t>::max());

    std::vector<uint8_t> encoded;
    roaring::Roaring decoded;
    EXPECT_OK(encoder_->encode(original, &encoded));
    EXPECT_FALSE(encoded.empty());

    EXPECT_OK(encoder_->decode(encoded, &decoded));
    EXPECT_EQ(1, decoded.cardinality());
    EXPECT_TRUE(decoded.contains(std::numeric_limits<uint32_t>::max()));
}

// Test encodeValue and decodeValue helper methods
TEST_F(VarIntEncoderTest, test_encode_decode_value_helpers) {
    std::vector<uint8_t> output;

    // Test small value
    VarIntEncoder::encodeValue(42, &output);
    size_t offset = 0;
    uint32_t decoded = VarIntEncoder::decodeValue(output, offset);
    EXPECT_EQ(42, decoded);

    // Test larger value
    output.clear();
    VarIntEncoder::encodeValue(12345, &output);
    offset = 0;
    decoded = VarIntEncoder::decodeValue(output, offset);
    EXPECT_EQ(12345, decoded);

    // Test max value
    output.clear();
    VarIntEncoder::encodeValue(std::numeric_limits<uint32_t>::max(), &output);
    offset = 0;
    decoded = VarIntEncoder::decodeValue(output, offset);
    EXPECT_EQ(std::numeric_limits<uint32_t>::max(), decoded);
}

// Test large dataset encode/decode
TEST_F(VarIntEncoderTest, test_encode_decode_large_dataset) {
    roaring::Roaring original;
    for (uint32_t i = 0; i < 10000; i += 7) {
        original.add(i);
    }

    std::vector<uint8_t> encoded;
    roaring::Roaring decoded;
    EXPECT_OK(encoder_->encode(original, &encoded));
    EXPECT_OK(encoder_->decode(encoded, &decoded));

    EXPECT_EQ(original.cardinality(), decoded.cardinality());
    EXPECT_EQ(original, decoded);
}

} // namespace starrocks