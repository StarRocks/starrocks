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

#include "storage/posting/encoder.h"

#include "gtest/gtest.h"

namespace starrocks {

class EncoderFactoryTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// Test EncoderFactory creates VarIntEncoder correctly
TEST_F(EncoderFactoryTest, test_create_varint_encoder) {
    auto encoder = EncoderFactory::createEncoder(EncodingType::VARINT);
    ASSERT_NE(nullptr, encoder);
    EXPECT_EQ(EncodingType::VARINT, encoder->getType());
    EXPECT_STREQ("VarInt", encoder->getName());
}

// Test EncoderFactory returns nullptr for unknown encoding type
TEST_F(EncoderFactoryTest, test_create_unknown_encoder) {
    auto encoder = EncoderFactory::createEncoder(EncodingType::UNKNOWN);
    EXPECT_EQ(nullptr, encoder);
}

} // namespace starrocks
