/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "BloomFilter.hh"
#include "orc/OrcFile.hh"
#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

namespace orc {

TEST(TestBloomFilter, testBitSetEqual) {
    BitSet bitSet64_1(64), bitSet64_2(64), bitSet32(128);
    EXPECT_TRUE(bitSet64_1 == bitSet64_2);
    EXPECT_FALSE(bitSet64_1 == bitSet32);

    bitSet64_1.set(6U);
    bitSet64_1.set(16U);
    bitSet64_1.set(26U);
    bitSet64_2.set(6U);
    bitSet64_2.set(16U);
    bitSet64_2.set(26U);
    EXPECT_TRUE(bitSet64_1 == bitSet64_2);
    EXPECT_EQ(bitSet64_1.get(6U), bitSet64_2.get(6U));
    EXPECT_EQ(bitSet64_1.get(16U), bitSet64_2.get(16U));
    EXPECT_EQ(bitSet64_1.get(26U), bitSet64_2.get(26U));

    bitSet64_1.set(36U);
    bitSet64_2.set(46U);
    EXPECT_FALSE(bitSet64_1 == bitSet64_2);
    EXPECT_TRUE(bitSet64_1.get(36U));
    EXPECT_TRUE(bitSet64_2.get(46U));

    bitSet64_1.clear();
    bitSet64_2.clear();
    EXPECT_TRUE(bitSet64_1 == bitSet64_2);
}

// ported from Java ORC
TEST(TestBloomFilter, testSetGetBitSet) {
    BitSet bitset(128);

    // set every 9th bit for a rotating pattern
    for (uint64_t l = 0; l < 8; ++l) {
        bitset.set(l * 9);
    }

    // set every non-9th bit
    for (uint64_t l = 8; l < 16; ++l) {
        for (uint64_t b = 0; b < 8; ++b) {
            if (b != l - 8) {
                bitset.set(l * 8 + b);
            }
        }
    }

    for (uint64_t b = 0; b < 64; ++b) {
        EXPECT_EQ(b % 9 == 0, bitset.get(b));
    }

    for (uint64_t b = 64; b < 128; ++b) {
        EXPECT_EQ((b % 8) != (b - 64) / 8, bitset.get(b));
    }

    // test that the longs are mapped correctly
    const uint64_t* longs = bitset.getData();
    EXPECT_EQ(128, bitset.bitSize());
    EXPECT_EQ(0x8040201008040201L, longs[0]);
    EXPECT_EQ(~0x8040201008040201L, longs[1]);
}

TEST(TestBloomFilter, testBloomFilterBasicOperations) {
    BloomFilterImpl bloomFilter(128);

    // test integers
    bloomFilter.reset();
    EXPECT_FALSE(bloomFilter.testLong(1));
    EXPECT_FALSE(bloomFilter.testLong(11));
    EXPECT_FALSE(bloomFilter.testLong(111));
    EXPECT_FALSE(bloomFilter.testLong(1111));
    EXPECT_FALSE(bloomFilter.testLong(0));
    EXPECT_FALSE(bloomFilter.testLong(-1));
    EXPECT_FALSE(bloomFilter.testLong(-11));
    EXPECT_FALSE(bloomFilter.testLong(-111));
    EXPECT_FALSE(bloomFilter.testLong(-1111));

    bloomFilter.addLong(1);
    bloomFilter.addLong(11);
    bloomFilter.addLong(111);
    bloomFilter.addLong(1111);
    bloomFilter.addLong(0);
    bloomFilter.addLong(-1);
    bloomFilter.addLong(-11);
    bloomFilter.addLong(-111);
    bloomFilter.addLong(-1111);

    EXPECT_TRUE(bloomFilter.testLong(1));
    EXPECT_TRUE(bloomFilter.testLong(11));
    EXPECT_TRUE(bloomFilter.testLong(111));
    EXPECT_TRUE(bloomFilter.testLong(1111));
    EXPECT_TRUE(bloomFilter.testLong(0));
    EXPECT_TRUE(bloomFilter.testLong(-1));
    EXPECT_TRUE(bloomFilter.testLong(-11));
    EXPECT_TRUE(bloomFilter.testLong(-111));
    EXPECT_TRUE(bloomFilter.testLong(-1111));

    // test doubles
    bloomFilter.reset();
    EXPECT_FALSE(bloomFilter.testDouble(1.1));
    EXPECT_FALSE(bloomFilter.testDouble(11.11));
    EXPECT_FALSE(bloomFilter.testDouble(111.111));
    EXPECT_FALSE(bloomFilter.testDouble(1111.1111));
    EXPECT_FALSE(bloomFilter.testDouble(0.0));
    EXPECT_FALSE(bloomFilter.testDouble(-1.1));
    EXPECT_FALSE(bloomFilter.testDouble(-11.11));
    EXPECT_FALSE(bloomFilter.testDouble(-111.111));
    EXPECT_FALSE(bloomFilter.testDouble(-1111.1111));

    bloomFilter.addDouble(1.1);
    bloomFilter.addDouble(11.11);
    bloomFilter.addDouble(111.111);
    bloomFilter.addDouble(1111.1111);
    bloomFilter.addDouble(0.0);
    bloomFilter.addDouble(-1.1);
    bloomFilter.addDouble(-11.11);
    bloomFilter.addDouble(-111.111);
    bloomFilter.addDouble(-1111.1111);

    EXPECT_TRUE(bloomFilter.testDouble(1.1));
    EXPECT_TRUE(bloomFilter.testDouble(11.11));
    EXPECT_TRUE(bloomFilter.testDouble(111.111));
    EXPECT_TRUE(bloomFilter.testDouble(1111.1111));
    EXPECT_TRUE(bloomFilter.testDouble(0.0));
    EXPECT_TRUE(bloomFilter.testDouble(-1.1));
    EXPECT_TRUE(bloomFilter.testDouble(-11.11));
    EXPECT_TRUE(bloomFilter.testDouble(-111.111));
    EXPECT_TRUE(bloomFilter.testDouble(-1111.1111));

    // test strings
    bloomFilter.reset();
    const char* emptyStr = "";
    const char* enStr = "english";
    const char* cnStr = "中国字";

    EXPECT_FALSE(bloomFilter.testBytes(emptyStr, static_cast<int64_t>(strlen(emptyStr))));
    EXPECT_FALSE(bloomFilter.testBytes(enStr, static_cast<int64_t>(strlen(enStr))));
    EXPECT_FALSE(bloomFilter.testBytes(cnStr, static_cast<int64_t>(strlen(cnStr))));

    bloomFilter.addBytes(emptyStr, static_cast<int64_t>(strlen(emptyStr)));
    bloomFilter.addBytes(enStr, static_cast<int64_t>(strlen(enStr)));
    bloomFilter.addBytes(cnStr, static_cast<int64_t>(strlen(cnStr)));

    EXPECT_TRUE(bloomFilter.testBytes(emptyStr, static_cast<int64_t>(strlen(emptyStr))));
    EXPECT_TRUE(bloomFilter.testBytes(enStr, static_cast<int64_t>(strlen(enStr))));
    EXPECT_TRUE(bloomFilter.testBytes(cnStr, static_cast<int64_t>(strlen(cnStr))));
}

TEST(TestBloomFilter, testBloomFilterSerialization) {
    BloomFilterImpl emptyFilter1(128), emptyFilter2(256);
    EXPECT_FALSE(emptyFilter1 == emptyFilter2);

    BloomFilterImpl emptyFilter3(128, 0.05), emptyFilter4(128, 0.01);
    EXPECT_FALSE(emptyFilter3 == emptyFilter4);

    BloomFilterImpl srcBloomFilter(64);
    srcBloomFilter.addLong(1);
    srcBloomFilter.addLong(11);
    srcBloomFilter.addLong(111);
    srcBloomFilter.addLong(1111);
    srcBloomFilter.addLong(0);
    srcBloomFilter.addLong(-1);
    srcBloomFilter.addLong(-11);
    srcBloomFilter.addLong(-111);
    srcBloomFilter.addLong(-1111);

    proto::BloomFilter pbBloomFilter;
    proto::ColumnEncoding encoding;
    encoding.set_bloomencoding(1);

    // serialize
    BloomFilterUTF8Utils::serialize(srcBloomFilter, pbBloomFilter);

    // deserialize
    std::unique_ptr<BloomFilter> dstBloomFilter =
            BloomFilterUTF8Utils::deserialize(proto::Stream_Kind_BLOOM_FILTER_UTF8, encoding, pbBloomFilter);

    EXPECT_TRUE(srcBloomFilter == dynamic_cast<BloomFilterImpl&>(*dstBloomFilter));
    EXPECT_TRUE(dstBloomFilter->testLong(1));
    EXPECT_TRUE(dstBloomFilter->testLong(11));
    EXPECT_TRUE(dstBloomFilter->testLong(111));
    EXPECT_TRUE(dstBloomFilter->testLong(1111));
    EXPECT_TRUE(dstBloomFilter->testLong(0));
    EXPECT_TRUE(dstBloomFilter->testLong(-1));
    EXPECT_TRUE(dstBloomFilter->testLong(-11));
    EXPECT_TRUE(dstBloomFilter->testLong(-111));
    EXPECT_TRUE(dstBloomFilter->testLong(-1111));
}

} // namespace orc
