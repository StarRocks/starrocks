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

#include "exprs/ai/ai_aggregate_state.h"

#include <gtest/gtest.h>

#include <limits>
#include <string>
#include <vector>

#include "base/coding.h"

namespace starrocks {
namespace {
std::string encoded_state(const std::vector<std::string>& records) {
    std::string result(kAiAggregateStateHeaderBytes, '\0');
    for (const auto& record : records) {
        put_fixed64_le(&result, record.size());
        result.append(record);
    }
    encode_ai_aggregate_state_header(reinterpret_cast<uint8_t*>(result.data()), records.size(),
                                     result.size() - kAiAggregateStateHeaderBytes);
    return result;
}
} // namespace

TEST(AiAggregateStateTest, PreservesRecordBoundariesAndBytes) {
    const std::vector<std::string> records = {"", "duplicate", "duplicate", std::string("a\0b\nc", 5)};
    auto bytes = encoded_state(records);
    auto parsed = parse_ai_aggregate_state(Slice(bytes));
    ASSERT_TRUE(parsed.ok()) << parsed.status();
    EXPECT_EQ(records.size(), parsed->count());
    EXPECT_EQ(bytes.data() + kAiAggregateStateHeaderBytes, parsed->payload().data);
    auto cursor = parsed->cursor();
    auto copy = cursor;
    Slice record;
    for (const auto& expected : records) {
        ASSERT_TRUE(cursor.next(&record));
        EXPECT_EQ(expected, record.to_string());
    }
    EXPECT_FALSE(cursor.next(&record));
    ASSERT_TRUE(copy.next(&record));
    EXPECT_TRUE(record.empty());
}

TEST(AiAggregateStateTest, DefaultCursorIsExhausted) {
    AiAggregateStateCursor cursor;
    Slice record;
    EXPECT_FALSE(cursor.next(&record));
}

TEST(AiAggregateStateTest, EmptyStateIsValidAndBounded) {
    auto bytes = encoded_state({});
    ASSERT_EQ(kAiAggregateStateHeaderBytes, bytes.size());
    auto parsed = parse_ai_aggregate_state(Slice(bytes));
    ASSERT_TRUE(parsed.ok());
    EXPECT_EQ(0, parsed->count());
    Slice record;
    EXPECT_FALSE(parsed->cursor().next(&record));
    EXPECT_TRUE(parse_ai_aggregate_state(Slice(bytes), bytes.size()).ok());
    EXPECT_FALSE(parse_ai_aggregate_state(Slice(bytes), bytes.size() - 1).ok());
}

TEST(AiAggregateStateTest, RejectsEveryTruncationAndTrailingBytes) {
    auto bytes = encoded_state({"a", "", "bc"});
    for (size_t size = 0; size < bytes.size(); ++size) {
        EXPECT_FALSE(parse_ai_aggregate_state(Slice(bytes.data(), size)).ok()) << size;
    }
    bytes.push_back('x');
    EXPECT_FALSE(parse_ai_aggregate_state(Slice(bytes)).ok());
}

TEST(AiAggregateStateTest, RejectsUnknownMagicVersionAndFlags) {
    const auto valid = encoded_state({"a"});
    for (size_t field : {0, 4, 6}) {
        auto bytes = valid;
        bytes[field] ^= 0x40;
        EXPECT_FALSE(parse_ai_aggregate_state(Slice(bytes)).ok()) << field;
    }
}

TEST(AiAggregateStateTest, RejectsInconsistentAndOverflowingLengths) {
    const auto valid = encoded_state({"a", "b"});
    for (size_t field : {8, 16, 24}) {
        auto bytes = valid;
        encode_fixed64_le(reinterpret_cast<uint8_t*>(bytes.data() + field), std::numeric_limits<uint64_t>::max());
        EXPECT_FALSE(parse_ai_aggregate_state(Slice(bytes)).ok()) << field;
    }
    for (uint64_t count : {0, 1, 3}) {
        auto bytes = valid;
        encode_fixed64_le(reinterpret_cast<uint8_t*>(bytes.data() + 8), count);
        EXPECT_FALSE(parse_ai_aggregate_state(Slice(bytes)).ok()) << count;
    }
    auto oversized = encoded_state({std::string(kAiAggregateStateMaxBytes, 'a')});
    EXPECT_FALSE(parse_ai_aggregate_state(Slice(oversized)).ok());
}

TEST(AiAggregateStateTest, HeaderUsesLittleEndianWithoutAlignmentRequirements) {
    std::string storage(kAiAggregateStateHeaderBytes + 1, '\0');
    auto* header = reinterpret_cast<uint8_t*>(storage.data() + 1);
    encode_ai_aggregate_state_header(header, 0x0102030405060708ULL, 0x1112131415161718ULL);
    EXPECT_EQ(0x08, header[8]);
    EXPECT_EQ(0x01, header[15]);
    EXPECT_EQ(0x18, header[16]);
    EXPECT_EQ(0x11, header[23]);
}
} // namespace starrocks
