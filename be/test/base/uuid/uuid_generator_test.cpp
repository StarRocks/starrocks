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

#include "base/uuid/uuid_generator.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <boost/uuid/uuid_io.hpp>
#include <chrono>
#include <cstring>
#include <thread>
#include <unordered_set>
#include <vector>

namespace starrocks {
class ThreadLocalUUIDGeneratorTest : public testing::Test {
public:
    ThreadLocalUUIDGeneratorTest() = default;
    ~ThreadLocalUUIDGeneratorTest() override = default;
};

// Test that UUIDs are unique
TEST_F(ThreadLocalUUIDGeneratorTest, TestUniqueness) {
    const int NUM_UUIDS = 10000;
    std::unordered_set<std::string> uuids;

    for (int i = 0; i < NUM_UUIDS; i++) {
        boost::uuids::uuid uuid = ThreadLocalUUIDGenerator::next_uuid();
        std::string uuid_string = boost::uuids::to_string(uuid);
        uuids.insert(uuid_string);
    }

    ASSERT_EQ(NUM_UUIDS, uuids.size()) << "Duplicate UUID generated";
}

// Test parallel generation of UUIDs to ensure they're still unique
TEST_F(ThreadLocalUUIDGeneratorTest, TestParallelGeneration) {
    const int NUM_THREADS = 8;
    const int UUIDS_PER_THREAD = 1000;
    std::vector<std::thread> threads;
    std::vector<std::vector<std::string>> thread_uuids(NUM_THREADS);

    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([t, &thread_uuids]() {
            for (int i = 0; i < UUIDS_PER_THREAD; i++) {
                boost::uuids::uuid uuid = ThreadLocalUUIDGenerator::next_uuid();
                std::string uuid_string = boost::uuids::to_string(uuid);
                thread_uuids[t].push_back(uuid_string);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Combine all UUIDs and check uniqueness
    std::unordered_set<std::string> all_uuids;
    for (const auto& thread_uuid_list : thread_uuids) {
        for (const auto& uuid_string : thread_uuid_list) {
            all_uuids.insert(uuid_string);
        }
    }

    ASSERT_EQ(UUIDS_PER_THREAD * NUM_THREADS, all_uuids.size()) << "Duplicate UUID was generated in parallel";
}

// Test that UUID version is 7
TEST_F(ThreadLocalUUIDGeneratorTest, TestVersion) {
    // Generate a UUID
    boost::uuids::uuid uuid = ThreadLocalUUIDGenerator::next_uuid();

    // Expect UUID version to be 4
    ASSERT_EQ(uuid.version(), 4);

    // Expect UUID variant to be RFC 4122
    ASSERT_LE(uuid.variant(), 2);
}

// Test UUID v7 generation
TEST_F(ThreadLocalUUIDGeneratorTest, TestUUIDv7Generation) {
    // Generate multiple UUID v7s
    const int NUM_UUIDS = 100;
    std::vector<boost::uuids::uuid> uuids;

    for (int i = 0; i < NUM_UUIDS; i++) {
        uuids.push_back(ThreadLocalUUIDGenerator::next_uuid_v7());
    }

    // Verify all UUIDs are unique
    std::unordered_set<std::string> uuid_strings;
    for (const auto& uuid : uuids) {
        std::string uuid_string = boost::uuids::to_string(uuid);
        uuid_strings.insert(uuid_string);
    }
    ASSERT_EQ(NUM_UUIDS, uuid_strings.size()) << "Duplicate UUID v7 generated";

    // Verify all UUIDs have version 7
    // Extract version from byte 6, high nibble (bits 4-7)
    for (const auto& uuid : uuids) {
        uint8_t version = (uuid.data[6] >> 4) & 0x0F;
        ASSERT_EQ(7, version) << "UUID version should be 7";
    }

    // Verify all UUIDs have RFC 4122 variant
    // Extract variant from byte 8, high 2 bits
    for (const auto& uuid : uuids) {
        uint8_t variant = (uuid.data[8] >> 6) & 0x03;
        ASSERT_EQ(2, variant) << "UUID variant should be RFC 4122 (10 binary = 2)";
    }
}

// Test UUID v7 time ordering
TEST_F(ThreadLocalUUIDGeneratorTest, TestUUIDv7TimeOrdering) {
    // Generate UUIDs with small delays to ensure different timestamps
    std::vector<boost::uuids::uuid> uuids;

    for (int i = 0; i < 10; i++) {
        uuids.push_back(ThreadLocalUUIDGenerator::next_uuid_v7());
        // Small delay to ensure different timestamp
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }

    // Verify UUIDs are in ascending order (time-ordered)
    for (size_t i = 1; i < uuids.size(); i++) {
        std::string prev_str = boost::uuids::to_string(uuids[i - 1]);
        std::string curr_str = boost::uuids::to_string(uuids[i]);

        // Compare the timestamp portion (first 48 bits / 12 hex chars)
        std::string prev_ts = prev_str.substr(0, 13); // First 8 chars + dash + 4 chars
        std::string curr_ts = curr_str.substr(0, 13);

        // Later UUIDs should have equal or greater timestamps
        ASSERT_LE(prev_ts, curr_ts) << "UUID v7 should be time-ordered";
    }
}

// Test UUID v7 parallel generation
TEST_F(ThreadLocalUUIDGeneratorTest, TestUUIDv7ParallelGeneration) {
    const int NUM_THREADS = 8;
    const int UUIDS_PER_THREAD = 1000;
    std::vector<std::thread> threads;
    std::vector<std::vector<std::string>> thread_uuids(NUM_THREADS);

    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([t, &thread_uuids]() {
            for (int i = 0; i < UUIDS_PER_THREAD; i++) {
                boost::uuids::uuid uuid = ThreadLocalUUIDGenerator::next_uuid_v7();
                std::string uuid_string = boost::uuids::to_string(uuid);
                thread_uuids[t].push_back(uuid_string);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Combine all UUIDs and check uniqueness
    std::unordered_set<std::string> all_uuids;
    for (const auto& thread_uuid_list : thread_uuids) {
        for (const auto& uuid_string : thread_uuid_list) {
            all_uuids.insert(uuid_string);
        }
    }

    ASSERT_EQ(UUIDS_PER_THREAD * NUM_THREADS, all_uuids.size()) << "Duplicate UUID v7 was generated in parallel";
}

// Test UUID v7 format compliance
TEST_F(ThreadLocalUUIDGeneratorTest, TestUUIDv7FormatCompliance) {
    boost::uuids::uuid uuid = ThreadLocalUUIDGenerator::next_uuid_v7();

    // Check version bits (byte 6, high nibble should be 0x7)
    ASSERT_EQ(0x70, uuid.data[6] & 0xF0) << "Version bits should be 0x70 for UUID v7";

    // Check variant bits (byte 8, high 2 bits should be 0b10)
    ASSERT_EQ(0x80, uuid.data[8] & 0xC0) << "Variant bits should be 0x80 for RFC 4122";

    // Verify string format (8-4-4-4-12)
    std::string uuid_str = boost::uuids::to_string(uuid);
    ASSERT_EQ(36, uuid_str.length());
    ASSERT_EQ('-', uuid_str[8]);
    ASSERT_EQ('-', uuid_str[13]);
    ASSERT_EQ('-', uuid_str[18]);
    ASSERT_EQ('-', uuid_str[23]);
}

} // namespace starrocks
