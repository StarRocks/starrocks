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

#include "util/uuid_generator.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <boost/uuid/uuid_io.hpp>
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
        threads.emplace_back([t, &thread_uuids, this]() {
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

} // namespace starrocks
