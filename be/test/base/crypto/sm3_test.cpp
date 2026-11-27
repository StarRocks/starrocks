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

#include "base/crypto/sm3.h"

#include <gtest/gtest.h>

#include <cstring>
#include <iomanip>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace starrocks {
namespace {

std::string to_hex(const unsigned char digest[Sm3::SM3_HASH_SIZE]) {
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (int i = 0; i < Sm3::SM3_HASH_SIZE; ++i) {
        oss << std::setw(2) << static_cast<unsigned>(digest[i]);
    }
    return oss.str();
}

} // namespace

TEST(Sm3Test, StandardVectors) {
    unsigned char digest[Sm3::SM3_HASH_SIZE]{};

    Sm3::sm3_compute(nullptr, 0, digest);
    EXPECT_EQ("1ab21d8355cfa17f8e61194831e81a8f22bec8c728fefb747ed035eb5082aa2b", to_hex(digest));

    const char* abc = "abc";
    Sm3::sm3_compute(reinterpret_cast<const unsigned char*>(abc), std::strlen(abc), digest);
    EXPECT_EQ("66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0", to_hex(digest));

    const std::string block64 = "abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd";
    Sm3::sm3_compute(reinterpret_cast<const unsigned char*>(block64.data()), block64.size(), digest);
    EXPECT_EQ("debe9ff92275b8a138604889c18e5a4d6fdb70e5387e5765293dcba39c0c5732", to_hex(digest));
}

TEST(Sm3Test, BoundaryAndMultiThread) {
    // Verify repeated calls across multiple threads reuse thread_local ctx safely
    // and match single-thread results across block/padding boundaries.
    std::vector<std::string> inputs = {
            "",
            std::string(55, 'a'),
            std::string(56, 'b'),
            std::string(63, 'c'),
            std::string(64, 'd'),
            std::string(65, 'e'),
            std::string(1024, 'f'),
    };
    std::vector<std::string> expected(inputs.size());
    for (size_t i = 0; i < inputs.size(); ++i) {
        unsigned char digest[Sm3::SM3_HASH_SIZE]{};
        Sm3::sm3_compute(reinterpret_cast<const unsigned char*>(inputs[i].data()), inputs[i].size(), digest);
        expected[i] = to_hex(digest);
    }

    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&]() {
            for (int iter = 0; iter < 100; ++iter) {
                for (size_t i = 0; i < inputs.size(); ++i) {
                    unsigned char digest[Sm3::SM3_HASH_SIZE]{};
                    Sm3::sm3_compute(reinterpret_cast<const unsigned char*>(inputs[i].data()), inputs[i].size(),
                                     digest);
                    EXPECT_EQ(expected[i], to_hex(digest));
                }
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }
}

} // namespace starrocks
