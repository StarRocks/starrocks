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

#include "column/german_string.h"

#include <gtest/gtest.h>

#include <iostream>
#include <random>
#include <string>
#include <tuple>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "testutil/parallel_test.h"

namespace starrocks {
class GermanStringExternalAllocator {
public:
    static constexpr auto PAGE_SIZE = 4096;
    static constexpr auto MEDIUM_STRING_MAX_SIZE = 512;
    virtual ~GermanStringExternalAllocator() = default;
    size_t size() const;

    void clear();

    char* allocate(size_t n);
    virtual void incorporate(std::shared_ptr<GermanStringExternalAllocator>&& other);
    bool is_from_binary() const { return false; }

private:
    std::vector<std::vector<char>> medium_string_pages;
    std::vector<std::string> large_strings;
};

size_t GermanStringExternalAllocator::size() const {
    size_t total_size = 0;
    for (const auto& page : medium_string_pages) {
        total_size += page.size();
    }
    for (const auto& str : large_strings) {
        total_size += str.size();
    }
    return total_size;
}

void GermanStringExternalAllocator::clear() {
    large_strings.clear();
    medium_string_pages.clear();
}

char* GermanStringExternalAllocator::allocate(size_t n) {
    if (n <= GermanString::INLINE_MAX_LENGTH) {
        return nullptr;
    }
    if (n <= MEDIUM_STRING_MAX_SIZE) {
        if (medium_string_pages.empty() || medium_string_pages.back().size() + n > PAGE_SIZE) {
            medium_string_pages.emplace_back();
            medium_string_pages.back().reserve(PAGE_SIZE);
        }
        auto& page = medium_string_pages.back();
        raw::stl_vector_resize_uninitialized(&page, page.size() + n);
        return page.data() + page.size() - n;
    } else {
        large_strings.emplace_back();
        auto& str = large_strings.back();
        raw::make_room(&str, n);
        return large_strings.back().data();
    }
}

void GermanStringExternalAllocator::incorporate(std::shared_ptr<GermanStringExternalAllocator>&& other) {
    DCHECK(!other->is_from_binary());
    large_strings.insert(large_strings.end(), std::make_move_iterator(other->large_strings.begin()),
                         std::make_move_iterator(other->large_strings.end()));
    medium_string_pages.insert(medium_string_pages.end(), std::make_move_iterator(other->medium_string_pages.begin()),
                               std::make_move_iterator(other->medium_string_pages.end()));
    other->clear();
}

static inline std::string gen_string(std::size_t length) {
    const std::string chars =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz"
            "0123456789";

    std::random_device rd;  // For seeding
    std::mt19937 gen(rd()); // Mersenne Twister RNG
    std::uniform_int_distribution<> dist(0, chars.size() - 1);

    std::string result;
    result.reserve(length);
    for (std::size_t i = 0; i < length; ++i) {
        result += chars[dist(gen)];
    }

    return result;
}

TEST(GermanStringTest, test_german_string) {
    GermanStringExternalAllocator allocator;
    std::vector<std::string> strings{gen_string(0),  gen_string(1),    gen_string(3),   gen_string(4),
                                     gen_string(5),  gen_string(11),   gen_string(12),  gen_string(13),
                                     gen_string(15), gen_string(4096), gen_string(4097)};
    for (const auto& s : strings) {
        GermanString gs1(s.data(), s.size(), allocator.allocate(s.size()));
        auto s1 = static_cast<std::string>(gs1);
        ASSERT_EQ(s, s1);
        GermanString gs2(gs1, allocator.allocate(gs1.len));
        auto s2 = static_cast<std::string>(gs2);
        ASSERT_EQ(s, s2);
    }
}

TEST(GermanStringTest, test_german_string_compare) {
    GermanStringExternalAllocator allocator;
    // short vs short
    {
        GermanString gs1("Hello", 5, allocator.allocate(5));
        GermanString gs2("Hello", 5, allocator.allocate(5));
        GermanString gs3("Hello!", 6, allocator.allocate(6));
        GermanString gs4("Helloi", 6, allocator.allocate(6));
        ASSERT_TRUE(gs1 == gs2);
        ASSERT_FALSE(gs1 == gs3);
        ASSERT_FALSE(gs1 == gs4);
        ASSERT_FALSE(gs3 == gs4);
        ASSERT_FALSE(gs3 == gs1);
        ASSERT_FALSE(gs4 == gs1);
        ASSERT_FALSE(gs4 == gs3);

        ASSERT_TRUE(gs1.compare(gs2) == 0);
        ASSERT_TRUE(gs1 < gs3);
        ASSERT_TRUE(gs3 < gs4);
        ASSERT_TRUE(gs4 > gs1);
    }
    // long vs long
    {
        GermanString gs1("Hello GermanString", 18, allocator.allocate(18));
        GermanString gs2("Hello GermanString", 18, allocator.allocate(18));
        GermanString gs3("Hello GermanString!", 19, allocator.allocate(19));
        GermanString gs4("Hello GermanStringi", 19, allocator.allocate(19));
        ASSERT_TRUE(gs1 == gs2);
        ASSERT_FALSE(gs1 == gs3);
        ASSERT_FALSE(gs1 == gs4);
        ASSERT_FALSE(gs3 == gs4);
        ASSERT_FALSE(gs3 == gs1);
        ASSERT_FALSE(gs4 == gs1);
        ASSERT_FALSE(gs4 == gs3);

        ASSERT_TRUE(gs1.compare(gs2) == 0);
        ASSERT_TRUE(gs1 < gs3);
        ASSERT_TRUE(gs3 < gs4);
        ASSERT_TRUE(gs4 > gs1);
    }
    // short vs long
    {
        GermanString gs1("Hello", 5, allocator.allocate(5));
        GermanString gs2("Hello GermanString", 18, allocator.allocate(18));
        GermanString gs3("Hello GermanString!", 19, allocator.allocate(19));
        GermanString gs4("Hello GermanStringi", 19, allocator.allocate(19));
        ASSERT_TRUE(gs1 < gs2);
        ASSERT_TRUE(gs1 < gs3);
        ASSERT_TRUE(gs1 < gs4);
        ASSERT_FALSE(gs2 < gs1);
        ASSERT_FALSE(gs3 < gs1);
        ASSERT_FALSE(gs4 < gs1);

        ASSERT_TRUE(gs2 > gs1);
        ASSERT_TRUE(gs3 > gs1);
        ASSERT_TRUE(gs4 > gs1);
    }
    {
        GermanString gs0("", 0, allocator.allocate(0));
        GermanString gs4("abcd", 4, allocator.allocate(4));
        GermanString gs12("abcdabcdabcd", 12, allocator.allocate(12));
        GermanString gs16("abcdabcdabcdefgh", 16, allocator.allocate(16));
        ASSERT_TRUE(gs0 < gs4);
        ASSERT_TRUE(gs0 < gs12);
        ASSERT_TRUE(gs0 < gs16);
        ASSERT_TRUE(gs4 < gs12);
        ASSERT_TRUE(gs4 < gs16);
        ASSERT_TRUE(gs12 < gs16);

        ASSERT_TRUE(gs0 == gs0);
        ASSERT_TRUE(gs4 == gs4);
        ASSERT_TRUE(gs12 == gs12);
        ASSERT_TRUE(gs16 == gs16);

        ASSERT_TRUE(gs0 != gs4);
        ASSERT_TRUE(gs0 != gs12);
        ASSERT_TRUE(gs0 != gs16);
        ASSERT_TRUE(gs4 != gs12);
        ASSERT_TRUE(gs4 != gs16);
        ASSERT_TRUE(gs12 != gs16);
    }
}
} // namespace starrocks
