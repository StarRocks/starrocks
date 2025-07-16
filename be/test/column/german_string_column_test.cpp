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

#include "column/german_string_column.cpp"

#include <gtest/gtest.h>

#include <iostream>
#include <random>
#include <string>
#include <tuple>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/german_string.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "testutil/parallel_test.h"

namespace starrocks {
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

TEST(GermanStringColumnTest, test_german_string) {
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

TEST(GermanStringColumnTest, test_german_string_compare) {
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
}

TEST(GermanStringColumnTest, test_german_string_column) {
    auto gs_column = GermanStringColumn::create();
    auto& gs_col = *down_cast<GermanStringColumn*>(gs_column.get());
    auto bin_column = BinaryColumn::create();
    auto& bin_col = *down_cast<BinaryColumn*>(bin_column.get());

    ASSERT_TRUE(gs_col.empty());
    std::vector<std::string> strings{gen_string(0),  gen_string(1),    gen_string(3),   gen_string(4),
                                     gen_string(5),  gen_string(11),   gen_string(12),  gen_string(13),
                                     gen_string(15), gen_string(4096), gen_string(4097)};
    for (const auto& s : strings) {
        gs_col.append_string(s);
        bin_col.append_string(s);
    }
    ASSERT_EQ(gs_col.debug_string(), bin_col.debug_string());

    GermanStringColumn gs_col2 = gs_col;
    ASSERT_EQ(gs_col2.debug_string(), bin_col.debug_string());

    GermanStringColumn gs_col3 = std::move(gs_col);
    ASSERT_EQ(gs_col3.debug_string(), bin_col.debug_string());

    GermanStringColumn gs_col4(0);
    gs_col4 = gs_col2;
    ASSERT_EQ(gs_col4.debug_string(), bin_col.debug_string());

    GermanStringColumn gs_col5(0);
    gs_col5 = std::move(gs_col3);
    ASSERT_EQ(gs_col5.debug_string(), bin_col.debug_string());
    ASSERT_EQ(gs_col5.xor_checksum(0, gs_col5.size()), bin_col.xor_checksum(0, bin_col.size()));
}

inline static std::tuple<GermanStringColumn, BinaryColumn> gen_columns(const std::vector<size_t> lengths) {
    GermanStringColumn gs_col;
    BinaryColumn bin_col;
    std::vector<std::string> strings;
    for (auto length : lengths) {
        strings.push_back(gen_string(length));
    }
    gs_col.reserve(strings.size());
    bin_col.reserve(strings.size());

    for (const auto& s : strings) {
        gs_col.append_string(s);
        bin_col.append_string(s);
    }
    return std::make_tuple<GermanStringColumn, BinaryColumn>(std::move(gs_col), std::move(bin_col));
}

TEST(GermanStringColumnTest, test_german_string_column_append) {
    auto [gs_col, bin_col] = gen_columns({1, 2, 12, 13});
    auto [gs_col1, bin_col1] = gen_columns({1, 2, 12, 13});

    // append_default
    gs_col.append_default();
    bin_col.append_default();
    ASSERT_TRUE(gs_col.debug_string() == bin_col.debug_string());

    // append_strings
    auto slices = bin_col1.get_data();
    gs_col.append_strings(slices.data(), slices.size());
    bin_col.append_strings(slices.data(), slices.size());
    ASSERT_TRUE(gs_col.debug_string() == bin_col.debug_string());

    // cut, filter_range
    auto gs_cut_result = gs_col.cut(3, 4);
    auto bin_cut_result = bin_col.cut(3, 4);
    ASSERT_TRUE(gs_cut_result->debug_string() == bin_cut_result->debug_string());
    Filter filter{1, 0, 1, 0};
    auto gs_sz = gs_cut_result->filter_range(filter, 0, filter.size());
    auto bin_sz = bin_cut_result->filter_range(filter, 0, filter.size());
    ASSERT_TRUE(gs_sz == bin_sz);
    ASSERT_TRUE(gs_cut_result->debug_string() == bin_cut_result->debug_string());

    // append another column
    gs_col.append(gs_col1, 0, 2);
    bin_col.append(bin_col1, 0, 2);
    ASSERT_TRUE(gs_col.debug_string() == bin_col.debug_string());

    // append selective
    std::vector<uint32_t> indexes{0, 1, 3};
    gs_col.append_selective(gs_col1, indexes.data(), 0, indexes.size());
    bin_col.append_selective(bin_col1, indexes.data(), 0, indexes.size());
    ASSERT_TRUE(gs_col.debug_string() == bin_col.debug_string());

    // append value multiple times
    gs_col.append_value_multiple_times(gs_col1, 1, 3);
    bin_col.append_value_multiple_times(bin_col1, 1, 3);
    ASSERT_TRUE(gs_col.debug_string() == bin_col.debug_string());

    GermanStringExternalAllocator allocator;
    std::string s = "hello german string";
    GermanString gs(s.data(), s.size(), allocator.allocate(s.size()));
    Slice slice(s.data(), s.size());

    gs_col.append_value_multiple_times(&gs, 3);
    bin_col.append_value_multiple_times(&slice, 3);
    ASSERT_TRUE(gs_col.debug_string() == bin_col.debug_string());

    // remove first n values
    gs_col.remove_first_n_values(2);
    bin_col.remove_first_n_values(2);
    ASSERT_TRUE(gs_col.debug_string() == bin_col.debug_string());

    // update_rows
    auto gs_col2 = gs_col1.clone();
    auto bin_col2 = bin_col1.clone();
    gs_col2->remove_first_n_values(1);
    bin_col2->remove_first_n_values(1);
    std::vector<uint32_t> indexes2(gs_col.size());
    std::iota(indexes2.begin(), indexes2.end(), 0);
    std::shuffle(indexes2.begin(), indexes2.end(), std::mt19937(std::random_device()()));
    indexes2.resize(gs_col2->size());
    std::sort(indexes2.begin(), indexes2.end());
    gs_col.update_rows(*gs_col2.get(), indexes2.data());
    bin_col.update_rows(*bin_col2.get(), indexes2.data());
    ASSERT_TRUE(gs_col.debug_string() == bin_col.debug_string());
}

TEST(GermanStringColumnTest, test_german_string_column_replicate) {
    auto [gs_col, bin_col] = gen_columns({1, 2, 12, 13});
    Buffer<uint32_t> offsets{0, 1, 3, 15, 27};
    auto maybe_gs_replicated = gs_col.replicate(offsets);
    auto maybe_bin_replicated = bin_col.replicate(offsets);
    ASSERT_TRUE(maybe_gs_replicated.ok());
    ASSERT_TRUE(maybe_bin_replicated.ok());
    ASSERT_TRUE(maybe_gs_replicated.value()->debug_string() == maybe_bin_replicated.value()->debug_string());
}

TEST(GermanStringColumnTest, test_serde) {
    auto [gs_col, bin_col] = gen_columns({1, 2, 12, 13});
    auto gs_max_serialize_sz = gs_col.max_one_element_serialize_size();
    auto bin_max_serialize_sz = bin_col.max_one_element_serialize_size();
    ASSERT_TRUE(gs_max_serialize_sz == bin_max_serialize_sz);

    auto gs_slice_sizes_col = FixedLengthColumn<uint32_t>::create(gs_col.size());
    auto bin_slice_sizes_col = FixedLengthColumn<uint32_t>::create(bin_col.size());
    std::vector<uint8_t> gs_buff(gs_max_serialize_sz * gs_col.size(), 0);
    std::vector<uint8_t> bin_buff(bin_max_serialize_sz * bin_col.size(), 0);
    Buffer<uint32_t>& gs_slice_sizes = gs_slice_sizes_col->get_data();
    Buffer<uint32_t>& bin_slice_sizes = bin_slice_sizes_col->get_data();

    gs_col.serialize_batch(gs_buff.data(), gs_slice_sizes, gs_col.size(), gs_max_serialize_sz);
    bin_col.serialize_batch(bin_buff.data(), bin_slice_sizes, bin_col.size(), bin_max_serialize_sz);
    ASSERT_TRUE(gs_buff == bin_buff);
    ASSERT_TRUE(gs_slice_sizes_col->debug_string() == bin_slice_sizes_col->debug_string());

    auto gs_col2 = GermanStringColumn::create();
    auto bin_col2 = BinaryColumn::create();
    Buffer<Slice> gs_buff_slices;
    Buffer<Slice> bin_buff_slices;
    for (auto i = 0; i < gs_col.size(); ++i) {
        gs_buff_slices.emplace_back(gs_buff.data() + i * gs_max_serialize_sz, gs_slice_sizes[i]);
        bin_buff_slices.emplace_back(bin_buff.data() + i * bin_max_serialize_sz, bin_slice_sizes[i]);
    }
    gs_col2->deserialize_and_append_batch(gs_buff_slices, gs_buff_slices.size());
    bin_col2->deserialize_and_append_batch(bin_buff_slices, bin_buff_slices.size());
    ASSERT_TRUE(gs_col2->debug_string() == bin_col2->debug_string());
    ASSERT_TRUE(gs_col.debug_string() == gs_col2->debug_string());
}

} // namespace starrocks