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

#include "column/binary_column.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "testutil/parallel_test.h"

namespace starrocks {

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_create) {
    BinaryColumn::Ptr column = BinaryColumn::create();
    ASSERT_TRUE(column->is_binary());
    ASSERT_FALSE(column->is_nullable());
    ASSERT_EQ(0u, column->size());
}

// NOLINTNEXTLINE
GROUP_SLOW_PARALLEL_TEST(BinaryColumnTest, test_binary_column_upgrade_if_overflow) {
    // small column
    BinaryColumn::Ptr column = BinaryColumn::create();
    for (size_t i = 0; i < 10; i++) {
        column->append(std::to_string(i));
    }
    auto ret = column->upgrade_if_overflow();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);

    // offset overflow
    column = BinaryColumn::create();
    size_t count = 1 << 30;
    for (size_t i = 0; i < count; i++) {
        column->append(std::to_string(i));
    }
    ret = column->upgrade_if_overflow();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value()->is_large_binary());
    ASSERT_EQ(ret.value()->size(), count);

    for (size_t i = 0; i < count; i++) {
        ASSERT_EQ(ret.value()->get(i).get_slice().to_string(), std::to_string(i));
    }

    // row size overflow
    // the case will allocate a lot of memory, so temp remove it
    count = Column::MAX_CAPACITY_LIMIT + 5;
    column = BinaryColumn::create();
    column->reserve(count);
    for (size_t i = 0; i < count; i++) {
        column->append("a");
    }
    ret = column->upgrade_if_overflow();
    ASSERT_TRUE(!ret.ok());
}

// NOLINTNEXTLINE
GROUP_SLOW_PARALLEL_TEST(BinaryColumnTest, test_binary_column_downgrade) {
    BinaryColumn::Ptr column = BinaryColumn::create();
    column->append_string("test");
    ASSERT_FALSE(column->has_large_column());
    auto ret = column->downgrade();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);

    LargeBinaryColumn::Ptr large_column = LargeBinaryColumn::create();
    ASSERT_TRUE(large_column->has_large_column());
    for (size_t i = 0; i < 10; i++) {
        large_column->append_string(std::to_string(i));
    }
    ret = large_column->downgrade();
    ASSERT_TRUE(ret.ok());
    ASSERT_FALSE(ret.value()->has_large_column());
    ASSERT_EQ(ret.value()->size(), 10);
    for (size_t i = 0; i < 10; i++) {
        ASSERT_EQ(ret.value()->get(i).get_slice(), Slice(std::to_string(i)));
    }

    large_column = LargeBinaryColumn::create();
    size_t count = 1 << 29;
    for (size_t i = 0; i < count; i++) {
        large_column->append("0123456789");
    }
    ret = large_column->downgrade();
    ASSERT_FALSE(ret.ok());
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_get_data) {
    BinaryColumn::Ptr column = BinaryColumn::create();
    for (int i = 0; i < 100; i++) {
        column->append(std::string("str:").append(std::to_string(i)));
    }
    auto& slices = column->get_data();
    for (int i = 0; i < slices.size(); ++i) {
        ASSERT_EQ(std::string("str:").append(std::to_string(i)), slices[i].to_string());
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_byte_size) {
    BinaryColumn::Ptr column = BinaryColumn::create();
    ASSERT_EQ(sizeof(BinaryColumn::Offset), column->byte_size());
    std::string s("test_string");
    for (int i = 0; i < 10; i++) {
        column->append(s);
    }
    ASSERT_EQ(10, column->size());
    ASSERT_EQ(10 * s.size() + 11 * sizeof(BinaryColumn::Offset), column->byte_size());
    //                            ^^^ one more element in offset array.
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_filter) {
    BinaryColumn::Ptr column = BinaryColumn::create();
    for (int i = 0; i < 100; ++i) {
        column->append(std::to_string(i));
    }

    Filter filter;
    for (int k = 0; k < 100; ++k) {
        filter.push_back(k % 2);
    }

    column->filter(filter);
    ASSERT_EQ(50, column->size());

    const auto& slices = column->get_data();

    for (int i = 0; i < 50; ++i) {
        ASSERT_EQ(std::to_string(i * 2 + 1), slices[i].to_string());
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_append_strings) {
    std::vector<Slice> values{{"hello"}, {"starrocks"}};
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    ASSERT_TRUE(c1->append_strings(values.data(), values.size()));
    ASSERT_EQ(values.size(), c1->size());
    for (size_t i = 0; i < values.size(); i++) {
        ASSERT_EQ(values[i], c1->get_data()[i]);
    }

    std::vector<Slice> values2{{"abcd"}, {"123456"}};
    ASSERT_TRUE(c1->append_strings(values2.data(), values2.size()));
    ASSERT_EQ(values.size() + values2.size(), c1->size());
    for (size_t i = 0; i < values.size(); i++) {
        ASSERT_EQ(values[i], c1->get_data()[i]);
    }
    for (size_t i = 0; i < values2.size(); i++) {
        ASSERT_EQ(values2[i], c1->get_data()[i + 2]);
    }

    // Nullable BinaryColumn
    NullableColumn::Ptr c2 = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    ASSERT_TRUE(c2->append_strings(values.data(), values.size()));
    ASSERT_EQ(values.size(), c2->size());
    auto* c = reinterpret_cast<BinaryColumn*>(c2->mutable_data_column());
    for (size_t i = 0; i < values.size(); i++) {
        ASSERT_EQ(values[i], c->get_data()[i]);
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_append_numbers) {
    std::vector<int32_t> values{1, 2, 3, 4, 5};
    void* buff = values.data();
    size_t length = values.size() * sizeof(values[0]);

    BinaryColumn::Ptr c4 = BinaryColumn::create();
    ASSERT_EQ(-1, c4->append_numbers(buff, length));
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_append_nulls) {
    // BinaryColumn
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    ASSERT_FALSE(c1->append_nulls(10));

    // NullableColumn
    NullableColumn::Ptr c2 = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    ASSERT_TRUE(c2->append_nulls(10));
    ASSERT_EQ(10U, c2->size());
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(c2->is_null(i));
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_append_defaults) {
    // BinaryColumn
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    c1->append_default(10);
    ASSERT_EQ(10U, c1->size());
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ("", c1->get_data()[i]);
    }

    // NullableColumn
    NullableColumn::Ptr c2 = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    c2->append_default(10);
    ASSERT_EQ(10U, c2->size());
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(c2->is_null(i));
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_compare_at) {
    // Binary columns
    std::vector<Slice> strings{{"bbb"}, {"bbc"}, {"ccc"}};
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    BinaryColumn::Ptr c2 = BinaryColumn::create();
    c1->append_strings(strings.data(), strings.size());
    c2->append_strings(strings.data(), strings.size());
    for (size_t i = 0; i < strings.size(); i++) {
        ASSERT_EQ(0, c1->compare_at(i, i, *c2, -1));
        ASSERT_EQ(0, c2->compare_at(i, i, *c1, -1));
    }
    for (size_t i = 0; i < strings.size(); i++) {
        for (size_t j = i + 1; j < strings.size(); j++) {
            ASSERT_LT(c1->compare_at(i, j, *c2, -1), 0);
            ASSERT_GT(c2->compare_at(j, i, *c1, -1), 0);
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_append_binary) {
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    BinaryColumn::Ptr c2 = BinaryColumn::create();
    c1->append(Slice("first"));

    c2->append(Slice("second"));
    c2->append(Slice("third"));

    c1->append(*c2, 0, 0);
    EXPECT_EQ(1u, c1->size());
    EXPECT_EQ("first", c1->get_slice(0));

    c1->append(*c2, 0, 2);
    EXPECT_EQ(3u, c1->size());
    EXPECT_EQ("first", c1->get_slice(0));
    EXPECT_EQ("second", c1->get_slice(1));
    EXPECT_EQ("third", c1->get_slice(2));

    c1->append(*c2, 1, 1);
    EXPECT_EQ(4u, c1->size());
    EXPECT_EQ("first", c1->get_slice(0));
    EXPECT_EQ("second", c1->get_slice(1));
    EXPECT_EQ("third", c1->get_slice(2));
    EXPECT_EQ("third", c1->get_slice(3));
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_filter_range) {
    BinaryColumn::Ptr column = BinaryColumn::create();

    column->append(Slice("m"));

    for (size_t i = 0; i < 63; i++) {
        column->append(Slice("a"));
    }
    column->append(Slice("bbbbbfffff"));
    column->append(Slice("c"));

    Buffer<uint8_t> filter;
    filter.emplace_back(0);
    for (size_t i = 0; i < 63; i++) {
        filter.emplace_back(1);
    }
    filter.emplace_back(1);
    filter.emplace_back(1);

    column->filter_range(filter, 0, 66);

    auto* binary_column = ColumnHelper::as_raw_column<BinaryColumn>(column.get());
    auto& data = binary_column->get_data();
    for (size_t i = 0; i < 63; i++) {
        ASSERT_EQ(data[i], "a");
    }
    ASSERT_EQ(data[63], "bbbbbfffff");
    ASSERT_EQ(data[64], "c");
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_resize) {
    BinaryColumn::Ptr c = BinaryColumn::create();
    c->append(Slice("abc"));
    c->append(Slice("def"));
    c->append(Slice("xyz"));
    c->resize(1);
    ASSERT_EQ(1u, c->size());
    ASSERT_EQ("abc", c->get_slice(0));
    c->append(Slice("xxxxxx"));
    ASSERT_EQ(2u, c->size());
    ASSERT_EQ("abc", c->get_slice(0));
    ASSERT_EQ("xxxxxx", c->get_slice(1));
    c->resize(4);
    ASSERT_EQ(4u, c->size());
    ASSERT_EQ("abc", c->get_slice(0));
    ASSERT_EQ("xxxxxx", c->get_slice(1));
    ASSERT_EQ("", c->get_slice(2));
    ASSERT_EQ("", c->get_slice(3));
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_assign) {
    std::vector<Slice> strings{{"bbb"}, {"bbc"}, {"ccc"}};
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    BinaryColumn::Ptr c2 = BinaryColumn::create();
    c1->append_strings(strings.data(), strings.size());
    c2->append_strings(strings.data(), strings.size());

    c1->assign(c1->size(), 0);
    for (size_t i = 0; i < strings.size(); i++) {
        ASSERT_EQ(c1->get_slice(i), strings[0]);
    }

    c2->assign(c2->size(), 2);
    for (size_t i = 0; i < strings.size(); i++) {
        ASSERT_EQ(c2->get_slice(i), strings[2]);
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_reset_column) {
    std::vector<Slice> strings{{"bbb"}, {"bbc"}, {"ccc"}};
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    c1->append_strings(strings.data(), strings.size());
    c1->set_delete_state(DEL_PARTIAL_SATISFIED);

    c1->reset_column();
    ASSERT_EQ(0, c1->size());
    ASSERT_EQ(0, c1->get_data().size());
    ASSERT_EQ(DEL_NOT_SATISFIED, c1->delete_state());
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_swap_column) {
    std::vector<Slice> strings{{"bbb"}, {"bbc"}, {"ccc"}};
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    c1->append_strings(strings.data(), strings.size());
    c1->set_delete_state(DEL_PARTIAL_SATISFIED);

    BinaryColumn::Ptr c2 = BinaryColumn::create();

    c1->swap_column(*c2);

    ASSERT_EQ(0, c1->size());
    ASSERT_EQ(0, c1->get_data().size());
    ASSERT_EQ(DEL_NOT_SATISFIED, c1->delete_state());

    ASSERT_EQ(3, c2->size());
    ASSERT_EQ(3, c2->get_data().size());
    ASSERT_EQ(DEL_PARTIAL_SATISFIED, c2->delete_state());
    ASSERT_EQ("bbb", c2->get_slice(0));
    ASSERT_EQ("bbc", c2->get_slice(1));
    ASSERT_EQ("ccc", c2->get_slice(2));
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_slice_cache) {
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    c1->get_data().reserve(10);
    c1->append_default();
    ASSERT_FALSE(c1->_slices_cache);
    ASSERT_EQ(c1->get_offset().size(), 2);

    BinaryColumn::Ptr c2 = BinaryColumn::create();
    c2->get_data().reserve(10);
    c2->append_default(5);
    ASSERT_FALSE(c2->_slices_cache);
    ASSERT_EQ(c2->get_offset().size(), 6);

    BinaryColumn::Ptr c3 = BinaryColumn::create();
    c3->get_data().reserve(10);
    c3->append(Slice("1"));
    ASSERT_FALSE(c3->_slices_cache);
    ASSERT_EQ(c3->get_offset().size(), 2);
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_reserve) {
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    c1->reserve(10, 40);

    ASSERT_FALSE(c1->_slices_cache);
    ASSERT_EQ(c1->_offsets.capacity(), 11);
    ASSERT_EQ(c1->_bytes.capacity(), 40);
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_copy_constructor) {
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    c1->append_datum("abc");
    c1->append_datum("def");

    // trigger cache building.
    auto slices = c1->get_data();
    ASSERT_EQ("abc", slices[0]);
    ASSERT_EQ("def", slices[1]);

    auto c2(*c1);

    c1.reset();
    slices = c2.get_data();
    ASSERT_EQ(2, c2.size());
    ASSERT_EQ("abc", slices[0]);
    ASSERT_EQ("def", slices[1]);
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_move_constructor) {
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    c1->append_datum("abc");
    c1->append_datum("def");

    // trigger cache building.
    auto slices = c1->get_data();
    ASSERT_EQ("abc", slices[0]);
    ASSERT_EQ("def", slices[1]);

    auto c2(std::move(*c1));

    c1.reset();
    slices = c2.get_data();
    ASSERT_EQ(2, c2.size());
    ASSERT_EQ("abc", slices[0]);
    ASSERT_EQ("def", slices[1]);
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_copy_assignment) {
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    c1->append_datum("abc");
    c1->append_datum("def");

    // trigger cache building.
    auto slices = c1->get_data();
    ASSERT_EQ("abc", slices[0]);
    ASSERT_EQ("def", slices[1]);

    BinaryColumn c2;
    c2 = *c1;

    c1.reset();
    slices = c2.get_data();
    ASSERT_EQ(2, c2.size());
    ASSERT_EQ("abc", slices[0]);
    ASSERT_EQ("def", slices[1]);
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_move_assignment) {
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    c1->append_datum("abc");
    c1->append_datum("def");

    // trigger cache building.
    auto slices = c1->get_data();
    ASSERT_EQ("abc", slices[0]);
    ASSERT_EQ("def", slices[1]);

    BinaryColumn c2;
    c2 = std::move(*c1);

    c1.reset();
    slices = c2.get_data();
    ASSERT_EQ(2, c2.size());
    ASSERT_EQ("abc", slices[0]);
    ASSERT_EQ("def", slices[1]);
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_clone) {
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    c1->append_datum("abc");
    c1->append_datum("def");

    // trigger cache building.
    auto slices = c1->get_data();
    ASSERT_EQ("abc", slices[0]);
    ASSERT_EQ("def", slices[1]);

    auto c2 = c1->clone();

    c1.reset();

    slices = down_cast<BinaryColumn*>(c2.get())->get_data();
    ASSERT_EQ(2, c2->size());
    ASSERT_EQ("abc", slices[0]);
    ASSERT_EQ("def", slices[1]);
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_clone_shared) {
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    c1->append_datum("abc");
    c1->append_datum("def");

    // trigger cache building.
    auto slices = c1->get_data();
    ASSERT_EQ("abc", slices[0]);
    ASSERT_EQ("def", slices[1]);

    auto c2 = c1->clone();
    ASSERT_TRUE(c2->use_count() == 1);

    c1.reset();

    slices = down_cast<BinaryColumn*>(c2.get())->get_data();
    ASSERT_EQ(2, c2->size());
    ASSERT_EQ("abc", slices[0]);
    ASSERT_EQ("def", slices[1]);
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_clone_empty) {
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    c1->append_datum("abc");
    c1->append_datum("def");

    // trigger cache building.
    auto slices = c1->get_data();
    ASSERT_EQ("abc", slices[0]);
    ASSERT_EQ("def", slices[1]);

    auto c2 = c1->clone_empty();

    c1.reset();

    ASSERT_EQ(0, c2->size());
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_update_rows) {
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    c1->append_datum("abc");
    c1->append_datum("def");
    c1->append_datum("ghi");
    c1->append_datum("jkl");
    c1->append_datum("mno");

    std::vector<uint32_t> replace_idxes = {1, 3};
    BinaryColumn::Ptr c2 = BinaryColumn::create();
    c2->append_datum("pq");
    c2->append_datum("rstu");
    c1->update_rows(*c2.get(), replace_idxes.data());

    auto slices = c1->get_data();
    EXPECT_EQ(5, c1->size());
    ASSERT_EQ("abc", slices[0]);
    ASSERT_EQ("pq", slices[1]);
    ASSERT_EQ("ghi", slices[2]);
    ASSERT_EQ("rstu", slices[3]);
    ASSERT_EQ("mno", slices[4]);

    BinaryColumn::Ptr c3 = BinaryColumn::create();
    c3->append_datum("ab");
    c3->append_datum("cdef");
    c1->update_rows(*c3.get(), replace_idxes.data());

    slices = c1->get_data();
    EXPECT_EQ(5, c1->size());
    ASSERT_EQ("abc", slices[0]);
    EXPECT_EQ("ab", slices[1]);
    ASSERT_EQ("ghi", slices[2]);
    ASSERT_EQ("cdef", slices[3]);
    ASSERT_EQ("mno", slices[4]);

    BinaryColumn::Ptr c4 = BinaryColumn::create();
    std::vector<uint32_t> new_replace_idxes = {0, 1};
    c4->append_datum("ab");
    c4->append_datum("cdef");
    c1->update_rows(*c4.get(), new_replace_idxes.data());

    slices = c1->get_data();
    EXPECT_EQ(5, c1->size());
    ASSERT_EQ("ab", slices[0]);
    EXPECT_EQ("cdef", slices[1]);
    ASSERT_EQ("ghi", slices[2]);
    ASSERT_EQ("cdef", slices[3]);
    ASSERT_EQ("mno", slices[4]);

#ifdef NDEBUG
    // This case will alloc a lot of memory (16G) and run slowly,
    // So i temp comment it and will open if i find one better solution
    /*
    size_t count = (1ul << 31ul) + 5;
    BinaryColumn::Ptr c5 = BinaryColumn::create();
    c5->get_bytes().resize(count);
    c5->get_offset().resize(count + 1);
    for (size_t i = 0; i < c5->get_offset().size(); i++) {
        c5->get_offset()[i] = i;
    }

    BinaryColumn::Ptr c6 = BinaryColumn::create();
    c6->append("22");

    std::vector<uint32_t> c6_replace_idxes = {0};
    ASSERT_TRUE(c5->update_rows(*c6, c6_replace_idxes.data()).ok());
    ASSERT_EQ(c5->size(), count);
    */
#endif
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_xor_checksum) {
    BinaryColumn::Ptr column = BinaryColumn::create();
    std::string str;
    str.reserve(3000);
    for (int i = 0; i <= 1000; i++) {
        str.append(std::to_string(i));
    }
    column->append(str);
    int64_t checksum = column->xor_checksum(0, 1);
    int64_t expected_checksum = 3546653113525744178L;
    ASSERT_EQ(checksum, expected_checksum);
}

// NOLINTNEXTLINE
PARALLEL_TEST(BinaryColumnTest, test_replicate) {
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    c1->append_datum("abc");
    c1->append_datum("def");

    Offsets offsets;
    offsets.push_back(0);
    offsets.push_back(3);
    offsets.push_back(5);

    auto c2 = c1->replicate(offsets).value();

    auto slices = down_cast<BinaryColumn*>(c2.get())->get_data();
    ASSERT_EQ(5, c2->size());
    ASSERT_EQ("abc", slices[0]);
    ASSERT_EQ("abc", slices[1]);
    ASSERT_EQ("abc", slices[2]);
    ASSERT_EQ("def", slices[3]);
    ASSERT_EQ("def", slices[4]);
}

PARALLEL_TEST(BinaryColumnTest, test_reference_memory_usage) {
    BinaryColumn::Ptr column = BinaryColumn::create();
    column->append("");
    column->append("1");
    column->append("23");
    column->append("456");

    ASSERT_EQ(0, column->Column::reference_memory_usage());
}

} // namespace starrocks
