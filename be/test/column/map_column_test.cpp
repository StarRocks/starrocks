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

#include "column/map_column.h"

#include <gtest/gtest.h>

#include <cstdint>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "testutil/parallel_test.h"

namespace starrocks {

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_create) {
    auto offsets = UInt32Column::create();
    auto keys_data = Int32Column::create();
    auto keys_null = NullColumn::create();
    auto keys = NullableColumn::create(keys_data, keys_null);
    auto values_data = Int32Column::create();
    auto values_null = NullColumn::create();
    auto values = NullableColumn::create(values_data, values_null);
    auto column = MapColumn::create(keys, values, offsets);
    ASSERT_TRUE(column->is_map());
    ASSERT_FALSE(column->is_nullable());
    ASSERT_EQ(0, column->size());
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_map_column_update_if_overflow) {
    // normal
    auto offsets = UInt32Column::create();
    auto keys_data = BinaryColumn::create();
    auto keys_null = NullColumn::create();
    auto keys = NullableColumn::create(keys_data, keys_null);
    auto values_data = BinaryColumn::create();
    auto null_column = NullColumn::create();
    auto values = NullableColumn::create(values_data, null_column);
    auto column = MapColumn::create(keys, values, offsets);

    DatumMap map;
    map[(Slice) "a"] = (Slice) "hello";
    map[(Slice) "b"] = (Slice) " ";
    map[(Slice) "c"] = (Slice) "world";
    column->append_datum(map);

    DatumMap map1;
    map1[(Slice) "def"] = (Slice) "haha";
    map1[(Slice) "g h"] = (Slice) "let's dance";
    column->append_datum(map1);

    // it does not upgrade because of not overflow
    auto ret = column->upgrade_if_overflow();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);
    ASSERT_EQ(column->size(), 2);
    ASSERT_EQ("{'a':'hello','b':' ','c':'world'}", column->debug_item(0));
    ASSERT_EQ("{'def':'haha','g h':'let's dance'}", column->debug_item(1));

#ifdef NDEBUG
    /*
    // the test case case will use a lot of memory, so temp comment it
    // upgrade
    auto offsets_large = UInt32Column::create();
    auto keys_large = BinaryColumn::create();
    auto values_data_large = Int32Column::create();
    auto values_null_large = NullColumn::create();
    auto values_large = NullableColumn::create(values_data_large, values_null_large);
    auto column_large = MapColumn::create(keys_large, values_large, offsets_large);

    size_t item_count = 1<<30;
    for (size_t i = 0; i < item_count; i++) {
        column_large->append_datum(DatumMap{{Slice(std::to_string(i)), (int32_t)i}});
    }

    ret = column->upgrade_if_overflow();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);
    ASSERT_TRUE(column->has_large_column());
    */
#endif
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_map_column_downgrade) {
    auto offsets = UInt32Column::create();
    auto keys_data = BinaryColumn::create();
    auto keys_null = NullColumn::create();
    auto keys = NullableColumn::create(keys_data, keys_null);
    auto values_data = BinaryColumn::create();
    auto null_column = NullColumn::create();
    auto values = NullableColumn::create(values_data, null_column);
    auto column = MapColumn::create(keys, values, offsets);

    DatumMap map;
    map[(Slice) "a"] = (Slice) "hello";
    map[(Slice) "b"] = (Slice) " ";
    map[(Slice) "c"] = (Slice) "world";
    column->append_datum(map);

    DatumMap map1;
    map1[(Slice) "def"] = (Slice) "haha";
    map1[(Slice) "g h"] = (Slice) "let's dance";
    column->append_datum(map1);

    ASSERT_FALSE(column->has_large_column());
    auto ret = column->downgrade();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);

    auto offsets_large = UInt32Column::create();
    auto keys_data_large = LargeBinaryColumn::create();
    auto keys_null_large = NullColumn::create();
    auto keys_large = NullableColumn::create(keys_data_large, keys_null_large);
    auto values_data_large = LargeBinaryColumn::create();
    auto null_column_large = NullColumn::create();
    auto values_large = NullableColumn::create(values_data_large, null_column_large);
    auto column_large = MapColumn::create(keys_large, values_large, offsets_large);

    for (size_t i = 0; i < 10; i++) {
        column_large->append_datum(DatumMap{{Slice(std::to_string(i)), Slice(std::to_string(i))}});
    }

    ASSERT_TRUE(column_large->has_large_column());
    ret = column_large->downgrade();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);
    ASSERT_FALSE(column_large->has_large_column());
    ASSERT_EQ(column_large->size(), 10);
    for (size_t i = 0; i < 10; i++) {
        ASSERT_EQ(column_large->get(i).get<DatumMap>().find(std::to_string(i))->second.get_slice(),
                  Slice(std::to_string(i)));
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_get_kvs) {
    auto offsets = UInt32Column::create();
    auto keys_data = Int32Column::create();
    auto keys_null = NullColumn::create();
    auto keys = NullableColumn::create(keys_data, keys_null);
    auto values_data = Int32Column::create();
    auto values_null = NullColumn::create();
    auto values = NullableColumn::create(values_data, values_null);
    auto column = MapColumn::create(keys, values, offsets);

    // insert [1, 2, 3], [4, 5, 6]
    DatumMap map;
    map[(int32_t)1] = (int32_t)11;
    map[(int32_t)2] = (int32_t)22;
    map[(int32_t)3] = (int32_t)33;
    column->append_datum(map);

    DatumMap map1;
    map1[(int32_t)4] = (int32_t)44;
    map1[(int32_t)5] = (int32_t)55;
    map1[(int32_t)6] = (int32_t)66;
    column->append_datum(map1);

    ASSERT_EQ("{1:11,2:22,3:33}", column->debug_item(0));
    ASSERT_EQ("{4:44,5:55,6:66}", column->debug_item(1));
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_byte_size) {
    auto offsets = UInt32Column::create();
    auto keys_data = Int32Column::create();
    auto keys_null = NullColumn::create();
    auto keys = NullableColumn::create(keys_data, keys_null);
    auto values_data = Int32Column::create();
    auto values_null = NullColumn::create();
    auto values = NullableColumn::create(values_data, values_null);
    auto column = MapColumn::create(keys, values, offsets);

    // insert [1, 2, 3], [4, 5, 6]
    DatumMap map;
    map[(int32_t)1] = (int32_t)11;
    map[(int32_t)2] = (int32_t)22;
    map[(int32_t)3] = (int32_t)33;
    column->append_datum(map);

    DatumMap map1;
    map1[(int32_t)4] = (int32_t)44;
    map1[(int32_t)5] = (int32_t)55;
    map1[(int32_t)6] = (int32_t)66;
    column->append_datum(map1);

    ASSERT_EQ(2, column->size());

    // keys data has six element, with 24 bytes.
    // keys null has six element, with 6 bytes.
    // values data has six element, with 24 bytes.
    // values null has six element, with 6 bytes.
    // offsets has three element, with 12 bytes.
    ASSERT_EQ(72, column->byte_size());
    // keys 0 data with 12 bytes.
    // keys 0 null with 3 bytes.
    // values 0 data with 12 bytes.
    // values 0 null with 3 bytes.
    // offset 0 with 4 bytes.
    ASSERT_EQ(34, column->byte_size(0, 1));

    // keys 1 data with 12 bytes.
    // keys 1 null with 3 bytes.
    // values 1 data with 12 bytes.
    // values 1 null with 3 bytes.
    // offset 1 with 4 bytes.
    ASSERT_EQ(34, column->byte_size(1, 1));
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_filter) {
    // MAP<INT->INT>
    {
        auto column = MapColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                        NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                        UInt32Column::create());
        // The width of AVX2 register is 256 bits, aka 32 bytes, make the column size equals
        // to 2 * 32 + 31 in order to cover both the SIMD instructions and non-SIMD instructions.
        const int N = 2 * 32 + 31;
        for (int32_t i = 0; i < N; i++) {
            column->append_datum(DatumMap{{i, i + 1}});
        }

        Filter filter(N, 1);

        column->filter_range(filter, 0, N);
        column->filter_range(filter, N / 10, N);
        column->filter_range(filter, N / 5, N);
        column->filter_range(filter, N / 2, N);
        ASSERT_EQ(N, column->size()) << column->debug_string();
        for (int i = 0; i < N; i++) {
            auto map = column->get(i).get<DatumMap>();
            ASSERT_EQ(1, map.size());
            ASSERT_EQ(i + 1, map.find(i)->second.get_int32());
        }

        filter.clear();
        filter.resize(N, 0);
        for (int i = N - 40; i < N; i++) {
            filter[i] = (i % 2 == 1);
        }
        column->filter_range(filter, N - 40, N);
        ASSERT_EQ(N - 20, column->size()) << column->debug_string();
        // First N-40 elements should keep unchanged.
        for (int i = 0; i < N - 40; i++) {
            auto map = column->get(i).get<DatumMap>();
            ASSERT_EQ(1, map.size());
            ASSERT_EQ(i + 1, map.find(i)->second.get_int32());
        }
        // Check the last elements.
        int j = 0;
        for (int i = N - 40; i < N; i++) {
            if (i % 2 == 1) {
                auto map = column->get(N - 40 + j).get<DatumMap>();
                ASSERT_EQ(1, map.size());
                ASSERT_EQ(i + 1, map.find(i)->second.get_int32());
                j++;
            }
        }

        // Remove the last 20 elements.
        filter.clear();
        filter.resize(column->size(), 0);
        column->filter_range(filter, N - 40, N - 20);
        ASSERT_EQ(N - 40, column->size());
        // First N-40 elements should keep unchanged.
        for (int i = 0; i < N - 40; i++) {
            auto map = column->get(i).get<DatumMap>();
            ASSERT_EQ(1, map.size());
            ASSERT_EQ(i + 1, map.find(i)->second.get_int32());
        }

        size_t expect_size = 0;
        filter.clear();
        filter.resize(column->size(), 0);
        for (int i = 0; i < filter.size(); i++) {
            filter[i] = (i % 2 == 0);
            expect_size += filter[i];
        }
        column->filter_range(filter, 0, filter.size());
        EXPECT_EQ(expect_size, column->size());
        j = 0;
        for (int i = 0; i < N - 40; i++) {
            if (i % 2 == 0) {
                auto map = column->get(j).get<DatumMap>();
                ASSERT_EQ(1, map.size());
                ASSERT_EQ(i + 1, map.find(i)->second.get_int32());
                j++;
            }
        }
    }
    // MAP<INT->INT>
    {
        auto column = MapColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                        NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                        UInt32Column::create());
        // The width of AVX2 register is 256 bits, aka 32 bytes, make the column size equals
        // to 2 * 32 + 31 in order to cover both the SIMD instructions and non-SIMD instructions.
        const int N = 3 * 32 + 31;
        for (int32_t i = 0; i < N; i++) {
            column->append_datum(DatumMap{{i, i * 2}});
        }

        Filter filter(N, 0);
        for (int i = 0; i < 32; i++) {
            filter[i] = i % 2;
        }
        for (int i = 32; i < 96; i++) {
            filter[i] = 1;
        }

        column->filter_range(filter, 0, N);
        ASSERT_EQ(80, column->size());
        int j = 0;
        for (int i = 0; i < 96; i++) {
            if (i < 32 && i % 2) {
                auto map = column->get(j).get<DatumMap>();
                ASSERT_EQ(1, map.size());
                ASSERT_EQ(i * 2, map.find(i)->second.get_int32());
                j++;
            } else if (32 <= i) {
                auto map = column->get(j).get<DatumMap>();
                ASSERT_EQ(1, map.size());
                ASSERT_EQ(i * 2, map.find(i)->second.get_int32());
                j++;
            }
        }
        ASSERT_EQ(80, j);
    }
    // MAP<INT->INT> with the number of elements > 2^16
    {
        auto column = MapColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                        NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                        UInt32Column::create());
        column->reserve(4096);
        for (int i = 0; i < 4096; i++) {
            DatumMap map;
            for (int j = 1; j <= 20; j++) {
                map.insert({j, j * i});
            }
            column->append_datum(map);
        }
        Filter filter(4096);
        for (int i = 0; i < 4096; i++) {
            filter[i] = i % 2;
        }
        column->filter_range(filter, 0, 4096);
        ASSERT_EQ(2048, column->size());
        int j = 0;
        for (int i = 0; i < 4096; i++) {
            if (i % 2) {
                auto map = column->get(j).get<DatumMap>();
                ASSERT_EQ(20, map.size());
                for (int k = 1; k <= 20; k++) {
                    ASSERT_EQ(i * k, map.find(k)->second.get_int32());
                }
                j++;
            }
        }
        filter.clear();
        filter.resize(column->size(), 0);
        column->filter(filter);
        ASSERT_EQ(0, column->size());
        ASSERT_EQ(0, column->keys_column()->size());
        ASSERT_EQ(0, column->values_column()->size());
    }
    // MAP<INT->ARRAY<INT>>
    {
        const int N = 100;
        auto array_nullable = NullableColumn::create(Int32Column::create(), NullColumn::create());
        auto value_column = NullableColumn::create(ArrayColumn::create(array_nullable, UInt32Column::create()),
                                                   NullColumn::create());
        auto column = MapColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                        value_column, UInt32Column::create());

        for (int i = 0; i < N; i++) {
            column->append_datum(DatumMap{{i, DatumArray{i + 1, i + 2}}, {i + 1, DatumArray{i + 1, i + 2}}});
        }

        Filter filter(N, 1);
        column->filter_range(filter, 0, N);
        column->filter_range(filter, N / 10, N);
        column->filter_range(filter, N / 2, N);
        ASSERT_EQ(N, column->size());
        for (int i = 0; i < N; i++) {
            auto map = column->get(i).get<DatumMap>();
            ASSERT_EQ(2, map.size());
            auto sub_array0 = map.find(i)->second.get_array();
            auto sub_array1 = map.find(i + 1)->second.get_array();
            ASSERT_EQ(2, sub_array0.size());
            ASSERT_EQ(i + 1, sub_array0[0].get_int32());
            ASSERT_EQ(i + 2, sub_array0[1].get_int32());
            ASSERT_EQ(2, sub_array1.size());
            ASSERT_EQ(i + 1, sub_array1[0].get_int32());
            ASSERT_EQ(i + 2, sub_array1[1].get_int32());
        }

        size_t expect_size = 0;
        for (int i = 0; i < N; i++) {
            filter[i] = (i % 3 != 0);
            expect_size += filter[i];
        }
        column->filter(filter);
        ASSERT_EQ(expect_size, column->size());
        int j = 0;
        for (int i = 0; i < N; i++) {
            if (i % 3 != 0) {
                auto map = column->get(j).get<DatumMap>();
                ASSERT_EQ(2, map.size());
                auto sub_array0 = map.find(i)->second.get_array();
                auto sub_array1 = map.find(i + 1)->second.get_array();
                ASSERT_EQ(2, sub_array0.size());
                ASSERT_EQ(i + 1, sub_array0[0].get_int32());
                ASSERT_EQ(i + 2, sub_array0[1].get_int32());
                ASSERT_EQ(2, sub_array1.size());
                ASSERT_EQ(i + 1, sub_array1[0].get_int32());
                ASSERT_EQ(i + 2, sub_array1[1].get_int32());
                j++;
            }
        }
        filter.clear();
        filter.resize(column->size(), 0);
        for (int i = filter.size() - 10; i < filter.size(); i++) {
            filter[i] = 1;
        }
        // No record should be filtered out.
        column->filter_range(filter, filter.size() - 10, filter.size());
        EXPECT_EQ(filter.size(), column->size());
        j = 0;
        for (int i = 0; i < N; i++) {
            if (i % 3 != 0) {
                auto map = column->get(j).get<DatumMap>();
                ASSERT_EQ(2, map.size());
                auto sub_array0 = map.find(i)->second.get_array();
                auto sub_array1 = map.find(i + 1)->second.get_array();
                ASSERT_EQ(2, sub_array0.size());
                ASSERT_EQ(i + 1, sub_array0[0].get_int32());
                ASSERT_EQ(i + 2, sub_array0[1].get_int32());
                ASSERT_EQ(2, sub_array1.size());
                ASSERT_EQ(i + 1, sub_array1[0].get_int32());
                ASSERT_EQ(i + 2, sub_array1[1].get_int32());
                j++;
            }
        }
        filter.clear();
        filter.resize(column->size(), 0);
        // All records will be filtered out.
        column->filter_range(filter, 0, filter.size());
        ASSERT_EQ(0, column->size());
        ASSERT_EQ(1, column->offsets_column()->size());
        ASSERT_EQ(0, column->keys_column()->size());
        ASSERT_EQ(0, column->values_column()->size());
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_append_datumMap_with_null_value) {
    auto offsets = UInt32Column::create();
    auto keys_data = Int32Column::create();
    auto keys_null = NullColumn::create();
    auto keys = NullableColumn::create(keys_data, keys_null);
    auto values_data = Int32Column::create();
    auto values_null = NullColumn::create();
    auto values = NullableColumn::create(values_data, values_null);
    auto column = MapColumn::create(keys, values, offsets);

    // insert [1, 2, 3], [4, 5, 6]
    DatumMap map;
    map[(int32_t)1] = (int32_t)11;
    map[(int32_t)2] = (int32_t)22;
    map[(int32_t)3] = (int32_t)33;
    column->append_datum(map);

    DatumMap map1;
    map1[(int32_t)4] = (int32_t)44;
    map1[(int32_t)5] = (int32_t)55;
    map1[(int32_t)6] = (int32_t)66;
    column->append_datum(map1);

    DatumMap map2;
    map2[(int32_t)7] = (int32_t)77;
    map2[(int32_t)8] = Datum();
    column->append_datum(map2);

    ASSERT_EQ("{7:77,8:NULL}", column->debug_item(2));
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_append_nulls) {
    auto offsets = UInt32Column::create();
    auto keys_data = Int32Column::create();
    auto keys_null = NullColumn::create();
    auto keys = NullableColumn::create(keys_data, keys_null);
    auto values_data = Int32Column::create();
    auto values_null = NullColumn::create();
    auto values = NullableColumn::create(values_data, values_null);
    auto column = MapColumn::create(keys, values, offsets);

    ASSERT_TRUE(column->append_nulls(1));

    // insert [1, 2, 3], [4, 5, 6]
    DatumMap map;
    map[(int32_t)1] = (int32_t)11;
    map[(int32_t)2] = (int32_t)22;
    map[(int32_t)3] = (int32_t)33;
    column->append_datum(map);

    DatumMap map1;
    map1[(int32_t)4] = (int32_t)44;
    map1[(int32_t)5] = (int32_t)55;
    map1[(int32_t)6] = (int32_t)66;
    column->append_datum(map1);

    ASSERT_EQ(3, column->size());
    ASSERT_EQ("{}", column->debug_item(0));
    ASSERT_EQ("{4:44,5:55,6:66}", column->debug_item(2));
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_append_defaults) {
    auto offsets = UInt32Column::create();
    auto keys_data = Int32Column::create();
    auto keys_null = NullColumn::create();
    auto keys = NullableColumn::create(keys_data, keys_null);
    auto values_data = Int32Column::create();
    auto null_column = NullColumn::create();
    auto values = NullableColumn::create(values_data, null_column);
    auto column = MapColumn::create(keys, values, offsets);

    // insert [1, 2, 3], [4, 5, 6]
    DatumMap map;
    map[(int32_t)1] = (int32_t)11;
    map[(int32_t)2] = (int32_t)22;
    map[(int32_t)3] = (int32_t)33;
    column->append_datum(map);

    DatumMap map1;
    map1[(int32_t)4] = (int32_t)44;
    map1[(int32_t)5] = (int32_t)55;
    map1[(int32_t)6] = (int32_t)66;
    column->append_datum(map1);

    // append_default
    column->append_default(2);

    ASSERT_EQ(4, column->size());
    ASSERT_EQ("{}", column->debug_item(2));
    ASSERT_EQ("{}", column->debug_item(3));
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_string_key) {
    auto offsets = UInt32Column::create();
    auto keys_data = BinaryColumn::create();
    auto keys_null = NullColumn::create();
    auto keys = NullableColumn::create(keys_data, keys_null);
    auto values_data = Int32Column::create();
    auto null_column = NullColumn::create();
    auto values = NullableColumn::create(values_data, null_column);
    auto column = MapColumn::create(keys, values, offsets);

    // insert ["a"->1, "b"->2, "c"->3], ["d"->4, "e"->5]
    DatumMap map;
    map[(Slice) "a"] = (int32_t)1;
    map[(Slice) "b"] = (int32_t)2;
    map[(Slice) "c"] = (int32_t)3;
    column->append_datum(map);

    DatumMap map1;
    map1[(Slice) "def"] = (int32_t)4;
    map1[(Slice) "g h"] = (int32_t)5;
    column->append_datum(map1);

    ASSERT_EQ("{'a':1,'b':2,'c':3}", column->debug_item(0));
    ASSERT_EQ("{'def':4,'g h':5}", column->debug_item(1));
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_string_value) {
    auto offsets = UInt32Column::create();
    auto keys_data = BinaryColumn::create();
    auto keys_null = NullColumn::create();
    auto keys = NullableColumn::create(keys_data, keys_null);
    auto values_data = BinaryColumn::create();
    auto null_column = NullColumn::create();
    auto values = NullableColumn::create(values_data, null_column);
    auto column = MapColumn::create(keys, values, offsets);

    // insert ["a"->1, "b"->2, "c"->3], ["d"->4, "e"->5]
    DatumMap map;
    map[(Slice) "a"] = (Slice) "hello";
    map[(Slice) "b"] = (Slice) " ";
    map[(Slice) "c"] = (Slice) "world";
    column->append_datum(map);

    DatumMap map1;
    map1[(Slice) "def"] = (Slice) "haha";
    map1[(Slice) "g h"] = (Slice) "let's dance";
    column->append_datum(map1);

    ASSERT_EQ("{'a':'hello','b':' ','c':'world'}", column->debug_item(0));
    ASSERT_EQ("{'def':'haha','g h':'let's dance'}", column->debug_item(1));
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_array_value) {
    auto offsets = UInt32Column::create();
    auto keys_data = BinaryColumn::create();
    auto keys_null = NullColumn::create();
    auto keys = NullableColumn::create(keys_data, keys_null);
    auto array_offsets = UInt32Column::create();
    auto array_elements_data = Int32Column::create();
    auto array_elements_null = NullColumn::create();
    auto array_elements = NullableColumn::create(array_elements_data, array_elements_null);
    auto values_data = ArrayColumn::create(array_elements, array_offsets);
    auto null_column = NullColumn::create();
    auto values = NullableColumn::create(values_data, null_column);
    auto column = MapColumn::create(keys, values, offsets);

    // insert ["a"->[1, 2, 3], "b"->[2, 3, 4], "c"->[3]], ["def"->[4, 5, 6, 7], "g h"->[5]]
    DatumMap map;
    DatumArray array_a(3);
    array_a[0] = (int32_t)1;
    array_a[1] = (int32_t)2;
    array_a[2] = (int32_t)3;
    map[(Slice) "a"] = array_a;
    DatumArray array_b(3);
    array_b[0] = (int32_t)2;
    array_b[1] = (int32_t)3;
    array_b[2] = (int32_t)4;
    map[(Slice) "b"] = array_b;
    DatumArray array_c(1);
    array_c[0] = (int32_t)3;
    map[(Slice) "c"] = array_c;
    column->append_datum(map);

    DatumMap map1;
    DatumArray array_d(4);
    array_d[0] = (int32_t)4;
    array_d[1] = (int32_t)5;
    array_d[2] = (int32_t)6;
    array_d[3] = (int32_t)7;
    map1[(Slice) "def"] = array_d;
    DatumArray array_e(1);
    array_e[0] = (int32_t)5;
    map1[(Slice) "g h"] = array_e;
    column->append_datum(map1);

    ASSERT_EQ("{'a':[1,2,3],'b':[2,3,4],'c':[3]}", column->debug_item(0));
    ASSERT_EQ("{'def':[4,5,6,7],'g h':[5]}", column->debug_item(1));
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_resize) {
    auto offsets = UInt32Column::create();
    auto keys_data = Int32Column::create();
    auto keys_null = NullColumn::create();
    auto keys = NullableColumn::create(keys_data, keys_null);
    auto values_data = Int32Column::create();
    auto null_column = NullColumn::create();
    auto values = NullableColumn::create(values_data, null_column);
    auto column = MapColumn::create(keys, values, offsets);

    // insert [1, 2, 3], [4, 5, 6], [7, 8, 9]
    DatumMap map;
    map[(int32_t)1] = (int32_t)11;
    map[(int32_t)2] = (int32_t)22;
    map[(int32_t)3] = (int32_t)33;
    column->append_datum(map);

    DatumMap map1;
    map1[(int32_t)4] = (int32_t)44;
    map1[(int32_t)5] = (int32_t)55;
    map1[(int32_t)6] = (int32_t)66;
    column->append_datum(map1);

    DatumMap map3;
    map3[(int32_t)7] = (int32_t)77;
    map3[(int32_t)8] = (int32_t)88;
    map3[(int32_t)9] = (int32_t)99;
    column->append_datum(map3);

    column->resize(1);
    ASSERT_EQ(1, column->size());
    ASSERT_EQ("{1:11,2:22,3:33}", column->debug_item(0));
    ASSERT_EQ(3, column->keys_column()->size());
    ASSERT_EQ(3, column->values_column()->size());
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_reset_column) {
    auto offsets = UInt32Column::create();
    auto keys_data = Int32Column::create();
    auto keys_null = NullColumn::create();
    auto keys = NullableColumn::create(keys_data, keys_null);
    auto values_data = Int32Column::create();
    auto null_column = NullColumn::create();
    auto values = NullableColumn::create(values_data, null_column);
    auto column = MapColumn::create(keys, values, offsets);

    // insert [1, 2, 3], [4, 5, 6], [7, 8, 9]
    // insert [1, 2, 3], [4, 5, 6], [7, 8, 9]
    DatumMap map;
    map[(int32_t)1] = (int32_t)11;
    map[(int32_t)2] = (int32_t)22;
    map[(int32_t)3] = (int32_t)33;
    column->append_datum(map);

    DatumMap map1;
    map1[(int32_t)4] = (int32_t)44;
    map1[(int32_t)5] = (int32_t)55;
    map1[(int32_t)6] = (int32_t)66;
    column->append_datum(map1);

    DatumMap map3;
    map3[(int32_t)7] = (int32_t)77;
    map3[(int32_t)8] = (int32_t)88;
    map3[(int32_t)9] = (int32_t)99;
    column->append_datum(map3);

    column->reset_column();
    ASSERT_EQ(0, column->size());
    ASSERT_EQ(0, column->keys_column()->size());
    ASSERT_EQ(0, column->values_column()->size());
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_swap_column) {
    auto offsets = UInt32Column::create();
    auto keys_data = Int32Column::create();
    auto keys_null = NullColumn::create();
    auto keys = NullableColumn::create(keys_data, keys_null);
    auto values_data = Int32Column::create();
    auto null_column = NullColumn::create();
    auto values = NullableColumn::create(values_data, null_column);
    auto column = MapColumn::create(keys, values, offsets);

    // insert [1, 2, 3], [4, 5, 6]
    DatumMap map;
    map[(int32_t)1] = (int32_t)11;
    map[(int32_t)2] = (int32_t)22;
    map[(int32_t)3] = (int32_t)33;
    column->append_datum(map);

    DatumMap map1;
    map1[(int32_t)4] = (int32_t)44;
    map1[(int32_t)5] = (int32_t)55;
    map1[(int32_t)6] = (int32_t)66;
    column->append_datum(map1);

    auto offsets2 = UInt32Column::create();
    auto keys2_data = Int32Column::create();
    auto keys2_null = NullColumn::create();
    auto keys2 = NullableColumn::create(keys2_data, keys2_null);
    auto values_data2 = Int32Column::create();
    auto null_column2 = NullColumn::create();
    auto values2 = NullableColumn::create(values_data2, null_column2);
    auto column2 = MapColumn::create(keys2, values2, offsets2);

    // insert [7, 8, 9]
    DatumMap map3;
    map3[(int32_t)7] = (int32_t)77;
    map3[(int32_t)8] = (int32_t)88;
    map3[(int32_t)9] = (int32_t)99;
    column2->append_datum(map3);

    column->swap_column(*column2);
    ASSERT_EQ(1, column->size());
    ASSERT_EQ("{7:77,8:88,9:99}", column->debug_item(0));
    ASSERT_EQ(2, column2->size());
    ASSERT_EQ("{1:11,2:22,3:33}", column2->debug_item(0));
    ASSERT_EQ("{4:44,5:55,6:66}", column2->debug_item(1));
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_copy_constructor) {
    auto c0 = MapColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                UInt32Column::create());

    // insert [1, 2, 3], [4, 5, 6]
    DatumMap map;
    map[(int32_t)1] = (int32_t)11;
    map[(int32_t)2] = (int32_t)22;
    map[(int32_t)3] = (int32_t)33;
    c0->append_datum(map);

    DatumMap map1;
    map1[(int32_t)4] = (int32_t)44;
    map1[(int32_t)5] = (int32_t)55;
    map1[(int32_t)6] = (int32_t)66;
    c0->append_datum(map1);

    MapColumn c1(*c0);
    c0->reset_column();
    ASSERT_EQ("{1:11,2:22,3:33}", c1.debug_item(0));
    ASSERT_EQ("{4:44,5:55,6:66}", c1.debug_item(1));
    ASSERT_TRUE(c1.keys_column().unique());
    ASSERT_TRUE(c1.values_column().unique());
    ASSERT_TRUE(c1.offsets_column().unique());
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_move_constructor) {
    auto c0 = MapColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                UInt32Column::create());

    // insert [1, 2, 3], [4, 5, 6]
    DatumMap map;
    map[(int32_t)1] = (int32_t)11;
    map[(int32_t)2] = (int32_t)22;
    map[(int32_t)3] = (int32_t)33;
    c0->append_datum(map);

    DatumMap map1;
    map1[(int32_t)4] = (int32_t)44;
    map1[(int32_t)5] = (int32_t)55;
    map1[(int32_t)6] = (int32_t)66;
    c0->append_datum(map1);

    MapColumn c1(std::move(*c0));
    ASSERT_EQ("{1:11,2:22,3:33}", c1.debug_item(0));
    ASSERT_EQ("{4:44,5:55,6:66}", c1.debug_item(1));
    ASSERT_TRUE(c1.keys_column().unique());
    ASSERT_TRUE(c1.values_column().unique());
    ASSERT_TRUE(c1.offsets_column().unique());
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_copy_assignment) {
    auto c0 = MapColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                UInt32Column::create());

    // insert [1, 2, 3], [4, 5, 6]
    DatumMap map;
    map[(int32_t)1] = (int32_t)11;
    map[(int32_t)2] = (int32_t)22;
    map[(int32_t)3] = (int32_t)33;
    c0->append_datum(map);

    DatumMap map1;
    map1[(int32_t)4] = (int32_t)44;
    map1[(int32_t)5] = (int32_t)55;
    map1[(int32_t)6] = (int32_t)66;
    c0->append_datum(map1);

    MapColumn c1(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                 NullableColumn::create(Int32Column::create(), NullColumn::create()), UInt32Column::create());
    c1 = *c0;
    c0->reset_column();
    ASSERT_EQ("{1:11,2:22,3:33}", c1.debug_item(0));
    ASSERT_EQ("{4:44,5:55,6:66}", c1.debug_item(1));
    ASSERT_TRUE(c1.keys_column().unique());
    ASSERT_TRUE(c1.values_column().unique());
    ASSERT_TRUE(c1.offsets_column().unique());
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_move_assignment) {
    auto c0 = MapColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                UInt32Column::create());

    // insert [1, 2, 3], [4, 5, 6]
    DatumMap map;
    map[(int32_t)1] = (int32_t)11;
    map[(int32_t)2] = (int32_t)22;
    map[(int32_t)3] = (int32_t)33;
    c0->append_datum(map);

    DatumMap map1;
    map1[(int32_t)4] = (int32_t)44;
    map1[(int32_t)5] = (int32_t)55;
    map1[(int32_t)6] = (int32_t)66;
    c0->append_datum(map1);

    MapColumn c1(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                 NullableColumn::create(Int32Column::create(), NullColumn::create()), UInt32Column::create());
    c1 = std::move(*c0);
    ASSERT_EQ("{1:11,2:22,3:33}", c1.debug_item(0));
    ASSERT_EQ("{4:44,5:55,6:66}", c1.debug_item(1));
    ASSERT_TRUE(c1.keys_column().unique());
    ASSERT_TRUE(c1.values_column().unique());
    ASSERT_TRUE(c1.offsets_column().unique());
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_clone) {
    auto c0 = MapColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                UInt32Column::create());

    // insert [1, 2, 3], [4, 5, 6]
    DatumMap map;
    map[(int32_t)1] = (int32_t)11;
    map[(int32_t)2] = (int32_t)22;
    map[(int32_t)3] = (int32_t)33;
    c0->append_datum(map);

    DatumMap map1;
    map1[(int32_t)4] = (int32_t)44;
    map1[(int32_t)5] = (int32_t)55;
    map1[(int32_t)6] = (int32_t)66;
    c0->append_datum(map1);

    auto c1 = c0->clone();
    c0->reset_column();
    ASSERT_EQ("{1:11,2:22,3:33}", down_cast<MapColumn*>(c1.get())->debug_item(0));
    ASSERT_EQ("{4:44,5:55,6:66}", down_cast<MapColumn*>(c1.get())->debug_item(1));
    ASSERT_TRUE(down_cast<MapColumn*>(c1.get())->keys_column().unique());
    ASSERT_TRUE(down_cast<MapColumn*>(c1.get())->values_column().unique());
    ASSERT_TRUE(down_cast<MapColumn*>(c1.get())->offsets_column().unique());
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_clone_shared) {
    auto c0 = MapColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                UInt32Column::create());

    // insert [1, 2, 3], [4, 5, 6]
    DatumMap map;
    map[(int32_t)1] = (int32_t)11;
    map[(int32_t)2] = (int32_t)22;
    map[(int32_t)3] = (int32_t)33;
    c0->append_datum(map);

    DatumMap map1;
    map1[(int32_t)4] = (int32_t)44;
    map1[(int32_t)5] = (int32_t)55;
    map1[(int32_t)6] = (int32_t)66;
    c0->append_datum(map1);

    auto c1 = c0->clone_shared();
    c0->reset_column();
    ASSERT_EQ("{1:11,2:22,3:33}", down_cast<MapColumn*>(c1.get())->debug_item(0));
    ASSERT_EQ("{4:44,5:55,6:66}", down_cast<MapColumn*>(c1.get())->debug_item(1));
    ASSERT_TRUE(c1.unique());
    ASSERT_TRUE(down_cast<MapColumn*>(c1.get())->keys_column().unique());
    ASSERT_TRUE(down_cast<MapColumn*>(c1.get())->values_column().unique());
    ASSERT_TRUE(down_cast<MapColumn*>(c1.get())->offsets_column().unique());
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_clone_column) {
    auto c0 = MapColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                UInt32Column::create());

    // insert [1, 2, 3], [4, 5, 6]
    DatumMap map;
    map[(int32_t)1] = (int32_t)11;
    map[(int32_t)2] = (int32_t)22;
    map[(int32_t)3] = (int32_t)33;
    c0->append_datum(map);

    DatumMap map1;
    map1[(int32_t)4] = (int32_t)44;
    map1[(int32_t)5] = (int32_t)55;
    map1[(int32_t)6] = (int32_t)66;
    c0->append_datum(map1);

    auto cloned_column = c0->clone_empty();
    ASSERT_TRUE(cloned_column->is_map());
    ASSERT_EQ(0, cloned_column->size());
    ASSERT_EQ(0, down_cast<MapColumn*>(cloned_column.get())->keys_column()->size());
    ASSERT_EQ(0, down_cast<MapColumn*>(cloned_column.get())->values_column()->size());
    ASSERT_EQ(1, down_cast<MapColumn*>(cloned_column.get())->offsets_column()->size());
}

//PARALLEL_TEST(ArrayColumnTest, test_array_hash) {
//    auto c0 = ArrayColumn::create(Int32Column::create(), UInt32Column::create());
//
//    auto* offsets = down_cast<UInt32Column*>(c0->offsets_column().get());
//    auto* elements = down_cast<Int32Column*>(c0->elements_column().get());
//
//    // insert [1, 2, 3], [4, 5, 6]
//    size_t array_size_1 = 3;
//    elements->append(1);
//    elements->append(2);
//    elements->append(3);
//    offsets->append(3);
//
//    size_t array_size_2 = 3;
//    elements->append(4);
//    elements->append(5);
//    elements->append(6);
//    offsets->append(6);
//
//    uint32_t hash_value[2] = {0, 0};
//    c0->crc32_hash(hash_value, 0, 2);
//
//    uint32_t hash_value_1 = HashUtil::zlib_crc_hash(&array_size_1, sizeof(array_size_1), 0);
//    for (int i = 0; i < 3; ++i) {
//        elements->crc32_hash(&hash_value_1 - i, i, i + 1);
//    }
//    uint32_t hash_value_2 = HashUtil::zlib_crc_hash(&array_size_2, sizeof(array_size_2), 0);
//    for (int i = 3; i < 6; ++i) {
//        elements->crc32_hash(&hash_value_2 - i, i, i + 1);
//    }
//    ASSERT_EQ(hash_value_1, hash_value[0]);
//    ASSERT_EQ(hash_value_2, hash_value[1]);
//
//    uint32_t hash_value_fnv[2] = {0, 0};
//    c0->fnv_hash(hash_value_fnv, 0, 2);
//    uint32_t hash_value_1_fnv = HashUtil::fnv_hash(&array_size_1, sizeof(array_size_1), 0);
//    for (int i = 0; i < 3; ++i) {
//        elements->fnv_hash(&hash_value_1_fnv - i, i, i + 1);
//    }
//    uint32_t hash_value_2_fnv = HashUtil::fnv_hash(&array_size_2, sizeof(array_size_2), 0);
//    for (int i = 3; i < 6; ++i) {
//        elements->fnv_hash(&hash_value_2_fnv - i, i, i + 1);
//    }
//
//    ASSERT_EQ(hash_value_1_fnv, hash_value_fnv[0]);
//    ASSERT_EQ(hash_value_2_fnv, hash_value_fnv[1]);
//
//    // overflow test
//    for (int i = 0; i < 100000; ++i) {
//        elements->append(i);
//    }
//    offsets->append(elements->size());
//    uint32_t hash_value_overflow_test[3] = {0, 0, 0};
//    c0->crc32_hash(hash_value_overflow_test, 0, 3);
//
//    auto& offset_values = offsets->get_data();
//    size_t sz = offset_values[offset_values.size() - 1] - offset_values[offset_values.size() - 2];
//
//    uint32_t hash_value_overflow = HashUtil::zlib_crc_hash(&sz, sizeof(sz), 0);
//    for (int i = 0; i < 100000; ++i) {
//        uint32_t value = i;
//        hash_value_overflow = HashUtil::zlib_crc_hash(&value, sizeof(value), hash_value_overflow);
//    }
//
//    ASSERT_EQ(hash_value_overflow, hash_value_overflow_test[2]);
//}
//
//PARALLEL_TEST(ArrayColumnTest, test_xor_checksum) {
//    auto c0 = ArrayColumn::create(Int32Column::create(), UInt32Column::create());
//
//    auto* offsets = down_cast<UInt32Column*>(c0->offsets_column().get());
//    auto* elements = down_cast<Int32Column*>(c0->elements_column().get());
//
//    // insert [1, 2, 3], [4, 5, 6, 7]
//    elements->append(1);
//    elements->append(2);
//    elements->append(3);
//    offsets->append(3);
//
//    elements->append(4);
//    elements->append(5);
//    elements->append(6);
//    elements->append(7);
//    elements->append(8);
//    offsets->append(8);
//
//    int64_t checksum = c0->xor_checksum(0, 2);
//    int64_t expected_checksum = 14;
//
//    ASSERT_EQ(checksum, expected_checksum);
//}

PARALLEL_TEST(MapColumnTest, test_update_rows) {
    auto c0 = MapColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                UInt32Column::create());

    // insert [1, 2, 3], [4, 5, 6]
    DatumMap map;
    map[1] = 11;
    map[2] = 22;
    map[3] = 33;
    c0->append_datum(map);

    DatumMap map1;
    map1[4] = 44;
    map1[5] = 55;
    map1[6] = 66;
    c0->append_datum(map1);

    c0->append_nulls(1);

    // append [7, 8, 9]
    DatumMap map2;
    map2[7] = 77;
    map2[8] = 88;
    map2[9] = 99;
    c0->append_datum(map2);

    auto c1 = MapColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                UInt32Column::create());

    // insert [101, 102], [103, 104]
    DatumMap map3;
    map3[101] = 111;
    map3[102] = 112;
    c1->append_datum(map3);

    DatumMap map4;
    map4[103] = 113;
    map4[104] = 114;
    c1->append_datum(map4);

    std::vector<uint32_t> replace_idxes = {1, 3};
    ASSERT_TRUE(c0->update_rows(*c1.get(), replace_idxes.data()).ok());

    ASSERT_EQ(4, c0->size());
    ASSERT_EQ("{1:11,2:22,3:33}", c0->debug_item(0));
    ASSERT_EQ("{101:111,102:112}", c0->debug_item(1));
    ASSERT_EQ("{}", c0->debug_item(2));
    ASSERT_EQ("{103:113,104:114}", c0->debug_item(3));

    auto c2 = MapColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                UInt32Column::create());

    // insert [201, 202], [203, 204]
    DatumMap map5;
    map5[201] = 211;
    map5[202] = 212;
    c2->append_datum(map5);

    DatumMap map6;
    map6[203] = 213;
    map6[204] = 214;
    c2->append_datum(map6);

    std::vector<uint32_t> replace_idxes_new = {1, 2};
    ASSERT_TRUE(c0->update_rows(*c2.get(), replace_idxes_new.data()).ok());

    ASSERT_EQ(4, c0->size());
    ASSERT_EQ("{1:11,2:22,3:33}", c0->debug_item(0));
    ASSERT_EQ("{201:211,202:212}", c0->debug_item(1));
    ASSERT_EQ("{203:213,204:214}", c0->debug_item(2));
    ASSERT_EQ("{103:113,104:114}", c0->debug_item(3));
}

PARALLEL_TEST(MapColumnTest, test_assign) {
    auto c0 = MapColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                UInt32Column::create());

    // insert [1, 2, 3], [4, 5, 6]
    DatumMap map;
    map[1] = 11;
    map[2] = 22;
    map[3] = 33;
    c0->append_datum(map);

    DatumMap map1;
    map1[4] = 44;
    map1[5] = 55;
    map1[6] = 66;
    c0->append_datum(map1);

    // assign
    c0->assign(4, 0);
    ASSERT_EQ(4, c0->size());
    ASSERT_EQ("{1:11,2:22,3:33}", c0->debug_item(0));
    ASSERT_EQ("{1:11,2:22,3:33}", c0->debug_item(1));
    ASSERT_EQ("{1:11,2:22,3:33}", c0->debug_item(2));
    ASSERT_EQ("{1:11,2:22,3:33}", c0->debug_item(3));

    /// test assign [key->null]
    c0->reset_column();
    DatumMap map2;
    map2[1] = Datum();
    c0->append_datum(map2);
    c0->assign(5, 0);
    ASSERT_EQ(5, c0->size());
    ASSERT_EQ(6, c0->offsets_column()->size());
    ASSERT_EQ(5, c0->keys_column()->size());
    ASSERT_EQ(5, c0->values_column()->size());
    ASSERT_EQ(5, down_cast<NullableColumn*>(c0->values_column().get())->data_column()->size());
    ASSERT_EQ(5, down_cast<NullableColumn*>(c0->values_column().get())->null_column()->size());

    /// test assign []
    c0->reset_column();
    c0->append_datum(DatumMap{});

    c0->assign(5, 0);
    ASSERT_EQ(5, c0->size());
    ASSERT_TRUE(c0->get(0).get<DatumMap>().empty());
    ASSERT_TRUE(c0->get(4).get<DatumMap>().empty());
    ASSERT_EQ(0, c0->keys_column()->size());
    ASSERT_EQ(0, c0->values_column()->size());
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_euqals) {
    // lhs: {1:1,2:2}, {4:4,3:3}, {null:null}, {1:null}
    // rhs: {2:2,1:1}, {4:4}, {null, 1}, {1, 1}
    MapColumn::Ptr lhs;
    {
        auto offsets = UInt32Column::create();
        auto keys_data = Int32Column::create();
        auto keys_null = NullColumn::create();
        auto keys = NullableColumn::create(keys_data, keys_null);
        auto values_data = Int32Column::create();
        auto values_null = NullColumn::create();
        auto values = NullableColumn::create(values_data, values_null);
        lhs = MapColumn::create(keys, values, offsets);
    }
    {
        DatumMap map;
        map[(int32_t)1] = (int32_t)1;
        map[(int32_t)2] = (int32_t)2;
        lhs->append_datum(map);
    }
    {
        DatumMap map;
        map[(int32_t)4] = (int32_t)4;
        map[(int32_t)3] = (int32_t)3;
        lhs->append_datum(map);
    }
    {
        lhs->keys_column()->append_nulls(1);
        lhs->values_column()->append_nulls(1);
        lhs->offsets_column()->append(lhs->keys_column()->size());
    }
    {
        DatumMap map;
        map[(int32_t)1] = Datum();
        lhs->append_datum(map);
    }

    MapColumn::Ptr rhs;
    {
        auto offsets = UInt32Column::create();
        auto keys_data = Int32Column::create();
        auto keys_null = NullColumn::create();
        auto keys = NullableColumn::create(keys_data, keys_null);
        auto values = Int32Column::create();
        rhs = MapColumn::create(keys, values, offsets);
    }
    {
        DatumMap map;
        map[(int32_t)2] = (int32_t)2;
        map[(int32_t)1] = (int32_t)1;
        rhs->append_datum(map);
    }
    {
        DatumMap map;
        map[(int32_t)3] = (int32_t)4;
        map[(int32_t)4] = (int32_t)4;
        rhs->append_datum(map);
    }
    {
        rhs->keys_column()->append_nulls(1);
        rhs->values_column()->append_datum(Datum((int32_t)1));
        rhs->offsets_column()->append(rhs->keys_column()->size());
    }
    {
        DatumMap map;
        map[(int32_t)1] = (int32_t)1;
        rhs->append_datum(map);
    }
    ASSERT_TRUE(lhs->equals(0, *rhs, 0));
    ASSERT_FALSE(lhs->equals(1, *rhs, 1));
    ASSERT_FALSE(lhs->equals(2, *rhs, 2));
    ASSERT_FALSE(lhs->equals(3, *rhs, 3));
}

PARALLEL_TEST(MapColumnTest, test_element_memory_usage) {
    auto column = MapColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                    NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                    UInt32Column::create());

    // {}, {1:2},{3:4,5:6}
    column->append_datum(DatumMap{});
    column->append_datum(DatumMap{{1, 2}});
    column->append_datum(DatumMap{{3, 4}, {5, 6}});

    ASSERT_EQ(42, column->Column::element_memory_usage());

    std::vector<size_t> element_mem_usages = {4, 14, 24};
    size_t element_num = element_mem_usages.size();
    for (size_t start = 0; start < element_num; start++) {
        size_t expected_usage = 0;
        ASSERT_EQ(0, column->element_memory_usage(start, 0));
        for (size_t size = 1; start + size <= element_num; size++) {
            expected_usage += element_mem_usages[start + size - 1];
            ASSERT_EQ(expected_usage, column->element_memory_usage(start, size));
        }
    }
}
} // namespace starrocks
