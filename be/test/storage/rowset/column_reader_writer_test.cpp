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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/rowset/segment_v2/column_reader_writer_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column.h"
#include "column/column_access_path.h"
#include "column/datum_convert.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "fs/fs_memory.h"
#include "gen_cpp/segment.pb.h"
#include "runtime/mem_pool.h"
#include "storage/aggregate_type.h"
#include "storage/chunk_helper.h"
#include "storage/decimal12.h"
#include "storage/olap_common.h"
#include "storage/range.h"
#include "storage/rowset/binary_plain_page.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/column_writer.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/rowset/ordinal_page_index.h"
#include "storage/rowset/page_io.h"
#include "storage/rowset/scalar_column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/storage_engine.h"
#include "storage/tablet_schema_helper.h"
#include "storage/type_traits.h"
#include "storage/types.h"
#include "testutil/assert.h"
#include "types/date_value.h"
#include "util/compression/block_compression.h"
#include "util/compression/zstd_dict.h"

using std::string;

namespace starrocks {

// NOLINTNEXTLINE
static const std::string TEST_DIR = "/column_reader_writer_test";

class ColumnReaderWriterTest : public testing::Test {
public:
    ColumnReaderWriterTest() {
        TabletSchemaPB schema_pb;
        auto* c0 = schema_pb.add_column();
        c0->set_name("pk");
        c0->set_is_key(true);
        c0->set_type("BIGINT");

        auto* c1 = schema_pb.add_column();
        c1->set_name("v1");
        c1->set_is_key(false);
        c1->set_type("SMALLINT");

        auto* c2 = schema_pb.add_column();
        c2->set_name("v2");
        c2->set_type("INT");

        _dummy_segment_schema = TabletSchema::create(schema_pb);
    }

    ~ColumnReaderWriterTest() override = default;

protected:
    void SetUp() override {}

    void TearDown() override {}

    std::shared_ptr<Segment> create_dummy_segment(const std::shared_ptr<FileSystem>& fs, const std::string& fname) {
        return std::make_shared<Segment>(fs, FileInfo{fname}, 1, _dummy_segment_schema, nullptr);
    }

    template <LogicalType type, EncodingTypePB encoding, uint32_t version>
    void test_nullable_data(const Column& src, const std::string& null_encoding = "0",
                            const std::string& null_ratio = "0") {
        config::set_config("null_encoding", null_encoding);

        using Type = typename TypeTraits<type>::CppType;
        TypeInfoPtr type_info = get_type_info(type);
        int num_rows = src.size();
        ColumnMetaPB meta;

        auto fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(fs->create_dir(TEST_DIR).ok());

        const std::string fname = strings::Substitute("$0/test-$1-$2-$3-$4-$5.data", TEST_DIR, type, encoding, version,
                                                      null_encoding, null_ratio);
        auto segment = create_dummy_segment(fs, fname);
        // write data
        {
            ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(fname));

            ColumnWriterOptions writer_opts;
            writer_opts.page_format = version;
            writer_opts.meta = &meta;
            writer_opts.meta->set_column_id(0);
            writer_opts.meta->set_unique_id(0);
            writer_opts.meta->set_type(type);
            if (type == TYPE_CHAR || type == TYPE_VARCHAR) {
                writer_opts.meta->set_length(128);
            } else {
                writer_opts.meta->set_length(0);
            }
            writer_opts.meta->set_encoding(encoding);
            writer_opts.meta->set_compression(starrocks::LZ4_FRAME);
            writer_opts.meta->set_is_nullable(true);
            writer_opts.need_zone_map = true;

            TabletColumn column(STORAGE_AGGREGATE_NONE, type);
            if (type == TYPE_VARCHAR) {
                column = create_varchar_key(1, true, 128);
            } else if (type == TYPE_CHAR) {
                column = create_char_key(1, true, 128);
            }
            ASSIGN_OR_ABORT(auto writer, ColumnWriter::create(writer_opts, &column, wfile.get()));
            ASSERT_OK(writer->init());

            ASSERT_TRUE(writer->append(src).ok());

            ASSERT_TRUE(writer->finish().ok());
            ASSERT_TRUE(writer->write_data().ok());
            ASSERT_TRUE(writer->write_ordinal_index().ok());
            ASSERT_TRUE(writer->write_zone_map().ok());

            // close the file
            ASSERT_TRUE(wfile->close().ok());
            std::cout << "version=" << version << ", bytes append: " << wfile->size() << "\n";
        }
        // read and check
        {
            // read and check
            auto res = ColumnReader::create(&meta, segment.get(), nullptr);
            ASSERT_TRUE(res.ok());
            auto reader = std::move(res).value();

            ASSIGN_OR_ABORT(auto iter, reader->new_iterator());
            ASSIGN_OR_ABORT(auto read_file, fs->new_random_access_file(fname));

            ColumnIteratorOptions iter_opts;
            OlapReaderStatistics stats;
            iter_opts.stats = &stats;
            iter_opts.read_file = read_file.get();
            iter_opts.use_page_cache = true;
            auto st = iter->init(iter_opts);
            ASSERT_TRUE(st.ok());

            // first read get data from disk
            // second read get data from page cache
            for (int i = 0; i < 2; ++i) {
                // sequence read
                {
                    st = iter->seek_to_first();
                    ASSERT_TRUE(st.ok()) << st.to_string();
                    MutableColumnPtr dst = ChunkHelper::column_from_field_type(type, true);
                    // will do direct copy to column
                    size_t rows_read = src.size();
                    dst->reserve(rows_read);
                    st = iter->next_batch(&rows_read, dst.get());
                    ASSERT_TRUE(st.ok());
                    ASSERT_EQ(src.size(), rows_read);
                    ASSERT_EQ(dst->size(), rows_read);

                    for (size_t i = 0; i < rows_read; i++) {
                        ASSERT_EQ(0, type_info->cmp(src.get(i), dst->get(i)))
                                << " row " << i << ": " << datum_to_string(type_info.get(), src.get(i)) << " vs "
                                << datum_to_string(type_info.get(), dst->get(i));
                    }
                }

                {
                    for (int rowid = 0; rowid < num_rows; rowid += 4025) {
                        st = iter->seek_to_ordinal(rowid);
                        ASSERT_TRUE(st.ok());

                        size_t rows_read = 1024;
                        MutableColumnPtr dst = ChunkHelper::column_from_field_type(type, true);

                        st = iter->next_batch(&rows_read, dst.get());
                        ASSERT_TRUE(st.ok());
                        for (int i = 0; i < rows_read; ++i) {
                            ASSERT_EQ(0, type_info->cmp(src.get(rowid + i), dst->get(i)))
                                    << " row " << rowid + i << ": "
                                    << datum_to_string(type_info.get(), src.get(rowid + i)) << " vs "
                                    << datum_to_string(type_info.get(), dst->get(i));
                        }
                    }
                }

                {
                    st = iter->seek_to_first();
                    ASSERT_TRUE(st.ok());

                    MutableColumnPtr dst = ChunkHelper::column_from_field_type(type, true);
                    SparseRange<> read_range;
                    size_t write_num = src.size();
                    read_range.add(Range<>(0, write_num / 3));
                    read_range.add(Range<>(write_num / 2, (write_num * 2 / 3)));
                    read_range.add(Range<>((write_num * 3 / 4), write_num));
                    size_t read_num = read_range.span_size();

                    st = iter->next_batch(read_range, dst.get());
                    ASSERT_TRUE(st.ok());
                    ASSERT_EQ(read_num, dst->size());

                    size_t offset = 0;
                    SparseRangeIterator<> read_iter = read_range.new_iterator();
                    while (read_iter.has_more()) {
                        Range<> r = read_iter.next(read_num);
                        for (int i = 0; i < r.span_size(); ++i) {
                            ASSERT_EQ(0, type_info->cmp(src.get(r.begin() + i), dst->get(i + offset)))
                                    << " row " << r.begin() + i << ": "
                                    << datum_to_string(type_info.get(), src.get(r.begin() + i)) << " vs "
                                    << datum_to_string(type_info.get(), dst->get(i + offset));
                        }
                        offset += r.span_size();
                    }
                }
            }
        }
    }

    template <LogicalType type>
    void test_read_default_value(string value, void* result) {
        using Type = typename TypeTraits<type>::CppType;
        TypeInfoPtr type_info = get_type_info(type);
        // read and check
        {
            TabletColumn tablet_column = create_with_default_value<type>(value);
            DefaultValueColumnIterator iter(tablet_column.has_default_value(), tablet_column.default_value(),
                                            tablet_column.is_nullable(), type_info, tablet_column.length(), 100);
            ColumnIteratorOptions iter_opts;
            auto st = iter.init(iter_opts);
            ASSERT_TRUE(st.ok());
            // sequence read
            {
                st = iter.seek_to_first();
                ASSERT_TRUE(st.ok()) << st.to_string();

                auto column = ChunkHelper::column_from_field_type(type, true);

                size_t rows_read = 512;
                st = iter.next_batch(&rows_read, column.get());
                ASSERT_TRUE(st.ok());
                for (int j = 0; j < rows_read; ++j) {
                    if (type == TYPE_CHAR) {
                        ASSERT_EQ(*(string*)result, reinterpret_cast<const Slice*>(column->raw_data())[j].to_string())
                                << "j:" << j;
                    } else if (type == TYPE_VARCHAR || type == TYPE_HLL || type == TYPE_OBJECT) {
                        ASSERT_EQ(value, reinterpret_cast<const Slice*>(column->raw_data())[j].to_string())
                                << "j:" << j;
                    } else {
                        ASSERT_EQ(*(Type*)result, reinterpret_cast<const Type*>(column->raw_data())[j]);
                    }
                }
            }

            {
                auto column = ChunkHelper::column_from_field_type(type, true);

                for (int rowid = 0; rowid < 1024; rowid += 128) {
                    st = iter.seek_to_ordinal(rowid);
                    ASSERT_TRUE(st.ok());

                    size_t rows_read = 512;
                    st = iter.next_batch(&rows_read, column.get());
                    ASSERT_TRUE(st.ok());
                    for (int j = 0; j < rows_read; ++j) {
                        if (type == TYPE_CHAR) {
                            ASSERT_EQ(*(string*)result,
                                      reinterpret_cast<const Slice*>(column->raw_data())[j].to_string())
                                    << "j:" << j;
                        } else if (type == TYPE_VARCHAR || type == TYPE_HLL || type == TYPE_OBJECT) {
                            ASSERT_EQ(value, reinterpret_cast<const Slice*>(column->raw_data())[j].to_string());
                        } else {
                            ASSERT_EQ(*(Type*)result, reinterpret_cast<const Type*>(column->raw_data())[j]);
                        }
                    }
                }
            }
        }
    }

    template <uint32_t version>
    void test_int_array(const std::string& null_encoding = "0") {
        config::set_config("null_encoding", null_encoding);
        auto fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(fs->create_dir(TEST_DIR).ok());

        TabletColumn array_column = create_array(0, true, sizeof(Collection));
        TabletColumn int_column = create_int_value(0, STORAGE_AGGREGATE_NONE, true);
        array_column.add_sub_column(int_column);

        auto src_offsets = UInt32Column::create();
        auto src_elements = NullableColumn::create(Int32Column::create(), NullColumn::create());
        auto src_column = ArrayColumn::create(src_elements, src_offsets);

        // insert [1, 2, 3], [4, 5, 6]
        src_elements->append_datum(1);
        src_elements->append_datum(2);
        src_elements->append_datum(3);
        src_offsets->append(3);

        src_elements->append_datum(4);
        src_elements->append_datum(5);
        src_elements->append_datum(6);
        src_offsets->append(6);

        TypeInfoPtr type_info = get_type_info(array_column);
        ColumnMetaPB meta;

        // delete test file.
        const std::string fname = TEST_DIR + "/test_array_int.data";
        auto segment = create_dummy_segment(fs, fname);
        // write data
        {
            ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(fname));

            ColumnWriterOptions writer_opts;
            writer_opts.page_format = version;
            writer_opts.meta = &meta;
            writer_opts.meta->set_column_id(0);
            writer_opts.meta->set_unique_id(0);
            writer_opts.meta->set_type(TYPE_ARRAY);
            writer_opts.meta->set_length(0);
            writer_opts.meta->set_encoding(DEFAULT_ENCODING);
            writer_opts.meta->set_compression(starrocks::LZ4_FRAME);
            writer_opts.meta->set_is_nullable(false);
            writer_opts.need_zone_map = false;

            // init integer sub column
            ColumnMetaPB* element_meta = writer_opts.meta->add_children_columns();
            element_meta->set_column_id(0);
            element_meta->set_unique_id(0);
            element_meta->set_type(int_column.type());
            element_meta->set_length(int_column.length());
            element_meta->set_encoding(DEFAULT_ENCODING);
            element_meta->set_compression(LZ4_FRAME);
            element_meta->set_is_nullable(false);

            ASSIGN_OR_ABORT(auto writer, ColumnWriter::create(writer_opts, &array_column, wfile.get()));
            ASSERT_OK(writer->init());

            ASSERT_TRUE(writer->append(*src_column).ok());

            ASSERT_TRUE(writer->finish().ok());
            ASSERT_TRUE(writer->write_data().ok());
            ASSERT_TRUE(writer->write_ordinal_index().ok());

            // close the file
            ASSERT_TRUE(wfile->close().ok());
        }

        // read and check
        {
            auto res = ColumnReader::create(&meta, segment.get(), nullptr);
            ASSERT_TRUE(res.ok());
            auto reader = std::move(res).value();

            ASSIGN_OR_ABORT(auto iter, reader->new_iterator());
            ASSIGN_OR_ABORT(auto read_file, fs->new_random_access_file(fname));

            ColumnIteratorOptions iter_opts;
            OlapReaderStatistics stats;
            iter_opts.stats = &stats;
            iter_opts.read_file = read_file.get();
            ASSERT_TRUE(iter->init(iter_opts).ok());

            // sequence read
            {
                auto st = iter->seek_to_first();
                ASSERT_TRUE(st.ok()) << st.to_string();

                auto dst_offsets = UInt32Column::create();
                auto dst_elements = NullableColumn::create(Int32Column::create(), NullColumn::create());
                auto dst_column = ArrayColumn::create(std::move(dst_elements), std::move(dst_offsets));
                size_t rows_read = src_column->size();
                st = iter->next_batch(&rows_read, dst_column.get());
                ASSERT_TRUE(st.ok());
                ASSERT_EQ(src_column->size(), rows_read);

                ASSERT_EQ("[1,2,3]", dst_column->debug_item(0));
                ASSERT_EQ("[4,5,6]", dst_column->debug_item(1));
            }

            ASSERT_EQ(2, meta.num_rows());
            ASSERT_EQ(42, reader->total_mem_footprint());
        }
    }

    template <LogicalType type>
    MutableColumnPtr numeric_data(int null_ratio) {
        using CppType = typename CppTypeTraits<type>::CppType;
        auto col = ChunkHelper::column_from_field_type(type, true);
        CppType value = 0;
        size_t count = 2 * 1024 / sizeof(CppType);
        col->reserve(count);
        for (size_t i = 0; i < count; ++i) {
            (void)col->append_numbers(&value, sizeof(CppType));
            value = value + 1;
        }
        for (size_t i = 0; i < count; i += null_ratio) {
            ((CppType*)col->raw_data())[i] = 0;
            (void)col->set_null(i);
        }
        return col;
    }

    MutableColumnPtr low_cardinality_strings(int null_ratio) {
        static std::string s1(4, 'a');
        static std::string s2(4, 'b');
        size_t count = 128 * 1024 / 4;
        auto col = ChunkHelper::column_from_field_type(TYPE_VARCHAR, true);
        auto nc = down_cast<NullableColumn*>(col.get());
        nc->reserve(count);
        down_cast<BinaryColumn*>(nc->data_column_raw_ptr())->get_data().reserve(s1.size() * count);
        auto v = std::vector<Slice>{s1, s2, s1, s1, s2, s2, s1, s2, s1, s1, s2, s1, s2, s1, s1, s1};
        for (size_t i = 0; i < count; i += 16) {
            CHECK(col->append_strings(v));
        }
        CHECK_EQ(count, col->size());
        for (size_t i = 0; i < count; i += null_ratio) {
            ((Slice*)col->raw_data())[i] = Slice();
            CHECK(col->set_null(i));
        }
        return col;
    }

    MutableColumnPtr high_cardinality_strings(int null_ratio) {
        std::string s1("abcdefghijklmnopqrstuvwxyz");
        std::string s2("bbcdefghijklmnopqrstuvwxyz");
        std::string s3("cbcdefghijklmnopqrstuvwxyz");
        std::string s4("dbcdefghijklmnopqrstuvwxyz");
        std::string s5("ebcdefghijklmnopqrstuvwxyz");
        std::string s6("fbcdefghijklmnopqrstuvwxyz");
        std::string s7("gbcdefghijklmnopqrstuvwxyz");
        std::string s8("hbcdefghijklmnopqrstuvwxyz");

        auto col = ChunkHelper::column_from_field_type(TYPE_VARCHAR, true);
        size_t count = (128 * 1024 / s1.size()) / 8 * 8;
        auto nc = down_cast<NullableColumn*>(col.get());
        nc->reserve(count);
        down_cast<BinaryColumn*>(nc->data_column_raw_ptr())->get_data().reserve(count * s1.size());
        for (size_t i = 0; i < count; i += 8) {
            (void)col->append_strings(std::vector<Slice>{s1, s2, s3, s4, s5, s6, s7, s8});

            std::next_permutation(s1.begin(), s1.end());
            std::next_permutation(s2.begin(), s2.end());
            std::next_permutation(s3.begin(), s3.end());
            std::next_permutation(s4.begin(), s4.end());
            std::next_permutation(s5.begin(), s5.end());
            std::next_permutation(s6.begin(), s6.end());
            std::next_permutation(s7.begin(), s7.end());
            std::next_permutation(s8.begin(), s8.end());
        }
        CHECK_EQ(count, col->size());
        for (size_t i = 0; i < count; i += null_ratio) {
            ((Slice*)col->raw_data())[i] = Slice();
            CHECK(col->set_null(i));
        }
        return col;
    }

    MutableColumnPtr date_values(int null_ratio) {
        size_t count = 4 * 1024 / sizeof(DateValue);
        auto col = ChunkHelper::column_from_field_type(TYPE_DATE, true);
        DateValue value = DateValue::create(2020, 10, 1);
        for (size_t i = 0; i < count; i++) {
            CHECK_EQ(1, col->append_numbers(&value, sizeof(value)));
            value = value.add<TimeUnit::DAY>(1);
        }
        for (size_t i = 0; i < count; i += null_ratio) {
            ((DateValue*)col->raw_data())[i] = DateValue{0};
            CHECK(col->set_null(i));
        }
        return col;
    }

    MutableColumnPtr datetime_values(int null_ratio) {
        size_t count = 4 * 1024 / sizeof(TimestampValue);
        auto col = ChunkHelper::column_from_field_type(TYPE_DATETIME, true);
        TimestampValue value = TimestampValue::create(2020, 10, 1, 10, 20, 1);
        for (size_t i = 0; i < count; i++) {
            CHECK_EQ(1, col->append_numbers(&value, sizeof(value)));
            value = value.add<TimeUnit::MICROSECOND>(1);
        }
        for (size_t i = 0; i < count; i += null_ratio) {
            ((TimestampValue*)col->raw_data())[i] = TimestampValue{0};
            CHECK(col->set_null(i));
        }
        return col;
    }

    template <LogicalType type>
    void test_numeric_types() {
        auto col = numeric_data<type>(1);
        test_nullable_data<type, BIT_SHUFFLE, 1>(*col, "0", "1");
        test_nullable_data<type, BIT_SHUFFLE, 2>(*col, "0", "1");
        test_nullable_data<type, BIT_SHUFFLE, 2>(*col, "1", "1");

        col = numeric_data<type>(4);
        test_nullable_data<type, BIT_SHUFFLE, 1>(*col, "0", "4");
        test_nullable_data<type, BIT_SHUFFLE, 2>(*col, "0", "4");
        test_nullable_data<type, BIT_SHUFFLE, 2>(*col, "1", "4");

        col = numeric_data<type>(10000);
        test_nullable_data<type, BIT_SHUFFLE, 1>(*col, "0", "10000");
        test_nullable_data<type, BIT_SHUFFLE, 2>(*col, "0", "10000");
        test_nullable_data<type, BIT_SHUFFLE, 2>(*col, "1", "10000");
    }

    MemPool _pool;
    std::shared_ptr<TabletSchema> _dummy_segment_schema;
};

// NOLINTNEXTLINE
TEST_F(ColumnReaderWriterTest, test_int) {
    test_numeric_types<TYPE_INT>();
}

// NOLINTNEXTLINE
TEST_F(ColumnReaderWriterTest, test_double) {
    test_numeric_types<TYPE_DOUBLE>();
}

// NOLINTNEXTLINE
TEST_F(ColumnReaderWriterTest, test_date) {
    auto col = date_values(100);
    test_nullable_data<TYPE_DATE, BIT_SHUFFLE, 1>(*col, "0", "100");
    test_nullable_data<TYPE_DATE, BIT_SHUFFLE, 2>(*col, "0", "100");
    test_nullable_data<TYPE_DATE, BIT_SHUFFLE, 2>(*col, "1", "100");
}

// NOLINTNEXTLINE
TEST_F(ColumnReaderWriterTest, test_datetime) {
    auto col = datetime_values(100);
    test_nullable_data<TYPE_DATETIME, BIT_SHUFFLE, 1>(*col, "0", "100");
    test_nullable_data<TYPE_DATETIME, BIT_SHUFFLE, 2>(*col, "0", "100");
    test_nullable_data<TYPE_DATETIME, BIT_SHUFFLE, 2>(*col, "1", "100");
}

// NOLINTNEXTLINE
TEST_F(ColumnReaderWriterTest, test_binary) {
    auto c = low_cardinality_strings(10000);
    test_nullable_data<TYPE_VARCHAR, DICT_ENCODING, 1>(*c, "0", "10000");
    test_nullable_data<TYPE_VARCHAR, DICT_ENCODING, 2>(*c, "0", "10000");
    test_nullable_data<TYPE_VARCHAR, DICT_ENCODING, 2>(*c, "1", "10000");

    test_nullable_data<TYPE_CHAR, DICT_ENCODING, 1>(*c, "0", "10000");
    test_nullable_data<TYPE_CHAR, DICT_ENCODING, 2>(*c, "0", "10000");
    test_nullable_data<TYPE_CHAR, DICT_ENCODING, 2>(*c, "1", "10000");

    c = high_cardinality_strings(100);
    test_nullable_data<TYPE_VARCHAR, DICT_ENCODING, 1>(*c, "0", "100");
    test_nullable_data<TYPE_VARCHAR, DICT_ENCODING, 2>(*c, "0", "100");
    test_nullable_data<TYPE_VARCHAR, DICT_ENCODING, 2>(*c, "1", "100");

    test_nullable_data<TYPE_CHAR, DICT_ENCODING, 1>(*c, "0", "100");
    test_nullable_data<TYPE_CHAR, DICT_ENCODING, 2>(*c, "0", "100");
    test_nullable_data<TYPE_CHAR, DICT_ENCODING, 2>(*c, "1", "100");
}

#ifdef STRING_COLUMN_WRITER_TEST
// NOLINTNEXTLINE
TEST_F(ColumnReaderWriterTest, test_string_column_writer_benchmark) {
    for (int i = 0; i < 10000; i++) {
        auto c = high_cardinality_strings(100000);
        test_nullable_data<TYPE_VARCHAR, PLAIN_ENCODING, 1>(*c, "0", "10000");
    }
}
#endif

// NOLINTNEXTLINE
TEST_F(ColumnReaderWriterTest, test_default_value) {
    std::string v_int("1");
    int32_t result = 1;
    test_read_default_value<TYPE_TINYINT>(v_int, &result);
    test_read_default_value<TYPE_SMALLINT>(v_int, &result);
    test_read_default_value<TYPE_INT>(v_int, &result);

    std::string v_bigint("9223372036854775807");
    int64_t result_bigint = std::numeric_limits<int64_t>::max();
    test_read_default_value<TYPE_BIGINT>(v_bigint, &result_bigint);
    int128_t result_largeint = std::numeric_limits<int64_t>::max();
    test_read_default_value<TYPE_LARGEINT>(v_bigint, &result_largeint);

    std::string v_float("1.00");
    float result2 = 1.00;
    test_read_default_value<TYPE_FLOAT>(v_float, &result2);

    std::string v_double("1.00");
    double result3 = 1.00;
    test_read_default_value<TYPE_DOUBLE>(v_double, &result3);

    std::string v_varchar("varchar");
    test_read_default_value<TYPE_VARCHAR>(v_varchar, &v_varchar);

    std::string v_char("char");
    test_read_default_value<TYPE_CHAR>(v_char, &v_char);

    char* c = (char*)malloc(1);
    c[0] = 0;
    std::string v_object(c, 1);
    test_read_default_value<TYPE_HLL>(v_object, &v_object);
    test_read_default_value<TYPE_OBJECT>(v_object, &v_object);
    free(c);

    std::string v_date("2019-11-12");
    uint24_t result_date(1034092);
    test_read_default_value<TYPE_DATE_V1>(v_date, &result_date);

    std::string v_datetime("2019-11-12 12:01:08");
    int64_t result_datetime = 20191112120108;
    test_read_default_value<TYPE_DATETIME_V1>(v_datetime, &result_datetime);

    std::string v_decimal("102418.000000002");
    decimal12_t decimal(102418, 2);
    test_read_default_value<TYPE_DECIMAL>(v_decimal, &decimal);
}

// test array<int>, and nullable
TEST_F(ColumnReaderWriterTest, test_array_int) {
    test_int_array<2>();
    test_int_array<2>("1");
}

// Covers ArrayColumnIterator::fetch_values_by_rowid with an offsets-only access
// path (_access_values == false): only array sizes are materialized, element
// values are never read. This locks the semantics: returned offsets must match
// the written array lengths, and the elements column stays a const default
// column sized by the sum of the fetched array sizes.
TEST_F(ColumnReaderWriterTest, test_array_fetch_by_rowid_offsets_only) {
    auto fs = std::make_shared<MemoryFileSystem>();
    ASSERT_TRUE(fs->create_dir(TEST_DIR).ok());

    TabletColumn array_column = create_array(0, true, sizeof(Collection));
    TabletColumn int_column = create_int_value(0, STORAGE_AGGREGATE_NONE, true);
    array_column.add_sub_column(int_column);

    // Enough rows that the element data spans multiple 64KB pages, so scattered
    // rowids land on different pages.
    const size_t kNumRows = 20000;

    auto src_offsets = UInt32Column::create();
    auto src_elements = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto src_column = ArrayColumn::create(src_elements, src_offsets);

    // Row i holds i % 5 elements, so array lengths vary and are predictable.
    uint32_t offset = 0;
    for (size_t i = 0; i < kNumRows; ++i) {
        size_t array_size = i % 5;
        for (size_t j = 0; j < array_size; ++j) {
            src_elements->append_datum(static_cast<int32_t>(i * 10 + j));
        }
        offset += array_size;
        src_offsets->append(offset);
    }

    ColumnMetaPB meta;
    const std::string fname = TEST_DIR + "/test_array_fetch_by_rowid_offsets_only.data";
    auto segment = create_dummy_segment(fs, fname);

    // write data
    {
        ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(fname));

        ColumnWriterOptions writer_opts;
        writer_opts.page_format = 2;
        writer_opts.meta = &meta;
        writer_opts.meta->set_column_id(0);
        writer_opts.meta->set_unique_id(0);
        writer_opts.meta->set_type(TYPE_ARRAY);
        writer_opts.meta->set_length(0);
        writer_opts.meta->set_encoding(DEFAULT_ENCODING);
        writer_opts.meta->set_compression(starrocks::LZ4_FRAME);
        writer_opts.meta->set_is_nullable(false);
        writer_opts.need_zone_map = false;

        // init integer sub column
        ColumnMetaPB* element_meta = writer_opts.meta->add_children_columns();
        element_meta->set_column_id(0);
        element_meta->set_unique_id(0);
        element_meta->set_type(int_column.type());
        element_meta->set_length(int_column.length());
        element_meta->set_encoding(DEFAULT_ENCODING);
        element_meta->set_compression(LZ4_FRAME);
        element_meta->set_is_nullable(false);

        ASSIGN_OR_ABORT(auto writer, ColumnWriter::create(writer_opts, &array_column, wfile.get()));
        ASSERT_OK(writer->init());
        ASSERT_TRUE(writer->append(*src_column).ok());
        ASSERT_TRUE(writer->finish().ok());
        ASSERT_TRUE(writer->write_data().ok());
        ASSERT_TRUE(writer->write_ordinal_index().ok());
        ASSERT_TRUE(wfile->close().ok());
    }

    // read back through an offsets-only access path (root with a single OFFSET
    // child), which makes ArrayColumnIterator::init set _access_values = false.
    {
        ASSIGN_OR_ABORT(auto child_path, ColumnAccessPath::create(TAccessPathType::type::OFFSET, "offsets", 1));
        ASSIGN_OR_ABORT(auto path, ColumnAccessPath::create(TAccessPathType::type::ROOT, "root", 0));
        path->children().emplace_back(std::move(child_path));

        ASSIGN_OR_ABORT(auto reader, ColumnReader::create(&meta, segment.get(), nullptr));
        ASSIGN_OR_ABORT(auto iter, reader->new_iterator(path.get()));
        ASSIGN_OR_ABORT(auto read_file, fs->new_random_access_file(fname));

        ColumnIteratorOptions iter_opts;
        OlapReaderStatistics stats;
        iter_opts.stats = &stats;
        iter_opts.read_file = read_file.get();
        iter_opts.use_page_cache = true;
        ASSERT_OK(iter->init(iter_opts));

        // scattered rowids: first row, one every 997 rows, and the last row.
        std::vector<uint32_t> rowids;
        for (size_t i = 0; i < kNumRows; i += 997) {
            rowids.push_back(i);
        }
        rowids.push_back(kNumRows - 1);

        auto dst_offsets = UInt32Column::create();
        auto dst_elements = NullableColumn::create(Int32Column::create(), NullColumn::create());
        auto dst_column = ArrayColumn::create(std::move(dst_elements), std::move(dst_offsets));
        ASSERT_OK(iter->fetch_values_by_rowid(rowids.data(), rowids.size(), dst_column.get()));
        ASSERT_EQ(rowids.size(), dst_column->size());

        // each fetched row must report the same array length as written.
        size_t expected_element_rows = 0;
        for (size_t i = 0; i < rowids.size(); ++i) {
            size_t expected_size = rowids[i] % 5;
            ASSERT_EQ(expected_size, dst_column->get_element_size(i)) << " row " << i << " rowid " << rowids[i];
            expected_element_rows += expected_size;
        }

        // element values are not read: the elements column is a const default
        // column whose row count equals the sum of the fetched array sizes.
        ASSERT_TRUE(dst_column->elements_column()->is_constant());
        ASSERT_EQ(expected_element_rows, dst_column->elements_column()->size());
    }
}

TEST_F(ColumnReaderWriterTest, test_scalar_column_total_mem_footprint) {
    auto col = ChunkHelper::column_from_field_type(TYPE_INT, true);
    size_t count = 1024;
    col->reserve(count);
    for (int32_t i = 0; i < count; ++i) {
        (void)col->append_numbers(&i, sizeof(int32_t));
    }
    for (size_t i = 0; i < count; i += 2) {
        ((int32_t*)col->raw_data())[i] = 0;
        (void)col->set_null(i);
    }

    ColumnMetaPB meta;
    auto fs = std::make_shared<MemoryFileSystem>();
    ASSERT_TRUE(fs->create_dir(TEST_DIR).ok());
    const std::string fname = strings::Substitute("$0/test_scalar_column_total_mem_footprint.data", TEST_DIR);
    auto segment = create_dummy_segment(fs, fname);

    // write data
    {
        ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(fname));

        ColumnWriterOptions writer_opts;
        writer_opts.page_format = 2;
        writer_opts.meta = &meta;
        writer_opts.meta->set_column_id(0);
        writer_opts.meta->set_unique_id(0);
        writer_opts.meta->set_type(TYPE_INT);
        writer_opts.meta->set_length(0);
        writer_opts.meta->set_encoding(BIT_SHUFFLE);
        writer_opts.meta->set_compression(starrocks::LZ4_FRAME);
        writer_opts.meta->set_is_nullable(true);
        writer_opts.need_zone_map = true;

        TabletColumn column(STORAGE_AGGREGATE_NONE, TYPE_INT);
        ASSIGN_OR_ABORT(auto writer, ColumnWriter::create(writer_opts, &column, wfile.get()));
        ASSERT_OK(writer->init());

        ASSERT_TRUE(writer->append(*col).ok());

        ASSERT_TRUE(writer->finish().ok());
        ASSERT_TRUE(writer->write_data().ok());
        ASSERT_TRUE(writer->write_ordinal_index().ok());
        ASSERT_TRUE(writer->write_zone_map().ok());

        // close the file
        ASSERT_TRUE(wfile->close().ok());
    }

    // read and check
    {
        // read and check
        auto res = ColumnReader::create(&meta, segment.get(), nullptr);
        ASSERT_TRUE(res.ok());
        auto reader = std::move(res).value();
        ASSERT_EQ(1024, meta.num_rows());
        ASSERT_EQ(1024 * 4 + 1024, reader->total_mem_footprint());
    }
}

TEST_F(ColumnReaderWriterTest, test_large_varchar_column_writer) {
    auto fs = std::make_shared<MemoryFileSystem>();
    ASSERT_TRUE(fs->create_dir(TEST_DIR).ok());
    const std::string fname = strings::Substitute("$0/test_large_varchar_column_writer.data", TEST_DIR);
    // write data
    {
        int32_t old_config = config::dictionary_speculate_min_chunk_size;
        const int TEST_N = 100;
        // Test 3 different dictionary_speculate_min_chunk_size.
        int32_t dict_chunk_sizes[3] = {TEST_N - 1, TEST_N, TEST_N + 1};
        for (int32_t dict_chunk_size : dict_chunk_sizes) {
            fs->delete_file(fname);
            ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(fname));
            ColumnMetaPB meta;
            ColumnWriterOptions writer_opts;
            writer_opts.page_format = 2;
            writer_opts.meta = &meta;
            writer_opts.meta->set_column_id(0);
            writer_opts.meta->set_unique_id(0);
            writer_opts.meta->set_type(TYPE_VARCHAR);
            writer_opts.meta->set_length(1024 * 1024);
            writer_opts.meta->set_encoding(PLAIN_ENCODING);
            writer_opts.meta->set_compression(starrocks::LZ4_FRAME);
            writer_opts.meta->set_is_nullable(true);
            writer_opts.need_zone_map = true;

            TabletColumn column(STORAGE_AGGREGATE_NONE, TYPE_VARCHAR);
            column = create_varchar_key(1, true, 1024 * 1024);
            ASSIGN_OR_ABORT(auto writer, ColumnWriter::create(writer_opts, &column, wfile.get()));
            ASSERT_OK(writer->init());
            config::dictionary_speculate_min_chunk_size = dict_chunk_size;
            auto col = ChunkHelper::column_from_field_type(TYPE_VARCHAR, true);
            config::dictionary_speculate_min_chunk_size = 4096;
            std::vector<Slice> col_slices;
            std::vector<std::string> col_strs;
            col_strs.resize(TEST_N);
            for (int i = 0; i < TEST_N; i++) {
                col_strs[i] = strings::Substitute("test_$0", i);
                col_slices.emplace_back(col_strs[i]);
            }
            col->reserve(TEST_N);
            col->append_strings(col_slices);
            ASSERT_TRUE(writer->append(*col).ok());

            ASSERT_TRUE(writer->finish().ok());
            ASSERT_TRUE(writer->write_data().ok());
            ASSERT_TRUE(writer->write_ordinal_index().ok());
            ASSERT_TRUE(writer->write_zone_map().ok());

            // close the file
            ASSERT_TRUE(wfile->close().ok());
            // read and check result
            auto segment = create_dummy_segment(fs, fname);
            auto res = ColumnReader::create(&meta, segment.get(), nullptr);
            ASSERT_TRUE(res.ok());
            auto reader = std::move(res).value();
            ASSIGN_OR_ABORT(auto iter, reader->new_iterator());
            ASSIGN_OR_ABORT(auto read_file, fs->new_random_access_file(fname));
            ColumnIteratorOptions iter_opts;
            OlapReaderStatistics stats;
            iter_opts.stats = &stats;
            iter_opts.read_file = read_file.get();
            iter_opts.use_page_cache = true;
            auto st = iter->init(iter_opts);
            ASSERT_TRUE(st.ok());
            ASSERT_TRUE(iter->seek_to_first().ok());
            MutableColumnPtr dst = ChunkHelper::column_from_field_type(TYPE_VARCHAR, true);
            dst->reserve(TEST_N);
            size_t rows_read = TEST_N;
            ASSERT_TRUE(iter->next_batch(&rows_read, dst.get()).ok());
            ASSERT_EQ(dst->size(), TEST_N);

            TypeInfoPtr type_info = get_type_info(TYPE_VARCHAR);
            for (size_t i = 0; i < TEST_N; i++) {
                ASSERT_EQ(0, type_info->cmp(col->get(i), dst->get(i)))
                        << " row " << i << ": " << datum_to_string(type_info.get(), col->get(i)) << " vs "
                        << datum_to_string(type_info.get(), dst->get(i));
            }
        }
        config::dictionary_speculate_min_chunk_size = old_config;
    }
}

// End-to-end read of a column that carries a compression dictionary.
//
// This build has no writer for such a column -- that is the point of splitting the
// read support out -- so the column file is assembled here the way the writer will:
// a DICTIONARY_PAGE holding a raw sample taken from the first data page, and every
// page after it compressed against that sample. Reading it back through
// ColumnReader is what exercises the dictionary load and the per-page reference,
// and asserting every value round-trips is what proves they are right.
TEST_F(ColumnReaderWriterTest, read_column_with_compression_dictionary) {
    const std::string fname = strings::Substitute("$0/read_with_dict.data", TEST_DIR);
    const int kRowsPerPage = 400;
    const int kPages = 6;
    const int N = kRowsPerPage * kPages;

    // Rows that share a long scaffolding, so a dictionary taken from the first page
    // actually helps the ones after it.
    std::vector<std::string> values(N);
    for (int i = 0; i < N; i++) {
        values[i] = strings::Substitute(
                R"({"role":"assistant","trace":"trace_0001","parts":[{"type":"text","content":"row $0 of a )"
                R"(replayed conversation whose scaffolding repeats verbatim across every row"}]})",
                i);
    }

    const BlockCompressionCodec* codec = nullptr;
    ASSERT_OK(get_block_compression_codec(CompressionTypePB::ZSTD, &codec));

    auto fs = std::make_shared<MemoryFileSystem>();
    ASSERT_TRUE(fs->create_dir(TEST_DIR).ok());

    ColumnMetaPB meta;
    meta.set_column_id(0);
    meta.set_unique_id(0);
    meta.set_type(TYPE_VARCHAR);
    meta.set_length(1024);
    meta.set_encoding(PLAIN_ENCODING);
    meta.set_compression(CompressionTypePB::ZSTD);
    meta.set_compression_level(-1);
    meta.set_is_nullable(false);
    meta.set_num_rows(N);

    {
        ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(fname));

        // Encode each group of rows into a plain page body, exactly as the writer's
        // page builder would.
        PageBuilderOptions page_opts;
        page_opts.data_page_size = 1024 * 1024; // one page per group, no early flush
        std::vector<std::string> page_bodies;
        for (int p = 0; p < kPages; p++) {
            BinaryPlainPageBuilder builder(page_opts);
            for (int i = 0; i < kRowsPerPage; i++) {
                Slice v(values[p * kRowsPerPage + i]);
                ASSERT_EQ(1u, builder.add(reinterpret_cast<const uint8_t*>(&v), 1));
            }
            faststring* body = builder.finish();
            page_bodies.emplace_back(reinterpret_cast<const char*>(body->data()), body->size());
        }

        // The dictionary is the first page's encoded values, verbatim -- raw content,
        // which is why it must never be parsed as a structured dictionary.
        const std::string& sample = page_bodies.front();
        auto cdict = compression::ZstdCDict::create(Slice(sample), -1);
        ASSERT_TRUE(cdict.ok()) << cdict.status();

        PagePointer dict_pp;
        {
            PageFooterPB footer;
            footer.set_type(DICTIONARY_PAGE);
            footer.set_uncompressed_size(sample.size());
            footer.mutable_dict_page_footer()->set_encoding(PLAIN_ENCODING);
            std::vector<Slice> body{Slice(sample)};
            ASSERT_OK(PageIO::compress_and_write_page(codec, 0.1, wfile.get(), body, footer, &dict_pp));
        }

        OrdinalIndexWriter ordinal_index;
        for (int p = 0; p < kPages; p++) {
            PageFooterPB footer;
            footer.set_type(DATA_PAGE);
            footer.set_uncompressed_size(page_bodies[p].size());
            auto* data_footer = footer.mutable_data_page_footer();
            data_footer->set_first_ordinal(p * kRowsPerPage);
            data_footer->set_num_values(kRowsPerPage);
            data_footer->set_nullmap_size(0);
            data_footer->set_format_version(2);

            // Every page but the sample is compressed against the dictionary; the
            // sample page goes out plain, and the reader has to handle both.
            std::string compressed;
            std::vector<Slice> body{Slice(page_bodies[p])};
            if (p == 0) {
                faststring plain;
                ASSERT_OK(PageIO::compress_page_body(codec, 0.1, body, &plain));
                compressed.assign(reinterpret_cast<const char*>(plain.data()), plain.size());
            } else {
                compressed.resize(codec->max_compressed_len(page_bodies[p].size()));
                Slice out(compressed);
                ASSERT_OK(codec->compress(body, &out, false, page_bodies[p].size(), nullptr, nullptr,
                                          cdict.value().get()));
                compressed.resize(out.size);
                ASSERT_LT(compressed.size(), page_bodies[p].size());
            }
            PagePointer data_pp;
            std::vector<Slice> to_write{compressed.empty() ? body[0] : Slice(compressed)};
            ASSERT_OK(PageIO::write_page(wfile.get(), to_write, footer, &data_pp));
            ordinal_index.append_entry(p * kRowsPerPage, data_pp);
        }
        ASSERT_OK(ordinal_index.finish(wfile.get(), meta.add_indexes()));
        dict_pp.to_proto(meta.mutable_zstd_compression_dict_page());
        ASSERT_OK(wfile->close());
    }

    // The dictionary page must be recorded, or the read below would be testing
    // nothing in particular.
    ASSERT_TRUE(meta.has_zstd_compression_dict_page());
    ASSERT_GT(meta.zstd_compression_dict_page().size(), 0u);

    auto segment = create_dummy_segment(fs, fname);
    ASSIGN_OR_ABORT(auto reader, ColumnReader::create(&meta, segment.get(), nullptr));
    ASSIGN_OR_ABORT(auto iter, reader->new_iterator());
    ASSIGN_OR_ABORT(auto read_file, fs->new_random_access_file(fname));
    ColumnIteratorOptions iter_opts;
    OlapReaderStatistics stats;
    iter_opts.stats = &stats;
    iter_opts.read_file = read_file.get();
    iter_opts.use_page_cache = false;
    ASSERT_OK(iter->init(iter_opts));

    // Scan: every value has to come back byte for byte, including the plain first
    // page and the dictionary-compressed ones after it.
    ASSERT_OK(iter->seek_to_first());
    auto dst = ChunkHelper::column_from_field_type(TYPE_VARCHAR, false);
    size_t remaining = N;
    while (remaining > 0) {
        size_t n = std::min<size_t>(128, remaining);
        ASSERT_OK(iter->next_batch(&n, dst.get()));
        ASSERT_GT(n, 0u);
        remaining -= n;
    }
    ASSERT_EQ(N, dst->size());
    for (int i = 0; i < N; i++) {
        ASSERT_EQ(values[i], dst->get(i).get_slice().to_string()) << "row " << i;
    }

    // And a seek straight into a dictionary-compressed page, which is the path a
    // point lookup takes.
    ASSERT_OK(iter->seek_to_ordinal(kRowsPerPage * 3 + 7));
    auto one = ChunkHelper::column_from_field_type(TYPE_VARCHAR, false);
    size_t one_row = 1;
    ASSERT_OK(iter->next_batch(&one_row, one.get()));
    ASSERT_EQ(1, one->size());
    ASSERT_EQ(values[kRowsPerPage * 3 + 7], one->get(0).get_slice().to_string());
}

} // namespace starrocks
