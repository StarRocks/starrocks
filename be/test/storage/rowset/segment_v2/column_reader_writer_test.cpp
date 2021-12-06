// This file is made available under Elastic License 2.0.
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

#include <iostream>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column.h"
#include "column/datum_convert.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "env/env_memory.h"
#include "gen_cpp/segment_v2.pb.h"
#include "runtime/date_value.h"
#include "runtime/mem_pool.h"
#include "storage/column_block.h"
#include "storage/decimal12.h"
#include "storage/field.h"
#include "storage/fs/file_block_manager.h"
#include "storage/olap_common.h"
#include "storage/rowset/segment_v2/column_reader.h"
#include "storage/rowset/segment_v2/column_writer.h"
#include "storage/rowset/segment_v2/default_value_column_iterator.h"
#include "storage/rowset/segment_v2/scalar_column_iterator.h"
#include "storage/tablet_schema_helper.h"
#include "storage/types.h"
#include "storage/vectorized/chunk_helper.h"

using std::string;

namespace starrocks::segment_v2 {

// NOLINTNEXTLINE
static const std::string TEST_DIR = "/column_reader_writer_test";

class ColumnReaderWriterTest : public testing::Test {
public:
    ColumnReaderWriterTest() {}

    ~ColumnReaderWriterTest() override = default;

protected:
    void SetUp() override { _tablet_meta_mem_tracker = std::make_unique<MemTracker>(); }

    void TearDown() override {}

    template <FieldType type, EncodingTypePB encoding, uint32_t version, bool adaptive = true>
    void test_nullable_data(const vectorized::Column& src, const std::string null_encoding = "0") {
        config::set_config("null_encoding", null_encoding);

        using Type = typename TypeTraits<type>::CppType;
        TypeInfoPtr type_info = get_type_info(type);
        int num_rows = src.size();
        ColumnMetaPB meta;

        auto env = std::make_unique<EnvMemory>();
        auto block_mgr = std::make_unique<fs::FileBlockManager>(env.get(), fs::BlockManagerOptions());
        ASSERT_TRUE(env->create_dir(TEST_DIR).ok());

        const std::string fname =
                strings::Substitute("$0/test-$1-$2-$3-$4.data", TEST_DIR, type, encoding, version, adaptive);
        // write data
        {
            std::unique_ptr<fs::WritableBlock> wblock;
            fs::CreateBlockOptions opts({fname});
            Status st = block_mgr->create_block(opts, &wblock);
            ASSERT_TRUE(st.ok()) << st.to_string();

            ColumnWriterOptions writer_opts;
            writer_opts.page_format = version;
            writer_opts.meta = &meta;
            writer_opts.meta->set_column_id(0);
            writer_opts.meta->set_unique_id(0);
            writer_opts.meta->set_type(type);
            writer_opts.adaptive_page_format = adaptive;
            if (type == OLAP_FIELD_TYPE_CHAR || type == OLAP_FIELD_TYPE_VARCHAR) {
                writer_opts.meta->set_length(128);
            } else {
                writer_opts.meta->set_length(0);
            }
            writer_opts.meta->set_encoding(encoding);
            writer_opts.meta->set_compression(starrocks::LZ4_FRAME);
            writer_opts.meta->set_is_nullable(true);
            writer_opts.need_zone_map = true;

            TabletColumn column(OLAP_FIELD_AGGREGATION_NONE, type);
            if (type == OLAP_FIELD_TYPE_VARCHAR) {
                column = create_varchar_key(1, true, 128);
            } else if (type == OLAP_FIELD_TYPE_CHAR) {
                column = create_char_key(1, true, 128);
            }
            std::unique_ptr<ColumnWriter> writer;
            ColumnWriter::create(writer_opts, &column, wblock.get(), &writer);
            st = writer->init();
            ASSERT_TRUE(st.ok()) << st.to_string();

            ASSERT_TRUE(writer->append(src).ok());

            ASSERT_TRUE(writer->finish().ok());
            ASSERT_TRUE(writer->write_data().ok());
            ASSERT_TRUE(writer->write_ordinal_index().ok());
            ASSERT_TRUE(writer->write_zone_map().ok());

            // close the file
            ASSERT_TRUE(wblock->close().ok());
            std::cout << "version=" << version << ", bytes append: " << wblock->bytes_appended() << "\n";
        }
        // read and check
        {
            // read and check
            ColumnReaderOptions reader_opts;
            reader_opts.storage_format_version = version;
            reader_opts.block_mgr = block_mgr.get();
            auto res = ColumnReader::create(_tablet_meta_mem_tracker.get(), reader_opts, &meta, fname);
            ASSERT_TRUE(res.ok());
            auto reader = std::move(res).value();

            ColumnIterator* iter = nullptr;
            auto st = reader->new_iterator(&iter);
            ASSERT_TRUE(st.ok());
            std::unique_ptr<ColumnIterator> guard(iter);
            std::unique_ptr<fs::ReadableBlock> rblock;
            block_mgr->open_block(fname, &rblock);

            ASSERT_TRUE(st.ok());
            ColumnIteratorOptions iter_opts;
            OlapReaderStatistics stats;
            iter_opts.stats = &stats;
            iter_opts.rblock = rblock.get();
            st = iter->init(iter_opts);
            ASSERT_TRUE(st.ok());
            // sequence read
            {
                st = iter->seek_to_first();
                ASSERT_TRUE(st.ok()) << st.to_string();

                vectorized::ColumnPtr dst = vectorized::ChunkHelper::column_from_field_type(type, true);
                size_t rows_read = src.size();
                st = iter->next_batch(&rows_read, dst.get());
                ASSERT_TRUE(st.ok());
                ASSERT_EQ(src.size(), rows_read);

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
                    vectorized::ColumnPtr dst = vectorized::ChunkHelper::column_from_field_type(type, true);

                    st = iter->next_batch(&rows_read, dst.get());
                    ASSERT_TRUE(st.ok());
                    for (int i = 0; i < rows_read; ++i) {
                        ASSERT_EQ(0, type_info->cmp(src.get(rowid + i), dst->get(i)))
                                << " row " << rowid + i << ": " << datum_to_string(type_info.get(), src.get(rowid + i))
                                << " vs " << datum_to_string(type_info.get(), dst->get(i));
                    }
                }
            }
        }
    }

    template <FieldType type>
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

                MemPool pool;
                std::unique_ptr<ColumnVectorBatch> cvb;
                ColumnVectorBatch::create(0, true, type_info, nullptr, &cvb);
                cvb->resize(1024);
                ColumnBlock col(cvb.get(), &pool);

                int idx = 0;
                size_t rows_read = 1024;
                ColumnBlockView dst(&col);
                bool has_null;
                st = iter.next_batch(&rows_read, &dst, &has_null);
                ASSERT_TRUE(st.ok());
                for (int j = 0; j < rows_read; ++j) {
                    if (type == OLAP_FIELD_TYPE_CHAR) {
                        ASSERT_EQ(*(string*)result, reinterpret_cast<const Slice*>(col.cell_ptr(j))->to_string())
                                << "j:" << j;
                    } else if (type == OLAP_FIELD_TYPE_VARCHAR || type == OLAP_FIELD_TYPE_HLL ||
                               type == OLAP_FIELD_TYPE_OBJECT) {
                        ASSERT_EQ(value, reinterpret_cast<const Slice*>(col.cell_ptr(j))->to_string()) << "j:" << j;
                    } else {
                        ASSERT_EQ(*(Type*)result, *(reinterpret_cast<const Type*>(col.cell_ptr(j))));
                    }
                    idx++;
                }
            }

            {
                MemPool pool;
                std::unique_ptr<ColumnVectorBatch> cvb;
                ColumnVectorBatch::create(0, true, type_info, nullptr, &cvb);
                cvb->resize(1024);
                ColumnBlock col(cvb.get(), &pool);

                for (int rowid = 0; rowid < 2048; rowid += 128) {
                    st = iter.seek_to_ordinal(rowid);
                    ASSERT_TRUE(st.ok());

                    int idx = rowid;
                    size_t rows_read = 1024;
                    ColumnBlockView dst(&col);
                    bool has_null;
                    st = iter.next_batch(&rows_read, &dst, &has_null);
                    ASSERT_TRUE(st.ok());
                    for (int j = 0; j < rows_read; ++j) {
                        if (type == OLAP_FIELD_TYPE_CHAR) {
                            ASSERT_EQ(*(string*)result, reinterpret_cast<const Slice*>(col.cell_ptr(j))->to_string())
                                    << "j:" << j;
                        } else if (type == OLAP_FIELD_TYPE_VARCHAR || type == OLAP_FIELD_TYPE_HLL ||
                                   type == OLAP_FIELD_TYPE_OBJECT) {
                            ASSERT_EQ(value, reinterpret_cast<const Slice*>(col.cell_ptr(j))->to_string());
                        } else {
                            ASSERT_EQ(*(Type*)result, *(reinterpret_cast<const Type*>(col.cell_ptr(j))));
                        }
                        idx++;
                    }
                }
            }
        }
    }

    template <uint32_t version>
    void test_int_array(std::string null_encoding = "0") {
        config::set_config("null_encoding", null_encoding);
        auto env = std::make_unique<EnvMemory>();
        auto block_mgr = std::make_unique<fs::FileBlockManager>(env.get(), fs::BlockManagerOptions());
        ASSERT_TRUE(env->create_dir(TEST_DIR).ok());

        TabletColumn array_column = create_array(0, true, sizeof(Collection));
        TabletColumn int_column = create_int_value(0, OLAP_FIELD_AGGREGATION_NONE, true);
        array_column.add_sub_column(int_column);

        auto src_offsets = vectorized::UInt32Column::create();
        auto src_elements = vectorized::Int32Column::create();
        vectorized::ColumnPtr src_column = vectorized::ArrayColumn::create(src_elements, src_offsets);

        // insert [1, 2, 3], [4, 5, 6]
        src_elements->append(1);
        src_elements->append(2);
        src_elements->append(3);
        src_offsets->append(3);

        src_elements->append(4);
        src_elements->append(5);
        src_elements->append(6);
        src_offsets->append(6);

        TypeInfoPtr type_info = get_type_info(array_column);
        ColumnMetaPB meta;

        // delete test file.
        const std::string fname = TEST_DIR + "/test_array_int.data";
        // write data
        {
            std::unique_ptr<fs::WritableBlock> wblock;
            fs::CreateBlockOptions opts({fname});
            Status st = block_mgr->create_block(opts, &wblock);
            ASSERT_TRUE(st.ok()) << st.get_error_msg();

            ColumnWriterOptions writer_opts;
            writer_opts.page_format = version;
            writer_opts.meta = &meta;
            writer_opts.meta->set_column_id(0);
            writer_opts.meta->set_unique_id(0);
            writer_opts.meta->set_type(OLAP_FIELD_TYPE_ARRAY);
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

            std::unique_ptr<ColumnWriter> writer;
            ColumnWriter::create(writer_opts, &array_column, wblock.get(), &writer);
            st = writer->init();
            ASSERT_TRUE(st.ok()) << st.to_string();

            ASSERT_TRUE(writer->append(*src_column).ok());

            ASSERT_TRUE(writer->finish().ok());
            ASSERT_TRUE(writer->write_data().ok());
            ASSERT_TRUE(writer->write_ordinal_index().ok());

            // close the file
            ASSERT_TRUE(wblock->close().ok());
        }

        // read and check
        {
            ColumnReaderOptions reader_opts;
            reader_opts.block_mgr = block_mgr.get();
            reader_opts.storage_format_version = 2;
            auto res = ColumnReader::create(_tablet_meta_mem_tracker.get(), reader_opts, &meta, fname);
            ASSERT_TRUE(res.ok());
            auto reader = std::move(res).value();

            ColumnIterator* iter = nullptr;
            ASSERT_TRUE(reader->new_iterator(&iter).ok());
            std::unique_ptr<ColumnIterator> guard(iter);
            std::unique_ptr<fs::ReadableBlock> rblock;
            block_mgr->open_block(fname, &rblock);

            ColumnIteratorOptions iter_opts;
            OlapReaderStatistics stats;
            iter_opts.stats = &stats;
            iter_opts.rblock = rblock.get();
            ASSERT_TRUE(iter->init(iter_opts).ok());

            // sequence read
            {
                auto st = iter->seek_to_first();
                ASSERT_TRUE(st.ok()) << st.to_string();

                auto dst_offsets = vectorized::UInt32Column::create();
                auto dst_elements = vectorized::Int32Column::create();
                auto dst_column = vectorized::ArrayColumn::create(dst_elements, dst_offsets);
                size_t rows_read = src_column->size();
                st = iter->next_batch(&rows_read, dst_column.get());
                ASSERT_TRUE(st.ok());
                ASSERT_EQ(src_column->size(), rows_read);

                ASSERT_EQ("[1, 2, 3]", dst_column->debug_item(0));
                ASSERT_EQ("[4, 5, 6]", dst_column->debug_item(1));
            }
        }
    }

    template <FieldType type>
    vectorized::ColumnPtr numeric_data(int null_ratio) {
        using CppType = typename CppTypeTraits<type>::CppType;
        auto col = vectorized::ChunkHelper::column_from_field_type(type, true);
        CppType value = 0;
        size_t count = 2 * 1024 * 1024 / sizeof(CppType);
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

    vectorized::ColumnPtr low_cardinality_strings(int null_ratio) {
        static std::string s1(4, 'a');
        static std::string s2(4, 'b');
        size_t count = 128 * 1024 / 4;
        auto col = vectorized::ChunkHelper::column_from_field_type(OLAP_FIELD_TYPE_VARCHAR, true);
        auto nc = down_cast<vectorized::NullableColumn*>(col.get());
        nc->reserve(count);
        down_cast<vectorized::BinaryColumn*>(nc->data_column().get())->get_data().reserve(s1.size() * count);
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

    vectorized::ColumnPtr high_cardinality_strings(int null_ratio) {
        std::string s1("abcdefghijklmnopqrstuvwxyz");
        std::string s2("bbcdefghijklmnopqrstuvwxyz");
        std::string s3("cbcdefghijklmnopqrstuvwxyz");
        std::string s4("dbcdefghijklmnopqrstuvwxyz");
        std::string s5("ebcdefghijklmnopqrstuvwxyz");
        std::string s6("fbcdefghijklmnopqrstuvwxyz");
        std::string s7("gbcdefghijklmnopqrstuvwxyz");
        std::string s8("hbcdefghijklmnopqrstuvwxyz");

        auto col = vectorized::ChunkHelper::column_from_field_type(OLAP_FIELD_TYPE_VARCHAR, true);
        size_t count = (128 * 1024 / s1.size()) / 8 * 8;
        auto nc = down_cast<vectorized::NullableColumn*>(col.get());
        nc->reserve(count);
        down_cast<vectorized::BinaryColumn*>(nc->data_column().get())->get_data().reserve(count * s1.size());
        for (size_t i = 0; i < count; i += 8) {
            (void)col->append_strings({s1, s2, s3, s4, s5, s6, s7, s8});

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

    vectorized::ColumnPtr date_values(int null_ratio) {
        size_t count = 4 * 1024 * 1024 / sizeof(vectorized::DateValue);
        auto col = vectorized::ChunkHelper::column_from_field_type(OLAP_FIELD_TYPE_DATE_V2, true);
        vectorized::DateValue value = vectorized::DateValue::create(2020, 10, 1);
        for (size_t i = 0; i < count; i++) {
            CHECK_EQ(1, col->append_numbers(&value, sizeof(value)));
            value = value.add<vectorized::TimeUnit::DAY>(1);
        }
        for (size_t i = 0; i < count; i += null_ratio) {
            ((vectorized::DateValue*)col->raw_data())[i] = vectorized::DateValue{0};
            CHECK(col->set_null(i));
        }
        return col;
    }

    vectorized::ColumnPtr datetime_values(int null_ratio) {
        size_t count = 4 * 1024 * 1024 / sizeof(vectorized::TimestampValue);
        auto col = vectorized::ChunkHelper::column_from_field_type(OLAP_FIELD_TYPE_TIMESTAMP, true);
        vectorized::TimestampValue value = vectorized::TimestampValue::create(2020, 10, 1, 10, 20, 1);
        for (size_t i = 0; i < count; i++) {
            CHECK_EQ(1, col->append_numbers(&value, sizeof(value)));
            value = value.add<vectorized::TimeUnit::MICROSECOND>(1);
        }
        for (size_t i = 0; i < count; i += null_ratio) {
            ((vectorized::TimestampValue*)col->raw_data())[i] = vectorized::TimestampValue{0};
            CHECK(col->set_null(i));
        }
        return col;
    }

    template <FieldType type>
    void test_numeric_types() {
        auto col = numeric_data<type>(1);
        test_nullable_data<type, BIT_SHUFFLE, 1>(*col);
        test_nullable_data<type, BIT_SHUFFLE, 2>(*col);
        test_nullable_data<type, BIT_SHUFFLE, 2>(*col, "1");

        col = numeric_data<type>(4);
        test_nullable_data<type, BIT_SHUFFLE, 1>(*col);
        test_nullable_data<type, BIT_SHUFFLE, 2>(*col);
        test_nullable_data<type, BIT_SHUFFLE, 2>(*col, "1");

        col = numeric_data<type>(10000);
        test_nullable_data<type, BIT_SHUFFLE, 1>(*col);
        test_nullable_data<type, BIT_SHUFFLE, 2>(*col);
        test_nullable_data<type, BIT_SHUFFLE, 2>(*col, "1");
    }

    MemPool _pool;
    std::unique_ptr<MemTracker> _tablet_meta_mem_tracker;
};

// NOLINTNEXTLINE
TEST_F(ColumnReaderWriterTest, test_int) {
    test_numeric_types<OLAP_FIELD_TYPE_INT>();
}

// NOLINTNEXTLINE
TEST_F(ColumnReaderWriterTest, test_double) {
    test_numeric_types<OLAP_FIELD_TYPE_DOUBLE>();
}

// NOLINTNEXTLINE
TEST_F(ColumnReaderWriterTest, test_date) {
    auto col = date_values(100);
    test_nullable_data<OLAP_FIELD_TYPE_DATE_V2, BIT_SHUFFLE, 1>(*col);
    test_nullable_data<OLAP_FIELD_TYPE_DATE_V2, BIT_SHUFFLE, 2>(*col);
    test_nullable_data<OLAP_FIELD_TYPE_DATE_V2, BIT_SHUFFLE, 2>(*col, "1");
}

// NOLINTNEXTLINE
TEST_F(ColumnReaderWriterTest, test_datetime) {
    auto col = datetime_values(100);
    test_nullable_data<OLAP_FIELD_TYPE_TIMESTAMP, BIT_SHUFFLE, 1>(*col);
    test_nullable_data<OLAP_FIELD_TYPE_TIMESTAMP, BIT_SHUFFLE, 2>(*col);
    test_nullable_data<OLAP_FIELD_TYPE_TIMESTAMP, BIT_SHUFFLE, 2>(*col, "1");
}

// NOLINTNEXTLINE
TEST_F(ColumnReaderWriterTest, test_binary) {
    auto c = low_cardinality_strings(10000);
    test_nullable_data<OLAP_FIELD_TYPE_VARCHAR, DICT_ENCODING, 1>(*c);
    test_nullable_data<OLAP_FIELD_TYPE_VARCHAR, DICT_ENCODING, 2>(*c);
    test_nullable_data<OLAP_FIELD_TYPE_VARCHAR, DICT_ENCODING, 2>(*c, "1");

    test_nullable_data<OLAP_FIELD_TYPE_CHAR, DICT_ENCODING, 1>(*c);
    test_nullable_data<OLAP_FIELD_TYPE_CHAR, DICT_ENCODING, 2>(*c);
    test_nullable_data<OLAP_FIELD_TYPE_CHAR, DICT_ENCODING, 2>(*c, "1");

    c = high_cardinality_strings(100);
    test_nullable_data<OLAP_FIELD_TYPE_VARCHAR, DICT_ENCODING, 1>(*c);
    test_nullable_data<OLAP_FIELD_TYPE_VARCHAR, DICT_ENCODING, 2>(*c);
    test_nullable_data<OLAP_FIELD_TYPE_VARCHAR, DICT_ENCODING, 2>(*c, "1");

    test_nullable_data<OLAP_FIELD_TYPE_CHAR, DICT_ENCODING, 1>(*c);
    test_nullable_data<OLAP_FIELD_TYPE_CHAR, DICT_ENCODING, 2>(*c);
    test_nullable_data<OLAP_FIELD_TYPE_CHAR, DICT_ENCODING, 2>(*c, "1");
}

// NOLINTNEXTLINE
TEST_F(ColumnReaderWriterTest, test_default_value) {
    std::string v_int("1");
    int32_t result = 1;
    test_read_default_value<OLAP_FIELD_TYPE_TINYINT>(v_int, &result);
    test_read_default_value<OLAP_FIELD_TYPE_SMALLINT>(v_int, &result);
    test_read_default_value<OLAP_FIELD_TYPE_INT>(v_int, &result);

    std::string v_bigint("9223372036854775807");
    int64_t result_bigint = std::numeric_limits<int64_t>::max();
    test_read_default_value<OLAP_FIELD_TYPE_BIGINT>(v_bigint, &result_bigint);
    int128_t result_largeint = std::numeric_limits<int64_t>::max();
    test_read_default_value<OLAP_FIELD_TYPE_LARGEINT>(v_bigint, &result_largeint);

    std::string v_float("1.00");
    float result2 = 1.00;
    test_read_default_value<OLAP_FIELD_TYPE_FLOAT>(v_float, &result2);

    std::string v_double("1.00");
    double result3 = 1.00;
    test_read_default_value<OLAP_FIELD_TYPE_DOUBLE>(v_double, &result3);

    std::string v_varchar("varchar");
    test_read_default_value<OLAP_FIELD_TYPE_VARCHAR>(v_varchar, &v_varchar);

    std::string v_char("char");
    test_read_default_value<OLAP_FIELD_TYPE_CHAR>(v_char, &v_char);

    char* c = (char*)malloc(1);
    c[0] = 0;
    std::string v_object(c, 1);
    test_read_default_value<OLAP_FIELD_TYPE_HLL>(v_object, &v_object);
    test_read_default_value<OLAP_FIELD_TYPE_OBJECT>(v_object, &v_object);
    free(c);

    std::string v_date("2019-11-12");
    uint24_t result_date(1034092);
    test_read_default_value<OLAP_FIELD_TYPE_DATE>(v_date, &result_date);

    std::string v_datetime("2019-11-12 12:01:08");
    int64_t result_datetime = 20191112120108;
    test_read_default_value<OLAP_FIELD_TYPE_DATETIME>(v_datetime, &result_datetime);

    std::string v_decimal("102418.000000002");
    decimal12_t decimal(102418, 2);
    test_read_default_value<OLAP_FIELD_TYPE_DECIMAL>(v_decimal, &decimal);
}

// test array<int>, and nullable
TEST_F(ColumnReaderWriterTest, test_array_int) {
    test_int_array<2>();
    test_int_array<2>("1");
}

} // namespace starrocks::segment_v2
