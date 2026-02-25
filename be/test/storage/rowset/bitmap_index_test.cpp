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
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/rowset/segment_v2/bitmap_index_test.cpp

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

#include <string>
#include <thread>

#include "base/string/utf8.h"
#include "base/testutil/assert.h"
#include "column/column_viewer.h"
#include "fs/fs_memory.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/key_coder.h"
#include "storage/olap_common.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "storage/rowset/bitmap_index_writer.h"
#include "storage/types.h"

namespace starrocks {

class BitmapIndexTest : public testing::Test {
public:
    const std::string kTestDir = "/bitmap_index_test";

    BitmapIndexTest() = default;

protected:
    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(_fs->create_dir(kTestDir).ok());

        _opts.use_page_cache = true;
        _opts.stats = &_stats;
    }
    void TearDown() override {}

    void get_bitmap_reader_iter(RandomAccessFile* rfile, const ColumnIndexMetaPB& meta, BitmapIndexReader** reader,
                                BitmapIndexIterator** iter, int32_t gram_num = -1) {
        _opts.read_file = rfile;
        *reader = new BitmapIndexReader(gram_num);
        ASSIGN_OR_ABORT(auto r, (*reader)->load(_opts, meta.bitmap_index()));
        ASSERT_TRUE(r);
        ASSERT_OK((*reader)->new_iterator(_opts, iter));
    }

    template <LogicalType type>
    void write_index_file(std::string& filename, const void* values, size_t value_count, size_t null_count,
                          ColumnIndexMetaPB* meta) {
        TypeInfoPtr type_info = get_type_info(type);
        {
            ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(filename));

            std::unique_ptr<BitmapIndexWriter> writer;
            BitmapIndexWriter::create(type_info, &writer);
            writer->add_values(values, value_count);
            writer->add_nulls(null_count);
            ASSERT_TRUE(writer->finish(wfile.get(), meta).ok());
            ASSERT_EQ(BITMAP_INDEX, meta->type());
            ASSERT_TRUE(wfile->close().ok());
        }
    }

    void write_index_file_use_by_gin(const int32_t gram_num, const std::string& filename,
                                     const std::vector<std::string>& values, ColumnIndexMetaPB* meta) const {
        const TypeInfoPtr type_info = get_type_info(TYPE_VARCHAR);
        ASSIGN_OR_ABORT(const auto wfile, _fs->new_writable_file(filename));

        std::unique_ptr<BitmapIndexWriter> writer;
        BitmapIndexWriter::create(type_info, &writer, gram_num);

        for (size_t i = 0; i < values.size(); ++i) {
            Slice tmp(values[i]);
            writer->add_value_with_current_rowid(&tmp);
        }
        ASSERT_TRUE(writer->finish(wfile.get(), meta).ok());
        ASSERT_EQ(BITMAP_INDEX, meta->type());
        ASSERT_TRUE(wfile->close().ok());
    }

    std::shared_ptr<MemoryFileSystem> _fs = nullptr;
    MemPool _pool;
    IndexReadOptions _opts;
    OlapReaderStatistics _stats;
};

TEST_F(BitmapIndexTest, test_invert) {
    size_t num_uint8_rows = 1024 * 10;
    int* val = new int[num_uint8_rows];
    for (int i = 0; i < num_uint8_rows; ++i) {
        val[i] = i;
    }

    std::string file_name = kTestDir + "/invert";
    ColumnIndexMetaPB meta;
    write_index_file<TYPE_INT>(file_name, val, num_uint8_rows, 0, &meta);
    {
        BitmapIndexReader* reader = nullptr;
        BitmapIndexIterator* iter = nullptr;
        ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
        get_bitmap_reader_iter(rfile.get(), meta, &reader, &iter);

        int value = 2;
        bool exact_match;
        Status st = iter->seek_dictionary(&value, &exact_match);
        ASSERT_TRUE(st.ok());
        ASSERT_TRUE(exact_match);
        ASSERT_EQ(2, iter->current_ordinal());

        Roaring bitmap;
        iter->read_bitmap(iter->current_ordinal(), &bitmap);
        ASSERT_TRUE(Roaring::bitmapOf(1, 2) == bitmap);

        int value2 = 1024 * 9;
        st = iter->seek_dictionary(&value2, &exact_match);
        ASSERT_TRUE(st.ok());
        ASSERT_TRUE(exact_match);
        ASSERT_EQ(1024 * 9, iter->current_ordinal());

        iter->read_union_bitmap(iter->current_ordinal(), iter->bitmap_nums(), &bitmap);
        ASSERT_EQ(1025, bitmap.cardinality());

        int value3 = 1024;
        iter->seek_dictionary(&value3, &exact_match);
        ASSERT_EQ(1024, iter->current_ordinal());

        Roaring bitmap2;
        iter->read_union_bitmap(0, iter->current_ordinal(), &bitmap2);
        ASSERT_EQ(1024, bitmap2.cardinality());

        delete reader;
        delete iter;
    }
    delete[] val;
}

TEST_F(BitmapIndexTest, test_invert_2) {
    size_t num_uint8_rows = 1024 * 10;
    int* val = new int[num_uint8_rows];
    for (int i = 0; i < 1024; ++i) {
        val[i] = i;
    }

    for (int i = 1024; i < num_uint8_rows; ++i) {
        val[i] = i * 10;
    }

    std::string file_name = kTestDir + "/invert2";
    ColumnIndexMetaPB meta;
    write_index_file<TYPE_INT>(file_name, val, num_uint8_rows, 0, &meta);

    {
        BitmapIndexReader* reader = nullptr;
        BitmapIndexIterator* iter = nullptr;
        ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
        get_bitmap_reader_iter(rfile.get(), meta, &reader, &iter);

        int value = 1026;
        bool exact_match;
        auto st = iter->seek_dictionary(&value, &exact_match);
        ASSERT_TRUE(st.ok());
        ASSERT_TRUE(!exact_match);

        ASSERT_EQ(1024, iter->current_ordinal());

        Roaring bitmap;
        iter->read_union_bitmap(0, iter->current_ordinal(), &bitmap);
        ASSERT_EQ(1024, bitmap.cardinality());

        delete reader;
        delete iter;
    }
    delete[] val;
}

TEST_F(BitmapIndexTest, test_multi_pages) {
    size_t num_uint8_rows = 1024 * 1024;
    auto* val = new int64_t[num_uint8_rows];
    for (int i = 0; i < num_uint8_rows; ++i) {
        val[i] = random() + 10000;
    }
    val[1024 * 510] = 2019;

    std::string file_name = kTestDir + "/mul";
    ColumnIndexMetaPB meta;
    write_index_file<TYPE_BIGINT>(file_name, val, num_uint8_rows, 0, &meta);
    {
        BitmapIndexReader* reader = nullptr;
        BitmapIndexIterator* iter = nullptr;
        ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
        get_bitmap_reader_iter(rfile.get(), meta, &reader, &iter);

        int64_t value = 2019;
        bool exact_match;
        auto st = iter->seek_dictionary(&value, &exact_match);
        ASSERT_TRUE(st.ok()) << "status:" << st.to_string();
        ASSERT_EQ(0, iter->current_ordinal());

        Roaring bitmap;
        iter->read_bitmap(iter->current_ordinal(), &bitmap);
        ASSERT_EQ(1, bitmap.cardinality());

        delete reader;
        delete iter;
    }
    delete[] val;
}

TEST_F(BitmapIndexTest, test_null) {
    size_t num_uint8_rows = 1024;
    auto* val = new int64_t[num_uint8_rows];
    for (int i = 0; i < num_uint8_rows; ++i) {
        val[i] = i;
    }

    std::string file_name = kTestDir + "/null";
    ColumnIndexMetaPB meta;
    write_index_file<TYPE_BIGINT>(file_name, val, num_uint8_rows, 30, &meta);
    {
        BitmapIndexReader* reader = nullptr;
        BitmapIndexIterator* iter = nullptr;
        ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
        get_bitmap_reader_iter(rfile.get(), meta, &reader, &iter);

        Roaring bitmap;
        iter->read_null_bitmap(&bitmap);
        ASSERT_EQ(30, bitmap.cardinality());

        delete reader;
        delete iter;
    }
    delete[] val;
}

TEST_F(BitmapIndexTest, test_concurrent_load) {
    size_t num_uint8_rows = 1024;
    auto* val = new int64_t[num_uint8_rows];
    for (int i = 0; i < num_uint8_rows; ++i) {
        val[i] = i;
    }

    std::string file_name = kTestDir + "/null";
    ColumnIndexMetaPB meta;
    write_index_file<TYPE_BIGINT>(file_name, val, num_uint8_rows, 30, &meta);

    IndexReadOptions opts;
    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name))
    opts.read_file = rfile.get();
    opts.use_page_cache = true;
    OlapReaderStatistics stats;
    opts.stats = &stats;
    auto reader = std::make_unique<BitmapIndexReader>();
    std::atomic<int> count{0};
    std::atomic<int> loads{0};
    constexpr int kNumThreads = 5;
    std::vector<std::thread> threads;
    for (int i = 0; i < kNumThreads; i++) {
        threads.emplace_back([&]() {
            count.fetch_add(1);
            while (count.load() < count) {
                ;
            }
            ASSIGN_OR_ABORT(auto first_load, reader->load(opts, meta.bitmap_index()));
            loads.fetch_add(first_load);
        });
    }
    for (auto&& t : threads) {
        t.join();
    }
    ASSERT_EQ(1, loads.load());

    BitmapIndexIterator* iter = nullptr;
    ASSERT_OK(reader->new_iterator(opts, &iter));

    Roaring bitmap;
    iter->read_null_bitmap(&bitmap);
    ASSERT_EQ(30, bitmap.cardinality());

    delete iter;
    delete[] val;
}

// Exercise add_value_with_current_rowid / incre_rowid path directly, including
// duplicate values within the same row and across different rows.
TEST_F(BitmapIndexTest, test_add_value_with_current_rowid) {
    std::string file_name = kTestDir + "/add_value_with_rowid";
    ColumnIndexMetaPB meta;

    TypeInfoPtr type_info = get_type_info(TYPE_INT);
    {
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

        std::unique_ptr<BitmapIndexWriter> writer;
        ASSERT_OK(BitmapIndexWriter::create(type_info, &writer));

        int v1 = 1;
        int v2 = 2;
        int v3 = 3;

        // Row 0: value=1 once.
        writer->add_value_with_current_rowid(&v1);
        writer->incre_rowid();

        // Row 1: value=1 twice, but rowid is the same, so second add should be ignored.
        writer->add_value_with_current_rowid(&v1);
        writer->add_value_with_current_rowid(&v1);
        writer->incre_rowid();

        // Row 2 and 3: value=2, ensure a context is created and duplicate within row 3
        // is filtered by pending-add de-duplication.
        writer->add_value_with_current_rowid(&v2); // row 2
        writer->incre_rowid();
        writer->add_value_with_current_rowid(&v2); // row 3, first occurrence
        writer->add_value_with_current_rowid(&v2); // row 3, duplicate
        writer->incre_rowid();

        // Row 4: a distinct value=3.
        writer->add_value_with_current_rowid(&v3);
        writer->incre_rowid();

        ASSERT_OK(writer->finish(wfile.get(), &meta));
        ASSERT_EQ(BITMAP_INDEX, meta.type());
        ASSERT_TRUE(wfile->close().ok());
    }

    BitmapIndexReader* reader = nullptr;
    BitmapIndexIterator* iter = nullptr;
    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
    get_bitmap_reader_iter(rfile.get(), meta, &reader, &iter);

    // Verify postings for value=1: should contain rowids 0 and 1.
    {
        int search = 1;
        bool exact_match = false;
        ASSERT_OK(iter->seek_dictionary(&search, &exact_match));
        ASSERT_TRUE(exact_match);
        Roaring bitmap;
        ASSERT_OK(iter->read_bitmap(iter->current_ordinal(), &bitmap));
        ASSERT_EQ(2, bitmap.cardinality());
        ASSERT_TRUE(bitmap.contains(0));
        ASSERT_TRUE(bitmap.contains(1));
    }

    // Verify postings for value=2: should contain rowids 2 and 3 (duplicate in row 3 ignored).
    {
        int search = 2;
        bool exact_match = false;
        ASSERT_OK(iter->seek_dictionary(&search, &exact_match));
        ASSERT_TRUE(exact_match);
        Roaring bitmap;
        ASSERT_OK(iter->read_bitmap(iter->current_ordinal(), &bitmap));
        ASSERT_EQ(2, bitmap.cardinality());
        ASSERT_TRUE(bitmap.contains(2));
        ASSERT_TRUE(bitmap.contains(3));
    }

    delete reader;
    delete iter;
}

TEST_F(BitmapIndexTest, test_read_union_variants) {
    size_t num_rows = 100;
    int* val = new int[num_rows];
    for (int i = 0; i < num_rows; ++i) {
        val[i] = i;
    }

    std::string file_name = kTestDir + "/union_variants";
    ColumnIndexMetaPB meta;
    write_index_file<TYPE_INT>(file_name, val, num_rows, 0, &meta);
    {
        BitmapIndexReader* reader = nullptr;
        BitmapIndexIterator* iter = nullptr;
        ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
        get_bitmap_reader_iter(rfile.get(), meta, &reader, &iter);

        // Test read_union_bitmap(Buffer<rowid_t>)
        {
            Roaring result;
            Buffer<rowid_t> rowids = {1, 3, 5};
            ASSERT_OK(iter->read_union_bitmap(rowids, &result));
            ASSERT_EQ(3, result.cardinality());
            ASSERT_TRUE(result.contains(1));
            ASSERT_TRUE(result.contains(3));
            ASSERT_TRUE(result.contains(5));
        }

        // Test read_union_bitmap(SparseRange)
        {
            Roaring result;
            SparseRange<> range;
            range.add(Range<>(1, 3)); // rowids 1, 2
            range.add(Range<>(5, 6)); // rowid 5
            ASSERT_OK(iter->read_union_bitmap(range, &result));
            ASSERT_EQ(3, result.cardinality());
            ASSERT_TRUE(result.contains(1));
            ASSERT_TRUE(result.contains(2));
            ASSERT_TRUE(result.contains(5));
        }

        delete reader;
        delete iter;
    }
    delete[] val;
}

TEST_F(BitmapIndexTest, test_seek_dictionary_by_predicate) {
    std::vector<std::string> values = {"apple", "banana", "cherry", "date"};
    std::vector<Slice> slices;
    for (auto& v : values) slices.emplace_back(v);

    std::string file_name = kTestDir + "/seek_predicate";
    ColumnIndexMetaPB meta;
    write_index_file<TYPE_VARCHAR>(file_name, slices.data(), values.size(), 0, &meta);
    {
        BitmapIndexReader* reader = nullptr;
        BitmapIndexIterator* iter = nullptr;
        ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
        get_bitmap_reader_iter(rfile.get(), meta, &reader, &iter);

        auto predicate = [](const Column& value_column) -> StatusOr<ColumnPtr> {
            auto res = BooleanColumn::create();
            const auto& binary_col = down_cast<const BinaryColumn&>(value_column);
            for (size_t i = 0; i < binary_col.size(); ++i) {
                Slice s = binary_col.get_data()[i];
                res->append(s.to_string().find('a') != std::string::npos);
            }
            return res;
        };

        Slice from("");
        auto res = iter->seek_dictionary_by_predicate(predicate, from, values.size());
        ASSERT_TRUE(res.ok());
        auto hit_rowids = res.value();
        // "apple", "banana", "date" contain 'a'. Indices 0, 1, 3.
        ASSERT_EQ(3, hit_rowids.size());
        ASSERT_EQ(0, hit_rowids[0]);
        ASSERT_EQ(1, hit_rowids[1]);
        ASSERT_EQ(3, hit_rowids[2]);

        delete reader;
        delete iter;
    }
}

TEST_F(BitmapIndexTest, test_reader_load_twice) {
    size_t num_rows = 10;
    int* val = new int[num_rows];
    for (int i = 0; i < num_rows; ++i) val[i] = i;

    std::string file_name = kTestDir + "/load_twice";
    ColumnIndexMetaPB meta;
    write_index_file<TYPE_INT>(file_name, val, num_rows, 0, &meta);

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
    _opts.read_file = rfile.get();
    BitmapIndexReader reader;
    auto res1 = reader.load(_opts, meta.bitmap_index());
    ASSERT_TRUE(res1.ok());
    ASSERT_TRUE(res1.value()); // First load returns true

    auto res2 = reader.load(_opts, meta.bitmap_index());
    ASSERT_TRUE(res2.ok());
    ASSERT_FALSE(res2.value()); // Second load returns false

    delete[] val;
}

TEST_F(BitmapIndexTest, test_dict_ngram_index) {
    constexpr int32_t num_keywords = 10;
    constexpr int32_t gram_num = 3;

    std::set<std::string> ngram;
    std::vector<std::string> keywords;
    for (int i = 0; i < num_keywords; ++i) {
        // slice should be one of hel,ell,llo,low,wor,orl,rld,ld ,d {0,...,9}
        const std::string keyword = "hello, world " + std::to_string(i);

        std::vector<size_t> index;
        Slice cur_slice(keyword);
        const size_t slice_gram_num = get_utf8_index(cur_slice, &index);

        for (size_t j = 0; j + gram_num <= slice_gram_num; ++j) {
            // find next ngram
            size_t cur_ngram_length =
                    j + gram_num < slice_gram_num ? index[j + gram_num] - index[j] : cur_slice.get_size() - index[j];
            Slice cur_ngram(cur_slice.data + index[j], cur_ngram_length);
            ngram.emplace(cur_ngram.to_string());
        }

        keywords.emplace_back(keyword);
    }

    std::string file_name = kTestDir + "/dict_ngram_index";
    ColumnIndexMetaPB meta;
    write_index_file_use_by_gin(3, file_name, keywords, &meta);

    {
        BitmapIndexReader* reader = nullptr;
        BitmapIndexIterator* iter = nullptr;
        ASSIGN_OR_ABORT(const auto rfile, _fs->new_random_access_file(file_name));
        get_bitmap_reader_iter(rfile.get(), meta, &reader, &iter, 3);

        const size_t dict_num = reader->bitmap_nums();
        ASSERT_EQ(dict_num, num_keywords);

        const size_t ngram_num = reader->ngram_bitmap_nums();
        ASSERT_EQ(ngram_num, ngram.size());

        size_t to_read = ngram_num;
        const auto col = ChunkHelper::column_from_field_type(TYPE_VARCHAR, false);
        ASSERT_TRUE(iter->next_batch_ngram(0, &to_read, col.get()).ok());
        ASSERT_EQ(ngram_num, to_read);

        ColumnViewer<TYPE_VARCHAR> viewer(std::move(col));
        ASSERT_EQ(ngram.size(), viewer.size());

        auto it = ngram.begin();
        for (rowid_t i = 0; i < viewer.size(); ++i) {
            auto value = viewer.value(i);
            auto current_gram = *it;
            ASSERT_EQ(current_gram, value.to_string());
            ++it;

            roaring::Roaring r1, r2;
            ASSERT_TRUE(iter->read_ngram_bitmap(i, &r1).ok());
            ASSERT_TRUE(iter->seek_dict_by_ngram(&value, &r2).ok());
            ASSERT_EQ(r1, r2);
            if (current_gram.starts_with("d ")) {
                ASSERT_EQ(1, r1.cardinality());
            } else {
                ASSERT_EQ(num_keywords, r1.cardinality());
            }
        }

        delete reader;
        delete iter;
    }
}

} // namespace starrocks
