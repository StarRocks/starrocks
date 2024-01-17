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
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/rowset/segment_v2/bloom_filter_index_reader_writer_test.cpp

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

#include "common/logging.h"
#include "fs/fs_memory.h"
#include "runtime/mem_tracker.h"
#include "storage/key_coder.h"
#include "storage/olap_common.h"
#include "storage/rowset/bloom_filter.h"
#include "storage/rowset/bloom_filter_index_reader.h"
#include "storage/rowset/bloom_filter_index_writer.h"
#include "storage/types.h"
#include "testutil/assert.h"

namespace starrocks {

const std::string kTestDir = "/bloom_filter_index_reader_writer_test";

class BloomFilterIndexReaderWriterTest : public testing::Test {
protected:
    void SetUp() override {
        _mem_tracker = std::make_unique<MemTracker>();
        StoragePageCache::create_global_cache(_mem_tracker.get(), 1000000000);
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(_fs->create_dir(kTestDir).ok());

        _opts.use_page_cache = true;
        _opts.kept_in_memory = false;
        _opts.stats = &_stats;
    }
    void TearDown() override { StoragePageCache::release_global_cache(); }

    template <LogicalType type>
    void write_bloom_filter_index_file(const std::string& file_name, const void* values, size_t value_count,
                                       size_t null_count, ColumnIndexMetaPB* index_meta) {
        TypeInfoPtr type_info = get_type_info(type);
        using CppType = typename CppTypeTraits<type>::CppType;
        std::string fname = kTestDir + "/" + file_name;
        {
            ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(fname));

            std::unique_ptr<BloomFilterIndexWriter> bloom_filter_index_writer;
            BloomFilterOptions bf_options;
            BloomFilterIndexWriter::create(bf_options, type_info, &bloom_filter_index_writer);
            const auto* vals = (const CppType*)values;
            for (int i = 0; i < value_count;) {
                size_t num = std::min(1024, (int)value_count - i);
                bloom_filter_index_writer->add_values(vals + i, num);
                if (i == 2048) {
                    // second page
                    bloom_filter_index_writer->add_nulls(null_count);
                }
                ASSERT_OK(bloom_filter_index_writer->flush());
                i += 1024;
            }
            ASSERT_OK(bloom_filter_index_writer->finish(wfile.get(), index_meta));
            ASSERT_TRUE(wfile->close().ok());
            ASSERT_EQ(BLOOM_FILTER_INDEX, index_meta->type());
            ASSERT_EQ(bf_options.strategy, index_meta->bloom_filter_index().hash_strategy());
        }
    }

    void get_bloom_filter_reader_iter(const std::string& file_name, const ColumnIndexMetaPB& meta,
                                      std::unique_ptr<RandomAccessFile>* rfile, BloomFilterIndexReader** reader,
                                      std::unique_ptr<BloomFilterIndexIterator>* iter) {
        auto filename = kTestDir + "/" + file_name;
        ASSIGN_OR_ABORT(*rfile, _fs->new_random_access_file(filename))
        _opts.read_file = (*rfile).get();

        *reader = new BloomFilterIndexReader();
        ASSIGN_OR_ABORT(auto r, (*reader)->load(_opts, meta.bloom_filter_index()));
        ASSERT_TRUE(r);
        ASSERT_OK((*reader)->new_iterator(_opts, iter));
    }

    template <LogicalType Type>
    void test_bloom_filter_index_reader_writer_template(const std::string file_name,
                                                        typename TypeTraits<Type>::CppType* val, size_t num,
                                                        size_t null_num,
                                                        typename TypeTraits<Type>::CppType* not_exist_value,
                                                        bool is_slice_type = false) {
        typedef typename TypeTraits<Type>::CppType CppType;
        ColumnIndexMetaPB meta;
        write_bloom_filter_index_file<Type>(file_name, val, num, null_num, &meta);
        {
            std::unique_ptr<RandomAccessFile> rfile;
            BloomFilterIndexReader* reader = nullptr;
            std::unique_ptr<BloomFilterIndexIterator> iter;
            get_bloom_filter_reader_iter(file_name, meta, &rfile, &reader, &iter);

            // page 0
            std::unique_ptr<BloomFilter> bf;
            auto st = iter->read_bloom_filter(0, &bf);
            ASSERT_TRUE(st.ok());
            for (int i = 0; i < 1024; ++i) {
                if (is_slice_type) {
                    auto* value = (Slice*)(val + i);
                    ASSERT_TRUE(bf->test_bytes(value->data, value->size));
                } else {
                    ASSERT_TRUE(bf->test_bytes((char*)&val[i], sizeof(CppType)));
                }
            }

            // page 1
            st = iter->read_bloom_filter(1, &bf);
            ASSERT_TRUE(st.ok());
            for (int i = 1024; i < 2048; ++i) {
                if (is_slice_type) {
                    auto* value = (Slice*)(val + i);
                    ASSERT_TRUE(bf->test_bytes(value->data, value->size));
                } else {
                    ASSERT_TRUE(bf->test_bytes((char*)&val[i], sizeof(CppType)));
                }
            }

            // page 2
            st = iter->read_bloom_filter(2, &bf);
            ASSERT_TRUE(st.ok());
            for (int i = 2048; i < 3071; ++i) {
                if (is_slice_type) {
                    auto* value = (Slice*)(val + i);
                    ASSERT_TRUE(bf->test_bytes(value->data, value->size));
                } else {
                    ASSERT_TRUE(bf->test_bytes((char*)&val[i], sizeof(CppType)));
                }
            }
            // test nullptr
            ASSERT_TRUE(bf->test_bytes(nullptr, 1));

            delete reader;
        }
    }

    std::unique_ptr<MemTracker> _mem_tracker = nullptr;
    std::shared_ptr<MemoryFileSystem> _fs = nullptr;
    IndexReadOptions _opts;
    OlapReaderStatistics _stats;
};

TEST_F(BloomFilterIndexReaderWriterTest, test_int) {
    size_t num = 1024 * 3 - 1;
    int* val = new int[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        val[i] = 10000 + i + 1;
    }

    std::string file_name = "bloom_filter_int";
    int not_exist_value = 18888;
    test_bloom_filter_index_reader_writer_template<TYPE_INT>(file_name, val, num, 1, &not_exist_value);
    delete[] val;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_bigint) {
    size_t num = 1024 * 3 - 1;
    auto* val = new int64_t[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        val[i] = 100000000 + i + 1;
    }

    std::string file_name = "bloom_filter_bigint";
    int64_t not_exist_value = 18888;
    test_bloom_filter_index_reader_writer_template<TYPE_BIGINT>(file_name, val, num, 1, &not_exist_value);
    delete[] val;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_largeint) {
    size_t num = 1024 * 3 - 1;
    auto* val = new int128_t[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        val[i] = 100000000 + i + 1;
    }

    std::string file_name = "bloom_filter_largeint";
    int128_t not_exist_value = 18888;
    test_bloom_filter_index_reader_writer_template<TYPE_LARGEINT>(file_name, val, num, 1, &not_exist_value);
    delete[] val;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_varchar_type) {
    size_t num = 1024 * 3 - 1;
    auto* val = new std::string[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        val[i] = "prefix_" + std::to_string(i);
    }
    auto* slices = new Slice[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        slices[i] = Slice(val[i].c_str(), val[i].size());
    }
    std::string file_name = "bloom_filter_varchar";
    Slice not_exist_value("value_not_exist");
    test_bloom_filter_index_reader_writer_template<TYPE_VARCHAR>(file_name, slices, num, 1, &not_exist_value, true);
    delete[] val;
    delete[] slices;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_char) {
    size_t num = 1024 * 3 - 1;
    auto* val = new std::string[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        val[i] = "prefix_" + std::to_string(10000 + i);
    }
    auto* slices = new Slice[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        slices[i] = Slice(val[i].c_str(), val[i].size());
    }
    std::string file_name = "bloom_filter_char";
    Slice not_exist_value("char_value_not_exist");
    test_bloom_filter_index_reader_writer_template<TYPE_CHAR>(file_name, slices, num, 1, &not_exist_value, true);
    delete[] val;
    delete[] slices;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_date) {
    size_t num = 1024 * 3 - 1;
    auto* val = new uint24_t[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        val[i] = 10000 + i + 1;
    }

    std::string file_name = "bloom_filter_date";
    uint24_t not_exist_value = 18888;
    test_bloom_filter_index_reader_writer_template<TYPE_DATE_V1>(file_name, val, num, 1, &not_exist_value);
    delete[] val;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_datetime) {
    size_t num = 1024 * 3 - 1;
    auto* val = new int64_t[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        val[i] = 10000 + i + 1;
    }

    std::string file_name = "bloom_filter_datetime";
    int64_t not_exist_value = 18888;
    test_bloom_filter_index_reader_writer_template<TYPE_DATETIME_V1>(file_name, val, num, 1, &not_exist_value);
    delete[] val;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_decimal) {
    size_t num = 1024 * 3 - 1;
    auto* val = new decimal12_t[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        val[i] = decimal12_t(i + 1, i + 1);
    }

    std::string file_name = "bloom_filter_decimal";
    decimal12_t not_exist_value = decimal12_t(666, 666);
    test_bloom_filter_index_reader_writer_template<TYPE_DECIMAL>(file_name, val, num, 1, &not_exist_value);
    delete[] val;
}

} // namespace starrocks
