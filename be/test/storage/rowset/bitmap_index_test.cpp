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

#include <algorithm>
#include <limits>
#include <numeric>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "base/string/utf8.h"
#include "base/testutil/assert.h"
#include "column/chunk_factory.h"
#include "column/column_viewer.h"
#include "column/schema.h"
#include "exec/exec_env.h"
#include "fs/fs_memory.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/olap_common.h"
#include "storage/rowset/bitmap_index_evaluator.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "storage/rowset/bitmap_index_writer.h"
#include "storage/storage_metrics.h"
#include "storage/types.h"
#include "storage_primitive/column_predicate_factory.h"
#include "storage_primitive/key_coder.h"
#include "storage_primitive/predicate_tree/predicate_tree.hpp"

namespace starrocks {

// Fail an actual lazy page read after BitmapIndexReader::load/new_iterator succeeded.
// A local unlink would not model an object deleted behind an already-open cloud stream.
class FaultingBitmapInputStream final : public io::SeekableInputStreamWrapper {
public:
    explicit FaultingBitmapInputStream(std::shared_ptr<io::SeekableInputStream> stream)
            : SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership), _stream(std::move(stream)) {}

    Status read_at_fully(int64_t offset, void* out, int64_t count) override {
        if (!failure.ok() && (!failure_offset.has_value() || offset == *failure_offset)) {
            ++failed_reads;
            return failure;
        }
        return SeekableInputStreamWrapper::read_at_fully(offset, out, count);
    }

    Status failure;
    std::optional<int64_t> failure_offset;
    int failed_reads = 0;

private:
    std::shared_ptr<io::SeekableInputStream> _stream;
};

class FakeBitmapWarmHandle final : public io::CacheWarmHandle {
public:
    size_t submitted_blocks() const override { return 1; }
    size_t pending_blocks() const override { return 0; }
};

// Records, in order, the page reads ('r') and the background fill requests ('w') of a bitmap index by offset.
class RecordingBitmapInputStream final : public io::SeekableInputStreamWrapper {
public:
    explicit RecordingBitmapInputStream(std::shared_ptr<io::SeekableInputStream> stream)
            : SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership), _stream(std::move(stream)) {}

    Status read_at_fully(int64_t offset, void* out, int64_t count) override {
        events.emplace_back('r', offset);
        return SeekableInputStreamWrapper::read_at_fully(offset, out, count);
    }

    StatusOr<std::shared_ptr<io::CacheWarmHandle>> warm_cache_async(int64_t offset, size_t length,
                                                                    bool urgent) override {
        events.emplace_back('w', offset);
        EXPECT_TRUE(urgent);
        EXPECT_GT(length, 0);
        if (!warm_supported) {
            return Status::NotSupported("test: no background cache fill");
        }
        return std::make_shared<FakeBitmapWarmHandle>();
    }

    std::set<int64_t> offsets(char kind) const {
        std::set<int64_t> result;
        for (const auto& [k, offset] : events) {
            if (k == kind) result.insert(offset);
        }
        return result;
    }

    size_t count(char kind) const {
        return std::count_if(events.begin(), events.end(), [kind](const auto& e) { return e.first == kind; });
    }

    std::vector<std::pair<char, int64_t>> events;
    bool warm_supported = true;

private:
    std::shared_ptr<io::SeekableInputStream> _stream;
};

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
                                SegmentBitmapIndexIterator** iter, int32_t gram_num = -1) {
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

    // A BIGINT bitmap index of one value per row, prefetch_value(0..kPrefetchRows-1), and one null, over several
    // dictionary and bitmap pages, read through |stream| with prefetching allowed. The dictionary of a value seek
    // index is frame-of-reference encoded, which packs consecutive integers into one page; values 2^40 + 1 apart take
    // 41 bits each, about 3 MB. Their low bits are the row's, as the writer's hash map of the values needs: it hashes
    // an integer by its low bits, and values whose low bits are all zero would land in one bucket.
    static constexpr int kPrefetchRows = 600000;
    static int64_t prefetch_value(int64_t row) { return row * ((int64_t{1} << 40) + 1); }
    void open_prefetch_index(const std::string& name, ColumnIndexMetaPB* meta,
                             std::unique_ptr<RecordingBitmapInputStream>* stream, BitmapIndexReader* reader) {
        std::vector<int64_t> values(kPrefetchRows);
        for (int row = 0; row < kPrefetchRows; row++) {
            values[row] = prefetch_value(row);
        }
        std::string filename = kTestDir + "/" + name;
        write_index_file<TYPE_BIGINT>(filename, values.data(), values.size(), 1, meta);
        ASSERT_FALSE(meta->bitmap_index().dict_column().value_index_meta().is_root_data_page());
        ASSERT_FALSE(meta->bitmap_index().bitmap_column().ordinal_index_meta().is_root_data_page());
        ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(filename));
        *stream = std::make_unique<RecordingBitmapInputStream>(rfile->stream());
        _opts.read_file = stream->get();
        _opts.use_page_cache = false;
        _opts.prefetch_bitmap_index_pages = true;
        ASSIGN_OR_ABORT(auto first_load, reader->load(_opts, meta->bitmap_index()));
        ASSERT_TRUE(first_load);
        (*stream)->events.clear();
    }

    static bool reader_bitmap_pages(const BitmapIndexReader& reader, ordinal_t from, ordinal_t to,
                                    std::vector<PagePointer>* pages) {
        return reader._bitmap_column_reader->data_pages_of_ordinals(from, to, std::numeric_limits<size_t>::max(),
                                                                    pages);
    }

    static uint64_t reader_dictionary_page(const BitmapIndexReader& reader, int64_t value) {
        PagePointer page;
        EXPECT_TRUE(reader._dict_column_reader->data_page_at_or_after(&value, &page));
        return page.offset;
    }

    static uint64_t reader_bitmap_page(const BitmapIndexReader& reader, ordinal_t ordinal) {
        std::vector<PagePointer> pages;
        EXPECT_TRUE(reader_bitmap_pages(reader, ordinal, ordinal + 1, &pages));
        EXPECT_EQ(size_t{1}, pages.size());
        return pages.empty() ? 0 : pages[0].offset;
    }

    // Evaluates |root| with prefetch over the prefetch index of |reader|, which serves every column but |unindexed|,
    // and checks that it keeps row 3 only and reads every page it asked for.
    void check_evaluator_reads_every_page_it_asks_for(const Schema& schema, PredicateAndNode root,
                                                      RecordingBitmapInputStream* stream, BitmapIndexReader* reader,
                                                      std::optional<ColumnId> unindexed) {
        auto tree = PredicateTree::create(std::move(root));
        BitmapIndexEvaluator evaluator(schema, tree);
        ASSERT_OK(evaluator.init([&](ColumnId cid) -> StatusOr<BitmapIndexIterator*> {
            if (unindexed.has_value() && cid == *unindexed) {
                return nullptr;
            }
            BitmapIndexIterator* iter = nullptr;
            RETURN_IF_ERROR(reader->new_iterator(_opts, &iter));
            return iter;
        }));
        SparseRange<> range(0, kPrefetchRows + 1);
        stream->events.clear();
        ASSERT_OK(evaluator.evaluate(range, tree, true));
        EXPECT_EQ(SparseRange<>(3, 4), range);
        EXPECT_GT(stream->count('w'), 0);
        const auto read = stream->offsets('r');
        for (auto offset : stream->offsets('w')) {
            EXPECT_TRUE(read.count(offset)) << "page at " << offset << " asked for but never read";
        }
    }

    std::shared_ptr<MemoryFileSystem> _fs = nullptr;
    MemPool _pool;
    IndexReadOptions _opts;
    OlapReaderStatistics _stats;

    // Fail a lazy dictionary or bitmap page read of an optional IDG bitmap file with |failure| after the reader
    // and iterator were opened, then evaluate `indexed = 7` over two row ranges.
    void check_optional_bitmap_late_failure(bool fail_bitmap_page, const Status& failure) {
        constexpr int nrows = 2048;
        std::vector<int> values(nrows);
        std::iota(values.begin(), values.end(), 0);
        std::string filename = kTestDir + (fail_bitmap_page ? "/optional_bitmap_late_bitmap_failure"
                                                            : "/optional_bitmap_late_dictionary_failure");
        ColumnIndexMetaPB meta;
        write_index_file<TYPE_INT>(filename, values.data(), values.size(), 0, &meta);
        ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(filename));
        FaultingBitmapInputStream stream(rfile->stream());
        _opts.read_file = &stream;
        _opts.use_page_cache = false;
        _opts.idg_bitmap_file_missing = std::make_shared<bool>(false);
        BitmapIndexReader reader;
        ASSIGN_OR_ABORT(auto first_load, reader.load(_opts, meta.bitmap_index()));
        ASSERT_TRUE(first_load);

        Schema schema({std::make_shared<Field>(0, "indexed", TYPE_INT, false)});
        std::unique_ptr<ColumnPredicate> eq(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "7"));
        PredicateAndNode root;
        root.add_child(PredicateColumnNode{eq.get()});
        auto tree = PredicateTree::create(std::move(root));
        const auto original_tree = tree.root().debug_string();
        BitmapIndexEvaluator evaluator(schema, tree);
        ASSERT_OK(evaluator.init([&](ColumnId) -> StatusOr<BitmapIndexIterator*> {
            BitmapIndexIterator* iter = nullptr;
            RETURN_IF_ERROR(reader.new_iterator(_opts, &iter));
            return iter;
        }));
        ASSERT_TRUE(evaluator.has_bitmap_index());

        if (fail_bitmap_page) {
            const auto& ordinal_meta = meta.bitmap_index().bitmap_column().ordinal_index_meta();
            ASSERT_TRUE(ordinal_meta.is_root_data_page());
            stream.failure_offset = ordinal_meta.root_page().offset();
        }
        stream.failure = failure;
        const auto missing_before = StorageMetrics::instance()->lake_idg_files_missing_total.value();
        SparseRange<> ranges;
        ranges.add({2, 100});
        ranges.add({200, nrows});
        const auto original_ranges = ranges;
        Status st = evaluator.evaluate(ranges, tree);
        ASSERT_GT(stream.failed_reads, 0); // This must exercise lazy I/O, not an unindexed/no-op path.
        EXPECT_EQ(original_ranges, ranges);
        EXPECT_EQ(original_tree, tree.root().debug_string());
        if (!failure.is_not_found()) {
            EXPECT_EQ(failure.code(), st.code());
            EXPECT_FALSE(*_opts.idg_bitmap_file_missing);
            EXPECT_EQ(missing_before, StorageMetrics::instance()->lake_idg_files_missing_total.value());
            return;
        }
        ASSERT_OK(st);
        EXPECT_TRUE(*_opts.idg_bitmap_file_missing);
        EXPECT_EQ(missing_before + 1, StorageMetrics::instance()->lake_idg_files_missing_total.value());
        // A repeated evaluation neither reads nor counts the already-unavailable optional file again.
        const auto failed_reads = stream.failed_reads;
        ASSERT_OK(evaluator.evaluate(ranges, tree));
        EXPECT_EQ(failed_reads, stream.failed_reads);
        EXPECT_EQ(original_ranges, ranges);
    }
};

TEST_F(BitmapIndexTest, optional_idg_missing_dictionary_page_keeps_range_and_predicates) {
    check_optional_bitmap_late_failure(false, Status::NotFound("deleted IDG dictionary"));
}

TEST_F(BitmapIndexTest, optional_idg_missing_bitmap_page_keeps_range_and_predicates) {
    check_optional_bitmap_late_failure(true, Status::NotFound("deleted IDG bitmap"));
}

TEST_F(BitmapIndexTest, optional_idg_non_missing_errors_still_fail) {
    check_optional_bitmap_late_failure(false, Status::IOError("dictionary read failed"));
    check_optional_bitmap_late_failure(true, Status::Corruption("bitmap page is corrupt"));
}

TEST_F(BitmapIndexTest, optional_idg_absent_dictionary_key_is_not_a_missing_file) {
    int values[] = {1, 2, 3};
    std::string filename = kTestDir + "/optional_bitmap_absent_key";
    ColumnIndexMetaPB meta;
    write_index_file<TYPE_INT>(filename, values, 3, 0, &meta);
    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(filename));
    _opts.read_file = rfile.get();
    _opts.use_page_cache = false;
    _opts.idg_bitmap_file_missing = std::make_shared<bool>(false);
    BitmapIndexReader reader;
    ASSIGN_OR_ABORT(auto first_load, reader.load(_opts, meta.bitmap_index()));
    ASSERT_TRUE(first_load);
    Schema schema({std::make_shared<Field>(0, "indexed", TYPE_INT, false)});
    std::unique_ptr<ColumnPredicate> predicate(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "99"));
    PredicateAndNode root;
    root.add_child(PredicateColumnNode{predicate.get()});
    auto tree = PredicateTree::create(std::move(root));
    BitmapIndexEvaluator evaluator(schema, tree);
    ASSERT_OK(evaluator.init([&](ColumnId) -> StatusOr<BitmapIndexIterator*> {
        BitmapIndexIterator* iter = nullptr;
        RETURN_IF_ERROR(reader.new_iterator(_opts, &iter));
        return iter;
    }));
    const auto missing_before = StorageMetrics::instance()->lake_idg_files_missing_total.value();
    SparseRange<> ranges(0, 3);
    ASSERT_OK(evaluator.evaluate(ranges, tree));
    EXPECT_TRUE(ranges.empty());
    EXPECT_FALSE(*_opts.idg_bitmap_file_missing);
    EXPECT_EQ(missing_before, StorageMetrics::instance()->lake_idg_files_missing_total.value());
}

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
        SegmentBitmapIndexIterator* iter = nullptr;
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
        SegmentBitmapIndexIterator* iter = nullptr;
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
        SegmentBitmapIndexIterator* iter = nullptr;
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

// The writer keeps every distinct value until finish(), so it must copy the bytes of Slice values instead of
// pointing into the caller's buffer, which a column writer reuses for the next batch. The dictionary spans several
// data pages, so seeking also goes through the value index built from each page's first value.
TEST_F(BitmapIndexTest, test_varchar_values_outlive_input_multi_pages) {
    const size_t num = 40000;
    std::vector<std::string> expected(num);
    std::vector<std::string> buffer(num);
    std::vector<Slice> slices(num);
    for (size_t i = 0; i < num; ++i) {
        char prefix[16];
        snprintf(prefix, sizeof(prefix), "%08zu_", i);
        expected[i] = std::string(prefix) + std::string(64, static_cast<char>('a' + i % 26));
        buffer[i] = expected[i];
        slices[i] = Slice(buffer[i].data(), buffer[i].size());
    }

    std::string file_name = kTestDir + "/varchar_outlive";
    ColumnIndexMetaPB meta;
    {
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
        std::unique_ptr<BitmapIndexWriter> writer;
        ASSERT_OK(BitmapIndexWriter::create(get_type_info(TYPE_VARCHAR), &writer));
        writer->add_values(slices.data(), num);
        // Overwrite the input before the dictionary is written.
        for (auto& s : buffer) {
            std::fill(s.begin(), s.end(), 'z');
        }
        ASSERT_OK(writer->finish(wfile.get(), &meta));
        ASSERT_OK(wfile->close());
    }
    ASSERT_FALSE(meta.bitmap_index().dict_column().value_index_meta().is_root_data_page());

    BitmapIndexReader* reader = nullptr;
    SegmentBitmapIndexIterator* iter = nullptr;
    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
    get_bitmap_reader_iter(rfile.get(), meta, &reader, &iter);
    ASSERT_EQ(num, iter->bitmap_nums());
    for (size_t i = 0; i < num; i += 997) {
        Slice value(expected[i]);
        bool exact_match = false;
        ASSERT_OK(iter->seek_dictionary(&value, &exact_match));
        ASSERT_TRUE(exact_match) << expected[i];
        ASSERT_EQ(i, iter->current_ordinal());

        Roaring bitmap;
        ASSERT_OK(iter->read_bitmap(iter->current_ordinal(), &bitmap));
        ASSERT_TRUE(Roaring::bitmapOf(1, static_cast<uint32_t>(i)) == bitmap);
    }
    delete reader;
    delete iter;
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
        SegmentBitmapIndexIterator* iter = nullptr;
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

    SegmentBitmapIndexIterator* iter = nullptr;
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
    SegmentBitmapIndexIterator* iter = nullptr;
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
        SegmentBitmapIndexIterator* iter = nullptr;
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
        SegmentBitmapIndexIterator* iter = nullptr;
        ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
        get_bitmap_reader_iter(rfile.get(), meta, &reader, &iter);

        auto predicate = [](const Column& value_column) -> StatusOr<ColumnPtr> {
            auto res = BooleanColumn::create();
            const auto& binary_col = down_cast<const BinaryColumn&>(value_column);
            for (size_t i = 0; i < binary_col.size(); ++i) {
                Slice s = binary_col.get_slice(i);
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
        SegmentBitmapIndexIterator* iter = nullptr;
        ASSIGN_OR_ABORT(const auto rfile, _fs->new_random_access_file(file_name));
        get_bitmap_reader_iter(rfile.get(), meta, &reader, &iter, 3);

        const size_t dict_num = reader->bitmap_nums();
        ASSERT_EQ(dict_num, num_keywords);

        const size_t ngram_num = reader->ngram_bitmap_nums();
        ASSERT_EQ(ngram_num, ngram.size());

        size_t to_read = ngram_num;
        const auto col = ChunkFactory::column_from_field_type(TYPE_VARCHAR, false);
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

// Verify that BitmapIndexReader with owned_mem_tracker=false does NOT affect the
// bitmap_index_mem_tracker during construction, load, or destruction.
TEST_F(BitmapIndexTest, test_owned_mem_tracker_false_no_tracking) {
    auto* tracker = RuntimeEnv::GetInstance()->bitmap_index_mem_tracker();
    int64_t baseline = tracker != nullptr ? tracker->consumption() : 0;

    size_t num_rows = 10;
    int val[10];
    for (int i = 0; i < 10; ++i) val[i] = i;

    std::string file_name = kTestDir + "/owned_false";
    ColumnIndexMetaPB meta;
    write_index_file<TYPE_INT>(file_name, val, num_rows, 0, &meta);

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
    _opts.read_file = rfile.get();

    {
        // Create with owned_mem_tracker=false.
        BitmapIndexReader reader(-1, false);

        // Construction should NOT change the tracker.
        if (tracker != nullptr) {
            ASSERT_EQ(tracker->consumption(), baseline);
        }

        // Load should succeed.
        ASSIGN_OR_ABORT(auto first_load, reader.load(_opts, meta.bitmap_index()));
        ASSERT_TRUE(first_load);

        // After load, tracker should still be unchanged because owned_mem_tracker=false.
        if (tracker != nullptr) {
            ASSERT_EQ(tracker->consumption(), baseline);
        }

        // Verify the reader is functional.
        ASSERT_GT(reader.mem_usage(), sizeof(BitmapIndexReader));
    }
    // After destruction, tracker should remain at baseline.
    if (tracker != nullptr) {
        ASSERT_EQ(tracker->consumption(), baseline);
    }
}

// Verify that BitmapIndexReader with owned_mem_tracker=true (default) properly tracks
// memory via bitmap_index_mem_tracker during construction, load, and destruction.
TEST_F(BitmapIndexTest, test_owned_mem_tracker_true_tracks_memory) {
    auto* tracker = RuntimeEnv::GetInstance()->bitmap_index_mem_tracker();
    int64_t baseline = tracker != nullptr ? tracker->consumption() : 0;

    size_t num_rows = 10;
    int val[10];
    for (int i = 0; i < 10; ++i) val[i] = i;

    std::string file_name = kTestDir + "/owned_true";
    ColumnIndexMetaPB meta;
    write_index_file<TYPE_INT>(file_name, val, num_rows, 0, &meta);

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
    _opts.read_file = rfile.get();

    {
        // Create with default owned_mem_tracker=true.
        BitmapIndexReader reader;

        // Construction should consume sizeof(BitmapIndexReader).
        if (tracker != nullptr) {
            ASSERT_EQ(tracker->consumption() - baseline, static_cast<int64_t>(sizeof(BitmapIndexReader)));
        }

        // Load should succeed.
        ASSIGN_OR_ABORT(auto first_load, reader.load(_opts, meta.bitmap_index()));
        ASSERT_TRUE(first_load);

        // After load, tracker should have consumed the full mem_usage.
        if (tracker != nullptr) {
            ASSERT_EQ(tracker->consumption() - baseline, static_cast<int64_t>(reader.mem_usage()));
            ASSERT_GT(tracker->consumption() - baseline, static_cast<int64_t>(sizeof(BitmapIndexReader)));
        }
    }
    // After destruction, tracker should return to baseline.
    if (tracker != nullptr) {
        ASSERT_EQ(tracker->consumption(), baseline);
    }
}

// Verify that BitmapIndexReader's mem_usage() returns sizeof(BitmapIndexReader) before load
// and includes IndexedColumnReader memory after load.
TEST_F(BitmapIndexTest, test_mem_usage_before_and_after_load) {
    size_t num_rows = 10;
    int val[10];
    for (int i = 0; i < 10; ++i) val[i] = i;

    std::string file_name = kTestDir + "/mem_usage_check";
    ColumnIndexMetaPB meta;
    write_index_file<TYPE_INT>(file_name, val, num_rows, 0, &meta);

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
    _opts.read_file = rfile.get();

    BitmapIndexReader reader;

    // Before load: mem_usage should be sizeof(BitmapIndexReader) since internal readers are null.
    ASSERT_EQ(reader.mem_usage(), sizeof(BitmapIndexReader));
    ASSERT_FALSE(reader.loaded());

    ASSIGN_OR_ABORT(auto first_load, reader.load(_opts, meta.bitmap_index()));
    ASSERT_TRUE(first_load);
    ASSERT_TRUE(reader.loaded());

    // After load: mem_usage should be larger (includes dict and bitmap column readers).
    ASSERT_GT(reader.mem_usage(), sizeof(BitmapIndexReader));
}

// The pages prefetch_dictionary_page() and prefetch_bitmap_pages() ask for, found without any I/O, are exactly the pages
// the lookups and the bitmap reads that follow read, and each page is asked for once.
TEST_F(BitmapIndexTest, prefetch_asks_for_exactly_the_pages_the_lookups_read) {
    ColumnIndexMetaPB meta;
    std::unique_ptr<RecordingBitmapInputStream> stream;
    BitmapIndexReader reader;
    open_prefetch_index("prefetch_exact_pages", &meta, &stream, &reader);
    SegmentBitmapIndexIterator* raw = nullptr;
    ASSERT_OK(reader.new_iterator(_opts, &raw));
    std::unique_ptr<SegmentBitmapIndexIterator> iter(raw);

    // The rows (and ordinals) of the looked-up values: 3 and 4 share a dictionary page and a bitmap page, the others
    // are far apart, so the lookups read more than one page of each but fewer pages than lookups.
    const std::vector<int64_t> lookups{3, 4, kPrefetchRows / 2, kPrefetchRows - 1};
    for (auto row : lookups) {
        int64_t value = prefetch_value(row);
        iter->prefetch_dictionary_page(&value);
    }
    EXPECT_EQ(0, stream->count('r'));
    const auto dict_pages = stream->offsets('w');
    EXPECT_EQ(dict_pages.size(), stream->count('w'));
    EXPECT_GT(dict_pages.size(), 1);
    EXPECT_LT(dict_pages.size(), lookups.size());

    stream->events.clear();
    SparseRange<> ordinals;
    for (auto row : lookups) {
        int64_t value = prefetch_value(row);
        bool exact_match = false;
        ASSERT_OK(iter->seek_dictionary(&value, &exact_match));
        ASSERT_TRUE(exact_match);
        ASSERT_EQ(row, iter->current_ordinal());
        ordinals.add(Range<>(iter->current_ordinal(), iter->current_ordinal() + 1));
    }
    EXPECT_EQ(dict_pages, stream->offsets('r'));

    stream->events.clear();
    iter->prefetch_bitmap_pages(ordinals);
    const rowid_t null_ordinal = iter->bitmap_nums() - 1;
    iter->prefetch_bitmap_pages(SparseRange<>(null_ordinal, null_ordinal + 1));
    EXPECT_EQ(0, stream->count('r'));
    const auto bitmap_pages = stream->offsets('w');
    EXPECT_EQ(bitmap_pages.size(), stream->count('w'));
    EXPECT_GT(bitmap_pages.size(), 1);
    EXPECT_LT(bitmap_pages.size(), lookups.size() + 1);

    stream->events.clear();
    Roaring rows;
    ASSERT_OK(iter->read_union_bitmap(ordinals, &rows));
    Roaring nulls;
    ASSERT_OK(iter->read_null_bitmap(&nulls));
    EXPECT_EQ(bitmap_pages, stream->offsets('r'));
    EXPECT_EQ(lookups.size(), rows.cardinality());
    EXPECT_EQ(1, nulls.cardinality());
    EXPECT_TRUE(nulls.contains(kPrefetchRows));

    EXPECT_EQ(static_cast<int64_t>(dict_pages.size() + bitmap_pages.size()), _stats.bitmap_index_prefetch_pages);
    EXPECT_EQ(static_cast<int64_t>(dict_pages.size() + bitmap_pages.size()), _stats.bitmap_index_prefetch_blocks);

    // A page asked for once is not asked for again.
    stream->events.clear();
    for (auto row : lookups) {
        int64_t value = prefetch_value(row);
        iter->prefetch_dictionary_page(&value);
    }
    iter->prefetch_bitmap_pages(ordinals);
    EXPECT_TRUE(stream->events.empty());
}

// A long ordinal range asks for its first pages only, in order. The cap is lowered to fit the index of the test.
TEST_F(BitmapIndexTest, prefetch_caps_the_bitmap_pages_of_a_long_range) {
    ColumnIndexMetaPB meta;
    std::unique_ptr<RecordingBitmapInputStream> stream;
    BitmapIndexReader reader;
    open_prefetch_index("prefetch_long_range", &meta, &stream, &reader);
    SegmentBitmapIndexIterator* raw = nullptr;
    ASSERT_OK(reader.new_iterator(_opts, &raw));
    std::unique_ptr<SegmentBitmapIndexIterator> iter(raw);

    std::vector<PagePointer> all_pages;
    // Every bitmap of the index: the null bitmap is the last one.
    ASSERT_TRUE(reader_bitmap_pages(reader, 0, iter->bitmap_nums(), &all_pages));
    const size_t expected = 2;
    ASSERT_GT(all_pages.size(), expected);
    iter->_max_prefetch_bitmap_pages = expected;
    iter->prefetch_bitmap_pages(SparseRange<>(0, iter->bitmap_nums()));
    ASSERT_EQ(expected, stream->count('w'));
    for (size_t i = 0; i < expected; i++) {
        EXPECT_EQ(static_cast<int64_t>(all_pages[i].offset), stream->events[i].second);
    }
}

// A file that cannot fill its cache in the background is asked once; the lookups and their results are unchanged.
TEST_F(BitmapIndexTest, prefetch_stops_when_the_file_cannot_fill_in_the_background) {
    ColumnIndexMetaPB meta;
    std::unique_ptr<RecordingBitmapInputStream> stream;
    BitmapIndexReader reader;
    open_prefetch_index("prefetch_unsupported", &meta, &stream, &reader);
    stream->warm_supported = false;
    SegmentBitmapIndexIterator* raw = nullptr;
    ASSERT_OK(reader.new_iterator(_opts, &raw));
    std::unique_ptr<SegmentBitmapIndexIterator> iter(raw);

    for (int64_t row : {3, 99999}) {
        int64_t value = prefetch_value(row);
        iter->prefetch_dictionary_page(&value);
    }
    iter->prefetch_bitmap_pages(SparseRange<>(3, 4));
    EXPECT_EQ(1, stream->count('w'));
    EXPECT_EQ(0, _stats.bitmap_index_prefetch_pages);

    int64_t value = prefetch_value(99999);
    bool exact_match = false;
    ASSERT_OK(iter->seek_dictionary(&value, &exact_match));
    ASSERT_TRUE(exact_match);
    Roaring rows;
    ASSERT_OK(iter->read_bitmap(iter->current_ordinal(), &rows));
    EXPECT_EQ(1, rows.cardinality());
    EXPECT_TRUE(rows.contains(99999));
}

// An iterator created without IndexReadOptions::prefetch_bitmap_index_pages never asks.
TEST_F(BitmapIndexTest, prefetch_is_off_unless_the_options_allow_it) {
    ColumnIndexMetaPB meta;
    std::unique_ptr<RecordingBitmapInputStream> stream;
    BitmapIndexReader reader;
    open_prefetch_index("prefetch_off", &meta, &stream, &reader);
    _opts.prefetch_bitmap_index_pages = false;
    SegmentBitmapIndexIterator* raw = nullptr;
    ASSERT_OK(reader.new_iterator(_opts, &raw));
    std::unique_ptr<SegmentBitmapIndexIterator> iter(raw);
    int64_t value = prefetch_value(3);
    iter->prefetch_dictionary_page(&value);
    iter->prefetch_bitmap_pages(SparseRange<>(3, 4));
    EXPECT_EQ(0, stream->count('w'));
}

// With prefetch, the evaluator asks for every page before it reads it -- all dictionary pages of the IN values before
// the first dictionary lookup, all selected bitmap pages before the first bitmap read -- and gets the same result as
// without.
TEST_F(BitmapIndexTest, evaluator_prefetches_the_pages_before_reading_them) {
    ColumnIndexMetaPB meta;
    std::unique_ptr<RecordingBitmapInputStream> stream;
    BitmapIndexReader reader;
    open_prefetch_index("prefetch_evaluator", &meta, &stream, &reader);

    Schema schema({std::make_shared<Field>(0, "indexed", TYPE_BIGINT, true)});
    // 123456789 is not a value of the index.
    std::unique_ptr<ColumnPredicate> in(
            new_column_in_predicate(get_type_info(TYPE_BIGINT), 0,
                                    {std::to_string(prefetch_value(99999)), std::to_string(prefetch_value(3)),
                                     std::to_string(prefetch_value(40000)), "123456789"}));
    auto evaluate = [&](bool prefetch) {
        PredicateAndNode root;
        root.add_child(PredicateColumnNode{in.get()});
        auto tree = PredicateTree::create(std::move(root));
        BitmapIndexEvaluator evaluator(schema, tree);
        EXPECT_OK(evaluator.init([&](ColumnId) -> StatusOr<BitmapIndexIterator*> {
            BitmapIndexIterator* iter = nullptr;
            RETURN_IF_ERROR(reader.new_iterator(_opts, &iter));
            return iter;
        }));
        EXPECT_TRUE(evaluator.has_bitmap_index());
        SparseRange<> range(0, kPrefetchRows + 1);
        stream->events.clear();
        EXPECT_OK(evaluator.evaluate(range, tree, prefetch));
        return range;
    };

    const auto without = evaluate(false);
    EXPECT_EQ(0, stream->count('w'));
    const auto with = evaluate(true);
    EXPECT_EQ(without, with);
    SparseRange<> expected;
    for (rowid_t row : {3, 40000, 99999}) {
        expected.add(Range<>(row, row + 1));
    }
    EXPECT_EQ(expected, with);

    // Every page read was asked for before; the requests come in two waves, each before the reads it serves.
    std::set<int64_t> asked;
    size_t waves = 0;
    char last = 0;
    for (const auto& [kind, offset] : stream->events) {
        if (kind == 'w') {
            asked.insert(offset);
            if (last != 'w') waves++;
        } else {
            EXPECT_TRUE(asked.count(offset)) << "page at " << offset << " read without being asked for";
        }
        last = kind;
    }
    EXPECT_EQ(2, waves);
    EXPECT_GT(stream->count('r'), 0);
}

// c = v3 AND (a IN (vm, vl) OR b = 2), where b has no bitmap index: the seek does not descend into the OR, which is
// filtered row by row, so the dictionary pages of a's values are not asked for either, whichever child of the OR the
// initializer visits first.
TEST_F(BitmapIndexTest, evaluator_asks_for_no_page_under_an_or_without_bitmap_index) {
    ColumnIndexMetaPB meta;
    std::unique_ptr<RecordingBitmapInputStream> stream;
    BitmapIndexReader reader;
    open_prefetch_index("prefetch_unindexed_or", &meta, &stream, &reader);
    const int64_t c_value = prefetch_value(3);
    const std::vector<int64_t> a_values{prefetch_value(kPrefetchRows / 2), prefetch_value(kPrefetchRows - 1)};
    for (auto value : a_values) {
        ASSERT_NE(reader_dictionary_page(reader, c_value), reader_dictionary_page(reader, value));
    }

    Schema schema({std::make_shared<Field>(0, "c", TYPE_BIGINT, true),
                   std::make_shared<Field>(1, "b", TYPE_BIGINT, true),
                   std::make_shared<Field>(2, "a", TYPE_BIGINT, true)});
    const auto type = get_type_info(TYPE_BIGINT);
    std::unique_ptr<ColumnPredicate> c_eq(new_column_eq_predicate(type, 0, std::to_string(c_value)));
    std::unique_ptr<ColumnPredicate> b_eq(new_column_eq_predicate(type, 1, "2"));
    std::unique_ptr<ColumnPredicate> a_in(
            new_column_in_predicate(type, 2, {std::to_string(a_values[0]), std::to_string(a_values[1])}));
    PredicateOrNode or_node;
    or_node.add_child(PredicateColumnNode{a_in.get()});
    or_node.add_child(PredicateColumnNode{b_eq.get()});
    PredicateAndNode root;
    root.add_child(PredicateColumnNode{c_eq.get()});
    root.add_child(std::move(or_node));
    check_evaluator_reads_every_page_it_asks_for(schema, std::move(root), stream.get(), &reader, 1);
}

// d = v3 AND ((a = vm AND c = vm) OR (a >= v100 AND c >= v100)): the second AND selects too many rows, so the OR is
// not used and the retrieval does not descend into it. The bitmaps of the first AND, which the seek kept, are not
// asked for.
TEST_F(BitmapIndexTest, evaluator_asks_for_no_bitmap_under_an_or_it_does_not_use) {
    ColumnIndexMetaPB meta;
    std::unique_ptr<RecordingBitmapInputStream> stream;
    BitmapIndexReader reader;
    open_prefetch_index("prefetch_unused_or", &meta, &stream, &reader);
    const ordinal_t middle = kPrefetchRows / 2;
    ASSERT_NE(reader_bitmap_page(reader, 3), reader_bitmap_page(reader, middle));
    ASSERT_NE(reader_bitmap_page(reader, kPrefetchRows), reader_bitmap_page(reader, middle)); // the null bitmap

    Schema schema({std::make_shared<Field>(0, "d", TYPE_BIGINT, true),
                   std::make_shared<Field>(1, "a", TYPE_BIGINT, true),
                   std::make_shared<Field>(2, "c", TYPE_BIGINT, true)});
    const auto type = get_type_info(TYPE_BIGINT);
    std::unique_ptr<ColumnPredicate> d_eq(new_column_eq_predicate(type, 0, std::to_string(prefetch_value(3))));
    std::unique_ptr<ColumnPredicate> a_eq(new_column_eq_predicate(type, 1, std::to_string(prefetch_value(middle))));
    std::unique_ptr<ColumnPredicate> c_eq(new_column_eq_predicate(type, 2, std::to_string(prefetch_value(middle))));
    std::unique_ptr<ColumnPredicate> a_ge(new_column_ge_predicate(type, 1, std::to_string(prefetch_value(100))));
    std::unique_ptr<ColumnPredicate> c_ge(new_column_ge_predicate(type, 2, std::to_string(prefetch_value(100))));
    PredicateAndNode selective;
    selective.add_child(PredicateColumnNode{a_eq.get()});
    selective.add_child(PredicateColumnNode{c_eq.get()});
    PredicateAndNode unselective;
    unselective.add_child(PredicateColumnNode{a_ge.get()});
    unselective.add_child(PredicateColumnNode{c_ge.get()});
    PredicateOrNode or_node;
    or_node.add_child(std::move(selective));
    or_node.add_child(std::move(unselective));
    PredicateAndNode root;
    root.add_child(PredicateColumnNode{d_eq.get()});
    root.add_child(std::move(or_node));
    check_evaluator_reads_every_page_it_asks_for(schema, std::move(root), stream.get(), &reader, std::nullopt);
}

} // namespace starrocks
