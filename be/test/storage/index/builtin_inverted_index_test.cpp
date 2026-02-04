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

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "fs/fs_memory.h"
#include "gen_cpp/segment.pb.h"
#include "roaring/roaring.hh"
#include "storage/index/inverted/builtin/builtin_inverted_index_iterator.h"
#include "storage/index/inverted/builtin/builtin_inverted_reader.h"
#include "storage/index/inverted/builtin/builtin_inverted_writer.h"
#include "storage/index/inverted/builtin/builtin_simple_analyzer.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "storage/tablet_index.h"
#include "storage/types.h"

namespace starrocks {

class BuiltinInvertedIndexTest : public testing::Test {
public:
    const std::string kTestDir = "/builtin_inverted_index_test";

protected:
    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(_fs->create_dir(kTestDir).ok());

        _opts.use_page_cache = true;
        _opts.stats = &_stats;
    }

    void TearDown() override {}

protected:
    std::shared_ptr<MemoryFileSystem> _fs;
    IndexReadOptions _opts;
    OlapReaderStatistics _stats;
};

// Basic sanity test for parser=none: builtin inverted index should behave like a normal
// string bitmap index where each distinct whole value maps to a posting list of rowids.
TEST_F(BuiltinInvertedIndexTest, test_parser_none_equal_query) {
    // Prepare test data: "apple" appears twice, "banana" once.
    std::vector<std::string> values = {"apple", "banana", "apple"};
    std::vector<Slice> slices;
    slices.reserve(values.size());
    for (auto& v : values) {
        slices.emplace_back(v.data(), v.size());
    }

    // Configure a TabletIndex with parser=none.
    TabletIndex tablet_index;
    tablet_index.add_index_properties(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_NONE);

    TypeInfoPtr type_info = get_type_info(TYPE_VARCHAR);

    std::string file_name = kTestDir + "/parser_none";
    ColumnMetaPB meta;
    {
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

        std::unique_ptr<InvertedWriter> writer;
        ASSERT_OK(BuiltinInvertedWriter::create(type_info, &tablet_index, &writer));
        ASSERT_OK(writer->init());

        writer->add_values(slices.data(), slices.size());
        writer->add_nulls(0);
        ASSERT_OK(writer->finish(wfile.get(), &meta));
        ASSERT_TRUE(wfile->close().ok());
    }

    ASSERT_EQ(1, meta.indexes_size());
    const auto& index_meta = meta.indexes(0);
    ASSERT_EQ(BUILTIN_INVERTED_INDEX, index_meta.type());
    ASSERT_TRUE(index_meta.has_builtin_inverted_index());
    const auto& builtin_meta = index_meta.builtin_inverted_index();
    ASSERT_TRUE(builtin_meta.has_bitmap_index());

    // Load builtin inverted index via BuiltinInvertedReader.
    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
    _opts.read_file = rfile.get();
    _opts.segment_rows = slices.size();

    auto tablet_index_sp = std::make_shared<TabletIndex>(tablet_index);
    std::unique_ptr<InvertedReader> reader;
    ASSERT_OK(BuiltinInvertedReader::create(tablet_index_sp, TYPE_VARCHAR, &reader));

    // BuiltinInvertedReader::load expects a non-null meta pointer.
    BuiltinInvertedIndexPB builtin_meta_copy = builtin_meta;
    ASSERT_OK(reader->load(_opts, &builtin_meta_copy));

    InvertedIndexIterator* iter = nullptr;
    ASSERT_OK(reader->new_iterator(tablet_index_sp, &iter, _opts));

    // Query "apple" with EQUAL_QUERY. Should hit rowids 0 and 2.
    roaring::Roaring bitmap;
    Slice query("apple");
    ASSERT_OK(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::EQUAL_QUERY, &bitmap));
    ASSERT_EQ(2, bitmap.cardinality());
    ASSERT_TRUE(bitmap.contains(0));
    ASSERT_TRUE(bitmap.contains(2));

    delete iter;
}

// Test tokenized english parser: verify equality and prefix wildcard behaviour.
TEST_F(BuiltinInvertedIndexTest, test_english_parser_queries) {
    // Values:
    //  row0: "hello world"
    //  row1: "hello"
    //  row2: "world"
    //  row3: "other"
    std::vector<std::string> values = {"hello world", "hello", "world", "other"};
    std::vector<Slice> slices;
    slices.reserve(values.size());
    for (auto& v : values) {
        slices.emplace_back(v.data(), v.size());
    }

    TabletIndex tablet_index;
    tablet_index.add_index_properties(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH);

    TypeInfoPtr type_info = get_type_info(TYPE_VARCHAR);

    std::string file_name = kTestDir + "/english";
    ColumnMetaPB meta;
    {
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

        std::unique_ptr<InvertedWriter> writer;
        ASSERT_OK(BuiltinInvertedWriter::create(type_info, &tablet_index, &writer));
        ASSERT_OK(writer->init());

        writer->add_values(slices.data(), slices.size());
        writer->add_nulls(0);
        ASSERT_OK(writer->finish(wfile.get(), &meta));
        ASSERT_TRUE(wfile->close().ok());
    }

    ASSERT_EQ(1, meta.indexes_size());
    const auto& index_meta = meta.indexes(0);
    ASSERT_EQ(BUILTIN_INVERTED_INDEX, index_meta.type());
    const auto& builtin_meta = index_meta.builtin_inverted_index();

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
    _opts.read_file = rfile.get();
    _opts.segment_rows = slices.size();

    auto tablet_index_sp = std::make_shared<TabletIndex>(tablet_index);
    std::unique_ptr<InvertedReader> reader;
    ASSERT_OK(BuiltinInvertedReader::create(tablet_index_sp, TYPE_VARCHAR, &reader));

    BuiltinInvertedIndexPB builtin_meta_copy = builtin_meta;
    ASSERT_OK(reader->load(_opts, &builtin_meta_copy));

    InvertedIndexIterator* iter = nullptr;
    ASSERT_OK(reader->new_iterator(tablet_index_sp, &iter, _opts));

    // EQUAL_QUERY "hello" should hit rows 0 and 1.
    {
        roaring::Roaring bitmap;
        Slice query("hello");
        ASSERT_OK(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::EQUAL_QUERY, &bitmap));
        ASSERT_EQ(2, bitmap.cardinality());
        ASSERT_TRUE(bitmap.contains(0));
        ASSERT_TRUE(bitmap.contains(1));
    }

    // MATCH_WILDCARD_QUERY "he%" should behave like a prefix query on terms starting with "he".
    {
        roaring::Roaring bitmap;
        Slice query("he%");
        ASSERT_OK(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::MATCH_WILDCARD_QUERY, &bitmap));
        ASSERT_EQ(2, bitmap.cardinality());
        ASSERT_TRUE(bitmap.contains(0));
        ASSERT_TRUE(bitmap.contains(1));
    }

    delete iter;
}

// Test MATCH_ANY and MATCH_ALL semantics for english parser, with and without wildcard tokens.
TEST_F(BuiltinInvertedIndexTest, test_english_parser_match_any_all_queries) {
    // Values:
    //  row0: "hello world"
    //  row1: "hello"
    //  row2: "world"
    //  row3: "other"
    std::vector<std::string> values = {"hello world", "hello", "world", "other"};
    std::vector<Slice> slices;
    slices.reserve(values.size());
    for (auto& v : values) {
        slices.emplace_back(v.data(), v.size());
    }

    TabletIndex tablet_index;
    tablet_index.add_index_properties(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH);

    TypeInfoPtr type_info = get_type_info(TYPE_VARCHAR);

    std::string file_name = kTestDir + "/english_match_any_all";
    ColumnMetaPB meta;
    {
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

        std::unique_ptr<InvertedWriter> writer;
        ASSERT_OK(BuiltinInvertedWriter::create(type_info, &tablet_index, &writer));
        ASSERT_OK(writer->init());

        writer->add_values(slices.data(), slices.size());
        writer->add_nulls(0);
        ASSERT_OK(writer->finish(wfile.get(), &meta));
        ASSERT_TRUE(wfile->close().ok());
    }

    ASSERT_EQ(1, meta.indexes_size());
    const auto& index_meta = meta.indexes(0);
    ASSERT_EQ(BUILTIN_INVERTED_INDEX, index_meta.type());
    const auto& builtin_meta = index_meta.builtin_inverted_index();

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
    _opts.read_file = rfile.get();
    _opts.segment_rows = slices.size();

    auto tablet_index_sp = std::make_shared<TabletIndex>(tablet_index);
    std::unique_ptr<InvertedReader> reader;
    ASSERT_OK(BuiltinInvertedReader::create(tablet_index_sp, TYPE_VARCHAR, &reader));

    BuiltinInvertedIndexPB builtin_meta_copy = builtin_meta;
    ASSERT_OK(reader->load(_opts, &builtin_meta_copy));

    InvertedIndexIterator* iter = nullptr;
    ASSERT_OK(reader->new_iterator(tablet_index_sp, &iter, _opts));

    // MATCH_ANY with exact tokens "hello world" should hit rows 0, 1, and 2.
    {
        roaring::Roaring bitmap;
        Slice query("hello world");
        ASSERT_OK(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::MATCH_ANY_QUERY, &bitmap));
        ASSERT_EQ(3, bitmap.cardinality());
        ASSERT_TRUE(bitmap.contains(0));
        ASSERT_TRUE(bitmap.contains(1));
        ASSERT_TRUE(bitmap.contains(2));
        ASSERT_FALSE(bitmap.contains(3));
    }

    // MATCH_ALL with exact tokens "hello world" should hit only row 0.
    {
        roaring::Roaring bitmap;
        Slice query("hello world");
        ASSERT_OK(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::MATCH_ALL_QUERY, &bitmap));
        ASSERT_EQ(1, bitmap.cardinality());
        ASSERT_TRUE(bitmap.contains(0));
        ASSERT_FALSE(bitmap.contains(1));
        ASSERT_FALSE(bitmap.contains(2));
        ASSERT_FALSE(bitmap.contains(3));
    }

    // MATCH_ANY with wildcard tokens "he% wor%" should behave the same as above.
    {
        roaring::Roaring bitmap;
        std::string pattern = std::string("he% wor%");
        Slice query(pattern);
        ASSERT_OK(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::MATCH_ANY_QUERY, &bitmap));
        ASSERT_EQ(3, bitmap.cardinality());
        ASSERT_TRUE(bitmap.contains(0));
        ASSERT_TRUE(bitmap.contains(1));
        ASSERT_TRUE(bitmap.contains(2));
        ASSERT_FALSE(bitmap.contains(3));
    }

    // MATCH_ALL with wildcard tokens "he% wor%" should also hit only row 0.
    {
        roaring::Roaring bitmap;
        std::string pattern = std::string("he% wor%");
        Slice query(pattern);
        ASSERT_OK(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::MATCH_ALL_QUERY, &bitmap));
        ASSERT_EQ(1, bitmap.cardinality());
        ASSERT_TRUE(bitmap.contains(0));
        ASSERT_FALSE(bitmap.contains(1));
        ASSERT_FALSE(bitmap.contains(2));
        ASSERT_FALSE(bitmap.contains(3));
    }

    delete iter;
}

TEST_F(BuiltinInvertedIndexTest, test_get_next_prefix) {
    // Normal case
    ASSERT_EQ("abd", get_next_prefix(Slice("abc")));

    // Trailing 0xFF case: should increment and truncate
    ASSERT_EQ("b", get_next_prefix(Slice("a\xFF")));
    ASSERT_EQ("ac", get_next_prefix(Slice("ab\xFF")));
    ASSERT_EQ("b", get_next_prefix(Slice("a\xFF\xFF")));

    // All 0xFF case: should return empty string (overflow)
    ASSERT_EQ("", get_next_prefix(Slice("\xFF")));
    ASSERT_EQ("", get_next_prefix(Slice("\xFF\xFF")));

    // Empty prefix
    ASSERT_EQ("", get_next_prefix(Slice("")));
}

// Test wildcard query with various prefixes to verify range calculation and truncation
TEST_F(BuiltinInvertedIndexTest, test_prefix_overflow_query) {
    std::vector<std::string> values = {
            "abc",    "abca",       "abd",                 // Case 1: abc% -> range [abc, abd)
            "a\xFF",  "a\xFF\x01",  "b",                   // Case 2: a\xFF% -> range [a\xFF, b)
            "ab\xFF", "ab\xFF\x01", "ac",                  // Case 3: ab\xFF% -> range [ab\xFF, ac)
            "\xFE",   "\xFF",       "\xFF\x01", "\xFF\xFF" // Case 4 & 5: \xFF% -> [ \xFF, end]
    };
    std::vector<Slice> slices;
    for (auto& v : values) {
        slices.emplace_back(v.data(), v.size());
    }

    TabletIndex tablet_index;
    tablet_index.add_index_properties(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_NONE);
    TypeInfoPtr type_info = get_type_info(TYPE_VARCHAR);
    std::string file_name = kTestDir + "/prefix_overflow";
    ColumnMetaPB meta;
    {
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
        std::unique_ptr<InvertedWriter> writer;
        ASSERT_OK(BuiltinInvertedWriter::create(type_info, &tablet_index, &writer));
        ASSERT_OK(writer->init());
        writer->add_values(slices.data(), slices.size());
        writer->add_nulls(0);
        ASSERT_OK(writer->finish(wfile.get(), &meta));
        ASSERT_TRUE(wfile->close().ok());
    }

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
    _opts.read_file = rfile.get();
    _opts.segment_rows = slices.size();
    auto tablet_index_sp = std::make_shared<TabletIndex>(tablet_index);
    std::unique_ptr<InvertedReader> reader;
    ASSERT_OK(BuiltinInvertedReader::create(tablet_index_sp, TYPE_VARCHAR, &reader));
    BuiltinInvertedIndexPB builtin_meta_copy = meta.indexes(0).builtin_inverted_index();
    ASSERT_OK(reader->load(_opts, &builtin_meta_copy));

    InvertedIndexIterator* iter = nullptr;
    ASSERT_OK(reader->new_iterator(tablet_index_sp, &iter, _opts));

    // 1. Test "abc%"
    {
        roaring::Roaring bitmap;
        Slice query("abc%");
        ASSERT_OK(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::MATCH_WILDCARD_QUERY, &bitmap));
        // Should hit: "abc", "abca" (rowid 0, 1); should NOT include "abd" (rowid 2)
        ASSERT_EQ(2, bitmap.cardinality());
        ASSERT_TRUE(bitmap.contains(0));
        ASSERT_TRUE(bitmap.contains(1));
    }

    // 2. Test "a\xFF%"
    {
        roaring::Roaring bitmap;
        Slice query("a\xFF%");
        ASSERT_OK(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::MATCH_WILDCARD_QUERY, &bitmap));
        // Should hit: "a\xFF", "a\xFF\x01" (rowid 3, 4); should NOT include "b" (rowid 5)
        ASSERT_EQ(2, bitmap.cardinality());
        ASSERT_TRUE(bitmap.contains(3));
        ASSERT_TRUE(bitmap.contains(4));
    }

    // 3. Test "ab\xFF%"
    {
        roaring::Roaring bitmap;
        Slice query("ab\xFF%");
        ASSERT_OK(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::MATCH_WILDCARD_QUERY, &bitmap));
        // Should hit: "ab\xFF", "ab\xFF\x01" (rowid 6, 7); should NOT include "ac" (rowid 8)
        ASSERT_EQ(2, bitmap.cardinality());
        ASSERT_TRUE(bitmap.contains(6));
        ASSERT_TRUE(bitmap.contains(7));
    }

    // 4. Test "\xFF%" (Overflow case)
    {
        roaring::Roaring bitmap;
        Slice query("\xFF%");
        ASSERT_OK(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::MATCH_WILDCARD_QUERY, &bitmap));
        // Should hit all starting with \xFF: "\xFF", "\xFF\x01", "\xFF\xFF" (rowid 10, 11, 12)
        ASSERT_EQ(3, bitmap.cardinality());
        ASSERT_TRUE(bitmap.contains(10));
        ASSERT_TRUE(bitmap.contains(11));
        ASSERT_TRUE(bitmap.contains(12));
    }

    // 5. Test "\xFE%"
    {
        roaring::Roaring bitmap;
        Slice query("\xFE%");
        ASSERT_OK(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::MATCH_WILDCARD_QUERY, &bitmap));
        // Should hit: "\xFE" (rowid 9)
        ASSERT_EQ(1, bitmap.cardinality());
        ASSERT_TRUE(bitmap.contains(9));
    }

    delete iter;
}

// Test wildcard queries with NULL rows to ensure NULLs are never matched by LIKE predicates.
TEST_F(BuiltinInvertedIndexTest, test_wildcard_query_with_nulls) {
    // Row layout (parser = none):
    //  row0: "abc"
    //  row1: NULL
    //  row2: "abd"
    //  row3: "zzz"
    //  row4: NULL
    std::vector<std::string> values = {"abc", "abd", "zzz"};
    std::vector<Slice> slices;
    for (auto& v : values) {
        slices.emplace_back(v.data(), v.size());
    }

    TabletIndex tablet_index;
    tablet_index.add_index_properties(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_NONE);
    TypeInfoPtr type_info = get_type_info(TYPE_VARCHAR);
    std::string file_name = kTestDir + "/wildcard_with_nulls";
    ColumnMetaPB meta;
    {
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
        std::unique_ptr<InvertedWriter> writer;
        ASSERT_OK(BuiltinInvertedWriter::create(type_info, &tablet_index, &writer));
        ASSERT_OK(writer->init());

        // rows: 0:"abc", 1:NULL, 2:"abd", 3:"zzz", 4:NULL
        writer->add_values(&slices[0], 1); // row 0
        writer->add_nulls(1);              // row 1
        writer->add_values(&slices[1], 1); // row 2
        writer->add_values(&slices[2], 1); // row 3
        writer->add_nulls(1);              // row 4

        ASSERT_OK(writer->finish(wfile.get(), &meta));
        ASSERT_TRUE(wfile->close().ok());
    }

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
    _opts.read_file = rfile.get();
    _opts.segment_rows = slices.size();
    auto tablet_index_sp = std::make_shared<TabletIndex>(tablet_index);
    std::unique_ptr<InvertedReader> reader;
    ASSERT_OK(BuiltinInvertedReader::create(tablet_index_sp, TYPE_VARCHAR, &reader));
    BuiltinInvertedIndexPB builtin_meta_copy = meta.indexes(0).builtin_inverted_index();
    ASSERT_OK(reader->load(_opts, &builtin_meta_copy));

    InvertedIndexIterator* iter = nullptr;
    ASSERT_OK(reader->new_iterator(tablet_index_sp, &iter, _opts));

    // 1. "a%" should hit only non-NULL rows starting with "a" (rowids 0 and 2).
    {
        roaring::Roaring bitmap;
        Slice query("a%");
        ASSERT_OK(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::MATCH_WILDCARD_QUERY, &bitmap));
        ASSERT_EQ(2, bitmap.cardinality());
        ASSERT_TRUE(bitmap.contains(0));
        ASSERT_TRUE(bitmap.contains(2));
        ASSERT_FALSE(bitmap.contains(1));
        ASSERT_FALSE(bitmap.contains(3));
        ASSERT_FALSE(bitmap.contains(4));
    }

    // 2. "z%" (max term prefix) should hit only row3 and never include NULLs.
    {
        roaring::Roaring bitmap;
        Slice query("z%");
        ASSERT_OK(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::MATCH_WILDCARD_QUERY, &bitmap));
        ASSERT_EQ(1, bitmap.cardinality());
        ASSERT_TRUE(bitmap.contains(3));
        ASSERT_FALSE(bitmap.contains(0));
        ASSERT_FALSE(bitmap.contains(1));
        ASSERT_FALSE(bitmap.contains(2));
        ASSERT_FALSE(bitmap.contains(4));
    }

    delete iter;
}

// Test prefix overflow case (\xFF%) with NULL rows to ensure NULLs are excluded from results.
TEST_F(BuiltinInvertedIndexTest, test_prefix_overflow_query_with_nulls) {
    // Row layout (parser = none):
    //  row0: "\xFE"
    //  row1: "\xFF"
    //  row2: NULL
    //  row3: "\xFF\x01"
    //  row4: "\xFF\xFF"
    //  row5: NULL
    std::vector<std::string> values = {"\xFE", "\xFF", "\xFF\x01", "\xFF\xFF"};
    std::vector<Slice> slices;
    for (auto& v : values) {
        slices.emplace_back(v.data(), v.size());
    }

    TabletIndex tablet_index;
    tablet_index.add_index_properties(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_NONE);
    TypeInfoPtr type_info = get_type_info(TYPE_VARCHAR);
    std::string file_name = kTestDir + "/prefix_overflow_with_nulls";
    ColumnMetaPB meta;
    {
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
        std::unique_ptr<InvertedWriter> writer;
        ASSERT_OK(BuiltinInvertedWriter::create(type_info, &tablet_index, &writer));
        ASSERT_OK(writer->init());

        writer->add_values(&slices[0], 1); // row 0: "\xFE"
        writer->add_values(&slices[1], 1); // row 1: "\xFF"
        writer->add_nulls(1);              // row 2: NULL
        writer->add_values(&slices[2], 1); // row 3: "\xFF\x01"
        writer->add_values(&slices[3], 1); // row 4: "\xFF\xFF"
        writer->add_nulls(1);              // row 5: NULL

        ASSERT_OK(writer->finish(wfile.get(), &meta));
        ASSERT_TRUE(wfile->close().ok());
    }

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
    _opts.read_file = rfile.get();
    _opts.segment_rows = slices.size();
    auto tablet_index_sp = std::make_shared<TabletIndex>(tablet_index);
    std::unique_ptr<InvertedReader> reader;
    ASSERT_OK(BuiltinInvertedReader::create(tablet_index_sp, TYPE_VARCHAR, &reader));
    BuiltinInvertedIndexPB builtin_meta_copy = meta.indexes(0).builtin_inverted_index();
    ASSERT_OK(reader->load(_opts, &builtin_meta_copy));

    InvertedIndexIterator* iter = nullptr;
    ASSERT_OK(reader->new_iterator(tablet_index_sp, &iter, _opts));

    // "\xFF%" should only hit non-NULL rows whose values start with 0xFF (rowids 1, 3, 4).
    {
        roaring::Roaring bitmap;
        std::string pattern = std::string("\xFF") + "%";
        Slice query(pattern);
        ASSERT_OK(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::MATCH_WILDCARD_QUERY, &bitmap));
        ASSERT_EQ(3, bitmap.cardinality());
        ASSERT_TRUE(bitmap.contains(1));
        ASSERT_TRUE(bitmap.contains(3));
        ASSERT_TRUE(bitmap.contains(4));
        ASSERT_FALSE(bitmap.contains(0));
        ASSERT_FALSE(bitmap.contains(2));
        ASSERT_FALSE(bitmap.contains(5));
    }

    delete iter;
}

// Test BuiltinInvertedReader's query and query_null which should return InternalError
TEST_F(BuiltinInvertedIndexTest, test_reader_unsupported_query) {
    auto tablet_index = std::make_shared<TabletIndex>();
    std::unique_ptr<InvertedReader> reader;
    ASSERT_OK(BuiltinInvertedReader::create(tablet_index, TYPE_VARCHAR, &reader));

    roaring::Roaring bitmap;
    Slice query("test");
    ASSERT_TRUE(reader->query(nullptr, "c0", &query, InvertedIndexQueryType::EQUAL_QUERY, &bitmap).is_internal_error());
    ASSERT_TRUE(reader->query_null(nullptr, "c0", &bitmap).is_internal_error());
    ASSERT_EQ(InvertedIndexReaderType::TEXT, reader->get_inverted_index_reader_type());
}

// Test BuiltinInvertedIndexIterator's read_null and unsupported query types
TEST_F(BuiltinInvertedIndexTest, test_iterator_unsupported_ops) {
    TabletIndex tablet_index;
    tablet_index.add_index_properties(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_NONE);
    TypeInfoPtr type_info = get_type_info(TYPE_VARCHAR);
    std::string file_name = kTestDir + "/unsupported_ops";
    ColumnMetaPB meta;
    {
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
        std::unique_ptr<InvertedWriter> writer;
        ASSERT_OK(BuiltinInvertedWriter::create(type_info, &tablet_index, &writer));
        ASSERT_OK(writer->init());
        Slice val("test");
        writer->add_values(&val, 1);
        ASSERT_OK(writer->finish(wfile.get(), &meta));
    }

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
    _opts.read_file = rfile.get();
    _opts.segment_rows = 1;
    auto tablet_index_sp = std::make_shared<TabletIndex>(tablet_index);
    std::unique_ptr<InvertedReader> reader;
    ASSERT_OK(BuiltinInvertedReader::create(tablet_index_sp, TYPE_VARCHAR, &reader));
    BuiltinInvertedIndexPB builtin_meta_copy = meta.indexes(0).builtin_inverted_index();
    ASSERT_OK(reader->load(_opts, &builtin_meta_copy));

    InvertedIndexIterator* iter = nullptr;
    ASSERT_OK(reader->new_iterator(tablet_index_sp, &iter, _opts));

    roaring::Roaring bitmap;
    ASSERT_TRUE(iter->read_null("c0", &bitmap).is_internal_error());

    Slice query("test");
    ASSERT_TRUE(iter->read_from_inverted_index("c0", &query, static_cast<InvertedIndexQueryType>(-1), &bitmap)
                        .is_invalid_argument());

    delete iter;
}

// Test BuiltinInvertedWriter::create with unsupported types
TEST_F(BuiltinInvertedIndexTest, test_writer_create_unsupported_type) {
    TabletIndex tablet_index;
    TypeInfoPtr type_info = get_type_info(TYPE_INT);
    std::unique_ptr<InvertedWriter> writer;
    ASSERT_FALSE(BuiltinInvertedWriter::create(type_info, &tablet_index, &writer).ok());
}

// Test BuiltinInvertedWriter with TYPE_CHAR and different parsers
TEST_F(BuiltinInvertedIndexTest, test_writer_char_and_parsers) {
    std::vector<InvertedIndexParserType> parsers = {InvertedIndexParserType::PARSER_STANDARD,
                                                    InvertedIndexParserType::PARSER_CHINESE};

    for (auto parser_type : parsers) {
        TabletIndex tablet_index;
        tablet_index.add_index_properties(INVERTED_INDEX_PARSER_KEY, inverted_index_parser_type_to_string(parser_type));
        TypeInfoPtr type_info = get_type_info(TYPE_CHAR);

        std::string file_name = kTestDir + "/writer_test_" + std::to_string(static_cast<int>(parser_type));
        ColumnMetaPB meta;
        {
            ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
            std::unique_ptr<InvertedWriter> writer;
            ASSERT_OK(BuiltinInvertedWriter::create(type_info, &tablet_index, &writer));
            ASSERT_OK(writer->init());

            std::vector<std::string> values = {"Hello World", "你好世界"};
            std::vector<Slice> slices;
            for (auto& v : values) slices.emplace_back(v);

            writer->add_values(slices.data(), slices.size());
            ASSERT_OK(writer->finish(wfile.get(), &meta));
        }
    }
}

// Test BuiltinInvertedIndexIterator with MATCH_ALL and MATCH_ANY mixed tokens
TEST_F(BuiltinInvertedIndexTest, test_mixed_token_queries) {
    std::vector<std::string> values = {"apple banana cherry", "apple fruit", "banana split"};
    std::vector<Slice> slices;
    for (auto& v : values) slices.emplace_back(v);

    TabletIndex tablet_index;
    tablet_index.add_index_properties(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH);
    TypeInfoPtr type_info = get_type_info(TYPE_VARCHAR);
    std::string file_name = kTestDir + "/mixed_tokens";
    ColumnMetaPB meta;
    {
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
        std::unique_ptr<InvertedWriter> writer;
        ASSERT_OK(BuiltinInvertedWriter::create(type_info, &tablet_index, &writer));
        ASSERT_OK(writer->init());
        writer->add_values(slices.data(), slices.size());
        ASSERT_OK(writer->finish(wfile.get(), &meta));
    }

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
    _opts.read_file = rfile.get();
    _opts.segment_rows = slices.size();
    auto tablet_index_sp = std::make_shared<TabletIndex>(tablet_index);
    std::unique_ptr<InvertedReader> reader;
    ASSERT_OK(BuiltinInvertedReader::create(tablet_index_sp, TYPE_VARCHAR, &reader));
    BuiltinInvertedIndexPB builtin_meta_copy = meta.indexes(0).builtin_inverted_index();
    ASSERT_OK(reader->load(_opts, &builtin_meta_copy));

    InvertedIndexIterator* iter = nullptr;
    ASSERT_OK(reader->new_iterator(tablet_index_sp, &iter, _opts));

    // MATCH_ANY: "apple banan%" should hit all 3 rows
    {
        roaring::Roaring bitmap;
        Slice query("apple banan%");
        ASSERT_OK(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::MATCH_ANY_QUERY, &bitmap));
        ASSERT_EQ(3, bitmap.cardinality());
    }

    // MATCH_ALL: "apple fru%" should hit only row 1
    {
        roaring::Roaring bitmap;
        Slice query("apple fru%");
        ASSERT_OK(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::MATCH_ALL_QUERY, &bitmap));
        ASSERT_EQ(1, bitmap.cardinality());
        ASSERT_TRUE(bitmap.contains(1));
    }

    delete iter;
}

// Test BuiltinInvertedIndexIterator with invalid wildcard query (no %)
TEST_F(BuiltinInvertedIndexTest, test_invalid_wildcard) {
    TabletIndex tablet_index;
    tablet_index.add_index_properties(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_NONE);
    TypeInfoPtr type_info = get_type_info(TYPE_VARCHAR);
    std::string file_name = kTestDir + "/invalid_wildcard";
    ColumnMetaPB meta;
    {
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
        std::unique_ptr<InvertedWriter> writer;
        ASSERT_OK(BuiltinInvertedWriter::create(type_info, &tablet_index, &writer));
        ASSERT_OK(writer->init());
        Slice val("test");
        writer->add_values(&val, 1);
        ASSERT_OK(writer->finish(wfile.get(), &meta));
    }

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
    _opts.read_file = rfile.get();
    _opts.segment_rows = 1;
    auto tablet_index_sp = std::make_shared<TabletIndex>(tablet_index);
    std::unique_ptr<InvertedReader> reader;
    ASSERT_OK(BuiltinInvertedReader::create(tablet_index_sp, TYPE_VARCHAR, &reader));
    BuiltinInvertedIndexPB builtin_meta_copy = meta.indexes(0).builtin_inverted_index();
    ASSERT_OK(reader->load(_opts, &builtin_meta_copy));

    InvertedIndexIterator* iter = nullptr;
    ASSERT_OK(reader->new_iterator(tablet_index_sp, &iter, _opts));

    roaring::Roaring bitmap;
    Slice query("test"); // No %
    ASSERT_TRUE(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::MATCH_WILDCARD_QUERY, &bitmap)
                        .is_internal_error());

    delete iter;
}

// Test complex wildcard query like "a%c" to trigger predicate seek
TEST_F(BuiltinInvertedIndexTest, test_complex_wildcard_query) {
    std::vector<std::string> values = {"abc", "acc", "aec", "afc", "bbc"};
    std::vector<Slice> slices;
    for (auto& v : values) slices.emplace_back(v);

    TabletIndex tablet_index;
    tablet_index.add_index_properties(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_NONE);
    TypeInfoPtr type_info = get_type_info(TYPE_VARCHAR);
    std::string file_name = kTestDir + "/complex_wildcard";
    ColumnMetaPB meta;
    {
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
        std::unique_ptr<InvertedWriter> writer;
        ASSERT_OK(BuiltinInvertedWriter::create(type_info, &tablet_index, &writer));
        ASSERT_OK(writer->init());
        writer->add_values(slices.data(), slices.size());

        // Exercise size()
        ASSERT_GT(writer->size(), 0);

        ASSERT_OK(writer->finish(wfile.get(), &meta));
    }

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
    _opts.read_file = rfile.get();
    _opts.segment_rows = slices.size();
    auto tablet_index_sp = std::make_shared<TabletIndex>(tablet_index);
    std::unique_ptr<InvertedReader> reader;
    ASSERT_OK(BuiltinInvertedReader::create(tablet_index_sp, TYPE_VARCHAR, &reader));
    BuiltinInvertedIndexPB builtin_meta_copy = meta.indexes(0).builtin_inverted_index();
    ASSERT_OK(reader->load(_opts, &builtin_meta_copy));

    InvertedIndexIterator* iter = nullptr;
    ASSERT_OK(reader->new_iterator(tablet_index_sp, &iter, _opts));

    // "a%c" should hit abc, acc, aec, afc (rowids 0, 1, 2, 3)
    {
        roaring::Roaring bitmap;
        Slice query("a%c");
        ASSERT_OK(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::MATCH_WILDCARD_QUERY, &bitmap));
        ASSERT_EQ(4, bitmap.cardinality());
        ASSERT_TRUE(bitmap.contains(0));
        ASSERT_TRUE(bitmap.contains(1));
        ASSERT_TRUE(bitmap.contains(2));
        ASSERT_TRUE(bitmap.contains(3));
        ASSERT_FALSE(bitmap.contains(4));
    }

    // Prefix not found case: "z%"
    {
        roaring::Roaring bitmap;
        Slice query("z%");
        ASSERT_OK(iter->read_from_inverted_index("c0", &query, InvertedIndexQueryType::MATCH_WILDCARD_QUERY, &bitmap));
        ASSERT_EQ(0, bitmap.cardinality());
    }

    // Explicitly call close
    ASSERT_OK(iter->close());

    delete iter;
}

TEST_F(BuiltinInvertedIndexTest, test_simple_analyzer) {
    SimpleAnalyzer analyzer(true);
    char text[] = "Hello World 123";
    std::vector<SliceToken> tokens;
    analyzer.tokenize(text, strlen(text), tokens);

    ASSERT_EQ(3, tokens.size());
    ASSERT_EQ("hello", tokens[0].text.to_string());
    ASSERT_EQ("world", tokens[1].text.to_string());
    ASSERT_EQ("123", tokens[2].text.to_string());

    SimpleAnalyzer analyzer2(false);
    char text2[] = "Hello World";
    tokens.clear();
    analyzer2.tokenize(text2, strlen(text2), tokens);
    ASSERT_EQ(2, tokens.size());
    ASSERT_EQ("Hello", tokens[0].text.to_string());
    ASSERT_EQ("World", tokens[1].text.to_string());

    analyzer2.tokenize(nullptr, 0, tokens);
    ASSERT_TRUE(tokens.empty());
}

} // namespace starrocks
