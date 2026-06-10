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

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <vector>

#include "fs/fs.h"
#include "storage/index/compound_index_common.h"
#include "storage/index/compound_index_file_reader.h"
#include "storage/index/compound_index_file_writer.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/tantivy/tantivy_inverted_reader.h"
#include "storage/index/inverted/tantivy/tantivy_plugin.h"
#include "storage/tablet_index.h"
#include "testutil/assert.h"
#include "util/slice.h"

namespace starrocks {

namespace {

std::string make_tempdir(const std::string& prefix) {
    const char* base = std::getenv("TEST_TMPDIR");
    if (base == nullptr || *base == '\0') base = "/tmp";
    std::string tmpl = std::string(base) + "/" + prefix + "_XXXXXX";
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    char* dir = ::mkdtemp(buf.data());
    return dir ? std::string(dir) : std::string{};
}

struct PathCleanup {
    std::string p;
    ~PathCleanup() {
        std::error_code ec;
        std::filesystem::remove_all(p, ec);
    }
};

CompoundIndexEntry write_segment_index(const std::string& index_dir, const std::vector<Slice>& values, int num_nulls) {
    TypeInfoPtr typeinfo = get_type_info(TYPE_VARCHAR);
    TabletIndex tablet_index;
    tablet_index.add_common_properties(INVERTED_IMP_KEY, TYPE_TANTIVY);

    std::unique_ptr<InvertedWriter> writer;
    EXPECT_OK(TantivyPlugin::get_instance().create_inverted_index_writer(typeinfo, "content", index_dir, &tablet_index,
                                                                         &writer));
    EXPECT_OK(writer->init());

    if (!values.empty()) {
        writer->add_values(values.data(), values.size());
    }
    if (num_nulls > 0) {
        writer->add_nulls(num_nulls);
    }

    auto entry_or = writer->finish_compound(nullptr);
    EXPECT_TRUE(entry_or.ok()) << entry_or.status().message();
    return std::move(entry_or).value();
}

std::string pack_to_idx(const std::string& dir, const std::string& name, std::vector<CompoundIndexEntry>& entries) {
    std::string bin_path = dir + "/" + name;
    auto fs = FileSystem::Default();
    auto wfile = *fs->new_writable_file(bin_path);
    EXPECT_OK(CompoundIndexFileWriter::pack(entries, wfile.get()));
    EXPECT_OK(wfile->close());
    return bin_path;
}

std::string build_file_table_json(const CompoundIndexLayout& layout) {
    std::string json = "{";
    for (size_t i = 0; i < layout.files.size(); ++i) {
        if (i > 0) json += ",";
        json += fmt::format("\"{}\":{{\"offset\":{},\"length\":{}}}", layout.files[i].name, layout.files[i].offset,
                            layout.files[i].length);
    }
    json += "}";
    return json;
}

std::unique_ptr<TantivyInvertedReader> open_compound_reader(const std::string& bin_path,
                                                            const CompoundIndexLayout& layout,
                                                            const std::string& dummy_dir) {
    auto fs = FileSystem::Default();
    TabletIndex ti;
    ti.add_common_properties(INVERTED_IMP_KEY, TYPE_TANTIVY);
    auto ti_sp = std::make_shared<TabletIndex>(ti);
    std::unique_ptr<InvertedReader> reader;
    EXPECT_OK(TantivyPlugin::get_instance().create_inverted_index_reader(dummy_dir, ti_sp, TYPE_VARCHAR, &reader));
    auto* tr = dynamic_cast<TantivyInvertedReader*>(reader.get());
    EXPECT_NE(nullptr, tr);

    auto ra_file = *fs->new_random_access_file(bin_path);

    for (const auto& fe : layout.files) {
        if (fe.name == "_starrocks_null_bitmap" && fe.length > 0) {
            std::vector<char> nbuf(fe.length);
            EXPECT_OK(ra_file->read_at_fully(fe.offset, nbuf.data(), fe.length));
            tr->set_null_bitmap(roaring::Roaring::read(nbuf.data()));
            break;
        }
    }

    auto json = build_file_table_json(layout);
    EXPECT_OK(tr->load_compound(std::move(ra_file), json));
    return std::unique_ptr<TantivyInvertedReader>(dynamic_cast<TantivyInvertedReader*>(reader.release()));
}

} // namespace

// Simulate compaction: 3 input segments → merge values → 1 output segment.
// Verify the output .idx is queryable and covers all input documents.
TEST(TantivyCompactionTest, ThreeSegmentsMergedIntoOne) {
    std::string temp_dir = make_tempdir("tantivy_compaction");
    ASSERT_FALSE(temp_dir.empty());
    PathCleanup cleanup{temp_dir};

    // --- Input segment 0: 3 docs ---
    std::vector<Slice> seg0_values = {Slice("the quick brown fox"), Slice("lazy dog sleeps"), Slice("fox jumps high")};
    auto entry0 = write_segment_index(temp_dir + "/seg0", seg0_values, 0);
    entry0.index_id = 100;

    // --- Input segment 1: 2 docs + 1 null ---
    std::vector<Slice> seg1_values = {Slice("starrocks tantivy search"), Slice("compaction merge test")};
    auto entry1 = write_segment_index(temp_dir + "/seg1", seg1_values, 1);
    entry1.index_id = 100;

    // --- Input segment 2: 2 docs ---
    std::vector<Slice> seg2_values = {Slice("quick search engine"), Slice("brown fox returns")};
    auto entry2 = write_segment_index(temp_dir + "/seg2", seg2_values, 0);
    entry2.index_id = 100;

    // --- Simulate compaction: merge all values into one new writer ---
    std::vector<Slice> merged_values;
    merged_values.insert(merged_values.end(), seg0_values.begin(), seg0_values.end());
    merged_values.insert(merged_values.end(), seg1_values.begin(), seg1_values.end());
    merged_values.insert(merged_values.end(), seg2_values.begin(), seg2_values.end());
    int merged_nulls = 1;

    auto merged_entry = write_segment_index(temp_dir + "/merged", merged_values, merged_nulls);
    merged_entry.index_id = 100;

    // Pack into output .idx.
    std::vector<CompoundIndexEntry> out_entries = {merged_entry};
    std::string out_path = pack_to_idx(temp_dir, "merged.idx", out_entries);

    // Cleanup input temp dirs.
    std::error_code ec;
    std::filesystem::remove_all(temp_dir + "/seg0", ec);
    std::filesystem::remove_all(temp_dir + "/seg1", ec);
    std::filesystem::remove_all(temp_dir + "/seg2", ec);
    std::filesystem::remove_all(temp_dir + "/merged", ec);

    // --- Open and verify the merged .idx ---
    ASSIGN_OR_ABORT(auto compound_file, CompoundIndexFileReader::open(out_path));
    auto layout_or = compound_file->find_index(CompoundIndexKind::INVERTED_TANTIVY, 100);
    ASSERT_TRUE(layout_or.ok()) << layout_or.status().message();
    auto& layout = layout_or.value();

    auto reader = open_compound_reader(out_path, layout, temp_dir + "/dummy");

    // "fox" appears in seg0 doc 0, seg0 doc 2, seg2 doc 1 → merged docs 0, 2, 6
    {
        roaring::Roaring bm;
        Slice term("fox");
        ASSERT_OK(reader->query(nullptr, "content", &term, InvertedIndexQueryType::EQUAL_QUERY, &bm));
        EXPECT_EQ(3u, bm.cardinality());
        EXPECT_TRUE(bm.contains(0));
        EXPECT_TRUE(bm.contains(2));
        EXPECT_TRUE(bm.contains(6));
    }

    // "quick" appears in seg0 doc 0, seg2 doc 0 → merged docs 0, 5
    {
        roaring::Roaring bm;
        Slice term("quick");
        ASSERT_OK(reader->query(nullptr, "content", &term, InvertedIndexQueryType::EQUAL_QUERY, &bm));
        EXPECT_EQ(2u, bm.cardinality());
        EXPECT_TRUE(bm.contains(0));
        EXPECT_TRUE(bm.contains(5));
    }

    // MATCH_ANY "starrocks compaction" → seg1 doc 0 + seg1 doc 1 → merged docs 3, 4
    {
        roaring::Roaring bm;
        Slice text("starrocks compaction");
        ASSERT_OK(reader->query(nullptr, "content", &text, InvertedIndexQueryType::MATCH_ANY_QUERY, &bm));
        EXPECT_EQ(2u, bm.cardinality());
        EXPECT_TRUE(bm.contains(3));
        EXPECT_TRUE(bm.contains(4));
    }

    // MATCH_ALL "brown fox" → seg0 doc 0 + seg2 doc 1 → merged docs 0, 6
    {
        roaring::Roaring bm;
        Slice text("brown fox");
        ASSERT_OK(reader->query(nullptr, "content", &text, InvertedIndexQueryType::MATCH_ALL_QUERY, &bm));
        EXPECT_EQ(2u, bm.cardinality());
        EXPECT_TRUE(bm.contains(0));
        EXPECT_TRUE(bm.contains(6));
    }

    // MATCH_PHRASE "quick brown" → only seg0 doc 0 → merged doc 0
    {
        roaring::Roaring bm;
        PhraseQueryValue pqv;
        pqv.text = Slice("quick brown");
        pqv.slop = 0;
        ASSERT_OK(reader->query(nullptr, "content", &pqv, InvertedIndexQueryType::MATCH_PHRASE_QUERY, &bm));
        EXPECT_EQ(1u, bm.cardinality());
        EXPECT_TRUE(bm.contains(0));
    }

    // Null: 1 null from seg1 → merged doc 7 (7 values + 1 null → doc 7 is null)
    {
        roaring::Roaring bm;
        ASSERT_OK(reader->query_null(nullptr, "content", &bm));
        EXPECT_EQ(1u, bm.cardinality());
        EXPECT_TRUE(bm.contains(7));
    }

    // Total docs = 7 values + 1 null = 8
    // Verify a term that doesn't exist returns empty
    {
        roaring::Roaring bm;
        Slice term("nonexistent");
        ASSERT_OK(reader->query(nullptr, "content", &term, InvertedIndexQueryType::EQUAL_QUERY, &bm));
        EXPECT_EQ(0u, bm.cardinality());
    }
}

// Compaction with multiple indexed columns: verify each column's index is independently queryable.
TEST(TantivyCompactionTest, MultiColumnCompaction) {
    std::string temp_dir = make_tempdir("tantivy_compaction_multicol");
    ASSERT_FALSE(temp_dir.empty());
    PathCleanup cleanup{temp_dir};

    // Input segment with 2 columns.
    // Column A (index_id=200): 3 docs
    auto entryA0 = write_segment_index(temp_dir + "/seg0_colA", {Slice("alpha beta"), Slice("gamma")}, 0);
    entryA0.index_id = 200;
    // Column B (index_id=201): 3 docs (1 null)
    auto entryB0 = write_segment_index(temp_dir + "/seg0_colB", {Slice("one two"), Slice("three")}, 1);
    entryB0.index_id = 201;

    // Second input segment.
    auto entryA1 = write_segment_index(temp_dir + "/seg1_colA", {Slice("beta delta"), Slice("alpha epsilon")}, 0);
    entryA1.index_id = 200;
    auto entryB1 = write_segment_index(temp_dir + "/seg1_colB", {Slice("four five"), Slice("one six")}, 0);
    entryB1.index_id = 201;

    // Merge: column A gets all A values, column B gets all B values.
    auto mergedA =
            write_segment_index(temp_dir + "/merged_A",
                                {Slice("alpha beta"), Slice("gamma"), Slice("beta delta"), Slice("alpha epsilon")}, 0);
    mergedA.index_id = 200;

    auto mergedB = write_segment_index(temp_dir + "/merged_B",
                                       {Slice("one two"), Slice("three"), Slice("four five"), Slice("one six")}, 1);
    mergedB.index_id = 201;

    std::vector<CompoundIndexEntry> out_entries = {mergedA, mergedB};
    std::string out_path = pack_to_idx(temp_dir, "multi_merged.idx", out_entries);

    // Cleanup temp dirs.
    std::error_code ec;
    for (auto& d : {"seg0_colA", "seg0_colB", "seg1_colA", "seg1_colB", "merged_A", "merged_B"}) {
        std::filesystem::remove_all(temp_dir + "/" + d, ec);
    }

    ASSIGN_OR_ABORT(auto compound_file, CompoundIndexFileReader::open(out_path));

    // Query column A: "alpha" → docs 0 and 3
    {
        auto layout_or = compound_file->find_index(CompoundIndexKind::INVERTED_TANTIVY, 200);
        ASSERT_TRUE(layout_or.ok());
        auto reader = open_compound_reader(out_path, layout_or.value(), temp_dir + "/dummyA");

        roaring::Roaring bm;
        Slice term("alpha");
        ASSERT_OK(reader->query(nullptr, "content", &term, InvertedIndexQueryType::EQUAL_QUERY, &bm));
        EXPECT_EQ(2u, bm.cardinality());
        EXPECT_TRUE(bm.contains(0));
        EXPECT_TRUE(bm.contains(3));
    }

    // Query column B: "one" → docs 0 and 3
    {
        auto layout_or = compound_file->find_index(CompoundIndexKind::INVERTED_TANTIVY, 201);
        ASSERT_TRUE(layout_or.ok());
        auto reader = open_compound_reader(out_path, layout_or.value(), temp_dir + "/dummyB");

        roaring::Roaring bm;
        Slice term("one");
        ASSERT_OK(reader->query(nullptr, "content", &term, InvertedIndexQueryType::EQUAL_QUERY, &bm));
        EXPECT_EQ(2u, bm.cardinality());
        EXPECT_TRUE(bm.contains(0));
        EXPECT_TRUE(bm.contains(3));

        // Column B null: 1 null from seg0 → merged doc 4
        roaring::Roaring nbm;
        ASSERT_OK(reader->query_null(nullptr, "content", &nbm));
        EXPECT_EQ(1u, nbm.cardinality());
        EXPECT_TRUE(nbm.contains(4));
    }
}

} // namespace starrocks
