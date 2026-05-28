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

CompoundIndexEntry write_and_pack(const std::string& index_dir, const std::vector<Slice>& values,
                                  int num_nulls) {
    TypeInfoPtr typeinfo = get_type_info(TYPE_VARCHAR);
    TabletIndex tablet_index;
    tablet_index.add_common_properties(INVERTED_IMP_KEY, TYPE_TANTIVY);

    std::unique_ptr<InvertedWriter> writer;
    EXPECT_OK(TantivyPlugin::get_instance().create_inverted_index_writer(typeinfo, "content", index_dir,
                                                                         &tablet_index, &writer));
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

std::string build_file_table_json(const CompoundIndexLayout& layout) {
    std::string json = "{";
    for (size_t i = 0; i < layout.files.size(); ++i) {
        if (i > 0) json += ",";
        json += fmt::format("\"{}\":{{\"offset\":{},\"length\":{}}}", layout.files[i].name,
                            layout.files[i].offset, layout.files[i].length);
    }
    json += "}";
    return json;
}

} // namespace

// Write → pack into .idx → load via compound reader → query all 4 types.
TEST(TantivyCompoundReaderTest, FullRoundTrip) {
    std::string temp_dir = make_tempdir("tantivy_compound_rt");
    ASSERT_FALSE(temp_dir.empty());
    PathCleanup cleanup{temp_dir};

    std::string index_dir = temp_dir + "/index";
    std::vector<Slice> values = {Slice("the quick brown fox"), Slice("lazy brown dog"),
                                 Slice("quick fox jumps"), Slice("starrocks tantivy search")};

    auto entry = write_and_pack(index_dir, values, 1);
    entry.index_id = 100;

    // Pack into compound .idx file.
    std::string bin_path = temp_dir + "/test.idx";
    auto fs = FileSystem::Default();
    ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(bin_path));
    ASSERT_OK(CompoundIndexFileWriter::pack({entry}, wfile.get()));
    ASSERT_OK(wfile->close());

    // Remove the temp index directory (simulating production cleanup).
    std::error_code ec;
    std::filesystem::remove_all(index_dir, ec);
    ASSERT_FALSE(std::filesystem::exists(index_dir));

    // Open compound .idx and parse header.
    ASSIGN_OR_ABORT(auto compound_file, CompoundIndexFileReader::open(bin_path));
    auto layout_or = compound_file->find_index(CompoundIndexKind::INVERTED_TANTIVY, 100);
    ASSERT_TRUE(layout_or.ok()) << layout_or.status().message();
    auto& layout = layout_or.value();

    // Build file table JSON and open RandomAccessFile.
    std::string file_table_json = build_file_table_json(layout);
    ASSIGN_OR_ABORT(auto ra_file, fs->new_random_access_file(bin_path));

    // Create tantivy reader and load via compound path.
    TabletIndex tablet_index;
    tablet_index.add_common_properties(INVERTED_IMP_KEY, TYPE_TANTIVY);
    auto tablet_index_sp = std::make_shared<TabletIndex>(tablet_index);
    std::unique_ptr<InvertedReader> reader;
    ASSERT_OK(TantivyPlugin::get_instance().create_inverted_index_reader(index_dir, tablet_index_sp,
                                                                         TYPE_VARCHAR, &reader));

    auto* tantivy_reader = dynamic_cast<TantivyInvertedReader*>(reader.get());
    ASSERT_NE(nullptr, tantivy_reader);

    // Extract null bitmap from the compound file before passing ra_file to load_compound.
    for (const auto& fe : layout.files) {
        if (fe.name == "_starrocks_null_bitmap" && fe.length > 0) {
            std::vector<char> nbuf(fe.length);
            ASSERT_OK(ra_file->read_at_fully(fe.offset, nbuf.data(), fe.length));
            tantivy_reader->set_null_bitmap(roaring::Roaring::read(nbuf.data()));
            break;
        }
    }

    ASSERT_OK(tantivy_reader->load_compound(std::move(ra_file), file_table_json));

    // EQUAL_QUERY "quick" → docs 0 and 2
    {
        roaring::Roaring bm;
        Slice term("quick");
        ASSERT_OK(tantivy_reader->query(nullptr, "content", &term, InvertedIndexQueryType::EQUAL_QUERY, &bm));
        EXPECT_EQ(2u, bm.cardinality());
        EXPECT_TRUE(bm.contains(0));
        EXPECT_TRUE(bm.contains(2));
    }

    // MATCH_ANY_QUERY "fox dog" → docs 0, 1, 2
    {
        roaring::Roaring bm;
        Slice text("fox dog");
        ASSERT_OK(
                tantivy_reader->query(nullptr, "content", &text, InvertedIndexQueryType::MATCH_ANY_QUERY, &bm));
        EXPECT_EQ(3u, bm.cardinality());
    }

    // MATCH_ALL_QUERY "quick fox" → docs 0 and 2
    {
        roaring::Roaring bm;
        Slice text("quick fox");
        ASSERT_OK(
                tantivy_reader->query(nullptr, "content", &text, InvertedIndexQueryType::MATCH_ALL_QUERY, &bm));
        EXPECT_EQ(2u, bm.cardinality());
        EXPECT_TRUE(bm.contains(0));
        EXPECT_TRUE(bm.contains(2));
    }

    // MATCH_PHRASE_QUERY "quick brown" slop=0 → doc 0 only
    {
        roaring::Roaring bm;
        PhraseQueryValue pqv;
        pqv.text = Slice("quick brown");
        pqv.slop = 0;
        ASSERT_OK(tantivy_reader->query(nullptr, "content", &pqv,
                                        InvertedIndexQueryType::MATCH_PHRASE_QUERY, &bm));
        EXPECT_EQ(1u, bm.cardinality());
        EXPECT_TRUE(bm.contains(0));
    }

    // query_null → doc 4 (5 total: 4 values + 1 null)
    {
        roaring::Roaring bm;
        ASSERT_OK(tantivy_reader->query_null(nullptr, "content", &bm));
        EXPECT_EQ(1u, bm.cardinality());
        EXPECT_TRUE(bm.contains(4));
    }
}

// Multiple indexes in one .idx file.
TEST(TantivyCompoundReaderTest, MultipleIndexesInOneBin) {
    std::string temp_dir = make_tempdir("tantivy_compound_multi");
    ASSERT_FALSE(temp_dir.empty());
    PathCleanup cleanup{temp_dir};

    std::string idx0_dir = temp_dir + "/idx0";
    std::string idx1_dir = temp_dir + "/idx1";

    auto entry0 = write_and_pack(idx0_dir, {Slice("alpha beta gamma"), Slice("delta epsilon")}, 0);
    entry0.index_id = 200;

    auto entry1 = write_and_pack(idx1_dir, {Slice("one two three"), Slice("four five six")}, 1);
    entry1.index_id = 201;

    std::string bin_path = temp_dir + "/multi.idx";
    auto fs = FileSystem::Default();
    ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(bin_path));
    ASSERT_OK(CompoundIndexFileWriter::pack({entry0, entry1}, wfile.get()));
    ASSERT_OK(wfile->close());

    // Remove temp dirs.
    std::error_code ec;
    std::filesystem::remove_all(idx0_dir, ec);
    std::filesystem::remove_all(idx1_dir, ec);

    ASSIGN_OR_ABORT(auto compound_file, CompoundIndexFileReader::open(bin_path));

    // Query index 200: "alpha" → doc 0
    {
        auto layout_or = compound_file->find_index(CompoundIndexKind::INVERTED_TANTIVY, 200);
        ASSERT_TRUE(layout_or.ok());
        auto json = build_file_table_json(layout_or.value());
        ASSIGN_OR_ABORT(auto ra_file, fs->new_random_access_file(bin_path));

        TabletIndex ti;
        ti.add_common_properties(INVERTED_IMP_KEY, TYPE_TANTIVY);
        auto ti_sp = std::make_shared<TabletIndex>(ti);
        std::unique_ptr<InvertedReader> reader;
        ASSERT_OK(TantivyPlugin::get_instance().create_inverted_index_reader(idx0_dir, ti_sp, TYPE_VARCHAR,
                                                                              &reader));
        auto* tr = dynamic_cast<TantivyInvertedReader*>(reader.get());
        ASSERT_OK(tr->load_compound(std::move(ra_file), json));

        roaring::Roaring bm;
        Slice term("alpha");
        ASSERT_OK(tr->query(nullptr, "content", &term, InvertedIndexQueryType::EQUAL_QUERY, &bm));
        EXPECT_EQ(1u, bm.cardinality());
        EXPECT_TRUE(bm.contains(0));
    }

    // Query index 201: "four" → doc 0 of this index
    {
        auto layout_or = compound_file->find_index(CompoundIndexKind::INVERTED_TANTIVY, 201);
        ASSERT_TRUE(layout_or.ok());
        auto& lay = layout_or.value();
        auto json = build_file_table_json(lay);
        ASSIGN_OR_ABORT(auto ra_file, fs->new_random_access_file(bin_path));

        TabletIndex ti;
        ti.add_common_properties(INVERTED_IMP_KEY, TYPE_TANTIVY);
        auto ti_sp = std::make_shared<TabletIndex>(ti);
        std::unique_ptr<InvertedReader> reader;
        ASSERT_OK(TantivyPlugin::get_instance().create_inverted_index_reader(idx1_dir, ti_sp, TYPE_VARCHAR,
                                                                              &reader));
        auto* tr = dynamic_cast<TantivyInvertedReader*>(reader.get());

        for (const auto& fe : lay.files) {
            if (fe.name == "_starrocks_null_bitmap" && fe.length > 0) {
                std::vector<char> nbuf(fe.length);
                ASSERT_OK(ra_file->read_at_fully(fe.offset, nbuf.data(), fe.length));
                tr->set_null_bitmap(roaring::Roaring::read(nbuf.data()));
                break;
            }
        }

        ASSERT_OK(tr->load_compound(std::move(ra_file), json));

        roaring::Roaring bm;
        Slice term("four");
        ASSERT_OK(tr->query(nullptr, "content", &term, InvertedIndexQueryType::EQUAL_QUERY, &bm));
        EXPECT_EQ(1u, bm.cardinality());
        EXPECT_TRUE(bm.contains(1));

        // null bitmap → doc 2 (2 values + 1 null)
        roaring::Roaring nbm;
        ASSERT_OK(tr->query_null(nullptr, "content", &nbm));
        EXPECT_EQ(1u, nbm.cardinality());
        EXPECT_TRUE(nbm.contains(2));
    }
}

} // namespace starrocks
