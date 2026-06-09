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

#include "storage/index/inverted/tantivy/tantivy_plugin.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/inverted_index_option.h"
#include "storage/index/inverted/inverted_plugin_factory.h"
#include "storage/rowset/options.h"
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

} // namespace

TEST(TantivyPluginTest, FactoryReturnsPlugin) {
    auto res = InvertedPluginFactory::get_plugin(InvertedImplementType::TANTIVY);
    ASSERT_TRUE(res.ok()) << res.status().message();
    ASSERT_NE(nullptr, res.value());
    EXPECT_EQ(res.value(), &TantivyPlugin::get_instance());
}

TEST(TantivyPluginTest, CreateWriterAndReader) {
    auto* plugin = &TantivyPlugin::get_instance();

    std::string temp_dir = make_tempdir("tantivy_plugin_test");
    ASSERT_FALSE(temp_dir.empty());
    PathCleanup cleanup{temp_dir};

    TypeInfoPtr typeinfo = get_type_info(TYPE_VARCHAR);
    TabletIndex tablet_index;
    tablet_index.add_common_properties(INVERTED_IMP_KEY, TYPE_TANTIVY);

    std::unique_ptr<InvertedWriter> writer;
    ASSERT_OK(plugin->create_inverted_index_writer(typeinfo, "content", temp_dir, &tablet_index, &writer));
    ASSERT_NE(nullptr, writer);

    auto tablet_index_sp = std::make_shared<TabletIndex>(tablet_index);
    std::unique_ptr<InvertedReader> reader;
    ASSERT_OK(plugin->create_inverted_index_reader(temp_dir, tablet_index_sp, TYPE_VARCHAR, &reader));
    ASSERT_NE(nullptr, reader);
}

TEST(TantivyPluginTest, WriteAndFinishCompound) {
    auto* plugin = &TantivyPlugin::get_instance();

    std::string temp_dir = make_tempdir("tantivy_plugin_wr");
    ASSERT_FALSE(temp_dir.empty());
    PathCleanup cleanup{temp_dir};

    TypeInfoPtr typeinfo = get_type_info(TYPE_VARCHAR);
    TabletIndex tablet_index;
    tablet_index.add_common_properties(INVERTED_IMP_KEY, TYPE_TANTIVY);

    std::unique_ptr<InvertedWriter> writer;
    ASSERT_OK(plugin->create_inverted_index_writer(typeinfo, "content", temp_dir, &tablet_index, &writer));
    ASSERT_OK(writer->init());

    // Add some values.
    Slice slices[] = {Slice("hello world"), Slice("tantivy integration"), Slice("starrocks search")};
    writer->add_values(slices, 3);

    // Add null rows.
    writer->add_nulls(2);

    // finish_compound should produce a valid CompoundIndexEntry.
    auto entry_or = writer->finish_compound(nullptr);
    ASSERT_TRUE(entry_or.ok()) << entry_or.status().message();

    auto entry = std::move(entry_or).value();
    EXPECT_EQ(CompoundIndexKind::INVERTED_TANTIVY, entry.kind);
    EXPECT_FALSE(entry.files.empty()) << "tantivy should produce at least one index file";

    // Verify files exist on disk.
    for (const auto& f : entry.files) {
        EXPECT_TRUE(std::filesystem::exists(f.local_path)) << "missing: " << f.local_path;
    }

    // Check null bitmap file is present (we added 2 nulls).
    bool has_null_bitmap = false;
    for (const auto& f : entry.files) {
        if (f.name == "_starrocks_null_bitmap") {
            has_null_bitmap = true;
        }
    }
    EXPECT_TRUE(has_null_bitmap) << "null bitmap file should be produced when nulls were added";

    // tantivy reports `need_compound() == true`; ColumnWriter dispatches
    // accordingly and never invokes the legacy `finish` on this plugin.
    // Calling it directly hits the safety-net error path.
    std::unique_ptr<InvertedWriter> writer2;
    ASSERT_OK(plugin->create_inverted_index_writer(typeinfo, "content", temp_dir + "/w2", &tablet_index, &writer2));
    ASSERT_OK(writer2->init());
    EXPECT_TRUE(writer2->need_compound());
    auto st = writer2->finish(nullptr, nullptr);
    EXPECT_FALSE(st.ok()) << "tantivy::finish must not be called when need_compound() == true";
}

TEST(TantivyPluginTest, ReadAfterWrite) {
    auto* plugin = &TantivyPlugin::get_instance();

    std::string temp_dir = make_tempdir("tantivy_plugin_rw");
    ASSERT_FALSE(temp_dir.empty());
    PathCleanup cleanup{temp_dir};

    TypeInfoPtr typeinfo = get_type_info(TYPE_VARCHAR);
    TabletIndex tablet_index;
    tablet_index.add_common_properties(INVERTED_IMP_KEY, TYPE_TANTIVY);

    // Write phase.
    {
        std::unique_ptr<InvertedWriter> writer;
        ASSERT_OK(plugin->create_inverted_index_writer(typeinfo, "content", temp_dir, &tablet_index, &writer));
        ASSERT_OK(writer->init());

        Slice slices[] = {Slice("the quick brown fox"), Slice("lazy brown dog"), Slice("quick fox jumps")};
        writer->add_values(slices, 3);
        writer->add_nulls(1);

        auto entry_or = writer->finish_compound(nullptr);
        ASSERT_TRUE(entry_or.ok()) << entry_or.status().message();
    }

    // Read phase — load from the same local directory.
    auto tablet_index_sp = std::make_shared<TabletIndex>(tablet_index);
    std::unique_ptr<InvertedReader> reader;
    ASSERT_OK(plugin->create_inverted_index_reader(temp_dir, tablet_index_sp, TYPE_VARCHAR, &reader));

    IndexReadOptions opts;
    ASSERT_OK(reader->load(opts, nullptr));

    // EQUAL_QUERY for "quick" → should match docs 0 and 2.
    {
        roaring::Roaring bm;
        Slice term("quick");
        ASSERT_OK(reader->query(nullptr, "content", &term, InvertedIndexQueryType::EQUAL_QUERY, &bm));
        EXPECT_EQ(bm.cardinality(), 2u);
        EXPECT_TRUE(bm.contains(0));
        EXPECT_TRUE(bm.contains(2));
    }

    // MATCH_ANY_QUERY for "fox dog" → should match docs 0, 1, 2.
    {
        roaring::Roaring bm;
        Slice text("fox dog");
        ASSERT_OK(reader->query(nullptr, "content", &text, InvertedIndexQueryType::MATCH_ANY_QUERY, &bm));
        EXPECT_EQ(bm.cardinality(), 3u);
    }

    // MATCH_ALL_QUERY for "quick fox" → should match docs 0 and 2.
    {
        roaring::Roaring bm;
        Slice text("quick fox");
        ASSERT_OK(reader->query(nullptr, "content", &text, InvertedIndexQueryType::MATCH_ALL_QUERY, &bm));
        EXPECT_EQ(bm.cardinality(), 2u);
        EXPECT_TRUE(bm.contains(0));
        EXPECT_TRUE(bm.contains(2));
    }

    // MATCH_PHRASE_QUERY "quick brown" slop=0 → only doc 0.
    {
        roaring::Roaring bm;
        PhraseQueryValue pqv;
        pqv.text = Slice("quick brown");
        pqv.slop = 0;
        ASSERT_OK(reader->query(nullptr, "content", &pqv, InvertedIndexQueryType::MATCH_PHRASE_QUERY, &bm));
        EXPECT_EQ(bm.cardinality(), 1u);
        EXPECT_TRUE(bm.contains(0));
    }

    // query_null → should return the null row (doc 3).
    {
        roaring::Roaring bm;
        ASSERT_OK(reader->query_null(nullptr, "content", &bm));
        EXPECT_EQ(bm.cardinality(), 1u);
        EXPECT_TRUE(bm.contains(3));
    }
}

TEST(TantivyPluginTest, InitRefusesPreexistingTempDir) {
    auto* plugin = &TantivyPlugin::get_instance();

    // Per-attempt isolation: the temp dir is keyed by tablet_id + txn_id +
    // segment_id + index_id, so a fresh init MUST land on a fresh path.
    // If the directory already exists, init must fail loudly rather than
    // silently overwriting — masking the path-collision class of bug.
    // Crashed-prior-run residue is reclaimed by the vacuum sweep.
    std::string parent = make_tempdir("tantivy_init_preexisting");
    ASSERT_FALSE(parent.empty());
    PathCleanup cleanup{parent};
    std::string temp_dir = parent + "/idx_dir";
    ASSERT_TRUE(std::filesystem::create_directories(temp_dir));

    TypeInfoPtr typeinfo = get_type_info(TYPE_VARCHAR);
    TabletIndex tablet_index;
    tablet_index.add_common_properties(INVERTED_IMP_KEY, TYPE_TANTIVY);

    std::unique_ptr<InvertedWriter> writer;
    ASSERT_OK(plugin->create_inverted_index_writer(typeinfo, "content", temp_dir, &tablet_index, &writer));
    auto st = writer->init();
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is_already_exist()) << "expected AlreadyExist, got: " << st.message();
}

TEST(TantivyPluginTest, DestructorCleansTempDirOnFailedWrite) {
    auto* plugin = &TantivyPlugin::get_instance();

    std::string parent = make_tempdir("tantivy_dtor_clean");
    ASSERT_FALSE(parent.empty());
    PathCleanup cleanup{parent};
    std::string temp_dir = parent + "/idx_dir";

    TypeInfoPtr typeinfo = get_type_info(TYPE_VARCHAR);
    TabletIndex tablet_index;
    tablet_index.add_common_properties(INVERTED_IMP_KEY, TYPE_TANTIVY);

    {
        std::unique_ptr<InvertedWriter> writer;
        ASSERT_OK(plugin->create_inverted_index_writer(typeinfo, "content", temp_dir, &tablet_index, &writer));
        ASSERT_OK(writer->init());
        ASSERT_TRUE(std::filesystem::exists(temp_dir));
        // Drop without finish_compound — simulates a write that errored mid-flight.
    }

    EXPECT_FALSE(std::filesystem::exists(temp_dir))
            << "destructor must clean up _temp_dir when finish_compound was not reached";
}

TEST(TantivyPluginTest, DestructorPreservesTempDirAfterFinishCompound) {
    auto* plugin = &TantivyPlugin::get_instance();

    std::string parent = make_tempdir("tantivy_dtor_keep");
    ASSERT_FALSE(parent.empty());
    PathCleanup cleanup{parent};
    std::string temp_dir = parent + "/idx_dir";

    TypeInfoPtr typeinfo = get_type_info(TYPE_VARCHAR);
    TabletIndex tablet_index;
    tablet_index.add_common_properties(INVERTED_IMP_KEY, TYPE_TANTIVY);

    {
        std::unique_ptr<InvertedWriter> writer;
        ASSERT_OK(plugin->create_inverted_index_writer(typeinfo, "content", temp_dir, &tablet_index, &writer));
        ASSERT_OK(writer->init());
        Slice slices[] = {Slice("alpha")};
        writer->add_values(slices, 1);
        auto entry_or = writer->finish_compound(nullptr);
        ASSERT_TRUE(entry_or.ok()) << entry_or.status().message();
    }

    EXPECT_TRUE(std::filesystem::exists(temp_dir))
            << "destructor must NOT delete _temp_dir after a successful finish_compound; "
               "the compound packing flow owns it from that point on";
}

} // namespace starrocks
