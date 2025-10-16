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

#ifdef USE_STAROS

#include "storage/lake/tablet_cache_stats_manager.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/options.h"
#include "storage/tablet_schema.h"
#include "test_util.h"
#include "testutil/assert.h"
#include "util/filesystem_util.h"

namespace starrocks::lake {

using namespace starrocks;

class TabletCacheStatsManagerTest : public testing::Test {
public:
    TabletCacheStatsManagerTest() : _test_dir(){};

    void SetUp() override {
        std::vector<starrocks::StorePath> paths;
        CHECK_OK(starrocks::parse_conf_store_paths(starrocks::config::storage_root_path, &paths));
        _test_dir = paths[0].path + "/lake";
        _location_provider = std::make_shared<lake::FixedLocationProvider>(_test_dir);
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->metadata_root_location(1)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->txn_log_root_location(1)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->segment_root_location(1)));
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_manager = new starrocks::lake::TabletManager(_location_provider, _update_manager.get(), 1024 * 1024);
    }

    void TearDown() override {
        delete _tablet_manager;
        FileSystem::Default()->delete_dir_recursive(_test_dir);
    }
    starrocks::lake::TabletManager* _tablet_manager{nullptr};
    std::string _test_dir;
    std::shared_ptr<lake::LocationProvider> _location_provider{nullptr};
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<lake::UpdateManager> _update_manager;
};

TEST_F(TabletCacheStatsManagerTest, basic) {
    lake::TabletCacheStatsManager mgr(_tablet_manager);
    mgr.init();

    std::shared_ptr<TabletMetadata> tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
    std::shared_ptr<TabletSchema> tablet_schema = TabletSchema::create(tablet_metadata->schema());
    std::shared_ptr<Schema> schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));

    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 41, 44};

    std::vector<int> k1{30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41};
    std::vector<int> v1{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    auto c2 = Int32Column::create();
    auto c3 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));
    c2->append_numbers(k1.data(), k1.size() * sizeof(int));
    c3->append_numbers(v1.data(), v1.size() * sizeof(int));

    Chunk chunk0({std::move(c0), std::move(c1)}, schema);
    Chunk chunk1({std::move(c2), std::move(c3)}, schema);

    VersionedTablet tablet(_tablet_manager, tablet_metadata);
    {
        int64_t txn_id = next_id();
        // write rowset 1 with 2 segments
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(WriterType::kHorizontal, txn_id));
        ASSERT_OK(writer->open());

        // write rowset data
        // segment #1
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->write(chunk1));
        ASSERT_OK(writer->finish());

        // segment #2
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->write(chunk1));
        ASSERT_OK(writer->finish());

        auto files = writer->files();
        ASSERT_EQ(2, files.size());

        // add rowset metadata
        auto* rowset = tablet_metadata->add_rowsets();
        rowset->set_overlapped(true);
        rowset->set_id(1);
        auto* segs = rowset->mutable_segments();
        auto* segs_size = rowset->mutable_segment_size();
        for (auto& file : writer->files()) {
            segs->Add(std::move(file.path));
            segs_size->Add(std::move(file.size.value()));
        }

        writer->close();
    }

    // write tablet metadata
    int64_t version = 2;
    tablet_metadata->set_version(version);
    // set fake delvec meta
    auto& item = (*tablet_metadata->mutable_delvec_meta()->mutable_version_to_file())[version];
    item.set_name("delvec");
    item.set_size(10);
    CHECK_OK(_tablet_manager->put_tablet_metadata(*tablet_metadata));

    auto ctx = mgr.submit_get_tablet_cache_stats(tablet_metadata->id(), version);
    auto s = ctx->get();
    EXPECT_TRUE(s.ok());

    ctx = mgr.get_tablet_cache_stats(tablet_metadata->id() + 10086, version);
    s = ctx->get();
    EXPECT_TRUE(s.is_aborted());

    EXPECT_TRUE(mgr.update_max_threads(1).ok());

    mgr.stop();
    ctx = mgr.submit_get_tablet_cache_stats(tablet_metadata->id(), version);
    s = ctx->get();
    EXPECT_FALSE(s.ok());
}

} // namespace starrocks::lake

#endif // USE_STAROS
