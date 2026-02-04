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

#include "storage/lake/tablet_manager.h"

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <fstream>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "common/config.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/metacache.h"
#include "storage/lake/options.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/options.h"
#include "storage/tablet_schema.h"
#include "test_util.h"
#include "util/bthreads/util.h"
#include "util/failpoint/fail_point.h"
#include "util/filesystem_util.h"

// NOTE: intend to put the following header to the end of the include section
// so that our `gutil/dynamic_annotations.h` takes precedence of the absl's.
// NOLINTNEXTLINE
#include "script/script.h"
#include "service/staros_worker.h"

namespace starrocks {

class LakeTabletManagerTest : public testing::Test {
public:
    LakeTabletManagerTest() : _test_dir(){};

    ~LakeTabletManagerTest() override = default;

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

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, tablet_meta_write_and_read) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);
    auto rowset_meta_pb = metadata.add_rowsets();
    rowset_meta_pb->set_id(2);
    rowset_meta_pb->set_overlapped(false);
    rowset_meta_pb->set_data_size(1024);
    rowset_meta_pb->set_num_rows(5);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));
    string result;
    ASSERT_TRUE(
            execute_script(fmt::format("System.print(StorageEngine.get_lake_tablet_metadata_json({},2))", tablet_id),
                           result)
                    .ok());
    auto res = _tablet_manager->get_tablet_metadata(tablet_id, 2);
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value()->id(), tablet_id);
    EXPECT_EQ(res.value()->version(), 2);
    EXPECT_OK(_tablet_manager->delete_tablet_metadata(tablet_id, 2));
    res = _tablet_manager->get_tablet_metadata(tablet_id, 2);
    EXPECT_TRUE(res.status().is_not_found());
}

TEST_F(LakeTabletManagerTest, tablet_meta_read_corrupted_and_recover) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);
    auto rowset_meta_pb = metadata.add_rowsets();
    rowset_meta_pb->set_id(2);
    rowset_meta_pb->set_overlapped(false);
    rowset_meta_pb->set_data_size(1024);
    rowset_meta_pb->set_num_rows(5);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));
    auto res = _tablet_manager->get_tablet_metadata(tablet_id, 2);
    EXPECT_TRUE(res.ok());
    _tablet_manager->metacache()->prune();
    bool is_first_time = true;
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("ProtobufFile::load::corruption");
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::corrupted_tablet_meta_handler");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    SyncPoint::GetInstance()->SetCallBack("ProtobufFile::load::corruption", [&](void* arg) {
        if (is_first_time) {
            *(Status*)arg = Status::Corruption("injected error");
            is_first_time = false;
        }
    });
    SyncPoint::GetInstance()->SetCallBack("TabletManager::corrupted_tablet_meta_handler",
                                          [](void* arg) { *(Status*)arg = Status::OK(); });
    res = _tablet_manager->get_tablet_metadata(tablet_id, 2);
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value()->id(), tablet_id);
    EXPECT_EQ(res.value()->version(), 2);
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, txnlog_write_and_read) {
    starrocks::TxnLog txnLog;
    auto tablet_id = next_id();
    txnLog.set_tablet_id(tablet_id);
    txnLog.set_txn_id(2);
    EXPECT_OK(_tablet_manager->put_txn_log(txnLog));
    auto res = _tablet_manager->get_txn_log(tablet_id, 2);
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value()->tablet_id(), tablet_id);
    EXPECT_EQ(res.value()->txn_id(), 2);
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, create_tablet) {
    auto fs = FileSystem::Default();
    auto tablet_id = next_id();
    auto schema_id = next_id();

    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.__set_enable_persistent_index(true);
    req.__set_persistent_index_type(TPersistentIndexType::LOCAL);
    req.tablet_schema.__set_id(schema_id);
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);
    EXPECT_OK(_tablet_manager->create_tablet(req));
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    EXPECT_TRUE(fs->path_exists(_location_provider->tablet_metadata_location(tablet_id, 1)).ok());
    EXPECT_TRUE(fs->path_exists(_location_provider->schema_file_location(tablet_id, schema_id)).ok());
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(1));
    EXPECT_EQ(tablet_id, metadata->id());
    EXPECT_EQ(1, metadata->version());
    EXPECT_EQ(1, metadata->next_rowset_id());
    EXPECT_FALSE(metadata->has_commit_time());
    EXPECT_EQ(0, metadata->rowsets_size());
    EXPECT_EQ(0, metadata->cumulative_point());
    EXPECT_FALSE(metadata->has_delvec_meta());
    EXPECT_TRUE(metadata->enable_persistent_index());
    EXPECT_EQ(TPersistentIndexType::LOCAL, metadata->persistent_index_type());
    EXPECT_EQ(CompactionStrategyPB::DEFAULT, metadata->compaction_strategy());
}

TEST_F(LakeTabletManagerTest, create_tablet_enable_tablet_creation_optimization) {
    auto fs = FileSystem::Default();
    auto tablet_id = next_id();
    auto schema_id = next_id();

    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.__set_enable_persistent_index(true);
    req.__set_persistent_index_type(TPersistentIndexType::LOCAL);
    req.__set_enable_tablet_creation_optimization(true);
    req.tablet_schema.__set_id(schema_id);
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);
    EXPECT_OK(_tablet_manager->create_tablet(req));
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    EXPECT_TRUE(fs->path_exists(_tablet_manager->tablet_initial_metadata_location(tablet_id)).ok());
    EXPECT_TRUE(fs->path_exists(_location_provider->schema_file_location(tablet_id, schema_id)).ok());
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(1));
    EXPECT_EQ(tablet_id, metadata->id());
    EXPECT_EQ(1, metadata->version());
    EXPECT_EQ(1, metadata->next_rowset_id());
    EXPECT_FALSE(metadata->has_commit_time());
    EXPECT_EQ(0, metadata->rowsets_size());
    EXPECT_EQ(0, metadata->cumulative_point());
    EXPECT_FALSE(metadata->has_delvec_meta());
    EXPECT_TRUE(metadata->enable_persistent_index());
    EXPECT_EQ(TPersistentIndexType::LOCAL, metadata->persistent_index_type());

    ASSIGN_OR_ABORT(auto meta_iter, _tablet_manager->list_tablet_metadata(tablet_id));
    EXPECT_TRUE(meta_iter.has_next());
    ASSIGN_OR_ABORT(metadata, meta_iter.next());
    EXPECT_EQ(tablet_id, metadata->id());
    EXPECT_EQ(1, metadata->version());
    EXPECT_EQ(1, metadata->next_rowset_id());
    EXPECT_FALSE(metadata->has_commit_time());
    EXPECT_EQ(0, metadata->rowsets_size());
    EXPECT_EQ(0, metadata->cumulative_point());
    EXPECT_FALSE(metadata->has_delvec_meta());
    EXPECT_TRUE(metadata->enable_persistent_index());
    EXPECT_EQ(TPersistentIndexType::LOCAL, metadata->persistent_index_type());
    EXPECT_EQ(CompactionStrategyPB::DEFAULT, metadata->compaction_strategy());
}

TEST_F(LakeTabletManagerTest, create_tablet_with_duplicate_column_id_or_name) {
    auto tablet_id = next_id();
    auto schema_id = next_id();

    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.__set_enable_persistent_index(true);
    req.__set_persistent_index_type(TPersistentIndexType::LOCAL);
    req.tablet_schema.__set_id(schema_id);
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);
    TColumnType col_type;
    col_type.__set_type(TPrimitiveType::SMALLINT);
    req.tablet_schema.columns.resize(2);
    auto& c0 = req.tablet_schema.columns[0];
    c0.__set_is_key(true);
    c0.__set_is_allow_null(false);
    c0.__set_column_name("c0");
    c0.__set_aggregation_type(TAggregationType::NONE);
    c0.__set_col_unique_id(0);
    c0.__set_column_type(col_type);
    auto& c1 = req.tablet_schema.columns[1];
    c1 = c0;
    c1.__set_column_name("c1");
    auto st = _tablet_manager->create_tablet(req);
    ASSERT_EQ(TStatusCode::INVALID_ARGUMENT, st.code());
    ASSERT_TRUE(MatchPattern(st.message(), "*Duplicate column id*")) << st.message();

    c1.__set_col_unique_id(1);
    c1.__set_column_name(c0.column_name);
    st = _tablet_manager->create_tablet(req);
    ASSERT_EQ(TStatusCode::INVALID_ARGUMENT, st.code());
    ASSERT_TRUE(MatchPattern(st.message(), "*Duplicate column name*")) << st.message();
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, create_tablet_without_schema_file) {
    auto fs = FileSystem::Default();

    for (auto create_schema_file : {false, true}) {
        auto tablet_id = next_id();
        auto schema_id = next_id();

        TCreateTabletReq req;
        req.tablet_id = tablet_id;
        req.__set_version(1);
        req.__set_version_hash(0);
        req.tablet_schema.__set_id(schema_id);
        req.tablet_schema.__set_schema_hash(270068375);
        req.tablet_schema.__set_short_key_column_count(2);
        req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);
        req.__set_create_schema_file(create_schema_file);
        EXPECT_OK(_tablet_manager->create_tablet(req));
        EXPECT_TRUE(_tablet_manager->get_tablet(tablet_id).ok());
        EXPECT_TRUE(fs->path_exists(_location_provider->tablet_metadata_location(tablet_id, 1)).ok());
        auto st = fs->path_exists(_location_provider->schema_file_location(tablet_id, schema_id));
        if (create_schema_file) {
            EXPECT_TRUE(st.ok()) << st;
        } else {
            EXPECT_TRUE(st.is_not_found()) << st;
        }
    }
}

TEST_F(LakeTabletManagerTest, create_tablet_with_compaction_strategy) {
    auto fs = FileSystem::Default();
    auto tablet_id = next_id();
    auto schema_id = next_id();
    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.__set_enable_persistent_index(true);
    req.__set_persistent_index_type(TPersistentIndexType::LOCAL);
    req.tablet_schema.__set_id(schema_id);
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);
    req.__set_compaction_strategy(TCompactionStrategy::REAL_TIME);
    EXPECT_OK(_tablet_manager->create_tablet(req));
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    EXPECT_TRUE(fs->path_exists(_location_provider->tablet_metadata_location(tablet_id, 1)).ok());
    EXPECT_TRUE(fs->path_exists(_location_provider->schema_file_location(tablet_id, schema_id)).ok());
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(1));
    EXPECT_EQ(tablet_id, metadata->id());
    EXPECT_EQ(1, metadata->version());
    EXPECT_EQ(1, metadata->next_rowset_id());
    EXPECT_FALSE(metadata->has_commit_time());
    EXPECT_EQ(0, metadata->rowsets_size());
    EXPECT_EQ(0, metadata->cumulative_point());
    EXPECT_FALSE(metadata->has_delvec_meta());
    EXPECT_TRUE(metadata->enable_persistent_index());
    EXPECT_EQ(TPersistentIndexType::LOCAL, metadata->persistent_index_type());
    EXPECT_EQ(CompactionStrategyPB::REAL_TIME, metadata->compaction_strategy());
}

TEST_F(LakeTabletManagerTest, create_tablet_with_range) {
    auto fs = FileSystem::Default();
    auto tablet_id = next_id();
    auto schema_id = next_id();

    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.tablet_schema.__set_id(schema_id);
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

    // Set tablet range with lower and upper bounds
    TTabletRange range;
    TTuple lower_bound;
    TVariant lower_val1;
    TTypeDesc type_desc_int;
    type_desc_int.types.resize(1);
    type_desc_int.types[0].type = TTypeNodeType::SCALAR;
    type_desc_int.types[0].scalar_type.type = TPrimitiveType::INT;
    type_desc_int.types[0].__isset.scalar_type = true;
    type_desc_int.__isset.types = true;
    lower_val1.__set_type(type_desc_int);
    lower_val1.__set_value("100");
    lower_val1.__set_variant_type(TVariantType::NORMAL_VALUE);
    lower_bound.values.push_back(lower_val1);

    TVariant lower_val2;
    TTypeDesc type_desc_varchar;
    type_desc_varchar.types.resize(1);
    type_desc_varchar.types[0].type = TTypeNodeType::SCALAR;
    type_desc_varchar.types[0].scalar_type.type = TPrimitiveType::VARCHAR;
    type_desc_varchar.types[0].scalar_type.len = 10;
    type_desc_varchar.types[0].__isset.scalar_type = true;
    type_desc_varchar.__isset.types = true;
    lower_val2.__set_type(type_desc_varchar);
    lower_val2.__set_value("abc");
    lower_val2.__set_variant_type(TVariantType::NORMAL_VALUE);
    lower_bound.values.push_back(lower_val2);

    TTuple upper_bound;
    TVariant upper_val1;
    upper_val1.__set_type(type_desc_int);
    upper_val1.__set_value("200");
    upper_val1.__set_variant_type(TVariantType::NORMAL_VALUE);
    upper_bound.values.push_back(upper_val1);

    TVariant upper_val2;
    upper_val2.__set_type(type_desc_varchar);
    upper_val2.__set_value("xyz");
    upper_val2.__set_variant_type(TVariantType::NORMAL_VALUE);
    upper_bound.values.push_back(upper_val2);

    range.__set_lower_bound(lower_bound);
    range.__set_upper_bound(upper_bound);
    range.__set_lower_bound_included(true);
    range.__set_upper_bound_included(false);
    req.__set_range(range);

    EXPECT_OK(_tablet_manager->create_tablet(req));
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    EXPECT_TRUE(fs->path_exists(_location_provider->tablet_metadata_location(tablet_id, 1)).ok());
    EXPECT_TRUE(fs->path_exists(_location_provider->schema_file_location(tablet_id, schema_id)).ok());

    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(1));
    EXPECT_EQ(tablet_id, metadata->id());
    EXPECT_EQ(1, metadata->version());

    // Verify range is set correctly
    EXPECT_TRUE(metadata->has_range());
    const auto& pb_range = metadata->range();
    EXPECT_TRUE(pb_range.has_lower_bound());
    EXPECT_EQ(2, pb_range.lower_bound().values_size());
    EXPECT_EQ("100", pb_range.lower_bound().values(0).value());
    EXPECT_EQ("abc", pb_range.lower_bound().values(1).value());

    EXPECT_TRUE(pb_range.has_upper_bound());
    EXPECT_EQ(2, pb_range.upper_bound().values_size());
    EXPECT_EQ("200", pb_range.upper_bound().values(0).value());
    EXPECT_EQ("xyz", pb_range.upper_bound().values(1).value());

    EXPECT_TRUE(pb_range.has_lower_bound_included());
    EXPECT_TRUE(pb_range.lower_bound_included());
    EXPECT_TRUE(pb_range.has_upper_bound_included());
    EXPECT_FALSE(pb_range.upper_bound_included());
}

TEST_F(LakeTabletManagerTest, create_tablet_with_range_null_values) {
    auto fs = FileSystem::Default();
    auto tablet_id = next_id();
    auto schema_id = next_id();

    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.tablet_schema.__set_id(schema_id);
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

    // Set tablet range with NULL values
    TTabletRange range;
    TTuple lower_bound;
    TVariant null_val;
    TTypeDesc type_desc;
    type_desc.types.resize(1);
    type_desc.types[0].type = TTypeNodeType::SCALAR;
    type_desc.types[0].scalar_type.type = TPrimitiveType::INT;
    type_desc.types[0].__isset.scalar_type = true;
    type_desc.__isset.types = true;
    null_val.__set_type(type_desc);
    null_val.__set_value("");
    null_val.__set_variant_type(TVariantType::NULL_VALUE);
    lower_bound.values.push_back(null_val);
    range.__set_lower_bound(lower_bound);
    req.__set_range(range);

    EXPECT_OK(_tablet_manager->create_tablet(req));
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(1));

    // Verify range with NULL value
    EXPECT_TRUE(metadata->has_range());
    const auto& pb_range = metadata->range();
    EXPECT_TRUE(pb_range.has_lower_bound());
    EXPECT_EQ(1, pb_range.lower_bound().values_size());
    EXPECT_EQ(VariantTypePB::NULL_VALUE, pb_range.lower_bound().values(0).variant_type());
}

TEST_F(LakeTabletManagerTest, create_tablet_with_range_min_max_values) {
    auto fs = FileSystem::Default();
    auto tablet_id = next_id();
    auto schema_id = next_id();

    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.tablet_schema.__set_id(schema_id);
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

    // Set tablet range with MIN and MAX values
    TTabletRange range;
    TTuple lower_bound;
    TVariant min_val;
    TTypeDesc type_desc;
    type_desc.types.resize(1);
    type_desc.types[0].type = TTypeNodeType::SCALAR;
    type_desc.types[0].scalar_type.type = TPrimitiveType::BIGINT;
    type_desc.types[0].__isset.scalar_type = true;
    type_desc.__isset.types = true;
    min_val.__set_type(type_desc);
    min_val.__set_value("");
    min_val.__set_variant_type(TVariantType::MINIMUM);
    lower_bound.values.push_back(min_val);

    TTuple upper_bound;
    TVariant max_val;
    max_val.__set_type(type_desc);
    max_val.__set_value("");
    max_val.__set_variant_type(TVariantType::MAXIMUM);
    upper_bound.values.push_back(max_val);

    range.__set_lower_bound(lower_bound);
    range.__set_upper_bound(upper_bound);
    req.__set_range(range);

    EXPECT_OK(_tablet_manager->create_tablet(req));
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(1));

    // Verify range with MIN/MAX values
    EXPECT_TRUE(metadata->has_range());
    const auto& pb_range = metadata->range();
    EXPECT_TRUE(pb_range.has_lower_bound());
    EXPECT_EQ(1, pb_range.lower_bound().values_size());
    EXPECT_EQ(VariantTypePB::MINIMUM, pb_range.lower_bound().values(0).variant_type());

    EXPECT_TRUE(pb_range.has_upper_bound());
    EXPECT_EQ(1, pb_range.upper_bound().values_size());
    EXPECT_EQ(VariantTypePB::MAXIMUM, pb_range.upper_bound().values(0).variant_type());
}

TEST_F(LakeTabletManagerTest, create_tablet_without_range) {
    // Test backward compatibility: create tablet without range
    auto fs = FileSystem::Default();
    auto tablet_id = next_id();
    auto schema_id = next_id();

    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.tablet_schema.__set_id(schema_id);
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);
    // Explicitly not setting range

    EXPECT_OK(_tablet_manager->create_tablet(req));
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(1));

    // Verify no range is set
    EXPECT_FALSE(metadata->has_range());
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, list_tablet_meta) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);
    auto rowset_meta_pb = metadata.add_rowsets();
    rowset_meta_pb->set_id(2);
    rowset_meta_pb->set_overlapped(false);
    rowset_meta_pb->set_data_size(1024);
    rowset_meta_pb->set_num_rows(5);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    metadata.set_version(3);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    metadata.set_id(next_id());
    metadata.set_version(2);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    ASSIGN_OR_ABORT(auto metaIter, _tablet_manager->list_tablet_metadata(tablet_id));

    std::vector<std::string> objects;
    while (metaIter.has_next()) {
        ASSIGN_OR_ABORT(auto tabletmeta_ptr, metaIter.next());
        objects.emplace_back(fmt::format("{:016X}_{:016X}.meta", tabletmeta_ptr->id(), tabletmeta_ptr->version()));
    }

    EXPECT_EQ(objects.size(), 2);
    auto iter = std::find(objects.begin(), objects.end(), fmt::format("{:016X}_{:016X}.meta", tablet_id, 2));
    EXPECT_TRUE(iter != objects.end());
    iter = std::find(objects.begin(), objects.end(), fmt::format("{:016X}_{:016X}.meta", tablet_id, 3));
    EXPECT_TRUE(iter != objects.end());
}

// TODO: enable this test.
// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, DISABLED_put_get_tabletmetadata_witch_cache_evict) {
    int64_t tablet_id = 23456;
    std::vector<TabletMetadataPtr> vec;

    // we set meta cache capacity to 16K, and each meta here cost 232 bytes,putting 64 tablet meta will fill up the cache space.
    for (int i = 0; i < 64; ++i) {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(tablet_id);
        metadata->set_version(2 + i);
        auto rowset_meta_pb = metadata->add_rowsets();
        rowset_meta_pb->set_id(2);
        rowset_meta_pb->set_overlapped(false);
        rowset_meta_pb->set_data_size(1024);
        rowset_meta_pb->set_num_rows(5);
        EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));
        vec.emplace_back(metadata);
    }

    // get version 4 from cache
    {
        auto res = _tablet_manager->get_tablet_metadata(tablet_id, 4);
        EXPECT_TRUE(res.ok());
        EXPECT_EQ(res.value()->id(), tablet_id);
        EXPECT_EQ(res.value()->version(), 4);
    }

    // put another 32 tablet meta to trigger cache eviction.
    for (int i = 0; i < 32; ++i) {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(tablet_id);
        metadata->set_version(66 + i);
        auto rowset_meta_pb = metadata->add_rowsets();
        rowset_meta_pb->set_id(2);
        rowset_meta_pb->set_overlapped(false);
        rowset_meta_pb->set_data_size(1024);
        rowset_meta_pb->set_num_rows(5);
        EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));
    }

    // test eviction result;
    {
        // version 4 expect not evicted
        auto res = _tablet_manager->get_tablet_metadata(tablet_id, 4);
        EXPECT_TRUE(res.ok());
        EXPECT_EQ(res.value()->id(), tablet_id);
        EXPECT_EQ(res.value()->version(), 4);
        EXPECT_EQ(res.value().get(), vec[2].get());
    }
    {
        // version 6 expect evicted
        auto res = _tablet_manager->get_tablet_metadata(tablet_id, 6);
        EXPECT_TRUE(res.ok());
        EXPECT_EQ(res.value()->id(), tablet_id);
        EXPECT_EQ(res.value()->version(), 6);
        EXPECT_NE(res.value().get(), vec[4].get());
    }
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, tablet_schema_load) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);

    auto schema = metadata.mutable_schema();
    schema->set_id(10);
    schema->set_num_short_key_columns(1);
    schema->set_keys_type(DUP_KEYS);
    schema->set_num_rows_per_row_block(65535);
    auto c0 = schema->add_column();
    {
        c0->set_unique_id(0);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
    }
    auto c1 = schema->add_column();
    {
        c1->set_unique_id(1);
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
    }
    ASSERT_OK(_tablet_manager->put_tablet_metadata(metadata));

    const TabletSchema* ptr = nullptr;

    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    {
        auto st = tablet.get_schema();
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(st.value()->id(), 10);
        EXPECT_EQ(st.value()->num_columns(), 2);
        EXPECT_EQ(st.value()->column(0).name(), "c0");
        EXPECT_EQ(st.value()->column(1).name(), "c1");
        ptr = st.value().get();
    }
    {
        auto st = tablet.get_schema();
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(st.value()->id(), 10);
        EXPECT_EQ(st.value()->num_columns(), 2);
        EXPECT_EQ(st.value()->column(0).name(), "c0");
        EXPECT_EQ(st.value()->column(1).name(), "c1");
        EXPECT_EQ(ptr, st.value().get());
    }
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, create_from_base_tablet) {
    // Create base tablet:
    //  - c0 BIGINT KEY
    //  - c1 INT
    //  - c2 FLOAT
    {
        TCreateTabletReq req;
        req.tablet_id = 65535;
        req.__set_version(1);
        req.tablet_schema.__set_id(next_id());
        req.tablet_schema.__set_schema_hash(0);
        req.tablet_schema.__set_short_key_column_count(1);
        req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

        auto& c0 = req.tablet_schema.columns.emplace_back();
        c0.column_name = "c0";
        c0.is_key = true;
        c0.column_type.type = TPrimitiveType::BIGINT;
        c0.is_allow_null = false;

        auto& c1 = req.tablet_schema.columns.emplace_back();
        c1.column_name = "c1";
        c1.is_key = false;
        c1.column_type.type = TPrimitiveType::INT;
        c1.is_allow_null = false;
        c1.default_value = "10";

        auto& c2 = req.tablet_schema.columns.emplace_back();
        c2.column_name = "c2";
        c2.is_key = false;
        c2.column_type.type = TPrimitiveType::FLOAT;
        c2.is_allow_null = false;
        EXPECT_OK(_tablet_manager->create_tablet(req));

        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(65535, 1));
        auto schema = tablet.get_schema();
        ASSERT_EQ(0, schema->column(0).unique_id());
        ASSERT_EQ(1, schema->column(1).unique_id());
        ASSERT_EQ(2, schema->column(2).unique_id());
        ASSERT_EQ(3, schema->next_column_unique_id());
    }
    // Add a new column "c3" based on tablet 65535
    {
        TCreateTabletReq req;
        req.tablet_id = 65536;
        req.__set_version(1);
        req.__set_base_tablet_id(65535);
        req.tablet_schema.__set_id(next_id());
        req.tablet_schema.__set_schema_hash(0);
        req.tablet_schema.__set_short_key_column_count(1);
        req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

        auto& c0 = req.tablet_schema.columns.emplace_back();
        c0.column_name = "c0";
        c0.is_key = true;
        c0.column_type.type = TPrimitiveType::BIGINT;
        c0.is_allow_null = false;

        auto& c3 = req.tablet_schema.columns.emplace_back();
        c3.column_name = "c3";
        c3.is_key = false;
        c3.column_type.type = TPrimitiveType::DOUBLE;
        c3.is_allow_null = false;

        auto& c1 = req.tablet_schema.columns.emplace_back();
        c1.column_name = "c1";
        c1.is_key = false;
        c1.column_type.type = TPrimitiveType::INT;
        c1.is_allow_null = false;
        c1.default_value = "10";

        auto& c2 = req.tablet_schema.columns.emplace_back();
        c2.column_name = "c2";
        c2.is_key = false;
        c2.column_type.type = TPrimitiveType::FLOAT;
        c2.is_allow_null = false;
        EXPECT_OK(_tablet_manager->create_tablet(req));

        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(65536, 1));
        auto schema = tablet.get_schema();
        ASSERT_EQ(0, schema->column(0).unique_id());
        ASSERT_EQ(1, schema->column(1).unique_id());
        ASSERT_EQ(2, schema->column(2).unique_id());
        ASSERT_EQ(3, schema->column(3).unique_id());
        ASSERT_EQ(4, schema->next_column_unique_id());

        ASSERT_EQ("c0", schema->column(0).name());
        ASSERT_EQ("c3", schema->column(1).name());
        ASSERT_EQ("c1", schema->column(2).name());
        ASSERT_EQ("c2", schema->column(3).name());
    }
    // Drop column "c1" based on tablet 65536
    {
        TCreateTabletReq req;
        req.tablet_id = 65537;
        req.__set_version(1);
        req.__set_base_tablet_id(65536);
        req.tablet_schema.__set_id(next_id());
        req.tablet_schema.__set_schema_hash(0);
        req.tablet_schema.__set_short_key_column_count(1);
        req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

        auto& c0 = req.tablet_schema.columns.emplace_back();
        c0.column_name = "c0";
        c0.is_key = true;
        c0.column_type.type = TPrimitiveType::BIGINT;
        c0.is_allow_null = false;

        auto& c3 = req.tablet_schema.columns.emplace_back();
        c3.column_name = "c3";
        c3.is_key = false;
        c3.column_type.type = TPrimitiveType::DOUBLE;
        c3.is_allow_null = false;

        auto& c2 = req.tablet_schema.columns.emplace_back();
        c2.column_name = "c2";
        c2.is_key = false;
        c2.column_type.type = TPrimitiveType::FLOAT;
        c2.is_allow_null = false;
        EXPECT_OK(_tablet_manager->create_tablet(req));

        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(65537, 1));
        auto schema = tablet.get_schema();
        ASSERT_EQ(0, schema->column(0).unique_id());
        ASSERT_EQ(1, schema->column(1).unique_id());
        ASSERT_EQ(2, schema->column(2).unique_id());
        ASSERT_EQ(3, schema->next_column_unique_id());

        ASSERT_EQ("c0", schema->column(0).name());
        ASSERT_EQ("c3", schema->column(1).name());
        ASSERT_EQ("c2", schema->column(2).name());
    }
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, create_tablet_with_cloud_native_persistent_index) {
    auto fs = FileSystem::Default();
    auto tablet_id = next_id();
    auto schema_id = next_id();

    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.__set_enable_persistent_index(true);
    req.__set_persistent_index_type(TPersistentIndexType::CLOUD_NATIVE);
    req.tablet_schema.__set_id(schema_id);
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::PRIMARY_KEYS);
    EXPECT_OK(_tablet_manager->create_tablet(req));
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    EXPECT_TRUE(fs->path_exists(_location_provider->tablet_metadata_location(tablet_id, 1)).ok());
    EXPECT_TRUE(fs->path_exists(_location_provider->schema_file_location(tablet_id, schema_id)).ok());
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(1));
    EXPECT_EQ(tablet_id, metadata->id());
    EXPECT_EQ(1, metadata->version());
    EXPECT_EQ(1, metadata->next_rowset_id());
    EXPECT_FALSE(metadata->has_commit_time());
    EXPECT_EQ(0, metadata->rowsets_size());
    EXPECT_EQ(0, metadata->cumulative_point());
    EXPECT_FALSE(metadata->has_delvec_meta());
    EXPECT_TRUE(metadata->enable_persistent_index());
    EXPECT_EQ(TPersistentIndexType::CLOUD_NATIVE, metadata->persistent_index_type());
}

TEST_F(LakeTabletManagerTest, put_bundle_tablet_metadata) {
    std::map<int64_t, TabletMetadataPB> metadatas;
    TabletSchemaPB schema_pb1;
    {
        schema_pb1.set_id(10);
        schema_pb1.set_num_short_key_columns(1);
        schema_pb1.set_keys_type(DUP_KEYS);
        schema_pb1.set_num_rows_per_row_block(65535);
        auto c0 = schema_pb1.add_column();
        c0->set_unique_id(0);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
    }

    TabletSchemaPB schema_pb2;
    {
        schema_pb2.set_id(11);
        schema_pb2.set_num_short_key_columns(1);
        schema_pb2.set_keys_type(DUP_KEYS);
        schema_pb2.set_num_rows_per_row_block(65535);
        auto c1 = schema_pb2.add_column();
        c1->set_unique_id(1);
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
    }

    TabletSchemaPB schema_pb3;
    {
        schema_pb3.set_id(12);
        schema_pb3.set_num_short_key_columns(1);
        schema_pb3.set_keys_type(DUP_KEYS);
        schema_pb3.set_num_rows_per_row_block(65535);
        auto c2 = schema_pb3.add_column();
        c2->set_unique_id(2);
        c2->set_name("c2");
        c2->set_type("INT");
        c2->set_is_key(false);
        c2->set_is_nullable(false);
    }

    starrocks::TabletMetadataPB metadata1;
    {
        metadata1.set_id(1);
        metadata1.set_version(2);
        metadata1.mutable_schema()->CopyFrom(schema_pb1);
        auto& item1 = (*metadata1.mutable_historical_schemas())[10];
        item1.CopyFrom(schema_pb1);
        auto& item2 = (*metadata1.mutable_historical_schemas())[11];
        item2.CopyFrom(schema_pb2);
        (*metadata1.mutable_rowset_to_schema())[3] = 11;
    }

    starrocks::TabletMetadataPB metadata2;
    {
        metadata2.set_id(2);
        metadata2.set_version(2);
        metadata2.mutable_schema()->CopyFrom(schema_pb2);
        auto& item1 = (*metadata2.mutable_historical_schemas())[10];
        item1.CopyFrom(schema_pb1);
        auto& item2 = (*metadata2.mutable_historical_schemas())[12];
        item2.CopyFrom(schema_pb3);
        (*metadata2.mutable_rowset_to_schema())[3] = 10;
        (*metadata2.mutable_rowset_to_schema())[4] = 12;
    }

    metadatas.emplace(1, metadata1);
    metadatas.emplace(2, metadata2);
    ASSERT_OK(_tablet_manager->put_bundle_tablet_metadata(metadatas));

    {
        auto res = _tablet_manager->get_tablet_metadata(1, 2);
        EXPECT_TRUE(res.ok()) << res.status().to_string();
        TabletMetadataPtr metadata = std::move(res).value();
        ASSERT_EQ(metadata->schema().id(), 10);
        ASSERT_EQ(metadata->historical_schemas_size(), 2);
    }

    {
        _tablet_manager->metacache()->prune();
        // inject corruption error
        SyncPoint::GetInstance()->EnableProcessing();
        bool is_first_time = true;
        DeferOp defer([]() {
            SyncPoint::GetInstance()->ClearCallBack("TabletManager::parse_bundle_tablet_metadata::corruption");
            SyncPoint::GetInstance()->ClearCallBack("TabletManager::corrupted_tablet_meta_handler");
            SyncPoint::GetInstance()->DisableProcessing();
        });
        SyncPoint::GetInstance()->SetCallBack("TabletManager::parse_bundle_tablet_metadata::corruption",
                                              [&](void* arg) {
                                                  if (is_first_time) {
                                                      *(bool*)arg = true;
                                                      is_first_time = false;
                                                  }
                                              });
        SyncPoint::GetInstance()->SetCallBack("TabletManager::corrupted_tablet_meta_handler",
                                              [](void* arg) { *(Status*)arg = Status::OK(); });
        auto res = _tablet_manager->get_tablet_metadata(1, 2);
        EXPECT_TRUE(res.ok()) << res.status().to_string();
        TabletMetadataPtr metadata = std::move(res).value();
        ASSERT_EQ(metadata->schema().id(), 10);
        ASSERT_EQ(metadata->historical_schemas_size(), 2);
    }

    // multi thread read
    {
        std::vector<std::thread> threads;
        for (int i = 0; i < 2; ++i) {
            threads.emplace_back(
                    [&](int i) {
                        auto res = _tablet_manager->get_tablet_metadata(i + 1, 2);
                        ASSERT_TRUE(res.ok());
                        TabletMetadataPtr metadata = std::move(res).value();
                        ASSERT_EQ(metadata->schema().id(), 10 + i);
                        ASSERT_EQ(metadata->historical_schemas_size(), 2 + i);
                        // check id
                        ASSERT_EQ(metadata->id(), i + 1);
                        // check version
                        ASSERT_EQ(metadata->version(), 2);
                    },
                    i);
        }
        for (auto& thread : threads) {
            thread.join();
        }
    }

    {
        // prune metacache
        _tablet_manager->prune_metacache();
        std::string fp_name = "tablet_schema_not_found_in_bundle_metadata";
        auto fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get(fp_name);
        PFailPointTriggerMode trigger_mode;
        trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
        fp->setMode(trigger_mode);
        auto res = _tablet_manager->get_tablet_metadata(2, 2);
        ASSERT_FALSE(res.ok());
        trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
        fp->setMode(trigger_mode);
        res = _tablet_manager->get_tablet_metadata(2, 2);
        ASSERT_TRUE(res.ok());
        TabletMetadataPtr metadata = std::move(res).value();
        ASSERT_EQ(metadata->schema().id(), 11);
        ASSERT_EQ(metadata->historical_schemas_size(), 3);
    }

    starrocks::TabletMetadata metadata4;
    {
        metadata4.set_id(3);
        metadata4.set_version(3);
        metadata4.mutable_schema()->CopyFrom(schema_pb1);
        auto& item1 = (*metadata4.mutable_historical_schemas())[10];
        item1.CopyFrom(schema_pb1);
        auto& item2 = (*metadata4.mutable_historical_schemas())[12];
        item2.CopyFrom(schema_pb3);
    }
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata4));
    auto res = _tablet_manager->get_tablet_metadata(3, 3);
    ASSERT_TRUE(res.ok());

    // get initial tablet meta by path
    starrocks::TabletMetadata metadata5;
    {
        metadata5.set_id(4);
        metadata5.set_version(1);
        metadata5.mutable_schema()->CopyFrom(schema_pb1);
        auto& item1 = (*metadata5.mutable_historical_schemas())[10];
        item1.CopyFrom(schema_pb1);
        auto& item2 = (*metadata5.mutable_historical_schemas())[12];
        item2.CopyFrom(schema_pb3);
    }
    // put initial tablet meta
    EXPECT_OK(_tablet_manager->put_tablet_metadata(std::make_shared<starrocks::TabletMetadata>(metadata5),
                                                   _tablet_manager->tablet_initial_metadata_location(metadata5.id())));
    ASSERT_TRUE(_tablet_manager->get_tablet_metadata(4, 1).ok());
    ASSERT_TRUE(_tablet_manager->get_tablet_metadata(_tablet_manager->tablet_metadata_location(4, 1)).ok());
}

TEST_F(LakeTabletManagerTest, get_inital_tablet_metadata) {
    auto tablet_id = next_id();

    // Create initial tablet metadata without setting tablet id
    starrocks::TabletMetadata initial_metadata;
    initial_metadata.set_version(1);
    initial_metadata.set_next_rowset_id(1);

    // Save it to initial metadata location
    EXPECT_OK(_tablet_manager->put_tablet_metadata(std::make_shared<starrocks::TabletMetadata>(initial_metadata),
                                                   _tablet_manager->tablet_initial_metadata_location(tablet_id)));

    // Get tablet metadata by tablet_id and version
    auto res = _tablet_manager->get_tablet_metadata(tablet_id, 1);
    ASSERT_TRUE(res.ok());

    // Verify that tablet_id is correctly set from the initial metadata
    EXPECT_EQ(res.value()->id(), tablet_id);
    EXPECT_EQ(res.value()->version(), 1);
}

TEST_F(LakeTabletManagerTest, cache_tablet_metadata) {
    auto metadata = std::make_shared<TabletMetadata>();
    auto tablet_id = next_id();
    metadata->set_id(tablet_id);
    metadata->set_version(2);
    ASSERT_TRUE(_tablet_manager->cache_tablet_metadata(metadata).ok());
    auto path = _tablet_manager->tablet_metadata_location(tablet_id, 2);
    ASSERT_TRUE(_tablet_manager->metacache()->lookup_tablet_metadata(path) != nullptr);
    ASSERT_TRUE(_tablet_manager->get_latest_cached_tablet_metadata(tablet_id) != nullptr);
}

TEST_F(LakeTabletManagerTest, get_tablet_metadata_cache_options) {
    auto metadata = std::make_shared<TabletMetadata>();
    auto tablet_id = next_id();
    metadata->set_id(tablet_id);
    metadata->set_version(2);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    auto path = _tablet_manager->tablet_metadata_location(tablet_id, 2);

    // 1. fill_meta_cache=true
    _tablet_manager->metacache()->prune();
    auto res = _tablet_manager->get_tablet_metadata(tablet_id, 2, {true, true});
    EXPECT_TRUE(res.ok());
    EXPECT_TRUE(_tablet_manager->metacache()->lookup_tablet_metadata(path) != nullptr);

    // 2. fill_meta_cache=false
    _tablet_manager->metacache()->prune();
    res = _tablet_manager->get_tablet_metadata(tablet_id, 2, {false, true});
    EXPECT_TRUE(res.ok());
    EXPECT_TRUE(_tablet_manager->metacache()->lookup_tablet_metadata(path) == nullptr);
}

TEST_F(LakeTabletManagerTest, parse_bundle_tablet_metadata_with_zero_size) {
    // Create a corrupted bundle metadata file with bundle_metadata_size = 0
    std::string serialized_string;
    serialized_string.resize(sizeof(uint64_t));
    // Set bundle_metadata_size to 0 in the footer
    encode_fixed64_le((uint8_t*)serialized_string.data(), 0);

    auto res = starrocks::lake::TabletManager::parse_bundle_tablet_metadata("test_path", serialized_string);
    EXPECT_FALSE(res.ok());
    EXPECT_TRUE(res.status().is_corruption());
}

TEST_F(LakeTabletManagerTest, get_single_tablet_metadata_parse_failure) {
    // First, create a valid bundle metadata to get the file path
    auto tablet_id = next_id();
    std::map<int64_t, TabletMetadataPB> metadatas;
    TabletSchemaPB schema_pb;
    {
        schema_pb.set_id(10);
        schema_pb.set_num_short_key_columns(1);
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_num_rows_per_row_block(65535);
        auto c0 = schema_pb.add_column();
        c0->set_unique_id(0);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
    }

    starrocks::TabletMetadataPB metadata1;
    {
        metadata1.set_id(tablet_id);
        metadata1.set_version(2);
        metadata1.mutable_schema()->CopyFrom(schema_pb);
        auto& item1 = (*metadata1.mutable_historical_schemas())[10];
        item1.CopyFrom(schema_pb);
    }

    metadatas.emplace(tablet_id, metadata1);
    ASSERT_OK(_tablet_manager->put_bundle_tablet_metadata(metadatas));

    // Read the bundle file and corrupt it
    auto bundle_path = _tablet_manager->bundle_tablet_metadata_location(tablet_id, 2);
    auto fs = FileSystem::Default();
    ASSIGN_OR_ABORT(auto read_file, fs->new_random_access_file(bundle_path));
    ASSIGN_OR_ABORT(auto content, read_file->read_all());

    // Corrupt the tablet metadata portion by replacing with invalid data
    // Keep the bundle metadata at the end intact, but corrupt the tablet data
    // Replace the first part (tablet metadata) with garbage
    for (size_t i = 0; i < 5; i++) {
        content[i] = static_cast<char>(0xFF);
    }

    // Write the corrupted content back
    ASSERT_OK(fs->delete_file(bundle_path));
    ASSIGN_OR_ABORT(auto write_file, fs->new_writable_file(bundle_path));
    ASSERT_OK(write_file->append(content));
    ASSERT_OK(write_file->close());

    // Clear cache to force reload from disk
    _tablet_manager->metacache()->prune();

    // Use sync point to mock corrupted_tablet_meta_handler
    SyncPoint::GetInstance()->EnableProcessing();
    bool handler_called = false;
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::corrupted_tablet_meta_handler");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    SyncPoint::GetInstance()->SetCallBack("TabletManager::corrupted_tablet_meta_handler", [&](void* arg) {
        handler_called = true;
        // Return OK to allow the code to continue, but the parse will still fail
        *(Status*)arg = Status::OK();
    });

    // Try to get metadata - should fail with corruption error
    auto res = _tablet_manager->get_tablet_metadata(tablet_id, 2);

    // Should fail due to parse error
    EXPECT_FALSE(res.ok());
    EXPECT_TRUE(res.status().is_corruption());
    EXPECT_TRUE(handler_called) << "corrupted_tablet_meta_handler should have been called";
}

namespace {
class PartitionedLocationProvider : public lake::LocationProvider {
public:
    PartitionedLocationProvider(std::string root_dir, int num_partition)
            : _root_dir(std::move(root_dir)), _num_partition(num_partition) {
        for (int i = 0; i < _num_partition; i++) {
            auto dir = lake::join_path(_root_dir, std::to_string(i));
            CHECK_OK(fs::create_directories(lake::join_path(dir, lake::kMetadataDirectoryName)));
        }
    }

    std::string root_location(int64_t tablet_id) const override {
        return lake::join_path(_root_dir, std::to_string(tablet_id % _num_partition));
    }

private:
    std::string _root_dir;
    const int _num_partition;
};

TCreateTabletReq build_create_tablet_request(int64_t tablet_id, int64_t index_id) {
    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.tablet_schema.__set_id(index_id);
    req.tablet_schema.__set_schema_hash(0);
    req.tablet_schema.__set_short_key_column_count(1);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

    auto& c0 = req.tablet_schema.columns.emplace_back();
    c0.column_name = "c0";
    c0.is_key = true;
    c0.column_type.type = TPrimitiveType::BIGINT;
    c0.is_allow_null = false;

    return req;
}

} // namespace

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, test_multi_partition_schema_file) {
    const static int kNumPartition = 4;
    const static int64_t kIndexId = 123454321;
    auto lp = std::make_shared<PartitionedLocationProvider>(_test_dir, kNumPartition);
    _tablet_manager->TEST_set_location_provider(lp);
    for (int i = 0; i < 10; i++) {
        auto req = build_create_tablet_request(next_id(), kIndexId);
        ASSERT_OK(_tablet_manager->create_tablet(req));
    }
    for (int i = 0; i < kNumPartition; i++) {
        auto partition_dir = lake::join_path(_test_dir, std::to_string(i));
        auto schema_file_path = lake::join_path(partition_dir, lake::schema_filename(kIndexId));
        EXPECT_TRUE(fs::path_exist(schema_file_path)) << schema_file_path;
    }
}

TEST_F(LakeTabletManagerTest, test_get_schema_file_concurrently) {
    auto tablet_id = next_id();
    auto schema_id = next_id();
    auto req = build_create_tablet_request(tablet_id, schema_id);
    ASSERT_OK(_tablet_manager->create_tablet(req));
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    ASSIGN_OR_ABORT(auto schema, tablet.get_schema_by_id(schema_id));

    auto fn_read_schema = [&]() {
        for (int i = 0; i < 50; i++) {
            ASSIGN_OR_ABORT(auto real_schema, tablet.get_schema_by_id(schema_id));
            EXPECT_EQ(*schema, *real_schema);
            _tablet_manager->metacache()->prune();
        }
    };

    auto pthreads = std::vector<std::thread>{};
    pthreads.reserve(10);
    for (int i = 0; i < 10; i++) {
        pthreads.emplace_back(fn_read_schema);
    }

    auto bthreads = std::vector<bthread_t>{};
    bthreads.reserve(10);
    for (int i = 0; i < 10; i++) {
        ASSIGN_OR_ABORT(auto bid, bthreads::start_bthread(fn_read_schema));
        bthreads.emplace_back(bid);
    }

    for (auto&& t : pthreads) {
        t.join();
    }
    for (auto&& t : bthreads) {
        bthread_join(t, nullptr);
    }
}

#ifdef USE_STAROS
class MockStarOSWorker : public StarOSWorker {
public:
    MOCK_METHOD((absl::StatusOr<staros::starlet::ShardInfo>), _fetch_shard_info_from_remote,
                (staros::starlet::ShardId id));
};

TEST_F(LakeTabletManagerTest, tablet_schema_load_from_remote) {
    auto tablet_id = next_id();
    int64_t schema_id = 10086;

    TabletSchemaPB schema_pb;
    schema_pb.set_id(10);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_rows_per_row_block(65535);
    auto c0 = schema_pb.add_column();
    {
        c0->set_unique_id(0);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
    }
    auto c1 = schema_pb.add_column();
    {
        c1->set_unique_id(1);
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
    }
    // prepare and set schema info in global_schema_cache
    auto schema_ptr = TabletSchema::create(schema_pb);
    _tablet_manager->TEST_set_global_schema_cache(schema_id, schema_ptr);

    // prepare fake shard_info returned by mocked `_fetch_shard_info_from_remote()`
    staros::starlet::ShardInfo shard_info;
    shard_info.id = tablet_id;
    shard_info.properties.emplace("indexId", std::to_string(schema_id));

    // preserve original g_worker value, and reset it to our MockedWorker
    std::shared_ptr<StarOSWorker> origin_worker = g_worker;
    g_worker.reset(new MockStarOSWorker());
    DeferOp op([origin_worker] { g_worker = origin_worker; });

    // set mock function excepted call
    MockStarOSWorker* worker = dynamic_cast<MockStarOSWorker*>(g_worker.get());
    EXPECT_CALL(*worker, _fetch_shard_info_from_remote(tablet_id)).WillOnce(::testing::Return(shard_info));

    // fire the testing
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    auto st = tablet.get_schema();
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(st.value()->id(), 10);
    EXPECT_EQ(st.value()->num_columns(), 2);
    EXPECT_EQ(st.value()->column(0).name(), "c0");
    EXPECT_EQ(st.value()->column(1).name(), "c1");
}

TEST_F(LakeTabletManagerTest, test_in_writing_data_size) {
    ASSERT_EQ(_tablet_manager->in_writing_data_size(1), 0);
    _tablet_manager->add_in_writing_data_size(1, 100);

    _tablet_manager->add_in_writing_data_size(1, 100);
    _tablet_manager->clean_in_writing_data_size();
    ASSERT_EQ(_tablet_manager->in_writing_data_size(1), 200);

    // preserve original g_worker value, and reset it to our MockedWorker
    std::shared_ptr<StarOSWorker> origin_worker = g_worker;
    g_worker.reset(new MockStarOSWorker());
    DeferOp op([origin_worker] { g_worker = origin_worker; });

    _tablet_manager->clean_in_writing_data_size();
    ASSERT_EQ(_tablet_manager->in_writing_data_size(1), 0);
}

TEST_F(LakeTabletManagerTest, test_get_output_rorwset_schema) {
    std::shared_ptr<TabletMetadata> tablet_metadata = lake::generate_simple_tablet_metadata(DUP_KEYS);
    for (int i = 0; i < 5; i++) {
        auto rs = tablet_metadata->add_rowsets();
        rs->set_id(next_id());
    }

    // set historical schema
    auto schema_id1 = next_id();
    auto& schema_pb1 = (*tablet_metadata->mutable_historical_schemas())[schema_id1];
    schema_pb1.set_id(schema_id1);
    schema_pb1.set_schema_version(0);

    auto schema_id2 = next_id();
    auto& schema_pb2 = (*tablet_metadata->mutable_historical_schemas())[schema_id2];
    schema_pb2.set_id(schema_id2);
    schema_pb2.set_schema_version(1);

    auto schema_id3 = tablet_metadata->schema().id();
    auto& schema_pb3 = (*tablet_metadata->mutable_historical_schemas())[schema_id3];
    schema_pb3.set_id(schema_id3);
    schema_pb3.set_schema_version(2);

    (*tablet_metadata->mutable_rowset_to_schema())[tablet_metadata->rowsets(0).id()] = schema_id3;
    (*tablet_metadata->mutable_rowset_to_schema())[tablet_metadata->rowsets(1).id()] = schema_id1;
    (*tablet_metadata->mutable_rowset_to_schema())[tablet_metadata->rowsets(2).id()] = schema_id3;
    (*tablet_metadata->mutable_rowset_to_schema())[tablet_metadata->rowsets(3).id()] = schema_id2;
    (*tablet_metadata->mutable_rowset_to_schema())[tablet_metadata->rowsets(4).id()] = schema_id3;
    lake::VersionedTablet tablet(_tablet_manager, tablet_metadata);

    {
        for (int i = 0; i < 5; i++) {
            std::vector<uint32_t> input_rowsets;
            input_rowsets.emplace_back(tablet_metadata->rowsets(i).id());
            auto res = _tablet_manager->get_output_rowset_schema(input_rowsets, tablet_metadata.get());
            ASSERT_TRUE(res.ok());
            auto schema_id = tablet_metadata->rowset_to_schema().at(tablet_metadata->rowsets(i).id());
            ASSERT_EQ(res.value()->id(), schema_id);
        }
    }

    auto rs1 = std::make_shared<lake::Rowset>(_tablet_manager, tablet_metadata, 0, 0 /* compaction_segment_limit */);
    auto rs2 = std::make_shared<lake::Rowset>(_tablet_manager, tablet_metadata, 1, 0 /* compaction_segment_limit */);
    auto rs3 = std::make_shared<lake::Rowset>(_tablet_manager, tablet_metadata, 2, 0 /* compaction_segment_limit */);
    auto rs4 = std::make_shared<lake::Rowset>(_tablet_manager, tablet_metadata, 3, 0 /* compaction_segment_limit */);
    auto rs5 = std::make_shared<lake::Rowset>(_tablet_manager, tablet_metadata, 4, 0 /* compaction_segment_limit */);

    {
        std::vector<uint32_t> input_rowsets;
        input_rowsets.emplace_back(tablet_metadata->rowsets(0).id());
        input_rowsets.emplace_back(tablet_metadata->rowsets(1).id());
        auto res = _tablet_manager->get_output_rowset_schema(input_rowsets, tablet_metadata.get());
        ASSERT_TRUE(res.ok());
        ASSERT_EQ(res.value()->id(), schema_id3);

        input_rowsets.emplace_back(tablet_metadata->rowsets(2).id());
        res = _tablet_manager->get_output_rowset_schema(input_rowsets, tablet_metadata.get());
        ASSERT_TRUE(res.ok());
        ASSERT_EQ(res.value()->id(), schema_id3);

        input_rowsets.clear();
        input_rowsets.emplace_back(tablet_metadata->rowsets(3).id());
        input_rowsets.emplace_back(tablet_metadata->rowsets(1).id());
        res = _tablet_manager->get_output_rowset_schema(input_rowsets, tablet_metadata.get());
        ASSERT_TRUE(res.ok());
        ASSERT_EQ(res.value()->id(), schema_id2);
    }

    {
        tablet_metadata->mutable_rowset_to_schema()->clear();
        for (int i = 0; i < 5; i++) {
            std::vector<uint32_t> input_rowsets;
            input_rowsets.emplace_back(tablet_metadata->rowsets(i).id());
            auto res = _tablet_manager->get_output_rowset_schema(input_rowsets, tablet_metadata.get());
            ASSERT_TRUE(res.ok());
            ASSERT_EQ(res.value()->id(), tablet_metadata->schema().id());
        }
    }
}

TEST_F(LakeTabletManagerTest, capture_tablet_and_rowsets) {
    starrocks::TabletMetadata metadata;
    auto schema = metadata.mutable_schema();
    schema->set_id(1);
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(1);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    metadata.set_version(2);
    auto rowset_meta_pb2 = metadata.add_rowsets();
    rowset_meta_pb2->set_id(2);
    rowset_meta_pb2->set_overlapped(false);
    rowset_meta_pb2->set_data_size(1024);
    rowset_meta_pb2->set_num_rows(5);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    metadata.set_version(3);
    auto rowset_meta_pb3 = metadata.add_rowsets();
    rowset_meta_pb3->set_id(3);
    rowset_meta_pb3->set_overlapped(false);
    rowset_meta_pb3->set_data_size(1024);
    rowset_meta_pb3->set_num_rows(5);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    {
        auto res = _tablet_manager->capture_tablet_and_rowsets(tablet_id, 0, 3);
        EXPECT_TRUE(res.ok());
        auto& [tablet, rowsets] = res.value();
        ASSERT_EQ(2, rowsets.size());
    }

    {
        auto res = _tablet_manager->capture_tablet_and_rowsets(tablet_id, 1, 3);
        EXPECT_TRUE(res.ok());
        auto& [tablet, rowsets] = res.value();
        ASSERT_EQ(2, rowsets.size());
    }

    {
        auto res = _tablet_manager->capture_tablet_and_rowsets(tablet_id, 2, 3);
        EXPECT_TRUE(res.ok());
        auto& [tablet, rowsets] = res.value();
        ASSERT_EQ(2, rowsets.size());
    }

    {
        auto res = _tablet_manager->capture_tablet_and_rowsets(tablet_id, 3, 3);
        EXPECT_TRUE(res.ok());
        auto& [tablet, rowsets] = res.value();
        ASSERT_EQ(1, rowsets.size());
    }
}

#endif // USE_STAROS

} // namespace starrocks
