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

#include "exec/lake_meta_scanner.h"

#include <gtest/gtest.h>

#include "exec/lake_meta_scan_node.h"
#include "fs/fs_util.h"
#include "runtime/descriptor_helper.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake_meta_reader.h"
#include "testutil/id_generator.h"
#include "testutil/sync_point.h"

namespace starrocks {

// provides an empty implementation of the reader
struct MockLakeMetaReader : public LakeMetaReader {
public:
    Status init(const LakeMetaReaderParams& read_params) override { return Status::OK(); }
};

// access the protected members of LakeMetaScanner
struct MockLakeMetaScanner : public LakeMetaScanner {
public:
    MockLakeMetaScanner(LakeMetaScanNode* parent) : LakeMetaScanner(parent) {}

    std::shared_ptr<LakeMetaReader> reader() { return _reader; }
    int64_t tablet_id() const { return _tablet_id; }
    bool is_opened() const { return _is_open; }
};

class LakeMetaScannerTest : public ::testing::Test {
public:
    LakeMetaScannerTest() : _tablet_id(next_id()) {
        // setup TabletManager
        _location_provider = std::make_unique<lake::FixedLocationProvider>(kRootLocation);
        _tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
        _backup_location_provider = _tablet_mgr->TEST_set_location_provider(_location_provider.get());
        FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kSegmentDirectoryName));
        FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kMetadataDirectoryName));
        FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kTxnLogDirectoryName));

        {
            // create the tablet with its schema prepared
            lake::TabletMetadata metadata;
            metadata.set_id(_tablet_id);
            metadata.set_version(2);

            auto schema = metadata.mutable_schema();
            schema->set_id(10);
            schema->set_num_short_key_columns(1);
            schema->set_keys_type(DUP_KEYS);
            schema->set_num_rows_per_row_block(65535);
            auto c0 = schema->add_column();
            c0->set_unique_id(0);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
            auto st = _tablet_mgr->put_tablet_metadata(metadata);
            EXPECT_TRUE(st.ok());

            auto tablet_or = _tablet_mgr->get_tablet(_tablet_id);
            EXPECT_TRUE(tablet_or.ok());
            auto st2 = tablet_or->get_schema();
            EXPECT_TRUE(st2.ok());
        }

        _state = _pool.add(new RuntimeState(TQueryGlobals()));

        std::vector<::starrocks::TTupleId> tuple_ids{0};
        std::vector<bool> nullable_tuples{true};
        _tnode = std::make_unique<TPlanNode>();
        _tnode->__set_node_id(1);
        _tnode->__set_node_type(TPlanNodeType::LAKE_SCAN_NODE);
        _tnode->__set_row_tuples(tuple_ids);
        _tnode->__set_nullable_tuples(nullable_tuples);
        _tnode->__set_limit(-1);

        TDescriptorTableBuilder table_desc_builder;
        TSlotDescriptorBuilder slot_desc_builder;
        auto slot =
                slot_desc_builder.type(LogicalType::TYPE_INT).column_name("col1").column_pos(0).nullable(true).build();
        TTupleDescriptorBuilder tuple_desc_builder;
        tuple_desc_builder.add_slot(slot);
        tuple_desc_builder.build(&table_desc_builder);

        CHECK(DescriptorTbl::create(_state, &_pool, table_desc_builder.desc_tbl(), &_tbl, config::vector_chunk_size)
                      .ok());

        _parent = std::make_unique<LakeMetaScanNode>(&_pool, *_tnode, *_tbl);
    }

    ~LakeMetaScannerTest() {
        auto st = fs::remove_all(kRootLocation);
        EXPECT_TRUE(st.ok());
        (void)_tablet_mgr->TEST_set_location_provider(_backup_location_provider);
    }

public:
    constexpr static const char* const kRootLocation = "./LakeMetaScannerTest";
    lake::TabletManager* _tablet_mgr;
    std::unique_ptr<lake::LocationProvider> _location_provider;
    lake::LocationProvider* _backup_location_provider;
    int64_t _tablet_id;

    ObjectPool _pool;
    RuntimeState* _state = nullptr;
    std::unique_ptr<TPlanNode> _tnode;
    DescriptorTbl* _tbl;
    std::unique_ptr<LakeMetaScanNode> _parent;
};

TEST_F(LakeMetaScannerTest, test_init_lazy_and_real) {
    auto range = _pool.add(new TInternalScanRange());
    range->tablet_id = _tablet_id;
    range->version = 2;
    MetaScannerParams params{.scan_range = range};

    MockLakeMetaScanner scanner(_parent.get());
    auto st = scanner.init(_state, params);
    // after init() called, reader is not created at all
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(scanner.reader().get() == nullptr);
    EXPECT_EQ(range->tablet_id, scanner.tablet_id());

    std::shared_ptr<LakeMetaReader> mock_reader(new MockLakeMetaReader);
    SyncPoint::GetInstance()->SetCallBack("lake_meta_scanner:open_mock_reader", [=](void* arg) {
        std::shared_ptr<LakeMetaReader>* reader = static_cast<std::shared_ptr<LakeMetaReader>*>(arg);
        // non-empty reader
        EXPECT_TRUE((*reader).get() != nullptr);
        *reader = mock_reader;
    });
    SyncPoint::GetInstance()->EnableProcessing();

    auto st2 = scanner.open(nullptr);
    // after open() called, reader is created
    EXPECT_TRUE(st2.ok()) << st2;
    EXPECT_EQ(mock_reader.get(), scanner.reader().get());
    EXPECT_TRUE(scanner.is_opened());

    SyncPoint::GetInstance()->ClearCallBack("lake_meta_scanner:open_mock_reader");
    SyncPoint::GetInstance()->DisableProcessing();
}

} // namespace starrocks
