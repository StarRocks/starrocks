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

#include "storage/tablet.h"

#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "exec/schema_scanner/schema_be_tablets_scanner.h"
#include "storage/data_dir.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet_manager.h"

namespace starrocks {

using namespace std::chrono_literals;

class TabletTest : public testing::Test {
public:
    TabletTest() {}
};

static void add_rowset(Tablet* tablet, std::shared_ptr<Rowset>* rowset, std::atomic<bool>* stop) {
    while (!stop->load()) {
        tablet->add_committed_rowset(*rowset);
        std::this_thread::sleep_for(2us);
    }
}
static void erase_rowset(Tablet* tablet, std::shared_ptr<Rowset>* rowset, std::atomic<bool>* stop) {
    while (!stop->load()) {
        tablet->erase_committed_rowset(*rowset);
        std::this_thread::sleep_for(2us);
    }
}

TEST_F(TabletTest, test_concurrent_add_remove_committed_rowsets) {
    auto tablet_meta = std::make_shared<TabletMeta>();
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_id(1024);
    auto schema = std::make_shared<const TabletSchema>(schema_pb);
    tablet_meta->set_tablet_schema(schema);
    tablet_meta->set_tablet_id(1024);
    auto rs_meta_pb = std::make_unique<RowsetMetaPB>();
    rs_meta_pb->set_rowset_id("123");
    rs_meta_pb->set_start_version(0);
    rs_meta_pb->set_end_version(1);
    rs_meta_pb->mutable_tablet_schema()->CopyFrom(schema_pb);
    auto rowset_meta = std::make_shared<RowsetMeta>(rs_meta_pb);
    auto rowset = std::make_shared<Rowset>(schema, "", rowset_meta);
    DataDir data_dir("./data_dir");
    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, &data_dir);
    tablet->set_data_dir(&data_dir);
    tablet->add_committed_rowset(rowset);
    tablet->erase_committed_rowset(rowset);
    std::vector<std::thread> all_threads;
    std::atomic<bool> stop{false};
    for (int i = 0; i < 10; ++i) {
        all_threads.emplace_back(&add_rowset, tablet.get(), &rowset, &stop);
    }
    for (int i = 0; i < 10; ++i) {
        all_threads.emplace_back(&erase_rowset, tablet.get(), &rowset, &stop);
    }
    std::this_thread::sleep_for(5s);
    stop = true;
    for (auto& t : all_threads) {
        t.join();
    }
}

TEST_F(TabletTest, test_get_basic_info_uses_tablet_footprint) {
    auto tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->TEST_set_table_id(20001);
    tablet_meta->set_partition_id(20002);
    tablet_meta->set_tablet_id(20003);
    tablet_meta->set_creation_time(123456789);

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_id(20004);
    auto schema = std::make_shared<const TabletSchema>(schema_pb);
    tablet_meta->set_tablet_schema(schema);

    RowsetMetaPB rowset_meta_pb;
    rowset_meta_pb.set_tablet_id(20003);
    rowset_meta_pb.set_partition_id(20002);
    rowset_meta_pb.set_creation_time(0);
    rowset_meta_pb.set_empty(false);
    rowset_meta_pb.set_num_segments(1);
    rowset_meta_pb.set_num_rows(321);
    rowset_meta_pb.set_start_version(0);
    rowset_meta_pb.set_end_version(0);
    rowset_meta_pb.set_rowset_state(VISIBLE);
    rowset_meta_pb.set_data_disk_size(54321);
    rowset_meta_pb.set_index_disk_size(999);
    RowsetId rowset_id;
    rowset_id.init(2, 2, 0, 0);
    rowset_meta_pb.set_rowset_id(rowset_id.to_string());
    tablet_meta->add_rs_meta(std::make_shared<RowsetMeta>(rowset_meta_pb));

    DataDir data_dir("./data_dir");
    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, &data_dir);
    tablet->set_data_dir(&data_dir);

    TabletBasicInfo info;
    tablet->get_basic_info(info);
    ASSERT_EQ(54321 + 999, info.data_size);
    ASSERT_EQ(321, info.num_row);
    ASSERT_EQ(1, info.num_segment);
}
} // namespace starrocks
