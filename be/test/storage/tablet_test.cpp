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

#include "storage/data_dir.h"
#include "storage/rowset/rowset.h"
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
} // namespace starrocks
