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

#include "storage/lake/tablet_retain_info.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "json2pb/json_to_pb.h"
#include "storage/lake/join_path.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_metadata.h"
#include "test_util.h"

namespace starrocks::lake {

class LakeTabletRetainInfoTest : public TestBase {
public:
    LakeTabletRetainInfoTest() : TestBase(kTestDir) {}

    void SetUp() override { clear_and_init_test_dir(); }

    void TearDown() override {
        remove_test_dir_ignore_error();
        _tablet_mgr->prune_metacache();
    }

protected:
    constexpr static const char* const kTestDir = "./lake_tablet_retain_info_test";

    void create_data_file(const std::string& name) {
        auto full_path = join_path(join_path(kTestDir, kSegmentDirectoryName), name);
        ASSIGN_OR_ABORT(auto f, FileSystem::Default()->new_writable_file(full_path));
        ASSERT_OK(f->append("aaaa"));
        ASSERT_OK(f->close());
    }

    template <class ProtobufMessage>
    std::shared_ptr<ProtobufMessage> json_to_pb(const std::string& json) {
        auto message = std::make_shared<ProtobufMessage>();
        std::string error;
        CHECK(json2pb::JsonToProtoMessage(json, message.get(), &error)) << error;
        return message;
    }
};

// NOLINTNEXTLINE
TEST_F(LakeTabletRetainInfoTest, test_tablet_retain_info_normal) {
    create_data_file("00000000000159e3_3ea06130-ccac-4110-9de8-4813512c60da.delvec");
    create_data_file("00000000000159e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e11.dat");
    create_data_file("00000000000159e4_a542395a-bff5-48a7-a3a7-2ed05691b582.dat");
    create_data_file("0000000000011111_9ae981b3-7d4b-49e9-9723-d7f752686155.sst");
    create_data_file("00000000000159e4_a542395a-bff5-48a7-a3a7-2ed05691b583.dat");

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 666,
        "version": 1
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 666,
        "version": 2,
        "rowsets": [
            {
                "id" : 100,
                "segments": [
                    "00000000000159e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e11.dat",
                    "00000000000159e4_a542395a-bff5-48a7-a3a7-2ed05691b582.dat"
                ],
                "segment_size": [100, 100],
                "data_size": 200
            }
        ],
        "delvec_meta": {
            "version_to_file": [
                {
                    "key": 2,
                    "value": {
                        "name": "00000000000159e3_3ea06130-ccac-4110-9de8-4813512c60da.delvec",
                        "size": 23
                    }
                }
            ],
            "delvecs": [
                {
                    "key": 10,
                    "value": {
                        "version": 4,
                        "offset": 0,
                        "size": 23
                    }
                }
            ]
        },
        "sstable_meta": {
            "sstables": [
                {
                    "filename": "0000000000011111_9ae981b3-7d4b-49e9-9723-d7f752686155.sst"
                }
            ]
        },
        "prev_garbage_version": 0,
        "commit_time": 2
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 666,
        "version": 3,
        "rowsets": [
            {
                "id" : 101,
                "segments": [
                    "00000000000159e4_a542395a-bff5-48a7-a3a7-2ed05691b583.dat"
                ],
                "segment_size": [100],
                "data_size": 100
            }
        ],
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000159e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e11.dat",
                    "00000000000159e4_a542395a-bff5-48a7-a3a7-2ed05691b582.dat"
                ],
                "segment_size": [100, 100],
                "data_size": 200
            }
        ],
        "orphan_files": [
            {
                "name": "00000000000159e3_3ea06130-ccac-4110-9de8-4813512c60da.delvec",
                "size": 23
            },
            {
                "name": "0000000000011111_9ae981b3-7d4b-49e9-9723-d7f752686155.sst",
                "size": 24
            }
        ],
        "prev_garbage_version": 0,
        "commit_time": 3
        }
        )DEL")));

    TabletRetainInfo tablet_retain_info = TabletRetainInfo();
    std::unordered_set<int64_t> set;
    set.insert(2);
    tablet_retain_info.init(666, set, _tablet_mgr.get());

    EXPECT_FALSE(tablet_retain_info.contains_file("00000000000159e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e11.dat"));
    EXPECT_FALSE(tablet_retain_info.contains_file("00000000000159e4_a542395a-bff5-48a7-a3a7-2ed05691b582.dat"));
    EXPECT_TRUE(tablet_retain_info.contains_file("00000000000159e3_3ea06130-ccac-4110-9de8-4813512c60da.delvec"));
    EXPECT_TRUE(tablet_retain_info.contains_file("0000000000011111_9ae981b3-7d4b-49e9-9723-d7f752686155.sst"));
    EXPECT_FALSE(tablet_retain_info.contains_file("00000000000159e4_a542395a-bff5-48a7-a3a7-2ed05691b583.dat"));

    EXPECT_FALSE(tablet_retain_info.contains_version(1));
    EXPECT_TRUE(tablet_retain_info.contains_version(2));
    EXPECT_FALSE(tablet_retain_info.contains_version(3));

    EXPECT_TRUE(tablet_retain_info.contains_rowset(100));
    EXPECT_FALSE(tablet_retain_info.contains_rowset(101));

    EXPECT_TRUE(tablet_retain_info.tablet_id() == 666);
}

} // namespace starrocks::lake