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

#include "storage/lake/vacuum.h"

#include <gtest/gtest.h>

#include <ctime>
#include <set>

#include "common/config.h"
#include "fs/fs.h"
#include "json2pb/json_to_pb.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "test_util.h"
#include "testutil/assert.h"
#include "testutil/sync_point.h"
#include "util/uid_util.h"

namespace starrocks::lake {

struct VacuumTestArg {
    int64_t min_batch_size;
};

class LakeVacuumTest : public TestBase, testing::WithParamInterface<VacuumTestArg> {
public:
    LakeVacuumTest() : TestBase(kTestDir) {}

    void SetUp() override {
        clear_and_init_test_dir();
        config::lake_vacuum_min_batch_delete_size = GetParam().min_batch_size;
    }

    void TearDown() override {
        remove_test_dir_ignore_error();
        _tablet_mgr->prune_metacache();
    }

protected:
    constexpr static const char* const kTestDir = "./lake_vacuum_test";

    void create_data_file(const std::string& name) {
        auto full_path = join_path(join_path(kTestDir, kSegmentDirectoryName), name);
        ASSIGN_OR_ABORT(auto f, FileSystem::Default()->new_writable_file(full_path));
        ASSERT_OK(f->append("aaaa"));
        ASSERT_OK(f->close());
    }

    bool file_exist(const std::string& name) {
        std::string full_path;
        if (is_tablet_metadata(name)) {
            full_path = join_path(join_path(kTestDir, kMetadataDirectoryName), name);
        } else if (is_txn_log(name) || is_txn_vlog(name)) {
            full_path = join_path(join_path(kTestDir, kTxnLogDirectoryName), name);
        } else if (is_segment(name) || is_delvec(name) || is_del(name)) {
            full_path = join_path(join_path(kTestDir, kSegmentDirectoryName), name);
        } else {
            CHECK(false) << name;
        }
        auto st = FileSystem::Default()->path_exists(full_path);
        CHECK(st.ok() || st.is_not_found()) << st;
        return st.ok();
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
TEST_P(LakeVacuumTest, test_vacuum_1) {
    create_data_file("00000000000159e3_3ea06130-ccac-4110-9de8-4813512c60d4.delvec");
    create_data_file("00000000000159e3_9ae981b3-7d4b-49e9-9723-d7f752686154.delvec");
    create_data_file("00000000000159e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat");
    create_data_file("00000000000159e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat");

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 500,
        "version": 1
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 500,
        "version": 2,
        "rowsets": [
            {
                "segments": [
                    "00000000000159e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat",
                    "00000000000159e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"
                ],
                "data_size": 4096
            }
        ],
        "orphan_files": [
            {
                "name": "00000000000159e3_3ea06130-ccac-4110-9de8-4813512c60d4.delvec",
                "size": 128
            },
            {
                "name": "00000000000159e3_9ae981b3-7d4b-49e9-9723-d7f752686154.delvec",
                "size": 128
            }
        ]
        }
        )DEL")));

    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(true);
        request.add_tablet_ids(500);
        request.set_min_retain_version(2);
        request.set_grace_timestamp(::time(nullptr) + 10);
        request.set_min_active_txn_id(12345);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_EQ(3, response.vacuumed_files());
        EXPECT_GT(response.vacuumed_file_size(), 0);

        EXPECT_TRUE(file_exist(tablet_metadata_filename(500, 2)));

        EXPECT_FALSE(file_exist("00000000000159e3_3ea06130-ccac-4110-9de8-4813512c60d4.delvec"));
        EXPECT_FALSE(file_exist("00000000000159e3_9ae981b3-7d4b-49e9-9723-d7f752686154.delvec"));
        EXPECT_TRUE(file_exist("00000000000159e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
        EXPECT_TRUE(file_exist("00000000000159e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"));
    }
}

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_vacuum_2) {
    create_data_file("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat");
    create_data_file("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat");

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 600,
        "version": 1,
        "rowsets": []
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 600,
        "version": 2,
        "rowsets": [
            {
                "segments": [
                    "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                ],
                "data_size": 4096
            }
        ]
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 600,
        "version": 3,
        "rowsets": [
            {
                "segments": [
                    "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat",
                    "00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"
                ],
                "data_size": 8192
            }
        ]
        }
        )DEL")));

    int64_t grace_timestamp = 1687331159;

    SyncPoint::GetInstance()->SetCallBack("collect_files_to_vacuum:get_file_modified_time", [=](void* arg) {
        *(uint64_t*)arg = grace_timestamp; // modification time of version 3 tablet metadata
    });

    SyncPoint::GetInstance()->EnableProcessing();

    // No file will be deleted
    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(true);
        request.add_tablet_ids(600);
        request.set_min_retain_version(3);
        request.set_grace_timestamp(grace_timestamp);
        request.set_min_active_txn_id(12345);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_EQ(0, response.vacuumed_files());
        // The size of deleted metadata files is not counted in vacuumed_file_size.
        EXPECT_EQ(0, response.vacuumed_file_size());

        EXPECT_TRUE(file_exist(tablet_metadata_filename(600, 1)));
        // version 2 is the last version created before "grace_timestamp", should be retained
        EXPECT_TRUE(file_exist(tablet_metadata_filename(600, 2)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(600, 3)));

        EXPECT_TRUE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
        EXPECT_TRUE(file_exist("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"));
    }

    // tablet metadata of version 1, 2 will be deleted.
    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(true);
        request.add_tablet_ids(600);
        request.set_min_retain_version(3);
        // Now version 3 becomes the last version created before grace_timestamp, version 1/2 can be
        // deleted
        request.set_grace_timestamp(grace_timestamp + 1);
        request.set_min_active_txn_id(12345);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_EQ(2, response.vacuumed_files());
        EXPECT_EQ(0, response.vacuumed_file_size());

        EXPECT_FALSE(file_exist(tablet_metadata_filename(600, 1)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(600, 2)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(600, 3)));

        EXPECT_TRUE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
        EXPECT_TRUE(file_exist("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"));
    }

    SyncPoint::GetInstance()->ClearCallBack("collect_files_to_vacuum:get_file_modified_time");
    SyncPoint::GetInstance()->DisableProcessing();
}

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_vacuum_3) {
    create_data_file("00000000000059e3_3ea06130-ccac-4110-9de8-4813512c60d4.delvec");
    create_data_file("00000000000059e3_9ae981b3-7d4b-49e9-9723-d7f752686154.delvec");
    create_data_file("00000000000059e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat");
    create_data_file("00000000000059e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat");
    create_data_file("00000000000059e5_5b3c5f4b-2675-4b7a-b5e0-4006cc285815.dat");
    create_data_file("00000000000059e6_f7fa431d-b968-4ac7-a8e8-98e9f957f2dc.dat");
    create_data_file("00000000000059e7_41486e67-f4a0-4ae6-b2f0-453852652abc.dat");
    create_data_file("00000000000059e4_7c6505a3-f2b0-441d-9ea9-9781b87c0eda.dat");
    create_data_file("00000000000059e4_e231b341-dfc9-4fe6-9a0e-8b03868539dc.dat");

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 100,
        "version": 2,
        "rowsets": [
            {
                "segments": [
                    "00000000000059e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat",
                    "00000000000059e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"
                ],
                "data_size": 4096
            }
        ],
        "orphan_files": [
            {
                "name": "00000000000059e3_3ea06130-ccac-4110-9de8-4813512c60d4.delvec",
                "size": 128
            },
            {
                "name": "00000000000059e3_9ae981b3-7d4b-49e9-9723-d7f752686154.delvec",
                "size": 128
            }
        ]
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 100,
        "version": 3,
        "rowsets": [
            {
                "segments": [
                    "00000000000059e5_5b3c5f4b-2675-4b7a-b5e0-4006cc285815.dat"
                ],
                "data_size": 100
            }
        ],
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000059e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat",
                    "00000000000059e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"
                ],
                "data_size": 4096
            }
        ],
        "prev_garbage_version": 2
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 100,
        "version": 4,
        "rowsets": [
            {
                "segments": [
                    "00000000000059e5_5b3c5f4b-2675-4b7a-b5e0-4006cc285815.dat"
                ],
                "data_size": 4096
            },
            {
                "segments": [
                    "00000000000059e6_f7fa431d-b968-4ac7-a8e8-98e9f957f2dc.dat"
                ],
                "data_size": 1024
            }
        ],
        "prev_garbage_version": 3
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 100,
        "version": 5,
        "rowsets": [
            {
                "segments": [
                    "00000000000059e5_5b3c5f4b-2675-4b7a-b5e0-4006cc285815.dat"
                ],
                "data_size": 4096
            },
            {
                "segments": [
                    "00000000000059e6_f7fa431d-b968-4ac7-a8e8-98e9f957f2dc.dat"
                ],
                "data_size": 1024
            },
            {
                "segments": [
                    "00000000000059e7_41486e67-f4a0-4ae6-b2f0-453852652abc.dat"
                ],
                "data_size": 1024
            }
        ],
        "prev_garbage_version": 3
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 101,
        "version": 4,
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000059e4_7c6505a3-f2b0-441d-9ea9-9781b87c0eda.dat"
                ],
                "data_size": 2048 
            }
        ]
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 101,
        "version": 5,
        "prev_garbage_version": 4
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 102,
        "version": 4,
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000059e4_e231b341-dfc9-4fe6-9a0e-8b03868539dc.dat"
                ],
                "data_size": 2048 
            }
        ]
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 102,
        "version": 5,
        "prev_garbage_version": 4
        }
        )DEL")));

    // Vacuumed
    ASSERT_OK(_tablet_mgr->put_txn_log(json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 100,
            "txn_id": 12344
        }
    )DEL")));

    ASSERT_OK(_tablet_mgr->put_txn_log(json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 100,
            "txn_id": 12345
        }
    )DEL")));

    ASSERT_OK(_tablet_mgr->put_txn_log(json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 100,
            "txn_id": 12346
        }
    )DEL")));

    auto ensure_all_files_exist = [&]() {
        EXPECT_TRUE(file_exist(tablet_metadata_filename(100, 2)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(100, 4)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(100, 3)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(100, 5)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(101, 4)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(101, 5)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(102, 4)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(102, 5)));
        EXPECT_TRUE(file_exist(txn_log_filename(100, 12344)));
        EXPECT_TRUE(file_exist(txn_log_filename(100, 12345)));
        EXPECT_TRUE(file_exist(txn_log_filename(100, 12346)));

        EXPECT_TRUE(file_exist("00000000000059e3_3ea06130-ccac-4110-9de8-4813512c60d4.delvec"));
        EXPECT_TRUE(file_exist("00000000000059e3_9ae981b3-7d4b-49e9-9723-d7f752686154.delvec"));
        EXPECT_TRUE(file_exist("00000000000059e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
        EXPECT_TRUE(file_exist("00000000000059e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"));
        EXPECT_TRUE(file_exist("00000000000059e5_5b3c5f4b-2675-4b7a-b5e0-4006cc285815.dat"));
        EXPECT_TRUE(file_exist("00000000000059e6_f7fa431d-b968-4ac7-a8e8-98e9f957f2dc.dat"));
        EXPECT_TRUE(file_exist("00000000000059e7_41486e67-f4a0-4ae6-b2f0-453852652abc.dat"));
        EXPECT_TRUE(file_exist("00000000000059e4_7c6505a3-f2b0-441d-9ea9-9781b87c0eda.dat"));
        EXPECT_TRUE(file_exist("00000000000059e4_e231b341-dfc9-4fe6-9a0e-8b03868539dc.dat"));
    };
    // Invalid request: tablet_mgr is null
    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(true);
        request.add_tablet_ids(100);
        request.set_min_retain_version(5);
        request.set_grace_timestamp(::time(nullptr) + 60);
        request.set_min_active_txn_id(12345);
        vacuum(nullptr, request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_TRUE(MatchPattern(response.status().error_msgs(0), "*tablet_mgr is null*"))
                << response.status().error_msgs(0);
        ASSERT_NE(0, response.status().status_code());
        EXPECT_EQ(0, response.vacuumed_files());
        EXPECT_EQ(0, response.vacuumed_file_size());

        ensure_all_files_exist();
    }
    // Invalid request: "tablet_ids()" is empty
    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(true);
        request.set_min_retain_version(5);
        request.set_grace_timestamp(::time(nullptr) + 60);
        request.set_min_active_txn_id(12345);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        ASSERT_NE(0, response.status().status_code());
        EXPECT_TRUE(MatchPattern(response.status().error_msgs(0), "*tablet_ids is empty*"))
                << response.status().error_msgs(0);
        EXPECT_EQ(0, response.vacuumed_files());
        EXPECT_EQ(0, response.vacuumed_file_size());

        ensure_all_files_exist();
    }
    // Invalid request: min_retain_version is zero
    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(true);
        request.add_tablet_ids(100);
        request.set_min_retain_version(0);
        request.set_grace_timestamp(::time(nullptr) + 60);
        request.set_min_active_txn_id(12345);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        ASSERT_NE(0, response.status().status_code());
        EXPECT_TRUE(MatchPattern(response.status().error_msgs(0), "*value of min_retain_version is zero or negative*"))
                << response.status().error_msgs(0);
        EXPECT_EQ(0, response.vacuumed_files());
        EXPECT_EQ(0, response.vacuumed_file_size());

        ensure_all_files_exist();
    }
    // Invalid request: grace_timestamp is zero
    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(true);
        request.add_tablet_ids(101);
        request.add_tablet_ids(100);
        request.set_min_retain_version(5);
        request.set_grace_timestamp(0);
        request.set_min_active_txn_id(12345);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        ASSERT_NE(0, response.status().status_code());
        EXPECT_TRUE(MatchPattern(response.status().error_msgs(0), "*value of grace_timestamp is zero or nagative*"))
                << response.status().error_msgs(0);
        EXPECT_EQ(0, response.vacuumed_files());
        EXPECT_EQ(0, response.vacuumed_file_size());

        ensure_all_files_exist();
    }
    // No file been delted: all tablet metadata files are created after the "grace_timestamp"
    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(true);
        request.add_tablet_ids(101);
        request.add_tablet_ids(100);
        request.set_min_retain_version(5);
        request.set_grace_timestamp(::time(nullptr) - 60);
        request.set_min_active_txn_id(12344);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_EQ(0, response.vacuumed_files());
        EXPECT_EQ(0, response.vacuumed_file_size());

        ensure_all_files_exist();
    }
    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(true);
        // Does not delete files of tablet 102
        request.add_tablet_ids(101);
        request.add_tablet_ids(100);
        request.set_min_retain_version(5);
        request.set_grace_timestamp(::time(nullptr) + 10);
        request.set_min_active_txn_id(12345);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        // 4 tablet metadata files: tablet 100 of versions [1],2,3,4 and tablet 101 of version [1],[2],[3],4
        // 3 compaction input files
        // 2 orphan files
        // 1 txn log file
        EXPECT_EQ(14, response.vacuumed_files());
        EXPECT_GT(response.vacuumed_file_size(), 0);

        EXPECT_FALSE(file_exist(tablet_metadata_filename(100, 2)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(100, 3)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(100, 4)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(100, 5)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(101, 4)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(101, 5)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(102, 4)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(102, 5)));
        EXPECT_FALSE(file_exist(txn_log_filename(100, 12344)));
        EXPECT_TRUE(file_exist(txn_log_filename(100, 12345)));
        EXPECT_TRUE(file_exist(txn_log_filename(100, 12346)));

        EXPECT_FALSE(file_exist("00000000000059e3_3ea06130-ccac-4110-9de8-4813512c60d4.delvec"));
        EXPECT_FALSE(file_exist("00000000000059e3_9ae981b3-7d4b-49e9-9723-d7f752686154.delvec"));
        EXPECT_FALSE(file_exist("00000000000059e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
        EXPECT_FALSE(file_exist("00000000000059e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"));
        EXPECT_TRUE(file_exist("00000000000059e5_5b3c5f4b-2675-4b7a-b5e0-4006cc285815.dat"));
        EXPECT_TRUE(file_exist("00000000000059e6_f7fa431d-b968-4ac7-a8e8-98e9f957f2dc.dat"));
        EXPECT_TRUE(file_exist("00000000000059e7_41486e67-f4a0-4ae6-b2f0-453852652abc.dat"));
        EXPECT_FALSE(file_exist("00000000000059e4_7c6505a3-f2b0-441d-9ea9-9781b87c0eda.dat"));
        EXPECT_TRUE(file_exist("00000000000059e4_e231b341-dfc9-4fe6-9a0e-8b03868539dc.dat"));
    }
}

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_delete_tablets_01) {
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 700,
        "version": 2
        }
        )DEL")));
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 700,
        "version": 3
        }
        )DEL")));
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 701,
        "version": 2
        }
        )DEL")));
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 701,
        "version": 3
        }
        )DEL")));
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 702,
        "version": 2
        }
        )DEL")));
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 702,
        "version": 3
        }
        )DEL")));

    DeleteTabletRequest request;
    DeleteTabletResponse response;
    request.add_tablet_ids(700);
    request.add_tablet_ids(701);
    delete_tablets(_tablet_mgr.get(), request, &response);
    EXPECT_TRUE(response.has_status());
    EXPECT_EQ(0, response.status().status_code());
    EXPECT_FALSE(file_exist(tablet_metadata_filename(700, 2)));
    EXPECT_FALSE(file_exist(tablet_metadata_filename(700, 3)));
    EXPECT_FALSE(file_exist(tablet_metadata_filename(701, 2)));
    EXPECT_FALSE(file_exist(tablet_metadata_filename(701, 3)));
    EXPECT_TRUE(file_exist(tablet_metadata_filename(702, 2)));
    EXPECT_TRUE(file_exist(tablet_metadata_filename(702, 3)));
}

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_delete_tablets_02) {
    create_data_file("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat");
    create_data_file("00000000000359e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat");
    create_data_file("00000000000459e4_3d9c9edb-a69d-4a06-9093-a9f557e4c3b0.dat");
    create_data_file("00000000000459e3_9ae981b3-7d4b-49e9-9723-d7f752686154.delvec");

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 800,
        "version": 2,
        "rowsets": [
            {
                "segments": [
                    "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                ]
            },
            {
                "segments": [
                    "00000000000359e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"
                ]
            }
        ],
        "prev_garbage_version": 1
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 800,
        "version": 3,
        "rowsets": [
            {
                "segments": [
                    "00000000000459e4_3d9c9edb-a69d-4a06-9093-a9f557e4c3b0.dat"
                ]
            }
        ],
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                ]
            },
            {
                "segments": [
                    "00000000000359e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"
                ]
            }
        ],
        "prev_garbage_version": 1
        }
        )DEL")));
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 800,
        "version": 4,
        "rowsets": [
            {
                "segments": [
                    "00000000000459e4_3d9c9edb-a69d-4a06-9093-a9f557e4c3b0.dat"
                ]
            }
        ],
        "delvec_meta": {
            "version_to_file": [
                {
                    "key": 4,
                    "value": {
                        "name": "00000000000459e3_9ae981b3-7d4b-49e9-9723-d7f752686154.delvec",
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
        "prev_garbage_version": 3
        }
        )DEL")));

    {
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(800);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        EXPECT_FALSE(file_exist(tablet_metadata_filename(800, 2)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(800, 3)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(800, 4)));

        EXPECT_FALSE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
        EXPECT_FALSE(file_exist("00000000000359e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"));
        EXPECT_FALSE(file_exist("00000000000459e4_3d9c9edb-a69d-4a06-9093-a9f557e4c3b0.dat"));
        EXPECT_FALSE(file_exist("00000000000459e3_9ae981b3-7d4b-49e9-9723-d7f752686154.delvec"));
    }
    {
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(800);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
    }
}

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_delete_tablets_03) {
    // Referenced in the txn log of tablet id 900 and txn id 2000
    create_data_file("00000000001259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat");
    create_data_file("00000000001259e4_28dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.del");
    create_data_file("00000000001259e4_29dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.del");

    // Referenced in the txn log of tablet id 900 and txn id 3000
    create_data_file("00000000002259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat");

    // Referenced in the txn log of tablet id 900 and txn id 4000
    create_data_file("00000000003259e4_37dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat");
    create_data_file("00000000003259e4_47dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat");

    // Referenced in the txn log of tablet id 901 and txn id 5000
    create_data_file("00000000004259e4_47dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat");

    ASSERT_OK(_tablet_mgr->put_txn_log(json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 900,
            "txn_id": 2000,
            "op_write": {
                "rowset": {
                    "segments": ["00000000001259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"]
                },
                "dels": [
                    "00000000001259e4_28dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.del",
                    "00000000001259e4_29dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.del"
                ]
            }
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_txn_log(json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 900,
            "txn_id": 3000,
            "op_compaction": {
                "output_rowset": {
                    "segments": ["00000000002259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"]
                }
            }
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_txn_log(json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 900,
            "txn_id": 4000,
            "op_schema_change": {
                "rowsets": [
                    {
                         "segments": ["00000000003259e4_37dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"]
                    },
                    {
                        "segments": ["00000000003259e4_47dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"]
                    }
                ]
            }
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_txn_log(json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 901,
            "txn_id": 5000,
            "op_write": {
                "rowset": {
                    "segments": ["00000000004259e4_47dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"]
                }
            }
        }
        )DEL")));

    {
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(900);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        EXPECT_FALSE(file_exist(txn_log_filename(900, 2000)));
        EXPECT_FALSE(file_exist(txn_log_filename(900, 3000)));
        EXPECT_FALSE(file_exist(txn_log_filename(900, 4000)));
        EXPECT_TRUE(file_exist(txn_log_filename(901, 5000)));

        // Referenced in the txn log of tablet id 900 and txn id 2000
        EXPECT_FALSE(file_exist("00000000001259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
        EXPECT_FALSE(file_exist("00000000001259e4_28dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.del"));
        EXPECT_FALSE(file_exist("00000000001259e4_29dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.del"));
        // Referenced in the txn log of tablet id 900 and txn id 3000
        EXPECT_FALSE(file_exist("00000000002259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
        // Referenced in the txn log of tablet id 900 and txn id 4000
        EXPECT_FALSE(file_exist("00000000003259e4_37dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
        EXPECT_FALSE(file_exist("00000000003259e4_47dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
        // Referenced in the txn log of tablet id 901 and txn id 5000
        EXPECT_TRUE(file_exist("00000000004259e4_47dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
    }
}

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_delete_file_failed) {
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 500,
        "version": 2,
        "orphan_files": [
            {
                "name": "00000000000359e3_3ea06130-ccac-4110-9de8-4813512c60d4.delvec",
                "size": 128
            },
            {
                "name": "00000000000359e3_9ae981b3-7d4b-49e9-9723-d7f752686154.delvec",
                "size": 128
            }
        ]
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 500,
        "version": 3,
        "prev_garbage_version": 2
        }
        )DEL")));

    SyncPoint::GetInstance()->SetCallBack("vacuum.delete_files", [](void* arg) {
        auto st = (Status*)arg;
        EXPECT_TRUE(st->ok()) << *st;
        st->update(Status::IOError("injected error"));
    });

    SyncPoint::GetInstance()->EnableProcessing();

    VacuumRequest request;
    VacuumResponse response;
    request.set_delete_txn_log(true);
    request.add_tablet_ids(500);
    request.set_min_retain_version(3);
    request.set_grace_timestamp(::time(nullptr) + 3600);
    request.set_min_active_txn_id(12345);
    vacuum(_tablet_mgr.get(), request, &response);
    ASSERT_TRUE(response.has_status());
    EXPECT_TRUE(MatchPattern(response.status().error_msgs(0), "injected error")) << response.status().error_msgs(0);
    ASSERT_NE(0, response.status().status_code());
    EXPECT_EQ(0, response.vacuumed_files());
    EXPECT_EQ(0, response.vacuumed_file_size());

    EXPECT_TRUE(file_exist(tablet_metadata_filename(500, 2)));
    EXPECT_TRUE(file_exist(tablet_metadata_filename(500, 3)));

    SyncPoint::GetInstance()->ClearCallBack("vacuum.delete_files");
    SyncPoint::GetInstance()->DisableProcessing();
}

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_dont_delete_txn_log) {
    ASSERT_OK(_tablet_mgr->put_txn_log(json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 1900,
            "txn_id": 2000
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_txn_log(json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 1900,
            "txn_id": 3000
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_txn_log(json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 1900,
            "txn_id": 4000
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_txn_log(json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 2000,
            "txn_id": 3000
        }
        )DEL")));

    // delete_txn_log = false
    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(false);
        request.add_tablet_ids(1900);
        request.set_grace_timestamp(time(nullptr) - 3600);
        request.set_min_active_txn_id(4000);
        request.set_min_retain_version(1000);

        vacuum(_tablet_mgr.get(), request, &response);
        EXPECT_EQ(0, response.status().status_code());
        EXPECT_EQ(0, response.vacuumed_files());
        EXPECT_EQ(0, response.vacuumed_file_size());
        EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_log_location(1900, 2000)));
        EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_log_location(1900, 3000)));
        EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_log_location(1900, 4000)));
    }
    // delete_txn_log = true
    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(true);
        request.add_tablet_ids(1900);
        request.set_grace_timestamp(time(nullptr) - 3600);
        request.set_min_active_txn_id(4000);
        request.set_min_retain_version(1000);

        vacuum(_tablet_mgr.get(), request, &response);
        EXPECT_EQ(0, response.status().status_code());
        EXPECT_EQ(3, response.vacuumed_files());
        EXPECT_GT(response.vacuumed_file_size(), 0);
        EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_log_location(1900, 2000)));
        EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_log_location(1900, 3000)));
        EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_log_location(2000, 3000)));
        EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_log_location(1900, 4000)));
    }
}

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_commit_time) {
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 5000,
        "version": 1,
        "commit_time": 1696998530,
        "prev_garbage_version": 0
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 5000,
        "version": 2,
        "commit_time": 1696998535,
        "prev_garbage_version": 0
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 5000,
        "version": 3,
        "commit_time": 1696998540,
        "prev_garbage_version": 2
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 5000,
        "version": 4,
        "commit_time": 1696998545,
        "prev_garbage_version": 3
        }
        )DEL")));

    int invoked = 0;
    SyncPoint::GetInstance()->SetCallBack("collect_files_to_vacuum:get_file_modified_time",
                                          [&](void* arg) { invoked++; });

    SyncPoint::GetInstance()->EnableProcessing();

    {
        VacuumRequest request;
        VacuumResponse response;
        request.add_tablet_ids(5000);
        request.set_min_retain_version(4);
        request.set_grace_timestamp(1696998542); // <----- greater than the commit time of version 3
        request.set_min_active_txn_id(10);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        ASSERT_EQ(0, response.status().status_code());
        EXPECT_EQ(2, response.vacuumed_files());
        EXPECT_EQ(0, response.vacuumed_file_size());

        EXPECT_FALSE(file_exist(tablet_metadata_filename(5000, 1)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(5000, 2)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(5000, 3)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(5000, 4)));
        EXPECT_EQ(0, invoked);
    }
    {
        VacuumRequest request;
        VacuumResponse response;
        request.add_tablet_ids(5000);
        request.set_min_retain_version(4);
        request.set_grace_timestamp(1696998545); // <----- equals to the commit time of version 4
        request.set_min_active_txn_id(10);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        ASSERT_EQ(0, response.status().status_code());
        EXPECT_EQ(0, response.vacuumed_files());
        EXPECT_EQ(0, response.vacuumed_file_size());

        EXPECT_FALSE(file_exist(tablet_metadata_filename(5000, 1)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(5000, 2)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(5000, 3)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(5000, 4)));
        EXPECT_EQ(0, invoked);
    }
    {
        VacuumRequest request;
        VacuumResponse response;
        request.add_tablet_ids(5000);
        request.set_min_retain_version(4);
        request.set_grace_timestamp(1696998550); // <----- greater than the commit time of version 4
        request.set_min_active_txn_id(10);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        ASSERT_EQ(0, response.status().status_code());
        EXPECT_EQ(1, response.vacuumed_files());
        EXPECT_EQ(0, response.vacuumed_file_size());

        EXPECT_FALSE(file_exist(tablet_metadata_filename(5000, 1)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(5000, 2)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(5000, 3)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(5000, 4)));
        EXPECT_EQ(0, invoked);
    }
    {
        VacuumRequest request;
        VacuumResponse response;
        request.add_tablet_ids(5000);
        request.set_min_retain_version(4);
        request.set_grace_timestamp(1696998550); // <----- greater than the commit time of version 4
        request.set_min_active_txn_id(10);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        ASSERT_EQ(0, response.status().status_code());
        EXPECT_EQ(0, response.vacuumed_files());
        EXPECT_EQ(0, response.vacuumed_file_size());

        EXPECT_FALSE(file_exist(tablet_metadata_filename(5000, 1)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(5000, 2)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(5000, 3)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(5000, 4)));
        EXPECT_EQ(0, invoked);
    }
    SyncPoint::GetInstance()->ClearCallBack("collect_files_to_vacuum:get_file_modified_time");
    SyncPoint::GetInstance()->DisableProcessing();
}

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_thread_pool_full) {
    ASSERT_OK(_tablet_mgr->put_txn_log(json_to_pb<TxnLogPB>(R"DEL(
          {
              "tablet_id": 1900,
              "txn_id": 14000
          }
          )DEL")));

    ASSERT_OK(_tablet_mgr->put_txn_log(json_to_pb<TxnLogPB>(R"DEL(
          {
              "tablet_id": 2000,
              "txn_id": 13000
          }
          )DEL")));

    SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:1", [&](void* arg) { *(int64_t*)arg = 0; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    {
        VacuumRequest request;
        VacuumResponse response;
        request.add_tablet_ids(1900);
        request.set_min_retain_version(4);
        request.set_grace_timestamp(1696998550);
        request.set_min_active_txn_id(20000);
        request.set_delete_txn_log(true);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        ASSERT_EQ(TStatusCode::SERVICE_UNAVAILABLE, response.status().status_code());

        EXPECT_TRUE(file_exist(txn_log_filename(1900, 14000)));
        EXPECT_TRUE(file_exist(txn_log_filename(2000, 13000)));
    }
}

INSTANTIATE_TEST_SUITE_P(LakeVacuumTest, LakeVacuumTest,
                         ::testing::Values(VacuumTestArg{1}, VacuumTestArg{3}, VacuumTestArg{100}));

TEST(LakeVacuumTest2, test_delete_files_async) {
    delete_files_async({});

    ASSIGN_OR_ABORT(auto f1, fs::new_writable_file("test_vacuum_delete_files1.txt"));
    ASSIGN_OR_ABORT(auto f2, fs::new_writable_file("test_vacuum_delete_files2.txt"));
    ASSERT_OK(f1->append("111"));
    ASSERT_OK(f1->close());
    ASSERT_OK(f2->append("222"));
    ASSERT_OK(f2->close());

    delete_files_async({"test_vacuum_delete_files1.txt", "test_vacuum_delete_files2.txt"});
    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
    ASSERT_FALSE(fs::path_exist("test_vacuum_delete_files1.txt"));
    ASSERT_FALSE(fs::path_exist("test_vacuum_delete_files2.txt"));
}

TEST(LakeVacuumTest2, test_delete_files_callable) {
    auto future = delete_files_callable({});
    ASSERT_TRUE(future.valid());
    ASSERT_TRUE(future.get().ok());

    ASSIGN_OR_ABORT(auto f1, fs::new_writable_file("test_vacuum_delete_files_callable1.txt"));
    ASSIGN_OR_ABORT(auto f2, fs::new_writable_file("test_vacuum_delete_files_callable2.txt"));
    ASSERT_OK(f1->append("111"));
    ASSERT_OK(f1->close());
    ASSERT_OK(f2->append("222"));
    ASSERT_OK(f2->close());

    auto future2 =
            delete_files_callable({"test_vacuum_delete_files_callable1.txt", "test_vacuum_delete_files_callable2.txt"});
    ASSERT_TRUE(future2.valid());
    ASSERT_TRUE(future2.get().ok());
    ASSERT_FALSE(fs::path_exist("test_vacuum_delete_files_callable1.txt"));
    ASSERT_FALSE(fs::path_exist("test_vacuum_delete_files_callable2.txt"));
}

TEST(LakeVacuumTest2, test_delete_files_thread_pool_full) {
    SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:1", [](void* arg) { *(int64_t*)arg = 0; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    auto future = delete_files_callable({"any_non_exist_file"});
    ASSERT_TRUE(future.valid());
    ASSERT_EQ(TStatusCode::SERVICE_UNAVAILABLE, future.get().code());

    delete_files_async({"any_non_exist_file"});
}

} // namespace starrocks::lake
