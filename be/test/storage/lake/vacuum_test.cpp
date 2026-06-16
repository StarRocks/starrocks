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

// Forward-declare internal helper exposed for testing (defined in vacuum.cpp).
int64_t calculate_retry_delay(int64_t last_delay, int64_t base, int64_t max_retries);

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
        } else if (is_txn_log(name) || is_txn_slog(name) || is_txn_vlog(name) || is_combined_txn_log(name)) {
            full_path = join_path(join_path(kTestDir, kTxnLogDirectoryName), name);
        } else if (is_segment(name) || is_delvec(name) || is_del(name) || is_sst(name)) {
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

    const int64_t grace_timestamp = 1687331159;

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
        ],
        "commit_time": 1687331159
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
        "prev_garbage_version": 2,
        "commit_time": 1687331159
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
        "prev_garbage_version": 3,
        "commit_time": 1687331159
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
        "prev_garbage_version": 3,
        "commit_time": 1687331159
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
        ],
        "commit_time": 1687331159
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 101,
        "version": 5,
        "prev_garbage_version": 4,
        "commit_time": 1687331159
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
        ],
        "commit_time": 1687331159
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 102,
        "version": 5,
        "prev_garbage_version": 4,
        "commit_time": 1687331159
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

    // txn slog
    ASSERT_OK(_tablet_mgr->put_txn_slog(json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 100,
            "txn_id": 12344
        }
    )DEL")));

    ASSERT_OK(_tablet_mgr->put_txn_slog(json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 100,
            "txn_id": 12345
        }
    )DEL")));

    ASSERT_OK(_tablet_mgr->put_txn_slog(json_to_pb<TxnLogPB>(R"DEL(
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
        EXPECT_TRUE(file_exist(txn_slog_filename(100, 12344)));
        EXPECT_TRUE(file_exist(txn_slog_filename(100, 12345)));
        EXPECT_TRUE(file_exist(txn_slog_filename(100, 12346)));

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
        EXPECT_EQ(0, response.vacuumed_version());

        ensure_all_files_exist();
    }
    // Invalid request: "tablet_ids()" and "tablet_infos" are empty
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
        EXPECT_TRUE(MatchPattern(response.status().error_msgs(0), "*both tablet_ids and tablet_infos are empty*"))
                << response.status().error_msgs(0);
        EXPECT_EQ(0, response.vacuumed_files());
        EXPECT_EQ(0, response.vacuumed_file_size());
        EXPECT_EQ(0, response.vacuumed_version());

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
        EXPECT_EQ(0, response.vacuumed_version());

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
        EXPECT_EQ(0, response.vacuumed_version());

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
        request.set_grace_timestamp(grace_timestamp - 60);
        request.set_min_active_txn_id(12344);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_EQ(0, response.vacuumed_files());
        EXPECT_EQ(0, response.vacuumed_file_size());
        EXPECT_EQ(1, response.vacuumed_version());

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
        request.set_grace_timestamp(grace_timestamp + 10);
        request.set_min_active_txn_id(12345);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        // 4 tablet metadata files: tablet 100 of versions [1],2,3,4 and tablet 101 of version [1],[2],[3],4
        // 3 compaction input files
        // 2 orphan files
        // 1 txn log file
        // 1 txn slog file
        EXPECT_EQ(15, response.vacuumed_files());
        EXPECT_GT(response.vacuumed_file_size(), 0);
        EXPECT_EQ(5, response.vacuumed_version());

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
        EXPECT_FALSE(file_exist(txn_slog_filename(100, 12344)));
        EXPECT_TRUE(file_exist(txn_slog_filename(100, 12345)));
        EXPECT_TRUE(file_exist(txn_slog_filename(100, 12346)));

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
    create_data_file("0000000000011111_9ae981b3-7d4b-49e9-9723-d7f752686154.sst");

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
        "sstable_meta": {
            "sstables": [
                {
                    "filename": "0000000000011111_9ae981b3-7d4b-49e9-9723-d7f752686154.sst"
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
        EXPECT_FALSE(file_exist("0000000000011111_9ae981b3-7d4b-49e9-9723-d7f752686154.sst"));
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

    SyncPoint::GetInstance()->SetCallBack("PosixFileSystem::delete_file", [](void* arg) {
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

    SyncPoint::GetInstance()->ClearCallBack("PosixFileSystem::delete_file");
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

    // txn slog
    ASSERT_OK(_tablet_mgr->put_txn_slog(json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 1900,
            "txn_id": 2000
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_txn_slog(json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 1900,
            "txn_id": 3000
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_txn_slog(json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 1900,
            "txn_id": 4000
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_txn_slog(json_to_pb<TxnLogPB>(R"DEL(
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
        EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_slog_location(1900, 2000)));
        EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_slog_location(1900, 3000)));
        EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_slog_location(1900, 4000)));
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
        EXPECT_EQ(6, response.vacuumed_files());
        EXPECT_GT(response.vacuumed_file_size(), 0);
        EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_log_location(1900, 2000)));
        EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_log_location(1900, 3000)));
        EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_log_location(2000, 3000)));
        EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_log_location(1900, 4000)));
        EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_slog_location(1900, 2000)));
        EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_slog_location(1900, 3000)));
        EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_slog_location(2000, 3000)));
        EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_slog_location(1900, 4000)));
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

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_datafile_gc) {
    WritableFileOptions options;
    options.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_ABORT(auto f, fs::new_writable_file(options, join_path(kTestDir, "test_datafile_gc.txt")));
    ASSERT_OK(f->append("111"));
    ASSERT_OK(f->close());

    create_data_file("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat");
    create_data_file("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat");
    create_data_file("0000000000011111_a542395a-bff5-48a7-a3a7-2ed05691b58c.sst");
    create_data_file("0000000000022222_a542395a-bff5-48a7-a3a7-2ed05691b58c.sst");

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
        ],
        "sstable_meta": {
            "sstables": [
                {
                    "filename": "0000000000022222_a542395a-bff5-48a7-a3a7-2ed05691b58c.sst"
                }
            ]
        }
        }
        )DEL")));

    ASSERT_OK(datafile_gc(kTestDir, join_path(kTestDir, "audit.log"), 0, false));
    EXPECT_TRUE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
    EXPECT_TRUE(file_exist("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"));
    EXPECT_TRUE(file_exist("0000000000011111_a542395a-bff5-48a7-a3a7-2ed05691b58c.sst"));
    EXPECT_TRUE(file_exist("0000000000022222_a542395a-bff5-48a7-a3a7-2ed05691b58c.sst"));

    ASSERT_OK(datafile_gc(kTestDir, "", 0, true));
    EXPECT_TRUE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
    EXPECT_FALSE(file_exist("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"));
    EXPECT_FALSE(file_exist("0000000000011111_a542395a-bff5-48a7-a3a7-2ed05691b58c.sst"));
    EXPECT_TRUE(file_exist("0000000000022222_a542395a-bff5-48a7-a3a7-2ed05691b58c.sst"));
}

TEST_P(LakeVacuumTest, test_vacuum_combined_txn_log) {
    ASSERT_OK(_tablet_mgr->put_combined_txn_log(*json_to_pb<CombinedTxnLogPB>(R"DEL(
        {
            "txn_logs": [
               {
                  "tablet_id": 10,
                  "txn_id": 1000,
                  "partition_id": 11
               }
            ]
        }
        )DEL")));
    {
        VacuumRequest request;
        VacuumResponse response;
        request.add_tablet_ids(10);
        request.set_min_retain_version(4);
        request.set_grace_timestamp(1696998550);
        request.set_min_active_txn_id(1000);
        request.set_delete_txn_log(true);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        ASSERT_EQ(TStatusCode::OK, response.status().status_code());
        EXPECT_TRUE(file_exist(combined_txn_log_filename(1000)));
    }
    {
        VacuumRequest request;
        VacuumResponse response;
        request.add_tablet_ids(10);
        request.set_min_retain_version(4);
        request.set_grace_timestamp(1696998550);
        request.set_min_active_txn_id(1001);
        request.set_delete_txn_log(true);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        ASSERT_EQ(TStatusCode::OK, response.status().status_code());
        EXPECT_FALSE(file_exist(combined_txn_log_filename(1000)));
    }
}

TEST_P(LakeVacuumTest, test_vacuumed_version) {
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 10001,
        "version": 2,
        "rowsets": [
            {
                "segments": [
                    "00000000000059e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                ],
                "data_size": 4096
            }
        ],
        "prev_garbage_version": 0,
        "commit_time": 1687331159
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 10001,
        "version": 3,
        "rowsets": [
            {
                "segments": [
                    "00000000000059e5_5b3c5f4b-2675-4b7a-b5e0-4006cc285815.dat"
                ],
                "data_size": 100
            }
        ],
        "prev_garbage_version": 2,
        "commit_time": 1687331160
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 10001,
        "version": 4,
        "rowsets": [
            {
                "segments": [
                    "00000000000059e5_5b3c5f4b-2675-4b7a-b5e0-4006cc285815.dat"
                ],
                "data_size": 4096
            }
        ],
        "prev_garbage_version": 3,
        "commit_time": 1687331161
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 10002,
        "version": 4,
        "rowsets": [
            {
                "segments": [
                    "00000000000059e5_5b3c5f4b-2675-4b7a-b5e0-4006cc285815.dat"
                ],
                "data_size": 4096
            }
        ],
        "prev_garbage_version": 3,
        "commit_time": 1687331162
        }
        )DEL")));

    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(true);
        request.add_tablet_ids(10001);
        request.add_tablet_ids(10002);
        request.set_min_retain_version(4);
        request.set_grace_timestamp(1687331158);
        request.set_min_active_txn_id(12344);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_EQ(1, response.vacuumed_version());
    }

    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(true);
        request.add_tablet_ids(10001);
        request.add_tablet_ids(10002);
        request.set_min_retain_version(4);
        request.set_grace_timestamp(1687331161);
        request.set_min_active_txn_id(12344);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_EQ(3, response.vacuumed_version());
    }

    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(true);
        request.add_tablet_ids(10001);
        request.add_tablet_ids(10002);
        request.set_min_retain_version(4);
        request.set_grace_timestamp(1687331162);
        request.set_min_active_txn_id(12344);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_EQ(3, response.vacuumed_version());
    }
}

<<<<<<< HEAD
=======
// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_vacuum_version_control) {
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
                    "data_size": 200,
                    "segment_metas": [
                        {
                            "filename": "00000000000159e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e11.dat",
                            "size": 100
                        },
                        {
                            "filename": "00000000000159e4_a542395a-bff5-48a7-a3a7-2ed05691b582.dat",
                            "size": 100
                        }
                    ]
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
                    "data_size": 100,
                    "segment_metas": [
                        {
                            "filename": "00000000000159e4_a542395a-bff5-48a7-a3a7-2ed05691b583.dat",
                            "size": 100
                        }
                    ]
                }
            ],
            "compaction_inputs": [
                {
                    "data_size": 200,
                    "segment_metas": [
                        {
                            "filename": "00000000000159e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e11.dat",
                            "size": 100
                        },
                        {
                            "filename": "00000000000159e4_a542395a-bff5-48a7-a3a7-2ed05691b582.dat",
                            "size": 100
                        }
                    ]
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

    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(false);
        request.add_tablet_ids(666);
        request.set_min_retain_version(3);
        request.set_grace_timestamp(::time(nullptr) + 60);
        request.set_min_active_txn_id(12345);
        request.add_retain_versions(2);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_EQ(1, response.vacuumed_files());

        EXPECT_FALSE(file_exist(tablet_metadata_filename(666, 1)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(666, 2)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(666, 3)));

        EXPECT_TRUE(file_exist("00000000000159e3_3ea06130-ccac-4110-9de8-4813512c60da.delvec"));
        EXPECT_TRUE(file_exist("00000000000159e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e11.dat"));
        EXPECT_TRUE(file_exist("00000000000159e4_a542395a-bff5-48a7-a3a7-2ed05691b582.dat"));
        EXPECT_TRUE(file_exist("0000000000011111_9ae981b3-7d4b-49e9-9723-d7f752686155.sst"));
        EXPECT_TRUE(file_exist("00000000000159e4_a542395a-bff5-48a7-a3a7-2ed05691b583.dat"));
    }
}

// Test: vacuum deletes .vi files for compaction_inputs using segment_metas
// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_vacuum_vi_files_in_compaction_inputs) {
    // Segment files
    create_data_file("00000000000a59e4_aaaa1111-1111-1111-1111-111111111111.dat");
    create_data_file("00000000000a59e4_bbbb2222-2222-2222-2222-222222222222.dat");
    create_data_file("00000000000a59e5_cccc3333-3333-3333-3333-333333333333.dat");
    // .vi files for the compaction input segments
    create_data_file("00000000000a59e4_aaaa1111-1111-1111-1111-111111111111_100.vi");
    create_data_file("00000000000a59e4_aaaa1111-1111-1111-1111-111111111111_200.vi");
    create_data_file("00000000000a59e4_bbbb2222-2222-2222-2222-222222222222_100.vi");
    // .vi file for the alive segment (should NOT be deleted)
    create_data_file("00000000000a59e5_cccc3333-3333-3333-3333-333333333333_100.vi");

    // Version 2: has the old segments (will become compaction_inputs in v3)
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
            "id": 5000,
            "version": 2,
            "rowsets": [
                {
                    "data_size": 4096,
                    "segment_metas": [
                        {
                            "filename": "00000000000a59e4_aaaa1111-1111-1111-1111-111111111111.dat",
                            "vector_index_ids": [
                                100,
                                200
                            ]
                        },
                        {
                            "filename": "00000000000a59e4_bbbb2222-2222-2222-2222-222222222222.dat",
                            "vector_index_ids": [
                                100
                            ]
                        }
                    ]
                }
            ],
            "commit_time": 1
        }
        )DEL")));

    // Version 3: compaction output replaces old segments; compaction_inputs carries segment_metas
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
            "id": 5000,
            "version": 3,
            "rowsets": [
                {
                    "data_size": 100,
                    "segment_metas": [
                        {
                            "filename": "00000000000a59e5_cccc3333-3333-3333-3333-333333333333.dat",
                            "vector_index_ids": [
                                100
                            ]
                        }
                    ]
                }
            ],
            "compaction_inputs": [
                {
                    "data_size": 4096,
                    "segment_metas": [
                        {
                            "filename": "00000000000a59e4_aaaa1111-1111-1111-1111-111111111111.dat",
                            "vector_index_ids": [
                                100,
                                200
                            ]
                        },
                        {
                            "filename": "00000000000a59e4_bbbb2222-2222-2222-2222-222222222222.dat",
                            "vector_index_ids": [
                                100
                            ]
                        }
                    ]
                }
            ],
            "prev_garbage_version": 2,
            "commit_time": 1
        }
        )DEL")));

    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(false);
        auto* info = request.add_tablet_infos();
        info->set_tablet_id(5000);
        info->set_min_version(2);
        request.set_min_retain_version(3);
        request.set_grace_timestamp(::time(nullptr) + 10);
        request.set_min_active_txn_id(99999);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        // Compaction input segments and their .vi files should be deleted
        EXPECT_FALSE(file_exist("00000000000a59e4_aaaa1111-1111-1111-1111-111111111111.dat"));
        EXPECT_FALSE(file_exist("00000000000a59e4_bbbb2222-2222-2222-2222-222222222222.dat"));
        EXPECT_FALSE(file_exist("00000000000a59e4_aaaa1111-1111-1111-1111-111111111111_100.vi"));
        EXPECT_FALSE(file_exist("00000000000a59e4_aaaa1111-1111-1111-1111-111111111111_200.vi"));
        EXPECT_FALSE(file_exist("00000000000a59e4_bbbb2222-2222-2222-2222-222222222222_100.vi"));

        // Alive segment and its .vi file should survive
        EXPECT_TRUE(file_exist("00000000000a59e5_cccc3333-3333-3333-3333-333333333333.dat"));
        EXPECT_TRUE(file_exist("00000000000a59e5_cccc3333-3333-3333-3333-333333333333_100.vi"));
    }
}

// Test: vacuum does NOT blindly delete .vi files when vector_index_ids is empty
// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_vacuum_no_blind_vi_deletion) {
    create_data_file("00000000000b59e4_dddd4444-4444-4444-4444-444444444444.dat");
    create_data_file("00000000000b59e5_eeee5555-5555-5555-5555-555555555555.dat");
    // A .vi file that happens to match the naming pattern but is NOT tracked in segment_metas
    // (e.g., leftover from a different operation). It should NOT be deleted by vacuum.
    create_data_file("00000000000b59e4_dddd4444-4444-4444-4444-444444444444_999.vi");

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
            "id": 5100,
            "version": 2,
            "rowsets": [
                {
                    "data_size": 4096,
                    "segment_metas": [
                        {
                            "filename": "00000000000b59e4_dddd4444-4444-4444-4444-444444444444.dat"
                        }
                    ]
                }
            ],
            "commit_time": 1
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
            "id": 5100,
            "version": 3,
            "rowsets": [
                {
                    "data_size": 100,
                    "segment_metas": [
                        {
                            "filename": "00000000000b59e5_eeee5555-5555-5555-5555-555555555555.dat"
                        }
                    ]
                }
            ],
            "compaction_inputs": [
                {
                    "data_size": 4096,
                    "segment_metas": [
                        {
                            "filename": "00000000000b59e4_dddd4444-4444-4444-4444-444444444444.dat"
                        }
                    ]
                }
            ],
            "prev_garbage_version": 2,
            "commit_time": 1
        }
        )DEL")));

    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(false);
        auto* info = request.add_tablet_infos();
        info->set_tablet_id(5100);
        info->set_min_version(2);
        request.set_min_retain_version(3);
        request.set_grace_timestamp(::time(nullptr) + 10);
        request.set_min_active_txn_id(99999);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        // Segment should be deleted
        EXPECT_FALSE(file_exist("00000000000b59e4_dddd4444-4444-4444-4444-444444444444.dat"));
        // .vi file is NOT tracked in segment_metas, so vacuum should NOT delete it
        EXPECT_TRUE(file_exist("00000000000b59e4_dddd4444-4444-4444-4444-444444444444_999.vi"));
    }
}

// Test: partial compaction correctly trims segment_metas in compaction_inputs
// so vacuum only deletes .vi files for truly consumed segments, not reused ones.
// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_vacuum_vi_files_partial_compaction) {
    // Scenario: rowset has segments [a, b, c, d], partial compaction consumes [b, c] -> [m]
    // Output rowset: [a, m, d] (a and d are reused from original)
    // compaction_inputs should only contain consumed segments [b, c] with their vi info
    // a.vi and d.vi must survive because they're still in the output rowset

    // Original segments
    create_data_file("00000000000e59e4_aaaa0001-0001-0001-0001-000000000001.dat"); // a - reused
    create_data_file("00000000000e59e4_bbbb0002-0002-0002-0002-000000000002.dat"); // b - consumed
    create_data_file("00000000000e59e4_cccc0003-0003-0003-0003-000000000003.dat"); // c - consumed
    create_data_file("00000000000e59e4_dddd0004-0004-0004-0004-000000000004.dat"); // d - reused
    // New compacted segment
    create_data_file("00000000000e59e5_mmmm0005-0005-0005-0005-000000000005.dat"); // m - new

    // .vi files for all segments
    create_data_file("00000000000e59e4_aaaa0001-0001-0001-0001-000000000001_100.vi"); // a - must survive
    create_data_file("00000000000e59e4_bbbb0002-0002-0002-0002-000000000002_100.vi"); // b - must be deleted
    create_data_file("00000000000e59e4_cccc0003-0003-0003-0003-000000000003_100.vi"); // c - must be deleted
    create_data_file("00000000000e59e4_dddd0004-0004-0004-0004-000000000004_100.vi"); // d - must survive
    create_data_file("00000000000e59e5_mmmm0005-0005-0005-0005-000000000005_100.vi"); // m - must survive

    // Version 2: original rowset
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
            "id": 5400,
            "version": 2,
            "rowsets": [
                {
                    "id": 10,
                    "data_size": 8192,
                    "segment_metas": [
                        {
                            "filename": "00000000000e59e4_aaaa0001-0001-0001-0001-000000000001.dat",
                            "vector_index_ids": [
                                100
                            ]
                        },
                        {
                            "filename": "00000000000e59e4_bbbb0002-0002-0002-0002-000000000002.dat",
                            "vector_index_ids": [
                                100
                            ]
                        },
                        {
                            "filename": "00000000000e59e4_cccc0003-0003-0003-0003-000000000003.dat",
                            "vector_index_ids": [
                                100
                            ]
                        },
                        {
                            "filename": "00000000000e59e4_dddd0004-0004-0004-0004-000000000004.dat",
                            "vector_index_ids": [
                                100
                            ]
                        }
                    ]
                }
            ],
            "commit_time": 1
        }
        )DEL")));

    // Version 3: after partial compaction
    // Output rowset has segments [a, m, d] with their vi info
    // compaction_inputs has only consumed segments [b, c] with their vi info
    // (trim_partial_compaction_last_input_rowset should have removed a and d from inputs)
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
            "id": 5400,
            "version": 3,
            "rowsets": [
                {
                    "id": 11,
                    "data_size": 6144,
                    "segment_metas": [
                        {
                            "filename": "00000000000e59e4_aaaa0001-0001-0001-0001-000000000001.dat",
                            "vector_index_ids": [
                                100
                            ]
                        },
                        {
                            "filename": "00000000000e59e5_mmmm0005-0005-0005-0005-000000000005.dat",
                            "vector_index_ids": [
                                100
                            ]
                        },
                        {
                            "filename": "00000000000e59e4_dddd0004-0004-0004-0004-000000000004.dat",
                            "vector_index_ids": [
                                100
                            ]
                        }
                    ]
                }
            ],
            "compaction_inputs": [
                {
                    "id": 10,
                    "data_size": 4096,
                    "segment_metas": [
                        {
                            "filename": "00000000000e59e4_bbbb0002-0002-0002-0002-000000000002.dat",
                            "vector_index_ids": [
                                100
                            ]
                        },
                        {
                            "filename": "00000000000e59e4_cccc0003-0003-0003-0003-000000000003.dat",
                            "vector_index_ids": [
                                100
                            ]
                        }
                    ]
                }
            ],
            "prev_garbage_version": 2,
            "commit_time": 1
        }
        )DEL")));

    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(false);
        auto* info = request.add_tablet_infos();
        info->set_tablet_id(5400);
        info->set_min_version(2);
        request.set_min_retain_version(3);
        request.set_grace_timestamp(::time(nullptr) + 10);
        request.set_min_active_txn_id(99999);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        // Consumed segments and their .vi files should be deleted
        EXPECT_FALSE(file_exist("00000000000e59e4_bbbb0002-0002-0002-0002-000000000002.dat"));
        EXPECT_FALSE(file_exist("00000000000e59e4_cccc0003-0003-0003-0003-000000000003.dat"));
        EXPECT_FALSE(file_exist("00000000000e59e4_bbbb0002-0002-0002-0002-000000000002_100.vi"));
        EXPECT_FALSE(file_exist("00000000000e59e4_cccc0003-0003-0003-0003-000000000003_100.vi"));

        // Reused segments and their .vi files must survive
        EXPECT_TRUE(file_exist("00000000000e59e4_aaaa0001-0001-0001-0001-000000000001.dat"));
        EXPECT_TRUE(file_exist("00000000000e59e4_dddd0004-0004-0004-0004-000000000004.dat"));
        EXPECT_TRUE(file_exist("00000000000e59e4_aaaa0001-0001-0001-0001-000000000001_100.vi"));
        EXPECT_TRUE(file_exist("00000000000e59e4_dddd0004-0004-0004-0004-000000000004_100.vi"));

        // New compacted segment and its .vi file must survive
        EXPECT_TRUE(file_exist("00000000000e59e5_mmmm0005-0005-0005-0005-000000000005.dat"));
        EXPECT_TRUE(file_exist("00000000000e59e5_mmmm0005-0005-0005-0005-000000000005_100.vi"));
    }
}

// Test: delete_tablets deletes .vi files using segment_metas
// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_delete_tablets_vi_files) {
    // Segments in alive rowsets
    create_data_file("00000000000c59e4_1111aaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.dat");
    create_data_file("00000000000c59e4_2222bbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb.dat");
    // .vi files for alive rowsets
    create_data_file("00000000000c59e4_1111aaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa_300.vi");
    create_data_file("00000000000c59e4_2222bbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb_300.vi");
    create_data_file("00000000000c59e4_2222bbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb_400.vi");
    // Segments in compaction_inputs
    create_data_file("00000000000c59e3_3333cccc-cccc-cccc-cccc-cccccccccccc.dat");
    // .vi files for compaction_inputs
    create_data_file("00000000000c59e3_3333cccc-cccc-cccc-cccc-cccccccccccc_300.vi");

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
            "id": 5200,
            "version": 2,
            "rowsets": [
                {
                    "segment_metas": [
                        {
                            "filename": "00000000000c59e4_1111aaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.dat",
                            "vector_index_ids": [
                                300
                            ]
                        },
                        {
                            "filename": "00000000000c59e4_2222bbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb.dat",
                            "vector_index_ids": [
                                300,
                                400
                            ]
                        }
                    ]
                }
            ],
            "compaction_inputs": [
                {
                    "data_size": 2048,
                    "segment_metas": [
                        {
                            "filename": "00000000000c59e3_3333cccc-cccc-cccc-cccc-cccccccccccc.dat",
                            "vector_index_ids": [
                                300
                            ]
                        }
                    ]
                }
            ],
            "prev_garbage_version": 1
        }
        )DEL")));

    {
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(5200);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        // All segments should be deleted
        EXPECT_FALSE(file_exist("00000000000c59e4_1111aaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.dat"));
        EXPECT_FALSE(file_exist("00000000000c59e4_2222bbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb.dat"));
        EXPECT_FALSE(file_exist("00000000000c59e3_3333cccc-cccc-cccc-cccc-cccccccccccc.dat"));

        // All .vi files should be deleted (tracked in segment_metas)
        EXPECT_FALSE(file_exist("00000000000c59e4_1111aaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa_300.vi"));
        EXPECT_FALSE(file_exist("00000000000c59e4_2222bbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb_300.vi"));
        EXPECT_FALSE(file_exist("00000000000c59e4_2222bbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb_400.vi"));
        EXPECT_FALSE(file_exist("00000000000c59e3_3333cccc-cccc-cccc-cccc-cccccccccccc_300.vi"));

        // Metadata should be deleted
        EXPECT_FALSE(file_exist(tablet_metadata_filename(5200, 2)));
    }
}

// Test: find_orphan_data_files protects .vi files referenced by segment_metas
// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_find_orphan_vi_files) {
    // Referenced segment and its .vi file
    create_data_file("00000000000d59e4_aaaa1111-1111-1111-1111-111111111111.dat");
    create_data_file("00000000000d59e4_aaaa1111-1111-1111-1111-111111111111_500.vi");
    // Orphan .vi file (not tracked in any segment_metas)
    create_data_file("00000000000d59e4_aaaa1111-1111-1111-1111-111111111111_999.vi");

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
            "id": 5300,
            "version": 2,
            "rowsets": [
                {
                    "segment_metas": [
                        {
                            "filename": "00000000000d59e4_aaaa1111-1111-1111-1111-111111111111.dat",
                            "vector_index_ids": [
                                500
                            ]
                        }
                    ]
                }
            ],
            "commit_time": 1
        }
        )DEL")));

    ASSIGN_OR_ABORT(auto fs, FileSystemFactory::CreateSharedFromString(kTestDir));
    auto metadata_root = join_path(kTestDir, kMetadataDirectoryName);
    std::list<std::string> meta_files;
    ASSERT_OK(ignore_not_found(fs->iterate_dir(metadata_root, [&](std::string_view name) {
        if (is_tablet_metadata(name)) {
            meta_files.emplace_back(name);
        }
        return true;
    })));
    std::list<std::string> bundle_meta_files;

    ASSIGN_OR_ABORT(auto orphan_files, find_orphan_data_files(fs.get(), kTestDir, 0 /*expired_seconds*/, meta_files,
                                                              bundle_meta_files, nullptr /*audit_ostream*/));

    // The referenced .vi file should NOT be in orphan list
    EXPECT_EQ(orphan_files.count("00000000000d59e4_aaaa1111-1111-1111-1111-111111111111_500.vi"), 0);
    // The referenced segment should NOT be in orphan list
    EXPECT_EQ(orphan_files.count("00000000000d59e4_aaaa1111-1111-1111-1111-111111111111.dat"), 0);
    // The untracked .vi file SHOULD be in orphan list
    EXPECT_EQ(orphan_files.count("00000000000d59e4_aaaa1111-1111-1111-1111-111111111111_999.vi"), 1);
}

// Test: vacuum honours the `i < segment_metas_size()` defensive guard in
// delete_rowset_vi_files when a rowset has only partial segment_metas
// (a mix of segments — some with VI tracking, some without). Pre-existing
// rowsets that landed before precise-vacuum tracking can have segment_metas
// shorter than segments. The vacuum must:
//   * delete .vi files for segments that DO have segment_metas entries, and
//   * not crash and not fabricate filenames for segments without entries.
// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_vacuum_partial_segment_metas) {
    // Two segments in the compaction inputs. seg_a has segment_metas with a
    // .vi entry; seg_b does not. Vacuum should delete seg_a's .vi only.
    create_data_file("00000000000e59e4_p1111111-1111-1111-1111-111111111111.dat");
    create_data_file("00000000000e59e4_p2222222-2222-2222-2222-222222222222.dat");
    create_data_file("00000000000e59e4_p1111111-1111-1111-1111-111111111111_700.vi");
    // The fresh-version segment (alive after compaction).
    create_data_file("00000000000e59e5_p3333333-3333-3333-3333-333333333333.dat");

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
            "id": 5400,
            "version": 2,
            "rowsets": [
                {
                    "data_size": 4096,
                    "segment_metas": [
                        {
                            "filename": "00000000000e59e4_p1111111-1111-1111-1111-111111111111.dat",
                            "vector_index_ids": [
                                700
                            ]
                        },
                        {
                            "filename": "00000000000e59e4_p2222222-2222-2222-2222-222222222222.dat"
                        }
                    ]
                }
            ],
            "commit_time": 1
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
            "id": 5400,
            "version": 3,
            "rowsets": [
                {
                    "data_size": 200,
                    "segment_metas": [
                        {
                            "filename": "00000000000e59e5_p3333333-3333-3333-3333-333333333333.dat"
                        }
                    ]
                }
            ],
            "compaction_inputs": [
                {
                    "data_size": 4096,
                    "segment_metas": [
                        {
                            "filename": "00000000000e59e4_p1111111-1111-1111-1111-111111111111.dat",
                            "vector_index_ids": [
                                700
                            ]
                        },
                        {
                            "filename": "00000000000e59e4_p2222222-2222-2222-2222-222222222222.dat"
                        }
                    ]
                }
            ],
            "prev_garbage_version": 2,
            "commit_time": 1
        }
        )DEL")));

    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(true);
        request.add_tablet_ids(5400);
        request.set_min_retain_version(3);
        request.set_grace_timestamp(::time(nullptr) + 10);
        request.set_min_active_txn_id(1000);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_OK(Status(response.status()));
    }

    ASSIGN_OR_ABORT(auto fs, FileSystemFactory::CreateSharedFromString(kTestDir));
    auto data_dir = join_path(kTestDir, kSegmentDirectoryName);
    auto exists = [&](const std::string& name) { return fs->path_exists(join_path(data_dir, name)).ok(); };

    // Both compaction-input segments removed.
    EXPECT_FALSE(exists("00000000000e59e4_p1111111-1111-1111-1111-111111111111.dat"));
    EXPECT_FALSE(exists("00000000000e59e4_p2222222-2222-2222-2222-222222222222.dat"));
    // The .vi tracked by segment_metas[0] is deleted.
    EXPECT_FALSE(exists("00000000000e59e4_p1111111-1111-1111-1111-111111111111_700.vi"));
    // The alive segment must still exist; vacuum did not crash on the partial-metas rowset.
    EXPECT_TRUE(exists("00000000000e59e5_p3333333-3333-3333-3333-333333333333.dat"));
}

// IDG: a referenced .idx file (in idg_meta) survives vacuum, while one only
// listed in orphan_files is removed. Mirrors the DCG referenced/orphan split.
// The .idx file in idg_meta is *not* in orphan_files so it survives the
// orphan-cleanup pass; the .idx file marked orphan gets deleted.
TEST_P(LakeVacuumTest, idg_idx_files_referenced_by_metadata_are_kept) {
    const std::string ref_idx = "0000000000abc001_idg_referenced.idx";
    const std::string orphan_idx = "0000000000abc002_idg_orphan.idx";
    create_data_file(ref_idx);
    create_data_file(orphan_idx);

    auto meta = std::make_shared<TabletMetadataPB>();
    meta->set_id(8001);
    meta->set_version(2);
    auto& ver = (*meta->mutable_idg_meta()->mutable_idgs())[1];
    auto* e = ver.add_entries();
    e->set_index_file(ref_idx);
    auto* of = meta->add_orphan_files();
    of->set_name(orphan_idx);
    of->set_size(11);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(meta));

    VacuumRequest request;
    VacuumResponse response;
    request.set_delete_txn_log(true);
    request.add_tablet_ids(8001);
    request.set_min_retain_version(2);
    request.set_grace_timestamp(::time(nullptr) + 10);
    request.set_min_active_txn_id(12345);
    vacuum(_tablet_mgr.get(), request, &response);
    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

    EXPECT_TRUE(file_exist(ref_idx));
    EXPECT_FALSE(file_exist(orphan_idx));
}

// IDG: when an .idx file is in idg_meta of an old version but the latest
// retained metadata no longer references it, datafile_gc treats it as
// referenced via the live-versions check (lines 1237-1245).
TEST_P(LakeVacuumTest, idg_idx_files_unreferenced_are_orphaned) {
    const std::string live_idx = "0000000000abc010_idg_live.idx";
    const std::string stranded_idx = "0000000000abc011_idg_stranded.idx";
    create_data_file(live_idx);
    create_data_file(stranded_idx);

    auto live = std::make_shared<TabletMetadataPB>();
    live->set_id(8002);
    live->set_version(1);
    auto& ver = (*live->mutable_idg_meta()->mutable_idgs())[2];
    ver.add_entries()->set_index_file(live_idx);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(live));

    // datafile_gc with do_delete=true must drop stranded_idx but keep live_idx.
    ASSERT_OK(datafile_gc(kTestDir, "", 0, true));
    EXPECT_TRUE(file_exist(live_idx));
    EXPECT_FALSE(file_exist(stranded_idx));
}

// Drop tablet local cache evicts active IDG .idx files (vacuum.cpp 1524-1528).
TEST_P(LakeVacuumTest, full_vacuum_drops_local_cache_for_active_idx) {
    constexpr int64_t kTabletId = 8003;

    auto meta = std::make_shared<TabletMetadataPB>();
    meta->set_id(kTabletId);
    meta->set_version(2);
    meta->set_prev_garbage_version(0);
    auto& ver = (*meta->mutable_idg_meta()->mutable_idgs())[5];
    auto* e = ver.add_entries();
    e->set_index_file("8003_active.idx");
    e->set_file_size(77);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(meta));

    std::vector<std::string> dropped;
    SyncPoint::GetInstance()->SetCallBack("drop_tablet_cache:drop_local_cache", [&](void* arg) {
        auto* path = reinterpret_cast<const std::string*>(arg);
        dropped.emplace_back(::starrocks::path_util::base_name(*path));
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([&] {
        SyncPoint::GetInstance()->ClearCallBack("drop_tablet_cache:drop_local_cache");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    ASSERT_OK(drop_tablet_cache(_tablet_mgr.get(), kTabletId, 2));
    EXPECT_NE(std::find(dropped.begin(), dropped.end(), std::string("8003_active.idx")), dropped.end());
}

// A deadline that expires while walking the version chain must stop the walk early without
// deleting anything; the next run (no deadline pressure) completes normally.
// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_vacuum_deadline_expired_mid_walk) {
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
            "id": 20002,
            "version": 2,
            "prev_garbage_version": 0,
            "commit_time": 1687331159
        }
        )DEL")));
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
            "id": 20002,
            "version": 3,
            "prev_garbage_version": 2,
            "commit_time": 1687331160
        }
        )DEL")));
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
            "id": 20002,
            "version": 4,
            "prev_garbage_version": 3,
            "commit_time": 1687331161
        }
        )DEL")));

    // The first checks (request entry, first two walk iterations) observe a mocked clock
    // before the deadline, every later check observes one far past it, so the deadline
    // expires in the middle of the version chain walk.
    int64_t check_count = 0;
    SyncPoint::GetInstance()->SetCallBack("vacuum:check_deadline", [&](void* arg) {
        check_count++;
        *(int64_t*)arg = (check_count > 3) ? (int64_t{1} << 62) : 0;
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("vacuum:check_deadline");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    {
        VacuumRequest request;
        VacuumResponse response;
        request.add_tablet_ids(20002);
        request.set_min_retain_version(4);
        request.set_grace_timestamp(1687331162);
        request.set_min_active_txn_id(12344);
        vacuum(_tablet_mgr.get(), request, &response, /*deadline_ms=*/1);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(TStatusCode::TIMEOUT, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_GT(check_count, 3);
        EXPECT_EQ(0, response.vacuumed_files());
        EXPECT_TRUE(file_exist(tablet_metadata_filename(20002, 2)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(20002, 3)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(20002, 4)));
    }

    SyncPoint::GetInstance()->ClearCallBack("vacuum:check_deadline");

    {
        VacuumRequest request;
        VacuumResponse response;
        request.add_tablet_ids(20002);
        request.set_min_retain_version(4);
        request.set_grace_timestamp(1687331162);
        request.set_min_active_txn_id(12344);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_EQ(4, response.vacuumed_version());
        EXPECT_FALSE(file_exist(tablet_metadata_filename(20002, 2)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(20002, 3)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(20002, 4)));
    }
}

// A task that reaches the BE with less than 1/10 of the FE timeout window left must abort at the
// entry, before walking any metadata: the walk could not finish in the time remaining and would
// only end in a mid-walk timeout that advances nothing, so it should never start.
// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_vacuum_deadline_window_too_small_to_start) {
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
            "id": 20003,
            "version": 2,
            "prev_garbage_version": 0,
            "commit_time": 1687331159
        }
        )DEL")));
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
            "id": 20003,
            "version": 3,
            "prev_garbage_version": 2,
            "commit_time": 1687331160
        }
        )DEL")));

    // Pin the clock to a moment that is still before the real deadline (1000000) but inside the
    // minimum start window. With timeout_ms=600000 that window is min(5min, 600000/10)=60000ms, so
    // the entry brings the effective deadline forward to 940000 and 950000 >= 940000 fails fast,
    // even though the strict "now >= 1000000" test would not.
    int64_t check_count = 0;
    SyncPoint::GetInstance()->SetCallBack("vacuum:check_deadline", [&](void* arg) {
        check_count++;
        *(int64_t*)arg = 950000;
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("vacuum:check_deadline");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    VacuumRequest request;
    VacuumResponse response;
    request.add_tablet_ids(20003);
    request.set_min_retain_version(3);
    request.set_grace_timestamp(1687331162);
    request.set_min_active_txn_id(12344);
    request.set_timeout_ms(600000);
    vacuum(_tablet_mgr.get(), request, &response, /*deadline_ms=*/1000000);
    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(TStatusCode::TIMEOUT, response.status().status_code()) << response.status().error_msgs(0);
    // Aborted at the entry: the version chain was never walked.
    EXPECT_EQ(1, check_count);
    EXPECT_EQ(0, response.vacuumed_files());
    EXPECT_TRUE(file_exist(tablet_metadata_filename(20003, 2)));
    EXPECT_TRUE(file_exist(tablet_metadata_filename(20003, 3)));
}

>>>>>>> 4e25291a18 ([BugFix] Abort BE vacuum tasks once the FE caller's timeout elapses (#74694))
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

TEST(LakeVacuumTest2, test_delete_files_retry) {
    WritableFileOptions options;
    options.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_ABORT(auto f1, fs::new_writable_file(options, "test_vacuum_delete_files_retry.txt"));
    ASSERT_OK(f1->append("111"));
    ASSERT_OK(f1->close());

    int attempts = 0;
    SyncPoint::GetInstance()->SetCallBack("PosixFileSystem::delete_file", [&](void* arg) {
        if (attempts++ < 2) {
            auto st = (Status*)arg;
            EXPECT_TRUE(st->ok()) << *st;
            st->update(Status::InternalError("Reduce your request rate"));
        }
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([&]() {
        SyncPoint::GetInstance()->ClearCallBack("PosixFileSystem::delete_file");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    auto future = delete_files_callable({"test_vacuum_delete_files_retry.txt"});
    ASSERT_TRUE(future.valid());
    ASSERT_TRUE(future.get().ok());
    ASSERT_FALSE(fs::path_exist("test_vacuum_delete_files_retry.txt"));
    EXPECT_GT(attempts, 1);
}

TEST(LakeVacuumTest2, test_delete_files_retry2) {
    WritableFileOptions options;
    options.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    std::string testFile("test_vacuum_delete_files_retry2.txt");
    ASSIGN_OR_ABORT(auto f1, fs::new_writable_file(options, testFile));
    ASSERT_OK(f1->append("111"));
    ASSERT_OK(f1->close());

    auto backup = config::lake_vacuum_retry_pattern.value();
    config::lake_vacuum_retry_pattern = ""; // Disable retry
    DeferOp defer0([&]() { config::lake_vacuum_retry_pattern = backup; });

    int attempts = 0;
    SyncPoint::GetInstance()->SetCallBack("PosixFileSystem::delete_file", [&](void* arg) {
        auto st = (Status*)arg;
        EXPECT_TRUE(st->ok()) << *st;
        st->update(Status::InternalError("Reduce your request rate"));
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([&]() {
        attempts++;
        SyncPoint::GetInstance()->ClearCallBack("PosixFileSystem::delete_file");
        SyncPoint::GetInstance()->DisableProcessing();
        fs::delete_file(testFile);
    });

    auto future2 = delete_files_callable({testFile});
    ASSERT_TRUE(future2.valid());
    ASSERT_FALSE(future2.get().ok());
    ASSERT_TRUE(fs::path_exist(testFile));
    EXPECT_EQ(0, attempts);
}

TEST(LakeVacuumTest2, test_delete_files_retry3) {
    WritableFileOptions options;
    options.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    std::string testFile("test_vacuum_delete_files_retry3.txt");
    ASSIGN_OR_ABORT(auto f1, fs::new_writable_file(options, testFile));
    ASSERT_OK(f1->append("111"));
    ASSERT_OK(f1->close());

    auto backup = config::lake_vacuum_retry_max_attempts;
    config::lake_vacuum_retry_max_attempts = 0; // Disable retry
    DeferOp defer0([&]() { config::lake_vacuum_retry_max_attempts = backup; });

    int attempts = 0;
    SyncPoint::GetInstance()->SetCallBack("PosixFileSystem::delete_file", [&](void* arg) {
        auto st = (Status*)arg;
        EXPECT_TRUE(st->ok()) << *st;
        st->update(Status::InternalError("Reduce your request rate"));
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([&]() {
        attempts++;
        SyncPoint::GetInstance()->ClearCallBack("PosixFileSystem::delete_file");
        SyncPoint::GetInstance()->DisableProcessing();
        fs::delete_file(testFile);
    });

    auto future = delete_files_callable({testFile});
    ASSERT_TRUE(future.valid());
    ASSERT_FALSE(future.get().ok());
    ASSERT_TRUE(fs::path_exist(testFile));
    EXPECT_EQ(0, attempts);
}

TEST(LakeVacuumTest2, test_delete_files_retry4) {
    WritableFileOptions options;
    options.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_ABORT(auto f1, fs::new_writable_file(options, "test_vacuum_delete_files_retry.txt"));
    ASSERT_OK(f1->append("111"));
    ASSERT_OK(f1->close());

    int attempts = 0;
    SyncPoint::GetInstance()->SetCallBack("PosixFileSystem::delete_file", [&](void* arg) {
        if (attempts++ < 2) {
            auto st = (Status*)arg;
            EXPECT_TRUE(st->ok()) << *st;
            st->update(Status::ResourceBusy(""));
        }
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([&]() {
        SyncPoint::GetInstance()->ClearCallBack("PosixFileSystem::delete_file");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    auto future = delete_files_callable({"test_vacuum_delete_files_retry.txt"});
    ASSERT_TRUE(future.valid());
    ASSERT_TRUE(future.get().ok());
    ASSERT_FALSE(fs::path_exist("test_vacuum_delete_files_retry.txt"));
    EXPECT_GT(attempts, 1);
}

TEST(LakeVacuumTest2, test_calculate_retry_delay_jitter) {
    // Pure helper: no global config dependency. All knobs passed in.
    const int64_t base = 100;
    const int64_t max_retries = 5;
    const int64_t cap = base * (1L << max_retries); // = 3200
    constexpr int kSamples = 200;

    // 1. First call: last_delay = base. Window is [base, min(cap, base*3)] = [100, 300].
    {
        std::set<int64_t> observed;
        for (int i = 0; i < kSamples; ++i) {
            int64_t delay = calculate_retry_delay(base, base, max_retries);
            EXPECT_GE(delay, base) << "delay=" << delay;
            EXPECT_LE(delay, std::min(cap, base * 3)) << "delay=" << delay;
            observed.insert(delay);
        }
        EXPECT_GT(observed.size(), 1U) << "first-call jitter inactive";
    }

    // 2. Cap clamping: once last_delay * 3 exceeds cap, delays must not exceed cap.
    {
        int64_t large_last = cap; // last_delay already at cap
        std::set<int64_t> observed;
        for (int i = 0; i < kSamples; ++i) {
            int64_t delay = calculate_retry_delay(large_last, base, max_retries);
            EXPECT_GE(delay, base);
            EXPECT_LE(delay, cap) << "cap not enforced, delay=" << delay;
            observed.insert(delay);
        }
        EXPECT_GT(observed.size(), 1U) << "cap-clamped jitter inactive";
    }

    // 3. Simulated retry chain: feed last_delay back; every step stays in [base, cap].
    {
        int64_t last_delay = base;
        for (int step = 0; step < 20; ++step) {
            last_delay = calculate_retry_delay(last_delay, base, max_retries);
            EXPECT_GE(last_delay, base) << "step=" << step;
            EXPECT_LE(last_delay, cap) << "step=" << step;
        }
    }
}

} // namespace starrocks::lake
