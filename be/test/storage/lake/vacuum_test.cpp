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
#include <unistd.h>

#include <algorithm>
#include <ctime>
#include <set>
#include <vector>

#include "base/path/path_util.h"
#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "common/config_lake_fwd.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "json2pb/json_to_pb.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/vacuum_full.h"
#include "test_util.h"

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
        } else if (is_txn_log(name) || is_txn_slog(name) || is_txn_vlog(name) || is_combined_txn_log(name)) {
            full_path = join_path(join_path(kTestDir, kTxnLogDirectoryName), name);
        } else if (is_segment(name) || is_delvec(name) || is_del(name) || is_sst(name) || is_vector_index(name)) {
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

// Check that vacuum_full cleans up the expected metadata files
// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_vacuum_full) {
    create_data_file("0000000000000001_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat");
    create_data_file("0000000000000001_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat");
    create_data_file("000000000000FFFF_a542f95a-bff5-48a7-a3a7-2ed05691b58c.dat");
    create_data_file("0000000000000002_a542ff5a-bff5-48a7-a3a7-2ed05691b58c.dat");

    VacuumFullRequest request;
    request.set_partition_id(1);
    request.set_tablet_id(66600);
    request.set_min_active_txn_id(10);
    request.set_grace_timestamp(100);
    request.add_retain_versions(3);
    request.set_min_check_version(0);
    request.set_max_check_version(5);

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 66601,
        "version": 5,
        "rowsets": [],
        "commit_time": 1
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 66600,
        "version": 6,
        "rowsets": [
            {
                "segments": [
                    "0000000000000001_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                ],
                "data_size": 4096
            }
        ],
        "commit_time": 99,
        "prev_garbage_version": 3
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 66600,
        "version": 5,
        "rowsets": [],
        "commit_time": 99,
        "prev_garbage_version": 3
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 66600,
        "version": 4,
        "rowsets": [
            {
                "segments": [
                    "0000000000000001_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"
                ],
                "data_size": 4096
            }
        ],
        "commit_time": 98,
        "prev_garbage_version": 3
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 66600,
        "version": 3,
        "rowsets": [],
        "commit_time": 97,
        "prev_garbage_version": 3
        }
        )DEL")));

    EXPECT_TRUE(file_exist(tablet_metadata_filename(66601, 5)));
    EXPECT_TRUE(file_exist(tablet_metadata_filename(66600, 6)));
    EXPECT_TRUE(file_exist(tablet_metadata_filename(66600, 5)));
    EXPECT_TRUE(file_exist(tablet_metadata_filename(66600, 4)));
    EXPECT_TRUE(file_exist(tablet_metadata_filename(66600, 3)));

    VacuumFullResponse response;
    vacuum_full(_tablet_mgr.get(), request, &response);

    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
    EXPECT_EQ(1 + 2, response.vacuumed_files()); // 1 metadata, 2 data

    EXPECT_TRUE(file_exist(tablet_metadata_filename(66601, 5)));
    EXPECT_TRUE(file_exist(tablet_metadata_filename(66600, 6)));
    EXPECT_TRUE(file_exist(tablet_metadata_filename(66600, 5)));
    EXPECT_FALSE(file_exist(tablet_metadata_filename(66600, 4)));
    EXPECT_TRUE(file_exist(tablet_metadata_filename(66600, 3)));

    EXPECT_TRUE(file_exist("0000000000000001_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
    EXPECT_FALSE(file_exist("0000000000000001_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"));
    EXPECT_TRUE(file_exist("000000000000FFFF_a542f95a-bff5-48a7-a3a7-2ed05691b58c.dat"));
    EXPECT_FALSE(file_exist("0000000000000002_a542ff5a-bff5-48a7-a3a7-2ed05691b58c.dat"));
}

// Ensure full vacuum does not fail when initial metadata 0_1.meta exists and
// there is at least one expired metadata. Previously, trying to read 0_1.meta
// would cause NotFound and fail the whole vacuum. Now it should succeed.
// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_vacuum_full_with_initial_meta_no_failure) {
    // Create a physically present but invalid 0_1.meta so that any attempt to read it fails.
    // This ensures pre-fix implementation (which reads 0_1.meta) will fail, while the fixed
    // implementation will skip reading it and succeed.
    {
        auto initial_meta_path =
                join_path(join_path(kTestDir, kMetadataDirectoryName), tablet_initial_metadata_filename());
        ASSIGN_OR_ABORT(auto f, FileSystem::Default()->new_writable_file(initial_meta_path));
        ASSERT_OK(f->append("not-a-valid-protobuf"));
        ASSERT_OK(f->close());
    }

    // Create an expired normal metadata which will be deleted by vacuum_full
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 77001,
        "version": 2,
        "rowsets": [],
        "commit_time": 1
        }
        )DEL")));

    // Sanity: files exist before vacuum
    EXPECT_TRUE(file_exist(tablet_initial_metadata_filename()));
    EXPECT_TRUE(file_exist(tablet_metadata_filename(77001, 2)));

    VacuumFullRequest request;
    request.set_partition_id(1);
    request.set_tablet_id(77000);
    request.set_min_active_txn_id(10);
    request.set_grace_timestamp(100);
    request.set_min_check_version(0);
    request.set_max_check_version(10);

    VacuumFullResponse response;
    vacuum_full(_tablet_mgr.get(), request, &response);

    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

    // The expired normal metadata must be deleted
    EXPECT_FALSE(file_exist(tablet_metadata_filename(77001, 2)));
}

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_vacuum_full_with_bundle) {
    create_data_file("0000000000000005_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat");
    create_data_file("0000000000000005_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat");
    create_data_file("0000000000000004_a542f95a-bff5-48a7-a3a7-2ed05691b58c.dat");
    create_data_file("0000000000000004_a542ff5a-bff5-48a7-a3a7-2ed05691b58c.dat");

    VacuumFullRequest request;
    request.set_partition_id(1);
    request.set_tablet_id(66600);
    request.set_min_active_txn_id(10);
    request.set_grace_timestamp(100);
    request.set_min_check_version(0);
    request.set_max_check_version(7);

    auto tablet_66601_v8 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 66601,
        "version": 8,
        "rowsets": [],
        "commit_time": 10010
        }
        )DEL");

    auto tablet_66600_v8 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 66600,
        "version": 8,
        "rowsets": [],
        "commit_time": 10010
        }
        )DEL");

    std::map<int64_t, TabletMetadataPB> tablet_metas_v8;
    tablet_metas_v8[66601] = *tablet_66601_v8;
    tablet_metas_v8[66600] = *tablet_66600_v8;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v8));

    auto tablet_66601_v7 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 66601,
        "version": 7,
        "rowsets": [
            {
                "segments": [
                    "0000000000000005_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                ],
                "data_size": 4096
            }
        ],
        "commit_time": 11,
        "prev_garbage_version": 3
        }
        )DEL");

    auto tablet_66600_v7 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 66600,
        "version": 7,
        "rowsets": [
            {
                "segments": [
                    "0000000000000005_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"
                ],
                "data_size": 4096
            }
        ],
        "commit_time": 11,
        "prev_garbage_version": 3
        }
        )DEL");

    std::map<int64_t, TabletMetadataPB> tablet_metas_v7;
    tablet_metas_v7[66601] = *tablet_66601_v7;
    tablet_metas_v7[66600] = *tablet_66600_v7;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v7));

    auto tablet_66601_v6 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 66601,
        "version": 6,
        "rowsets": [
            {
                "segments": [
                    "0000000000000004_a542f95a-bff5-48a7-a3a7-2ed05691b58c.dat"
                ],
                "data_size": 4096
            }
        ],
        "commit_time": 10,
        "prev_garbage_version": 3
        }
        )DEL");

    auto tablet_66600_v6 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 66600,
        "version": 6,
        "rowsets": [
            {
                "segments": [
                    "0000000000000004_a542ff5a-bff5-48a7-a3a7-2ed05691b58c.dat"
                ],
                "data_size": 4096
            }
        ],
        "commit_time": 10,
        "prev_garbage_version": 3
        }
        )DEL");

    std::map<int64_t, TabletMetadataPB> tablet_metas_v6;
    tablet_metas_v6[66601] = *tablet_66601_v6;
    tablet_metas_v6[66600] = *tablet_66600_v6;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v6));

    EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 6)));
    EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 7)));
    EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 8)));

    VacuumFullResponse response;
    vacuum_full(_tablet_mgr.get(), request, &response);

    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
    EXPECT_EQ(3, response.vacuumed_files()); // 1 metadata, 2 data

    EXPECT_FALSE(file_exist(tablet_metadata_filename(0, 6)));
    EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 7)));
    EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 8)));

    EXPECT_TRUE(file_exist("0000000000000005_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
    EXPECT_TRUE(file_exist("0000000000000005_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"));
    EXPECT_FALSE(file_exist("0000000000000004_a542f95a-bff5-48a7-a3a7-2ed05691b58c.dat"));
    EXPECT_FALSE(file_exist("0000000000000004_a542ff5a-bff5-48a7-a3a7-2ed05691b58c.dat"));
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
TEST_P(LakeVacuumTest, test_delete_tablets_bundle_txnlog_files) {
    create_data_file("00000000001259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat");
    ASSERT_OK(_tablet_mgr->put_combined_txn_log(*json_to_pb<CombinedTxnLogPB>(R"DEL(
        {
            "txn_logs": [
               {
                    "tablet_id": 10,
                    "txn_id": 1000,
                    "partition_id": 111,
                    "op_write": {
                        "rowset": {
                            "segments": ["00000000001259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"],
                            "bundle_file_offsets": [
                                0
                            ]
                        }
                    }
                },
                {
                    "tablet_id": 11,
                    "txn_id": 1000,
                    "partition_id": 111,
                    "op_write": {
                        "rowset": {
                            "segments": ["00000000001259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"],
                            "bundle_file_offsets": [
                                1024
                            ]
                        }
                    }
                },
                {
                    "tablet_id": 12,
                    "txn_id": 1000,
                    "partition_id": 111,
                    "op_write": {
                        "rowset": {
                            "segments": ["00000000001259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"],
                            "bundle_file_offsets": [
                                2048
                            ]
                        }
                    }
                }
            ]
        }
        )DEL")));
    {
        // delete tablet 10
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(10);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_TRUE(file_exist(combined_txn_log_filename(1000)));
        EXPECT_TRUE(file_exist("00000000001259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
    }
    {
        // delete tablet 10,11
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(10);
        request.add_tablet_ids(11);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_TRUE(file_exist(combined_txn_log_filename(1000)));
        EXPECT_TRUE(file_exist("00000000001259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
    }
    {
        // delete tablet 10,11,12
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(10);
        request.add_tablet_ids(11);
        request.add_tablet_ids(12);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_FALSE(file_exist(combined_txn_log_filename(1000)));
        EXPECT_FALSE(file_exist("00000000001259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
    }
}

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_delete_tablets_shared_txnlog_files) {
    const std::string shared_segment = "0000000000f259e4_22222222-2222-2222-2222-2222222222b1.dat";
    create_data_file(shared_segment);

    ASSERT_OK(_tablet_mgr->put_combined_txn_log(*json_to_pb<CombinedTxnLogPB>(R"DEL(
        {
            "txn_logs": [
               {
                    "tablet_id": 2100,
                    "txn_id": 6600,
                    "partition_id": 111,
                    "op_write": {
                        "rowset": {
                            "segments": ["0000000000f259e4_22222222-2222-2222-2222-2222222222b1.dat"],
                            "shared_segments": [true]
                        }
                    }
                },
                {
                    "tablet_id": 2101,
                    "txn_id": 6600,
                    "partition_id": 111,
                    "op_write": {
                        "rowset": {
                            "segments": ["0000000000f259e4_22222222-2222-2222-2222-2222222222b1.dat"],
                            "shared_segments": [true]
                        }
                    }
                }
            ]
        }
        )DEL")));

    {
        // delete tablet 2100 only, keep shared segment because tablet 2101 is still alive.
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(2100);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_TRUE(file_exist(combined_txn_log_filename(6600)));
        EXPECT_TRUE(file_exist(shared_segment));
    }
    {
        // delete tablet 2100 and 2101, shared segment can be deleted.
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(2100);
        request.add_tablet_ids(2101);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_FALSE(file_exist(combined_txn_log_filename(6600)));
        EXPECT_FALSE(file_exist(shared_segment));
    }
}

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_delete_tablets_shared_txnlog_files_compaction_and_schema_change) {
    const std::string shared_compaction_segment = "0000000000f459e4_44444444-4444-4444-4444-4444444444d1.dat";
    const std::string bundle_schema_segment = "0000000000f559e4_55555555-5555-5555-5555-5555555555e1.dat";
    create_data_file(shared_compaction_segment);
    create_data_file(bundle_schema_segment);

    ASSERT_OK(_tablet_mgr->put_combined_txn_log(*json_to_pb<CombinedTxnLogPB>(R"DEL(
        {
            "txn_logs": [
                {
                    "tablet_id": 2300,
                    "txn_id": 7700,
                    "partition_id": 111,
                    "op_compaction": {
                        "output_rowset": {
                            "segments": ["0000000000f459e4_44444444-4444-4444-4444-4444444444d1.dat"],
                            "shared_segments": [true]
                        }
                    }
                },
                {
                    "tablet_id": 2301,
                    "txn_id": 7700,
                    "partition_id": 111
                }
            ]
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_combined_txn_log(*json_to_pb<CombinedTxnLogPB>(R"DEL(
        {
            "txn_logs": [
                {
                    "tablet_id": 2310,
                    "txn_id": 7701,
                    "partition_id": 111,
                    "op_schema_change": {
                        "rowsets": [
                            {
                                "segments": ["0000000000f559e4_55555555-5555-5555-5555-5555555555e1.dat"],
                                "bundle_file_offsets": [0]
                            }
                        ]
                    }
                },
                {
                    "tablet_id": 2311,
                    "txn_id": 7701,
                    "partition_id": 111
                }
            ]
        }
        )DEL")));

    {
        // keep one tablet alive in each combined txn log, shared/bundle segment should not be deleted.
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(2300);
        request.add_tablet_ids(2310);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_TRUE(file_exist(combined_txn_log_filename(7700)));
        EXPECT_TRUE(file_exist(combined_txn_log_filename(7701)));
        EXPECT_TRUE(file_exist(shared_compaction_segment));
        EXPECT_TRUE(file_exist(bundle_schema_segment));
    }

    {
        // delete all tablets in each combined txn log, shared/bundle segment can be deleted.
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(2300);
        request.add_tablet_ids(2301);
        request.add_tablet_ids(2310);
        request.add_tablet_ids(2311);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_FALSE(file_exist(combined_txn_log_filename(7700)));
        EXPECT_FALSE(file_exist(combined_txn_log_filename(7701)));
        EXPECT_FALSE(file_exist(shared_compaction_segment));
        EXPECT_FALSE(file_exist(bundle_schema_segment));
    }
}

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_delete_tablets_bundle_metadata_files) {
    // create bundile metadata files
    create_data_file("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat");
    create_data_file("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat");
    create_data_file("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat");
    create_data_file("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58d.dat");

    auto t600_v1 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 600,
        "version": 1,
        "rowsets": []
        }
        )DEL");

    auto t601_v1 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 601,
        "version": 1,
        "rowsets": []
        }
        )DEL");

    auto t600_v2 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 600,
        "version": 2,
        "rowsets": [
            {
                "segments": [
                    "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                ],
                "data_size": 4096,
                "bundle_file_offsets": [
                    0
                ]
            }
        ]
        }
        )DEL");

    auto t601_v2 = json_to_pb<TabletMetadataPB>(R"DEL(
            {
            "id": 601,
            "version": 2,
            "rowsets": [
                {
                    "segments": [
                        "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                    ],
                    "data_size": 4096,
                    "bundle_file_offsets": [
                        4096
                    ]
                }
            ]
            }
            )DEL");

    auto t600_v3 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 600,
        "version": 3,
        "rowsets": [
            {
                "segments": [
                    "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                ],
                "data_size": 4096,
                "bundle_file_offsets": [
                    0
                ]
            },
            {
                "segments": [
                    "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat"
                ],
                "data_size": 4096,
                "bundle_file_offsets": [
                    0
                ]
            }
        ]
        }
        )DEL");

    auto t601_v3 = json_to_pb<TabletMetadataPB>(R"DEL(
            {
            "id": 601,
            "version": 3,
            "rowsets": [
                {
                    "segments": [
                        "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                    ],
                    "data_size": 4096,
                    "bundle_file_offsets": [
                        4096
                    ]
                },
                {
                    "segments": [
                        "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat"
                    ],
                    "data_size": 4096,
                    "bundle_file_offsets": [
                        4096
                    ]
                }
            ]
            }
            )DEL");
    // after compaction
    auto t600_v4 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 600,
        "version": 4,
        "rowsets": [
            {
                "segments": [
                    "00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"
                ],
                "data_size": 8192
            }
        ],
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                ],
                "data_size": 4096,
                "bundle_file_offsets": [
                    0
                ]
            },
            {
                "segments": [
                    "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat"
                ],
                "data_size": 4096,
                "bundle_file_offsets": [
                    0
                ]
            }
        ],
        "prev_garbage_version": 3
        }
        )DEL");

    auto t601_v4 = json_to_pb<TabletMetadataPB>(R"DEL(
            {
            "id": 601,
            "version": 4,
            "rowsets": [
                {
                    "segments": [
                        "00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58d.dat"
                    ],
                    "data_size": 8192
                }
            ],
            "compaction_inputs": [
                {
                    "segments": [
                        "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                    ],
                    "data_size": 4096,
                    "bundle_file_offsets": [
                        4096
                    ]
                },
                {
                    "segments": [
                        "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat"
                    ],
                    "data_size": 4096,
                    "bundle_file_offsets": [
                        4096
                    ]
                }
            ]
            }
            )DEL");

    // create SharedTabletMetadata
    std::map<int64_t, TabletMetadataPB> tablet_metas_v1;
    tablet_metas_v1[600] = *t600_v1;
    tablet_metas_v1[601] = *t601_v1;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v1));
    std::map<int64_t, TabletMetadataPB> tablet_metas_v2;
    tablet_metas_v2[600] = *t600_v2;
    tablet_metas_v2[601] = *t601_v2;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v2));
    std::map<int64_t, TabletMetadataPB> tablet_metas_v3;
    tablet_metas_v3[600] = *t600_v3;
    tablet_metas_v3[601] = *t601_v3;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v3));
    std::map<int64_t, TabletMetadataPB> tablet_metas_v4;
    tablet_metas_v4[600] = *t600_v4;
    tablet_metas_v4[601] = *t601_v4;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v4));

    // delete tablet 600
    {
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(600);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 1)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 2)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 3)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 4)));

        EXPECT_TRUE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
        EXPECT_TRUE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat"));
        EXPECT_FALSE(file_exist("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"));
        EXPECT_TRUE(file_exist("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58d.dat"));
    }
    // delete tablet 601
    {
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(601);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 1)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 2)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 3)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 4)));

        EXPECT_TRUE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
        EXPECT_TRUE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat"));
        EXPECT_FALSE(file_exist("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"));
        EXPECT_FALSE(file_exist("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58d.dat"));
    }
    // delete tablet 600,601
    {
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(600);
        request.add_tablet_ids(601);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 1)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(0, 2)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(0, 3)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(0, 4)));

        EXPECT_FALSE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
        EXPECT_FALSE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat"));
        EXPECT_FALSE(file_exist("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"));
        EXPECT_FALSE(file_exist("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58d.dat"));
    }
}

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_delete_tablets_shared_metadata_files) {
    const std::string shared_segment = "0000000000f159e4_11111111-1111-1111-1111-1111111111a1.dat";
    const std::string shared_delvec = "0000000000f159e4_11111111-1111-1111-1111-1111111111a2.delvec";
    const std::string shared_sstable = "0000000000f159e4_11111111-1111-1111-1111-1111111111a3.sst";
    create_data_file(shared_segment);
    create_data_file(shared_delvec);
    create_data_file(shared_sstable);

    auto t710_v1 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 710,
        "version": 1,
        "rowsets": []
        }
        )DEL");

    auto t711_v1 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 711,
        "version": 1,
        "rowsets": []
        }
        )DEL");

    auto t710_v2 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 710,
        "version": 2,
        "rowsets": [
            {
                "segments": [
                    "0000000000f159e4_11111111-1111-1111-1111-1111111111a1.dat"
                ],
                "data_size": 4096,
                "shared_segments": [true]
            }
        ],
        "delvec_meta": {
            "version_to_file": [
                {
                    "key": 2,
                    "value": {
                        "name": "0000000000f159e4_11111111-1111-1111-1111-1111111111a2.delvec",
                        "size": 32,
                        "shared": true
                    }
                }
            ]
        },
        "sstable_meta": {
            "sstables": [
                {
                    "filename": "0000000000f159e4_11111111-1111-1111-1111-1111111111a3.sst",
                    "shared": true
                }
            ]
        }
        }
        )DEL");

    auto t711_v2 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 711,
        "version": 2,
        "rowsets": [
            {
                "segments": [
                    "0000000000f159e4_11111111-1111-1111-1111-1111111111a1.dat"
                ],
                "data_size": 4096,
                "shared_segments": [true]
            }
        ],
        "delvec_meta": {
            "version_to_file": [
                {
                    "key": 2,
                    "value": {
                        "name": "0000000000f159e4_11111111-1111-1111-1111-1111111111a2.delvec",
                        "size": 32,
                        "shared": true
                    }
                }
            ]
        },
        "sstable_meta": {
            "sstables": [
                {
                    "filename": "0000000000f159e4_11111111-1111-1111-1111-1111111111a3.sst",
                    "shared": true
                }
            ]
        }
        }
        )DEL");

    std::map<int64_t, TabletMetadataPB> tablet_metas_v1;
    tablet_metas_v1[710] = *t710_v1;
    tablet_metas_v1[711] = *t711_v1;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v1));

    std::map<int64_t, TabletMetadataPB> tablet_metas_v2;
    tablet_metas_v2[710] = *t710_v2;
    tablet_metas_v2[711] = *t711_v2;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v2));

    {
        // delete tablet 710 only, shared files should be kept.
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(710);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        EXPECT_TRUE(file_exist(shared_segment));
        EXPECT_TRUE(file_exist(shared_delvec));
        EXPECT_TRUE(file_exist(shared_sstable));
    }

    {
        // delete both tablets, shared files can be deleted.
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(710);
        request.add_tablet_ids(711);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        EXPECT_FALSE(file_exist(shared_segment));
        EXPECT_FALSE(file_exist(shared_delvec));
        EXPECT_FALSE(file_exist(shared_sstable));
    }
}

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_delete_tablets_shared_metadata_files_with_dcg) {
    const std::string shared_dcg_file = "0000000000f359e4_33333333-3333-3333-3333-3333333333c1.dat";
    const std::string private_dcg_file_720 = "0000000000f359e4_33333333-3333-3333-3333-3333333333c2.dat";
    const std::string private_dcg_file_721 = "0000000000f359e4_33333333-3333-3333-3333-3333333333c3.dat";
    create_data_file(shared_dcg_file);
    create_data_file(private_dcg_file_720);
    create_data_file(private_dcg_file_721);

    auto t720_v1 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 720,
        "version": 1,
        "rowsets": []
        }
        )DEL");

    auto t721_v1 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 721,
        "version": 1,
        "rowsets": []
        }
        )DEL");

    auto t720_v2 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 720,
        "version": 2,
        "rowsets": [],
        "dcg_meta": {
            "dcgs": [
                {
                    "key": 1,
                    "value": {
                        "column_files": [
                            "0000000000f359e4_33333333-3333-3333-3333-3333333333c1.dat",
                            "0000000000f359e4_33333333-3333-3333-3333-3333333333c2.dat"
                        ],
                        "versions": [
                            2,
                            2
                        ],
                        "shared_files": [
                            true,
                            false
                        ]
                    }
                }
            ]
        }
        }
        )DEL");

    auto t721_v2 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 721,
        "version": 2,
        "rowsets": [],
        "dcg_meta": {
            "dcgs": [
                {
                    "key": 1,
                    "value": {
                        "column_files": [
                            "0000000000f359e4_33333333-3333-3333-3333-3333333333c1.dat",
                            "0000000000f359e4_33333333-3333-3333-3333-3333333333c3.dat"
                        ],
                        "versions": [
                            2,
                            2
                        ],
                        "shared_files": [
                            true,
                            false
                        ]
                    }
                }
            ]
        }
        }
        )DEL");

    std::map<int64_t, TabletMetadataPB> tablet_metas_v1;
    tablet_metas_v1[720] = *t720_v1;
    tablet_metas_v1[721] = *t721_v1;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v1));

    std::map<int64_t, TabletMetadataPB> tablet_metas_v2;
    tablet_metas_v2[720] = *t720_v2;
    tablet_metas_v2[721] = *t721_v2;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v2));

    {
        // delete tablet 720 only, keep shared dcg file because tablet 721 is still alive.
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(720);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        EXPECT_TRUE(file_exist(shared_dcg_file));
        EXPECT_FALSE(file_exist(private_dcg_file_720));
        EXPECT_TRUE(file_exist(private_dcg_file_721));
    }

    {
        // delete both tablets, shared dcg file can be deleted.
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(720);
        request.add_tablet_ids(721);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        EXPECT_FALSE(file_exist(shared_dcg_file));
        EXPECT_FALSE(file_exist(private_dcg_file_720));
        EXPECT_FALSE(file_exist(private_dcg_file_721));
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

TEST_P(LakeVacuumTest, test_datafile_gc_with_bundle_metadata) {
    WritableFileOptions options;
    options.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_ABORT(auto f, fs::new_writable_file(options, join_path(kTestDir, "test_datafile_gc.txt")));
    ASSERT_OK(f->append("111"));
    ASSERT_OK(f->close());

    create_data_file("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat");
    create_data_file("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat");
    create_data_file("0000000000011111_a542395a-bff5-48a7-a3a7-2ed05691b58c.sst");
    create_data_file("0000000000022222_a542395a-bff5-48a7-a3a7-2ed05691b58c.sst");

    TabletSchemaPB schema_pb1;
    {
        schema_pb1.set_id(0);
        schema_pb1.set_num_short_key_columns(1);
        schema_pb1.set_keys_type(DUP_KEYS);
    }

    auto t600_v2 = json_to_pb<TabletMetadataPB>(R"DEL(
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
        )DEL");
    auto t601_v2 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 601,
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
        )DEL");

    t600_v2->mutable_schema()->CopyFrom(schema_pb1);
    t601_v2->mutable_schema()->CopyFrom(schema_pb1);

    std::map<int64_t, TabletMetadataPB> tablet_metas_v2;
    tablet_metas_v2[600] = *t600_v2;
    tablet_metas_v2[601] = *t601_v2;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v2));

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

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_drop_tablet_cache) {
    constexpr int64_t kTabletId = 700;

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 700,
        "version": 2,
        "rowsets": [
            {
                "segments": [
                    "700_seg_c.dat"
                ],
                "segment_size": [300],
                "data_size": 300
            }
        ],
        "delvec_meta": {
            "version_to_file": [
                {
                    "key": 2,
                    "value": {
                        "name": "700_delvec.delvec",
                        "size": 23
                    }
                }
            ]
        },
        "sstable_meta": {
            "sstables": [
                {
                    "filename": "700_sst.sst",
                    "filesize": 333
                }
            ]
        },
        "dcg_meta": {
            "dcgs": [
                {
                    "key": 0,
                    "value": {
                        "column_files": [
                            "700_col_a.cols",
                            "700_col_b.cols"
                        ]
                    }
                }
            ]
        },
        "prev_garbage_version": 0
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 700,
        "version": 3,
        "rowsets": [
            {
                "segments": [
                    "700_seg_a.dat",
                    "700_seg_b.dat"
                ],
                "segment_size": [100, 200],
                "bundle_file_offsets": [0, 1000],
                "data_size": 300
            }
        ],
        "prev_garbage_version": 2
        }
        )DEL")));

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

    ASSERT_OK(drop_tablet_cache(_tablet_mgr.get(), kTabletId, 3));

    std::vector<std::string> expected{
            "700_seg_a.dat", "700_seg_b.dat",  "700_seg_c.dat",  "700_delvec.delvec",
            "700_sst.sst",   "700_col_a.cols", "700_col_b.cols",
    };

    std::sort(dropped.begin(), dropped.end());
    std::sort(expected.begin(), expected.end());
    ASSERT_EQ(expected, dropped);
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
                "segments": [
                    "00000000000a59e4_aaaa1111-1111-1111-1111-111111111111.dat",
                    "00000000000a59e4_bbbb2222-2222-2222-2222-222222222222.dat"
                ],
                "data_size": 4096,
                "segment_metas": [
                    { "vector_index_ids": [100, 200] },
                    { "vector_index_ids": [100] }
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
                "segments": [
                    "00000000000a59e5_cccc3333-3333-3333-3333-333333333333.dat"
                ],
                "data_size": 100,
                "segment_metas": [
                    { "vector_index_ids": [100] }
                ]
            }
        ],
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000a59e4_aaaa1111-1111-1111-1111-111111111111.dat",
                    "00000000000a59e4_bbbb2222-2222-2222-2222-222222222222.dat"
                ],
                "data_size": 4096,
                "segment_metas": [
                    { "vector_index_ids": [100, 200] },
                    { "vector_index_ids": [100] }
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
                "segments": [
                    "00000000000b59e4_dddd4444-4444-4444-4444-444444444444.dat"
                ],
                "data_size": 4096
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
                "segments": [
                    "00000000000b59e5_eeee5555-5555-5555-5555-555555555555.dat"
                ],
                "data_size": 100
            }
        ],
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000b59e4_dddd4444-4444-4444-4444-444444444444.dat"
                ],
                "data_size": 4096
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
                "segments": [
                    "00000000000e59e4_aaaa0001-0001-0001-0001-000000000001.dat",
                    "00000000000e59e4_bbbb0002-0002-0002-0002-000000000002.dat",
                    "00000000000e59e4_cccc0003-0003-0003-0003-000000000003.dat",
                    "00000000000e59e4_dddd0004-0004-0004-0004-000000000004.dat"
                ],
                "data_size": 8192,
                "segment_metas": [
                    { "vector_index_ids": [100] },
                    { "vector_index_ids": [100] },
                    { "vector_index_ids": [100] },
                    { "vector_index_ids": [100] }
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
                "segments": [
                    "00000000000e59e4_aaaa0001-0001-0001-0001-000000000001.dat",
                    "00000000000e59e5_mmmm0005-0005-0005-0005-000000000005.dat",
                    "00000000000e59e4_dddd0004-0004-0004-0004-000000000004.dat"
                ],
                "data_size": 6144,
                "segment_metas": [
                    { "vector_index_ids": [100] },
                    { "vector_index_ids": [100] },
                    { "vector_index_ids": [100] }
                ]
            }
        ],
        "compaction_inputs": [
            {
                "id": 10,
                "segments": [
                    "00000000000e59e4_bbbb0002-0002-0002-0002-000000000002.dat",
                    "00000000000e59e4_cccc0003-0003-0003-0003-000000000003.dat"
                ],
                "data_size": 4096,
                "segment_metas": [
                    { "vector_index_ids": [100] },
                    { "vector_index_ids": [100] }
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
                "segments": [
                    "00000000000c59e4_1111aaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.dat",
                    "00000000000c59e4_2222bbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb.dat"
                ],
                "segment_metas": [
                    { "vector_index_ids": [300] },
                    { "vector_index_ids": [300, 400] }
                ]
            }
        ],
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000c59e3_3333cccc-cccc-cccc-cccc-cccccccccccc.dat"
                ],
                "data_size": 2048,
                "segment_metas": [
                    { "vector_index_ids": [300] }
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
                "segments": [
                    "00000000000d59e4_aaaa1111-1111-1111-1111-111111111111.dat"
                ],
                "segment_metas": [
                    { "vector_index_ids": [500] }
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
                "segments": [
                    "00000000000e59e4_p1111111-1111-1111-1111-111111111111.dat",
                    "00000000000e59e4_p2222222-2222-2222-2222-222222222222.dat"
                ],
                "data_size": 4096,
                "segment_metas": [
                    { "vector_index_ids": [700] }
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
                "segments": [
                    "00000000000e59e5_p3333333-3333-3333-3333-333333333333.dat"
                ],
                "data_size": 200
            }
        ],
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000e59e4_p1111111-1111-1111-1111-111111111111.dat",
                    "00000000000e59e4_p2222222-2222-2222-2222-222222222222.dat"
                ],
                "data_size": 4096,
                "segment_metas": [
                    { "vector_index_ids": [700] }
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

TEST_P(LakeVacuumTest, test_vacuum_bundle_metadata) {
    create_data_file("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat");
    create_data_file("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat");
    create_data_file("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat");
    create_data_file("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58d.dat");

    auto t600_v1 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 600,
        "version": 1,
        "rowsets": []
        }
        )DEL");

    auto t601_v1 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 601,
        "version": 1,
        "rowsets": []
        }
        )DEL");

    auto t600_v2 = json_to_pb<TabletMetadataPB>(R"DEL(
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
        )DEL");

    auto t601_v2 = json_to_pb<TabletMetadataPB>(R"DEL(
            {
            "id": 601,
            "version": 2,
            "rowsets": [
                {
                    "segments": [
                        "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat"
                    ],
                    "data_size": 4096
                }
            ]
            }
            )DEL");

    auto t600_v3 = json_to_pb<TabletMetadataPB>(R"DEL(
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
        )DEL");

    auto t601_v3 = json_to_pb<TabletMetadataPB>(R"DEL(
            {
            "id": 601,
            "version": 3,
            "rowsets": [
                {
                    "segments": [
                        "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat",
                        "00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58d.dat"
                    ],
                    "data_size": 8192
                }
            ]
            }
            )DEL");

    TabletSchemaPB schema_pb1;
    {
        schema_pb1.set_id(0);
        schema_pb1.set_num_short_key_columns(1);
        schema_pb1.set_keys_type(DUP_KEYS);
    }
    // create SharedTabletMetadata
    std::map<int64_t, TabletMetadataPB> tablet_metas_v1;
    t600_v1->mutable_schema()->CopyFrom(schema_pb1);
    tablet_metas_v1[600] = *t600_v1;
    tablet_metas_v1[601] = *t601_v1;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v1));
    std::map<int64_t, TabletMetadataPB> tablet_metas_v2;
    t600_v2->mutable_schema()->CopyFrom(schema_pb1);
    tablet_metas_v2[600] = *t600_v2;
    tablet_metas_v2[601] = *t601_v2;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v2));
    std::map<int64_t, TabletMetadataPB> tablet_metas_v3;
    t600_v3->mutable_schema()->CopyFrom(schema_pb1);
    tablet_metas_v3[600] = *t600_v3;
    tablet_metas_v3[601] = *t601_v3;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v3));
    auto* metacache = _tablet_mgr->metacache();
    metacache->cache_tablet_metadata(_tablet_mgr->tablet_metadata_location(600, 1), t600_v1);
    metacache->cache_tablet_metadata(_tablet_mgr->tablet_metadata_location(601, 1), t601_v1);
    metacache->cache_tablet_metadata(_tablet_mgr->tablet_metadata_location(600, 2), t600_v2);
    metacache->cache_tablet_metadata(_tablet_mgr->tablet_metadata_location(601, 2), t601_v2);
    metacache->cache_tablet_metadata(_tablet_mgr->tablet_metadata_location(600, 3), t600_v3);
    metacache->cache_tablet_metadata(_tablet_mgr->tablet_metadata_location(601, 3), t601_v3);

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
        request.add_tablet_ids(601);
        request.set_min_retain_version(3);
        request.set_grace_timestamp(grace_timestamp);
        request.set_min_active_txn_id(12345);
        request.set_enable_file_bundling(true);
        request.set_enable_shared_file_cleanup(true);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_EQ(2, response.vacuumed_files());
        // The size of deleted metadata files is not counted in vacuumed_file_size.
        EXPECT_EQ(0, response.vacuumed_file_size());

        EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 1)));
        // version 2 is the last version created before "grace_timestamp", should be retained
        EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 2)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 3)));

        EXPECT_TRUE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
        EXPECT_TRUE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat"));
        EXPECT_TRUE(file_exist("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"));
        EXPECT_TRUE(file_exist("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58d.dat"));
    }

    // tablet metadata of version 1, 2 will be deleted.
    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(true);
        request.add_tablet_ids(600);
        request.add_tablet_ids(601);
        request.set_min_retain_version(3);
        // Now version 3 becomes the last version created before grace_timestamp, version 1/2 can be
        // deleted
        request.set_grace_timestamp(grace_timestamp + 1);
        request.set_min_active_txn_id(12345);
        request.set_enable_file_bundling(true);
        request.set_enable_shared_file_cleanup(true);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_EQ(4, response.vacuumed_files());
        EXPECT_EQ(0, response.vacuumed_file_size());

        EXPECT_FALSE(file_exist(tablet_metadata_filename(0, 1)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(0, 2)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 3)));

        EXPECT_TRUE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
        EXPECT_TRUE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat"));
        EXPECT_TRUE(file_exist("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"));
        EXPECT_TRUE(file_exist("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58d.dat"));
    }

    SyncPoint::GetInstance()->ClearCallBack("collect_files_to_vacuum:get_file_modified_time");
    SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(LakeVacuumTest, test_vacuum_shared_data_files) {
    create_data_file("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat");
    create_data_file("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat");
    create_data_file("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat");
    create_data_file("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58d.dat");
    TabletSchemaPB schema_pb1;
    {
        schema_pb1.set_id(0);
        schema_pb1.set_num_short_key_columns(1);
        schema_pb1.set_keys_type(DUP_KEYS);
    }

    auto t600_v1 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 600,
        "version": 1,
        "rowsets": []
        }
        )DEL");

    auto t601_v1 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 601,
        "version": 1,
        "rowsets": []
        }
        )DEL");

    auto t600_v2 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 600,
        "version": 2,
        "rowsets": [
            {
                "segments": [
                    "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                ],
                "data_size": 4096,
                "bundle_file_offsets": [
                    0
                ]
            }
        ]
        }
        )DEL");

    auto t601_v2 = json_to_pb<TabletMetadataPB>(R"DEL(
            {
            "id": 601,
            "version": 2,
            "rowsets": [
                {
                    "segments": [
                        "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                    ],
                    "data_size": 4096,
                    "bundle_file_offsets": [
                        4096
                    ]
                }
            ]
            }
            )DEL");

    auto t600_v3 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 600,
        "version": 3,
        "rowsets": [
            {
                "segments": [
                    "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                ],
                "data_size": 4096,
                "bundle_file_offsets": [
                    0
                ]
            },
            {
                "segments": [
                    "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat"
                ],
                "data_size": 4096,
                "bundle_file_offsets": [
                    0
                ]
            }
        ]
        }
        )DEL");

    auto t601_v3 = json_to_pb<TabletMetadataPB>(R"DEL(
            {
            "id": 601,
            "version": 3,
            "rowsets": [
                {
                    "segments": [
                        "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                    ],
                    "data_size": 4096,
                    "bundle_file_offsets": [
                        4096
                    ]
                },
                {
                    "segments": [
                        "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat"
                    ],
                    "data_size": 4096,
                    "bundle_file_offsets": [
                        4096
                    ]
                }
            ]
            }
            )DEL");
    // after compaction
    auto t600_v4 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 600,
        "version": 4,
        "rowsets": [
            {
                "segments": [
                    "00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"
                ],
                "data_size": 8192
            }
        ],
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                ],
                "data_size": 4096,
                "bundle_file_offsets": [
                    0
                ]
            },
            {
                "segments": [
                    "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat"
                ],
                "data_size": 4096,
                "bundle_file_offsets": [
                    0
                ]
            }
        ],
        "prev_garbage_version": 3
        }
        )DEL");

    auto t601_v4 = json_to_pb<TabletMetadataPB>(R"DEL(
            {
            "id": 601,
            "version": 4,
            "rowsets": [
                {
                    "segments": [
                        "00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58d.dat"
                    ],
                    "data_size": 8192
                }
            ],
            "compaction_inputs": [
                {
                    "segments": [
                        "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                    ],
                    "data_size": 4096,
                    "bundle_file_offsets": [
                        4096
                    ]
                },
                {
                    "segments": [
                        "00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat"
                    ],
                    "data_size": 4096,
                    "bundle_file_offsets": [
                        4096
                    ]
                }
            ]
            }
            )DEL");

    // create SharedTabletMetadata
    std::map<int64_t, TabletMetadataPB> tablet_metas_v1;
    t600_v1->mutable_schema()->CopyFrom(schema_pb1);
    tablet_metas_v1[600] = *t600_v1;
    tablet_metas_v1[601] = *t601_v1;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v1));
    std::map<int64_t, TabletMetadataPB> tablet_metas_v2;
    t600_v2->mutable_schema()->CopyFrom(schema_pb1);
    tablet_metas_v2[600] = *t600_v2;
    tablet_metas_v2[601] = *t601_v2;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v2));
    std::map<int64_t, TabletMetadataPB> tablet_metas_v3;
    t600_v3->mutable_schema()->CopyFrom(schema_pb1);
    tablet_metas_v3[600] = *t600_v3;
    tablet_metas_v3[601] = *t601_v3;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v3));
    auto* metacache = _tablet_mgr->metacache();
    metacache->cache_tablet_metadata(_tablet_mgr->tablet_metadata_location(600, 1), t600_v1);
    metacache->cache_tablet_metadata(_tablet_mgr->tablet_metadata_location(601, 1), t601_v1);
    metacache->cache_tablet_metadata(_tablet_mgr->tablet_metadata_location(600, 2), t600_v2);
    metacache->cache_tablet_metadata(_tablet_mgr->tablet_metadata_location(601, 2), t601_v2);
    metacache->cache_tablet_metadata(_tablet_mgr->tablet_metadata_location(600, 3), t600_v3);
    metacache->cache_tablet_metadata(_tablet_mgr->tablet_metadata_location(601, 3), t601_v3);

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
        request.add_tablet_ids(601);
        request.set_min_retain_version(3);
        request.set_grace_timestamp(grace_timestamp);
        request.set_min_active_txn_id(12345);
        request.set_enable_file_bundling(true);
        request.set_enable_shared_file_cleanup(true);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_EQ(2, response.vacuumed_files());
        // The size of deleted metadata files is not counted in vacuumed_file_size.
        EXPECT_EQ(0, response.vacuumed_file_size());

        EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 1)));
        // version 2 is the last version created before "grace_timestamp", should be retained
        EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 2)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 3)));

        EXPECT_TRUE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
        EXPECT_TRUE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat"));
    }

    // tablet metadata of version 1, 2 will be deleted.
    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(true);
        request.add_tablet_ids(600);
        request.add_tablet_ids(601);
        request.set_min_retain_version(3);
        // Now version 3 becomes the last version created before grace_timestamp, version 1/2 can be
        // deleted
        request.set_grace_timestamp(grace_timestamp + 1);
        request.set_min_active_txn_id(12345);
        request.set_enable_file_bundling(true);
        request.set_enable_shared_file_cleanup(true);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_EQ(4, response.vacuumed_files());
        EXPECT_EQ(0, response.vacuumed_file_size());

        EXPECT_FALSE(file_exist(tablet_metadata_filename(0, 1)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(0, 2)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 3)));

        EXPECT_TRUE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
        EXPECT_TRUE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat"));
    }

    // after compaction
    std::map<int64_t, TabletMetadataPB> tablet_metas_v4;
    t600_v4->mutable_schema()->CopyFrom(schema_pb1);
    tablet_metas_v4[600] = *t600_v4;
    tablet_metas_v4[601] = *t601_v4;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v4));

    // shared cleanup is false, shared files will not be deleted.
    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(false);
        request.add_tablet_ids(600);
        request.add_tablet_ids(601);
        TabletInfoPB tablet_info1;
        tablet_info1.set_tablet_id(600);
        tablet_info1.set_min_version(3);
        TabletInfoPB tablet_info2;
        tablet_info2.set_tablet_id(601);
        tablet_info2.set_min_version(3);
        request.add_tablet_infos()->CopyFrom(tablet_info1);
        request.add_tablet_infos()->CopyFrom(tablet_info2);
        request.set_min_retain_version(4);
        request.set_grace_timestamp(grace_timestamp + 1);
        request.set_min_active_txn_id(12345);
        // If shared cleanup is false, we need to clear the shared_file_deleter.
        request.set_enable_shared_file_cleanup(false);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        EXPECT_TRUE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
        EXPECT_TRUE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat"));
        EXPECT_TRUE(file_exist("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"));
        EXPECT_TRUE(file_exist("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58d.dat"));
    }

    // shared files will be deleted.
    {
        VacuumRequest request;
        VacuumResponse response;
        request.set_delete_txn_log(true);
        request.add_tablet_ids(600);
        request.add_tablet_ids(601);
        TabletInfoPB tablet_info1;
        tablet_info1.set_tablet_id(600);
        tablet_info1.set_min_version(3);
        TabletInfoPB tablet_info2;
        tablet_info2.set_tablet_id(601);
        tablet_info2.set_min_version(3);
        request.add_tablet_infos()->CopyFrom(tablet_info1);
        request.add_tablet_infos()->CopyFrom(tablet_info2);
        request.set_min_retain_version(4);
        request.set_grace_timestamp(grace_timestamp + 1);
        request.set_min_active_txn_id(12345);
        request.set_enable_file_bundling(true);
        request.set_enable_shared_file_cleanup(true);
        vacuum(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        EXPECT_EQ(7, response.vacuumed_files());
        EXPECT_EQ(16384, response.vacuumed_file_size());

        EXPECT_FALSE(file_exist(tablet_metadata_filename(0, 1)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(0, 2)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(0, 3)));
        EXPECT_TRUE(file_exist(tablet_metadata_filename(0, 4)));

        EXPECT_FALSE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"));
        EXPECT_FALSE(file_exist("00000000000259e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1e.dat"));
        EXPECT_TRUE(file_exist("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat"));
        EXPECT_TRUE(file_exist("00000000000259e4_a542395a-bff5-48a7-a3a7-2ed05691b58d.dat"));
    }

    SyncPoint::GetInstance()->ClearCallBack("collect_files_to_vacuum:get_file_modified_time");
    SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(LakeVacuumTest, test_vacuum_shared_file_cleanup) {
    const std::string compaction_file = "00000000000159e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat";
    const std::string orphan_file = "00000000000159e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat";
    create_data_file(compaction_file);
    create_data_file(orphan_file);

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 700,
        "version": 1
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 700,
        "version": 2,
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000159e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                ],
                "data_size": 4096,
                "shared_segments": [ true ]
            }
        ],
        "orphan_files": [
            {
                "name": "00000000000159e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat",
                "size": 4096,
                "shared": true
            }
        ],
        "prev_garbage_version": 1
        }
        )DEL")));

    VacuumRequest request;
    VacuumResponse response;
    TabletInfoPB tablet_info;
    tablet_info.set_tablet_id(700);
    tablet_info.set_min_version(1);
    request.add_tablet_infos()->CopyFrom(tablet_info);
    request.set_min_retain_version(2);
    request.set_grace_timestamp(::time(nullptr) + 10);
    request.set_min_active_txn_id(1000000);
    request.set_enable_shared_file_cleanup(true);
    request.set_delete_txn_log(false);

    vacuum(_tablet_mgr.get(), request, &response);
    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
    EXPECT_FALSE(file_exist(compaction_file));
    EXPECT_FALSE(file_exist(orphan_file));
}

TEST_P(LakeVacuumTest, test_vacuum_shared_file_cleanup_without_file_bundling) {
    const std::string shared_file = "00000000000159e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2f1d.dat";
    create_data_file(shared_file);

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 705,
        "version": 1
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 705,
        "version": 2,
        "prev_garbage_version": 1
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 705,
        "version": 3,
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000159e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2f1d.dat"
                ],
                "data_size": 4096,
                "shared_segments": [ true ]
            }
        ],
        "prev_garbage_version": 2
        }
        )DEL")));

    VacuumRequest request;
    VacuumResponse response;
    TabletInfoPB tablet_info;
    tablet_info.set_tablet_id(705);
    tablet_info.set_min_version(1);
    request.add_tablet_infos()->CopyFrom(tablet_info);
    request.set_min_retain_version(3);
    request.set_grace_timestamp(::time(nullptr) + 10);
    request.set_min_active_txn_id(1000000);
    request.set_enable_file_bundling(false);
    request.set_enable_shared_file_cleanup(true);
    request.set_delete_txn_log(false);

    vacuum(_tablet_mgr.get(), request, &response);
    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
    EXPECT_FALSE(file_exist(tablet_metadata_filename(705, 1)));
    EXPECT_FALSE(file_exist(tablet_metadata_filename(705, 2)));
    EXPECT_TRUE(file_exist(tablet_metadata_filename(705, 3)));
    EXPECT_FALSE(file_exist(shared_file));
}

TEST_P(LakeVacuumTest, test_vacuum_shared_file_cleanup_without_bundling_flag) {
    const std::string shared_file = "00000000000159e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2f2d.dat";
    create_data_file(shared_file);

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 706,
        "version": 1
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 706,
        "version": 2,
        "prev_garbage_version": 1
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 706,
        "version": 3,
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000159e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2f2d.dat"
                ],
                "data_size": 4096,
                "shared_segments": [ true ]
            }
        ],
        "prev_garbage_version": 2
        }
        )DEL")));

    VacuumRequest request;
    VacuumResponse response;
    TabletInfoPB tablet_info;
    tablet_info.set_tablet_id(706);
    tablet_info.set_min_version(1);
    request.add_tablet_infos()->CopyFrom(tablet_info);
    request.set_min_retain_version(3);
    request.set_grace_timestamp(::time(nullptr) + 10);
    request.set_min_active_txn_id(1000000);
    request.set_enable_shared_file_cleanup(true);
    request.set_delete_txn_log(false);

    vacuum(_tablet_mgr.get(), request, &response);
    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
    EXPECT_FALSE(file_exist(tablet_metadata_filename(706, 1)));
    EXPECT_FALSE(file_exist(tablet_metadata_filename(706, 2)));
    EXPECT_TRUE(file_exist(tablet_metadata_filename(706, 3)));
    EXPECT_FALSE(file_exist(shared_file));
}

TEST_P(LakeVacuumTest, test_vacuum_shared_file_cleanup_keep_referenced) {
    const std::string shared_file = "00000000000159e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat";
    create_data_file(shared_file);

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 710,
        "version": 1
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 710,
        "version": 2,
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000159e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                ],
                "data_size": 4096,
                "shared_segments": [ true ]
            }
        ],
        "prev_garbage_version": 1
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 711,
        "version": 1
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 711,
        "version": 2,
        "rowsets": [
            {
                "segments": [
                    "00000000000159e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                ],
                "data_size": 4096,
                "shared_segments": [ true ]
            }
        ]
        }
        )DEL")));

    VacuumRequest request;
    VacuumResponse response;
    TabletInfoPB tablet_info_710;
    tablet_info_710.set_tablet_id(710);
    tablet_info_710.set_min_version(1);
    request.add_tablet_infos()->CopyFrom(tablet_info_710);
    TabletInfoPB tablet_info_711;
    tablet_info_711.set_tablet_id(711);
    tablet_info_711.set_min_version(1);
    request.add_tablet_infos()->CopyFrom(tablet_info_711);
    request.set_min_retain_version(2);
    request.set_grace_timestamp(::time(nullptr) + 10);
    request.set_min_active_txn_id(1000000);
    request.set_enable_shared_file_cleanup(true);
    request.set_delete_txn_log(false);

    vacuum(_tablet_mgr.get(), request, &response);
    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
    EXPECT_TRUE(file_exist(shared_file));
}

TEST_P(LakeVacuumTest, test_vacuum_shared_file_cleanup_reference_scan) {
    const std::string shared_segment = "00000000000000a1_44444444-4444-4444-4444-444444444444.dat";
    const std::string shared_del = "00000000000000a2_55555555-5555-5555-5555-555555555555.del";
    create_data_file(shared_segment);
    create_data_file(shared_del);

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 720,
        "version": 1
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 720,
        "version": 2,
        "rowsets": [
            {
                "segments": [
                    "00000000000000a3_66666666-6666-6666-6666-666666666666.dat"
                ],
                "data_size": 4096,
                "shared_segments": [ true ],
                "del_files": [
                    {
                        "name": "00000000000000a4_77777777-7777-7777-7777-777777777777.del",
                        "shared": true
                    }
                ]
            }
        ],
        "delvec_meta": {
            "version_to_file": [
                {
                    "key": 1,
                    "value": {
                        "name": "00000000000000a5_88888888-8888-8888-8888-888888888888.delvec",
                        "shared": true
                    }
                }
            ]
        },
        "dcg_meta": {
            "dcgs": [
                {
                    "key": 1,
                    "value": {
                        "column_files": [ "00000000000000a6_99999999-9999-9999-9999-999999999999.cols" ],
                        "shared_files": [ true ]
                    }
                }
            ]
        },
        "sstable_meta": {
            "sstables": [
                {
                    "filename": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.sst",
                    "shared": true
                }
            ]
        },
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000000a1_44444444-4444-4444-4444-444444444444.dat"
                ],
                "data_size": 4096,
                "shared_segments": [ true ],
                "del_files": [
                    {
                        "name": "00000000000000a2_55555555-5555-5555-5555-555555555555.del",
                        "shared": true
                    }
                ]
            }
        ],
        "prev_garbage_version": 1
        }
        )DEL")));

    VacuumRequest request;
    VacuumResponse response;
    TabletInfoPB tablet_info;
    tablet_info.set_tablet_id(720);
    tablet_info.set_min_version(1);
    request.add_tablet_infos()->CopyFrom(tablet_info);
    request.set_min_retain_version(2);
    request.set_grace_timestamp(::time(nullptr) + 10);
    request.set_min_active_txn_id(1000000);
    request.set_enable_shared_file_cleanup(true);
    request.set_delete_txn_log(false);

    vacuum(_tablet_mgr.get(), request, &response);
    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
    EXPECT_FALSE(file_exist(shared_segment));
    EXPECT_FALSE(file_exist(shared_del));
}

TEST_P(LakeVacuumTest, test_vacuum_shared_file_cleanup_skip_bundle_range) {
    const std::string shared_segment = "00000000000000b1_66666666-6666-6666-6666-666666666666.dat";
    create_data_file(shared_segment);

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 730,
        "version": 3,
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000000b1_66666666-6666-6666-6666-666666666666.dat"
                ],
                "data_size": 4096,
                "shared_segments": [ true ]
            }
        ],
        "prev_garbage_version": 1
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 731,
        "version": 1
        }
        )DEL")));
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 731,
        "version": 3,
        "rowsets": []
        }
        )DEL")));

    TabletSchemaPB schema_pb;
    schema_pb.set_id(0);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_keys_type(DUP_KEYS);

    auto t730_v2 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 730,
        "version": 2,
        "rowsets": []
        }
        )DEL");
    t730_v2->mutable_schema()->CopyFrom(schema_pb);
    std::map<int64_t, TabletMetadataPB> tablet_metas_v2;
    tablet_metas_v2[730] = *t730_v2;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v2));

    VacuumRequest request;
    VacuumResponse response;
    TabletInfoPB tablet_info_730;
    tablet_info_730.set_tablet_id(730);
    tablet_info_730.set_min_version(1);
    request.add_tablet_infos()->CopyFrom(tablet_info_730);
    TabletInfoPB tablet_info_731;
    tablet_info_731.set_tablet_id(731);
    tablet_info_731.set_min_version(2);
    request.add_tablet_infos()->CopyFrom(tablet_info_731);
    request.set_min_retain_version(3);
    request.set_grace_timestamp(::time(nullptr) + 10);
    request.set_min_active_txn_id(1000000);
    request.set_enable_shared_file_cleanup(true);
    request.set_delete_txn_log(false);

    vacuum(_tablet_mgr.get(), request, &response);
    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
    EXPECT_FALSE(file_exist(shared_segment));
}

TEST_P(LakeVacuumTest, test_vacuum_shared_file_cleanup_bundle_meta_read_and_missing_meta) {
    const std::string shared_segment = "00000000000000c1_77777777-7777-7777-7777-777777777777.dat";
    create_data_file(shared_segment);

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 740,
        "version": 2,
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000000c1_77777777-7777-7777-7777-777777777777.dat"
                ],
                "data_size": 4096,
                "shared_segments": [ true ]
            }
        ],
        "prev_garbage_version": 1
        }
        )DEL")));

    TabletSchemaPB schema_pb;
    schema_pb.set_id(0);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_keys_type(DUP_KEYS);
    auto t740_v2 = json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 740,
        "version": 2,
        "rowsets": []
        }
        )DEL");
    t740_v2->mutable_schema()->CopyFrom(schema_pb);
    std::map<int64_t, TabletMetadataPB> tablet_metas_v2;
    tablet_metas_v2[740] = *t740_v2;
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas_v2));

    std::string meta_dir = join_path(kTestDir, kMetadataDirectoryName);
    std::string broken_meta = join_path(meta_dir, tablet_metadata_filename(0, 0xabc402));
    std::string broken_single_meta = join_path(meta_dir, tablet_metadata_filename(742, 0xabc403));
    DeferOp cleanup_symlink_files([&]() {
        (void)FileSystem::Default()->delete_file(broken_meta);
        (void)FileSystem::Default()->delete_file(broken_single_meta);
    });
    auto delete_status = FileSystem::Default()->delete_file(broken_meta);
    if (!delete_status.ok() && !delete_status.is_not_found()) {
        ASSERT_OK(delete_status);
    }
    ASSERT_EQ(0, symlink("missing_target", broken_meta.c_str()));
    delete_status = FileSystem::Default()->delete_file(broken_single_meta);
    if (!delete_status.ok() && !delete_status.is_not_found()) {
        ASSERT_OK(delete_status);
    }
    ASSERT_EQ(0, symlink("missing_target", broken_single_meta.c_str()));

    VacuumRequest request;
    VacuumResponse response;
    TabletInfoPB tablet_info_740;
    tablet_info_740.set_tablet_id(740);
    tablet_info_740.set_min_version(1);
    request.add_tablet_infos()->CopyFrom(tablet_info_740);
    request.set_min_retain_version(2);
    request.set_grace_timestamp(::time(nullptr) + 10);
    request.set_min_active_txn_id(1000000);
    request.set_enable_shared_file_cleanup(true);
    request.set_delete_txn_log(false);

    vacuum(_tablet_mgr.get(), request, &response);
    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
    EXPECT_FALSE(file_exist(shared_segment));
}

TEST_P(LakeVacuumTest, test_vacuum_shared_file_cleanup_scan_error) {
    const std::string shared_segment = "00000000000000d1_88888888-8888-8888-8888-888888888888.dat";
    create_data_file(shared_segment);

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 750,
        "version": 2,
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000000d1_88888888-8888-8888-8888-888888888888.dat"
                ],
                "data_size": 4096,
                "shared_segments": [ true ]
            }
        ],
        "prev_garbage_version": 1
        }
        )DEL")));

    std::string meta_dir = join_path(kTestDir, kMetadataDirectoryName);
    std::string corrupt_bundle_meta = join_path(meta_dir, tablet_metadata_filename(0, 2));
    ASSIGN_OR_ABORT(auto f, FileSystem::Default()->new_writable_file(corrupt_bundle_meta));
    ASSERT_OK(f->append("corrupted"));
    ASSERT_OK(f->close());

    VacuumRequest request;
    VacuumResponse response;
    TabletInfoPB tablet_info;
    tablet_info.set_tablet_id(750);
    tablet_info.set_min_version(1);
    request.add_tablet_infos()->CopyFrom(tablet_info);
    request.set_min_retain_version(2);
    request.set_grace_timestamp(::time(nullptr) + 10);
    request.set_min_active_txn_id(1000000);
    request.set_enable_shared_file_cleanup(true);
    request.set_delete_txn_log(false);

    vacuum(_tablet_mgr.get(), request, &response);
    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
    EXPECT_FALSE(file_exist(shared_segment));
}

TEST_P(LakeVacuumTest, test_vacuum_shared_file_cleanup_skip_new_txn) {
    const std::string shared_segment = "00000000000000f1_99999999-9999-9999-9999-999999999999.dat";
    create_data_file(shared_segment);

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 760,
        "version": 2,
        "compaction_inputs": [
            {
                "segments": [
                    "00000000000000f1_99999999-9999-9999-9999-999999999999.dat"
                ],
                "data_size": 4096,
                "shared_segments": [ true ]
            }
        ],
        "prev_garbage_version": 1
        }
        )DEL")));

    VacuumRequest request;
    VacuumResponse response;
    TabletInfoPB tablet_info;
    tablet_info.set_tablet_id(760);
    tablet_info.set_min_version(1);
    request.add_tablet_infos()->CopyFrom(tablet_info);
    request.set_min_retain_version(2);
    request.set_grace_timestamp(::time(nullptr) + 10);
    request.set_min_active_txn_id(1);
    request.set_enable_shared_file_cleanup(true);
    request.set_delete_txn_log(false);

    vacuum(_tablet_mgr.get(), request, &response);
    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
    EXPECT_FALSE(file_exist(shared_segment));
}

TEST_P(LakeVacuumTest, test_garbage_file_check) {
    create_data_file("00000000000359e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat");
    create_data_file("00000000000359e4_a542395a-bff5-48a7-a3a7-2ed05691b58c.dat");

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 800,
        "version": 1
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 800,
        "version": 2,
        "rowsets": [
            {
                "segments": [
                    "00000000000359e4_27dc159f-6bfc-4a3a-9d9c-c97c10bb2e1d.dat"
                ],
                "data_size": 4096
            }
        ]
        }
        )DEL")));

    auto res = garbage_file_check(kTestDir);
    ASSERT_TRUE(res.ok()) << res.status();
    EXPECT_EQ(1, res.value());
}

// Test for the fix that skips deleting txnlog files for tablets being deleted
TEST_P(LakeVacuumTest, test_delete_tablets_skip_txnlog_files_for_deleted_tablets) {
    // Create data files referenced by different tablets
    create_data_file("00000000001359e4_tablet1_file.dat");
    create_data_file("00000000002359e4_tablet2_file.dat");
    create_data_file("00000000003359e4_tablet3_file.dat");

    // Create combined txn log containing multiple tablets
    ASSERT_OK(_tablet_mgr->put_combined_txn_log(*json_to_pb<CombinedTxnLogPB>(R"DEL(
        {
            "txn_logs": [
                {
                    "tablet_id": 1000,
                    "txn_id": 5000,
                    "partition_id": 111,
                    "op_write": {
                        "rowset": {
                            "segments": ["00000000001359e4_tablet1_file.dat"]
                        }
                    }
                },
                {
                    "tablet_id": 1001,
                    "txn_id": 5000,
                    "partition_id": 111,
                    "op_write": {
                        "rowset": {
                            "segments": ["00000000002359e4_tablet2_file.dat"]
                        }
                    }
                },
                {
                    "tablet_id": 1002,
                    "txn_id": 5000,
                    "partition_id": 111,
                    "op_write": {
                        "rowset": {
                            "segments": ["00000000003359e4_tablet3_file.dat"]
                        }
                    }
                }
            ]
        }
        )DEL")));

    // Verify combined txn log and all data files exist
    EXPECT_TRUE(file_exist(combined_txn_log_filename(5000)));
    EXPECT_TRUE(file_exist("00000000001359e4_tablet1_file.dat"));
    EXPECT_TRUE(file_exist("00000000002359e4_tablet2_file.dat"));
    EXPECT_TRUE(file_exist("00000000003359e4_tablet3_file.dat"));

    {
        // Delete only tablets 1000 and 1001, leaving 1002 alive
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(1000);
        request.add_tablet_ids(1001);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        // Combined txn log should still exist because tablet 1002 is alive
        EXPECT_TRUE(file_exist(combined_txn_log_filename(5000)));

        // Files for deleted tablets (1000, 1001) should be deleted
        // But file for alive tablet (1002) should be preserved
        // Note: The fix ensures that delete_files_under_txnlog is NOT called for tablets being deleted
        // So files for tablets 1000 and 1001 should actually be preserved
        // because they are in the deletion list
        EXPECT_FALSE(file_exist("00000000001359e4_tablet1_file.dat"));
        EXPECT_FALSE(file_exist("00000000002359e4_tablet2_file.dat"));
        EXPECT_TRUE(file_exist("00000000003359e4_tablet3_file.dat"));
    }

    {
        // Now delete the remaining tablet 1002
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(1000);
        request.add_tablet_ids(1001);
        request.add_tablet_ids(1002);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        // Now combined txn log should be deleted since all tablets are deleted
        EXPECT_FALSE(file_exist(combined_txn_log_filename(5000)));

        // All data files should be deleted now
        EXPECT_FALSE(file_exist("00000000001359e4_tablet1_file.dat"));
        EXPECT_FALSE(file_exist("00000000002359e4_tablet2_file.dat"));
        EXPECT_FALSE(file_exist("00000000003359e4_tablet3_file.dat"));
    }
}

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_delete_range_distribution_tablets_skip_metadata_data_files) {
    // Simulate deleting old range distribution tablets after tablet split.
    // The latest metadata contains data files (segments, delvecs, sstables, del files, dcg files)
    // that may be shared with new split tablets. All data files should be retained for range
    // distribution tablets; they will be cleaned up by new tablets' regular vacuum.
    const std::string segment1 = "0000000000f659e4_66666666-6666-6666-6666-6666666666f1.dat";
    const std::string segment2 = "0000000000f759e4_77777777-7777-7777-7777-7777777777g1.dat";
    const std::string delvec1 = "0000000000f659e4_66666666-6666-6666-6666-6666666666f2.delvec";
    const std::string sstable1 = "0000000000f659e4_66666666-6666-6666-6666-6666666666f3.sst";
    const std::string del_file1 = "0000000000f659e4_66666666-6666-6666-6666-6666666666f4.del";
    const std::string del_file2 = "0000000000f759e4_77777777-7777-7777-7777-7777777777g2.del";
    create_data_file(segment1);
    create_data_file(segment2);
    create_data_file(delvec1);
    create_data_file(sstable1);
    create_data_file(del_file1);
    create_data_file(del_file2);

    // Txn log: file is NOT marked as shared (written before split).
    ASSERT_OK(_tablet_mgr->put_txn_log(*json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 5000,
            "txn_id": 9900,
            "partition_id": 111,
            "op_write": {
                "rowset": {
                    "segments": [
                        "0000000000f659e4_66666666-6666-6666-6666-6666666666f1.dat",
                        "0000000000f759e4_77777777-7777-7777-7777-7777777777g1.dat"
                    ]
                },
                "dels": [
                    "0000000000f659e4_66666666-6666-6666-6666-6666666666f4.del",
                    "0000000000f759e4_77777777-7777-7777-7777-7777777777g2.del"
                ]
            }
        }
        )DEL")));

    // Metadata v2: range distribution tablet with data files.
    // Some files are marked shared, some are not. After split, ALL data files should be
    // retained regardless of shared flag.
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 5000,
        "version": 2,
        "range": {
            "lower_bound_included": true,
            "upper_bound_included": false
        },
        "rowsets": [
            {
                "segments": [
                    "0000000000f659e4_66666666-6666-6666-6666-6666666666f1.dat",
                    "0000000000f759e4_77777777-7777-7777-7777-7777777777g1.dat"
                ],
                "data_size": 4096,
                "shared_segments": [true, false],
                "del_files": [
                    {
                        "name": "0000000000f659e4_66666666-6666-6666-6666-6666666666f4.del",
                        "shared": true
                    },
                    {
                        "name": "0000000000f759e4_77777777-7777-7777-7777-7777777777g2.del",
                        "shared": false
                    }
                ]
            }
        ],
        "delvec_meta": {
            "version_to_file": [
                {
                    "key": 2,
                    "value": {
                        "name": "0000000000f659e4_66666666-6666-6666-6666-6666666666f2.delvec",
                        "size": 32,
                        "shared": true
                    }
                }
            ]
        },
        "sstable_meta": {
            "sstables": [
                {
                    "filename": "0000000000f659e4_66666666-6666-6666-6666-6666666666f3.sst",
                    "shared": true
                }
            ]
        }
        }
        )DEL")));

    {
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(5000);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        // All data files should be retained for range distribution tablets.
        // Both shared and private files are kept because after split, even "private"
        // files in pre-split metadata may be referenced by new tablets.
        EXPECT_TRUE(file_exist(segment1));
        EXPECT_TRUE(file_exist(segment2));
        EXPECT_TRUE(file_exist(delvec1));
        EXPECT_TRUE(file_exist(sstable1));
        EXPECT_TRUE(file_exist(del_file1));
        EXPECT_TRUE(file_exist(del_file2));

        // Txn log and metadata files should be deleted.
        EXPECT_FALSE(file_exist(txn_log_filename(5000, 9900)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(5000, 2)));
    }
}

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_delete_range_distribution_tablets_skip_txnlog_data_files) {
    // Simulate deleting old range distribution tablets after tablet split.
    // Txn logs of the old tablet reference data files that have been applied to new split tablets,
    // so those data files must NOT be deleted.
    const std::string segment1 = "00000000001259e4_aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.dat";
    const std::string del_file1 = "00000000001259e4_bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb.del";
    const std::string compaction_segment = "00000000002259e4_cccccccc-cccc-cccc-cccc-cccccccccccc.dat";
    const std::string schema_change_segment = "00000000003259e4_dddddddd-dddd-dddd-dddd-dddddddddddd.dat";
    create_data_file(segment1);
    create_data_file(del_file1);
    create_data_file(compaction_segment);
    create_data_file(schema_change_segment);

    // Tablet metadata with range field (range distribution tablet)
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 6000,
        "version": 2,
        "range": {
            "lower_bound_included": true,
            "upper_bound_included": false
        }
        }
        )DEL")));

    // Txn log with op_write
    ASSERT_OK(_tablet_mgr->put_txn_log(json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 6000,
            "txn_id": 7000,
            "op_write": {
                "rowset": {
                    "segments": ["00000000001259e4_aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.dat"]
                },
                "dels": [
                    "00000000001259e4_bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb.del"
                ]
            }
        }
        )DEL")));

    // Txn log with op_compaction
    ASSERT_OK(_tablet_mgr->put_txn_log(json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 6000,
            "txn_id": 8000,
            "op_compaction": {
                "output_rowset": {
                    "segments": ["00000000002259e4_cccccccc-cccc-cccc-cccc-cccccccccccc.dat"]
                }
            }
        }
        )DEL")));

    // Txn log with op_schema_change
    ASSERT_OK(_tablet_mgr->put_txn_log(json_to_pb<TxnLogPB>(R"DEL(
        {
            "tablet_id": 6000,
            "txn_id": 9000,
            "op_schema_change": {
                "rowsets": [
                    {
                        "segments": ["00000000003259e4_dddddddd-dddd-dddd-dddd-dddddddddddd.dat"]
                    }
                ]
            }
        }
        )DEL")));

    {
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(6000);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        // Data files referenced by txn logs should be retained (not deleted).
        EXPECT_TRUE(file_exist(segment1));
        EXPECT_TRUE(file_exist(del_file1));
        EXPECT_TRUE(file_exist(compaction_segment));
        EXPECT_TRUE(file_exist(schema_change_segment));

        // Txn log files themselves should be deleted.
        EXPECT_FALSE(file_exist(txn_log_filename(6000, 7000)));
        EXPECT_FALSE(file_exist(txn_log_filename(6000, 8000)));
        EXPECT_FALSE(file_exist(txn_log_filename(6000, 9000)));

        // Metadata file should be deleted.
        EXPECT_FALSE(file_exist(tablet_metadata_filename(6000, 2)));
    }
}

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_delete_range_distribution_tablets_skip_combined_txnlog_data_files) {
    // Simulate deleting range distribution tablets with combined txn logs after tablet split.
    const std::string segment1 = "00000000001259e4_eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee.dat";
    create_data_file(segment1);

    // Tablet metadata with range field for both tablets
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 6100,
        "version": 2,
        "range": {
            "lower_bound_included": true,
            "upper_bound_included": false
        }
        }
        )DEL")));

    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 6101,
        "version": 2,
        "range": {
            "lower_bound_included": true,
            "upper_bound_included": false
        }
        }
        )DEL")));

    // Combined txn log referencing data file from both tablets
    ASSERT_OK(_tablet_mgr->put_combined_txn_log(*json_to_pb<CombinedTxnLogPB>(R"DEL(
        {
            "txn_logs": [
               {
                    "tablet_id": 6100,
                    "txn_id": 7100,
                    "partition_id": 111,
                    "op_write": {
                        "rowset": {
                            "segments": ["00000000001259e4_eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee.dat"]
                        }
                    }
                },
                {
                    "tablet_id": 6101,
                    "txn_id": 7100,
                    "partition_id": 111,
                    "op_write": {
                        "rowset": {
                            "segments": ["00000000001259e4_eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee.dat"]
                        }
                    }
                }
            ]
        }
        )DEL")));

    {
        // Delete both tablets
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(6100);
        request.add_tablet_ids(6101);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        // Data file should be retained.
        EXPECT_TRUE(file_exist(segment1));

        // Combined txn log file should be deleted.
        EXPECT_FALSE(file_exist(combined_txn_log_filename(7100)));

        // Metadata files should be deleted.
        EXPECT_FALSE(file_exist(tablet_metadata_filename(6100, 2)));
        EXPECT_FALSE(file_exist(tablet_metadata_filename(6101, 2)));
    }
}

// NOLINTNEXTLINE
TEST_P(LakeVacuumTest, test_delete_range_distribution_tablets_skip_shared_orphan_files) {
    // Verify that shared files in orphan_files and compaction_inputs are NOT deleted
    // for range distribution tablets, even when can_bundle_meta_file_to_be_deleted
    // would allow it. This protects files that may still be referenced by new split tablets.
    const std::string shared_orphan = "0000000000f859e4_88888888-8888-8888-8888-8888888888h1.dat";
    const std::string private_orphan = "0000000000f859e4_88888888-8888-8888-8888-8888888888h2.dat";
    const std::string shared_compaction_input = "0000000000f859e4_88888888-8888-8888-8888-8888888888h3.dat";
    const std::string latest_segment = "0000000000f959e4_99999999-9999-9999-9999-9999999999i1.dat";
    create_data_file(shared_orphan);
    create_data_file(private_orphan);
    create_data_file(shared_compaction_input);
    create_data_file(latest_segment);

    // Version 3 (latest): range distribution tablet with orphan_files and compaction_inputs
    // from a previous compaction. One orphan file is shared, one is not.
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(json_to_pb<TabletMetadataPB>(R"DEL(
        {
        "id": 7000,
        "version": 3,
        "range": {
            "lower_bound_included": true,
            "upper_bound_included": false
        },
        "rowsets": [
            {
                "segments": [
                    "0000000000f959e4_99999999-9999-9999-9999-9999999999i1.dat"
                ],
                "data_size": 4096
            }
        ],
        "orphan_files": [
            {
                "name": "0000000000f859e4_88888888-8888-8888-8888-8888888888h1.dat",
                "size": 100,
                "shared": true
            },
            {
                "name": "0000000000f859e4_88888888-8888-8888-8888-8888888888h2.dat",
                "size": 100,
                "shared": false
            }
        ],
        "compaction_inputs": [
            {
                "segments": [
                    "0000000000f859e4_88888888-8888-8888-8888-8888888888h3.dat"
                ],
                "shared_segments": [true]
            }
        ]
        }
        )DEL")));

    {
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(7000);
        delete_tablets(_tablet_mgr.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);

        // Shared orphan file should be retained (protected by range distribution check)
        EXPECT_TRUE(file_exist(shared_orphan));
        // Shared compaction input should be retained
        EXPECT_TRUE(file_exist(shared_compaction_input));
        // Private orphan file can be deleted (not shared, so safe)
        EXPECT_FALSE(file_exist(private_orphan));
        // Latest metadata data files should be retained (existing is_range_distribution protection)
        EXPECT_TRUE(file_exist(latest_segment));

        // Metadata file should be deleted
        EXPECT_FALSE(file_exist(tablet_metadata_filename(7000, 3)));
    }
}

} // namespace starrocks::lake
