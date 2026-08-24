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

// Aggregate publish assembling the query-side parent of a split family.
//
// Deliberately NOT in lake_service_test.cpp: that fixture's destructor runs fs::remove_all over a
// root every one of its tests shares, and these cases publish real parent metadata through a live
// brpc service. Sharing the root made them perturb unrelated tests in that file. This fixture owns
// its own root and tears down only that.

#include <brpc/controller.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <functional>
#include <memory>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "data_workflows/load/tablet_writer/load_channel_mgr.h"
#include "exec/exec_env.h"
#include "fs/fs_util.h"
#include "platform/platform_env.h"
#include "runtime/runtime_env.h"
#include "service/service_be/lake_service.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/test_util.h"
#include "storage/storage_env.h"
#include "storage/tablet_schema.h"
#include "storage/variant_tuple.h"

namespace starrocks {

using ::testing::_;
using ::testing::Invoke;

class MockParentViewLakeService : public starrocks::LakeService {
public:
    MOCK_METHOD4(publish_version, void(::google::protobuf::RpcController*, const ::starrocks::PublishVersionRequest*,
                                       ::starrocks::PublishVersionResponse*, ::google::protobuf::Closure* done));
};

class ParentViewTestBase : public testing::Test {
public:
    ParentViewTestBase()
            : _location_provider(std::make_shared<lake::FixedLocationProvider>(kRootLocation)),
              _tablet_mgr(StorageEnv::GetInstance()->lake_tablet_manager()),
              _load_channel_mgr(std::make_unique<LoadChannelMgr>(_tablet_mgr,
                                                                 RuntimeEnv::GetInstance()->diagnose_daemon(),
                                                                 PlatformEnv::GetInstance()->brpc_stub_cache())),
              _lake_service(ExecEnv::GetInstance(), StorageEnv::GetInstance()->lake_tablet_manager(),
                            _load_channel_mgr.get()) {
        CHECK_OK(_load_channel_mgr->init(RuntimeEnv::GetInstance()->load_mem_tracker()));
        // The tablet manager is a process-wide singleton: point it at this fixture's own root and put
        // the previous provider back on the way out, or every test that runs afterwards resolves its
        // files under a directory this fixture has already deleted.
        _backup_location_provider = _tablet_mgr->TEST_set_location_provider(_location_provider);
        CHECK_OK(FileSystem::Default()->create_dir_recursive(kRootLocation));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(
                lake::join_path(kRootLocation, lake::kMetadataDirectoryName)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(
                lake::join_path(kRootLocation, lake::kTxnLogDirectoryName)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(
                lake::join_path(kRootLocation, lake::kSegmentDirectoryName)));
    }

    ~ParentViewTestBase() override {
        (void)_tablet_mgr->TEST_set_location_provider(_backup_location_provider);
        (void)fs::remove_all(kRootLocation);
    }

protected:
    void init_server_with_mock(MockParentViewLakeService* mock_service, brpc::Server* server, int* port_out) {
        brpc::ServerOptions options;
        options.num_threads = 1;
        ASSERT_EQ(server->AddService(mock_service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
        ASSERT_EQ(server->Start(0, &options), 0);
        *port_out = server->listen_address().port;
    }

    AggregatePublishVersionRequest build_default_agg_request(int port) {
        AggregatePublishVersionRequest request;
        auto* compute_node = request.add_compute_nodes();
        compute_node->set_host("127.0.0.1");
        compute_node->set_brpc_port(port);
        request.add_publish_reqs()->set_timeout_ms(5000);
        return request;
    }

    TuplePB generate_sort_key(int value) {
        VariantTuple tuple;
        tuple.append(DatumVariant(get_type_info(LogicalType::TYPE_INT), Datum(value)));
        TuplePB tuple_pb;
        tuple.to_proto(&tuple_pb);
        return tuple_pb;
    }

    constexpr static const char* const kRootLocation = "./lake_service_parent_view_test";
    std::shared_ptr<lake::FixedLocationProvider> _location_provider;
    std::shared_ptr<lake::LocationProvider> _backup_location_provider;
    lake::TabletManager* _tablet_mgr = nullptr;
    std::unique_ptr<LoadChannelMgr> _load_channel_mgr;
    LakeServiceImpl _lake_service;
};

// The validation build_parent_tablet_metadata does before it assembles anything. Each case is a way
// the FE could describe a split family that cannot be turned into a readable parent, and every one of
// them has to fail the aggregate publish rather than persist a half-built parent that queries would
// then be pinned to.
class AggregateParentViewTest : public ParentViewTestBase {
protected:
    // Drives one aggregate publish whose child publish succeeds and returns |child| for
    // |child_tablet_id|, with the parent family described by |configure|. Returns the response status
    // code, so a caller asserts on the failure rather than on a thrown status.
    int publish_with_parent(int64_t child_tablet_id, int64_t version,
                            const std::function<void(AggregatePublishVersionRequest*)>& configure,
                            const std::shared_ptr<TabletMetadataPB>& child, bool expect_child_publish) {
        brpc::Server server;
        MockParentViewLakeService mock_service;
        int port = 0;
        init_server_with_mock(&mock_service, &server, &port);

        auto request = build_default_agg_request(port);
        auto* publish_req = request.mutable_publish_reqs(0);
        publish_req->set_new_version(version);
        publish_req->add_tablet_ids(child_tablet_id);
        publish_req->add_txn_infos()->set_txn_id(12345);
        configure(&request);

        auto respond = [&](::google::protobuf::RpcController*, const PublishVersionRequest*,
                           PublishVersionResponse* resp, ::google::protobuf::Closure* done) {
            resp->mutable_status()->set_status_code(0);
            if (child != nullptr) {
                (*resp->mutable_tablet_metas())[child_tablet_id].CopyFrom(*child);
            }
            done->Run();
        };
        if (expect_child_publish) {
            EXPECT_CALL(mock_service, publish_version(_, _, _, _)).WillOnce(Invoke(respond));
        } else {
            EXPECT_CALL(mock_service, publish_version(_, _, _, _)).WillRepeatedly(Invoke(respond));
        }

        PublishVersionResponse response;
        brpc::Controller cntl;
        google::protobuf::Closure* done = brpc::NewCallback([]() {});
        _lake_service.aggregate_publish_version(&cntl, &request, &response, done);
        const int code = response.status().status_code();

        server.Stop(0);
        server.Join();
        return code;
    }

    std::shared_ptr<TabletMetadataPB> make_child(int64_t id, int64_t version) {
        auto child = lake::generate_simple_tablet_metadata(PRIMARY_KEYS);
        child->set_id(id);
        child->set_version(version);
        child->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(0));
        child->mutable_range()->set_lower_bound_included(true);
        child->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(10));
        child->mutable_range()->set_upper_bound_included(false);
        return child;
    }
};

// A family with no children at all describes nothing to assemble from.
TEST_F(AggregateParentViewTest, test_rejects_a_family_without_children) {
    const int64_t kChild = next_id();
    const int64_t kParent = next_id();
    constexpr int64_t kVersion = 81;
    const int code = publish_with_parent(
            kChild, kVersion,
            [&](AggregatePublishVersionRequest* request) {
                request->add_parent_tablet_publish_infos()->set_parent_tablet_id(kParent);
            },
            make_child(kChild, kVersion), /*expect_child_publish=*/false);
    EXPECT_NE(0, code) << "a parent with no children must not publish";
}

// The parent is a read alias over its children; naming it as one of them is incoherent and would make
// the assembly read its own half-written output.
TEST_F(AggregateParentViewTest, test_rejects_a_parent_that_is_also_its_own_child) {
    const int64_t kTablet = next_id();
    constexpr int64_t kVersion = 81;
    const int code = publish_with_parent(
            kTablet, kVersion,
            [&](AggregatePublishVersionRequest* request) {
                auto* info = request->add_parent_tablet_publish_infos();
                info->set_parent_tablet_id(kTablet);
                info->add_child_tablet_ids(kTablet);
            },
            make_child(kTablet, kVersion), /*expect_child_publish=*/false);
    EXPECT_NE(0, code) << "a tablet cannot be both the parent and a child";
}

// A child whose publish returned no metadata cannot contribute its half of the parent. Assembling
// from what did arrive would publish a parent that is missing a key range.
TEST_F(AggregateParentViewTest, test_rejects_a_missing_child_metadata) {
    const int64_t kChild = next_id();
    const int64_t kAbsentChild = next_id();
    const int64_t kParent = next_id();
    constexpr int64_t kVersion = 81;
    const int code = publish_with_parent(
            kChild, kVersion,
            [&](AggregatePublishVersionRequest* request) {
                auto* info = request->add_parent_tablet_publish_infos();
                info->set_parent_tablet_id(kParent);
                info->add_child_tablet_ids(kChild);
                info->add_child_tablet_ids(kAbsentChild);
            },
            make_child(kChild, kVersion), /*expect_child_publish=*/true);
    EXPECT_NE(0, code) << "a child that reported no metadata must fail the family";
}

// DCG/IDG sidecars are not merged into the parent view yet, so a child carrying one must be refused
// rather than silently dropped from the assembled parent.
TEST_F(AggregateParentViewTest, test_rejects_dcg_metadata_on_a_child) {
    const int64_t kChild = next_id();
    const int64_t kParent = next_id();
    constexpr int64_t kVersion = 81;
    auto child = make_child(kChild, kVersion);
    (*child->mutable_dcg_meta()->mutable_dcgs())[1].add_column_files("child.cols");
    const int code = publish_with_parent(
            kChild, kVersion,
            [&](AggregatePublishVersionRequest* request) {
                auto* info = request->add_parent_tablet_publish_infos();
                info->set_parent_tablet_id(kParent);
                info->add_child_tablet_ids(kChild);
            },
            child, /*expect_child_publish=*/true);
    EXPECT_NE(0, code) << "a DCG sidecar has no place in the parent view yet";
}

TEST_F(AggregateParentViewTest, test_aggregate_publish_builds_query_parent_in_same_bundle) {
    brpc::Server server;
    MockParentViewLakeService mock_service;
    int port = 0;
    init_server_with_mock(&mock_service, &server, &port);

    // Drawn from next_id() rather than fixed: this binary runs ~2000 tests in one process and every
    // fixture draws from the same counter, so a hard-coded tablet id is eventually handed out to
    // somebody else and the two tests corrupt each other's metadata.
    const int64_t kParentTabletId = next_id();
    const int64_t kChildTabletId1 = next_id();
    const int64_t kChildTabletId2 = next_id();
    const int64_t kIdenticalParentTabletId = next_id();
    const int64_t kIdenticalChildTabletId = next_id();
    constexpr int64_t kVersion = 79;
    auto request = build_default_agg_request(port);
    auto* publish_req = request.mutable_publish_reqs(0);
    publish_req->set_new_version(kVersion);
    publish_req->add_tablet_ids(kChildTabletId1);
    publish_req->add_tablet_ids(kChildTabletId2);
    publish_req->add_tablet_ids(kIdenticalChildTabletId);
    publish_req->add_txn_infos()->set_txn_id(12345);
    auto* parent_info = request.add_parent_tablet_publish_infos();
    parent_info->set_parent_tablet_id(kParentTabletId);
    parent_info->add_child_tablet_ids(kChildTabletId1);
    parent_info->add_child_tablet_ids(kChildTabletId2);
    auto* identical_parent_info = request.add_parent_tablet_publish_infos();
    identical_parent_info->set_parent_tablet_id(kIdenticalParentTabletId);
    identical_parent_info->add_child_tablet_ids(kIdenticalChildTabletId);

    auto child1 = lake::generate_simple_tablet_metadata(PRIMARY_KEYS);
    child1->set_id(kChildTabletId1);
    child1->set_version(kVersion);
    child1->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(0));
    child1->mutable_range()->set_lower_bound_included(true);
    child1->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(10));
    child1->mutable_range()->set_upper_bound_included(false);
    auto child2 = std::make_shared<TabletMetadataPB>(*child1);
    child2->set_id(kChildTabletId2);
    child2->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(10));
    child2->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(20));
    auto identical_child = std::make_shared<TabletMetadataPB>(*child1);
    identical_child->set_id(kIdenticalChildTabletId);
    identical_child->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(30));
    identical_child->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(40));

    EXPECT_CALL(mock_service, publish_version(_, _, _, _))
            .WillOnce(Invoke([&](::google::protobuf::RpcController*, const PublishVersionRequest*,
                                 PublishVersionResponse* resp, ::google::protobuf::Closure* done) {
                resp->mutable_status()->set_status_code(0);
                (*resp->mutable_tablet_metas())[kChildTabletId1].CopyFrom(*child1);
                (*resp->mutable_tablet_metas())[kChildTabletId2].CopyFrom(*child2);
                (*resp->mutable_tablet_metas())[kIdenticalChildTabletId].CopyFrom(*identical_child);
                done->Run();
            }));

    PublishVersionResponse response;
    brpc::Controller cntl;
    google::protobuf::Closure* done = brpc::NewCallback([]() {});
    _lake_service.aggregate_publish_version(&cntl, &request, &response, done);

    ASSERT_EQ(0, response.status().status_code());
    ASSIGN_OR_ABORT(auto parent, _tablet_mgr->get_single_tablet_metadata(kParentTabletId, kVersion));
    EXPECT_EQ(kParentTabletId, parent->id());
    EXPECT_EQ(kVersion, parent->version());
    EXPECT_EQ(0, parent->range().lower_bound().values(0).value().compare("0"));
    EXPECT_EQ(0, parent->range().upper_bound().values(0).value().compare("20"));
    ASSIGN_OR_ABORT(auto identical_parent, _tablet_mgr->get_single_tablet_metadata(kIdenticalParentTabletId, kVersion));
    EXPECT_EQ(kIdenticalParentTabletId, identical_parent->id());
    EXPECT_EQ(0, identical_parent->range().lower_bound().values(0).value().compare("30"));
    EXPECT_EQ(0, identical_parent->range().upper_bound().values(0).value().compare("40"));

    server.Stop(0);
    server.Join();
}

} // namespace starrocks
