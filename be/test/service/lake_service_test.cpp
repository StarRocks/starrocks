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

#include "service/service_be/lake_service.h"

#include <brpc/controller.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "base/concurrency/await.h"
#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "fs/fs_util.h"
#include "gutil/strings/util.h"
#include "runtime/exec_env.h"
#include "runtime/load_channel_mgr.h"
#include "service/brpc_service_test_util.h"
#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/metacache.h"
#include "storage/lake/schema_change.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/test_util.h"
#include "storage/lake/txn_log.h"
#include "storage/variant_tuple.h"
#include "util/bthreads/util.h"
#include "util/countdown_latch.h"

namespace starrocks {

class MockLakeServiceImpl : public starrocks::LakeService {
public:
    MOCK_METHOD4(publish_version, void(::google::protobuf::RpcController*, const ::starrocks::PublishVersionRequest*,
                                       ::starrocks::PublishVersionResponse*, ::google::protobuf::Closure* done));
    MOCK_METHOD4(compact, void(::google::protobuf::RpcController*, const ::starrocks::CompactRequest*,
                               ::starrocks::CompactResponse*, ::google::protobuf::Closure* done));
};
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

class LakeServiceTest : public testing::Test {
public:
    LakeServiceTest()
            : _tablet_id(next_id()),
              _partition_id(next_id()),
              _location_provider(std::make_shared<lake::FixedLocationProvider>(kRootLocation)),
              _tablet_mgr(ExecEnv::GetInstance()->lake_tablet_manager()),
              _lake_service(ExecEnv::GetInstance(), ExecEnv::GetInstance()->lake_tablet_manager()) {
        _backup_location_provider = _tablet_mgr->TEST_set_location_provider(_location_provider);
        FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kSegmentDirectoryName));
        FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kMetadataDirectoryName));
        FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kTxnLogDirectoryName));
    }

    ~LakeServiceTest() override {
        CHECK_OK(fs::remove_all(kRootLocation));
        (void)_tablet_mgr->TEST_set_location_provider(_backup_location_provider);
    }

    void create_tablet() {
        auto metadata = lake::generate_simple_tablet_metadata(DUP_KEYS);
        _tablet_id = metadata->id();
        auto* tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
        ASSERT_TRUE(tablet_mgr != nullptr);
        ASSERT_OK(tablet_mgr->put_tablet_metadata(metadata));
    }

    void SetUp() override { create_tablet(); }

    void TearDown() override {}

protected:
    // Return the new generated segment file name
    std::string generate_segment_file(int64_t txn_id) {
        auto seg_name = lake::gen_segment_filename(txn_id);
        auto seg_path = _tablet_mgr->segment_location(_tablet_id, seg_name);
        ASSIGN_OR_ABORT(auto f, fs::new_writable_file(seg_path));
        CHECK_OK(f->append(seg_path));
        CHECK_OK(f->close());
        return seg_name;
    }

    TuplePB generate_sort_key(int value) {
        DatumVariant variant(get_type_info(LogicalType::TYPE_INT), Datum(value));
        VariantTuple tuple;
        tuple.append(variant);
        TuplePB tuplePB;
        tuple.to_proto(&tuplePB);
        return tuplePB;
    }

    TxnLog generate_write_txn_log(int num_segments, int64_t num_rows, int64_t data_size) {
        auto txn_id = next_id();
        TxnLog log;
        log.set_tablet_id(_tablet_id);
        log.set_partition_id(_partition_id);
        log.set_txn_id(txn_id);
        int sort_key = 0;
        for (int i = 0; i < num_segments; i++) {
            log.mutable_op_write()->mutable_rowset()->add_segments(generate_segment_file(txn_id));
            log.mutable_op_write()->mutable_rowset()->add_segment_size(1024);
            auto* segment_meta = log.mutable_op_write()->mutable_rowset()->add_segment_metas();
            segment_meta->mutable_sort_key_min()->CopyFrom(generate_sort_key(sort_key));
            sort_key += 100;
            segment_meta->mutable_sort_key_max()->CopyFrom(generate_sort_key(sort_key));
            segment_meta->set_num_rows(100);
        }
        log.mutable_op_write()->mutable_rowset()->set_data_size(data_size);
        log.mutable_op_write()->mutable_rowset()->set_num_rows(num_rows);
        log.mutable_op_write()->mutable_rowset()->set_overlapped(num_segments > 1);
        return log;
    }

    TxnLog generate_write_txn_log_with_segments(const std::vector<int>& min_keys, const std::vector<int>& max_keys,
                                                const std::vector<int64_t>& segment_num_rows,
                                                const std::vector<int64_t>& segment_sizes) {
        CHECK_EQ(min_keys.size(), max_keys.size());
        CHECK_EQ(min_keys.size(), segment_num_rows.size());
        CHECK_EQ(min_keys.size(), segment_sizes.size());

        auto txn_id = next_id();
        TxnLog log;
        log.set_tablet_id(_tablet_id);
        log.set_partition_id(_partition_id);
        log.set_txn_id(txn_id);

        int64_t total_rows = 0;
        int64_t total_size = 0;
        for (size_t i = 0; i < min_keys.size(); ++i) {
            log.mutable_op_write()->mutable_rowset()->add_segments(generate_segment_file(txn_id));
            log.mutable_op_write()->mutable_rowset()->add_segment_size(segment_sizes[i]);
            auto* segment_meta = log.mutable_op_write()->mutable_rowset()->add_segment_metas();
            segment_meta->mutable_sort_key_min()->CopyFrom(generate_sort_key(min_keys[i]));
            segment_meta->mutable_sort_key_max()->CopyFrom(generate_sort_key(max_keys[i]));
            segment_meta->set_num_rows(segment_num_rows[i]);
            total_rows += segment_num_rows[i];
            total_size += segment_sizes[i];
        }
        log.mutable_op_write()->mutable_rowset()->set_data_size(total_size);
        log.mutable_op_write()->mutable_rowset()->set_num_rows(total_rows);
        log.mutable_op_write()->mutable_rowset()->set_overlapped(min_keys.size() > 1);
        return log;
    }

    void run_aggregate_compact(::google::protobuf::RpcController* cntl, const AggregateCompactRequest* request,
                               CompactResponse* response) {
        CountDownLatch latch(1);
        auto cb = ::google::protobuf::NewCallback(&latch, &CountDownLatch::count_down);
        _lake_service.aggregate_compact(cntl, request, response, cb);
        latch.wait();
    }

    void init_server_with_mock(MockLakeServiceImpl* mock_service, brpc::Server* server, int* port_out) {
        brpc::ServerOptions options;
        options.num_threads = 1;
        ASSERT_EQ(server->AddService(mock_service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
        ASSERT_EQ(server->Start(0, &options), 0);
        butil::EndPoint server_addr = server->listen_address();
        *port_out = server_addr.port;
    }

    AggregatePublishVersionRequest build_default_agg_request(int port) {
        AggregatePublishVersionRequest request;
        auto* compute_node = request.add_compute_nodes();
        compute_node->set_host("127.0.0.1");
        compute_node->set_brpc_port(port);
        auto* publish_req = request.add_publish_reqs();
        publish_req->set_timeout_ms(5000);
        return request;
    }

    void build_schemas_and_metadata(TabletSchemaPB* schema_pb1, TabletSchemaPB* schema_pb2, TabletSchemaPB* schema_pb3,
                                    starrocks::TabletMetadataPB* metadata1, starrocks::TabletMetadataPB* metadata2) {
        // schema 1
        schema_pb1->set_id(10);
        schema_pb1->set_num_short_key_columns(1);
        schema_pb1->set_keys_type(DUP_KEYS);
        schema_pb1->set_num_rows_per_row_block(65535);
        auto c0 = schema_pb1->add_column();
        c0->set_unique_id(0);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);

        // schema 2
        schema_pb2->set_id(11);
        schema_pb2->set_num_short_key_columns(1);
        schema_pb2->set_keys_type(DUP_KEYS);
        schema_pb2->set_num_rows_per_row_block(65535);
        auto c1 = schema_pb2->add_column();
        c1->set_unique_id(1);
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);

        // schema 3
        if (schema_pb3 != nullptr) {
            schema_pb3->set_id(12);
            schema_pb3->set_num_short_key_columns(1);
            schema_pb3->set_keys_type(DUP_KEYS);
            schema_pb3->set_num_rows_per_row_block(65535);
            auto c2 = schema_pb3->add_column();
            c2->set_unique_id(2);
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
        }

        // metadata 1
        metadata1->set_id(1);
        metadata1->set_version(2);
        metadata1->mutable_schema()->CopyFrom(*schema_pb1);
        auto& item1 = (*metadata1->mutable_historical_schemas())[10];
        item1.CopyFrom(*schema_pb1);
        auto& item2 = (*metadata1->mutable_historical_schemas())[11];
        item2.CopyFrom(*schema_pb2);
        (*metadata1->mutable_rowset_to_schema())[3] = 11;

        // metadata 2
        if (metadata2 != nullptr) {
            metadata2->set_id(2);
            metadata2->set_version(2);
            metadata2->mutable_schema()->CopyFrom(*schema_pb1);
            auto& item1_b = (*metadata1->mutable_historical_schemas())[10];
            item1_b.CopyFrom(*schema_pb1);
            if (schema_pb3 != nullptr) {
                auto& item2_b = (*metadata1->mutable_historical_schemas())[12];
                item2_b.CopyFrom(*schema_pb3);
            }
        }
    }

    constexpr static const char* const kRootLocation = "./lake_service_test";
    int64_t _tablet_id;
    int64_t _partition_id;
    std::shared_ptr<lake::LocationProvider> _location_provider;
    lake::TabletManager* _tablet_mgr;
    std::shared_ptr<lake::LocationProvider> _backup_location_provider;
    LakeServiceImpl _lake_service;
};

TEST_F(LakeServiceTest, test_publish_version_missing_tablet_ids) {
    brpc::Controller cntl;
    PublishVersionRequest request;
    PublishVersionResponse response;
    request.set_base_version(1);
    request.set_new_version(2);
    request.add_txn_ids(1000);
    _lake_service.publish_version(&cntl, &request, &response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ("neither tablet_ids nor resharding_tablet_infos is set, one of them must be set", cntl.ErrorText());
}

TEST_F(LakeServiceTest, test_publish_version_missing_txn_ids) {
    brpc::Controller cntl;
    PublishVersionRequest request;
    PublishVersionResponse response;
    request.set_base_version(1);
    request.set_new_version(2);
    request.add_tablet_ids(_tablet_id);
    _lake_service.publish_version(&cntl, &request, &response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ("neither txn_ids nor txn_infos is set, one of them must be set", cntl.ErrorText());
}

TEST_F(LakeServiceTest, test_publish_version_missing_base_version) {
    brpc::Controller cntl;
    PublishVersionRequest request;
    PublishVersionResponse response;
    request.set_new_version(2);
    request.add_tablet_ids(_tablet_id);
    request.add_txn_ids(1000);
    _lake_service.publish_version(&cntl, &request, &response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ("missing base version", cntl.ErrorText());
}

TEST_F(LakeServiceTest, test_publish_version_missing_new_version) {
    brpc::Controller cntl;
    PublishVersionRequest request;
    PublishVersionResponse response;
    request.set_base_version(1);
    request.add_tablet_ids(_tablet_id);
    request.add_txn_ids(1000);
    _lake_service.publish_version(&cntl, &request, &response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ("missing new version", cntl.ErrorText());
}

TEST_F(LakeServiceTest, test_publish_version_thread_pool_full) {
    SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:1", [](void* arg) { *(int64_t*)arg = 0; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    brpc::Controller cntl;
    PublishVersionRequest request;
    PublishVersionResponse response;
    request.set_base_version(1);
    request.set_new_version(2);
    request.add_tablet_ids(_tablet_id);
    request.add_txn_ids(1000);
    _lake_service.publish_version(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(1, response.failed_tablets_size());
    ASSERT_EQ(_tablet_id, response.failed_tablets(0));
}

TEST_F(LakeServiceTest, test_publish_version_for_write) {
    std::vector<TxnLog> logs;
    // Empty TxnLog
    logs.emplace_back(generate_write_txn_log(0, 0, 0));
    ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));

    // TxnLog with 2 segments
    logs.emplace_back(generate_write_txn_log(2, 101, 4096));
    ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));

    // Publish version request for the first transaction
    PublishVersionRequest publish_request_1000;
    publish_request_1000.set_base_version(1);
    publish_request_1000.set_new_version(2);
    publish_request_1000.add_tablet_ids(_tablet_id);
    publish_request_1000.add_txn_ids(logs[0].txn_id());

    // Publish txn failed: get base tablet metadata failed
    {
        _tablet_mgr->prune_metacache();

        TEST_ENABLE_ERROR_POINT("TabletManager::load_tablet_metadata",
                                Status::IOError("injected get tablet metadata error"));

        SyncPoint::GetInstance()->EnableProcessing();

        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("TabletManager::load_tablet_metadata");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1000, &response, nullptr);
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
        EXPECT_TRUE(MatchPattern(response.status().error_msgs(0), "injected get tablet metadata error"))
                << response.status().error_msgs(0);
    }
    // Publish failed: get txn log failed
    {
        TEST_ENABLE_ERROR_POINT("TabletManager::load_txn_log", Status::IOError("injected get txn log error"));

        SyncPoint::GetInstance()->EnableProcessing();

        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("TabletManager::load_txn_log");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1000, &response, nullptr);
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
        EXPECT_TRUE(MatchPattern(response.status().error_msgs(0), "injected get txn log error"))
                << response.status().error_msgs(0);
    }
    // Publish txn success
    {
        PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1000, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
    }

    // publish version request for the second transaction
    PublishVersionRequest publish_request_1;
    publish_request_1.set_base_version(2);
    publish_request_1.set_new_version(3);
    publish_request_1.add_tablet_ids(_tablet_id);
    publish_request_1.add_txn_ids(logs[1].txn_id());
    publish_request_1.set_commit_time(987654321);

    // Publish txn put tablet metadata failed
    {
        TEST_ENABLE_ERROR_POINT("TabletManager::put_tablet_metadata",
                                Status::IOError("injected put tablet metadata error"));

        SyncPoint::GetInstance()->EnableProcessing();

        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("TabletManager::put_tablet_metadata");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1, &response, nullptr);
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
        EXPECT_TRUE(MatchPattern(response.status().error_msgs(0), "injected put tablet metadata error"))
                << response.status().error_msgs(0);
    }

    // Publish txn success
    {
        PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
    }
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_id));
    {
        ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(3));
        ASSERT_EQ(3, metadata->version());
        ASSERT_EQ(_tablet_id, metadata->id());
        ASSERT_EQ(3, metadata->next_rowset_id());
        ASSERT_EQ(1, metadata->rowsets_size());
        ASSERT_EQ(1, metadata->rowsets(0).id());
        ASSERT_EQ(2, metadata->rowsets(0).segments_size());
        ASSERT_TRUE(metadata->rowsets(0).overlapped());
        ASSERT_EQ(logs[1].op_write().rowset().num_rows(), metadata->rowsets(0).num_rows());
        ASSERT_EQ(logs[1].op_write().rowset().data_size(), metadata->rowsets(0).data_size());
        ASSERT_EQ(logs[1].op_write().rowset().segments(0), metadata->rowsets(0).segments(0));
        ASSERT_EQ(logs[1].op_write().rowset().segments(1), metadata->rowsets(0).segments(1));
        EXPECT_EQ(987654321, metadata->commit_time());
    }
    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
    // TxnLog`s should have been deleted
    ASSERT_TRUE(tablet.get_txn_log(logs[0].txn_id()).status().is_not_found());
    ASSERT_TRUE(tablet.get_txn_log(logs[1].txn_id()).status().is_not_found());

    // Send publish version request again.
    for (int i = 0; i < 2; i++) {
        if (i == 1) {
            _tablet_mgr->prune_metacache();
        }
        PublishVersionRequest request;
        PublishVersionResponse response;
        request.set_base_version(2);
        request.set_new_version(3);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(logs[1].txn_id());
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
        ASSERT_EQ(1, response.compaction_scores_size());
    }
    // Send publish version request again with an non-exist tablet
    {
        PublishVersionRequest request;
        PublishVersionResponse response;
        request.set_base_version(2);
        request.set_new_version(3);
        request.add_tablet_ids(_tablet_id);
        request.add_tablet_ids(9999);
        request.add_txn_ids(logs[1].txn_id());
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(9999, response.failed_tablets(0));
        ASSERT_EQ(1, response.compaction_scores_size());
        ASSERT_TRUE(response.compaction_scores().contains(_tablet_id));
    }
    // Send publish version request again with an non-exist txnlog
    {
        PublishVersionRequest request;
        PublishVersionResponse response;
        request.set_base_version(3);
        request.set_new_version(4);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1111);
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
        ASSERT_EQ(0, response.compaction_scores_size());
    }
    // Delete old version metadata then send publish version again
    ASSERT_OK(tablet.delete_metadata(1));
    ASSERT_OK(tablet.delete_metadata(2));
    {
        PublishVersionRequest request;
        PublishVersionResponse response;
        request.set_base_version(2);
        request.set_new_version(3);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(logs[1].txn_id());
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
        ASSERT_TRUE(response.compaction_scores().contains(_tablet_id));
    }

    // Empty TxnLog
    {
        logs.emplace_back(generate_write_txn_log(0, 0, 0));
        ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));
    }
    // Publish txn
    {
        PublishVersionRequest request;
        PublishVersionResponse response;
        request.set_base_version(3);
        request.set_new_version(4);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(logs[2].txn_id());
        request.set_commit_time(0);
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());

        ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(4));
        EXPECT_EQ(0, metadata->commit_time());
    }
}

TEST_F(LakeServiceTest, test_publish_version_for_write_batch) {
    // Empty TxnLog
    {
        TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(1002);
        txnlog.mutable_op_write()->mutable_rowset()->set_num_rows(0);
        txnlog.mutable_op_write()->mutable_rowset()->set_data_size(0);
        txnlog.mutable_op_write()->mutable_rowset()->set_overlapped(false);
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog));
    }
    // TxnLog with 2 segments
    {
        TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(1003);
        txnlog.mutable_op_write()->mutable_rowset()->set_overlapped(true);
        txnlog.mutable_op_write()->mutable_rowset()->set_num_rows(101);
        txnlog.mutable_op_write()->mutable_rowset()->set_data_size(4096);
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("1.dat");
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("2.dat");
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog));
    }

    // Publish txn 1002 and txn 1003
    {
        PublishVersionRequest request;
        PublishVersionResponse response;
        request.set_base_version(1);
        request.set_new_version(3);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1002);
        request.add_txn_ids(1003);
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
    }

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_id));
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(3));
    ASSERT_EQ(3, metadata->version());
    ASSERT_EQ(_tablet_id, metadata->id());
    ASSERT_EQ(3, metadata->next_rowset_id());
    ASSERT_EQ(1, metadata->rowsets_size());
    ASSERT_EQ(1, metadata->rowsets(0).id());
    ASSERT_EQ(2, metadata->rowsets(0).segments_size());
    ASSERT_TRUE(metadata->rowsets(0).overlapped());
    ASSERT_EQ(101, metadata->rowsets(0).num_rows());
    ASSERT_EQ(4096, metadata->rowsets(0).data_size());
    ASSERT_EQ("1.dat", metadata->rowsets(0).segments(0));
    ASSERT_EQ("2.dat", metadata->rowsets(0).segments(1));

    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
    // TxnLog should't have been deleted
    ASSERT_TRUE(tablet.get_txn_log(1002).status().ok());
    ASSERT_TRUE(tablet.get_txn_log(1003).status().ok());

    // Send publish version request again.
    {
        PublishVersionRequest request;
        PublishVersionResponse response;
        request.set_base_version(2);
        request.set_new_version(3);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1003);
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
        ASSERT_EQ(1, response.compaction_scores_size());
    }
    // Send publish version request again with an non-exist tablet
    {
        PublishVersionRequest request;
        PublishVersionResponse response;
        request.set_base_version(2);
        request.set_new_version(3);
        request.add_tablet_ids(_tablet_id);
        request.add_tablet_ids(9999);
        request.add_txn_ids(1003);
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(9999, response.failed_tablets(0));
        ASSERT_EQ(1, response.compaction_scores_size());
        ASSERT_TRUE(response.compaction_scores().contains(_tablet_id));
    }
    // Send publish version request again with an non-exist txnlog
    {
        PublishVersionRequest request;
        PublishVersionResponse response;
        request.set_base_version(3);
        request.set_new_version(4);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1111);
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
        ASSERT_EQ(0, response.compaction_scores_size());
    }
    // Delete old version metadata then send publish version again
    ASSERT_OK(tablet.delete_metadata(1));
    {
        PublishVersionRequest request;
        PublishVersionResponse response;
        request.set_base_version(1);
        request.set_new_version(3);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1002);
        request.add_txn_ids(1003);
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
        ASSERT_TRUE(response.compaction_scores().contains(_tablet_id));
    }
}

TEST_F(LakeServiceTest, test_publish_version_transform_single_to_batch) {
    std::vector<TxnLog> logs;
    // Empty TxnLog
    logs.emplace_back(generate_write_txn_log(0, 0, 0));
    ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));

    // Empty TxnLog
    logs.emplace_back(generate_write_txn_log(0, 0, 0));
    ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));

    // TxnLog with 2 segments
    logs.emplace_back(generate_write_txn_log(2, 101, 4096));
    ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));

    // Publish version request for the first transaction
    PublishVersionRequest publish_request_1000;
    publish_request_1000.set_base_version(1);
    publish_request_1000.set_new_version(2);
    publish_request_1000.add_tablet_ids(_tablet_id);
    publish_request_1000.add_txn_ids(logs[0].txn_id());

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_id));

    // Publish txn single
    {
        PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1000, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        // TxnLog should have been deleted
        ASSERT_TRUE(tablet.get_txn_log(logs[0].txn_id()).status().is_not_found());
    }

    // Publish version request for the two transactions
    PublishVersionRequest publish_request_1001;
    publish_request_1001.set_base_version(1);
    publish_request_1001.set_new_version(4);
    publish_request_1001.add_tablet_ids(_tablet_id);
    publish_request_1001.add_txn_ids(logs[0].txn_id());
    publish_request_1001.add_txn_ids(logs[1].txn_id());
    publish_request_1001.add_txn_ids(logs[2].txn_id());

    // publish txn batch with previous txns which have been published
    {
        PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1001, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        // TxnLog of logs[0] should have been deleted
        ASSERT_TRUE(tablet.get_txn_log(logs[0].txn_id()).status().is_not_found());
        // the other txn_logs should't have been deleted
        ASSERT_TRUE(tablet.get_txn_log(logs[1].txn_id()).status().ok());
        ASSERT_TRUE(tablet.get_txn_log(logs[2].txn_id()).status().ok());

        ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(4));
        ASSERT_EQ(4, metadata->version());
        ASSERT_EQ(_tablet_id, metadata->id());
        ASSERT_EQ(3, metadata->next_rowset_id());
        ASSERT_EQ(1, metadata->rowsets_size());
        ASSERT_EQ(1, metadata->rowsets(0).id());
        ASSERT_EQ(2, metadata->rowsets(0).segments_size());
        ASSERT_TRUE(metadata->rowsets(0).overlapped());
        ASSERT_EQ(101, metadata->rowsets(0).num_rows());
        ASSERT_EQ(4096, metadata->rowsets(0).data_size());

        // middle tablet meta should't exist
        ASSERT_FALSE(tablet.get_metadata(3).status().ok());
    }
}

TEST_F(LakeServiceTest, test_publish_version_transform_batch_to_single) {
    std::vector<TxnLog> logs;
    // Empty TxnLog
    logs.emplace_back(generate_write_txn_log(0, 0, 0));
    ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));

    // TxnLog with 2 segments
    logs.emplace_back(generate_write_txn_log(2, 101, 4096));
    ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));

    // Publish version request
    PublishVersionRequest publish_request_1000;
    publish_request_1000.set_base_version(1);
    publish_request_1000.set_new_version(3);
    publish_request_1000.add_tablet_ids(_tablet_id);
    publish_request_1000.add_txn_ids(logs[0].txn_id());
    publish_request_1000.add_txn_ids(logs[1].txn_id());

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_id));

    // Publish txn batch
    {
        PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1000, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        // TxnLog should't have been deleted
        ASSERT_TRUE(tablet.get_txn_log(logs[0].txn_id()).status().ok());
        ASSERT_TRUE(tablet.get_txn_log(logs[1].txn_id()).status().ok());

        ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(3));
        ASSERT_EQ(3, metadata->version());
        ASSERT_EQ(_tablet_id, metadata->id());
        ASSERT_EQ(101, metadata->rowsets(0).num_rows());
        ASSERT_EQ(4096, metadata->rowsets(0).data_size());
    }

    // Publish single
    PublishVersionRequest publish_request_1001;
    publish_request_1001.set_base_version(1);
    publish_request_1001.set_new_version(2);
    publish_request_1001.add_tablet_ids(_tablet_id);
    publish_request_1001.add_txn_ids(logs[0].txn_id());

    // publish first txn
    {
        PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1001, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        // TxnLog of logs[0] should have been deleted
        ASSERT_TRUE(tablet.get_txn_log(logs[0].txn_id()).status().is_not_found());
        // TxnLog of logs[1] should't have been deleted
        ASSERT_TRUE(tablet.get_txn_log(logs[1].txn_id()).status().ok());

        ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(2));
        ASSERT_EQ(2, metadata->version());
        ASSERT_EQ(_tablet_id, metadata->id());
        ASSERT_EQ(0, metadata->rowsets_size());
    }

    // Publish single
    PublishVersionRequest publish_request_1002;
    publish_request_1002.set_base_version(2);
    publish_request_1002.set_new_version(3);
    publish_request_1002.add_tablet_ids(_tablet_id);
    publish_request_1002.add_txn_ids(logs[1].txn_id());

    // publish second txn
    {
        _tablet_mgr->metacache()->prune();

        PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1002, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        // TxnLog of logs[1] should have been deleted
        ASSERT_TRUE(tablet.get_txn_log(logs[1].txn_id()).status().is_not_found());

        ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(3));
        ASSERT_EQ(3, metadata->version());
        ASSERT_EQ(_tablet_id, metadata->id());
        ASSERT_EQ(101, metadata->rowsets(0).num_rows());
        ASSERT_EQ(4096, metadata->rowsets(0).data_size());
    }
}

TEST_F(LakeServiceTest, test_publish_splitting_tablet) {
    {
        auto txn_log = generate_write_txn_log(1, 100, 100);
        ASSERT_OK(_tablet_mgr->put_txn_log(txn_log));

        PublishVersionRequest publish_request;
        publish_request.set_base_version(1);
        publish_request.set_new_version(2);
        publish_request.add_tablet_ids(_tablet_id);
        publish_request.add_txn_ids(txn_log.txn_id());

        {
            SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:1", [](void* arg) { *(int64_t*)arg = 0; });
            SyncPoint::GetInstance()->EnableProcessing();
            DeferOp defer([]() {
                SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:1");
                SyncPoint::GetInstance()->DisableProcessing();
            });

            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(1, response.failed_tablets_size());
            EXPECT_NE(0, response.status().status_code()) << response.status().error_msgs(0);
        }

        {
            class MockRunnable : public Runnable {
            public:
                MockRunnable() {}
                virtual ~MockRunnable() override {}
                virtual void run() override {}
                virtual void cancel() override {}
            };

            SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:replace_task", [](void* arg) {
                auto ptr = (*(std::shared_ptr<Runnable>*)arg);
                ptr->cancel();
                (*(std::shared_ptr<Runnable>*)arg) = std::make_shared<MockRunnable>();
            });
            SyncPoint::GetInstance()->EnableProcessing();
            DeferOp defer([]() {
                SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:replace_task");
                SyncPoint::GetInstance()->DisableProcessing();
            });

            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(1, response.failed_tablets_size());
            EXPECT_NE(0, response.status().status_code()) << response.status().error_msgs(0);
        }

        {
            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(0, response.failed_tablets_size());
            EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
            ASSERT_EQ(0, response.tablet_metas_size());
            ASSERT_EQ(0, response.tablet_ranges_size());
        }

        {
            publish_request.set_enable_aggregate_publish(true);
            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(0, response.failed_tablets_size());
            EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
            ASSERT_EQ(1, response.tablet_metas_size());
            ASSERT_EQ(0, response.tablet_ranges_size());
        }
    }

    ReshardingTabletInfoPB resharding_tablet_info;
    auto* splitting_tablet_info = resharding_tablet_info.mutable_splitting_tablet_info();
    splitting_tablet_info->set_old_tablet_id(_tablet_id);
    splitting_tablet_info->add_new_tablet_ids(next_id());
    splitting_tablet_info->add_new_tablet_ids(next_id());

    // Test tablet reshard transaction
    {
        PublishVersionRequest publish_request;
        publish_request.set_base_version(2);
        publish_request.set_new_version(3);

        auto* txn_info = publish_request.add_txn_infos();
        txn_info->set_txn_id(next_id());
        txn_info->set_txn_type(TXN_TABLET_RESHARD);
        txn_info->set_combined_txn_log(false);
        txn_info->set_commit_time(12345);
        txn_info->set_force_publish(false);

        publish_request.add_resharding_tablet_infos()->CopyFrom(resharding_tablet_info);

        {
            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(0, response.failed_tablets_size());
            EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
            ASSERT_EQ(0, response.tablet_metas_size());
            ASSERT_EQ(2, response.tablet_ranges_size());
        }

        {
            publish_request.set_enable_aggregate_publish(true);
            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(0, response.failed_tablets_size());
            EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
            ASSERT_EQ(3, response.tablet_metas_size());
            ASSERT_EQ(2, response.tablet_ranges_size());
        }
    }

    // Test cross publish
    {
        auto txn_log = generate_write_txn_log(1, 100, 100);
        ASSERT_OK(_tablet_mgr->put_txn_log(txn_log));

        PublishVersionRequest publish_request;
        publish_request.set_base_version(3);
        publish_request.set_new_version(4);
        publish_request.add_txn_ids(txn_log.txn_id());
        publish_request.add_resharding_tablet_infos()->CopyFrom(resharding_tablet_info);

        {
            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(0, response.failed_tablets_size());
            EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
            ASSERT_EQ(0, response.tablet_metas_size());
            ASSERT_EQ(0, response.tablet_ranges_size());
        }

        {
            publish_request.set_enable_aggregate_publish(true);
            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(0, response.failed_tablets_size());
            EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
            ASSERT_EQ(2, response.tablet_metas_size());
            ASSERT_EQ(0, response.tablet_ranges_size());
        }
    }
}

TEST_F(LakeServiceTest, test_splitting_tablet_split_count_three_with_tail_stats) {
    auto txn_log = generate_write_txn_log_with_segments({0, 100, 200, 300}, {100, 200, 300, 400}, {34, 34, 34, 1},
                                                        {100, 100, 100, 10});
    ASSERT_OK(_tablet_mgr->put_txn_log(txn_log));

    PublishVersionRequest publish_request;
    publish_request.set_base_version(1);
    publish_request.set_new_version(2);
    publish_request.add_tablet_ids(_tablet_id);
    publish_request.add_txn_ids(txn_log.txn_id());

    PublishVersionResponse response;
    _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
    ASSERT_EQ(0, response.status().status_code());

    ReshardingTabletInfoPB resharding_tablet_info;
    auto* splitting_tablet_info = resharding_tablet_info.mutable_splitting_tablet_info();
    splitting_tablet_info->set_old_tablet_id(_tablet_id);
    std::vector<int64_t> new_tablet_ids{next_id(), next_id(), next_id()};
    for (auto new_tablet_id : new_tablet_ids) {
        splitting_tablet_info->add_new_tablet_ids(new_tablet_id);
    }

    PublishVersionRequest reshard_request;
    reshard_request.set_base_version(2);
    reshard_request.set_new_version(3);
    auto* txn_info = reshard_request.add_txn_infos();
    txn_info->set_txn_id(next_id());
    txn_info->set_txn_type(TXN_TABLET_RESHARD);
    txn_info->set_combined_txn_log(false);
    txn_info->set_commit_time(12345);
    txn_info->set_force_publish(false);
    reshard_request.add_resharding_tablet_infos()->CopyFrom(resharding_tablet_info);

    PublishVersionResponse reshard_response;
    _lake_service.publish_version(nullptr, &reshard_request, &reshard_response, nullptr);
    ASSERT_EQ(0, reshard_response.status().status_code());
    ASSERT_EQ(3, reshard_response.tablet_ranges_size());

    int64_t total_rows = 0;
    int64_t total_size = 0;
    for (auto new_tablet_id : new_tablet_ids) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(new_tablet_id, 3));
        ASSERT_EQ(1, metadata->rowsets_size());
        EXPECT_GT(metadata->rowsets(0).num_rows(), 0);
        EXPECT_GT(metadata->rowsets(0).data_size(), 0);
        total_rows += metadata->rowsets(0).num_rows();
        total_size += metadata->rowsets(0).data_size();
    }
    EXPECT_EQ(103, total_rows);
    EXPECT_EQ(310, total_size);
}

TEST_F(LakeServiceTest, test_splitting_tablet_split_count_three_with_final_acc_stats) {
    auto txn_log = generate_write_txn_log_with_segments({0, 100, 200}, {100, 200, 300}, {34, 34, 32}, {100, 100, 320});
    ASSERT_OK(_tablet_mgr->put_txn_log(txn_log));

    PublishVersionRequest publish_request;
    publish_request.set_base_version(1);
    publish_request.set_new_version(2);
    publish_request.add_tablet_ids(_tablet_id);
    publish_request.add_txn_ids(txn_log.txn_id());

    PublishVersionResponse response;
    _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
    ASSERT_EQ(0, response.status().status_code());

    ReshardingTabletInfoPB resharding_tablet_info;
    auto* splitting_tablet_info = resharding_tablet_info.mutable_splitting_tablet_info();
    splitting_tablet_info->set_old_tablet_id(_tablet_id);
    std::vector<int64_t> new_tablet_ids{next_id(), next_id(), next_id()};
    for (auto new_tablet_id : new_tablet_ids) {
        splitting_tablet_info->add_new_tablet_ids(new_tablet_id);
    }

    PublishVersionRequest reshard_request;
    reshard_request.set_base_version(2);
    reshard_request.set_new_version(3);
    auto* txn_info = reshard_request.add_txn_infos();
    txn_info->set_txn_id(next_id());
    txn_info->set_txn_type(TXN_TABLET_RESHARD);
    txn_info->set_combined_txn_log(false);
    txn_info->set_commit_time(12345);
    txn_info->set_force_publish(false);
    reshard_request.add_resharding_tablet_infos()->CopyFrom(resharding_tablet_info);

    PublishVersionResponse reshard_response;
    _lake_service.publish_version(nullptr, &reshard_request, &reshard_response, nullptr);
    ASSERT_EQ(0, reshard_response.status().status_code());
    ASSERT_EQ(3, reshard_response.tablet_ranges_size());

    int64_t total_rows = 0;
    int64_t total_size = 0;
    for (auto new_tablet_id : new_tablet_ids) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(new_tablet_id, 3));
        ASSERT_EQ(1, metadata->rowsets_size());
        EXPECT_GT(metadata->rowsets(0).num_rows(), 0);
        EXPECT_GT(metadata->rowsets(0).data_size(), 0);
        total_rows += metadata->rowsets(0).num_rows();
        total_size += metadata->rowsets(0).data_size();
    }
    EXPECT_EQ(100, total_rows);
    EXPECT_EQ(520, total_size);
}

TEST_F(LakeServiceTest, test_splitting_tablet_pk_with_delvec_stats) {
    auto metadata = lake::generate_simple_tablet_metadata(PRIMARY_KEYS);
    metadata->set_id(next_id());
    metadata->set_version(2);
    metadata->set_next_rowset_id(3);

    auto* rowset = metadata->add_rowsets();
    rowset->set_id(1);
    rowset->set_overlapped(false);
    rowset->set_num_rows(150);
    rowset->set_data_size(1500);

    rowset->add_segments("seg_0");
    rowset->add_segment_size(1000);
    auto* segment_meta0 = rowset->add_segment_metas();
    segment_meta0->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
    segment_meta0->mutable_sort_key_max()->CopyFrom(generate_sort_key(50));
    segment_meta0->set_num_rows(100);

    rowset->add_segments("seg_1");
    rowset->add_segment_size(500);
    auto* segment_meta1 = rowset->add_segment_metas();
    segment_meta1->mutable_sort_key_min()->CopyFrom(generate_sort_key(100));
    segment_meta1->mutable_sort_key_max()->CopyFrom(generate_sort_key(150));
    segment_meta1->set_num_rows(50);

    _tablet_id = metadata->id();
    auto tablet = std::make_shared<lake::Tablet>(_tablet_mgr, _tablet_id);
    lake::MetaFileBuilder builder(*tablet, metadata);

    DelVector dv0;
    dv0.set_empty();
    std::shared_ptr<DelVector> ndv0;
    std::vector<uint32_t> del_ids0;
    del_ids0.reserve(40);
    for (uint32_t i = 0; i < 40; ++i) {
        del_ids0.push_back(i);
    }
    dv0.add_dels_as_new_version(del_ids0, metadata->version(), &ndv0);
    builder.append_delvec(ndv0, rowset->id());

    DelVector dv1;
    dv1.set_empty();
    std::shared_ptr<DelVector> ndv1;
    std::vector<uint32_t> del_ids1;
    del_ids1.reserve(10);
    for (uint32_t i = 0; i < 10; ++i) {
        del_ids1.push_back(i);
    }
    dv1.add_dels_as_new_version(del_ids1, metadata->version(), &ndv1);
    builder.append_delvec(ndv1, rowset->id() + 1);

    ASSERT_OK(builder.finalize(next_id()));

    ReshardingTabletInfoPB resharding_tablet_info;
    auto* splitting_tablet_info = resharding_tablet_info.mutable_splitting_tablet_info();
    splitting_tablet_info->set_old_tablet_id(_tablet_id);
    std::vector<int64_t> new_tablet_ids{next_id(), next_id()};
    for (auto new_tablet_id : new_tablet_ids) {
        splitting_tablet_info->add_new_tablet_ids(new_tablet_id);
    }

    PublishVersionRequest reshard_request;
    reshard_request.set_base_version(metadata->version());
    reshard_request.set_new_version(metadata->version() + 1);
    auto* txn_info = reshard_request.add_txn_infos();
    txn_info->set_txn_id(next_id());
    txn_info->set_txn_type(TXN_TABLET_RESHARD);
    txn_info->set_combined_txn_log(false);
    txn_info->set_commit_time(12345);
    txn_info->set_force_publish(false);
    reshard_request.add_resharding_tablet_infos()->CopyFrom(resharding_tablet_info);

    PublishVersionResponse reshard_response;
    _lake_service.publish_version(nullptr, &reshard_request, &reshard_response, nullptr);
    ASSERT_EQ(0, reshard_response.status().status_code());
    ASSERT_EQ(2, reshard_response.tablet_ranges_size());

    int64_t total_rows = 0;
    int64_t total_size = 0;
    for (auto new_tablet_id : new_tablet_ids) {
        ASSIGN_OR_ABORT(auto new_metadata, _tablet_mgr->get_tablet_metadata(new_tablet_id, metadata->version() + 1));
        ASSERT_EQ(1, new_metadata->rowsets_size());
        EXPECT_GT(new_metadata->rowsets(0).num_rows(), 0);
        EXPECT_GT(new_metadata->rowsets(0).data_size(), 0);
        total_rows += new_metadata->rowsets(0).num_rows();
        total_size += new_metadata->rowsets(0).data_size();
    }
    EXPECT_EQ(100, total_rows);
    EXPECT_EQ(1000, total_size);
}

TEST_F(LakeServiceTest, test_splitting_tablet_split_count_too_large_fallback) {
    auto txn_log = generate_write_txn_log_with_segments({0}, {100}, {100}, {100});
    ASSERT_OK(_tablet_mgr->put_txn_log(txn_log));

    PublishVersionRequest publish_request;
    publish_request.set_base_version(1);
    publish_request.set_new_version(2);
    publish_request.add_tablet_ids(_tablet_id);
    publish_request.add_txn_ids(txn_log.txn_id());

    PublishVersionResponse response;
    _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
    ASSERT_EQ(0, response.status().status_code());

    ReshardingTabletInfoPB resharding_tablet_info;
    auto* splitting_tablet_info = resharding_tablet_info.mutable_splitting_tablet_info();
    splitting_tablet_info->set_old_tablet_id(_tablet_id);
    std::vector<int64_t> new_tablet_ids{next_id(), next_id(), next_id()};
    for (auto new_tablet_id : new_tablet_ids) {
        splitting_tablet_info->add_new_tablet_ids(new_tablet_id);
    }

    PublishVersionRequest reshard_request;
    reshard_request.set_base_version(2);
    reshard_request.set_new_version(3);
    auto* txn_info = reshard_request.add_txn_infos();
    txn_info->set_txn_id(next_id());
    txn_info->set_txn_type(TXN_TABLET_RESHARD);
    txn_info->set_combined_txn_log(false);
    txn_info->set_commit_time(12345);
    txn_info->set_force_publish(false);
    reshard_request.add_resharding_tablet_infos()->CopyFrom(resharding_tablet_info);

    PublishVersionResponse reshard_response;
    _lake_service.publish_version(nullptr, &reshard_request, &reshard_response, nullptr);
    ASSERT_EQ(0, reshard_response.status().status_code());
    ASSERT_EQ(1, reshard_response.tablet_ranges_size());

    ASSIGN_OR_ABORT(auto metadata_first, _tablet_mgr->get_tablet_metadata(new_tablet_ids.front(), 3));
    EXPECT_EQ(new_tablet_ids.front(), metadata_first->id());
    auto metadata_second = _tablet_mgr->get_tablet_metadata(new_tablet_ids[1], 3);
    EXPECT_TRUE(metadata_second.status().is_not_found());
    auto metadata_third = _tablet_mgr->get_tablet_metadata(new_tablet_ids[2], 3);
    EXPECT_TRUE(metadata_third.status().is_not_found());
}

TEST_F(LakeServiceTest, test_publish_merging_tablet) {
    {
        auto txn_log = generate_write_txn_log(1, 100, 100);
        ASSERT_OK(_tablet_mgr->put_txn_log(txn_log));

        PublishVersionRequest publish_request;
        publish_request.set_base_version(1);
        publish_request.set_new_version(2);
        publish_request.add_tablet_ids(_tablet_id);
        auto* txn_info = publish_request.add_txn_infos();
        txn_info->set_txn_id(txn_log.txn_id());

        {
            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(0, response.failed_tablets_size());
            EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        }

        {
            publish_request.set_enable_aggregate_publish(true);
            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(0, response.failed_tablets_size());
            EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
        }
    }

    ReshardingTabletInfoPB resharding_tablet_info;
    auto* merging_tablet_info = resharding_tablet_info.mutable_merging_tablet_info();
    merging_tablet_info->add_old_tablet_ids(_tablet_id);
    merging_tablet_info->add_old_tablet_ids(_tablet_id);
    merging_tablet_info->set_new_tablet_id(next_id());

    {
        PublishVersionRequest publish_request;
        publish_request.set_base_version(2);
        publish_request.set_new_version(3);

        auto* txn_info = publish_request.add_txn_infos();
        txn_info->set_txn_id(next_id());
        txn_info->set_txn_type(TXN_TABLET_RESHARD);
        txn_info->set_combined_txn_log(false);
        txn_info->set_commit_time(12345);
        txn_info->set_force_publish(false);

        publish_request.add_resharding_tablet_infos()->CopyFrom(resharding_tablet_info);

        {
            SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:1", [](void* arg) { *(int64_t*)arg = 0; });
            SyncPoint::GetInstance()->EnableProcessing();
            DeferOp defer([]() {
                SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:1");
                SyncPoint::GetInstance()->DisableProcessing();
            });

            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(2, response.failed_tablets_size());
            EXPECT_NE(0, response.status().status_code()) << response.status().error_msgs(0);
        }

        {
            class MockRunnable : public Runnable {
            public:
                MockRunnable() {}
                virtual ~MockRunnable() override {}
                virtual void run() override {}
                virtual void cancel() override {}
            };

            SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:replace_task", [](void* arg) {
                auto ptr = (*(std::shared_ptr<Runnable>*)arg);
                ptr->cancel();
                (*(std::shared_ptr<Runnable>*)arg) = std::make_shared<MockRunnable>();
            });
            SyncPoint::GetInstance()->EnableProcessing();
            DeferOp defer([]() {
                SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:replace_task");
                SyncPoint::GetInstance()->DisableProcessing();
            });

            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(2, response.failed_tablets_size());
            EXPECT_NE(0, response.status().status_code()) << response.status().error_msgs(0);
        }

        // Failed due to not implemented
        {
            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(2, response.failed_tablets_size());
            EXPECT_NE(0, response.status().status_code());
        }

        {
            publish_request.set_enable_aggregate_publish(true);
            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(2, response.failed_tablets_size());
            EXPECT_NE(0, response.status().status_code());
        }
    }

    // Test cross publish
    {
        auto txn_log = generate_write_txn_log(1, 100, 100);
        ASSERT_OK(_tablet_mgr->put_txn_log(txn_log));

        PublishVersionRequest publish_request;
        publish_request.set_base_version(3);
        publish_request.set_new_version(4);
        publish_request.add_txn_ids(txn_log.txn_id());
        publish_request.add_resharding_tablet_infos()->CopyFrom(resharding_tablet_info);

        {
            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(2, response.failed_tablets_size());
            EXPECT_NE(0, response.status().status_code()) << response.status().error_msgs(0);
        }

        {
            publish_request.set_enable_aggregate_publish(true);
            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(2, response.failed_tablets_size());
            EXPECT_NE(0, response.status().status_code()) << response.status().error_msgs(0);
        }
    }
}

TEST_F(LakeServiceTest, test_publish_identical_tablet) {
    {
        auto txn_log = generate_write_txn_log(1, 100, 100);
        ASSERT_OK(_tablet_mgr->put_txn_log(txn_log));

        PublishVersionRequest publish_request;
        publish_request.set_base_version(1);
        publish_request.set_new_version(2);
        publish_request.add_tablet_ids(_tablet_id);
        publish_request.add_txn_ids(txn_log.txn_id());

        {
            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(0, response.failed_tablets_size());
            EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
            ASSERT_EQ(0, response.tablet_metas_size());
            ASSERT_EQ(0, response.tablet_ranges_size());
        }

        {
            publish_request.set_enable_aggregate_publish(true);
            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(0, response.failed_tablets_size());
            EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
            ASSERT_EQ(1, response.tablet_metas_size());
            ASSERT_EQ(0, response.tablet_ranges_size());
        }
    }

    ReshardingTabletInfoPB resharding_tablet_info;
    auto* identical_tablet_info = resharding_tablet_info.mutable_identical_tablet_info();
    identical_tablet_info->set_old_tablet_id(_tablet_id);
    identical_tablet_info->set_new_tablet_id(next_id());

    {
        PublishVersionRequest publish_request;
        publish_request.set_base_version(2);
        publish_request.set_new_version(3);

        auto* txn_info = publish_request.add_txn_infos();
        txn_info->set_txn_id(next_id());
        txn_info->set_txn_type(TXN_TABLET_RESHARD);
        txn_info->set_combined_txn_log(false);
        txn_info->set_commit_time(12345);
        txn_info->set_force_publish(false);

        publish_request.add_resharding_tablet_infos()->CopyFrom(resharding_tablet_info);

        {
            SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:1", [](void* arg) { *(int64_t*)arg = 0; });
            SyncPoint::GetInstance()->EnableProcessing();
            DeferOp defer([]() {
                SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:1");
                SyncPoint::GetInstance()->DisableProcessing();
            });

            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(1, response.failed_tablets_size());
            EXPECT_NE(0, response.status().status_code()) << response.status().error_msgs(0);
        }

        {
            class MockRunnable : public Runnable {
            public:
                MockRunnable() {}
                virtual ~MockRunnable() override {}
                virtual void run() override {}
                virtual void cancel() override {}
            };

            SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:replace_task", [](void* arg) {
                auto ptr = (*(std::shared_ptr<Runnable>*)arg);
                ptr->cancel();
                (*(std::shared_ptr<Runnable>*)arg) = std::make_shared<MockRunnable>();
            });
            SyncPoint::GetInstance()->EnableProcessing();
            DeferOp defer([]() {
                SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:replace_task");
                SyncPoint::GetInstance()->DisableProcessing();
            });

            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(1, response.failed_tablets_size());
            EXPECT_NE(0, response.status().status_code()) << response.status().error_msgs(0);
        }

        {
            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(0, response.failed_tablets_size());
            EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
            ASSERT_EQ(0, response.tablet_metas_size());
            ASSERT_EQ(0, response.tablet_ranges_size());
        }

        {
            publish_request.set_enable_aggregate_publish(true);
            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(0, response.failed_tablets_size());
            EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
            ASSERT_EQ(2, response.tablet_metas_size());
            ASSERT_EQ(0, response.tablet_ranges_size());
        }
    }

    // Test cross publish
    {
        auto txn_log = generate_write_txn_log(1, 100, 100);
        ASSERT_OK(_tablet_mgr->put_txn_log(txn_log));

        PublishVersionRequest publish_request;
        publish_request.set_base_version(3);
        publish_request.set_new_version(4);
        publish_request.add_txn_ids(txn_log.txn_id());
        publish_request.add_resharding_tablet_infos()->CopyFrom(resharding_tablet_info);

        {
            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(0, response.failed_tablets_size());
            EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
            ASSERT_EQ(0, response.tablet_metas_size());
            ASSERT_EQ(0, response.tablet_ranges_size());
        }

        {
            publish_request.set_enable_aggregate_publish(true);
            PublishVersionResponse response;
            _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
            ASSERT_EQ(0, response.failed_tablets_size());
            EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
            ASSERT_EQ(1, response.tablet_metas_size());
            ASSERT_EQ(0, response.tablet_ranges_size());
        }
    }
}

TEST_F(LakeServiceTest, test_abort) {
    std::vector<TxnLog> logs;

    // Empty TxnLog
    {
        auto txn_id = next_id();
        TxnLog log;
        log.set_tablet_id(_tablet_id);
        log.set_txn_id(txn_id);
        ASSERT_OK(_tablet_mgr->put_txn_log(log));

        logs.emplace_back(log);
    }

    // Write txn log
    {
        auto txn_id = next_id();
        TxnLog log;
        log.set_tablet_id(_tablet_id);
        log.set_txn_id(txn_id);
        log.mutable_op_write()->mutable_rowset()->add_segments(generate_segment_file(txn_id));
        log.mutable_op_write()->mutable_rowset()->add_segments(generate_segment_file(txn_id));
        log.mutable_op_write()->mutable_rowset()->set_data_size(4096);
        log.mutable_op_write()->mutable_rowset()->set_num_rows(101);
        log.mutable_op_write()->mutable_rowset()->set_overlapped(true);
        ASSERT_OK(_tablet_mgr->put_txn_log(log));

        logs.emplace_back(log);
    }
    // Compaction txn log
    {
        auto txn_id = next_id();
        TxnLog log;
        log.set_tablet_id(_tablet_id);
        log.set_txn_id(txn_id);
        log.mutable_op_compaction()->mutable_output_rowset()->set_overlapped(false);
        log.mutable_op_compaction()->mutable_output_rowset()->set_num_rows(101);
        log.mutable_op_compaction()->mutable_output_rowset()->set_data_size(4096);
        log.mutable_op_compaction()->mutable_output_rowset()->add_segments(generate_segment_file(txn_id));
        log.mutable_op_compaction()->mutable_output_rowset()->add_segments(generate_segment_file(txn_id));
        log.mutable_op_compaction()->set_new_segment_offset(0);
        log.mutable_op_compaction()->set_new_segment_count(2);
        ASSERT_OK(_tablet_mgr->put_txn_log(log));

        logs.emplace_back(log);
    }
    // Schema change txn log
    {
        auto txn_id = next_id();
        TxnLog log;
        log.set_tablet_id(_tablet_id);
        log.set_txn_id(txn_id);
        log.mutable_op_schema_change()->add_rowsets()->add_segments(generate_segment_file(txn_id));
        log.mutable_op_schema_change()->add_rowsets()->add_segments(generate_segment_file(txn_id));
        ASSERT_OK(_tablet_mgr->put_txn_log(log));

        logs.emplace_back(log);
    }

    AbortTxnRequest request;
    request.add_tablet_ids(_tablet_id);
    request.set_skip_cleanup(false);
    for (auto&& log : logs) {
        request.add_txn_ids(log.txn_id());
    }

    {
        TEST_ENABLE_ERROR_POINT("TabletManager::load_txn_log", Status::IOError("injected get txn log error"));
        SyncPoint::GetInstance()->EnableProcessing();

        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("TabletManager::load_txn_log");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        AbortTxnResponse response;
        _lake_service.abort_txn(nullptr, &request, &response, nullptr);
    }
    {
        AbortTxnResponse response;
        _lake_service.abort_txn(nullptr, &request, &response, nullptr);
    }

    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();

    // TxnLog`s and segments should have been deleted
    for (auto&& log : logs) {
        for (auto&& s : log.op_write().rowset().segments()) {
            EXPECT_FALSE(fs::path_exist(_tablet_mgr->segment_location(_tablet_id, s)));
        }
        for (auto&& s : log.op_compaction().output_rowset().segments()) {
            EXPECT_FALSE(fs::path_exist(_tablet_mgr->segment_location(_tablet_id, s)));
        }
        for (auto&& r : log.op_schema_change().rowsets()) {
            for (auto&& s : r.segments()) {
                EXPECT_FALSE(fs::path_exist(_tablet_mgr->segment_location(_tablet_id, s)));
            }
        }
        EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_log_location(_tablet_id, log.txn_id())));
    }

    // Send AbortTxn request again
    {
        AbortTxnResponse response;
        _lake_service.abort_txn(nullptr, &request, &response, nullptr);
    }
    // Thread pool is full
    {
        SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:1", [](void* arg) { *(int64_t*)arg = 0; });
        SyncPoint::GetInstance()->EnableProcessing();

        DeferOp defer([]() {
            SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:1");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        AbortTxnResponse response;
        _lake_service.abort_txn(nullptr, &request, &response, nullptr);
    }
}

TEST_F(LakeServiceTest, test_delete_tablet) {
    brpc::Controller cntl;
    DeleteTabletRequest request;
    DeleteTabletResponse response;
    request.add_tablet_ids(_tablet_id);
    _lake_service.delete_tablet(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(0, response.failed_tablets_size());
    EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
}

TEST_F(LakeServiceTest, test_delete_txn_log) {
    // missing tablet_ids
    {
        brpc::Controller cntl;
        DeleteTxnLogRequest request;
        DeleteTxnLogResponse response;
        _lake_service.delete_txn_log(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("missing tablet_ids", cntl.ErrorText());
    }

    // missing txn_ids
    {
        brpc::Controller cntl;
        DeleteTxnLogRequest request;
        DeleteTxnLogResponse response;
        request.add_tablet_ids(_tablet_id);
        _lake_service.delete_txn_log(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("neither txn_ids nor txn_infos is set, one of them must be set", cntl.ErrorText());
    }

    // test normal
    {
        std::vector<TxnLog> logs;

        // TxnLog with 2 segments
        logs.emplace_back(generate_write_txn_log(2, 101, 4096));
        ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));

        brpc::Controller cntl;
        DeleteTxnLogRequest request;
        DeleteTxnLogResponse response;
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(logs.back().txn_id());
        _lake_service.delete_txn_log(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        auto path = _tablet_mgr->txn_log_location(_tablet_id, logs.back().txn_id());
        ASSERT_EQ(TStatusCode::NOT_FOUND, FileSystem::Default()->path_exists(path).code());
    }
    // test delete txn log with new API
    {
        std::vector<TxnLog> logs;

        logs.emplace_back(generate_write_txn_log(2, 101, 4096));
        ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));

        brpc::Controller cntl;
        DeleteTxnLogRequest request;
        DeleteTxnLogResponse response;
        request.add_tablet_ids(_tablet_id);
        auto info = request.add_txn_infos();
        info->set_txn_id(logs.back().txn_id());
        info->set_combined_txn_log(false);
        _lake_service.delete_txn_log(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        auto path = _tablet_mgr->txn_log_location(_tablet_id, logs.back().txn_id());
        ASSERT_EQ(TStatusCode::NOT_FOUND, FileSystem::Default()->path_exists(path).code());
    }
    // test delete combined txn log
    {
        CombinedTxnLogPB combined_txn_log_pb;
        combined_txn_log_pb.add_txn_logs()->CopyFrom(generate_write_txn_log(2, 101, 4096));
        ASSERT_OK(_tablet_mgr->put_combined_txn_log(combined_txn_log_pb));
        auto txn_id = combined_txn_log_pb.txn_logs(0).txn_id();

        brpc::Controller cntl;
        DeleteTxnLogRequest request;
        DeleteTxnLogResponse response;
        request.add_tablet_ids(_tablet_id);
        auto info = request.add_txn_infos();
        info->set_txn_id(txn_id);
        info->set_combined_txn_log(true);
        _lake_service.delete_txn_log(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        auto log_path = _tablet_mgr->combined_txn_log_location(_tablet_id, txn_id);
        ASSERT_TRUE(FileSystem::Default()->path_exists(log_path).is_not_found());
    }
}

TEST_F(LakeServiceTest, test_delete_tablet_dir_not_exit) {
    ASSERT_OK(fs::remove_all(kRootLocation));
    brpc::Controller cntl;
    DeleteTabletRequest request;
    DeleteTabletResponse response;
    request.add_tablet_ids(_tablet_id);
    _lake_service.delete_tablet(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(0, response.failed_tablets_size());
    EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
    // restore test directory
    ASSERT_OK(fs::create_directories(kRootLocation));
}

TEST_F(LakeServiceTest, test_compact) {
    auto compact = [this](::google::protobuf::RpcController* cntl, const CompactRequest* request,
                          CompactResponse* response) {
        CountDownLatch latch(1);
        auto cb = ::google::protobuf::NewCallback(&latch, &CountDownLatch::count_down);
        _lake_service.compact(cntl, request, response, cb);
        latch.wait();
    };

    auto txn_id = next_id();
    // missing tablet_ids
    {
        brpc::Controller cntl;
        CompactRequest request;
        CompactResponse response;
        // request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        request.set_version(1);
        compact(&cntl, &request, &response);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("missing tablet_ids", cntl.ErrorText());
    }
    // missing txn_id
    {
        brpc::Controller cntl;
        CompactRequest request;
        CompactResponse response;
        request.add_tablet_ids(_tablet_id);
        //request.set_txn_id(txn_id);
        request.set_version(1);
        compact(&cntl, &request, &response);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("missing txn_id", cntl.ErrorText());
    }
    // missing version
    {
        brpc::Controller cntl;
        CompactRequest request;
        CompactResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        //request.set_version(1);
        compact(&cntl, &request, &response);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("missing version", cntl.ErrorText());
    }
    // tablet not exist
    {
        brpc::Controller cntl;
        CompactRequest request;
        CompactResponse response;
        request.add_tablet_ids(_tablet_id + 1);
        request.set_txn_id(txn_id);
        request.set_version(1);
        compact(&cntl, &request, &response);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id + 1, response.failed_tablets(0));
    }
    // compact
    {
        brpc::Controller cntl;
        CompactRequest request;
        CompactResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        request.set_version(1);
        compact(&cntl, &request, &response);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());
    }
    // publish version
    {
        brpc::Controller cntl;
        PublishVersionRequest request;
        PublishVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(txn_id);
        request.set_base_version(1);
        request.set_new_version(2);
        _lake_service.publish_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());
        ASSERT_TRUE(response.compaction_scores().contains(_tablet_id));
    }
}

TEST_F(LakeServiceTest, test_aggregate_compact_failed) {
    auto txn_id = next_id();

    // empty requests
    {
        brpc::Controller cntl;
        AggregateCompactRequest agg_request;
        CompactResponse response;
        run_aggregate_compact(&cntl, &agg_request, &response);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("empty requests", cntl.ErrorText());
    }

    // compute nodes size not equal to requests size
    {
        brpc::Controller cntl;
        AggregateCompactRequest agg_request;
        CompactRequest request;
        CompactResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        request.set_version(1);
        agg_request.add_requests()->CopyFrom(request);
        run_aggregate_compact(&cntl, &agg_request, &response);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("compute nodes size not equal to requests size", cntl.ErrorText());
    }

    // compute node missing host/port
    {
        brpc::Controller cntl;
        AggregateCompactRequest agg_request;
        CompactRequest request;
        ComputeNodePB cn;
        cn.set_id(1);
        CompactResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        request.set_version(1);
        agg_request.add_requests()->CopyFrom(request);
        agg_request.add_compute_nodes()->CopyFrom(cn);
        run_aggregate_compact(&cntl, &agg_request, &response);
        ASSERT_EQ("compute node missing host/port", response.status().error_msgs(0));
    }

    // get stub failed
    {
        brpc::Controller cntl;
        AggregateCompactRequest agg_request;
        CompactRequest request;
        ComputeNodePB cn;
        cn.set_id(1);
        cn.set_host("invalid.host");
        cn.set_brpc_port(123);
        CompactResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        request.set_version(1);
        agg_request.add_requests()->CopyFrom(request);
        agg_request.add_compute_nodes()->CopyFrom(cn);
        run_aggregate_compact(&cntl, &agg_request, &response);
        ASSERT_TRUE(response.status().status_code() != 0);
    }
}

TEST_F(LakeServiceTest, test_aggregate_compact_success) {
    brpc::ServerOptions options;
    options.num_threads = 1;
    brpc::Server server;
    MockLakeServiceImpl mock_service;
    ASSERT_EQ(server.AddService(&mock_service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server.Start(0, &options), 0);

    butil::EndPoint server_addr = server.listen_address();
    const int port = server_addr.port;
    EXPECT_CALL(mock_service, compact(_, _, _, _))
            .WillRepeatedly(Invoke([&](::google::protobuf::RpcController*, const CompactRequest*, CompactResponse* resp,
                                       ::google::protobuf::Closure* done) {
                TxnLogPB txnlog;
                txnlog.set_tablet_id(100);
                txnlog.set_txn_id(100);
                resp->add_txn_logs()->CopyFrom(txnlog);
                TxnLogPB txnlog2;
                txnlog2.set_tablet_id(101);
                txnlog2.set_txn_id(100);
                resp->add_txn_logs()->CopyFrom(txnlog2);
                resp->add_compact_stats();
                resp->mutable_status()->set_status_code(0);
                done->Run();
            }));

    auto txn_id = next_id();

    // compact success - single cn
    {
        brpc::Controller cntl;
        AggregateCompactRequest agg_request;
        CompactRequest request;
        ComputeNodePB cn;
        cn.set_host("127.0.0.1");
        cn.set_brpc_port(port);
        cn.set_id(1);
        CompactResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        request.set_version(1);
        request.set_timeout_ms(3000);
        agg_request.add_requests()->CopyFrom(request);
        agg_request.add_compute_nodes()->CopyFrom(cn);
        agg_request.set_partition_id(99);
        run_aggregate_compact(&cntl, &agg_request, &response);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());
    }

    // compact success - 3 cn
    {
        brpc::Controller cntl;
        AggregateCompactRequest agg_request;
        for (int i = 1; i <= 3; i++) {
            CompactRequest request;
            ComputeNodePB cn;
            cn.set_host("127.0.0." + std::to_string(i));
            cn.set_brpc_port(port);
            cn.set_id(i);
            request.add_tablet_ids(_tablet_id);
            request.set_txn_id(txn_id);
            request.set_version(1);
            request.set_timeout_ms(3000);
            agg_request.add_requests()->CopyFrom(request);
            agg_request.add_compute_nodes()->CopyFrom(cn);
            agg_request.set_partition_id(99);
        }
        CompactResponse response;
        run_aggregate_compact(&cntl, &agg_request, &response);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());
    }

    server.Stop(0);
    server.Join();
}

TEST_F(LakeServiceTest, test_aggregate_compact_with_error) {
    brpc::Server server;
    MockLakeServiceImpl mock_service;
    int port = 0;
    init_server_with_mock(&mock_service, &server, &port);

    EXPECT_CALL(mock_service, compact(_, _, _, _))
            .WillRepeatedly(Invoke([&](::google::protobuf::RpcController*, const CompactRequest*, CompactResponse* resp,
                                       ::google::protobuf::Closure* done) {
                resp->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
                resp->mutable_status()->add_error_msgs("injected error");
                done->Run();
            }));

    auto txn_id = next_id();
    brpc::Controller cntl;
    AggregateCompactRequest agg_request;
    CompactRequest request;
    ComputeNodePB cn;
    cn.set_host("127.0.0.1");
    cn.set_brpc_port(port);
    cn.set_id(1);
    CompactResponse response;
    request.add_tablet_ids(_tablet_id);
    request.set_txn_id(txn_id);
    request.set_version(1);
    request.set_timeout_ms(3000);
    agg_request.add_requests()->CopyFrom(request);
    agg_request.add_compute_nodes()->CopyFrom(cn);

    run_aggregate_compact(&cntl, &agg_request, &response);

    ASSERT_FALSE(cntl.Failed());
    ASSERT_EQ(TStatusCode::INTERNAL_ERROR, response.status().status_code());
    ASSERT_EQ(1, response.status().error_msgs_size());
    ASSERT_EQ("injected error", response.status().error_msgs(0));

    server.Stop(0);
    server.Join();
}

TEST_F(LakeServiceTest, test_drop_table) {
    ASSERT_OK(FileSystem::Default()->path_exists(kRootLocation));
    DropTableRequest request;
    DropTableResponse response;

    brpc::Controller cntl;
    _lake_service.drop_table(&cntl, &request, &response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ("missing tablet_id", cntl.ErrorText());

    cntl.Reset();
    request.set_tablet_id(_tablet_id);
    _lake_service.drop_table(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed());
    ASSERT_TRUE(response.has_status());
    ASSERT_EQ(0, response.status().status_code());

    auto st = FileSystem::Default()->path_exists(kRootLocation);
    ASSERT_TRUE(st.is_not_found()) << st;

    _lake_service.drop_table(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed());
    ASSERT_TRUE(response.has_status());
    ASSERT_EQ(0, response.status().status_code());
}

TEST_F(LakeServiceTest, test_publish_log_version) {
    auto txn_id = next_id();
    {
        TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(txn_id);
        txnlog.mutable_op_write()->mutable_rowset()->set_overlapped(true);
        txnlog.mutable_op_write()->mutable_rowset()->set_num_rows(101);
        txnlog.mutable_op_write()->mutable_rowset()->set_data_size(4096);
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("1.dat");
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("2.dat");
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog));
    }
    {
        PublishLogVersionRequest request;
        PublishLogVersionResponse response;
        brpc::Controller cntl;
        _lake_service.publish_log_version(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("missing tablet_ids", cntl.ErrorText());
    }
    {
        PublishLogVersionRequest request;
        PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        brpc::Controller cntl;
        _lake_service.publish_log_version(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("missing txn_id and txn_info", cntl.ErrorText());
    }
    {
        PublishLogVersionRequest request;
        PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        brpc::Controller cntl;
        _lake_service.publish_log_version(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("missing version", cntl.ErrorText());
    }
    for (auto inject_error : {Status::InternalError("injected"), Status::NotFound("injected")}) {
        std::cerr << "Injected error: " << inject_error << '\n';
        TEST_ENABLE_ERROR_POINT("fs::copy_file", inject_error);
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("fs::copy_file");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        PublishLogVersionRequest request;
        PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        request.set_version(10);
        brpc::Controller cntl;
        _lake_service.publish_log_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_log_location(_tablet_id, txn_id)));
        EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_vlog_location(_tablet_id, 10)));
    }
    {
        PublishLogVersionRequest request;
        PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        request.set_version(10);
        brpc::Controller cntl;
        _lake_service.publish_log_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_log_location(_tablet_id, txn_id)));
        EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_vlog_location(_tablet_id, 10)));
    }
    // duplicate request
    {
        PublishLogVersionRequest request;
        PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        request.set_version(10);
        brpc::Controller cntl;
        _lake_service.publish_log_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_vlog_location(_tablet_id, 10)));
    }
    // Publish combined txn log
    {
        auto partition_id = next_id();
        txn_id = next_id();
        std::vector<int64_t> tablet_ids{next_id(), next_id(), next_id()};
        CombinedTxnLogPB combined_txn_log;
        for (auto tablet_id : tablet_ids) {
            auto* log = combined_txn_log.add_txn_logs();
            log->set_partition_id(partition_id);
            log->set_tablet_id(tablet_id);
            log->set_txn_id(txn_id);
            log->mutable_op_write()->mutable_rowset()->set_overlapped(true);
            log->mutable_op_write()->mutable_rowset()->set_num_rows(0);
            log->mutable_op_write()->mutable_rowset()->set_data_size(0);
        }
        ASSERT_OK(_tablet_mgr->put_combined_txn_log(combined_txn_log));

        int64_t version = 12;
        PublishLogVersionRequest request;
        PublishLogVersionResponse response;
        for (auto tablet_id : tablet_ids) {
            request.add_tablet_ids(tablet_id);
        }
        request.set_version(version);
        auto* txn_info = request.mutable_txn_info();
        txn_info->set_txn_id(txn_id);
        txn_info->set_combined_txn_log(true);
        txn_info->set_txn_type(TXN_NORMAL);
        txn_info->set_commit_time(::time(nullptr));
        brpc::Controller cntl;
        _lake_service.publish_log_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        for (auto tablet_id : tablet_ids) {
            EXPECT_TRUE(fs::path_exist(_tablet_mgr->combined_txn_log_location(tablet_id, txn_id)));
            EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_vlog_location(tablet_id, version)));
        }
    }
}

TEST_F(LakeServiceTest, test_publish_log_version_batch) {
    {
        TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(1001);
        txnlog.mutable_op_write()->mutable_rowset()->set_overlapped(true);
        txnlog.mutable_op_write()->mutable_rowset()->set_num_rows(101);
        txnlog.mutable_op_write()->mutable_rowset()->set_data_size(4096);
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("1.dat");
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("2.dat");
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog));

        TxnLog txnlog2;
        txnlog2.set_tablet_id(_tablet_id);
        txnlog2.set_txn_id(1002);
        txnlog2.mutable_op_write()->mutable_rowset()->set_overlapped(true);
        txnlog2.mutable_op_write()->mutable_rowset()->set_num_rows(101);
        txnlog2.mutable_op_write()->mutable_rowset()->set_data_size(4096);
        txnlog2.mutable_op_write()->mutable_rowset()->add_segments("3.dat");
        txnlog2.mutable_op_write()->mutable_rowset()->add_segments("4.dat");
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog2));
    }
    {
        PublishLogVersionBatchRequest request;
        PublishLogVersionResponse response;
        brpc::Controller cntl;
        _lake_service.publish_log_version_batch(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("missing tablet_ids", cntl.ErrorText());
    }
    {
        PublishLogVersionBatchRequest request;
        PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        brpc::Controller cntl;
        _lake_service.publish_log_version_batch(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("neither txn_ids nor txn_infos is set, one of them must be set", cntl.ErrorText());
    }
    {
        PublishLogVersionBatchRequest request;
        PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1001);
        brpc::Controller cntl;
        _lake_service.publish_log_version_batch(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("missing versions", cntl.ErrorText());
    }
    {
        PublishLogVersionBatchRequest request;
        PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1001);
        request.add_txn_ids(1002);
        request.add_versions(10);
        request.add_versions(11);
        brpc::Controller cntl;
        _lake_service.publish_log_version_batch(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());
        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();

        _tablet_mgr->prune_metacache();
        ASSERT_TRUE(_tablet_mgr->get_txn_log(_tablet_id, 1001).status().is_not_found())
                << _tablet_mgr->get_txn_log(_tablet_id, 1001).status();
        ASSERT_TRUE(_tablet_mgr->get_txn_log(_tablet_id, 1002).status().is_not_found())
                << _tablet_mgr->get_txn_log(_tablet_id, 1002).status();

        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_vlog(_tablet_id, 10));
        ASSERT_EQ(_tablet_id, txn_log->tablet_id());
        ASSERT_EQ(1001, txn_log->txn_id());

        ASSIGN_OR_ABORT(auto txn_log2, _tablet_mgr->get_txn_vlog(_tablet_id, 11));
        ASSERT_EQ(_tablet_id, txn_log2->tablet_id());
        ASSERT_EQ(1002, txn_log2->txn_id());
    }
    // duplicate request
    {
        PublishLogVersionBatchRequest request;
        PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1001);
        request.add_txn_ids(1002);
        request.add_versions(10);
        request.add_versions(11);
        brpc::Controller cntl;
        _lake_service.publish_log_version_batch(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());
        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();

        _tablet_mgr->prune_metacache();
        ASSERT_TRUE(_tablet_mgr->get_txn_log(_tablet_id, 1001).status().is_not_found())
                << _tablet_mgr->get_txn_log(_tablet_id, 1001).status();

        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_vlog(_tablet_id, 10));
        ASSERT_EQ(_tablet_id, txn_log->tablet_id());
        ASSERT_EQ(1001, txn_log->txn_id());

        ASSERT_TRUE(_tablet_mgr->get_txn_log(_tablet_id, 1002).status().is_not_found())
                << _tablet_mgr->get_txn_log(_tablet_id, 1002).status();

        ASSIGN_OR_ABORT(auto txn_log2, _tablet_mgr->get_txn_vlog(_tablet_id, 11));
        ASSERT_EQ(_tablet_id, txn_log2->tablet_id());
        ASSERT_EQ(1002, txn_log2->txn_id());
    }

    // not existing txnId
    {
        PublishLogVersionBatchRequest request;
        PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1111);
        brpc::Controller cntl;
        _lake_service.publish_log_version_batch(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
    }
    // Publish combined txn log
    {
        auto partition_id = next_id();
        std::vector<int64_t> txn_ids{next_id(), next_id(), next_id()};
        std::vector<int64_t> tablet_ids{next_id(), next_id(), next_id()};
        // prepare combined logs
        for (auto txn_id : txn_ids) {
            CombinedTxnLogPB combined_txn_log;
            for (auto tablet_id : tablet_ids) {
                auto* log = combined_txn_log.add_txn_logs();
                log->set_partition_id(partition_id);
                log->set_tablet_id(tablet_id);
                log->set_txn_id(txn_id);
                log->mutable_op_write()->mutable_rowset()->set_overlapped(true);
                log->mutable_op_write()->mutable_rowset()->set_num_rows(0);
                log->mutable_op_write()->mutable_rowset()->set_data_size(0);
            }
            ASSERT_OK(_tablet_mgr->put_combined_txn_log(combined_txn_log));
        }

        std::vector<int64_t> versions = {12, 13, 14};
        PublishLogVersionBatchRequest request;
        PublishLogVersionResponse response;
        for (auto tablet_id : tablet_ids) {
            request.add_tablet_ids(tablet_id);
        }
        for (auto version : versions) {
            request.add_versions(version);
        }
        for (auto txn_id : txn_ids) {
            auto* txn_info = request.add_txn_infos();
            txn_info->set_txn_id(txn_id);
            txn_info->set_combined_txn_log(true);
            txn_info->set_txn_type(TXN_NORMAL);
            txn_info->set_commit_time(::time(nullptr));
        }
        brpc::Controller cntl;
        _lake_service.publish_log_version_batch(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        for (auto txn_id : txn_ids) {
            for (auto tablet_id : tablet_ids) {
                EXPECT_TRUE(fs::path_exist(_tablet_mgr->combined_txn_log_location(tablet_id, txn_id)));
                for (auto version : versions) {
                    EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_vlog_location(tablet_id, version)));
                }
            }
        }
    }
}

TEST_F(LakeServiceTest, test_publish_version_empty_txn_log) {
    // Publish EMPTY_TXN_LOG
    {
        PublishVersionRequest request;
        PublishVersionResponse response;
        request.set_base_version(1);
        request.set_new_version(2);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(-1);
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
    }

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_id));
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(2));
    ASSERT_EQ(2, metadata->version());
    ASSERT_EQ(_tablet_id, metadata->id());
}

TEST_F(LakeServiceTest, test_publish_version_for_schema_change) {
    // write 1 rowset when schema change
    {
        TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(1000);
        txnlog.mutable_op_write()->mutable_rowset()->set_overlapped(false);
        txnlog.mutable_op_write()->mutable_rowset()->set_num_rows(4);
        txnlog.mutable_op_write()->mutable_rowset()->set_data_size(14);
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("4.dat");
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("5.dat");
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("6.dat");
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog));

        PublishLogVersionRequest request;
        PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(1000);
        request.set_version(4);
        brpc::Controller cntl;
        _lake_service.publish_log_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());
    }

    // schema change with 2 rowsets
    {
        TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(1001);
        auto op_schema_change = txnlog.mutable_op_schema_change();
        op_schema_change->set_alter_version(3);
        auto rowset0 = op_schema_change->add_rowsets();
        rowset0->set_id(1);
        rowset0->set_overlapped(true);
        rowset0->set_num_rows(2);
        rowset0->set_data_size(12);
        rowset0->add_segments("1.dat");
        rowset0->add_segments("2.dat");
        auto rowset1 = op_schema_change->add_rowsets();
        rowset1->set_id(3);
        rowset1->set_overlapped(false);
        rowset1->set_num_rows(3);
        rowset1->set_data_size(13);
        rowset1->add_segments("3.dat");
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog));
    }

    PublishVersionRequest request;
    request.set_base_version(1);
    request.set_new_version(5);
    request.add_tablet_ids(_tablet_id);
    request.add_txn_ids(1001);

    // fail to get txn vlog
    {
        TEST_ENABLE_ERROR_POINT("TabletManager::get_txn_vlog", Status::InternalError("injected internal error"));
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("TabletManager::get_txn_vlog");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        PublishVersionResponse response;
        brpc::Controller cntl;
        _lake_service.publish_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
    }
    // txn vlog does not exit
    {
        TEST_ENABLE_ERROR_POINT("TabletManager::get_txn_vlog", Status::NotFound("injected not found"));
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("TabletManager::get_txn_vlog");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        PublishVersionResponse response;
        brpc::Controller cntl;
        _lake_service.publish_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
    }
    // apply schema change log failed
    {
        TEST_ENABLE_ERROR_POINT("NonPrimaryKeyTxnLogApplier::apply_schema_change_log",
                                Status::InternalError("injected apply error"));
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("NonPrimaryKeyTxnLogApplier::apply_schema_change_log");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        PublishVersionResponse response;
        brpc::Controller cntl;
        _lake_service.publish_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
    }
    // apply write log failed
    {
        TEST_ENABLE_ERROR_POINT("NonPrimaryKeyTxnLogApplier::apply_write_log",
                                Status::InternalError("injected apply error"));
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("NonPrimaryKeyTxnLogApplier::apply_write_log");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        PublishVersionResponse response;
        brpc::Controller cntl;
        _lake_service.publish_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
    }
    // apply success
    {
        PublishVersionResponse response;
        brpc::Controller cntl;
        _lake_service.publish_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());
        ASSERT_TRUE(response.compaction_scores().contains(_tablet_id));
    }
    _tablet_mgr->prune_metacache();
    // publish again
    {
        PublishVersionResponse response;
        brpc::Controller cntl;
        _lake_service.publish_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());
        ASSERT_TRUE(response.compaction_scores().contains(_tablet_id));
    }
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_id));
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(5));
    ASSERT_EQ(5, metadata->version());
    ASSERT_EQ(3, metadata->rowsets().size());
    const auto& rowset0 = metadata->rowsets(0);
    ASSERT_TRUE(rowset0.overlapped());
    ASSERT_EQ(2, rowset0.num_rows());
    ASSERT_EQ(12, rowset0.data_size());
    ASSERT_EQ(2, rowset0.segments_size());
    const auto& rowset1 = metadata->rowsets(1);
    ASSERT_FALSE(rowset1.overlapped());
    ASSERT_EQ(3, rowset1.num_rows());
    ASSERT_EQ(13, rowset1.data_size());
    ASSERT_EQ(1, rowset1.segments_size());
    const auto& rowset2 = metadata->rowsets(2);
    ASSERT_FALSE(rowset2.overlapped());
    ASSERT_EQ(4, rowset2.num_rows());
    ASSERT_EQ(14, rowset2.data_size());
    ASSERT_EQ(3, rowset2.segments_size());

    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
    EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_log_location(_tablet_id, 1000)));
    EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_log_location(_tablet_id, 1001)));
    EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_vlog_location(_tablet_id, 4)));
}

TEST_F(LakeServiceTest, test_abort_compaction) {
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->LoadDependency(
            {{"CompactionScheduler::compact:return", "LakeServiceImpl::abort_compaction:enter"},
             {"LakeServiceImpl::abort_compaction:aborted", "CompactionScheduler::do_compaction:before_execute_task"}});

    DeferOp defer([]() { SyncPoint::GetInstance()->DisableProcessing(); });

    auto txn_id = next_id();

    auto compaction_thread = std::thread([&]() {
        brpc::Controller cntl;
        CompactRequest request;
        CompactResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        request.set_version(1);
        CountDownLatch latch(1);
        auto cb = ::google::protobuf::NewCallback(&latch, &CountDownLatch::count_down);
        _lake_service.compact(&cntl, &request, &response, cb);
        latch.wait();
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(TStatusCode::ABORTED, response.status().status_code());
    });

    {
        brpc::Controller cntl;
        AbortCompactionRequest request;
        AbortCompactionResponse response;
        request.set_txn_id(txn_id);
        _lake_service.abort_compaction(&cntl, &request, &response, nullptr);
        ASSERT_EQ(TStatusCode::OK, response.status().status_code());
    }

    compaction_thread.join();

    {
        brpc::Controller cntl;
        AbortCompactionRequest request;
        AbortCompactionResponse response;
        request.set_txn_id(txn_id);
        _lake_service.abort_compaction(&cntl, &request, &response, nullptr);
        ASSERT_EQ(TStatusCode::NOT_FOUND, response.status().status_code());
    }
}

// https://github.com/StarRocks/starrocks/issues/28244
TEST_F(LakeServiceTest, test_publish_version_issue28244) {
    {
        TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(102301);
        txnlog.mutable_op_write()->mutable_rowset()->set_overlapped(true);
        txnlog.mutable_op_write()->mutable_rowset()->set_num_rows(101);
        txnlog.mutable_op_write()->mutable_rowset()->set_data_size(4096);
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("xxxxx.dat");
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog));
    }

    SyncPoint::GetInstance()->SetCallBack("publish_version:delete_txn_log",
                                          [](void* st) { *(Status*)st = Status::InternalError("injected"); });
    SyncPoint::GetInstance()->LoadDependency(
            {{"LakeServiceImpl::publish_version:return", "publish_version:delete_txn_log"}});
    SyncPoint::GetInstance()->EnableProcessing();

    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("publish_version:delete_txn_log");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    {
        PublishVersionRequest request;
        PublishVersionResponse response;
        request.set_base_version(1);
        request.set_new_version(2);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(102301);
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
    }

    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
    ASSERT_TRUE(_tablet_mgr->get_txn_log(_tablet_id, 102301).status().is_not_found());
}

TEST_F(LakeServiceTest, test_get_tablet_stats) {
    TabletStatRequest request;
    TabletStatResponse response;
    auto* info = request.add_tablet_infos();
    info->set_tablet_id(_tablet_id);
    info->set_version(1);

    // Prune metadata cache before getting tablet stats
    _tablet_mgr->metacache()->prune();

    _lake_service.get_tablet_stats(nullptr, &request, &response, nullptr);
    ASSERT_EQ(1, response.tablet_stats_size());
    ASSERT_EQ(_tablet_id, response.tablet_stats(0).tablet_id());
    ASSERT_EQ(0, response.tablet_stats(0).num_rows());
    ASSERT_EQ(0, response.tablet_stats(0).data_size());

    // Write some data into the tablet, num_rows = 1024, data_size=65536
    size_t expected_num_rows = 1024;
    size_t expected_data_size = 65536;
    auto txn_log = generate_write_txn_log(2, expected_num_rows, expected_data_size);
    ASSERT_OK(_tablet_mgr->put_txn_log(txn_log));

    { // Publish version request
        PublishVersionRequest request;
        request.set_base_version(1);
        request.set_new_version(3);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(txn_log.txn_id());
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_id));
        // Publish txn batch
        PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
    }

    { // get the tablet stat again
        TabletStatRequest request;
        TabletStatResponse response;
        auto* info = request.add_tablet_infos();
        info->set_tablet_id(_tablet_id);
        info->set_version(3);
        _lake_service.get_tablet_stats(nullptr, &request, &response, nullptr);
        EXPECT_EQ(1, response.tablet_stats_size());
        EXPECT_EQ(_tablet_id, response.tablet_stats(0).tablet_id());
        EXPECT_EQ(expected_num_rows, response.tablet_stats(0).num_rows());
        EXPECT_EQ(expected_data_size, response.tablet_stats(0).data_size());
    }

    // get_tablet_stats() should not fill metadata cache
    auto cache_key = _tablet_mgr->tablet_metadata_location(_tablet_id, 1);
    ASSERT_TRUE(_tablet_mgr->metacache()->lookup_tablet_metadata(cache_key) == nullptr);

    // test timeout
    response.clear_tablet_stats();
    request.set_timeout_ms(5);

    SyncPoint::GetInstance()->SetCallBack("LakeServiceImpl::get_tablet_stats:before_submit",
                                          [](void*) { std::this_thread::sleep_for(std::chrono::milliseconds(10)); });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([&]() {
        SyncPoint::GetInstance()->ClearCallBack("LakeServiceImpl::get_tablet_stats:before_submit");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    _lake_service.get_tablet_stats(nullptr, &request, &response, nullptr);
    ASSERT_EQ(0, response.tablet_stats_size());
}

TEST_F(LakeServiceTest, test_drop_table_no_thread_pool) {
    ASSERT_OK(FileSystem::Default()->path_exists(kRootLocation));

    SyncPoint::GetInstance()->SetCallBack("AgentServer::Impl::get_thread_pool:1",
                                          [](void* arg) { *(ThreadPool**)arg = nullptr; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("AgentServer::Impl::get_thread_pool:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    DropTableRequest request;
    DropTableResponse response;
    request.set_tablet_id(_tablet_id);
    brpc::Controller cntl;
    _lake_service.drop_table(&cntl, &request, &response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ("no thread pool to run task", cntl.ErrorText());
}

TEST_F(LakeServiceTest, test_drop_table_duplicate_request) {
    ASSERT_OK(FileSystem::Default()->path_exists(kRootLocation));
    SyncPoint::GetInstance()->LoadDependency(
            {{"LakeService::drop_table:duplicate_path_id", "LakeService::drop_table:task_run"}});
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() { SyncPoint::GetInstance()->DisableProcessing(); });

    auto path = "/path/for/test/drop/table";

    bthread_t tids[2];
    Status result_status[2];
    for (int i = 0; i < 2; i++) {
        ASSIGN_OR_ABORT(tids[i], bthreads::start_bthread([&, id = i]() {
                            DropTableRequest request;
                            DropTableResponse response;
                            request.set_tablet_id(100);
                            request.set_path(path);
                            brpc::Controller cntl;
                            _lake_service.drop_table(&cntl, &request, &response, nullptr);
                            result_status[id] = Status(response.status());
                        }));
    }
    bthread_join(tids[0], nullptr);
    bthread_join(tids[1], nullptr);
    if (result_status[0].ok()) {
        ASSERT_TRUE(result_status[1].is_already_exist()) << result_status[1];
    } else if (result_status[1].ok()) {
        ASSERT_TRUE(result_status[0].is_already_exist()) << result_status[0];
    } else {
        FAIL() << "All tasks failed. " << result_status[0] << " : " << result_status[1];
    }
}

TEST_F(LakeServiceTest, test_delete_tablet_no_thread_pool) {
    SyncPoint::GetInstance()->SetCallBack("AgentServer::Impl::get_thread_pool:1",
                                          [](void* arg) { *(ThreadPool**)arg = nullptr; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("AgentServer::Impl::get_thread_pool:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    brpc::Controller cntl;
    DeleteTabletRequest request;
    DeleteTabletResponse response;
    request.add_tablet_ids(_tablet_id);
    _lake_service.delete_tablet(&cntl, &request, &response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ("no thread pool to run task", cntl.ErrorText());
}

TEST_F(LakeServiceTest, test_vacuum_null_thread_pool) {
    SyncPoint::GetInstance()->SetCallBack("AgentServer::Impl::get_thread_pool:1",
                                          [](void* arg) { *(ThreadPool**)arg = nullptr; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("AgentServer::Impl::get_thread_pool:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    brpc::Controller cntl;
    VacuumRequest request;
    VacuumResponse response;
    request.add_tablet_ids(_tablet_id);
    request.set_partition_id(next_id());
    _lake_service.vacuum(&cntl, &request, &response, nullptr);
    ASSERT_EQ("vacuum thread pool is null", cntl.ErrorText());
}

TEST_F(LakeServiceTest, test_vacuum_thread_pool_full) {
    SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:1", [](void* arg) { *(int64_t*)arg = 0; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    brpc::Controller cntl;
    VacuumRequest request;
    VacuumResponse response;
    request.add_tablet_ids(_tablet_id);
    request.set_partition_id(next_id());
    _lake_service.vacuum(&cntl, &request, &response, nullptr);
    EXPECT_FALSE(cntl.Failed());
    EXPECT_EQ(TStatusCode::SERVICE_UNAVAILABLE, response.status().status_code()) << response.status().status_code();
}

TEST_F(LakeServiceTest, test_vacuum_full_null_thread_pool) {
    SyncPoint::GetInstance()->SetCallBack("AgentServer::Impl::get_thread_pool:1",
                                          [](void* arg) { *(ThreadPool**)arg = nullptr; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("AgentServer::Impl::get_thread_pool:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    brpc::Controller cntl;
    VacuumFullRequest request;
    VacuumFullResponse response;
    request.set_tablet_id(_tablet_id);
    _lake_service.vacuum_full(&cntl, &request, &response, nullptr);
    ASSERT_EQ("full vacuum thread pool is null", cntl.ErrorText());
}

TEST_F(LakeServiceTest, test_vacuum_full_thread_pool_full) {
    SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:1", [](void* arg) { *(int64_t*)arg = 0; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    brpc::Controller cntl;
    VacuumFullRequest request;
    VacuumFullResponse response;
    request.set_tablet_id(_tablet_id);
    _lake_service.vacuum_full(&cntl, &request, &response, nullptr);
    EXPECT_FALSE(cntl.Failed()) << cntl.ErrorText();
    EXPECT_EQ(TStatusCode::SERVICE_UNAVAILABLE, response.status().status_code()) << response.status().status_code();
}

TEST_F(LakeServiceTest, test_duplicated_vacuum_request) {
    SyncPoint::GetInstance()->LoadDependency({{"LakeServiceImpl::vacuum:1", "LakeServiceImpl::vacuum:2"}});
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() { SyncPoint::GetInstance()->DisableProcessing(); });

    auto duplicate = false;
    auto partition_id = next_id();

    auto t = std::thread([&]() {
        brpc::Controller cntl;
        VacuumRequest request;
        VacuumResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_partition_id(partition_id);
        _lake_service.vacuum(&cntl, &request, &response, nullptr);
        if (cntl.ErrorText() == fmt::format("duplicated vacuum request of partition {}", partition_id)) {
            duplicate = true;
        }
    });

    {
        brpc::Controller cntl;
        VacuumRequest request;
        VacuumResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_partition_id(partition_id);
        _lake_service.vacuum(&cntl, &request, &response, nullptr);
        if (cntl.ErrorText() == fmt::format("duplicated vacuum request of partition {}", partition_id)) {
            duplicate = true;
        }
    }

    t.join();

    ASSERT_TRUE(duplicate);
}

TEST_F(LakeServiceTest, test_lock_and_unlock_tablet_metadata) {
    {
        LockTabletMetadataRequest request;
        LockTabletMetadataResponse response;
        request.set_tablet_id(10);
        request.set_version(5);
        brpc::Controller cntl;
        _lake_service.lock_tablet_metadata(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
    }
    {
        UnlockTabletMetadataRequest request;
        UnlockTabletMetadataResponse response;
        request.set_tablet_id(10);
        request.set_version(13);
        request.set_expire_time(10000);
        brpc::Controller cntl;
        _lake_service.unlock_tablet_metadata(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
    }
}

TEST_F(LakeServiceTest, test_abort_txn2) {
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_id));
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(1));

    auto load_mgr = ExecEnv::GetInstance()->load_channel_mgr();
    auto db_id = next_id();
    auto table_id = next_id();
    auto partition_id = next_id();
    auto index_id = metadata->schema().id();
    auto txn_id = next_id();
    PUniqueId load_id;
    load_id.set_hi(next_id());
    load_id.set_lo(next_id());
    // Open load channel
    {
        PTabletWriterOpenRequest request;
        PTabletWriterOpenResult response;
        request.set_is_lake_tablet(true);
        request.mutable_id()->CopyFrom(load_id);
        request.set_table_id(table_id);
        request.set_index_id(index_id);
        request.set_txn_id(txn_id);
        request.set_num_senders(1);
        request.set_need_gen_rollup(false);
        request.set_load_channel_timeout_s(10000000);
        request.set_is_vectorized(true);
        request.set_table_id(next_id());
        request.mutable_schema()->set_db_id(db_id);
        request.mutable_schema()->set_table_id(table_id);
        request.mutable_schema()->set_version(1);
        auto index = request.mutable_schema()->add_indexes();
        index->set_id(index_id);
        index->set_schema_hash(0);
        index->set_schema_id(metadata->schema().id());
        for (int i = 0, sz = metadata->schema().column_size(); i < sz; i++) {
            const auto& column = metadata->schema().column(i);
            auto slot = request.mutable_schema()->add_slot_descs();
            slot->set_id(i);
            slot->set_byte_offset(i * sizeof(int) /*unused*/);
            slot->set_col_name(column.name() /*unused*/);
            slot->set_slot_idx(i);
            slot->set_is_materialized(true);
            ASSERT_EQ("INT", column.type());
            slot->mutable_slot_type()->add_types()->mutable_scalar_type()->set_type(TYPE_INT);

            index->add_columns(metadata->schema().column(i).name());
        }
        request.mutable_schema()->mutable_tuple_desc()->set_id(1);
        request.mutable_schema()->mutable_tuple_desc()->set_byte_size(8 /*unused*/);
        request.mutable_schema()->mutable_tuple_desc()->set_num_null_bytes(0 /*unused*/);
        request.mutable_schema()->mutable_tuple_desc()->set_table_id(10 /*unused*/);

        auto ptablet = request.add_tablets();
        ptablet->set_partition_id(partition_id);
        ptablet->set_tablet_id(metadata->id());

        MockClosure closure;
        load_mgr->open(nullptr, request, &response, &closure);
        ASSERT_TRUE(Awaitility().timeout(60000).until([&] { return closure.has_run(); }));
        ASSERT_EQ(TStatusCode::OK, response.status().status_code()) << response.status().error_msgs(0);
    }

    auto tablet_schema = TabletSchema::create(metadata->schema());
    auto schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));

    auto generate_data = [=](int64_t chunk_size) -> Chunk {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        std::iota(v0.begin(), v0.end(), 0);
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        Chunk chunk({std::move(c0), std::move(c1)}, schema);
        chunk.set_slot_id_to_index(0, 0);
        chunk.set_slot_id_to_index(1, 1);
        return chunk;
    };

    auto do_write = [&]() {
        auto chunk_size = 10;
        auto chunk = generate_data(chunk_size);
        bool cancelled = false;
        for (int64_t i = 0; i < 1000; i++) {
            PTabletWriterAddChunkRequest add_chunk_request;
            PTabletWriterAddBatchResult add_chunk_response;
            add_chunk_request.mutable_id()->CopyFrom(load_id);
            add_chunk_request.set_index_id(index_id);
            add_chunk_request.set_sender_id(0);
            add_chunk_request.set_eos(false);
            add_chunk_request.set_packet_seq(i);
            add_chunk_request.set_timeout_ms(60000);

            for (int j = 0; j < chunk_size; j++) {
                add_chunk_request.add_tablet_ids(_tablet_id);
                add_chunk_request.add_partition_ids(partition_id);
            }

            ASSIGN_OR_ABORT(auto chunk_pb, serde::ProtobufChunkSerde::serialize(chunk));
            add_chunk_request.mutable_chunk()->Swap(&chunk_pb);

            load_mgr->add_chunk(add_chunk_request, &add_chunk_response);
            if (add_chunk_response.status().status_code() != TStatusCode::OK) {
                cancelled = MatchPattern(add_chunk_response.status().error_msgs(0),
                                         "*was aborted at *, reason: transaction aborted via LakeService rpc");
                ASSERT_TRUE(cancelled) << add_chunk_response.status();
                break;
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
        ASSERT_TRUE(cancelled);
    };

    auto t1 = std::thread(do_write);

    std::this_thread::sleep_for(std::chrono::seconds(2));
    {
        AbortTxnRequest request;
        AbortTxnResponse response;
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(txn_id);
        request.set_skip_cleanup(false);
        _lake_service.abort_txn(nullptr, &request, &response, nullptr);
    }

    t1.join();
}

TEST_F(LakeServiceTest, test_abort3) {
    auto txn_id = next_id();
    TxnLog log;
    log.set_tablet_id(_tablet_id);
    log.set_txn_id(txn_id);
    ASSERT_OK(_tablet_mgr->put_txn_log(log));

    AbortTxnRequest request;
    AbortTxnResponse response;
    request.add_tablet_ids(_tablet_id);
    request.set_skip_cleanup(true);
    request.add_txn_ids(log.txn_id());

    _lake_service.abort_txn(nullptr, &request, &response, nullptr);

    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();

    EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_log_location(_tablet_id, log.txn_id())));
}

TEST_F(LakeServiceTest, test_drop_table_thread_pool_full) {
    SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:1", [](void* arg) { *(int64_t*)arg = 0; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    DropTableRequest request;
    DropTableResponse response;
    request.set_tablet_id(_tablet_id);
    brpc::Controller cntl;
    _lake_service.drop_table(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed());
    ASSERT_TRUE(response.has_status());
    ASSERT_EQ(TStatusCode::SERVICE_UNAVAILABLE, response.status().status_code());
}

TEST_F(LakeServiceTest, test_drop_table_no_permission) {
    SyncPoint::GetInstance()->SetCallBack("PosixFileSystem::delete_dir",
                                          [](void* arg) { *(Status*)arg = Status::IOError("Permission denied"); });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("PosixFileSystem::delete_dir");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    DropTableRequest request;
    DropTableResponse response;
    request.set_tablet_id(_tablet_id);
    brpc::Controller cntl;
    _lake_service.drop_table(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed());
    ASSERT_EQ(TStatusCode::IO_ERROR, response.status().status_code());
    ASSERT_EQ(1, response.status().error_msgs_size());
    ASSERT_TRUE(MatchPattern(response.status().error_msgs(0), "*Permission denied*"));
}

static TTabletSchema gen_tablet_schema_thrift() {
    TTabletSchema schema;
    schema.__set_id(next_id());
    schema.__set_keys_type(TKeysType::DUP_KEYS);
    schema.__set_schema_hash(0);
    schema.__set_schema_version(2);
    schema.__set_short_key_column_count(1);
    schema.__set_storage_type(TStorageType::COLUMN);
    {
        auto& col = schema.columns.emplace_back();
        col.__set_column_name("c0");
        col.__set_is_key(true);
        col.__set_aggregation_type(TAggregationType::NONE);
        col.__set_col_unique_id(0);
        col.__set_is_allow_null(true);
        col.__set_type_desc(gen_type_desc(TPrimitiveType::BIGINT));
    }
    {
        auto& col = schema.columns.emplace_back();
        col.__set_column_name("d2");
        col.__set_is_key(false);
        col.__set_aggregation_type(TAggregationType::NONE);
        col.__set_col_unique_id(3);
        col.__set_is_allow_null(true);
        col.__set_type_desc(gen_type_desc(TPrimitiveType::DOUBLE));
    }
    return schema;
}

TEST_F(LakeServiceTest, test_publish_version_for_fast_schema_evolution) {
    int64_t alter_txn_id = next_id();
    auto new_schema = gen_tablet_schema_thrift();
    // 1. write txn log for schema evolution
    {
        TUpdateTabletMetaInfoReq req;
        req.__set_tablet_type(TTabletType::TABLET_TYPE_LAKE);
        req.__set_txn_id(alter_txn_id);

        auto& update = req.tabletMetaInfos.emplace_back();
        update.__set_tablet_id(_tablet_id);
        update.__set_create_schema_file(true);
        update.__set_tablet_schema(new_schema);

        lake::SchemaChangeHandler handler(_tablet_mgr);
        ASSERT_OK(handler.process_update_tablet_meta(req));
    }
    // 2. publish version for schema evolution
    {
        brpc::Controller cntl;
        PublishVersionRequest req;
        PublishVersionResponse resp;
        req.set_base_version(1);
        req.set_new_version(2);
        req.add_tablet_ids(_tablet_id);
        req.add_txn_ids(alter_txn_id);
        req.set_commit_time(::time(nullptr));
        _lake_service.publish_version(&cntl, &req, &resp, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ(0, resp.status().status_code());
    }
    // 3. verify the new schema
    {
        auto compare_column = [](const TColumn& col1, const TabletColumn& col2) {
            EXPECT_EQ(col1.column_name, col2.name());
            EXPECT_EQ(col1.col_unique_id, col2.unique_id());
            EXPECT_EQ(col1.is_allow_null, col2.is_nullable());
            EXPECT_EQ(col1.is_key, col2.is_key());
            auto t1 = thrift_to_type(col1.type_desc.types[0].scalar_type.type);
            EXPECT_EQ(t1, col2.type());
        };
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_id, 2));
        auto& schema = metadata->schema();
        EXPECT_EQ(new_schema.id, schema.id());
        EXPECT_EQ(new_schema.columns.size(), schema.column_size());
        EXPECT_EQ(new_schema.short_key_column_count, schema.num_short_key_columns());
        compare_column(new_schema.columns[0], schema.column(0));
        compare_column(new_schema.columns[1], schema.column(1));
    }
}

TEST_F(LakeServiceTest, test_publish_version_with_combined_log) {
    // Put empty CombinedTxnLog should return error
    {
        auto combined_log = CombinedTxnLogPB();
        ASSERT_FALSE(_tablet_mgr->put_combined_txn_log(combined_log).ok());
    }

    auto do_test = [&](int64_t txn_id, TStatusCode::type expect_code) {
        PublishVersionRequest publish_request;
        publish_request.set_base_version(1);
        publish_request.set_new_version(2);
        publish_request.add_tablet_ids(_tablet_id);
        auto info = publish_request.add_txn_infos();
        info->set_txn_id(txn_id);
        info->set_combined_txn_log(true);
        info->set_commit_time(987654321);
        info->set_txn_type(TXN_NORMAL);
        PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request, &response, nullptr);
        EXPECT_EQ(expect_code, response.status().status_code());
    };

    // combined log does not exist
    { do_test(next_id(), TStatusCode::NOT_FOUND); }
    // CombinedTxnLog does not contain the target txn log
    {
        auto txn_log = generate_write_txn_log(2, 101, 4096);
        txn_log.set_tablet_id(_tablet_id + 1);
        auto combined_log = CombinedTxnLogPB();
        combined_log.add_txn_logs()->CopyFrom(txn_log);
        ASSERT_OK(_tablet_mgr->put_combined_txn_log(combined_log));

        do_test(txn_log.txn_id(), TStatusCode::INTERNAL_ERROR);
    }
    // Publish txn success
    {
        auto txn_log = std::make_shared<TxnLogPB>(generate_write_txn_log(2, 101, 4096));
        auto txn_id = txn_log->txn_id();
        auto combined_log = CombinedTxnLogPB();
        combined_log.add_txn_logs()->CopyFrom(*txn_log);

        _tablet_mgr->metacache()->cache_txn_log(_tablet_mgr->txn_log_location(_tablet_id, txn_id), txn_log);

        ASSERT_OK(_tablet_mgr->put_combined_txn_log(combined_log));

        do_test(txn_id, TStatusCode::OK);
        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();

        // CombinedTxnLogPB should still exist
        auto path = _tablet_mgr->combined_txn_log_location(_tablet_id, txn_id);
        ASSERT_OK(FileSystem::Default()->path_exists(path));

        _tablet_mgr->metacache()->erase(_tablet_mgr->txn_log_location(_tablet_id, txn_id));
        // publish again without txn log cache
        do_test(txn_id, TStatusCode::OK);

        // publish again without txn log cache and combined txn log cache
        _tablet_mgr->metacache()->erase(_tablet_mgr->txn_log_location(_tablet_id, txn_id));
        _tablet_mgr->metacache()->erase(_tablet_mgr->combined_txn_log_location(_tablet_id, txn_id));
        do_test(txn_id, TStatusCode::OK);
    }
}

TEST_F(LakeServiceTest, test_publish_version_with_txn_info) {
    std::vector<TxnLog> logs;
    // TxnLog with 2 segments
    logs.emplace_back(generate_write_txn_log(2, 101, 4096));
    ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));

    // publish version
    {
        PublishVersionRequest request;
        request.set_base_version(1);
        request.set_new_version(2);
        request.add_tablet_ids(_tablet_id);
        auto info = request.add_txn_infos();
        info->set_txn_id(logs[0].txn_id());
        info->set_txn_type(TXN_NORMAL);
        info->set_combined_txn_log(false);
        info->set_commit_time(987654321);

        PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
        EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
    }
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_id));
    {
        ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(2));
        ASSERT_EQ(2, metadata->version());
        ASSERT_EQ(_tablet_id, metadata->id());
        ASSERT_EQ(3, metadata->next_rowset_id());
        ASSERT_EQ(1, metadata->rowsets_size());
        ASSERT_EQ(1, metadata->rowsets(0).id());
        ASSERT_EQ(2, metadata->rowsets(0).segments_size());
        ASSERT_TRUE(metadata->rowsets(0).overlapped());
        ASSERT_EQ(logs[0].op_write().rowset().num_rows(), metadata->rowsets(0).num_rows());
        ASSERT_EQ(logs[0].op_write().rowset().data_size(), metadata->rowsets(0).data_size());
        ASSERT_EQ(logs[0].op_write().rowset().segments(0), metadata->rowsets(0).segments(0));
        ASSERT_EQ(logs[0].op_write().rowset().segments(1), metadata->rowsets(0).segments(1));
        EXPECT_EQ(987654321, metadata->commit_time());
    }
    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
    // TxnLog`s should have been deleted
    ASSERT_TRUE(tablet.get_txn_log(logs[0].txn_id()).status().is_not_found());
}

TEST_F(LakeServiceTest, test_abort_with_combined_txn_log) {
    auto txn_id = next_id();
    auto combined_log = std::make_shared<CombinedTxnLogPB>();
    for (int i = 0; i < 3; i++) {
        TxnLog log;
        log.set_tablet_id(_tablet_id);
        log.set_txn_id(txn_id);
        log.set_partition_id(_partition_id);
        log.mutable_op_write()->mutable_rowset()->add_segments(generate_segment_file(txn_id));
        log.mutable_op_write()->mutable_rowset()->set_data_size(4096);
        log.mutable_op_write()->mutable_rowset()->set_num_rows(101);
        log.mutable_op_write()->mutable_rowset()->set_overlapped(true);
        combined_log->add_txn_logs()->CopyFrom(log);
    }
    _tablet_mgr->put_combined_txn_log(*combined_log);

    AbortTxnRequest request;
    request.add_tablet_ids(_tablet_id);
    request.set_skip_cleanup(false);
    auto info = request.add_txn_infos();
    info->set_txn_id(txn_id);
    info->set_combined_txn_log(true);
    info->set_txn_type(TXN_NORMAL);

    {
        TEST_ENABLE_ERROR_POINT("TabletManager::get_combined_txn_log", Status::IOError("injected error"));
        SyncPoint::GetInstance()->EnableProcessing();

        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("TabletManager::load_txn_log");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        AbortTxnResponse response;
        _lake_service.abort_txn(nullptr, &request, &response, nullptr);
        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();

        for (auto&& log : combined_log->txn_logs()) {
            for (auto&& s : log.op_write().rowset().segments()) {
                EXPECT_TRUE(fs::path_exist(_tablet_mgr->segment_location(_tablet_id, s)));
            }
        }
        EXPECT_TRUE(fs::path_exist(_tablet_mgr->combined_txn_log_location(_tablet_id, txn_id)));
    }
    {
        AbortTxnResponse response;
        _lake_service.abort_txn(nullptr, &request, &response, nullptr);

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();

        // TxnLog`s and segments should have been deleted
        for (auto&& log : combined_log->txn_logs()) {
            for (auto&& s : log.op_write().rowset().segments()) {
                EXPECT_FALSE(fs::path_exist(_tablet_mgr->segment_location(_tablet_id, s)));
            }
        }
        EXPECT_FALSE(fs::path_exist(_tablet_mgr->combined_txn_log_location(_tablet_id, txn_id)));
    }
}

TEST_F(LakeServiceTest, test_delete_data_ok) {
    // delete the data from a tablet, but the tablet is not found from TabletManager
    DeleteDataRequest request;
    request.add_tablet_ids(_tablet_id);
    request.set_txn_id(12345);
    request.mutable_delete_predicate()->set_version(1);

    DeleteDataResponse response;
    _tablet_mgr->prune_metacache();
    _lake_service.delete_data(nullptr, &request, &response, nullptr);

    EXPECT_EQ(0L, response.failed_tablets().size());
}

TEST_F(LakeServiceTest, test_aggregate_publish_version_normal) {
    brpc::Server server;
    MockLakeServiceImpl mock_service;
    int port = 0;
    init_server_with_mock(&mock_service, &server, &port);

    auto request = build_default_agg_request(port);

    TabletSchemaPB schema_pb1;
    TabletSchemaPB schema_pb2;
    TabletSchemaPB schema_pb3;
    starrocks::TabletMetadataPB metadata1;
    starrocks::TabletMetadataPB metadata2;
    build_schemas_and_metadata(&schema_pb1, &schema_pb2, &schema_pb3, &metadata1, &metadata2);

    EXPECT_CALL(mock_service, publish_version(_, _, _, _))
            .WillOnce(Invoke([&](::google::protobuf::RpcController*, const PublishVersionRequest*,
                                 PublishVersionResponse* resp, ::google::protobuf::Closure* done) {
                resp->mutable_status()->set_status_code(0);
                auto& item1 = (*resp->mutable_tablet_metas())[1];
                item1.CopyFrom(metadata1);
                auto& item2 = (*resp->mutable_tablet_metas())[2];
                item2.CopyFrom(metadata2);
                done->Run();
            }));

    PublishVersionResponse response;
    brpc::Controller cntl;
    google::protobuf::Closure* done = brpc::NewCallback([]() {});
    _lake_service.aggregate_publish_version(&cntl, &request, &response, done);

    EXPECT_EQ(response.status().status_code(), 0);
    auto res = _tablet_mgr->get_single_tablet_metadata(1, 2);
    ASSERT_TRUE(res.ok());
    ASSERT_EQ(res.value()->id(), 1);
    TabletMetadataPtr metadata3 = std::move(res).value();
    ASSERT_EQ(metadata3->schema().id(), 10);
    ASSERT_EQ(metadata3->historical_schemas_size(), 2);
    auto res2 = _tablet_mgr->get_single_tablet_metadata(2, 2);
    ASSERT_TRUE(res2.ok());
    ASSERT_EQ(res2.value()->id(), 2);

    server.Stop(0);
    server.Join();
}

TEST_F(LakeServiceTest, test_aggregate_publish_version_failed) {
    brpc::Server server;
    MockLakeServiceImpl mock_service;
    int port = 0;
    init_server_with_mock(&mock_service, &server, &port);

    auto request = build_default_agg_request(port);

    TabletSchemaPB schema_pb1;
    TabletSchemaPB schema_pb2;
    starrocks::TabletMetadataPB metadata1;
    build_schemas_and_metadata(&schema_pb1, &schema_pb2, /*schema_pb3=*/nullptr, &metadata1, /*metadata2=*/nullptr);

    EXPECT_CALL(mock_service, publish_version(_, _, _, _))
            .WillOnce(Invoke([&](::google::protobuf::RpcController*, const PublishVersionRequest*,
                                 PublishVersionResponse* resp, ::google::protobuf::Closure* done) {
                resp->mutable_status()->set_status_code(1);
                auto& item1 = (*resp->mutable_tablet_metas())[1];
                item1.CopyFrom(metadata1);
                done->Run();
            }));

    PublishVersionResponse response;
    brpc::Controller cntl;
    google::protobuf::Closure* done = brpc::NewCallback([]() {});
    _lake_service.aggregate_publish_version(&cntl, &request, &response, done);

    EXPECT_EQ(response.status().status_code(), 6);

    server.Stop(0);
    server.Join();
}

TEST_F(LakeServiceTest, test_task_cleared_in_thread_pool_queue) {
    class MockRunnable : public Runnable {
    public:
        MockRunnable() {}
        virtual ~MockRunnable() override {}
        virtual void run() override {}
        virtual void cancel() override {}
    };

    SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:replace_task", [](void* arg) {
        auto ptr = (*(std::shared_ptr<Runnable>*)arg);
        ptr->cancel();
        (*(std::shared_ptr<Runnable>*)arg) = std::make_shared<MockRunnable>();
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:replace_task");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    {
        brpc::Controller cntl;
        PublishVersionRequest request;
        PublishVersionResponse response;
        request.set_base_version(1);
        request.set_new_version(2);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1000);
        _lake_service.publish_version(&cntl, &request, &response, nullptr);
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
        ASSERT_TRUE(MatchPattern(response.status().error_msgs(0), "*has been cancelled*"));
    }

    {
        auto txn_id = next_id();
        PublishLogVersionRequest request;
        PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        request.set_version(10);
        brpc::Controller cntl;
        _lake_service.publish_log_version(&cntl, &request, &response, nullptr);
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
    }

    {
        AbortTxnRequest request;
        request.add_tablet_ids(_tablet_id);
        request.set_skip_cleanup(false);
        request.add_txn_ids(next_id());
        AbortTxnResponse response;
        _lake_service.abort_txn(nullptr, &request, &response, nullptr);
    }

    {
        brpc::Controller cntl;
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(_tablet_id);
        _lake_service.delete_tablet(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
        ASSERT_TRUE(MatchPattern(response.status().error_msgs(0), "*has been cancelled*"));
    }

    {
        std::vector<TxnLog> logs;

        // TxnLog with 2 segments
        logs.emplace_back(generate_write_txn_log(2, 101, 4096));
        ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));

        brpc::Controller cntl;
        DeleteTxnLogRequest request;
        DeleteTxnLogResponse response;
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(logs.back().txn_id());
        _lake_service.delete_txn_log(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(MatchPattern(response.status().error_msgs(0), "*has been cancelled*"));
    }

    {
        ASSERT_OK(FileSystem::Default()->path_exists(kRootLocation));
        DropTableRequest request;
        DropTableResponse response;

        brpc::Controller cntl;
        request.set_tablet_id(_tablet_id);
        _lake_service.drop_table(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(response.has_status());
        ASSERT_TRUE(MatchPattern(response.status().error_msgs(0), "*has been cancelled*"));
    }

    {
        DeleteDataRequest request;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(12345);
        request.mutable_delete_predicate()->set_version(1);

        DeleteDataResponse response;
        _lake_service.delete_data(nullptr, &request, &response, nullptr);
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
    }

    {
        TabletStatRequest request;
        TabletStatResponse response;
        auto* info = request.add_tablet_infos();
        info->set_tablet_id(_tablet_id);
        info->set_version(1);

        // Prune metadata cache before getting tablet stats
        _tablet_mgr->metacache()->prune();

        _lake_service.get_tablet_stats(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.tablet_stats_size());
    }

    {
        brpc::Controller cntl;
        VacuumRequest request;
        VacuumResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_partition_id(next_id());
        _lake_service.vacuum(&cntl, &request, &response, nullptr);
    }
}

TEST_F(LakeServiceTest, test_get_tablet_metadatas) {
    // Helper to check if a specific version exists in metadata_entries
    auto has_version = [](const ::starrocks::TabletResult& tablet_result, int64_t version_to_find) {
        for (const auto& entry : tablet_result.metadata_entries()) {
            if (entry.metadata().version() == version_to_find) {
                return true;
            }
        }
        return false;
    };

    // Helper to get metadata entry for a specific version
    auto get_metadata_entry = [](const ::starrocks::TabletResult& tablet_result,
                                 int64_t version_to_find) -> const TabletMetadataEntry* {
        for (const auto& entry : tablet_result.metadata_entries()) {
            if (entry.metadata().version() == version_to_find) {
                return &entry;
            }
        }
        return nullptr;
    };

    // 0. setup: create tablet with version 1, 2, 3, 4
    auto publish_version = [&](int64_t base_version, int64_t new_version) {
        auto txn_log = generate_write_txn_log(1, 10 * new_version, 100 * new_version);
        CHECK_OK(_tablet_mgr->put_txn_log(txn_log));

        PublishVersionRequest request;
        PublishVersionResponse response;
        request.set_base_version(base_version);
        request.set_new_version(new_version);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(txn_log.txn_id());
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        CHECK_EQ(0, response.failed_tablets_size());
        CHECK_EQ(0, response.status().status_code());
    };

    publish_version(1, 2);
    publish_version(2, 3);
    publish_version(3, 4);

    // 1. check request
    // 1.1 missing request fields
    {
        brpc::Controller cntl;
        GetTabletMetadatasRequest request;
        GetTabletMetadatasResponse response;
        _lake_service.get_tablet_metadatas(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(TStatusCode::INVALID_ARGUMENT, response.status().status_code());
        ASSERT_TRUE(MatchPattern(response.status().error_msgs(0), "missing tablet_ids"));
    }

    {
        brpc::Controller cntl;
        GetTabletMetadatasRequest request;
        GetTabletMetadatasResponse response;
        request.add_tablet_ids(_tablet_id);
        _lake_service.get_tablet_metadatas(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(TStatusCode::INVALID_ARGUMENT, response.status().status_code());
        ASSERT_TRUE(MatchPattern(response.status().error_msgs(0), "missing max_version"));
    }

    {
        brpc::Controller cntl;
        GetTabletMetadatasRequest request;
        GetTabletMetadatasResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_max_version(3);
        _lake_service.get_tablet_metadatas(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(TStatusCode::INVALID_ARGUMENT, response.status().status_code());
        ASSERT_TRUE(MatchPattern(response.status().error_msgs(0), "missing min_version"));
    }

    // 1.2 max_version < min_version
    {
        brpc::Controller cntl;
        GetTabletMetadatasRequest request;
        GetTabletMetadatasResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_max_version(3);
        request.set_min_version(4);
        _lake_service.get_tablet_metadatas(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(TStatusCode::INVALID_ARGUMENT, response.status().status_code());
        ASSERT_TRUE(MatchPattern(response.status().error_msgs(0), "max_version should be >= min_version"));
    }

    // 2. thread pool is null
    {
        SyncPoint::GetInstance()->SetCallBack("AgentServer::Impl::get_thread_pool:1",
                                              [](void* arg) { *(ThreadPool**)arg = nullptr; });
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            SyncPoint::GetInstance()->ClearCallBack("AgentServer::Impl::get_thread_pool:1");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        brpc::Controller cntl;
        GetTabletMetadatasRequest request;
        GetTabletMetadatasResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_max_version(10);
        request.set_min_version(1);
        _lake_service.get_tablet_metadatas(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(TStatusCode::SERVICE_UNAVAILABLE, response.status().status_code());
        ASSERT_TRUE(MatchPattern(response.status().error_msgs(0), "tablet stats thread pool is null"));
    }

    // 3. success case
    // 3.1 normal case
    {
        GetTabletMetadatasRequest request;
        GetTabletMetadatasResponse response;
        brpc::Controller cntl;

        request.add_tablet_ids(_tablet_id);
        request.set_max_version(3);
        request.set_min_version(2);

        _lake_service.get_tablet_metadatas(&cntl, &request, &response, nullptr);

        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ(0, response.status().status_code());
        ASSERT_EQ(response.tablet_results_size(), 1);
        const auto& tablet_result = response.tablet_results(0);
        ASSERT_EQ(tablet_result.tablet_id(), _tablet_id);
        ASSERT_EQ(tablet_result.metadata_entries_size(), 2);
        ASSERT_TRUE(has_version(tablet_result, 2));
        ASSERT_TRUE(has_version(tablet_result, 3));
        auto* entry2 = get_metadata_entry(tablet_result, 2);
        ASSERT_TRUE(entry2 != nullptr);
        ASSERT_EQ(entry2->metadata().version(), 2);
        auto* entry3 = get_metadata_entry(tablet_result, 3);
        ASSERT_TRUE(entry3 != nullptr);
        ASSERT_EQ(entry3->metadata().version(), 3);
    }

    // 3.2 tablet not found
    {
        GetTabletMetadatasRequest request;
        GetTabletMetadatasResponse response;
        brpc::Controller cntl;

        int64_t non_existent_tablet_id = -1;
        request.add_tablet_ids(non_existent_tablet_id);
        request.set_max_version(2);
        request.set_min_version(1);

        _lake_service.get_tablet_metadatas(&cntl, &request, &response, nullptr);

        ASSERT_FALSE(cntl.Failed());
        // rpc-level status should be OK
        ASSERT_EQ(0, response.status().status_code());
        ASSERT_EQ(response.tablet_results_size(), 1);
        const auto& tablet_result = response.tablet_results(0);
        ASSERT_EQ(tablet_result.tablet_id(), non_existent_tablet_id);
        ASSERT_EQ(TStatusCode::NOT_FOUND, tablet_result.status().status_code());
        ASSERT_TRUE(MatchPattern(tablet_result.status().error_msgs(0),
                                 "tablet -1 metadata not found in version range [1, 2]"));
    }

    // 3.3 some versions not found
    {
        GetTabletMetadatasRequest request;
        GetTabletMetadatasResponse response;
        brpc::Controller cntl;

        request.add_tablet_ids(_tablet_id);
        // version 5 does not exist
        request.set_max_version(5);
        request.set_min_version(3);

        _lake_service.get_tablet_metadatas(&cntl, &request, &response, nullptr);

        ASSERT_FALSE(cntl.Failed());
        // rpc-level status should be OK
        ASSERT_EQ(0, response.status().status_code());
        ASSERT_EQ(response.tablet_results_size(), 1);
        const auto& tablet_result = response.tablet_results(0);
        ASSERT_EQ(tablet_result.tablet_id(), _tablet_id);
        ASSERT_EQ(TStatusCode::OK, tablet_result.status().status_code());

        // should find version 3 and 4
        ASSERT_EQ(tablet_result.metadata_entries_size(), 2);
        ASSERT_TRUE(has_version(tablet_result, 3));
        ASSERT_TRUE(has_version(tablet_result, 4));
    }

    // 4. failed case
    // 4.1 get tablet metadata failed
    {
        TEST_ENABLE_ERROR_POINT("TabletManager::get_tablet_metadata",
                                Status::IOError("injected get tablet metadata error"));
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("TabletManager::get_tablet_metadata");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        GetTabletMetadatasRequest request;
        GetTabletMetadatasResponse response;
        brpc::Controller cntl;

        request.add_tablet_ids(_tablet_id);
        request.set_max_version(3);
        request.set_min_version(2);

        _lake_service.get_tablet_metadatas(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        // rpc-level status should be OK even if one tablet fails
        ASSERT_EQ(0, response.status().status_code());
        ASSERT_EQ(response.tablet_results_size(), 1);
        const auto& tablet_result = response.tablet_results(0);
        ASSERT_EQ(tablet_result.tablet_id(), _tablet_id);
        ASSERT_EQ(TStatusCode::IO_ERROR, tablet_result.status().status_code());
        ASSERT_TRUE(MatchPattern(tablet_result.status().error_msgs(0), "injected get tablet metadata error"));
    }

    // 4.2 thread pool is full
    {
        SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:1", [](void* arg) { *(int64_t*)arg = 0; });
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:1");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        brpc::Controller cntl;
        GetTabletMetadatasRequest request;
        GetTabletMetadatasResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_max_version(10);
        request.set_min_version(1);
        _lake_service.get_tablet_metadatas(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        // rpc-level status should be OK even if one tablet fails
        ASSERT_EQ(0, response.status().status_code());
        ASSERT_EQ(response.tablet_results_size(), 1);
        const auto& tablet_result = response.tablet_results(0);
        ASSERT_EQ(tablet_result.tablet_id(), _tablet_id);
        ASSERT_EQ(TStatusCode::SERVICE_UNAVAILABLE, tablet_result.status().status_code());
    }

    // 4.3 task cancelled
    {
        class MockRunnable : public Runnable {
        public:
            MockRunnable() {}
            virtual ~MockRunnable() override {}
            virtual void run() override {}
            virtual void cancel() override {}
        };

        SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:replace_task", [](void* arg) {
            auto ptr = (*(std::shared_ptr<Runnable>*)arg);
            ptr->cancel();
            (*(std::shared_ptr<Runnable>*)arg) = std::make_shared<MockRunnable>();
        });
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:replace_task");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        brpc::Controller cntl;
        GetTabletMetadatasRequest request;
        GetTabletMetadatasResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_max_version(10);
        request.set_min_version(1);
        _lake_service.get_tablet_metadatas(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        // rpc-level status should be OK even if one tablet fails
        ASSERT_EQ(0, response.status().status_code());
        ASSERT_EQ(response.tablet_results_size(), 1);
        const auto& tablet_result = response.tablet_results(0);
        ASSERT_EQ(tablet_result.tablet_id(), _tablet_id);
        ASSERT_EQ(TStatusCode::CANCELLED, tablet_result.status().status_code());
        ASSERT_TRUE(
                MatchPattern(tablet_result.status().error_msgs(0), "*get tablet metadatas task has been cancelled*"));
    }

    // 5. check missing files
    {
        // 5.1 no missing files for version 4
        brpc::Controller cntl;
        GetTabletMetadatasRequest request;
        GetTabletMetadatasResponse response;

        request.add_tablet_ids(_tablet_id);
        request.set_min_version(4);
        request.set_max_version(4);
        request.set_check_missing_files(true);
        _lake_service.get_tablet_metadatas(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ(TStatusCode::OK, response.status().status_code());
        ASSERT_EQ(1, response.tablet_results_size());
        ASSERT_EQ(_tablet_id, response.tablet_results(0).tablet_id());
        ASSERT_EQ(1, response.tablet_results(0).metadata_entries_size());
        const auto& entry = response.tablet_results(0).metadata_entries(0);
        ASSERT_EQ(0, entry.missing_files_size());
        const auto& metadata = entry.metadata();
        ASSERT_EQ(3, metadata.rowsets_size());
        ASSERT_EQ(1, metadata.rowsets(0).segments_size());
        std::string seg_name = metadata.rowsets(0).segments(0);

        response.Clear();
        request.Clear();

        // 5.2 missing segment files for version 4
        // delete one segment file from version 4
        ASSERT_OK(fs::remove(_tablet_mgr->segment_location(_tablet_id, seg_name)));
        request.add_tablet_ids(_tablet_id);
        request.set_min_version(4);
        request.set_max_version(4);
        request.set_check_missing_files(true);
        _lake_service.get_tablet_metadatas(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ(TStatusCode::OK, response.status().status_code());
        ASSERT_EQ(1, response.tablet_results_size());
        ASSERT_EQ(_tablet_id, response.tablet_results(0).tablet_id());
        ASSERT_EQ(1, response.tablet_results(0).metadata_entries_size());
        ASSERT_EQ(1, response.tablet_results(0).metadata_entries(0).missing_files_size());
        ASSERT_EQ(seg_name, response.tablet_results(0).metadata_entries(0).missing_files(0));
    }
}

TEST_F(LakeServiceTest, test_repair_tablet_metadata) {
    // 1. check request
    // 1.1 missing tablet_metadatas
    {
        brpc::Controller cntl;
        RepairTabletMetadataRequest request;
        RepairTabletMetadataResponse response;
        _lake_service.repair_tablet_metadata(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(TStatusCode::INVALID_ARGUMENT, response.status().status_code());
        ASSERT_TRUE(MatchPattern(response.status().error_msgs(0), "missing tablet_metadatas"));
    }

    // 1.2 tablet metadatas with different versions
    {
        brpc::Controller cntl;
        RepairTabletMetadataRequest request;
        RepairTabletMetadataResponse response;

        TabletMetadataPB metadata1;
        metadata1.set_id(next_id());
        metadata1.set_version(1);
        request.add_tablet_metadatas()->CopyFrom(metadata1);

        TabletMetadataPB metadata2;
        metadata2.set_id(next_id());
        metadata2.set_version(2);
        request.add_tablet_metadatas()->CopyFrom(metadata2);

        _lake_service.repair_tablet_metadata(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(TStatusCode::INVALID_ARGUMENT, response.status().status_code());
        ASSERT_TRUE(MatchPattern(response.status().error_msgs(0), "tablet metadatas should have the same version"));
    }

    // 1.3 invalid version (version <= 0)
    {
        brpc::Controller cntl;
        RepairTabletMetadataRequest request;
        RepairTabletMetadataResponse response;

        TabletMetadataPB metadata;
        metadata.set_id(next_id());
        metadata.set_version(0);
        request.add_tablet_metadatas()->CopyFrom(metadata);

        _lake_service.repair_tablet_metadata(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(TStatusCode::INVALID_ARGUMENT, response.status().status_code());
        ASSERT_TRUE(MatchPattern(response.status().error_msgs(0), "invalid version: 0"));
    }

    // 1.4 duplicated tablet id
    {
        brpc::Controller cntl;
        RepairTabletMetadataRequest request;
        RepairTabletMetadataResponse response;

        int64_t duplicated_tablet_id = next_id();
        TabletMetadataPB metadata1;
        metadata1.set_id(duplicated_tablet_id);
        metadata1.set_version(100);
        request.add_tablet_metadatas()->CopyFrom(metadata1);

        TabletMetadataPB metadata2;
        metadata2.set_id(duplicated_tablet_id);
        metadata2.set_version(100);
        request.add_tablet_metadatas()->CopyFrom(metadata2);

        _lake_service.repair_tablet_metadata(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(TStatusCode::INVALID_ARGUMENT, response.status().status_code());
        ASSERT_TRUE(MatchPattern(response.status().error_msgs(0),
                                 fmt::format("duplicated tablet id: {}", duplicated_tablet_id)));
    }

    // 2. thread pool is null
    {
        SyncPoint::GetInstance()->SetCallBack("AgentServer::Impl::get_thread_pool:1",
                                              [](void* arg) { *(ThreadPool**)arg = nullptr; });
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            SyncPoint::GetInstance()->ClearCallBack("AgentServer::Impl::get_thread_pool:1");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        brpc::Controller cntl;
        RepairTabletMetadataRequest request;
        RepairTabletMetadataResponse response;

        TabletMetadataPB metadata_to_repair;
        metadata_to_repair.set_id(next_id());
        metadata_to_repair.set_version(100);
        request.add_tablet_metadatas()->CopyFrom(metadata_to_repair);

        _lake_service.repair_tablet_metadata(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(TStatusCode::SERVICE_UNAVAILABLE, response.status().status_code());
        ASSERT_TRUE(MatchPattern(response.status().error_msgs(0), "publish version thread pool is null"));
    }

    // 3. successful repair of non-bundling tablet metadata
    {
        brpc::Controller cntl;
        RepairTabletMetadataRequest request;
        RepairTabletMetadataResponse response;

        TabletMetadataPB metadata_to_repair;
        metadata_to_repair.set_id(next_id());
        metadata_to_repair.set_version(100);
        metadata_to_repair.mutable_schema()->set_id(200);
        metadata_to_repair.mutable_schema()->set_keys_type(DUP_KEYS);
        metadata_to_repair.mutable_schema()->set_num_short_key_columns(1);
        metadata_to_repair.mutable_schema()->set_num_rows_per_row_block(65535);
        metadata_to_repair.add_rowsets()->set_id(1);

        request.add_tablet_metadatas()->CopyFrom(metadata_to_repair);
        request.set_enable_file_bundling(false);

        _lake_service.repair_tablet_metadata(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ(TStatusCode::OK, response.status().status_code()) << response.status().error_msgs(0);
        ASSERT_EQ(1, response.tablet_repair_statuses_size());
        ASSERT_EQ(metadata_to_repair.id(), response.tablet_repair_statuses(0).tablet_id());
        ASSERT_EQ(TStatusCode::OK, response.tablet_repair_statuses(0).status().status_code());

        // verify the metadata was put
        ASSIGN_OR_ABORT(auto tablet_read, _tablet_mgr->get_tablet(metadata_to_repair.id()));
        ASSIGN_OR_ABORT(auto retrieved_metadata, tablet_read.get_metadata(metadata_to_repair.version()));
        ASSERT_EQ(metadata_to_repair.id(), retrieved_metadata->id());
        ASSERT_EQ(metadata_to_repair.version(), retrieved_metadata->version());
        ASSERT_EQ(metadata_to_repair.schema().id(), retrieved_metadata->schema().id());
        ASSERT_EQ(1, retrieved_metadata->rowsets_size());
        ASSERT_EQ(1, retrieved_metadata->rowsets(0).id());
    }

    // 4. successful repair of bundling tablet metadata
    {
        brpc::Controller cntl;
        RepairTabletMetadataRequest request;
        RepairTabletMetadataResponse response;

        TabletMetadataPB metadata_to_repair_1;
        int64_t tablet_id_1 = next_id();
        int64_t version_1 = 200;
        metadata_to_repair_1.set_id(tablet_id_1);
        metadata_to_repair_1.set_version(version_1);
        metadata_to_repair_1.mutable_schema()->set_id(300);
        metadata_to_repair_1.mutable_schema()->set_keys_type(DUP_KEYS);
        metadata_to_repair_1.mutable_schema()->set_num_short_key_columns(1);
        metadata_to_repair_1.mutable_schema()->set_num_rows_per_row_block(65535);
        metadata_to_repair_1.add_rowsets()->set_id(1);

        TabletMetadataPB metadata_to_repair_2;
        int64_t tablet_id_2 = next_id();
        // same version for bundling
        int64_t version_2 = 200;
        metadata_to_repair_2.set_id(tablet_id_2);
        metadata_to_repair_2.set_version(version_2);
        metadata_to_repair_2.mutable_schema()->set_id(301);
        metadata_to_repair_2.mutable_schema()->set_keys_type(DUP_KEYS);
        metadata_to_repair_2.mutable_schema()->set_num_short_key_columns(1);
        metadata_to_repair_2.mutable_schema()->set_num_rows_per_row_block(65535);
        metadata_to_repair_2.add_rowsets()->set_id(2);

        request.add_tablet_metadatas()->CopyFrom(metadata_to_repair_1);
        request.add_tablet_metadatas()->CopyFrom(metadata_to_repair_2);
        request.set_enable_file_bundling(true);
        request.set_write_bundling_file(true);

        _lake_service.repair_tablet_metadata(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ(TStatusCode::OK, response.status().status_code()) << response.status().error_msgs(0);
        ASSERT_EQ(1, response.tablet_repair_statuses_size());
        ASSERT_EQ(0, response.tablet_repair_statuses(0).tablet_id());
        ASSERT_EQ(TStatusCode::OK, response.tablet_repair_statuses(0).status().status_code());

        // verify metadata 1 was put
        ASSIGN_OR_ABORT(auto tablet_read_1, _tablet_mgr->get_tablet(tablet_id_1));
        ASSIGN_OR_ABORT(auto retrieved_metadata_1, tablet_read_1.get_metadata(version_1));
        ASSERT_EQ(metadata_to_repair_1.id(), retrieved_metadata_1->id());
        ASSERT_EQ(metadata_to_repair_1.version(), retrieved_metadata_1->version());
        ASSERT_EQ(metadata_to_repair_1.schema().id(), retrieved_metadata_1->schema().id());
        ASSERT_EQ(1, retrieved_metadata_1->rowsets_size());
        ASSERT_EQ(1, retrieved_metadata_1->rowsets(0).id());

        // verify metadata 2 was put
        ASSIGN_OR_ABORT(auto tablet_read_2, _tablet_mgr->get_tablet(tablet_id_2));
        ASSIGN_OR_ABORT(auto retrieved_metadata_2, tablet_read_2.get_metadata(version_2));
        ASSERT_EQ(metadata_to_repair_2.id(), retrieved_metadata_2->id());
        ASSERT_EQ(metadata_to_repair_2.version(), retrieved_metadata_2->version());
        ASSERT_EQ(metadata_to_repair_2.schema().id(), retrieved_metadata_2->schema().id());
        ASSERT_EQ(1, retrieved_metadata_2->rowsets_size());
        ASSERT_EQ(2, retrieved_metadata_2->rowsets(0).id());
    }

    // 7. failed case
    // 7.1 put_tablet_metadata fail (non-bundling)
    {
        TEST_ENABLE_ERROR_POINT("TabletManager::put_tablet_metadata",
                                Status::IOError("injected put tablet metadata error"));
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("TabletManager::put_tablet_metadata");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        brpc::Controller cntl;
        RepairTabletMetadataRequest request;
        RepairTabletMetadataResponse response;

        TabletMetadataPB metadata_to_repair;
        metadata_to_repair.set_id(next_id());
        metadata_to_repair.set_version(101);
        request.add_tablet_metadatas()->CopyFrom(metadata_to_repair);
        request.set_enable_file_bundling(false);

        _lake_service.repair_tablet_metadata(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(TStatusCode::OK, response.status().status_code());
        ASSERT_EQ(1, response.tablet_repair_statuses_size());
        ASSERT_EQ(metadata_to_repair.id(), response.tablet_repair_statuses(0).tablet_id());
        ASSERT_EQ(TStatusCode::IO_ERROR, response.tablet_repair_statuses(0).status().status_code());
        ASSERT_TRUE(MatchPattern(response.tablet_repair_statuses(0).status().error_msgs(0),
                                 "injected put tablet metadata error"));
    }

    // 7.2 put_bundle_tablet_metadata fail (bundling)
    {
        TEST_ENABLE_ERROR_POINT("TabletManager::put_bundle_tablet_metadata",
                                Status::IOError("injected put bundle tablet metadata error"));
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("TabletManager::put_bundle_tablet_metadata");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        brpc::Controller cntl;
        RepairTabletMetadataRequest request;
        RepairTabletMetadataResponse response;

        TabletMetadataPB metadata_to_repair;
        metadata_to_repair.set_id(next_id());
        metadata_to_repair.set_version(100);
        request.add_tablet_metadatas()->CopyFrom(metadata_to_repair);
        request.set_enable_file_bundling(true);
        request.set_write_bundling_file(true);

        _lake_service.repair_tablet_metadata(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(TStatusCode::OK, response.status().status_code());
        ASSERT_EQ(1, response.tablet_repair_statuses_size());
        ASSERT_EQ(0, response.tablet_repair_statuses(0).tablet_id());
        ASSERT_EQ(TStatusCode::IO_ERROR, response.tablet_repair_statuses(0).status().status_code());
        ASSERT_TRUE(MatchPattern(response.tablet_repair_statuses(0).status().error_msgs(0),
                                 "injected put bundle tablet metadata error"));
    }

    // 7.3 task cancelled
    {
        class MockRunnable : public Runnable {
        public:
            MockRunnable() {}
            virtual ~MockRunnable() override {}
            virtual void run() override {}
            virtual void cancel() override {}
        };

        SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:replace_task", [](void* arg) {
            auto ptr = (*(std::shared_ptr<Runnable>*)arg);
            ptr->cancel();
            (*(std::shared_ptr<Runnable>*)arg) = std::make_shared<MockRunnable>();
        });
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:replace_task");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        {
            // test bundling cancelled
            brpc::Controller cntl;
            RepairTabletMetadataRequest request;
            RepairTabletMetadataResponse response;

            TabletMetadataPB metadata_to_repair;
            metadata_to_repair.set_id(next_id());
            metadata_to_repair.set_version(100);
            request.add_tablet_metadatas()->CopyFrom(metadata_to_repair);
            request.set_enable_file_bundling(true);
            request.set_write_bundling_file(true);

            _lake_service.repair_tablet_metadata(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(TStatusCode::OK, response.status().status_code());
            ASSERT_EQ(1, response.tablet_repair_statuses_size());
            ASSERT_EQ(0, response.tablet_repair_statuses(0).tablet_id());
            ASSERT_EQ(TStatusCode::CANCELLED, response.tablet_repair_statuses(0).status().status_code());
            ASSERT_TRUE(MatchPattern(response.tablet_repair_statuses(0).status().error_msgs(0),
                                     "*repair bundling tablet metadata task has been cancelled*"));
        }

        {
            // test non-bundling cancelled
            brpc::Controller cntl;
            RepairTabletMetadataRequest request;
            RepairTabletMetadataResponse response;

            TabletMetadataPB metadata_to_repair;
            metadata_to_repair.set_id(next_id());
            metadata_to_repair.set_version(100);
            request.add_tablet_metadatas()->CopyFrom(metadata_to_repair);
            request.set_enable_file_bundling(false);

            _lake_service.repair_tablet_metadata(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(0, response.status().status_code());
            ASSERT_EQ(1, response.tablet_repair_statuses_size());
            ASSERT_EQ(metadata_to_repair.id(), response.tablet_repair_statuses(0).tablet_id());
            ASSERT_EQ(TStatusCode::CANCELLED, response.tablet_repair_statuses(0).status().status_code());
            ASSERT_TRUE(MatchPattern(response.tablet_repair_statuses(0).status().error_msgs(0),
                                     "*repair tablet metadata task has been cancelled*"));
        }
    }
}

// Test cases for get_txn_ids_string function
// This function extracts txn_ids from PublishVersionRequest, supporting both new and old FE versions
TEST_F(LakeServiceTest, test_get_txn_ids_string_with_txn_infos) {
    // Test case 1: txn_infos has data (new FE version)
    {
        PublishVersionRequest request;
        auto* txn_info1 = request.add_txn_infos();
        txn_info1->set_txn_id(12345);
        auto* txn_info2 = request.add_txn_infos();
        txn_info2->set_txn_id(67890);

        std::string result = get_txn_ids_string(&request);
        EXPECT_EQ("12345,67890", result);
    }

    // Test case 2: single txn_info
    {
        PublishVersionRequest request;
        auto* txn_info = request.add_txn_infos();
        txn_info->set_txn_id(99999);

        std::string result = get_txn_ids_string(&request);
        EXPECT_EQ("99999", result);
    }
}

TEST_F(LakeServiceTest, test_get_txn_ids_string_with_txn_ids) {
    // Test case 3: txn_ids has data (old FE version, txn_infos is empty)
    {
        PublishVersionRequest request;
        request.add_txn_ids(11111);
        request.add_txn_ids(22222);
        request.add_txn_ids(33333);

        std::string result = get_txn_ids_string(&request);
        EXPECT_EQ("11111,22222,33333", result);
    }

    // Test case 4: single txn_id
    {
        PublishVersionRequest request;
        request.add_txn_ids(44444);

        std::string result = get_txn_ids_string(&request);
        EXPECT_EQ("44444", result);
    }
}

TEST_F(LakeServiceTest, test_get_txn_ids_string_empty) {
    // Test case 5: both txn_infos and txn_ids are empty
    {
        PublishVersionRequest request;

        std::string result = get_txn_ids_string(&request);
        EXPECT_EQ("", result);
    }
}

TEST_F(LakeServiceTest, test_get_txn_ids_string_priority) {
    // Test case 6: both txn_infos and txn_ids have data, txn_infos takes priority
    {
        PublishVersionRequest request;
        // Add txn_ids (old way)
        request.add_txn_ids(11111);
        request.add_txn_ids(22222);
        // Add txn_infos (new way) - should take priority
        auto* txn_info = request.add_txn_infos();
        txn_info->set_txn_id(99999);

        std::string result = get_txn_ids_string(&request);
        // Should use txn_infos since it has data
        EXPECT_EQ("99999", result);
    }
}

} // namespace starrocks
