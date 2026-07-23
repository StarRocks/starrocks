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

#include <gtest/gtest.h>

#include <thread>

#include "base/testutil/assert.h"
#include "compute_env/data_stream/data_stream_mgr.h"
#include "compute_env/data_stream/data_stream_recvr.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class ExchangeDynamicSendersTest : public ::testing::Test {
protected:
    void SetUp() override {
        _mgr = std::make_unique<DataStreamMgr>();
        _mgr->prepare_pass_through_chunk_buffer(_state.query_id());
    }

    void TearDown() override {
        if (_recvr != nullptr) {
            _recvr->close();
            _recvr.reset();
        }
        _mgr->close();
        _mgr->destroy_pass_through_chunk_buffer(_state.query_id());
    }

    std::shared_ptr<DataStreamRecvr> create_recvr(int num_senders, bool is_merging = false, bool is_pipeline = true) {
        _finst_id.hi = 20260716;
        _finst_id.lo = ++_next_instance;
        _recvr = _mgr->create_recvr(&_state, RowDescriptor(), _finst_id, _node_id, num_senders,
                                    /*buffer_size=*/1024 * 1024, is_merging, std::make_shared<QueryStatisticsRecvr>(),
                                    is_pipeline, /*degree_of_parallelism=*/1, /*keep_order=*/false);
        return _recvr;
    }

    // Drives EOS accounting through the public transmit path, like a real sender's last packet.
    Status send_eos(int be_number) {
        PTransmitChunkParams request;
        request.mutable_finst_id()->set_hi(_finst_id.hi);
        request.mutable_finst_id()->set_lo(_finst_id.lo);
        request.set_node_id(_node_id);
        request.set_sender_id(be_number);
        request.set_be_number(be_number);
        request.set_eos(true);
        request.set_sequence(0);
        google::protobuf::Closure* done = nullptr;
        return _mgr->transmit_chunk(request, &done);
    }

    Status update_senders(int be_number, bool unregister) {
        PUpdateExchangeSendersRequest request;
        request.mutable_finst_id()->set_hi(_finst_id.hi);
        request.mutable_finst_id()->set_lo(_finst_id.lo);
        request.set_node_id(_node_id);
        request.set_be_number(be_number);
        request.set_unregister(unregister);
        return _mgr->update_exchange_senders(request);
    }

    RuntimeState _state;
    std::unique_ptr<DataStreamMgr> _mgr;
    std::shared_ptr<DataStreamRecvr> _recvr;
    TUniqueId _finst_id;
    int64_t _next_instance = 0;
    const int _node_id = 7;
};

TEST_F(ExchangeDynamicSendersTest, register_extends_stream) {
    auto recvr = create_recvr(2);
    ASSERT_OK(send_eos(0));
    ASSERT_FALSE(recvr->is_finished());

    ASSERT_OK(update_senders(5, /*unregister=*/false));
    ASSERT_OK(send_eos(1));
    // The dynamically registered sender is still live.
    ASSERT_FALSE(recvr->is_finished());
    ASSERT_OK(send_eos(5));
    ASSERT_TRUE(recvr->is_finished());
}

TEST_F(ExchangeDynamicSendersTest, duplicate_register_is_noop) {
    auto recvr = create_recvr(2);
    ASSERT_OK(update_senders(5, false));
    ASSERT_OK(update_senders(5, false));
    ASSERT_OK(send_eos(0));
    ASSERT_OK(send_eos(1));
    ASSERT_FALSE(recvr->is_finished());
    // One EOS finishes it: the duplicate registration must not have double-counted.
    ASSERT_OK(send_eos(5));
    ASSERT_TRUE(recvr->is_finished());
}

TEST_F(ExchangeDynamicSendersTest, register_after_finished_is_rejected) {
    auto recvr = create_recvr(1);
    ASSERT_OK(send_eos(0));
    ASSERT_TRUE(recvr->is_finished());
    Status st = update_senders(5, false);
    ASSERT_TRUE(st.is_internal_error()) << st;
    ASSERT_TRUE(recvr->is_finished());
}

TEST_F(ExchangeDynamicSendersTest, unregister_unknown_is_noop) {
    auto recvr = create_recvr(1);
    ASSERT_OK(update_senders(42, /*unregister=*/true));
    ASSERT_FALSE(recvr->is_finished());
    ASSERT_OK(send_eos(0));
    ASSERT_TRUE(recvr->is_finished());
}

TEST_F(ExchangeDynamicSendersTest, unregister_compensates_registration) {
    auto recvr = create_recvr(2);
    ASSERT_OK(update_senders(5, false));
    ASSERT_OK(update_senders(5, true));
    ASSERT_OK(send_eos(0));
    ASSERT_FALSE(recvr->is_finished());
    ASSERT_OK(send_eos(1));
    // The compensated sender must not be waited for.
    ASSERT_TRUE(recvr->is_finished());
}

TEST_F(ExchangeDynamicSendersTest, unregister_after_eos_is_noop) {
    auto recvr = create_recvr(2);
    ASSERT_OK(update_senders(5, false));
    ASSERT_OK(send_eos(5));
    // The sender finished normally; a late compensation must not double-decrement.
    ASSERT_OK(update_senders(5, true));
    ASSERT_OK(send_eos(0));
    ASSERT_FALSE(recvr->is_finished());
    ASSERT_OK(send_eos(1));
    ASSERT_TRUE(recvr->is_finished());
}

TEST_F(ExchangeDynamicSendersTest, merging_receiver_not_supported) {
    auto recvr = create_recvr(2, /*is_merging=*/true);
    Status st = update_senders(5, false);
    ASSERT_TRUE(st.is_not_supported()) << st;
}

TEST_F(ExchangeDynamicSendersTest, non_pipeline_receiver_not_supported) {
    auto recvr = create_recvr(2, /*is_merging=*/false, /*is_pipeline=*/false);
    Status st = update_senders(5, false);
    ASSERT_TRUE(st.is_not_supported()) << st;
}

TEST_F(ExchangeDynamicSendersTest, unknown_receiver_not_found) {
    create_recvr(1);
    PUpdateExchangeSendersRequest request;
    request.mutable_finst_id()->set_hi(_finst_id.hi);
    request.mutable_finst_id()->set_lo(_finst_id.lo + 1000);
    request.set_node_id(_node_id);
    request.set_be_number(5);
    Status st = _mgr->update_exchange_senders(request);
    ASSERT_TRUE(st.is_not_found()) << st;
}

TEST_F(ExchangeDynamicSendersTest, cancelled_receiver_is_rejected) {
    auto recvr = create_recvr(1);
    _mgr->cancel(_finst_id);
    Status st = update_senders(5, false);
    ASSERT_FALSE(st.ok()) << st;
}

TEST_F(ExchangeDynamicSendersTest, unregister_then_late_register_is_rejected) {
    // Register and unregister are independent RPCs with no ordering guarantee: a compensating
    // unregister can execute before the delayed register it compensates. The unregister must
    // tombstone the be_number so the late register cannot add a sender that will never deploy.
    auto recvr = create_recvr(2);
    ASSERT_OK(update_senders(7, /*unregister=*/true));
    Status st = update_senders(7, /*unregister=*/false);
    ASSERT_TRUE(st.is_internal_error()) << st;
    // Neither leg may have changed the sender count.
    ASSERT_OK(send_eos(0));
    ASSERT_FALSE(recvr->is_finished());
    ASSERT_OK(send_eos(1));
    ASSERT_TRUE(recvr->is_finished());
}

TEST_F(ExchangeDynamicSendersTest, register_after_close_is_rejected) {
    // A register racing receiver close must not be silently accepted for a stream nobody
    // will ever drain (e.g. LIMIT short-circuit closed the exchange source).
    auto recvr = create_recvr(1);
    recvr->close();
    Status st = recvr->update_senders(5, /*unregister=*/false);
    ASSERT_FALSE(st.ok()) << st;
    // Compensation after close stays a quiet no-op.
    ASSERT_OK(recvr->update_senders(5, /*unregister=*/true));
}

TEST_F(ExchangeDynamicSendersTest, concurrent_register_and_eos) {
    constexpr int kBaseSenders = 4;
    constexpr int kDynamicSenders = 32;
    auto recvr = create_recvr(kBaseSenders);

    // Register all dynamic senders concurrently with base senders' EOS: with the
    // register-before-deploy protocol, registrations only happen while at least one
    // base sender is still live, so keep base sender 0 alive until all are registered.
    std::thread registrar([&]() {
        for (int i = 0; i < kDynamicSenders; ++i) {
            ASSERT_OK(update_senders(100 + i, false));
        }
    });
    std::thread finisher([&]() {
        for (int i = 1; i < kBaseSenders; ++i) {
            ASSERT_OK(send_eos(i));
        }
    });
    registrar.join();
    finisher.join();

    ASSERT_OK(send_eos(0));
    ASSERT_FALSE(recvr->is_finished());
    for (int i = 0; i < kDynamicSenders; ++i) {
        ASSERT_OK(send_eos(100 + i));
    }
    ASSERT_TRUE(recvr->is_finished());
}

} // namespace starrocks
