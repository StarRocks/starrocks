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

#include "runtime/load_fail_point.h"

#include <gtest/gtest.h>

namespace starrocks::load::failpoint {

TEST(LoadFailPointTest, tablet_writer_open) {
    RefCountClosure<PTabletWriterOpenResult> closure;
    PTabletWriterOpenRequest request;
    request.mutable_id()->set_hi(9382);
    request.mutable_id()->set_lo(482323);
    request.set_txn_id(123456);
    request.set_sender_id(1);
    auto tablet = request.add_tablets();
    tablet->set_tablet_id(1);
    tablet_writer_open_fp_action("127.0.0.1", &closure, &request);
    ASSERT_TRUE(closure.cntl.Failed());
    ASSERT_EQ(closure.cntl.ErrorText(),
              "load_tablet_writer_open failpoint triggered failure, rpc:  -> 127.0.0.1, txn_id: 123456");
}

TEST(LoadFailPointTest, tablet_writer_add_chunks) {
    ReusableClosure<PTabletWriterAddBatchResult> closure;
    PTabletWriterAddChunksRequest request;
    request.mutable_id()->set_hi(9382);
    request.mutable_id()->set_lo(482323);
    auto sub_request = request.add_requests();
    sub_request->mutable_id()->set_hi(9382);
    sub_request->mutable_id()->set_lo(482323);
    sub_request->set_txn_id(123456);
    sub_request->set_sender_id(1);
    tablet_writer_add_chunks_fp_action("127.0.0.1", &closure, &request);
    ASSERT_TRUE(closure.cntl.Failed());
    ASSERT_EQ(closure.cntl.ErrorText(),
              "load_tablet_writer_add_chunks failpoint triggered failure, rpc:  -> 127.0.0.1, txn_id: 123456");
}

TEST(LoadFailPointTest, tablet_writer_add_segment) {
    ReusableClosure<PTabletWriterAddSegmentResult> closure;
    PTabletWriterAddSegmentRequest request;
    request.mutable_id()->set_hi(9382);
    request.mutable_id()->set_lo(482323);
    request.set_txn_id(123456);
    request.set_tablet_id(7890);
    tablet_writer_add_segment_fp_action("127.0.0.1", &closure, &request);
    ASSERT_TRUE(closure.cntl.Failed());
    ASSERT_EQ(closure.cntl.ErrorText(),
              "load_tablet_writer_add_segment failpoint triggered failure, rpc:  -> 127.0.0.1, txn_id: 123456, "
              "tablet_id: 7890");
}

TEST(LoadFailPointTest, tablet_writer_cancel) {
    RefCountClosure<PTabletWriterCancelResult> closure;
    PTabletWriterCancelRequest request;
    request.mutable_id()->set_hi(9382);
    request.mutable_id()->set_lo(482323);
    request.set_txn_id(123456);
    request.add_tablet_ids(1);
    request.add_tablet_ids(2);
    tablet_writer_cancel_fp_action("127.0.0.1", &closure, &closure.cntl, &request);
    ASSERT_TRUE(closure.cntl.Failed());
    ASSERT_EQ(closure.cntl.ErrorText(),
              "load_tablet_writer_cancel failpoint triggered failure, rpc:  -> 127.0.0.1, txn_id: 123456");
}

TEST(LoadFailPointTest, memtable_flush) {
    auto status = memtable_flush_fp_action(123456, 7890);
    ASSERT_TRUE(status.is_io_error());
    ASSERT_EQ(status.message(),
              "load_memtable_flush failpoint triggered failure, be: , txn_id: 123456, tablet_id: 7890");
}

TEST(LoadFailPointTest, segment_flush) {
    auto status = segment_flush_fp_action(123456, 7890);
    ASSERT_TRUE(status.is_io_error());
    ASSERT_EQ(status.message(),
              "load_segment_flush failpoint triggered failure, be: , txn_id: 123456, tablet_id: 7890");
}

TEST(LoadFailPointTest, pk_preload) {
    auto status = pk_preload_fp_action(123456, 7890);
    ASSERT_TRUE(status.is_io_error());
    ASSERT_EQ(status.message(), "load_pk_preload failpoint triggered failure, be: , txn_id: 123456, tablet_id: 7890");
}

TEST(LoadFailPointTest, commit_txn) {
    auto status = commit_txn_fp_action(123456, 7890);
    ASSERT_TRUE(status.is_io_error());
    ASSERT_EQ(status.message(), "load_commit_txn failpoint triggered failure, be: , txn_id: 123456, tablet_id: 7890");
}

} // namespace starrocks::load::failpoint