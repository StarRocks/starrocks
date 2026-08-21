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

#include "schema_scanner/schema_running_transactions_scanner.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "runtime/runtime_state.h"
#include "types/timestamp_value.h"

namespace starrocks {

// BE-side coverage for information_schema.running_transactions. The scanner's start() goes through a thrift
// RPC to the FE; the test bypasses that by populating _result directly and exercises the fill_chunk path
// (scalar columns and specifically _fill_datetime_column_from_ms's epoch-ms -> DATETIME contract). Access
// to private fields/methods is allowed because BE unit tests are compiled with -fno-access-control.
class SchemaRunningTransactionsScannerTest : public ::testing::Test {
protected:
    void SetUp() override {
        _params.ip = &_ip;
        _params.port = 9020;
        _state = std::make_unique<RuntimeState>(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    }

    void init_scanner(SchemaRunningTransactionsScanner& scanner, const std::string& session_tz) {
        EXPECT_TRUE(_state->set_timezone(session_tz));
        EXPECT_OK(scanner.init(&_params, &_pool));
        scanner._runtime_state = _state.get();
    }

    ChunkPtr create_chunk(const std::vector<SlotDescriptor*>& slot_descs) {
        ChunkPtr chunk = std::make_shared<Chunk>();
        for (const auto* slot_desc : slot_descs) {
            MutableColumnPtr column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
            chunk->append_column(std::move(column), slot_desc->id());
        }
        return chunk;
    }

    // A running transaction with the always-set scalar fields populated; the caller layers timestamps and
    // optional strings on top per scenario.
    TRunningTxnInfo make_min_txn_info(int64_t txn_id, const std::string& state) {
        TRunningTxnInfo info;
        info.__set_txn_id(txn_id);
        info.__set_global_txn_id(0);
        info.__set_label("test_label_" + std::to_string(txn_id));
        info.__set_database_id(1001);
        info.__set_state(state);
        info.__set_warehouse_id(0);
        info.__set_pending_publish_ms(0);
        info.__set_timeout_ms(0);
        info.__set_prepared_timeout_ms(0);
        info.__set_error_replica_num(0);
        info.__set_is_no_op_publish(false);
        return info;
    }

    ChunkPtr scan_one(SchemaRunningTransactionsScanner& scanner, const TRunningTxnInfo& info) {
        scanner._result.txns = {info};
        scanner._cur_idx = 0;
        ChunkPtr chunk = create_chunk(scanner.get_slot_descs());
        bool eos = false;
        EXPECT_OK(scanner.get_next(&chunk, &eos));
        EXPECT_EQ(1, chunk->num_rows());
        return chunk;
    }

    int64_t read_bigint(const ChunkPtr& chunk, int slot_id) {
        auto* column = chunk->get_column_raw_ptr_by_slot_id(slot_id);
        return down_cast<Int64Column*>(ColumnHelper::get_data_column(column))->get_data()[0];
    }

    std::string read_string(const ChunkPtr& chunk, int slot_id) {
        auto* column = chunk->get_column_raw_ptr_by_slot_id(slot_id);
        return down_cast<BinaryColumn*>(ColumnHelper::get_data_column(column))->get_slice(0).to_string();
    }

    bool read_bool(const ChunkPtr& chunk, int slot_id) {
        auto* column = chunk->get_column_raw_ptr_by_slot_id(slot_id);
        return down_cast<BooleanColumn*>(ColumnHelper::get_data_column(column))->get_data()[0] != 0;
    }

    TimestampValue read_datetime(const ChunkPtr& chunk, int slot_id) {
        auto* column = chunk->get_column_raw_ptr_by_slot_id(slot_id);
        auto* nullable = down_cast<NullableColumn*>(column);
        EXPECT_FALSE(nullable->is_null(0));
        return down_cast<TimestampColumn*>(nullable->data_column_raw_ptr())->get_data()[0];
    }

    bool is_null_at(const ChunkPtr& chunk, int slot_id) {
        auto* column = chunk->get_column_raw_ptr_by_slot_id(slot_id);
        if (!column->is_nullable()) {
            return false;
        }
        return down_cast<NullableColumn*>(column)->is_null(0);
    }

    SchemaScannerParam _params;
    std::string _ip = "127.0.0.1";
    ObjectPool _pool;
    std::unique_ptr<RuntimeState> _state;

    // 1-indexed slot ids, matching RunningTransactionsSystemTable.create() column order and the case labels
    // in SchemaRunningTransactionsScanner::fill_chunk.
    static constexpr int TXN_ID = 1;
    static constexpr int LABEL = 3;
    static constexpr int STATE = 8;
    static constexpr int TABLE_NAMES = 7;
    static constexpr int PREPARE_TIME = 12;
    static constexpr int COMMIT_TIME = 14;
    static constexpr int PUBLISH_TIME = 15;
    static constexpr int FINISH_TIME = 16;
    static constexpr int PENDING_PUBLISH_MS = 17;
    static constexpr int REASON = 21;
    static constexpr int ERROR_MSG = 22;
    static constexpr int IS_NO_OP_PUBLISH = 23;
    static constexpr int NO_OP_PUBLISH_REASON = 24;
};

// Scalar columns (ids, label, state, the headline PENDING_PUBLISH_MS, the no-op-publish pair) copy through
// to the chunk verbatim.
TEST_F(SchemaRunningTransactionsScannerTest, scalar_columns_materialize) {
    SchemaRunningTransactionsScanner scanner;
    init_scanner(scanner, "UTC");

    TRunningTxnInfo info = make_min_txn_info(42, "COMMITTED");
    info.__set_pending_publish_ms(1234);
    info.__set_is_no_op_publish(true);
    info.__set_no_op_publish_reason("nothing to publish");

    auto chunk = scan_one(scanner, info);
    EXPECT_EQ(42, read_bigint(chunk, TXN_ID));
    EXPECT_EQ("test_label_42", read_string(chunk, LABEL));
    EXPECT_EQ("COMMITTED", read_string(chunk, STATE));
    EXPECT_EQ(1234, read_bigint(chunk, PENDING_PUBLISH_MS));
    EXPECT_TRUE(read_bool(chunk, IS_NO_OP_PUBLISH));
    EXPECT_EQ("nothing to publish", read_string(chunk, NO_OP_PUBLISH_REASON));
}

// Every timestamp is a UTC epoch-ms field rendered to the session zone (the loads epoch-ms contract, not the
// epoch-seconds mayCast path). The same instant shows as the querying session's local wall-clock.
TEST_F(SchemaRunningTransactionsScannerTest, ms_timestamps_materialize_in_session_zone) {
    const int64_t epoch_ms = 1778827508123L; // 2026-05-15 06:45:08.123 UTC

    struct Case {
        std::string session_tz;
        std::string expected_wallclock;
    };
    const std::vector<Case> cases = {
            {"UTC", "2026-05-15 06:45:08"},
            {"Asia/Shanghai", "2026-05-15 14:45:08"},
    };

    for (const auto& c : cases) {
        SchemaRunningTransactionsScanner scanner;
        init_scanner(scanner, c.session_tz);

        TRunningTxnInfo info = make_min_txn_info(1, "COMMITTED");
        info.__set_prepare_time_ms(epoch_ms);
        info.__set_commit_time_ms(epoch_ms);

        auto chunk = scan_one(scanner, info);
        EXPECT_EQ(c.expected_wallclock, read_datetime(chunk, PREPARE_TIME).to_string(true)) << c.session_tz;
        EXPECT_EQ(c.expected_wallclock, read_datetime(chunk, COMMIT_TIME).to_string(true)) << c.session_tz;
    }
}

// A running transaction has no finish time, and commit/publish may not have happened yet. Unset ms fields,
// and the 0/-1 sentinels, must render NULL rather than a 1970 epoch wall-clock.
TEST_F(SchemaRunningTransactionsScannerTest, unset_and_sentinel_timestamps_render_null) {
    SchemaRunningTransactionsScanner scanner;
    init_scanner(scanner, "UTC");

    TRunningTxnInfo info = make_min_txn_info(1, "PREPARE");
    // finish_time_ms left unset; commit/publish carry explicit sentinels.
    info.__set_commit_time_ms(-1);
    info.__set_publish_time_ms(0);

    auto chunk = scan_one(scanner, info);
    EXPECT_TRUE(is_null_at(chunk, FINISH_TIME));
    EXPECT_TRUE(is_null_at(chunk, COMMIT_TIME));
    EXPECT_TRUE(is_null_at(chunk, PUBLISH_TIME));
}

// The optional text/name columns render NULL when the FE leaves them unset (e.g. a running txn with no error
// reason yet, or a row before off-lock table-name resolution), and carry their value when set.
TEST_F(SchemaRunningTransactionsScannerTest, nullable_strings_unset_render_null) {
    SchemaRunningTransactionsScanner scanner;
    init_scanner(scanner, "UTC");

    TRunningTxnInfo info = make_min_txn_info(1, "PREPARE");
    // reason / table_names left unset; error_msg set.
    info.__set_error_msg("boom");

    auto chunk = scan_one(scanner, info);
    EXPECT_TRUE(is_null_at(chunk, REASON));
    EXPECT_TRUE(is_null_at(chunk, TABLE_NAMES));
    EXPECT_FALSE(is_null_at(chunk, ERROR_MSG));
    EXPECT_EQ("boom", read_string(chunk, ERROR_MSG));
}

} // namespace starrocks
