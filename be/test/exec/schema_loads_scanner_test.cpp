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

#include "exec/schema_scanner/schema_loads_scanner.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "types/timestamp_value.h"

namespace starrocks {

// BE-side coverage for the FE->BE epoch-ms contract introduced for
// information_schema.loads. The scanner's start() goes through a thrift RPC to
// FE; the test bypasses that by populating _result directly and exercises the
// fill_chunk path (and specifically _fill_datetime_column_from_ms). Access to
// private fields/methods is allowed because BE unit tests are compiled with
// -fno-access-control (see be/CMakeLists.txt).
class SchemaLoadsScannerTest : public ::testing::Test {
protected:
    void SetUp() override {
        _params.ip = &_ip;
        _params.port = 9020;
        _state = std::make_unique<RuntimeState>(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    }

    // Configure the scanner for a specific session timezone, mirroring the
    // production path where _runtime_state->timezone_obj() drives the
    // wall-clock rendering of every DATETIME column.
    void init_scanner(SchemaLoadsScanner& scanner, const std::string& session_tz) {
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

    // Construct a minimal TLoadInfo with required string fields populated; the
    // caller layers DATETIME fields on top per scenario.
    TLoadInfo make_min_load_info(int64_t job_id) {
        TLoadInfo info;
        info.__set_job_id(job_id);
        info.__set_label("test_label_" + std::to_string(job_id));
        info.__set_db("test_db");
        info.__set_table("test_tbl");
        info.__set_user("test_user");
        info.__set_state("FINISHED");
        info.__set_progress("100%");
        info.__set_type("BROKER");
        info.__set_priority("NORMAL");
        info.__set_num_scan_rows(0);
        info.__set_num_scan_bytes(0);
        info.__set_num_filtered_rows(0);
        info.__set_num_unselected_rows(0);
        info.__set_num_sink_rows(0);
        info.__set_properties("{}");
        return info;
    }

    // Extract the TimestampValue from the nullable DATETIME column at the
    // given 1-indexed slot id, expecting exactly one materialized row.
    TimestampValue read_datetime(const ChunkPtr& chunk, int slot_id) {
        auto* column = chunk->get_column_raw_ptr_by_slot_id(slot_id);
        auto* nullable = down_cast<NullableColumn*>(column);
        EXPECT_FALSE(nullable->is_null(0));
        auto* data = down_cast<TimestampColumn*>(nullable->data_column_raw_ptr());
        return data->get_data()[0];
    }

    bool is_null_at(const ChunkPtr& chunk, int slot_id) {
        auto* column = chunk->get_column_raw_ptr_by_slot_id(slot_id);
        return down_cast<NullableColumn*>(column)->is_null(0);
    }

    SchemaScannerParam _params;
    std::string _ip = "127.0.0.1";
    ObjectPool _pool;
    std::unique_ptr<RuntimeState> _state;

    // Slot ids for the four DATETIME columns under test (matches the case
    // labels in SchemaLoadsScanner::fill_chunk).
    static constexpr int CREATE_TIME = 18;
    static constexpr int LOAD_START_TIME = 19;
    static constexpr int LOAD_COMMIT_TIME = 20;
    static constexpr int LOAD_FINISH_TIME = 21;
};

// Pin the wall-clock that an epoch-ms job timestamp renders to in two
// different session zones. This is the symptom the PR fixes: the same UTC
// instant must show up as the local wall-clock of whoever is querying, with
// no UTC+8 contamination from the legacy string path.
TEST_F(SchemaLoadsScannerTest, ms_field_materializes_in_session_zone) {
    // 2026-05-15 06:45:08.123 UTC.
    const int64_t epoch_ms = 1778827508123L;

    struct Case {
        std::string session_tz;
        std::string expected_wallclock;
    };
    const std::vector<Case> cases = {
            // UTC+8 baseline (the legacy zone, which used to be hard-coded).
            {"Asia/Shanghai", "2026-05-15 14:45:08"},
            // EDT in May (DST-active; this is the case to_unixtime(zone) used
            // to silently mis-handle by resolving the offset at the epoch).
            {"America/New_York", "2026-05-15 02:45:08"},
            {"UTC", "2026-05-15 06:45:08"},
    };

    for (const auto& c : cases) {
        SchemaLoadsScanner scanner;
        init_scanner(scanner, c.session_tz);

        TLoadInfo info = make_min_load_info(1);
        info.__set_create_time_ms(epoch_ms);
        info.__set_load_start_time_ms(epoch_ms);
        info.__set_load_commit_time_ms(epoch_ms);
        info.__set_load_finish_time_ms(epoch_ms);
        scanner._result.loads = {info};
        scanner._cur_idx = 0;

        auto chunk = create_chunk(scanner.get_slot_descs());
        bool eos = false;
        EXPECT_OK(scanner.get_next(&chunk, &eos));
        EXPECT_EQ(1, chunk->num_rows()) << c.session_tz;

        // Drop the sub-second part for the wall-clock comparison; sub-second
        // precision is covered by a dedicated test below.
        for (int slot : {CREATE_TIME, LOAD_START_TIME, LOAD_COMMIT_TIME, LOAD_FINISH_TIME}) {
            auto ts = read_datetime(chunk, slot);
            EXPECT_EQ(c.expected_wallclock, ts.to_string(/*ignore_microsecond=*/true))
                    << "tz=" << c.session_tz << " slot=" << slot;
        }
    }
}

// information_schema.loads is a second-precision view (the legacy "YYYY-MM-DD
// HH:MM:SS" wire format never carried fractional seconds, and the column
// filler at schema_column_filler.h drops microseconds when building a
// TimestampValue). Pin that the materialized column truncates to whole
// seconds regardless of any ms remainder in the wire payload.
TEST_F(SchemaLoadsScannerTest, ms_field_is_truncated_to_second_precision) {
    SchemaLoadsScanner scanner;
    init_scanner(scanner, "UTC");

    const int64_t epoch_ms = 1778827508789L; // 2026-05-15 06:45:08.789 UTC.

    TLoadInfo info = make_min_load_info(1);
    info.__set_create_time_ms(epoch_ms);
    scanner._result.loads = {info};
    scanner._cur_idx = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    EXPECT_OK(scanner.get_next(&chunk, &eos));

    auto ts = read_datetime(chunk, CREATE_TIME);
    // Wall-clock rendered at second precision; microsecond field is zero.
    EXPECT_EQ("2026-05-15 06:45:08", ts.to_string(/*ignore_microsecond=*/true));
    EXPECT_EQ(std::string::npos, ts.to_string(/*ignore_microsecond=*/false).find("789"))
            << "rendered: " << ts.to_string(false);
}

// Old BE never sets the ms field. New BE must keep parsing the legacy
// wall-clock string for compatibility, preserving the pre-fix semantics.
TEST_F(SchemaLoadsScannerTest, legacy_string_fallback_when_ms_unset) {
    SchemaLoadsScanner scanner;
    init_scanner(scanner, "Asia/Shanghai");

    TLoadInfo info = make_min_load_info(1);
    // Intentionally leave *_ms unset; only the legacy string field is provided.
    info.__set_create_time("2026-05-15 14:45:08");
    scanner._result.loads = {info};
    scanner._cur_idx = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    EXPECT_OK(scanner.get_next(&chunk, &eos));

    auto ts = read_datetime(chunk, CREATE_TIME);
    EXPECT_EQ("2026-05-15 14:45:08", ts.to_string(true));
    // The other three datetime columns are neither ms nor legacy-string set;
    // they must render NULL rather than silently appearing as epoch.
    EXPECT_TRUE(is_null_at(chunk, LOAD_START_TIME));
    EXPECT_TRUE(is_null_at(chunk, LOAD_COMMIT_TIME));
    EXPECT_TRUE(is_null_at(chunk, LOAD_FINISH_TIME));
}

// Predicate-literal conversion: a well-defined (UNIQUE) civil time maps to
// the same second-aligned epoch ms for both lower and upper bounds. Pre-fix
// the FE-side string parse produced the same second-aligned value, so this
// matches historical behavior.
TEST_F(SchemaLoadsScannerTest, literal_to_epoch_ms_unique_civil_time) {
    cctz::time_zone utc;
    ASSERT_TRUE(cctz::load_time_zone("UTC", &utc));

    // 2026-05-15 06:45:08 UTC is unix second 1778827508.
    const cctz::civil_second cs(2026, 5, 15, 6, 45, 8);
    const int64_t second_aligned = 1778827508L * 1000;

    EXPECT_EQ(second_aligned, SchemaLoadsScanner::literal_to_epoch_ms(utc, cs, /*is_lower_bound=*/true));
    EXPECT_EQ(second_aligned, SchemaLoadsScanner::literal_to_epoch_ms(utc, cs, /*is_lower_bound=*/false));
}

// DST fall-back (REPEATED civil time): the same wall-clock occurs at two
// distinct UTC instants. Lower bound picks the earlier; upper bound the
// later. America/New_York 2026-11-01 01:30 sits inside the fall-back hour:
// 01:30 EDT (= 05:30 UTC) and 01:30 EST (= 06:30 UTC) are both valid
// interpretations.
TEST_F(SchemaLoadsScannerTest, literal_to_epoch_ms_handles_dst_repeated) {
    cctz::time_zone ny;
    ASSERT_TRUE(cctz::load_time_zone("America/New_York", &ny));

    const cctz::civil_second cs(2026, 11, 1, 1, 30, 0);
    const int64_t edt_ms = 1793511000L * 1000; // 2026-11-01 05:30:00 UTC
    const int64_t est_ms = 1793514600L * 1000; // 2026-11-01 06:30:00 UTC

    EXPECT_EQ(edt_ms, SchemaLoadsScanner::literal_to_epoch_ms(ny, cs, /*is_lower_bound=*/true));
    EXPECT_EQ(est_ms, SchemaLoadsScanner::literal_to_epoch_ms(ny, cs, /*is_lower_bound=*/false));
}

// DST spring-forward (SKIPPED civil time): the wall-clock doesn't exist; cctz
// returns two candidates with the pre/post offsets, and they are swapped
// relative to the temporal order (lookup.pre uses the pre-transition offset
// and is the LATER instant; lookup.post the EARLIER). min/max(pre,post) must
// disambiguate by absolute time, not by struct name. America/New_York
// 2026-03-08 02:30 sits inside the spring-forward gap.
TEST_F(SchemaLoadsScannerTest, literal_to_epoch_ms_handles_dst_skipped) {
    cctz::time_zone ny;
    ASSERT_TRUE(cctz::load_time_zone("America/New_York", &ny));

    const cctz::civil_second cs(2026, 3, 8, 2, 30, 0);
    // Interpreted with EDT (post-transition, -4): 02:30 -> 06:30 UTC = 1772951400.
    // Interpreted with EST (pre-transition, -5):  02:30 -> 07:30 UTC = 1772955000.
    const int64_t earlier_ms = 1772951400L * 1000;
    const int64_t later_ms = 1772955000L * 1000;

    EXPECT_EQ(earlier_ms, SchemaLoadsScanner::literal_to_epoch_ms(ny, cs, /*is_lower_bound=*/true));
    EXPECT_EQ(later_ms, SchemaLoadsScanner::literal_to_epoch_ms(ny, cs, /*is_lower_bound=*/false));
}

// Defensive branch: when the ms field is technically set but carries 0/-1, BE
// renders NULL rather than 1970. FE writers (LoadJob, StreamLoadTask,
// MergeCommitTask) never actually send those sentinels - they leave the field
// unset - so this exercises pure defense in depth.
TEST_F(SchemaLoadsScannerTest, sentinel_ms_renders_null) {
    SchemaLoadsScanner scanner;
    init_scanner(scanner, "UTC");

    TLoadInfo info = make_min_load_info(1);
    info.__set_create_time_ms(0);
    info.__set_load_start_time_ms(-1);
    scanner._result.loads = {info};
    scanner._cur_idx = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    EXPECT_OK(scanner.get_next(&chunk, &eos));

    EXPECT_TRUE(is_null_at(chunk, CREATE_TIME));
    EXPECT_TRUE(is_null_at(chunk, LOAD_START_TIME));
}

} // namespace starrocks
