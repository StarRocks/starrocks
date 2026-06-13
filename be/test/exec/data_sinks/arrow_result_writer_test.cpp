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

#include "exec/data_sinks/arrow_result_writer.h"

#include <arrow/api.h>
#include <gtest/gtest.h>
#include <pthread.h>

#include <string>

#include "common/runtime_profile.h"
#include "compute_env/result/buffer_control_block.h"
#include "compute_env/result/result_buffer_mgr.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/arrow/arrow_flight_compression.h"

namespace starrocks {

TEST(ArrowFlightCompressionTest, from_string_maps_codecs) {
    EXPECT_EQ(arrow::Compression::LZ4_FRAME, arrow_flight_compression_from_string("lz4"));
    EXPECT_EQ(arrow::Compression::LZ4_FRAME, arrow_flight_compression_from_string("LZ4"));
    EXPECT_EQ(arrow::Compression::ZSTD, arrow_flight_compression_from_string("zstd"));
    EXPECT_EQ(arrow::Compression::ZSTD, arrow_flight_compression_from_string("ZSTD"));
    EXPECT_EQ(arrow::Compression::UNCOMPRESSED, arrow_flight_compression_from_string("none"));
    EXPECT_EQ(arrow::Compression::UNCOMPRESSED, arrow_flight_compression_from_string(""));
    // Unknown values are defensively treated as uncompressed (FE rejects them at SET time).
    EXPECT_EQ(arrow::Compression::UNCOMPRESSED, arrow_flight_compression_from_string("gzip"));
}

TEST(ArrowFlightCompressionTest, resolve_session_overrides_config) {
    // Empty session value falls back to the BE config default.
    EXPECT_EQ(arrow::Compression::ZSTD, resolve_arrow_flight_compression("", "zstd"));
    // A non-empty session value wins, including explicit "none" to opt out.
    EXPECT_EQ(arrow::Compression::LZ4_FRAME, resolve_arrow_flight_compression("lz4", "zstd"));
    EXPECT_EQ(arrow::Compression::UNCOMPRESSED, resolve_arrow_flight_compression("none", "zstd"));
    // Both empty => uncompressed.
    EXPECT_EQ(arrow::Compression::UNCOMPRESSED, resolve_arrow_flight_compression("", ""));
}

TEST(ResultBufferMgrArrowCompressionTest, set_get_roundtrip) {
    ResultBufferMgr mgr;
    TUniqueId fragment_id;
    fragment_id.hi = 1;
    fragment_id.lo = 2;

    // Absent entry defaults to uncompressed.
    EXPECT_EQ(arrow::Compression::UNCOMPRESSED, mgr.get_arrow_compression(fragment_id));

    mgr.set_arrow_compression(fragment_id, arrow::Compression::ZSTD);
    EXPECT_EQ(arrow::Compression::ZSTD, mgr.get_arrow_compression(fragment_id));

    mgr.set_arrow_compression(fragment_id, arrow::Compression::LZ4_FRAME);
    EXPECT_EQ(arrow::Compression::LZ4_FRAME, mgr.get_arrow_compression(fragment_id));
}

TEST(ArrowResultWriterTest, close) {
    BufferControlBlock* sinker = new BufferControlBlock(TUniqueId(), 1024);
    ASSERT_TRUE(sinker->init().ok());

    std::vector<ExprContext*> output_expr_ctxs;
    std::vector<std::string> output_column_names;

    RowDescriptor row_desc;

    RuntimeProfile* profile = new RuntimeProfile("ArrowResultWriterTest");

    ArrowResultWriter writer(sinker, output_expr_ctxs, output_column_names, profile, row_desc);

    Status st = writer.close();
    ASSERT_TRUE(st.ok());

    delete profile;
    delete sinker;
}

} // namespace starrocks