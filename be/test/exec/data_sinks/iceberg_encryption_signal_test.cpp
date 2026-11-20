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

#include "base/testutil/assert.h"
#include "connector/iceberg/iceberg_chunk_sink.h"
#include "exec/data_sinks/iceberg_table_sink_pipeline_builder.h"
#include "gen_cpp/DataSinks_types.h"

namespace starrocks {

// The FE→BE encryption signal reaches BE at two points: INSERT's data sink, which applies it, and the
// position-delete sinks (DELETE, UPDATE / MERGE), which must refuse it. Dropping it on the data sink
// writes plaintext into a table that declares encryption. That does eventually fail closed, at the FE
// commit guard, but only after the BE has written the files, so it is worth catching here.
class IcebergEncryptionSignalTest : public testing::Test {
protected:
    static TIcebergTableSink sink_with_signal(bool set_dek_length, const std::string& algorithm, int32_t dek_length) {
        TIcebergTableSink t_sink;
        TParquetEncryptionInfo enc;
        if (!algorithm.empty()) {
            enc.__set_encryption_algorithm(algorithm);
        }
        if (set_dek_length) {
            enc.__set_dek_length(dek_length);
        }
        t_sink.__set_parquet_encryption_info(enc);
        return t_sink;
    }
};

TEST_F(IcebergEncryptionSignalTest, DataContextTakesTheSignal) {
    TIcebergTableSink t_sink = sink_with_signal(true, "AES_GCM_V1", 32);
    connector::IcebergChunkSinkContext ctx;

    ASSERT_OK(apply_iceberg_encryption_signal(t_sink, &ctx));
    ASSERT_TRUE(ctx.encryption_enabled);
    ASSERT_EQ("AES_GCM_V1", ctx.encryption_algorithm);
    ASSERT_EQ(32, ctx.encryption_dek_length);
}

TEST_F(IcebergEncryptionSignalTest, DataContextUntouchedWithoutASignal) {
    TIcebergTableSink t_sink; // parquet_encryption_info unset: an unencrypted table, or an old FE
    connector::IcebergChunkSinkContext ctx;

    ASSERT_OK(apply_iceberg_encryption_signal(t_sink, &ctx));
    ASSERT_FALSE(ctx.encryption_enabled);
    ASSERT_EQ(0, ctx.encryption_dek_length);
}

// The length must never be defaulted here: a guess could be weaker than the table's
// encryption.data-key-length policy, and the file would be committed with nothing surfaced.
TEST_F(IcebergEncryptionSignalTest, DataContextRefusesAMissingDekLength) {
    TIcebergTableSink t_sink = sink_with_signal(false, "AES_GCM_V1", 0);
    connector::IcebergChunkSinkContext ctx;

    Status st = apply_iceberg_encryption_signal(t_sink, &ctx);
    ASSERT_TRUE(st.is_invalid_argument()) << st.to_string();
    ASSERT_TRUE(st.to_string().find("no DEK length") != std::string::npos) << st.to_string();
}

// An absent or empty algorithm leaves the field alone rather than writing an empty string, so the
// writer's own default applies instead of an unparseable cipher name.
TEST_F(IcebergEncryptionSignalTest, DataContextLeavesAnEmptyAlgorithmAlone) {
    TIcebergTableSink t_sink = sink_with_signal(true, "", 16);
    connector::IcebergChunkSinkContext ctx;
    ctx.encryption_algorithm = "AES_GCM_V1";

    ASSERT_OK(apply_iceberg_encryption_signal(t_sink, &ctx));
    ASSERT_TRUE(ctx.encryption_enabled);
    ASSERT_EQ("AES_GCM_V1", ctx.encryption_algorithm);
    ASSERT_EQ(16, ctx.encryption_dek_length);
}

// The delete context is a sibling of the chunk context, not a subclass, and its writer factory reads
// ctx->options -- so the same signal has to be expressed as string keys. Position-delete files on an
// encrypted table were written as plaintext until this existed.
// DELETE and UPDATE / MERGE write position-delete files, which an encrypted (format version 3) table
// cannot accept. FE never signals encryption to those sinks; if it does, BE must refuse rather than write
// plaintext delete files.
TEST_F(IcebergEncryptionSignalTest, PositionDeleteSinksRefuseTheSignal) {
    TIcebergTableSink t_sink = sink_with_signal(true, "AES_GCM_V1", 32);

    Status st = refuse_iceberg_encryption_signal_for_position_deletes(t_sink);
    ASSERT_TRUE(st.is_not_supported()) << st.to_string();
    ASSERT_TRUE(st.to_string().find("position delete files") != std::string::npos) << st.to_string();
}

TEST_F(IcebergEncryptionSignalTest, PositionDeleteSinksAcceptNoSignal) {
    TIcebergTableSink t_sink;
    ASSERT_OK(refuse_iceberg_encryption_signal_for_position_deletes(t_sink));
}

} // namespace starrocks
