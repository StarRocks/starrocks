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
#include "connector/iceberg/iceberg_delete_sink.h"
#include "exec/data_sinks/iceberg_table_sink_pipeline_builder.h"
#include "gen_cpp/DataSinks_types.h"

namespace starrocks {

// The FE→BE encryption signal is translated onto a sink context in exactly two places, and both had
// no coverage at all. There are four call sites -- data and delete contexts, for INSERT and for
// DELETE/UPDATE/MERGE -- and dropping the signal at any of them writes plaintext into a table that
// declares encryption. That does eventually fail closed, at the FE commit guard, but only after the
// BE has written the files, so it is worth catching here.
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
TEST_F(IcebergEncryptionSignalTest, DeleteContextTakesTheSignalAsOptions) {
    TIcebergTableSink t_sink = sink_with_signal(true, "AES_GCM_V1", 32);
    connector::IcebergDeleteSinkContext ctx;

    ASSERT_OK(apply_iceberg_encryption_signal_to_delete_ctx(t_sink, &ctx));
    ASSERT_EQ("true", ctx.options["encryption_enabled"]);
    ASSERT_EQ("AES_GCM_V1", ctx.options["encryption_algorithm"]);
    ASSERT_EQ("32", ctx.options["encryption_dek_length"]);
}

TEST_F(IcebergEncryptionSignalTest, DeleteContextUntouchedWithoutASignal) {
    TIcebergTableSink t_sink;
    connector::IcebergDeleteSinkContext ctx;

    ASSERT_OK(apply_iceberg_encryption_signal_to_delete_ctx(t_sink, &ctx));
    ASSERT_TRUE(ctx.options.find("encryption_enabled") == ctx.options.end());
}

TEST_F(IcebergEncryptionSignalTest, DeleteContextRefusesAMissingDekLength) {
    TIcebergTableSink t_sink = sink_with_signal(false, "AES_GCM_V1", 0);
    connector::IcebergDeleteSinkContext ctx;

    Status st = apply_iceberg_encryption_signal_to_delete_ctx(t_sink, &ctx);
    ASSERT_TRUE(st.is_invalid_argument()) << st.to_string();
    ASSERT_TRUE(st.to_string().find("no DEK length") != std::string::npos) << st.to_string();
    // And nothing partially applied: encryption must not look enabled with no length behind it.
    ASSERT_TRUE(ctx.options.find("encryption_enabled") == ctx.options.end());
}

} // namespace starrocks
