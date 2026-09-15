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

#include "common/util/thrift_key_redaction.h"

#include <gtest/gtest.h>

#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {

// The status report carries the per-file Parquet DEK for Iceberg encrypted tables, and the fragment
// reporter dumps the whole struct at VLOG(10). A DEK in be.INFO outlives the query and is copied off
// the host by log collection, so the redaction is a security control -- and a silent regression in it
// (someone reverting to ThriftDebugString(params)) would leave no trace. Hence these tests.
class ThriftKeyRedactionTest : public testing::Test {
protected:
    static constexpr const char* kSecretDek = "SECRET_DEK_BYTES";
    static constexpr const char* kSecretPrefix = "SECRET_AAD_PREFIX";

    static TReportExecStatusParams params_with_key_material() {
        TReportExecStatusParams params;
        params.__set_query_id(TUniqueId());

        TIcebergDataFile data_file;
        data_file.__set_path("s3://bucket/db/t/data/enc.parquet");
        data_file.__set_record_count(42);
        data_file.__set_file_dek(kSecretDek);
        data_file.__set_aad_prefix(kSecretPrefix);

        TSinkCommitInfo commit_info;
        commit_info.__set_iceberg_data_file(data_file);
        params.__set_sink_commit_infos({commit_info});
        return params;
    }
};

TEST_F(ThriftKeyRedactionTest, KeyMaterialIsNotInTheDump) {
    TReportExecStatusParams params = params_with_key_material();
    const std::string dump = redacted_debug_string(params);

    ASSERT_EQ(std::string::npos, dump.find(kSecretDek)) << dump;
    ASSERT_EQ(std::string::npos, dump.find(kSecretPrefix)) << dump;
    ASSERT_NE(std::string::npos, dump.find("redacted")) << dump;
}

// Redaction must not cost the rest of the dump: this is a debugging aid, and gutting it would push
// whoever is debugging a sink issue back to logging the raw params.
TEST_F(ThriftKeyRedactionTest, EverythingElseIsStillShown) {
    TReportExecStatusParams params = params_with_key_material();
    const std::string dump = redacted_debug_string(params);

    ASSERT_NE(std::string::npos, dump.find("s3://bucket/db/t/data/enc.parquet")) << dump;
    ASSERT_NE(std::string::npos, dump.find("42")) << dump;
}

// The caller's struct is what goes on the wire; redacting must happen on a copy.
TEST_F(ThriftKeyRedactionTest, TheCallersParamsAreNotMutated) {
    TReportExecStatusParams params = params_with_key_material();
    (void)redacted_debug_string(params);

    ASSERT_EQ(kSecretDek, params.sink_commit_infos[0].iceberg_data_file.file_dek);
    ASSERT_EQ(kSecretPrefix, params.sink_commit_infos[0].iceberg_data_file.aad_prefix);
}

TEST_F(ThriftKeyRedactionTest, UnencryptedReportIsUnaffected) {
    TReportExecStatusParams params;
    params.__set_query_id(TUniqueId());
    TIcebergDataFile data_file;
    data_file.__set_path("s3://bucket/db/t/data/plain.parquet");
    TSinkCommitInfo commit_info;
    commit_info.__set_iceberg_data_file(data_file);
    params.__set_sink_commit_infos({commit_info});

    const std::string dump = redacted_debug_string(params);
    ASSERT_NE(std::string::npos, dump.find("s3://bucket/db/t/data/plain.parquet")) << dump;
    // Nothing to redact, so no placeholder should appear.
    ASSERT_EQ(std::string::npos, dump.find("redacted")) << dump;
}

TEST_F(ThriftKeyRedactionTest, ReportWithNoCommitInfosIsUnaffected) {
    TReportExecStatusParams params;
    params.__set_query_id(TUniqueId());

    const std::string dump = redacted_debug_string(params);
    ASSERT_FALSE(dump.empty());
    ASSERT_EQ(std::string::npos, dump.find("redacted")) << dump;
}

// The read side is the other direction and was missed on the first pass: the DEK FE recovered from
// key_metadata travels out to BE on every scan range, and TExecPlanFragmentParams is dumped in full at
// VLOG by both the backend service entrypoint and the external-plan path.
class ExecFragmentKeyRedactionTest : public testing::Test {
protected:
    static constexpr const char* kScanDek = "SCAN_SIDE_DEK";
    static constexpr const char* kDeleteDek = "DELETE_FILE_DEK";

    static TExecPlanFragmentParams params_with_scan_keys() {
        TExecPlanFragmentParams params;

        TParquetEncryptionInfo data_enc;
        data_enc.__set_file_dek(kScanDek);
        data_enc.__set_aad_prefix("SCAN_AAD");

        THdfsScanRange hdfs_range;
        hdfs_range.__set_full_path("s3://bucket/db/t/data/enc.parquet");
        hdfs_range.__set_parquet_encryption_info(data_enc);

        // A position-delete file carries its own key on its own field.
        TIcebergDeleteFile delete_file;
        delete_file.__set_full_path("s3://bucket/db/t/data/deletes.parquet");
        TParquetEncryptionInfo delete_enc;
        delete_enc.__set_file_dek(kDeleteDek);
        delete_file.__set_parquet_encryption_info(delete_enc);
        hdfs_range.__set_delete_files({delete_file});

        TScanRange scan_range;
        scan_range.__set_hdfs_scan_range(hdfs_range);
        TScanRangeParams srp;
        srp.__set_scan_range(scan_range);

        TPlanFragmentExecParams exec_params;
        exec_params.per_node_scan_ranges = {{0, {srp}}};
        params.__set_params(exec_params);
        return params;
    }
};

TEST_F(ExecFragmentKeyRedactionTest, ScanRangeAndDeleteFileKeysAreNotInTheDump) {
    TExecPlanFragmentParams params = params_with_scan_keys();
    const std::string dump = redacted_debug_string(params);

    ASSERT_EQ(std::string::npos, dump.find(kScanDek)) << dump;
    ASSERT_EQ(std::string::npos, dump.find(kDeleteDek)) << dump;
    ASSERT_EQ(std::string::npos, dump.find("SCAN_AAD")) << dump;
    ASSERT_NE(std::string::npos, dump.find("redacted")) << dump;
    // The rest of the scan range must still be visible; this is a debugging aid.
    ASSERT_NE(std::string::npos, dump.find("s3://bucket/db/t/data/enc.parquet")) << dump;
}

TEST_F(ExecFragmentKeyRedactionTest, TheCallersParamsAreNotMutated) {
    TExecPlanFragmentParams params = params_with_scan_keys();
    (void)redacted_debug_string(params);

    const auto& srp = params.params.per_node_scan_ranges.at(0)[0];
    ASSERT_EQ(kScanDek, srp.scan_range.hdfs_scan_range.parquet_encryption_info.file_dek);
    ASSERT_EQ(kDeleteDek, srp.scan_range.hdfs_scan_range.delete_files[0].parquet_encryption_info.file_dek);
}

// An unencrypted scan takes the no-copy path; it must still dump normally.
TEST_F(ExecFragmentKeyRedactionTest, PlaintextScanIsUnaffected) {
    TExecPlanFragmentParams params;
    THdfsScanRange hdfs_range;
    hdfs_range.__set_full_path("s3://bucket/db/t/data/plain.parquet");
    TScanRange scan_range;
    scan_range.__set_hdfs_scan_range(hdfs_range);
    TScanRangeParams srp;
    srp.__set_scan_range(scan_range);
    TPlanFragmentExecParams exec_params;
    exec_params.per_node_scan_ranges = {{0, {srp}}};
    params.__set_params(exec_params);

    const std::string dump = redacted_debug_string(params);
    ASSERT_NE(std::string::npos, dump.find("s3://bucket/db/t/data/plain.parquet")) << dump;
    ASSERT_EQ(std::string::npos, dump.find("redacted")) << dump;
}

} // namespace starrocks
