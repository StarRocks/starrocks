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

#include <thrift/protocol/TDebugProtocol.h>

#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {

namespace {
constexpr const char* kRedacted = "<redacted>";

void redact_encryption_info(TParquetEncryptionInfo* enc) {
    if (enc->__isset.file_dek) {
        enc->__set_file_dek(kRedacted);
    }
    if (enc->__isset.aad_prefix) {
        enc->__set_aad_prefix(kRedacted);
    }
}
} // namespace

std::string redacted_debug_string(const TReportExecStatusParams& params) {
    if (!params.__isset.sink_commit_infos) {
        return apache::thrift::ThriftDebugString(params);
    }
    TReportExecStatusParams redacted = params;
    for (auto& commit_info : redacted.sink_commit_infos) {
        if (!commit_info.__isset.iceberg_data_file) {
            continue;
        }
        auto& data_file = commit_info.iceberg_data_file;
        if (data_file.__isset.file_dek) {
            data_file.__set_file_dek(kRedacted);
        }
        if (data_file.__isset.aad_prefix) {
            data_file.__set_aad_prefix(kRedacted);
        }
    }
    return apache::thrift::ThriftDebugString(redacted);
}

std::string redacted_debug_string(const TExecPlanFragmentParams& params) {
    // Copying the whole struct only to print it is wasteful, so skip it unless a key is actually
    // present. This is also the common case: only Iceberg encrypted tables set these fields.
    bool has_key = false;
    // per_node_scan_ranges is a REQUIRED thrift field, so it has no __isset member -- only the
    // enclosing params is optional.
    if (params.__isset.params) {
        for (const auto& [_, scan_range_params] : params.params.per_node_scan_ranges) {
            for (const auto& srp : scan_range_params) {
                if (srp.scan_range.__isset.hdfs_scan_range &&
                    srp.scan_range.hdfs_scan_range.__isset.parquet_encryption_info) {
                    has_key = true;
                    break;
                }
            }
            if (has_key) {
                break;
            }
        }
    }
    if (!has_key) {
        return apache::thrift::ThriftDebugString(params);
    }

    TExecPlanFragmentParams redacted = params;
    for (auto& [_, scan_range_params] : redacted.params.per_node_scan_ranges) {
        for (auto& srp : scan_range_params) {
            if (!srp.scan_range.__isset.hdfs_scan_range) {
                continue;
            }
            auto& hdfs_range = srp.scan_range.hdfs_scan_range;
            if (hdfs_range.__isset.parquet_encryption_info) {
                redact_encryption_info(&hdfs_range.parquet_encryption_info);
            }
            // Position-delete files carry their own key, on their own field.
            for (auto& delete_file : hdfs_range.delete_files) {
                if (delete_file.__isset.parquet_encryption_info) {
                    redact_encryption_info(&delete_file.parquet_encryption_info);
                }
            }
        }
    }
    return apache::thrift::ThriftDebugString(redacted);
}

} // namespace starrocks
