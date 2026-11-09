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

#include "common/util/thrift_util.h"
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
    // Scan ranges reach BE through two maps: per_node_scan_ranges, and node_to_per_driver_seq_scan_ranges,
    // which FE fills instead for per-driver pipeline assignment. Both carry the same THdfsScanRange, so
    // both are walked; checking only the first let a per-driver range dump its key verbatim.
    //
    // A key can sit on a position-delete file even when the data file it belongs to has none -- e.g. a
    // data file written before the table was encrypted, deleted from afterwards -- so delete files are
    // checked as well, not only the range's own key.
    auto range_has_key = [](const TScanRangeParams& srp) {
        if (!srp.scan_range.__isset.hdfs_scan_range) {
            return false;
        }
        const auto& hdfs_range = srp.scan_range.hdfs_scan_range;
        if (hdfs_range.__isset.parquet_encryption_info) {
            return true;
        }
        for (const auto& delete_file : hdfs_range.delete_files) {
            if (delete_file.__isset.parquet_encryption_info) {
                return true;
            }
        }
        return false;
    };
    auto redact_range = [](TScanRangeParams* srp) {
        if (!srp->scan_range.__isset.hdfs_scan_range) {
            return;
        }
        auto& hdfs_range = srp->scan_range.hdfs_scan_range;
        if (hdfs_range.__isset.parquet_encryption_info) {
            redact_encryption_info(&hdfs_range.parquet_encryption_info);
        }
        // Position-delete files carry their own key, on their own field.
        for (auto& delete_file : hdfs_range.delete_files) {
            if (delete_file.__isset.parquet_encryption_info) {
                redact_encryption_info(&delete_file.parquet_encryption_info);
            }
        }
    };

    // Copying the whole struct only to print it is wasteful, so skip the copy unless a key is actually
    // present. This is also the common case: only Iceberg encrypted tables set these fields.
    bool any_key = false;
    if (params.__isset.params) {
        // per_node_scan_ranges is a REQUIRED thrift field, so it has no __isset member -- only the
        // enclosing params is optional.
        for (const auto& [_, ranges] : params.params.per_node_scan_ranges) {
            for (const auto& srp : ranges) {
                any_key = any_key || range_has_key(srp);
            }
        }
        if (params.params.__isset.node_to_per_driver_seq_scan_ranges) {
            for (const auto& [_, per_driver] : params.params.node_to_per_driver_seq_scan_ranges) {
                for (const auto& [driver_seq, ranges] : per_driver) {
                    for (const auto& srp : ranges) {
                        any_key = any_key || range_has_key(srp);
                    }
                }
            }
        }
    }
    // Both paths end in thrift_plan_debug_string rather than ThriftDebugString, so the plan
    // credentials it masks (AI provider API keys) stay masked in this dump as well.
    if (!any_key) {
        return thrift_plan_debug_string(params);
    }

    TExecPlanFragmentParams redacted = params;
    for (auto& [_, ranges] : redacted.params.per_node_scan_ranges) {
        for (auto& srp : ranges) {
            redact_range(&srp);
        }
    }
    if (redacted.params.__isset.node_to_per_driver_seq_scan_ranges) {
        for (auto& [_, per_driver] : redacted.params.node_to_per_driver_seq_scan_ranges) {
            for (auto& [driver_seq, ranges] : per_driver) {
                for (auto& srp : ranges) {
                    redact_range(&srp);
                }
            }
        }
    }
    return thrift_plan_debug_string(redacted);
}

} // namespace starrocks
