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

#pragma once

#include <string>

// Debug dumps of thrift RPC structs that carry Iceberg per-file encryption keys.
//
// Parquet Modular Encryption moves the plaintext per-file data encryption key across the FE/BE
// boundary in both directions, so two widely-dumped structs now contain key material:
//
//   TReportExecStatusParams -> sink_commit_infos -> TIcebergDataFile.file_dek / .aad_prefix
//       the write side: the key BE generated, travelling back so FE can build key_metadata
//   TExecPlanFragmentParams -> params.per_node_scan_ranges -> THdfsScanRange
//                              .parquet_encryption_info.file_dek / .aad_prefix
//       the read side: the key FE recovered from key_metadata, travelling out to BE
//
// Both are printed verbatim by ThriftDebugString at VLOG level in several places. A key written to
// be.INFO outlives the query and is copied off the host by log collection, so every one of those
// dumps has to go through these helpers instead.
//
// Both fields are secret, not just the key: key_metadata pairs the DEK with the AAD prefix, and the
// confidentiality of that pair rests on the manifest carrying it being encrypted.
//
// Lives in Common so every module that dumps these structs can reach it -- the sites span
// Orchestration, Service and Exec.
namespace starrocks {

class TReportExecStatusParams;
class TExecPlanFragmentParams;

// ThriftDebugString(params) with the write-side per-file keys replaced by "<redacted>".
std::string redacted_debug_string(const TReportExecStatusParams& params);

// ThriftDebugString(params) with the read-side per-file keys on every scan range replaced.
std::string redacted_debug_string(const TExecPlanFragmentParams& params);

} // namespace starrocks
