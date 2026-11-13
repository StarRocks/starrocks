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

package com.starrocks.load;

/**
 * Machine-readable reason for a failed load. The free-text error message stays
 * as it is; this enum is a fixed, coarse classification derived from it (and from
 * the structured cancel type when there is one) so that callers can group failures
 * without parsing text. See {@link LoadErrorCodeMapper} for the derivation.
 */
public enum LoadErrorCode {
    // Too many rows were filtered or unselected (max_filter_ratio, parse errors).
    FILTER_RATIO_EXCEEDED,
    // A BE process, query or load memory limit was hit.
    MEM_LIMIT_EXCEEDED,
    // Commit rejected or load cancelled because the lake compaction score is too high.
    COMPACTION_SCORE_EXCEEDED,
    // Rejected by max_running_txn_num_per_db.
    TOO_MANY_RUNNING_TXNS,
    // The label is already used by another load.
    LABEL_ALREADY_EXISTS,
    // Load, transaction or publish timeout.
    TIMEOUT,
    // CANCEL LOAD, or the client aborted the transaction.
    USER_CANCELLED,
    // The source could not be read: broker/HDFS/object storage/Kafka errors, missing files, access denied.
    SOURCE_READ_FAILED,
    // Column count or type mismatch, schema changed during the load, partition not found.
    SCHEMA_MISMATCH,
    // Backend down, RPC failure, no available backend or replica.
    BE_UNAVAILABLE,
    // Publish version conflict, version already exists, transaction aborted by another operation.
    TXN_CONFLICT,
    // A failure the FE itself classifies as internal.
    INTERNAL_ERROR,
    // Nothing above matched.
    UNKNOWN
}
