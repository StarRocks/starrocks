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

package com.starrocks.catalog;

import com.starrocks.sql.analyzer.SemanticException;

/** Marks an authoritative catalog definition that could not be analyzed during table image recovery. */
public final class SyncMaterializedViewRecoveryException extends SemanticException {
    // Package-private: the marker originates in the authoritative OlapTable.onReload branch only.
    SyncMaterializedViewRecoveryException(String tableName, long indexId, SemanticException cause) {
        super("Failed to recover authoritative synchronous MV index " + indexId + " on table " + tableName
                + ": " + cause.getMessage(), cause);
    }
}
