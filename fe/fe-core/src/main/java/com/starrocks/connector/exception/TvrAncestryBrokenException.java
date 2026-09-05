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

package com.starrocks.connector.exception;

/**
 * Means a table's version history is broken, usually because data was dropped or overwritten.
 * Thrown by {@code listTableDeltaTraits}. {@code MVIVMRefreshProcessor} catches this to show a
 * clear "drop and recreate the MV" message instead of a raw error.
 */
public class TvrAncestryBrokenException extends StarRocksConnectorException {
    public TvrAncestryBrokenException(String message) {
        super(message);
    }

    public TvrAncestryBrokenException(String formatString, Object... args) {
        super(formatString, args);
    }

    public TvrAncestryBrokenException(String message, Throwable cause) {
        super(message, cause);
    }
}
