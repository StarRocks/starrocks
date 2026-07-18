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
 * Thrown by {@code ConnectorMetadata#listTableDeltaTraits} when the requested {@code from} snapshot
 * is not a parent ancestor of the {@code to} snapshot — i.e. the TVR snapshot lineage is broken
 * (typically a partition-shape change such as DROP/TRUNCATE/OVERWRITE that regressed the watermark).
 *
 * <p>This is a typed signal for {@code MVIVMRefreshProcessor}, which treats a broken ancestry as a
 * recoverable "partition-shape change" (drop and recreate the MV) rather than a hard failure. Prefer
 * catching this type over string-matching the exception message.
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
