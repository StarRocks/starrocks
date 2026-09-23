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

package com.starrocks.connector;

/**
 * What a caller of {@code ConnectorMetadata.getTable} intends to do with the table; only connectors that
 * pay something to make one readable act on it. DATA_ACCESS is the default because forgetting to declare
 * costs an unused credential, while the reverse hands back a table that cannot be scanned.
 */
public enum TableLoadPurpose {
    /** The table will be scanned; whatever is needed to read it must be acquired now. */
    DATA_ACCESS,

    /** Only metadata. Such a table need not be scannable, and asking to scan it later is an error. */
    METADATA_ONLY
}
