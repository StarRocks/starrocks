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

package com.starrocks.sql.optimizer.dump;

import com.starrocks.catalog.Table;

/**
 * The form of a table a query dump is allowed to record.
 *
 * A dump outlives the statement and reaches whoever opens the file, so a table the statement saw only in
 * part must not be recorded in full. By default a table is recorded as it is.
 */
public class DumpTableProjection {

    public Table forDump(Table table) {
        return table;
    }
}
