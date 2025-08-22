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

import com.starrocks.common.DdlException;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.IncrementalRefreshSchemeDesc;
import com.starrocks.sql.ast.ManualRefreshSchemeDesc;
import com.starrocks.sql.ast.RefreshSchemeClause;
import com.starrocks.sql.ast.SyncRefreshSchemeDesc;

public enum MaterializedViewRefreshType {
    SYNC,
    ASYNC,
    MANUAL,
    INCREMENTAL;

    public static MaterializedViewRefreshType getType(RefreshSchemeClause refreshSchemeDesc) throws DdlException {
        MaterializedViewRefreshType newRefreshType;
        if (refreshSchemeDesc instanceof AsyncRefreshSchemeDesc) {
            newRefreshType = MaterializedViewRefreshType.ASYNC;
        } else if (refreshSchemeDesc instanceof SyncRefreshSchemeDesc) {
            newRefreshType = MaterializedViewRefreshType.SYNC;
        } else if (refreshSchemeDesc instanceof ManualRefreshSchemeDesc) {
            newRefreshType = MaterializedViewRefreshType.MANUAL;
        } else if (refreshSchemeDesc instanceof IncrementalRefreshSchemeDesc) {
            newRefreshType = MaterializedViewRefreshType.INCREMENTAL;
        } else {
            throw new DdlException("Unsupported refresh scheme type");
        }

        return newRefreshType;
    }
}