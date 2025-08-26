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

package com.starrocks.sql.optimizer.rule.tvr.common;

import com.google.common.base.Supplier;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;

public class TvrOptContext {
    private final SessionVariable sessionVariable;

    private final Supplier<MaterializedView> lazyMV;

    public TvrOptContext(SessionVariable sessionVariable) {
        this.sessionVariable = sessionVariable;
        this.lazyMV = () -> loadTvrTargetMV();
    }

    private MaterializedView loadTvrTargetMV() {
        // TODO: How to set the logical aggregate operator's aggregate state table?
        // get the aggregate state table
        // checks its schema consistent with the current aggregate's state
        String strMvId = sessionVariable.getTvrTargetMvId();
        if (strMvId == null) {
            throw new IllegalStateException("TVR target MV ID is not set in session variable");
        }
        MvId mvId = GsonUtils.GSON.fromJson(strMvId, MvId.class);
        if (mvId == null) {
            throw new IllegalStateException("Failed to parse TVR target MV ID from session variable: " + strMvId);
        }
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvId.getDbId());
        if (db == null) {
            throw new IllegalStateException("Database with ID " + mvId.getDbId() + " not found");
        }
        MaterializedView aggStateOlapTable = (MaterializedView) db.getTable(mvId.getId());
        if (aggStateOlapTable == null) {
            throw new IllegalStateException("Aggregate state table with ID " + mvId.getId() + " not found in database "
                    + db.getFullName());
        }
        return aggStateOlapTable;
    }

    public SessionVariable getSessionVariable() {
        return sessionVariable;
    }

    public MaterializedView getTvrTargetMV() {
        return lazyMV.get();
    }
}
