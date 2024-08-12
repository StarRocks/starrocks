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
package com.starrocks.scheduler.mv;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * MVVersionManager is used to update materialized view version info when base table partition changes after mv refresh finished.
 */
public class MVVersionManager {
    private static final Logger LOG = LogManager.getLogger(MVVersionManager.class);

    private final MaterializedView mv;
    private final MvTaskRunContext mvTaskRunContext;

    public MVVersionManager(MaterializedView mv,
                            MvTaskRunContext mvTaskRunContext) {
        this.mv = mv;
        this.mvTaskRunContext = mvTaskRunContext;
    }

    /**
     * Sync meta changes to followers by edit log after version meta changed.
     * @param mv  mv that need to update
     * @param maxChangedTableRefreshTime max changed table refresh time
     */
    public static void updateEditLogAfterVersionMetaChanged(MaterializedView mv,
                                                            long maxChangedTableRefreshTime) {
        mv.getRefreshScheme().setLastRefreshTime(maxChangedTableRefreshTime);
        ChangeMaterializedViewRefreshSchemeLog changeRefreshSchemeLog =
                new ChangeMaterializedViewRefreshSchemeLog(mv);
        GlobalStateMgr.getCurrentState().getEditLog().logMvChangeRefreshScheme(changeRefreshSchemeLog);
        LOG.info("update edit log after version changed for mv {}, maxChangedTableRefreshTime:{}",
                mv.getName(), maxChangedTableRefreshTime);
    }
}
