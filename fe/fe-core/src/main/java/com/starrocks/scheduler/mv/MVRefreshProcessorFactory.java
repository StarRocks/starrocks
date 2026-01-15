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
// limitations under the License

package com.starrocks.scheduler.mv;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.mv.hybrid.MVHybridBasedRefreshProcessor;
import com.starrocks.scheduler.mv.ivm.MVIVMBasedRefreshProcessor;
import com.starrocks.scheduler.mv.pct.MVPCTBasedRefreshProcessor;

import java.util.Map;

public class MVRefreshProcessorFactory {
    public static final MVRefreshProcessorFactory INSTANCE = new MVRefreshProcessorFactory();

    public BaseMVRefreshProcessor newProcessor(Database db, MaterializedView mv,
                                               MvTaskRunContext mvContext,
                                               IMaterializedViewMetricsEntity mvEntity) {
        MaterializedView.RefreshMode refreshMode = mv.getCurrentRefreshMode();
        switch (refreshMode) {
            case INCREMENTAL:
                return new MVIVMBasedRefreshProcessor(db, mv, mvContext, mvEntity, refreshMode);
            case AUTO:
                return new MVHybridBasedRefreshProcessor(db, mv, mvContext, mvEntity, refreshMode);
            case FULL: {
                // full refresh
                Map<String, String> props = mvContext.getProperties();
                props.put(TaskRun.FORCE, "true");
                return new MVPCTBasedRefreshProcessor(db, mv, mvContext, mvEntity, refreshMode);
            }
            default:
                return new MVPCTBasedRefreshProcessor(db, mv, mvContext, mvEntity, refreshMode);
        }
    }
}
