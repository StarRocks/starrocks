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
import com.starrocks.common.DdlException;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The FE stream MV operators were removed by sync #71764, so there are no remaining
 * stream-only intermediate tables to create during incremental MV preparation.
 */
class IMTCreator {
    private static final Logger LOG = LogManager.getLogger(IMTCreator.class);

    static void createIMT(CreateMaterializedViewStatement stmt,
                          MaterializedView view,
                          ExecPlan maintenancePlan,
                          ColumnRefFactory columnRefFactory) throws DdlException {
        LOG.debug("skip IMT creation for incremental MV {}", view.getName());
    }
}
