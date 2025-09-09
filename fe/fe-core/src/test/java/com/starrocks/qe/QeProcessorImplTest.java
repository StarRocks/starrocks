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

package com.starrocks.qe;

import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.thrift.TUniqueId;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class QeProcessorImplTest extends PlanTestNoneDBBase {

    @Test
    void registerPlanningQuery() throws StarRocksException {
        String sql = "SELECT * FROM lineitem";
        TUniqueId queryId = new TUniqueId(0, 1);
        connectContext.setExecutionId(queryId);
        UUID lastQueryId = new UUID(2L, 3L);
        connectContext.setLastQueryId(lastQueryId);

        QeProcessorImpl.INSTANCE.registerQuery(queryId, QeProcessorImpl.QueryInfo.fromPlanningQuery(connectContext, sql));
        Map<String, QueryStatisticsItem> queryStatistics = QeProcessorImpl.INSTANCE.getQueryStatistics();
        assertThat(queryStatistics.size()).isEqualTo(1);
        String queryIdStr = DebugUtil.printId(connectContext.getExecutionId());
        assertThat(queryStatistics.get(queryIdStr).getExecState()).isEqualTo(LogicalSlot.State.CREATED.toQueryStateString());

        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
        queryStatistics = QeProcessorImpl.INSTANCE.getQueryStatistics();
        assertThat(queryStatistics.size()).isEqualTo(0);
    }
}
