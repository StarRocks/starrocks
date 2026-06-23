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

package com.starrocks.connector.iceberg;

import com.google.common.collect.Lists;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.planner.IcebergDeleteSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.iceberg.DeleteFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IcebergConvertEqualityDeleteJobTest {

    // The convert sub-context forces the small equality-delete build side's runtime filter on and makes
    // the data-file scan wait for it, and removes the query timeout (convert is an admin batch).
    @Test
    public void testBuildSubConnectContextEnablesRuntimeFilter() {
        ConnectContext parent = new ConnectContext();
        parent.setSessionVariable(new SessionVariable());
        parent.setQueryId(UUIDUtil.genUUID());
        parent.setDatabase("eq_delete_db");

        IcebergConvertEqualityDeleteJob job = new IcebergConvertEqualityDeleteJob(
                parent, mock(IcebergTable.class), 1L, mock(TableRef.class));

        ConnectContext sub = Deencapsulation.invoke(job, "buildSubConnectContext", parent);
        Assertions.assertTrue(sub.getSessionVariable().getEnableGlobalRuntimeFilter(),
                "convert must build the equality-delete runtime filter");
        Assertions.assertEquals(1000L, sub.getSessionVariable().getRuntimeFilterScanWaitTime(),
                "convert must make the data-file scan wait for the runtime filter");
        Assertions.assertEquals(SessionVariable.MAX_QUERY_TIMEOUT, sub.getSessionVariable().getQueryTimeoutS(),
                "convert is an admin batch -> max query timeout");
        Assertions.assertEquals("eq_delete_db", sub.getDatabase());
    }

    // The metrics are read back from the sink's IcebergSinkExtra after execution: the equality-delete side
    // (file count + summed record counts) from the removal set, and the position-delete side (added file
    // count + delete rows) from the convert output the commit stamps on the extra.
    @Test
    public void testBuildMetricsCountsRemovalSet() {
        IcebergConvertEqualityDeleteJob job = new IcebergConvertEqualityDeleteJob(
                mock(ConnectContext.class), mock(IcebergTable.class), 1L, mock(TableRef.class));

        DeleteFile eq1 = mock(DeleteFile.class);
        DeleteFile eq2 = mock(DeleteFile.class);
        when(eq1.recordCount()).thenReturn(10L);
        when(eq2.recordCount()).thenReturn(15L);

        IcebergMetadata.IcebergSinkExtra extra = new IcebergMetadata.IcebergSinkExtra();
        extra.addEqualityDeleteFilesToRemove(Set.of(eq1, eq2));
        extra.setConvertOutput(3L, 30L);

        IcebergDeleteSink sink = mock(IcebergDeleteSink.class);
        when(sink.getSinkExtraInfo()).thenReturn(extra);
        PlanFragment fragment = mock(PlanFragment.class);
        when(fragment.getSink()).thenReturn(sink);
        ExecPlan plan = mock(ExecPlan.class);
        when(plan.getFragments()).thenReturn(Lists.newArrayList(fragment));

        IcebergConvertEqualityDeleteJob.ConvertMetrics metrics = Deencapsulation.invoke(job, "buildMetrics", plan);
        Assertions.assertEquals(2L, metrics.removedEqualityDeleteFiles());
        Assertions.assertEquals(25L, metrics.equalityDeleteRows());
        Assertions.assertEquals(3L, metrics.addedPositionDeleteFiles());
        Assertions.assertEquals(30L, metrics.positionDeleteRows());
    }
}
