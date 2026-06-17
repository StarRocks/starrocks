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

package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.MetaPreparationItem;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.task.PrepareCollectMetaTask;
import com.starrocks.sql.optimizer.task.TaskContext;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

public class PrepareCollectMetaTaskTest {

    @BeforeEach
    public void setUp() {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setSessionVariable(new SessionVariable());
        connectContext.getSessionVariable().setEnableProfile(true);
        ConnectContext.set(connectContext);
        Tracers.register(connectContext);
        Tracers.init(Tracers.Mode.TIMER, Tracers.Module.ALL, true, false);
    }

    @AfterEach
    public void tearDown() {
        Tracers.close();
        ConnectContext.remove();
    }

    @Test
    public void testForkJoinMergeEndToEnd() throws Exception {

        List<Column> columns = Lists.newArrayList(new Column("c1", Type.INT));
        IcebergTable tbl1 = new IcebergTable(1L, "t1", "default_catalog", "default_catalog",
                "db", "t1", "", columns, null, Maps.newHashMap());
        IcebergTable tbl2 = new IcebergTable(2L, "t2", "default_catalog", "default_catalog",
                "db", "t2", "", columns, null, Maps.newHashMap());

        LogicalIcebergScanOperator scan1 = new LogicalIcebergScanOperator.Builder()
                .setTable(tbl1).setColRefToColumnMetaMap(Maps.newHashMap()).build();
        LogicalIcebergScanOperator scan2 = new LogicalIcebergScanOperator.Builder()
                .setTable(tbl2).setColRefToColumnMetaMap(Maps.newHashMap()).build();

        OptExpression planTree = OptExpression.create(new LogicalJoinOperator(),
                OptExpression.create(scan1),
                OptExpression.create(scan2));

        // Mock MetadataMgr.prepareMetadata to return true (no actual connector work)
        new MockUp<MetadataMgr>() {
            @Mock
            public boolean prepareMetadata(String queryId, String catalogName,
                                           MetaPreparationItem item, Tracers tracers,
                                           ConnectContext connectContext) {
                return true;
            }
        };

        ConnectContext ctx = ConnectContext.get();
        ctx.getSessionVariable().setPrepareMetadataPoolSize(4);

        // Create a real OptimizerContext via mock — constructor is package-private
        // in com.starrocks.sql.optimizer, not accessible from .task subpackage
        new MockUp<OptimizerContext>() {
            @Mock
            public SessionVariable getSessionVariable() {
                return ctx.getSessionVariable();
            }

            @Mock
            public UUID getQueryId() {
                return UUID.randomUUID();
            }
        };

        OptimizerContext optimizerCtx = new OptimizerContext(ctx);
        TaskContext taskCtx = new TaskContext(optimizerCtx,
                new PhysicalPropertySet(), new ColumnRefSet(), 0);

        // Execute — this covers the fork/merge/join/shutdown path
        new PrepareCollectMetaTask(taskCtx, planTree).execute();

        // Verify profiling shows the outer scope
        String output = Tracers.printScopeTimer();
        Assertions.assertTrue(output.contains("EXTERNAL.parallel_prepare_metadata"),
                "Expected outer scope in output:\n" + output);
    }
}
