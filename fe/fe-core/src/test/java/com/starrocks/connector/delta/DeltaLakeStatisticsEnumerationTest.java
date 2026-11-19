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

package com.starrocks.connector.delta;

import com.starrocks.catalog.Column;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// The statistics pass enumerates a Delta table once per key; it keeps the file list only when the scan will list
// its files with getRemoteFiles, and a later getRemoteFiles must neither fail nor count the statistics twice.
public class DeltaLakeStatisticsEnumerationTest {
    // 120 adds over 4 commits, a checkpoint, then 3 tail commits of 100 adds
    private static final int FILES = 120 + 300;
    private static String path;

    private ConnectContext ctx;

    @BeforeAll
    public static void buildTable() throws Exception {
        path = DeltaLakePlanningBench.buildTable(120, 7, 30, 3);
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private OptimizerContext optimizerContext(boolean incremental) {
        ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        ctx.getSessionVariable().setEnableConnectorIncrementalScanRanges(incremental);
        return OptimizerFactory.mockContext(ctx, new ColumnRefFactory());
    }

    private static Map<ColumnRefOperator, Column> columns(DeltaLakePlanningBench.Env env, OptimizerContext oc) {
        Map<ColumnRefOperator, Column> columns = new HashMap<>();
        for (Column c : env.table.getFullSchema()) {
            columns.put(oc.getColumnRefFactory().create(c.getName(), c.getType(), c.isAllowNull()), c);
        }
        return columns;
    }

    private static Statistics stats(DeltaLakeMetadata metadata, DeltaLakePlanningBench.Env env, OptimizerContext oc,
                                    Map<ColumnRefOperator, Column> columns) {
        return metadata.getTableStatistics(oc, env.table, columns, null, null, -1,
                TvrTableSnapshot.of(env.table.getDeltaSnapshot().getVersion()));
    }

    private static Map<?, ?> splitTasks(DeltaLakeMetadata metadata) throws Exception {
        Field f = DeltaLakeMetadata.class.getDeclaredField("splitTasks");
        f.setAccessible(true);
        return (Map<?, ?>) f.get(metadata);
    }

    private static GetRemoteFilesParams scanParams(DeltaLakePlanningBench.Env env) {
        return GetRemoteFilesParams.newBuilder()
                .setTableVersionRange(TvrTableSnapshot.of(env.table.getDeltaSnapshot().getVersion())).build();
    }

    private static long rowsOf(List<RemoteFileInfo> files) {
        return files.stream().mapToLong(f -> ((DeltaRemoteFileInfo) f).getFileScanTask().getRecords()).sum();
    }

    @Test
    public void testIncrementalScanDoesNotKeepFileList() throws Exception {
        OptimizerContext oc = optimizerContext(true);
        DeltaLakePlanningBench.Env env = DeltaLakePlanningBench.openTable(path);
        Map<ColumnRefOperator, Column> columns = columns(env, oc);
        DeltaLakeMetadata metadata = DeltaLakePlanningBench.newMetadata();

        Statistics first = stats(metadata, env, oc, columns);
        Assertions.assertTrue(splitTasks(metadata).isEmpty());

        // the incremental scan enumerates on its own
        RemoteFileInfoSource source = metadata.getRemoteFilesAsync(env.table, scanParams(env));
        int n = 0;
        long rows = 0;
        while (source.hasMoreOutput()) {
            rows += ((DeltaRemoteFileInfo) source.getOutput()).getFileScanTask().getRecords();
            n++;
        }
        source.close();
        Assertions.assertEquals(FILES, n);
        Assertions.assertEquals(rows, (long) first.getOutputRowCount());

        // a listing scan with the same key still gets its files, and the statistics are not counted again
        List<RemoteFileInfo> files = metadata.getRemoteFiles(env.table, scanParams(env));
        Assertions.assertEquals(FILES, files.size());
        Assertions.assertEquals(rows, rowsOf(files));
        Assertions.assertEquals(first.getOutputRowCount(), stats(metadata, env, oc, columns).getOutputRowCount());
    }

    @Test
    public void testListingScanReusesKeptFileList() throws Exception {
        OptimizerContext oc = optimizerContext(false);
        DeltaLakePlanningBench.Env env = DeltaLakePlanningBench.openTable(path);
        Map<ColumnRefOperator, Column> columns = columns(env, oc);
        DeltaLakeMetadata metadata = DeltaLakePlanningBench.newMetadata();

        Statistics first = stats(metadata, env, oc, columns);
        Assertions.assertEquals(1, splitTasks(metadata).size());
        List<?> kept = (List<?>) splitTasks(metadata).values().iterator().next();
        Assertions.assertEquals(FILES, kept.size());

        List<RemoteFileInfo> files = metadata.getRemoteFiles(env.table, scanParams(env));
        Assertions.assertEquals(FILES, files.size());
        Assertions.assertEquals((long) first.getOutputRowCount(), rowsOf(files));
        // the same list, not a second enumeration
        Assertions.assertSame(kept, splitTasks(metadata).values().iterator().next());
        Assertions.assertEquals(first.getOutputRowCount(), stats(metadata, env, oc, columns).getOutputRowCount());
    }
}
