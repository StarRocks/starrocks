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

import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class StmtExecutorAnalyzeCancelTest {

    private static StarRocksAssert starRocksAssert;
    private ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert();
        String createTblStmtStr = "create table db.tbl(kk1 int, kk2 varchar(32), kk3 int, kk4 int) "
                + "AGGREGATE KEY(kk1, kk2,kk3,kk4) distributed by hash(kk1) buckets 3 properties('replication_num' = "
                + "'1');";
        starRocksAssert.withDatabase("db").useDatabase("db");
        starRocksAssert.withTable(createTblStmtStr);
    }

    @BeforeEach
    public void setUp() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setDatabase("db");
    }

    @Test
    @Timeout(30)
    public void testCancelAnalyzeDuringExecution() throws Exception {
        CountDownLatch taskStarted = new CountDownLatch(1);
        AtomicBoolean analyzeCancelled = new AtomicBoolean(false);

        // Mock executeAnalyze to block execution
        // This will be called by CancelableAnalyzeTask in the background thread pool
        new MockUp<StmtExecutor>() {
            @Mock
            protected void executeAnalyze(com.starrocks.sql.ast.AnalyzeStmt analyzeStmt,
                                          com.starrocks.statistic.AnalyzeStatus analyzeStatus,
                                          com.starrocks.catalog.Database db,
                                          com.starrocks.catalog.Table table) {
                taskStarted.countDown();
                try {
                    // Block here - when cancel() is called, it will interrupt this thread
                    // via cancelableTask.cancel(true), causing await() to throw InterruptedException
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    analyzeCancelled.set(true);
                    Thread.currentThread().interrupt();
                    // Re-throw to let CancelableAnalyzeTask handle it
                    throw new RuntimeException("Task interrupted", e);
                }
            }
        };

        // Execute ANALYZE in a separate thread (simulating client connection)
        Thread analyzeThread = new Thread(() -> {
            try {
                connectContext.executeSql("ANALYZE TABLE db.tbl");
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                Assertions.fail(e);
            }
        });
        analyzeThread.start();

        // Wait for task to start executing in background thread pool
        assertTrue(taskStarted.await(5, TimeUnit.SECONDS), "Task should start executing");

        // Wait for main thread to finish (it should exit due to InterruptedException)
        analyzeThread.interrupt();
        analyzeThread.join(5000);

        // Verify task is cancelled in finally block
        assertTrue(analyzeCancelled.get(), "the analyze task should be cancelled");
    }
}
