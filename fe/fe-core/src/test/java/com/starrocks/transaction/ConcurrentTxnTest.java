// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.transaction;

import com.starrocks.common.Config;
import com.starrocks.metric.MetricRepo;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.pseudocluster.Tablet;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentTxnTest {
    @BeforeClass
    public static void setUp() throws Exception {
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
    }

    @AfterClass
    public static void tearDown() throws Exception {
    }

    static AtomicInteger totalRead = new AtomicInteger(0);

    class TableLoad {
        String db;
        String table;
        int id;
        int numTablet;
        int replicationNum;
        int loadIntervalMs;
        boolean withRead;

        TableLoad(String db, int id, int numTablet, int replicationNum, int loadIntervalMs, boolean withRead) {
            this.db = db;
            this.table = "table_" + id;
            this.id = id;
            this.numTablet = numTablet;
            this.replicationNum = replicationNum;
            this.loadIntervalMs = loadIntervalMs;
            this.withRead = withRead;
        }

        void createTable() throws SQLException {
            PseudoCluster.getInstance().runSql(db,
                    "create table " + table +
                            " ( pk bigint NOT NULL, v0 string not null) primary KEY (pk) DISTRIBUTED BY HASH(pk) BUCKETS " +
                            numTablet + " PROPERTIES(\"replication_num\" = \"" + replicationNum +
                            "\", \"storage_medium\" = \"SSD\");");
        }

        boolean hasTask(int tsMs) {
            return tsMs % loadIntervalMs == 0;
        }

        void loadOnce() throws SQLException {
            List<String> sqls = new ArrayList<>();
            sqls.add("insert into " + db + "." + table + " values (1,\"1\"), (2,\"2\"), (3,\"3\");");
            if (withRead) {
                sqls.add("select * from " + db + "." + table + ";");
                totalRead.addAndGet(numTablet);
            }
            PseudoCluster.getInstance().runSqlList(null, sqls);
        }
    }

    class DBLoad {
        int numDB;
        int numTable;
        boolean withRead = false;
        List<TableLoad> tableLoads;
        volatile Exception error;

        AtomicInteger finishedTask = new AtomicInteger(0);

        DBLoad(int numDB, int numTable, boolean withRead) {
            this.numDB = numDB;
            this.numTable = numTable;
            this.withRead = withRead;
        }

        void run(int numThread, int runSeconds) {
            for (int i = 0; i < numDB; i++) {
                try {
                    PseudoCluster.getInstance().runSql(null, "create database db_" + i);
                } catch (SQLException e) {
                    e.printStackTrace();
                    Assert.fail(e.getMessage());
                }
            }
            ThreadPoolExecutor executor
                    = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThread);
            tableLoads = new ArrayList<>();
            Random r = new Random(0);
            int scheduleIntervalMs = 200;
            List<Future<?>> futures = new ArrayList<Future<?>>();
            for (int i = 0; i < numTable; i++) {
                int numTablet = r.nextInt(16) + 1;
                int replicationNum = 3;
                int loadIntervalMs = (r.nextInt(40) + 1) * scheduleIntervalMs;
                String dbName = "db_" + (i % numDB);
                TableLoad tableLoad = new TableLoad(dbName, i, numTablet, replicationNum, loadIntervalMs, withRead);
                tableLoads.add(tableLoad);
                futures.add(executor.submit(() -> {
                    try {
                        tableLoad.createTable();
                    } catch (SQLException e) {
                        error = e;
                    }
                }));
                if (error != null) {
                    Assert.fail(error.getMessage());
                }
            }
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                }
            }
            futures.clear();
            long numLogBefore = MetricRepo.COUNTER_EDIT_LOG_WRITE.getValue();
            finishedTask.set(0);
            long startTs = System.nanoTime();
            int runInterval = runSeconds * 1000 / scheduleIntervalMs;
            for (int i = 0; i < runInterval; i++) {
                for (TableLoad tableLoad : tableLoads) {
                    if (tableLoad.hasTask(i * scheduleIntervalMs)) {
                        futures.add(executor.submit(() -> {
                            try {
                                tableLoad.loadOnce();
                                finishedTask.incrementAndGet();
                            } catch (SQLException e) {
                                System.out.printf("load error db:%s table:%s %s\n", tableLoad.db, tableLoad.table,
                                        e.getMessage());
                                e.printStackTrace();
                                error = e;
                            }
                        }));
                    }
                }
                if (error != null) {
                    Assert.fail(error.getMessage());
                }
                try {
                    Thread.sleep(scheduleIntervalMs);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                }
            }
            if (error != null) {
                Assert.fail(error.getMessage());
            }
            double t = (System.nanoTime() - startTs) / 1e9;
            System.out.printf("numThread:%d numDB:%d numLoad:%d Time: %.2fs, %.2f tps\n", numThread, numDB, finishedTask.get(), t,
                    finishedTask.get() / t);
            long numLog = MetricRepo.COUNTER_EDIT_LOG_WRITE.getValue() - numLogBefore;
            double writeBatch = MetricRepo.HISTO_JOURNAL_WRITE_BATCH.getSnapshot().getMedian();
            double writeLatency = MetricRepo.HISTO_JOURNAL_WRITE_LATENCY.getSnapshot().getMedian();
            double writeBytes = MetricRepo.HISTO_JOURNAL_WRITE_BYTES.getSnapshot().getMedian();
            System.out.printf("numLog:%d writeBatch:%f writeLatency:%f writeBytes:%f\n", numLog, writeBatch, writeLatency,
                    writeBytes);
            for (int i = 0; i < numDB; i++) {
                try {
                    PseudoCluster.getInstance().runSql(null, "drop database db_" + i + " force");
                } catch (SQLException e) {
                    Assert.fail(e.getMessage());
                }
            }
        }
    }

    int runTime = 3;
    int numDB = 20;
    int numTable = 100;
    int numThread = 2;
    int runSeconds = 2;
    boolean withRead = true;

    void setup() throws SQLException {
        Config.enable_new_publish_mechanism = false;
    }

    @Test
    public void testConcurrentLoad() throws Exception {
        setup();
        for (int i = 0; i < runTime; i++) {
            DBLoad dbLoad = new DBLoad(numDB, numTable, withRead);
            dbLoad.run(numThread, runSeconds);
            System.out.printf("totalReadExpected: %d totalRead: %d totalSucceed: %d totalFail: %d\n",
                    totalRead.get(), Tablet.getTotalReadExecuted(), Tablet.getTotalReadSucceed(), Tablet.getTotalReadFailed());
            Assert.assertEquals(totalRead.get(), Tablet.getTotalReadSucceed());
        }
        PseudoCluster.getInstance().shutdown(true);
    }
}
