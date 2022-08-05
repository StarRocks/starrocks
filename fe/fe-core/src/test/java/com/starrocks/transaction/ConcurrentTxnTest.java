// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.transaction;

import com.ibm.icu.impl.Assert;
import com.starrocks.common.Config;
import com.starrocks.metric.MetricRepo;
import com.starrocks.pseudocluster.PseudoCluster;
import org.junit.AfterClass;
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
        int fePort = new Random().nextInt(10000) + 50000;
        PseudoCluster.getOrCreate("pseudo_cluster_" + fePort, true, fePort, 3);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    class TableLoad {
        String db;
        String table;
        int id;
        int numTablet;
        int replicationNum;
        int loadIntervalMs;

        TableLoad(String db, int id, int numTablet, int replicationNum, int loadIntervalMs) {
            this.db = db;
            this.table = "table_" + id;
            this.id = id;
            this.numTablet = numTablet;
            this.replicationNum = replicationNum;
            this.loadIntervalMs = loadIntervalMs;
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
            PseudoCluster.getInstance()
                    .runSql(null, "insert into " + db + "." + table + " values (1,\"1\"), (2,\"2\"), (3,\"3\");");
        }
    }

    class DBLoad {
        int numDB;
        int numTable;
        List<TableLoad> tableLoads;
        volatile Exception error;

        AtomicInteger finishedTask = new AtomicInteger(0);

        DBLoad(int numDB, int numTable) {
            this.numDB = numDB;
            this.numTable = numTable;
        }

        void run(int numThread, int runSeconds) {
            for (int i = 0; i < numDB; i++) {
                try {
                    PseudoCluster.getInstance().runSql(null, "create database db_" + i);
                } catch (SQLException e) {
                    Assert.fail(e);
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
                TableLoad tableLoad = new TableLoad(dbName, i, numTablet, replicationNum, loadIntervalMs);
                tableLoads.add(tableLoad);
                futures.add(executor.submit(() -> {
                    try {
                        tableLoad.createTable();
                    } catch (SQLException e) {
                        error = e;
                    }
                }));
                if (error != null) {
                    Assert.fail(error);
                }
            }
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    Assert.fail(e);
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
                                System.out.printf("load error db:%s table:%s\n", tableLoad.db, tableLoad.table);
                                e.printStackTrace();
                                error = e;
                            }
                        }));
                    }
                }
                if (error != null) {
                    Assert.fail(error);
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
                    Assert.fail(e);
                }
            }
            double t = (System.nanoTime() - startTs) / 1e9;
            System.out.printf("numThread:%d numDB:%d numLoad:%d Time: %.2fs, %.2f tps\n", numThread, numDB, finishedTask.get(), t,
                    finishedTask.get() / t);
            long numLog = MetricRepo.COUNTER_EDIT_LOG_WRITE.getValue() - numLogBefore;
            double writeBatch = MetricRepo.HISTO_JOURNAL_WRITE_BATCH.getSnapshot().getMedian();
            double writeLatency = MetricRepo.HISTO_JOURNAL_WRITE_LATENCY.getSnapshot().getMedian();
            double writeBytes = MetricRepo.HISTO_JOURNAL_WRITE_BYTES.getSnapshot().getMedian();
            System.out.printf("numLog:%d writeBatch:%f writeLatency:%f writeBytes:%f\n\n", numLog, writeBatch, writeLatency,
                    writeBytes);
        }
    }

    int numDB = 20;
    int numTable = 1000;
    int numThread = 20;
    int runSeconds = 10;

    void setup() throws SQLException {
        Config.enable_new_publish_mechanism = false;
    }

    @Test
    public void testConcurrentLoad() throws Exception {
        setup();
        DBLoad dbLoad = new DBLoad(numDB, numTable);
        dbLoad.run(numThread, runSeconds);
    }
}
