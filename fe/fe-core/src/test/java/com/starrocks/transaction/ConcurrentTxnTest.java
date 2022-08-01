// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.transaction;

import com.ibm.icu.impl.Assert;
import com.starrocks.common.Config;
import com.starrocks.pseudocluster.PseudoCluster;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class ConcurrentTxnTest {
    @BeforeClass
    public static void setUp() throws Exception {
        int fePort = new Random().nextInt(10000) + 50000;
        PseudoCluster.getOrCreate("pseudo_cluster_" + fePort, fePort, 3);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown();
    }

    static void runSql(String db, String sql) throws SQLException {
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();

        try {
            if (db != null) {
                stmt.execute("use " + db);
            }
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        } finally {
            stmt.close();
            connection.close();
        }
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
            runSql(db,
                    "create table " + table +
                            " ( pk bigint NOT NULL, v0 string not null) primary KEY (pk) DISTRIBUTED BY HASH(pk) BUCKETS " +
                            numTablet + " PROPERTIES(\"replication_num\" = \"" + replicationNum +
                            "\", \"storage_medium\" = \"SSD\");");
        }

        boolean hasTask(int tsMs) {
            return tsMs % loadIntervalMs == 0;
        }

        void loadOnce() throws SQLException {
            runSql(db, "insert into " + table + " values (1,\"1\"), (2,\"2\"), (3,\"3\");");
        }
    }

    class DBLoad {
        String db;
        int numTable;
        List<TableLoad> tableLoads;
        volatile Exception error;

        int finishedTask = 0;

        DBLoad(String db, int numTable) {
            this.db = db;
            this.numTable = numTable;
        }

        void run(int numThread, int runSeconds) {
            try {
                runSql(null, "create database " + db);
            } catch (SQLException e) {
                Assert.fail(e);
            }
            ThreadPoolExecutor executor
                    = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThread);
            tableLoads = new ArrayList<>();
            Random r = new Random(0);
            int scheduleIntervalMs = 200;
            for (int i = 0; i < numTable; i++) {
                int numTablet = r.nextInt(16) + 1;
                int replicationNum = r.nextInt(3) + 1;
                int loadIntervalMs = (r.nextInt(40) + 1) * scheduleIntervalMs;
                TableLoad tableLoad = new TableLoad(db, i, numTablet, replicationNum, loadIntervalMs);
                tableLoads.add(tableLoad);
                executor.submit(() -> {
                    try {
                        tableLoad.createTable();
                    } catch (SQLException e) {
                        error = e;
                    }
                });
                if (error != null) {
                    Assert.fail(error);
                }
            }
            List<Future<?>> futures = new ArrayList<Future<?>>();
            finishedTask = 0;
            long startTs = System.nanoTime();
            int runInterval = runSeconds * 1000 / scheduleIntervalMs;
            for (int i = 0; i < runInterval; i++) {
                for (TableLoad tableLoad : tableLoads) {
                    if (tableLoad.hasTask(i * scheduleIntervalMs)) {
                        futures.add(executor.submit(() -> {
                            try {
                                tableLoad.loadOnce();
                                finishedTask++;
                            } catch (SQLException e) {
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
            System.out.printf("numLoad:%d Time: %.2fs, %.2f tps\n", finishedTask, t, finishedTask / t);
        }
    }

    boolean getEnableNewPublish() throws SQLException {
        return false;
    }

    @Test
    public void testConcurrentLoad() throws Exception {
        Config.enable_new_publish_mechanism = getEnableNewPublish();
        DBLoad dbLoad = new DBLoad("test", 1000);
        dbLoad.run(10, 10);
        try {
            FileUtils.forceDelete(new File(PseudoCluster.getInstance().getRunDir()));
        } catch (IOException e) {
            Assert.fail(e);
        }

    }
}
