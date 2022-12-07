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

package com.starrocks.transaction;

import com.starrocks.metric.MetricRepo;
import com.starrocks.pseudocluster.PseudoCluster;
import org.junit.Assert;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

class DBLoad {
    static class TableLoad {
        static AtomicInteger totalTabletRead = new AtomicInteger(0);
        static AtomicInteger totalTableRead = new AtomicInteger(0);

        String db;
        String table;
        int id;
        int numTablet;
        int replicationNum;
        int loadIntervalMs;
        boolean withRead;
        boolean withUpdateDelete;
        int insertUpdateDeleteTurn = 0;

        TableLoad(String db, int id, int numTablet, int replicationNum, int loadIntervalMs, boolean withRead,
                  boolean withUpdateDelete) {
            this.db = db;
            this.table = "table_" + id;
            this.id = id;
            this.numTablet = numTablet;
            this.replicationNum = replicationNum;
            this.loadIntervalMs = loadIntervalMs;
            this.withRead = withRead;
            this.withUpdateDelete = withUpdateDelete;
        }

        void createTable() throws SQLException {
            PseudoCluster.getInstance().runSql(db,
                    "create table " + table +
                            " ( pk bigint NOT NULL, v0 string not null) primary KEY (pk) DISTRIBUTED BY HASH(pk) BUCKETS " +
                            numTablet + " PROPERTIES(\"replication_num\" = \"" + replicationNum +
                            "\", \"storage_medium\" = \"SSD\")");
        }

        boolean hasTask(int tsMs) {
            return tsMs % loadIntervalMs == 0;
        }

        void loadOnce() throws SQLException {
            List<String> sqls = new ArrayList<>();
            if (!withUpdateDelete || insertUpdateDeleteTurn % 3 == 0) {
                sqls.add("insert into " + db + "." + table + " values (1,\"1\"), (2,\"2\"), (3,\"3\")");
                if (withRead) {
                    sqls.add("select * from " + db + "." + table);
                    totalTableRead.incrementAndGet();
                    totalTabletRead.addAndGet(numTablet);
                }
            } else if (insertUpdateDeleteTurn % 3 == 1) {
                sqls.add("update " + db + "." + table + " set v0 = 'v0' where v0 = 'a'");
                totalTableRead.incrementAndGet();
                totalTabletRead.addAndGet(numTablet);
            } else if (insertUpdateDeleteTurn % 3 == 2) {
                sqls.add("delete from " + db + "." + table + " where v0 = 'a'");
                totalTableRead.incrementAndGet();
                totalTabletRead.addAndGet(numTablet);
            }
            if (withUpdateDelete) {
                insertUpdateDeleteTurn++;
            }
            PseudoCluster.getInstance().runSqlList(null, sqls, true);
        }
    }

    int numDB;
    int numTable;
    int numTabletPerTable = 0;
    boolean withRead = false;
    boolean withUpdateDelete = false;
    List<TableLoad> tableLoads;
    volatile Exception error;

    AtomicInteger finishedTask = new AtomicInteger(0);

    DBLoad(int numDB, int numTable, boolean withRead) {
        this.numDB = numDB;
        this.numTable = numTable;
        this.withRead = withRead;
    }

    DBLoad(int numDB, int numTable, int numTabletPerTable, boolean withRead, boolean withUpdateDelete) {
        this.numDB = numDB;
        this.numTable = numTable;
        this.withRead = withRead;
        this.numTabletPerTable = numTabletPerTable;
        this.withUpdateDelete = withUpdateDelete;
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
            if (this.numTabletPerTable != 0) {
                numTablet = this.numTabletPerTable;
            }
            int replicationNum = 3;
            int loadIntervalMs = (r.nextInt(40) + 1) * scheduleIntervalMs;
            String dbName = "db_" + (i % numDB);
            TableLoad tableLoad = new TableLoad(dbName, i, numTablet, replicationNum, loadIntervalMs, withRead, withUpdateDelete);
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
