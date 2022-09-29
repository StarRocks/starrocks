// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.transaction;

import com.starrocks.common.Config;
import com.starrocks.pseudocluster.PseudoBackend;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.pseudocluster.Tablet;
import com.starrocks.qe.Coordinator;
import com.starrocks.qe.ResultReceiver;
import com.starrocks.qe.StmtExecutor;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentTxnTest {
    @BeforeClass
    public static void setUp() throws Exception {
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
    }

    @AfterClass
    public static void tearDown() throws Exception {
    }

    int runTime = 2;
    int numDB = 2;
    int numTable = 100;
    int numThread = 2;
    // 0 means random num of tablets
    int numTabletPerTable = 0;
    int runSeconds = 3;
    boolean withRead = true;
    boolean withUpdateDelete = true;
    boolean deleteRunDir = true;

    void setup() throws SQLException {
        Config.enable_new_publish_mechanism = false;
    }

    @Test
    public void testConcurrentLoad() throws Exception {
        setup();
        for (int i = 0; i < runTime; i++) {
            DBLoad.TableLoad.totalTabletRead.set(0);
            DBLoad.TableLoad.totalTableRead.set(0);
            PseudoBackend.scansByQueryId.clear();
            DBLoad dbLoad = new DBLoad(numDB, numTable, numTabletPerTable, withRead, withUpdateDelete);
            dbLoad.run(numThread, runSeconds);
            System.out.printf("totalReadExpected: %d totalRead: %d totalSucceed: %d totalFail: %d\n",
                    DBLoad.TableLoad.totalTabletRead.get(), Tablet.getTotalReadExecuted(), Tablet.getTotalReadSucceed(),
                    Tablet.getTotalReadFailed());
            if (numTabletPerTable != 0) {
                for (Map.Entry<String, AtomicInteger> kv : PseudoBackend.scansByQueryId.entrySet()) {
                    if (kv.getValue().get() != numTabletPerTable) {
                        String msg = String.format("queryId: %s numScan: %d\n", kv.getKey(), kv.getValue().get());
                        Assert.fail(msg);
                    }
                }
            }
            System.out.printf("tableRead: %d scanQueryId: %d\n", DBLoad.TableLoad.totalTableRead.get(),
                    PseudoBackend.scansByQueryId.size());
            Assert.assertEquals(DBLoad.TableLoad.totalTableRead.get(), PseudoBackend.scansByQueryId.size());
            Assert.assertEquals(DBLoad.TableLoad.totalTabletRead.get(), Tablet.getTotalReadSucceed());
            Tablet.clearStats();
        }
        PseudoCluster.getInstance().shutdown(deleteRunDir);
    }
}
