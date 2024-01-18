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

import com.starrocks.common.conf.Config;
import com.starrocks.pseudocluster.PseudoBackend;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.pseudocluster.Tablet;
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
        Config.enable_statistic_collect_on_first_load = false;
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        Config.enable_statistic_collect_on_first_load = true;
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
