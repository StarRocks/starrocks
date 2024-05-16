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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/common/proc/DbsProcDirTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.proc;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DbsProcDirTest {
    private Database db1;
    private Database db2;
    @Mocked
    private GlobalStateMgr globalStateMgr;

    // construct test case
    //  globalStateMgr
    //  | - db1
    //  | - db2

    @Before
    public void setUp() {
        db1 = new Database(10000L, "db1");
        db2 = new Database(10001L, "db2");
    }

    @After
    public void tearDown() {
        globalStateMgr = null;
    }

    @Test
    public void testRegister() {
        DbsProcDir dir;

        dir = new DbsProcDir(globalStateMgr);
        Assert.assertFalse(dir.register("db1", new BaseProcDir()));
    }

    @Test(expected = AnalysisException.class)
    public void testLookupNormal() throws AnalysisException {
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getDb("db1");
                minTimes = 0;
                result = db1;

                globalStateMgr.getDb("db2");
                minTimes = 0;
                result = db2;

                globalStateMgr.getDb("db3");
                minTimes = 0;
                result = null;

                globalStateMgr.getDb(db1.getId());
                minTimes = 0;
                result = db1;

                globalStateMgr.getDb(db2.getId());
                minTimes = 0;
                result = db2;

                globalStateMgr.getDb(anyLong);
                minTimes = 0;
                result = null;
            }
        };

        DbsProcDir dir;
        ProcNodeInterface node;

        dir = new DbsProcDir(globalStateMgr);
        try {
            node = dir.lookup(String.valueOf(db1.getId()));
            Assert.assertNotNull(node);
            Assert.assertTrue(node instanceof TablesProcDir);
        } catch (AnalysisException e) {
            Assert.fail();
        }

        dir = new DbsProcDir(globalStateMgr);
        try {
            node = dir.lookup(String.valueOf(db2.getId()));
            Assert.assertNotNull(node);
            Assert.assertTrue(node instanceof TablesProcDir);
        } catch (AnalysisException e) {
            Assert.fail();
        }

        dir = new DbsProcDir(globalStateMgr);
        node = dir.lookup("10002");
        Assert.assertNull(node);
    }

    @Test
    public void testLookupInvalid() {
        DbsProcDir dir;
        ProcNodeInterface node;

        dir = new DbsProcDir(globalStateMgr);
        try {
            node = dir.lookup(null);
        } catch (AnalysisException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try {
            node = dir.lookup("");
        } catch (AnalysisException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Test
    public void testFetchResultNormal() throws AnalysisException {
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getLocalMetastore().listDbNames();
                minTimes = 0;
                result = Lists.newArrayList("db1", "db2");

                globalStateMgr.getDb("db1");
                minTimes = 0;
                result = db1;

                globalStateMgr.getDb("db2");
                minTimes = 0;
                result = db2;

                globalStateMgr.getDb("db3");
                minTimes = 0;
                result = null;

                globalStateMgr.getDb(db1.getId());
                minTimes = 0;
                result = db1;

                globalStateMgr.getDb(db2.getId());
                minTimes = 0;
                result = db2;

                globalStateMgr.getDb(anyLong);
                minTimes = 0;
                result = null;
            }
        };

        DbsProcDir dir;
        ProcResult result;

        dir = new DbsProcDir(globalStateMgr);
        result = dir.fetchResult();
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof BaseProcResult);

        Assert.assertEquals(
                Lists.newArrayList("DbId", "DbName", "TableNum", "Quota", "LastConsistencyCheckTime", "ReplicaQuota"),
                result.getColumnNames());
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Arrays.asList(String.valueOf(db1.getId()), db1.getOriginName(), "0", "8388608.000 TB",
                FeConstants.NULL_STRING, "9223372036854775807"));
        rows.add(Arrays.asList(String.valueOf(db2.getId()), db2.getOriginName(), "0", "8388608.000 TB",
                FeConstants.NULL_STRING, "9223372036854775807"));
        Assert.assertEquals(rows, result.getRows());
    }

    @Test
    public void testFetchResultInvalid() throws AnalysisException {
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getLocalMetastore().listDbNames();
                minTimes = 0;
                result = null;
            }
        };

        DbsProcDir dir;
        ProcResult result;

        dir = new DbsProcDir(null);
        try {
            result = dir.fetchResult();
        } catch (NullPointerException e) {
            e.printStackTrace();
        }

        dir = new DbsProcDir(globalStateMgr);
        result = dir.fetchResult();
        Assert.assertEquals(
                Lists.newArrayList("DbId", "DbName", "TableNum", "Quota", "LastConsistencyCheckTime", "ReplicaQuota"),
                result.getColumnNames());
        List<List<String>> rows = Lists.newArrayList();
        Assert.assertEquals(rows, result.getRows());
    }
}
