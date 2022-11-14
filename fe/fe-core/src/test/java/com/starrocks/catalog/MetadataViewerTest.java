// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/MetadataViewerTest.java

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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryPredicate.Operator;
import com.starrocks.backup.CatalogMocker;
import com.starrocks.catalog.Replica.ReplicaStatus;
import com.starrocks.common.AnalysisException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.system.SystemInfoService;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

public class MetadataViewerTest {

    private static Method getTabletStatusMethod;
    private static Method getTabletDistributionMethod;

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private SystemInfoService infoService;

    private static Database db;

    @BeforeClass
    public static void setUp() throws NoSuchMethodException, SecurityException, InstantiationException,
            IllegalAccessException, IllegalArgumentException, InvocationTargetException, AnalysisException {
        Class[] argTypes = new Class[] {String.class, String.class, List.class, ReplicaStatus.class, Operator.class};
        getTabletStatusMethod = MetadataViewer.class.getDeclaredMethod("getTabletStatus", argTypes);
        getTabletStatusMethod.setAccessible(true);

        argTypes = new Class[] {String.class, String.class, PartitionNames.class};
        getTabletDistributionMethod = MetadataViewer.class.getDeclaredMethod("getTabletDistribution", argTypes);
        getTabletDistributionMethod.setAccessible(true);

        db = CatalogMocker.mockDb();
    }

    @Before
    public void before() {

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getDb(anyString);
                minTimes = 0;
                result = db;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentSystemInfo();
                minTimes = 0;
                result = infoService;

                infoService.getBackendIds(anyBoolean);
                minTimes = 0;
                result = Lists.newArrayList(10000L, 10001L, 10002L);
            }
        };
    }

    @Test
    public void testGetTabletStatus()
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        List<String> partitions = Lists.newArrayList();
        Object[] args = new Object[] {CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, partitions, null,
                null};
        List<List<String>> result = (List<List<String>>) getTabletStatusMethod.invoke(null, args);
        Assert.assertEquals(3, result.size());
        System.out.println(result);

        args = new Object[] {CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, partitions, ReplicaStatus.DEAD,
                Operator.EQ};
        result = (List<List<String>>) getTabletStatusMethod.invoke(null, args);
        Assert.assertEquals(3, result.size());

        args = new Object[] {CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, partitions, ReplicaStatus.DEAD,
                Operator.NE};
        result = (List<List<String>>) getTabletStatusMethod.invoke(null, args);
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testGetTabletDistribution()
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        Object[] args = new Object[] {CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, null};
        List<List<String>> result = (List<List<String>>) getTabletDistributionMethod.invoke(null, args);
        Assert.assertEquals(3, result.size());
        System.out.println(result);
    }

}
