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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/PartitionKeyTest.java

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

import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.PartitionValue;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

public class PartitionKeyTest {

    private static List<Column> allColumns;
    private static Column tinyInt;
    private static Column smallInt;
    private static Column int32;
    private static Column bigInt;
    private static Column largeInt;
    private static Column date;
    private static Column datetime;

    private GlobalStateMgr globalStateMgr;

    @BeforeClass
    public static void setUp() {
        TimeZone tz = TimeZone.getTimeZone("ETC/GMT-0");
        TimeZone.setDefault(tz);

        tinyInt = new Column("tinyint", Type.TINYINT);
        smallInt = new Column("smallint", Type.SMALLINT);
        int32 = new Column("int32", Type.INT);
        bigInt = new Column("bigint", Type.BIGINT);
        largeInt = new Column("largeint", Type.LARGEINT);
        date = new Column("date", Type.DATE);
        datetime = new Column("datetime", Type.DATETIME);

        allColumns = Arrays.asList(tinyInt, smallInt, int32, bigInt, largeInt, date, datetime);
    }

    @Test
    public void compareTest() throws AnalysisException {
        PartitionKey pk1;
        PartitionKey pk2;

        // case1
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("127"), new PartitionValue("32767")),
                Arrays.asList(tinyInt, smallInt));
        pk2 = PartitionKey.createInfinityPartitionKey(Arrays.asList(tinyInt, smallInt), true);
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case2
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("127")),
                Arrays.asList(tinyInt, smallInt));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("127"), new PartitionValue("-32768")),
                Arrays.asList(tinyInt, smallInt));
        Assert.assertTrue(pk1.equals(pk2) && pk1.compareTo(pk2) == 0);

        // case3
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("127")),
                Arrays.asList(int32, bigInt));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("128"), new PartitionValue("-32768")),
                Arrays.asList(int32, bigInt));
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case4
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("127"), new PartitionValue("12345")),
                Arrays.asList(largeInt, bigInt));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("127"), new PartitionValue("12346")),
                Arrays.asList(largeInt, bigInt));
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case5
        pk1 = PartitionKey.createPartitionKey(
                Arrays.asList(new PartitionValue("2014-12-12"), new PartitionValue("2014-12-12 10:00:00")),
                Arrays.asList(date, datetime));
        pk2 = PartitionKey.createPartitionKey(
                Arrays.asList(new PartitionValue("2014-12-12"), new PartitionValue("2014-12-12 10:00:01")),
                Arrays.asList(date, datetime));
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case6
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("-128")),
                Arrays.asList(tinyInt, smallInt));
        pk2 = PartitionKey.createInfinityPartitionKey(Arrays.asList(tinyInt, smallInt), false);
        Assert.assertTrue(pk1.equals(pk2) && pk1.compareTo(pk2) == 0);

        // case7
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("127")),
                Arrays.asList(tinyInt, smallInt));
        pk2 = PartitionKey.createInfinityPartitionKey(Arrays.asList(tinyInt, smallInt), true);
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case7
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("127"), new PartitionValue("32767")),
                Arrays.asList(tinyInt, smallInt));
        pk2 = PartitionKey.createInfinityPartitionKey(Arrays.asList(tinyInt, smallInt), true);
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case8
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("127"), new PartitionValue("32767"),
                        new PartitionValue("2147483647"), new PartitionValue("9223372036854775807"),
                        new PartitionValue("170141183460469231731687303715884105727"),
                        new PartitionValue("9999-12-31"), new PartitionValue("9999-12-31 23:59:59")),
                allColumns);
        pk2 = PartitionKey.createInfinityPartitionKey(allColumns, true);
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case9
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("-128"), new PartitionValue("-32768"),
                        new PartitionValue("-2147483648"), new PartitionValue("-9223372036854775808"),
                        new PartitionValue("-170141183460469231731687303715884105728"),
                        new PartitionValue("0000-01-01"), new PartitionValue("0000-01-01 00:00:00")),
                allColumns);
        pk2 = PartitionKey.createInfinityPartitionKey(allColumns, false);
        Assert.assertTrue(pk1.equals(pk2) && pk1.compareTo(pk2) == 0);

        // case10
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("-128"), new PartitionValue("-32768"),
                        new PartitionValue("0"), new PartitionValue("-9223372036854775808"),
                        new PartitionValue("0"), new PartitionValue("1970-01-01"), new PartitionValue("1970-01-01 00:00:00")),
                allColumns);
        pk2 = PartitionKey.createInfinityPartitionKey(allColumns, false);
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == 1);
    }

    @Test
    public void testSerialization() throws Exception {
        FakeGlobalStateMgr fakeGlobalStateMgr = new FakeGlobalStateMgr();
        FakeGlobalStateMgr.setMetaVersion(FeConstants.META_VERSION);

        // 1. Write objects to file
        File file = new File("./keyRangePartition");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        PartitionKey keyEmpty = new PartitionKey();
        keyEmpty.write(dos);

        List<PartitionValue> keys = new ArrayList<PartitionValue>();
        List<Column> columns = new ArrayList<Column>();
        keys.add(new PartitionValue("100"));
        columns.add(new Column("column2", Type.TINYINT, true, null, "", ""));
        keys.add(new PartitionValue("101"));
        columns.add(new Column("column3", Type.SMALLINT, true, null, "", ""));
        keys.add(new PartitionValue("102"));
        columns.add(new Column("column4", Type.INT, true, null, "", ""));
        keys.add(new PartitionValue("103"));
        columns.add(new Column("column5", Type.BIGINT, true, null, "", ""));
        keys.add(new PartitionValue("2014-12-26"));
        columns.add(new Column("column10", Type.DATE, true, null, "", ""));
        keys.add(new PartitionValue("2014-12-27 11:12:13"));
        columns.add(new Column("column11", Type.DATETIME, true, null, "", ""));

        PartitionKey key = PartitionKey.createPartitionKey(keys, columns);
        key.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        PartitionKey rKeyEmpty = PartitionKey.read(dis);
        Assert.assertTrue(keyEmpty.equals(rKeyEmpty));

        PartitionKey rKey = new PartitionKey();
        rKey.readFields(dis);
        Assert.assertTrue(key.equals(rKey));
        Assert.assertTrue(key.equals(key));
        Assert.assertFalse(key.equals(this));

        // 3. delete files
        dis.close();
        file.delete();
    }
}
