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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/backup/CatalogMocker.java

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

package com.starrocks.backup;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.FakeEditLog;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.PhysicalPartitionImpl;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.Util;
import com.starrocks.load.Load;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import mockit.Expectations;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public class CatalogMocker {
    // user
    public static final String ROOTUSER = "root";
    public static final String SUPERUSER = "superuser";
    public static final String TESTUSER = "testuser";
    public static final String BLOCKUSER = "blockuser";

    // backend
    public static final long BACKEND1_ID = 10000;
    public static final long BACKEND2_ID = 10001;
    public static final long BACKEND3_ID = 10002;

    // db
    public static final String TEST_DB_NAME = "test_db";
    public static final long TEST_DB_ID = 20000;

    // single partition olap table
    public static final String TEST_TBL_NAME = "test_tbl";
    public static final long TEST_TBL_ID = 30000;

    public static final String TEST_SINGLE_PARTITION_NAME = TEST_TBL_NAME;
    public static final long TEST_SINGLE_PARTITION_ID = 40000;
    public static final long TEST_TABLET0_ID = 60000;
    public static final long TEST_REPLICA0_ID = 70000;
    public static final long TEST_REPLICA1_ID = 70001;
    public static final long TEST_REPLICA2_ID = 70002;

    // mysql table
    public static final long TEST_MYSQL_TABLE_ID = 30001;
    public static final String MYSQL_TABLE_NAME = "test_mysql";
    public static final String MYSQL_HOST = "mysql-host";
    public static final long MYSQL_PORT = 8321;
    public static final String MYSQL_USER = "mysql-user";
    public static final String MYSQL_PWD = "mysql-pwd";
    public static final String MYSQL_DB = "mysql-db";
    public static final String MYSQL_TBL = "mysql-tbl";

    // partition olap table with a rollup
    public static final String TEST_TBL2_NAME = "test_tbl2";
    public static final long TEST_TBL2_ID = 30002;

    // primary key olap table
    public static final String TEST_TBL3_NAME = "test_tbl3";
    public static final long TEST_TBL3_ID = 30003;

    // partition olap table with multi physical partition
    public static final String TEST_TBL4_NAME = "test_tbl4";
    public static final long TEST_TBL4_ID = 30004;

    public static final String TEST_PARTITION1_NAME = "p1";
    public static final long TEST_PARTITION1_ID = 40001;
    public static final String TEST_PARTITION2_NAME = "p2";
    public static final long TEST_PARTITION2_ID = 40002;
    public static final String TEST_PARTITION1_NAME_PK = "p1_pk";
    public static final long TEST_PARTITION1_PK_ID = 40003;
    public static final String TEST_PARTITION2_NAME_PK = "p2_pk";
    public static final long TEST_PARTITION2_PK_ID = 40004;

    public static final long TEST_BASE_TABLET_P1_ID = 60001;
    public static final long TEST_REPLICA3_ID = 70003;
    public static final long TEST_REPLICA4_ID = 70004;
    public static final long TEST_REPLICA5_ID = 70005;
    public static final long TEST_BASE_TABLET_P1_PK_ID = 60005;
    public static final long TEST_REPLICA3_PK_ID = 70015;
    public static final long TEST_REPLICA4_PK_ID = 70016;
    public static final long TEST_REPLICA5_PK_ID = 70017;

    public static final long TEST_BASE_TABLET_P2_ID = 60002;
    public static final long TEST_REPLICA6_ID = 70006;
    public static final long TEST_REPLICA7_ID = 70007;
    public static final long TEST_REPLICA8_ID = 70008;
    public static final long TEST_BASE_TABLET_P2_PK_ID = 60006;
    public static final long TEST_REPLICA6_PK_ID = 70018;
    public static final long TEST_REPLICA7_PK_ID = 70019;
    public static final long TEST_REPLICA8_PK_ID = 70020;

    public static final String TEST_ROLLUP_NAME = "test_rollup";
    public static final long TEST_ROLLUP_ID = 50000;

    public static final long TEST_ROLLUP_TABLET_P1_ID = 60003;
    public static final long TEST_REPLICA9_ID = 70009;
    public static final long TEST_REPLICA10_ID = 70010;
    public static final long TEST_REPLICA11_ID = 70011;

    public static final long TEST_ROLLUP_TABLET_P2_ID = 60004;
    public static final long TEST_REPLICA12_ID = 70012;
    public static final long TEST_REPLICA13_ID = 70013;
    public static final long TEST_REPLICA14_ID = 70014;

    public static final String WRONG_DB = "wrong_db";

    // schema
    public static List<Column> TEST_TBL_BASE_SCHEMA = Lists.newArrayList();
    public static int SCHEMA_HASH;
    public static int ROLLUP_SCHEMA_HASH;

    public static List<Column> TEST_MYSQL_SCHEMA = Lists.newArrayList();
    public static List<Column> TEST_ROLLUP_SCHEMA = Lists.newArrayList();

    static {
        Column k1 = new Column("k1", ScalarType.createType(PrimitiveType.TINYINT), true, null, "", "key1");
        Column k2 = new Column("k2", ScalarType.createType(PrimitiveType.SMALLINT), true, null, "", "key2");
        Column k3 = new Column("k3", ScalarType.createType(PrimitiveType.INT), true, null, "", "key3");
        Column k4 = new Column("k4", ScalarType.createType(PrimitiveType.BIGINT), true, null, "", "key4");
        Column k5 = new Column("k5", ScalarType.createType(PrimitiveType.LARGEINT), true, null, "", "key5");
        Column k6 = new Column("k6", ScalarType.createType(PrimitiveType.DATE), true, null, "", "key6");
        Column k7 = new Column("k7", ScalarType.createType(PrimitiveType.DATETIME), true, null, "", "key7");
        Column k8 = new Column("k8", ScalarType.createDecimalV2Type(10, 3), true, null, "", "key8");
        k1.setIsKey(true);
        k2.setIsKey(true);
        k3.setIsKey(true);
        k4.setIsKey(true);
        k5.setIsKey(true);
        k6.setIsKey(true);
        k7.setIsKey(true);
        k8.setIsKey(true);

        Column v1 = new Column("v1",
                ScalarType.createCharType(10), false, AggregateType.REPLACE, "none", " value1");
        Column v2 = new Column("v2",
                ScalarType.createType(PrimitiveType.FLOAT), false, AggregateType.MAX, "none", " value2");
        Column v3 = new Column("v3",
                ScalarType.createType(PrimitiveType.DOUBLE), false, AggregateType.MIN, "none", " value3");
        Column v4 = new Column("v4",
                ScalarType.createType(PrimitiveType.INT), false, AggregateType.SUM, "", " value4");

        TEST_TBL_BASE_SCHEMA.add(k1);
        TEST_TBL_BASE_SCHEMA.add(k2);
        TEST_TBL_BASE_SCHEMA.add(k3);
        TEST_TBL_BASE_SCHEMA.add(k4);
        TEST_TBL_BASE_SCHEMA.add(k5);
        TEST_TBL_BASE_SCHEMA.add(k6);
        TEST_TBL_BASE_SCHEMA.add(k7);
        TEST_TBL_BASE_SCHEMA.add(k8);

        TEST_TBL_BASE_SCHEMA.add(v1);
        TEST_TBL_BASE_SCHEMA.add(v2);
        TEST_TBL_BASE_SCHEMA.add(v3);
        TEST_TBL_BASE_SCHEMA.add(v4);

        TEST_MYSQL_SCHEMA.add(k1);
        TEST_MYSQL_SCHEMA.add(k2);
        TEST_MYSQL_SCHEMA.add(k3);
        TEST_MYSQL_SCHEMA.add(k4);
        TEST_MYSQL_SCHEMA.add(k5);
        TEST_MYSQL_SCHEMA.add(k6);
        TEST_MYSQL_SCHEMA.add(k7);
        TEST_MYSQL_SCHEMA.add(k8);

        TEST_ROLLUP_SCHEMA.add(k1);
        TEST_ROLLUP_SCHEMA.add(k2);
        TEST_ROLLUP_SCHEMA.add(v1);

        SCHEMA_HASH = Util.schemaHash(0, TEST_TBL_BASE_SCHEMA, null, 0);
        ROLLUP_SCHEMA_HASH = Util.schemaHash(0, TEST_ROLLUP_SCHEMA, null, 0);
    }

    private static Auth fetchAdminAccess() {
        Auth auth = new Auth();
        new Expectations(auth) {
            {
                auth.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.checkDbPriv((ConnectContext) any, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;
            }
        };
        return auth;
    }

    public static SystemInfoService fetchSystemInfoService() {
        SystemInfoService clusterInfo = new SystemInfoService();
        return clusterInfo;
    }

    public static Database mockDb() throws AnalysisException {
        // mock all meta obj
        Database db = new Database(TEST_DB_ID, TEST_DB_NAME);

        // 1. single partition olap table
        MaterializedIndex baseIndex = new MaterializedIndex(TEST_TBL_ID, IndexState.NORMAL);
        DistributionInfo distributionInfo = new RandomDistributionInfo(32);
        Partition partition =
                new Partition(TEST_SINGLE_PARTITION_ID, TEST_SINGLE_PARTITION_NAME, baseIndex, distributionInfo);
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setReplicationNum(TEST_SINGLE_PARTITION_ID, (short) 3);
        partitionInfo.setIsInMemory(TEST_SINGLE_PARTITION_ID, false);
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        partitionInfo.setDataProperty(TEST_SINGLE_PARTITION_ID, dataProperty);
        OlapTable olapTable = new OlapTable(TEST_TBL_ID, TEST_TBL_NAME, TEST_TBL_BASE_SCHEMA,
                KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(olapTable, "baseIndexId", TEST_TBL_ID);

        LocalTablet tablet0 = new LocalTablet(TEST_TABLET0_ID);
        TabletMeta tabletMeta = new TabletMeta(TEST_DB_ID, TEST_TBL_ID, TEST_SINGLE_PARTITION_ID,
                TEST_TBL_ID, SCHEMA_HASH, TStorageMedium.HDD);
        baseIndex.addTablet(tablet0, tabletMeta);
        Replica replica0 = new Replica(TEST_REPLICA0_ID, BACKEND1_ID, 0, ReplicaState.NORMAL);
        Replica replica1 = new Replica(TEST_REPLICA1_ID, BACKEND2_ID, 0, ReplicaState.NORMAL);
        Replica replica2 = new Replica(TEST_REPLICA2_ID, BACKEND3_ID, 0, ReplicaState.NORMAL);

        tablet0.addReplica(replica0);
        tablet0.addReplica(replica1);
        tablet0.addReplica(replica2);

        olapTable.setIndexMeta(TEST_TBL_ID, TEST_TBL_NAME, TEST_TBL_BASE_SCHEMA, 0, SCHEMA_HASH, (short) 1,
                TStorageType.COLUMN, KeysType.AGG_KEYS);
        olapTable.addPartition(partition);
        db.registerTableUnlocked(olapTable);

        // 2. mysql table
        Map<String, String> mysqlProp = Maps.newHashMap();
        mysqlProp.put("host", MYSQL_HOST);
        mysqlProp.put("port", String.valueOf(MYSQL_PORT));
        mysqlProp.put("user", MYSQL_USER);
        mysqlProp.put("password", MYSQL_PWD);
        mysqlProp.put("database", MYSQL_DB);
        mysqlProp.put("table", MYSQL_TBL);
        MysqlTable mysqlTable = null;
        try {
            mysqlTable = new MysqlTable(TEST_MYSQL_TABLE_ID, MYSQL_TABLE_NAME, TEST_MYSQL_SCHEMA, mysqlProp);
        } catch (DdlException e) {
            e.printStackTrace();
        }
        db.registerTableUnlocked(mysqlTable);

        // 3. range partition olap table
        MaterializedIndex baseIndexP1 = new MaterializedIndex(TEST_TBL2_ID, IndexState.NORMAL);
        MaterializedIndex baseIndexP2 = new MaterializedIndex(TEST_TBL2_ID, IndexState.NORMAL);
        DistributionInfo distributionInfo2 =
                new HashDistributionInfo(32, Lists.newArrayList(TEST_TBL_BASE_SCHEMA.get(1)));
        Partition partition1 =
                new Partition(TEST_PARTITION1_ID, TEST_PARTITION1_NAME, baseIndexP1, distributionInfo2);
        Partition partition2 =
                new Partition(TEST_PARTITION2_ID, TEST_PARTITION2_NAME, baseIndexP2, distributionInfo2);
        RangePartitionInfo rangePartitionInfo = new RangePartitionInfo(Lists.newArrayList(TEST_TBL_BASE_SCHEMA.get(0)));

        PartitionKey rangeP1Lower =
                PartitionKey.createInfinityPartitionKey(Lists.newArrayList(TEST_TBL_BASE_SCHEMA.get(0)), false);
        PartitionKey rangeP1Upper =
                PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("10")),
                        Lists.newArrayList(TEST_TBL_BASE_SCHEMA.get(0)));
        Range<PartitionKey> rangeP1 = Range.closedOpen(rangeP1Lower, rangeP1Upper);
        rangePartitionInfo.setRange(TEST_PARTITION1_ID, false, rangeP1);

        PartitionKey rangeP2Lower =
                PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("10")),
                        Lists.newArrayList(TEST_TBL_BASE_SCHEMA.get(0)));
        PartitionKey rangeP2Upper =
                PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("20")),
                        Lists.newArrayList(TEST_TBL_BASE_SCHEMA.get(0)));
        Range<PartitionKey> rangeP2 = Range.closedOpen(rangeP2Lower, rangeP2Upper);
        rangePartitionInfo.setRange(TEST_PARTITION2_ID, false, rangeP2);

        rangePartitionInfo.setReplicationNum(TEST_PARTITION1_ID, (short) 3);
        rangePartitionInfo.setReplicationNum(TEST_PARTITION2_ID, (short) 3);
        DataProperty dataPropertyP1 = new DataProperty(TStorageMedium.HDD);
        DataProperty dataPropertyP2 = new DataProperty(TStorageMedium.HDD);
        rangePartitionInfo.setDataProperty(TEST_PARTITION1_ID, dataPropertyP1);
        rangePartitionInfo.setDataProperty(TEST_PARTITION2_ID, dataPropertyP2);

        OlapTable olapTable2 = new OlapTable(TEST_TBL2_ID, TEST_TBL2_NAME, TEST_TBL_BASE_SCHEMA,
                KeysType.AGG_KEYS, rangePartitionInfo, distributionInfo2);
        Deencapsulation.setField(olapTable2, "baseIndexId", TEST_TBL2_ID);

        LocalTablet baseTabletP1 = new LocalTablet(TEST_BASE_TABLET_P1_ID);
        TabletMeta tabletMetaBaseTabletP1 = new TabletMeta(TEST_DB_ID, TEST_TBL2_ID, TEST_PARTITION1_ID,
                TEST_TBL2_ID, SCHEMA_HASH, TStorageMedium.HDD);
        baseIndexP1.addTablet(baseTabletP1, tabletMetaBaseTabletP1);
        Replica replica3 = new Replica(TEST_REPLICA3_ID, BACKEND1_ID, 0, ReplicaState.NORMAL);
        Replica replica4 = new Replica(TEST_REPLICA4_ID, BACKEND2_ID, 0, ReplicaState.NORMAL);
        Replica replica5 = new Replica(TEST_REPLICA5_ID, BACKEND3_ID, 0, ReplicaState.NORMAL);

        baseTabletP1.addReplica(replica3);
        baseTabletP1.addReplica(replica4);
        baseTabletP1.addReplica(replica5);

        LocalTablet baseTabletP2 = new LocalTablet(TEST_BASE_TABLET_P2_ID);
        TabletMeta tabletMetaBaseTabletP2 = new TabletMeta(TEST_DB_ID, TEST_TBL2_ID, TEST_PARTITION2_ID,
                TEST_TBL2_ID, SCHEMA_HASH, TStorageMedium.HDD);
        baseIndexP2.addTablet(baseTabletP2, tabletMetaBaseTabletP2);
        Replica replica6 = new Replica(TEST_REPLICA6_ID, BACKEND1_ID, 0, ReplicaState.NORMAL);
        Replica replica7 = new Replica(TEST_REPLICA7_ID, BACKEND2_ID, 0, ReplicaState.NORMAL);
        Replica replica8 = new Replica(TEST_REPLICA8_ID, BACKEND3_ID, 0, ReplicaState.NORMAL);

        baseTabletP2.addReplica(replica6);
        baseTabletP2.addReplica(replica7);
        baseTabletP2.addReplica(replica8);

        olapTable2.setIndexMeta(TEST_TBL2_ID, TEST_TBL2_NAME, TEST_TBL_BASE_SCHEMA, 0, SCHEMA_HASH, (short) 1,
                TStorageType.COLUMN, KeysType.AGG_KEYS);
        olapTable2.addPartition(partition1);
        olapTable2.addPartition(partition2);

        // rollup index p1
        MaterializedIndex rollupIndexP1 = new MaterializedIndex(TEST_ROLLUP_ID, IndexState.NORMAL);
        LocalTablet rollupTabletP1 = new LocalTablet(TEST_ROLLUP_TABLET_P1_ID);
        TabletMeta tabletMetaRollupTabletP1 = new TabletMeta(TEST_DB_ID, TEST_TBL2_ID, TEST_PARTITION1_ID,
                TEST_ROLLUP_TABLET_P1_ID, ROLLUP_SCHEMA_HASH,
                TStorageMedium.HDD);
        rollupIndexP1.addTablet(rollupTabletP1, tabletMetaRollupTabletP1);
        Replica replica9 = new Replica(TEST_REPLICA9_ID, BACKEND1_ID, 0, ReplicaState.NORMAL);
        Replica replica10 = new Replica(TEST_REPLICA10_ID, BACKEND2_ID, 0, ReplicaState.NORMAL);
        Replica replica11 = new Replica(TEST_REPLICA11_ID, BACKEND3_ID, 0, ReplicaState.NORMAL);

        rollupTabletP1.addReplica(replica9);
        rollupTabletP1.addReplica(replica10);
        rollupTabletP1.addReplica(replica11);

        partition1.createRollupIndex(rollupIndexP1);

        // rollup index p2
        MaterializedIndex rollupIndexP2 = new MaterializedIndex(TEST_ROLLUP_ID, IndexState.NORMAL);
        LocalTablet rollupTabletP2 = new LocalTablet(TEST_ROLLUP_TABLET_P2_ID);
        TabletMeta tabletMetaRollupTabletP2 = new TabletMeta(TEST_DB_ID, TEST_TBL2_ID, TEST_PARTITION1_ID,
                TEST_ROLLUP_TABLET_P2_ID, ROLLUP_SCHEMA_HASH,
                TStorageMedium.HDD);
        rollupIndexP2.addTablet(rollupTabletP2, tabletMetaRollupTabletP2);
        Replica replica12 = new Replica(TEST_REPLICA12_ID, BACKEND1_ID, 0, ReplicaState.NORMAL);
        Replica replica13 = new Replica(TEST_REPLICA13_ID, BACKEND2_ID, 0, ReplicaState.NORMAL);
        Replica replica14 = new Replica(TEST_REPLICA14_ID, BACKEND3_ID, 0, ReplicaState.NORMAL);
        rollupTabletP2.addReplica(replica12);
        rollupTabletP2.addReplica(replica13);
        rollupTabletP2.addReplica(replica14);

        partition2.createRollupIndex(rollupIndexP2);

        olapTable2.setIndexMeta(TEST_ROLLUP_ID, TEST_ROLLUP_NAME, TEST_ROLLUP_SCHEMA, 0, ROLLUP_SCHEMA_HASH,
                (short) 1, TStorageType.COLUMN, KeysType.AGG_KEYS);
        db.registerTableUnlocked(olapTable2);

        // 4. range partition primary key olap table
        MaterializedIndex baseIndexP1Pk = new MaterializedIndex(TEST_TBL3_ID, IndexState.NORMAL);
        MaterializedIndex baseIndexP2Pk = new MaterializedIndex(TEST_TBL3_ID, IndexState.NORMAL);
        DistributionInfo distributionInfo3 =
                new HashDistributionInfo(32, Lists.newArrayList(TEST_TBL_BASE_SCHEMA.get(1)));
        Partition partition1Pk =
                new Partition(TEST_PARTITION1_PK_ID, TEST_PARTITION1_NAME_PK, baseIndexP1Pk, distributionInfo3);
        Partition partition2Pk =
                new Partition(TEST_PARTITION2_PK_ID, TEST_PARTITION2_NAME_PK, baseIndexP2Pk, distributionInfo3);
        RangePartitionInfo rangePartitionInfoPk = new RangePartitionInfo(Lists.newArrayList(TEST_TBL_BASE_SCHEMA.get(0)));

        PartitionKey rangeP1LowerPk =
                PartitionKey.createInfinityPartitionKey(Lists.newArrayList(TEST_TBL_BASE_SCHEMA.get(0)), false);
        PartitionKey rangeP1UpperPk =
                PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("10")),
                        Lists.newArrayList(TEST_TBL_BASE_SCHEMA.get(0)));
        Range<PartitionKey> rangeP1Pk = Range.closedOpen(rangeP1LowerPk, rangeP1UpperPk);
        rangePartitionInfoPk.setRange(TEST_PARTITION1_PK_ID, false, rangeP1Pk);

        PartitionKey rangeP2LowerPk =
                PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("10")),
                        Lists.newArrayList(TEST_TBL_BASE_SCHEMA.get(0)));
        PartitionKey rangeP2UpperPk =
                PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("20")),
                        Lists.newArrayList(TEST_TBL_BASE_SCHEMA.get(0)));
        Range<PartitionKey> rangeP2Pk = Range.closedOpen(rangeP2LowerPk, rangeP2UpperPk);
        rangePartitionInfoPk.setRange(TEST_PARTITION2_PK_ID, false, rangeP2Pk);

        rangePartitionInfoPk.setReplicationNum(TEST_PARTITION1_PK_ID, (short) 3);
        rangePartitionInfoPk.setReplicationNum(TEST_PARTITION2_PK_ID, (short) 3);
        DataProperty dataPropertyP1Pk = new DataProperty(TStorageMedium.HDD);
        DataProperty dataPropertyP2Pk = new DataProperty(TStorageMedium.HDD);
        rangePartitionInfoPk.setDataProperty(TEST_PARTITION1_PK_ID, dataPropertyP1Pk);
        rangePartitionInfoPk.setDataProperty(TEST_PARTITION2_PK_ID, dataPropertyP2Pk);

        OlapTable olapTable3 = new OlapTable(TEST_TBL3_ID, TEST_TBL3_NAME, TEST_TBL_BASE_SCHEMA,
                KeysType.PRIMARY_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(olapTable3, "baseIndexId", TEST_TBL3_ID);

        LocalTablet baseTabletP1Pk = new LocalTablet(TEST_BASE_TABLET_P1_PK_ID);
        TabletMeta tabletMetaBaseTabletP1Pk = new TabletMeta(TEST_DB_ID, TEST_TBL3_ID, TEST_PARTITION1_PK_ID,
                TEST_TBL3_ID, SCHEMA_HASH, TStorageMedium.HDD);
        baseIndexP1Pk.addTablet(baseTabletP1Pk, tabletMetaBaseTabletP1Pk);
        Replica replica3Pk = new Replica(TEST_REPLICA3_PK_ID, BACKEND1_ID, 0, ReplicaState.NORMAL);
        Replica replica4Pk = new Replica(TEST_REPLICA4_PK_ID, BACKEND2_ID, 0, ReplicaState.NORMAL);
        Replica replica5Pk = new Replica(TEST_REPLICA5_PK_ID, BACKEND3_ID, 0, ReplicaState.NORMAL);

        baseTabletP1Pk.addReplica(replica3Pk);
        baseTabletP1Pk.addReplica(replica4Pk);
        baseTabletP1Pk.addReplica(replica5Pk);

        LocalTablet baseTabletP2Pk = new LocalTablet(TEST_BASE_TABLET_P2_PK_ID);
        TabletMeta tabletMetaBaseTabletP2Pk = new TabletMeta(TEST_DB_ID, TEST_TBL3_ID, TEST_PARTITION2_PK_ID,
                TEST_TBL3_ID, SCHEMA_HASH, TStorageMedium.HDD);
        baseIndexP2Pk.addTablet(baseTabletP2Pk, tabletMetaBaseTabletP2Pk);
        Replica replica6Pk = new Replica(TEST_REPLICA6_PK_ID, BACKEND1_ID, 0, ReplicaState.NORMAL);
        Replica replica7Pk = new Replica(TEST_REPLICA7_PK_ID, BACKEND2_ID, 0, ReplicaState.NORMAL);
        Replica replica8Pk = new Replica(TEST_REPLICA8_PK_ID, BACKEND3_ID, 0, ReplicaState.NORMAL);

        baseTabletP2Pk.addReplica(replica6Pk);
        baseTabletP2Pk.addReplica(replica7Pk);
        baseTabletP2Pk.addReplica(replica8Pk);

        olapTable3.setIndexMeta(TEST_TBL3_ID, TEST_TBL3_NAME, TEST_TBL_BASE_SCHEMA, 0, SCHEMA_HASH, (short) 1,
                TStorageType.COLUMN, KeysType.PRIMARY_KEYS);
        olapTable3.addPartition(partition1Pk);
        olapTable3.addPartition(partition2Pk);
        db.registerTableUnlocked(olapTable3);

        // 5. range partition multi physical partition olap table
        {
            baseIndexP1 = new MaterializedIndex(TEST_TBL4_ID, IndexState.NORMAL);
            baseIndexP2 = new MaterializedIndex(TEST_TBL4_ID, IndexState.NORMAL);
            DistributionInfo distributionInfo4 = new RandomDistributionInfo(1);
            partition1 =
                    new Partition(TEST_PARTITION1_ID, TEST_PARTITION1_NAME, baseIndexP1, distributionInfo4);

            PhysicalPartition physicalPartition2 = new PhysicalPartitionImpl(
                        TEST_PARTITION2_ID, TEST_PARTITION1_ID, 0, baseIndexP2);
            partition1.addSubPartition(physicalPartition2);

            rangePartitionInfo = new RangePartitionInfo(Lists.newArrayList(TEST_TBL_BASE_SCHEMA.get(0)));
            rangePartitionInfo.setRange(TEST_PARTITION1_ID, false, rangeP1);
            rangePartitionInfo.setReplicationNum(TEST_PARTITION1_ID, (short) 3);
            rangePartitionInfo.setDataProperty(TEST_PARTITION1_ID, dataPropertyP1);

            baseTabletP1 = new LocalTablet(TEST_BASE_TABLET_P1_ID);
            tabletMetaBaseTabletP1 = new TabletMeta(TEST_DB_ID, TEST_TBL4_ID, TEST_PARTITION1_ID,
                    TEST_TBL4_ID, SCHEMA_HASH, TStorageMedium.HDD);
            baseIndexP1.addTablet(baseTabletP1, tabletMetaBaseTabletP1);
            replica3 = new Replica(TEST_REPLICA3_ID, BACKEND1_ID, 0, ReplicaState.NORMAL);
            replica4 = new Replica(TEST_REPLICA4_ID, BACKEND2_ID, 0, ReplicaState.NORMAL);
            replica5 = new Replica(TEST_REPLICA5_ID, BACKEND3_ID, 0, ReplicaState.NORMAL);

            baseTabletP1.addReplica(replica3);
            baseTabletP1.addReplica(replica4);
            baseTabletP1.addReplica(replica5);

            baseTabletP2 = new LocalTablet(TEST_BASE_TABLET_P2_ID);
            tabletMetaBaseTabletP2 = new TabletMeta(TEST_DB_ID, TEST_TBL4_ID, TEST_PARTITION2_ID,
                    TEST_TBL4_ID, SCHEMA_HASH, TStorageMedium.HDD);
            baseIndexP2.addTablet(baseTabletP2, tabletMetaBaseTabletP2);
            replica6 = new Replica(TEST_REPLICA6_ID, BACKEND1_ID, 0, ReplicaState.NORMAL);
            replica7 = new Replica(TEST_REPLICA7_ID, BACKEND2_ID, 0, ReplicaState.NORMAL);
            replica8 = new Replica(TEST_REPLICA8_ID, BACKEND3_ID, 0, ReplicaState.NORMAL);

            baseTabletP2.addReplica(replica6);
            baseTabletP2.addReplica(replica7);
            baseTabletP2.addReplica(replica8);

            OlapTable olapTable4 = new OlapTable(TEST_TBL4_ID, TEST_TBL4_NAME, TEST_TBL_BASE_SCHEMA,
                    KeysType.DUP_KEYS, rangePartitionInfo, distributionInfo4);
            Deencapsulation.setField(olapTable4, "baseIndexId", TEST_TBL4_ID);
            olapTable4.setIndexMeta(TEST_TBL4_ID, TEST_TBL4_NAME, TEST_TBL_BASE_SCHEMA, 0, SCHEMA_HASH, (short) 1,
                            TStorageType.COLUMN, KeysType.DUP_KEYS);

            olapTable4.addPartition(partition1);

            db.registerTableUnlocked(olapTable4);
        }

        return db;
    }

    public static GlobalStateMgr fetchAdminCatalog() {
        try {
            FakeEditLog fakeEditLog = new FakeEditLog();

            GlobalStateMgr globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);

            Database db = new Database();
            Auth auth = fetchAdminAccess();

            new Expectations(globalStateMgr) {
                {
                    globalStateMgr.getAuth();
                    minTimes = 0;
                    result = auth;

                    globalStateMgr.getDb(TEST_DB_NAME);
                    minTimes = 0;
                    result = db;

                    globalStateMgr.getDb(WRONG_DB);
                    minTimes = 0;
                    result = null;

                    globalStateMgr.getDb(TEST_DB_ID);
                    minTimes = 0;
                    result = db;

                    globalStateMgr.getDb(anyString);
                    minTimes = 0;
                    result = new Database();

                    globalStateMgr.getLocalMetastore().listDbNames();
                    minTimes = 0;
                    result = Lists.newArrayList(TEST_DB_NAME);

                    globalStateMgr.getLoadInstance();
                    minTimes = 0;
                    result = new Load();

                    globalStateMgr.getEditLog();
                    minTimes = 0;
                    result = new EditLog(new ArrayBlockingQueue<>(100));

                    globalStateMgr.changeCatalogDb((ConnectContext) any, WRONG_DB);
                    minTimes = 0;
                    result = new DdlException("failed");

                    globalStateMgr.changeCatalogDb((ConnectContext) any, anyString);
                    minTimes = 0;
                }
            };
            return globalStateMgr;
        } catch (DdlException e) {
            return null;
        }
    }

    public static Auth fetchBlockAccess() {
        Auth auth = new Auth();
        new Expectations(auth) {
            {
                auth.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = false;

                auth.checkDbPriv((ConnectContext) any, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = false;

                auth.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = false;
            }
        };
        return auth;
    }
}
