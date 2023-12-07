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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/alter/MaterializedViewHandlerTest.java

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

package com.starrocks.alter;

import com.google.api.client.util.Sets;
import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Type;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.MVColumnItem;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class MaterializedViewHandlerTest {
    @Test
    public void testDifferentBaseTable(@Injectable CreateMaterializedViewStmt createMaterializedViewStmt,
                                       @Injectable Database db,
                                       @Injectable OlapTable olapTable) {
        new Expectations() {
            {
                createMaterializedViewStmt.getBaseIndexName();
                result = "t1";
                olapTable.getName();
                result = "t2";
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "processCreateMaterializedView", createMaterializedViewStmt,
                    db, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testNotNormalTable(@Injectable CreateMaterializedViewStmt createMaterializedViewStmt,
                                   @Injectable Database db,
                                   @Injectable OlapTable olapTable) {
        final String baseIndexName = "t1";
        new Expectations() {
            {
                createMaterializedViewStmt.getBaseIndexName();
                result = baseIndexName;
                olapTable.getName();
                result = baseIndexName;
                olapTable.getState();
                result = OlapTable.OlapTableState.ROLLUP;
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "processCreateMaterializedView", createMaterializedViewStmt,
                    db, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testErrorBaseIndexName(@Injectable CreateMaterializedViewStmt createMaterializedViewStmt,
                                       @Injectable Database db,
                                       @Injectable OlapTable olapTable) {
        final String baseIndexName = "t1";
        new Expectations() {
            {
                createMaterializedViewStmt.getBaseIndexName();
                result = baseIndexName;
                olapTable.getName();
                result = baseIndexName;
                olapTable.getState();
                result = OlapTable.OlapTableState.NORMAL;
                olapTable.getIndexIdByName(baseIndexName);
                result = null;
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "processCreateMaterializedView",
                    createMaterializedViewStmt, db, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testRollupReplica(@Injectable CreateMaterializedViewStmt createMaterializedViewStmt,
                                  @Injectable Database db,
                                  @Injectable OlapTable olapTable,
                                  @Injectable Partition partition,
                                  @Injectable MaterializedIndex materializedIndex) {
        final String baseIndexName = "t1";
        final Long baseIndexId = new Long(1);
        new Expectations() {
            {
                createMaterializedViewStmt.getBaseIndexName();
                result = baseIndexName;
                olapTable.getName();
                result = baseIndexName;
                olapTable.getState();
                result = OlapTable.OlapTableState.NORMAL;
                olapTable.getIndexIdByName(baseIndexName);
                result = baseIndexId;
                olapTable.getPhysicalPartitions();
                result = Lists.newArrayList(partition);
                partition.getIndex(baseIndexId);
                result = materializedIndex;
                materializedIndex.getState();
                result = MaterializedIndex.IndexState.SHADOW;
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "processCreateMaterializedView",
                    createMaterializedViewStmt, db, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testDuplicateMVName(@Injectable CreateMaterializedViewStmt createMaterializedViewStmt,
                                    @Injectable OlapTable olapTable, @Injectable Database db) {
        final String mvName = "mv1";
        new Expectations() {
            {
                olapTable.hasMaterializedIndex(mvName);
                result = true;
                createMaterializedViewStmt.getMVName();
                result = mvName;
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "checkAndPrepareMaterializedView",
                    createMaterializedViewStmt, db, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testInvalidAggregateType(@Injectable CreateMaterializedViewStmt createMaterializedViewStmt,
                                         @Injectable OlapTable olapTable, @Injectable Database db) {
        final String mvName = "mv1";
        final String mvColumName = "mv_sum_k1";
        MVColumnItem mvColumnItem = new MVColumnItem(mvColumName, Type.BIGINT, AggregateType.SUM, false, null, true,
                Sets.newHashSet());
        mvColumnItem.setIsKey(true);
        mvColumnItem.setAggregationType(null, false);
        new Expectations() {
            {
                olapTable.hasMaterializedIndex(mvName);
                result = false;
                createMaterializedViewStmt.getMVName();
                result = mvName;
                createMaterializedViewStmt.getMVColumnItemList();
                result = Lists.newArrayList(mvColumnItem);
                createMaterializedViewStmt.getMVKeysType();
                result = KeysType.AGG_KEYS;
                olapTable.getKeysType();
                result = KeysType.AGG_KEYS;
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "checkAndPrepareMaterializedView",
                    createMaterializedViewStmt, db, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testInvalidKeysType(@Injectable CreateMaterializedViewStmt createMaterializedViewStmt,
                                    @Injectable OlapTable olapTable, @Injectable Database db) {
        new Expectations() {
            {
                createMaterializedViewStmt.getMVKeysType();
                result = KeysType.DUP_KEYS;
                olapTable.getKeysType();
                result = KeysType.AGG_KEYS;
            }
        };

        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "checkAndPrepareMaterializedView",
                    createMaterializedViewStmt, db, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testDuplicateTable(@Injectable CreateMaterializedViewStmt createMaterializedViewStmt,
                                   @Injectable OlapTable olapTable, @Injectable Database db) {
        final String mvName = "mv1";
        final String columnName1 = "k1";
        Column baseColumn1 = new Column(columnName1, Type.VARCHAR, false, AggregateType.NONE, "", "");
        MVColumnItem mvColumnItem = new MVColumnItem(columnName1, Type.VARCHAR, AggregateType.NONE, false, null, true,
                Sets.newHashSet());

        mvColumnItem.setIsKey(true);
        mvColumnItem.setAggregationType(null, false);
        new Expectations() {
            {
                olapTable.hasMaterializedIndex(mvName);
                result = false;
                createMaterializedViewStmt.getMVName();
                result = mvName;
                createMaterializedViewStmt.getMVColumnItemList();
                result = Lists.newArrayList(mvColumnItem);
                olapTable.getBaseColumn(columnName1);
                result = baseColumn1;
                olapTable.getKeysType();
                result = KeysType.DUP_KEYS;
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            List<Column> mvColumns = Deencapsulation.invoke(materializedViewHandler,
                    "checkAndPrepareMaterializedView",
                    createMaterializedViewStmt, db, olapTable);
            Assert.assertEquals(1, mvColumns.size());
            Column newMVColumn = mvColumns.get(0);
            Assert.assertEquals(columnName1, newMVColumn.getName());
            Assert.assertTrue(newMVColumn.isKey());
            Assert.assertEquals(null, newMVColumn.getAggregationType());
            Assert.assertEquals(false, newMVColumn.isAggregationTypeImplicit());
            Assert.assertTrue(newMVColumn.getType().isVarchar());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void checkInvalidPartitionKeyMV(@Injectable CreateMaterializedViewStmt createMaterializedViewStmt,
                                           @Injectable OlapTable olapTable, @Injectable Database db) {
        final String mvName = "mv1";
        final String columnName1 = "k1";
        MVColumnItem mvColumnItem = new MVColumnItem(columnName1, Type.BIGINT, null, false, null, true, Sets.newHashSet());
        mvColumnItem.setIsKey(false);
        mvColumnItem.setAggregationType(AggregateType.SUM, false);
        List<String> partitionColumnNames = Lists.newArrayList();
        partitionColumnNames.add(columnName1);
        new Expectations() {
            {
                olapTable.hasMaterializedIndex(mvName);
                result = false;
                createMaterializedViewStmt.getMVName();
                result = mvName;
                createMaterializedViewStmt.getMVColumnItemList();
                result = Lists.newArrayList(mvColumnItem);
                olapTable.getKeysType();
                result = KeysType.DUP_KEYS;
                olapTable.getPartitionColumnNames();
                result = partitionColumnNames;
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "checkAndPrepareMaterializedView",
                    createMaterializedViewStmt, db, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testCheckDropMaterializedView(@Injectable OlapTable olapTable, @Injectable Partition partition,
                                              @Injectable MaterializedIndex materializedIndex,
                                              @Injectable Database db) {
        String mvName = "mv_1";
        new Expectations() {
            {
                olapTable.getState();
                result = OlapTable.OlapTableState.NORMAL;
                olapTable.getName();
                result = "table1";
                olapTable.hasMaterializedIndex(mvName);
                result = true;
                olapTable.getIndexIdByName(mvName);
                result = 1L;
                olapTable.getSchemaHashByIndexId(1L);
                result = 1;
                olapTable.getPhysicalPartitions();
                result = Lists.newArrayList(partition);
                partition.getIndex(1L);
                result = materializedIndex;
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "checkDropMaterializedView", mvName, olapTable);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

    }

}
