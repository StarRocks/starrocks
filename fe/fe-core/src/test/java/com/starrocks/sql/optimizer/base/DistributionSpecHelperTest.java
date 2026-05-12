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

package com.starrocks.sql.optimizer.base;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.ColocateGroupSchema;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for {@link DistributionSpecHelper#buildRangeDistributionSpecSkeleton}.
 * Covers the null-fallback paths (unknown group, non-range group, missing
 * groupSchema, catalog-schema-too-short, column not in map) plus the happy path.
 */
class DistributionSpecHelperTest {

    private static final long TABLE_ID = 100L;

    private static Column col(String name) {
        return new Column(name, IntegerType.INT);
    }

    private static Map<Column, ColumnRefOperator> mapping(Column... cs) {
        ImmutableMap.Builder<Column, ColumnRefOperator> b = ImmutableMap.builder();
        int nextId = 1;
        for (Column c : cs) {
            b.put(c, new ColumnRefOperator(nextId++, IntegerType.INT, c.getName(), true));
        }
        return b.build();
    }

    @Test
    void nullWhenTableNotColocate(@Mocked OlapTable olapTable,
                                  @Mocked GlobalStateMgr globalStateMgr,
                                  @Mocked ColocateTableIndex colocateTableIndex) {
        new Expectations() {
            {
                olapTable.getId();
                result = TABLE_ID;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getColocateTableIndex();
                result = colocateTableIndex;
                colocateTableIndex.isColocateTable(TABLE_ID);
                result = false;
            }
        };
        assertNull(DistributionSpecHelper.buildRangeDistributionSpecSkeleton(
                olapTable, mapping(col("a"))));
    }

    @Test
    void nullWhenGroupIsNull(@Mocked OlapTable olapTable,
                             @Mocked GlobalStateMgr globalStateMgr,
                             @Mocked ColocateTableIndex colocateTableIndex) {
        new Expectations() {
            {
                olapTable.getId();
                result = TABLE_ID;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getColocateTableIndex();
                result = colocateTableIndex;
                colocateTableIndex.isColocateTable(TABLE_ID);
                result = true;
                colocateTableIndex.getGroup(TABLE_ID);
                result = null;
            }
        };
        assertNull(DistributionSpecHelper.buildRangeDistributionSpecSkeleton(
                olapTable, mapping(col("a"))));
    }

    @Test
    void nullWhenHashGroup(@Mocked OlapTable olapTable,
                           @Mocked GlobalStateMgr globalStateMgr,
                           @Mocked ColocateTableIndex colocateTableIndex) {
        ColocateTableIndex.GroupId groupId = new ColocateTableIndex.GroupId(1L, 2L);
        new Expectations() {
            {
                olapTable.getId();
                result = TABLE_ID;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getColocateTableIndex();
                result = colocateTableIndex;
                colocateTableIndex.isColocateTable(TABLE_ID);
                result = true;
                colocateTableIndex.getGroup(TABLE_ID);
                result = groupId;
                colocateTableIndex.isRangeColocateGroup(groupId);
                result = false;
            }
        };
        assertNull(DistributionSpecHelper.buildRangeDistributionSpecSkeleton(
                olapTable, mapping(col("a"))));
    }

    @Test
    void nullWhenGroupSchemaMissing(@Mocked OlapTable olapTable,
                                    @Mocked GlobalStateMgr globalStateMgr,
                                    @Mocked ColocateTableIndex colocateTableIndex) {
        ColocateTableIndex.GroupId groupId = new ColocateTableIndex.GroupId(1L, 2L);
        new Expectations() {
            {
                olapTable.getId();
                result = TABLE_ID;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getColocateTableIndex();
                result = colocateTableIndex;
                colocateTableIndex.isColocateTable(TABLE_ID);
                result = true;
                colocateTableIndex.getGroup(TABLE_ID);
                result = groupId;
                colocateTableIndex.isRangeColocateGroup(groupId);
                result = true;
                colocateTableIndex.getGroupSchema(groupId);
                result = null;
            }
        };
        assertNull(DistributionSpecHelper.buildRangeDistributionSpecSkeleton(
                olapTable, mapping(col("a"))));
    }

    @Test
    void nullWhenCatalogSchemaShorterThanColocateCount(@Mocked OlapTable olapTable,
                                                        @Mocked GlobalStateMgr globalStateMgr,
                                                        @Mocked ColocateTableIndex colocateTableIndex,
                                                        @Mocked ColocateGroupSchema schema,
                                                        @Mocked MetaUtils metaUtils) {
        ColocateTableIndex.GroupId groupId = new ColocateTableIndex.GroupId(1L, 2L);
        Column onlyCol = col("a");
        new Expectations() {
            {
                olapTable.getId();
                result = TABLE_ID;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getColocateTableIndex();
                result = colocateTableIndex;
                colocateTableIndex.isColocateTable(TABLE_ID);
                result = true;
                colocateTableIndex.getGroup(TABLE_ID);
                result = groupId;
                colocateTableIndex.isRangeColocateGroup(groupId);
                result = true;
                colocateTableIndex.getGroupSchema(groupId);
                result = schema;
                MetaUtils.getRangeDistributionColumns(olapTable);
                result = List.of(onlyCol);
                schema.getColocateColumnCount();
                result = 2; // group expects 2 cols; catalog has 1 → fallback.
            }
        };
        assertNull(DistributionSpecHelper.buildRangeDistributionSpecSkeleton(
                olapTable, mapping(onlyCol)));
    }

    @Test
    void nullWhenColumnMissingFromRefMap(@Mocked OlapTable olapTable,
                                          @Mocked GlobalStateMgr globalStateMgr,
                                          @Mocked ColocateTableIndex colocateTableIndex,
                                          @Mocked ColocateGroupSchema schema,
                                          @Mocked MetaUtils metaUtils) {
        ColocateTableIndex.GroupId groupId = new ColocateTableIndex.GroupId(1L, 2L);
        Column colA = col("a");
        Column colB = col("b");
        new Expectations() {
            {
                olapTable.getId();
                result = TABLE_ID;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getColocateTableIndex();
                result = colocateTableIndex;
                colocateTableIndex.isColocateTable(TABLE_ID);
                result = true;
                colocateTableIndex.getGroup(TABLE_ID);
                result = groupId;
                colocateTableIndex.isRangeColocateGroup(groupId);
                result = true;
                colocateTableIndex.getGroupSchema(groupId);
                result = schema;
                MetaUtils.getRangeDistributionColumns(olapTable);
                result = List.of(colA, colB);
                schema.getColocateColumnCount();
                result = 2;
            }
        };
        // Provide mapping only for colA; colB missing → fallback to null.
        assertNull(DistributionSpecHelper.buildRangeDistributionSpecSkeleton(
                olapTable, mapping(colA)));
    }

    @Test
    void buildsSkeletonSpecOnHappyPath(@Mocked OlapTable olapTable,
                                       @Mocked GlobalStateMgr globalStateMgr,
                                       @Mocked ColocateTableIndex colocateTableIndex,
                                       @Mocked ColocateGroupSchema schema,
                                       @Mocked MetaUtils metaUtils) {
        ColocateTableIndex.GroupId groupId = new ColocateTableIndex.GroupId(1L, 2L);
        Column colA = col("a");
        Column colB = col("b");
        Map<Column, ColumnRefOperator> map = mapping(colA, colB);
        new Expectations() {
            {
                olapTable.getId();
                result = TABLE_ID;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getColocateTableIndex();
                result = colocateTableIndex;
                colocateTableIndex.isColocateTable(TABLE_ID);
                result = true;
                colocateTableIndex.getGroup(TABLE_ID);
                result = groupId;
                colocateTableIndex.isRangeColocateGroup(groupId);
                result = true;
                colocateTableIndex.getGroupSchema(groupId);
                result = schema;
                MetaUtils.getRangeDistributionColumns(olapTable);
                result = List.of(colA, colB);
                schema.getColocateColumnCount();
                result = 2;
            }
        };
        RangeDistributionSpec spec =
                DistributionSpecHelper.buildRangeDistributionSpecSkeleton(olapTable, map);
        assertNotNull(spec);
        assertEquals(2, spec.getColocateColumns().size());
        assertEquals(map.get(colA).getId(), spec.getColocateColumns().get(0).getColId());
        assertEquals(map.get(colB).getId(), spec.getColocateColumns().get(1).getColId());
        assertEquals(TABLE_ID, spec.getEquivalentDescriptor().getTableId());
        assertEquals(DistributionSpec.DistributionType.RANGE_LOCAL, spec.getType());
    }
}
