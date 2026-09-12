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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/planner/StreamLoadScanNodeTest.java

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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.load.Load;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TBrokerScanRangeParams;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TEnvelopeType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TSlotDescriptor;
import com.starrocks.thrift.TStreamLoadPutRequest;
import com.starrocks.thrift.TTypeNode;
import com.starrocks.type.ArrayType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.HLLType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class StreamLoadScanNodeTest {
    @Mocked
    GlobalStateMgr globalStateMgr;

    @Injectable
    ConnectContext connectContext;

    @Injectable
    OlapTable dstTable;

    @BeforeEach
    public void setUp() {
        SqlParser sqlParser = new SqlParser(AstBuilder.getInstance());
        new MockUp<GlobalStateMgr>() {
            @Mock
            public SqlParser getSqlParser() {
                return sqlParser;
            }
        };
    }

    TStreamLoadPutRequest getBaseRequest() {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setFileType(TFileType.FILE_STREAM);
        request.setFormatType(TFileFormatType.FORMAT_CSV_PLAIN);
        request.setColumnSeparator(",");
        request.setRowDelimiter("\n");
        return request;
    }

    List<Column> getBaseSchema() {
        List<Column> columns = Lists.newArrayList();

        Column k1 = new Column("k1", IntegerType.BIGINT);
        k1.setIsKey(true);
        k1.setIsAllowNull(false);
        columns.add(k1);

        Column k2 = new Column("k2", TypeFactory.createVarcharType(25));
        k2.setIsKey(true);
        k2.setIsAllowNull(true);
        columns.add(k2);

        Column v1 = new Column("v1", IntegerType.BIGINT);
        v1.setIsKey(false);
        v1.setIsAllowNull(true);
        v1.setAggregationType(AggregateType.SUM, false);

        columns.add(v1);

        Column v2 = new Column("v2", TypeFactory.createVarcharType(25));
        v2.setIsKey(false);
        v2.setAggregationType(AggregateType.REPLACE, false);
        v2.setIsAllowNull(false);
        columns.add(v2);

        return columns;
    }

    List<Column> getHllSchema() {
        List<Column> columns = Lists.newArrayList();

        Column k1 = new Column("k1", IntegerType.BIGINT);
        k1.setIsKey(true);
        k1.setIsAllowNull(false);
        columns.add(k1);

        Column v1 = new Column("v1", HLLType.HLL);
        v1.setIsKey(false);
        v1.setIsAllowNull(true);
        v1.setAggregationType(AggregateType.HLL_UNION, false);

        columns.add(v1);

        return columns;
    }

    List<Column> getDecimalSchema() {
        List<Column> columns = Lists.newArrayList();

        Column c0 = new Column("c0", DecimalType.DEFAULT_DECIMAL32);
        c0.setIsKey(false);
        columns.add(c0);

        Column c1 = new Column("c1", DecimalType.DEFAULT_DECIMAL64);
        c0.setIsKey(false);
        columns.add(c1);

        Column c2 = new Column("c2", DecimalType.DEFAULT_DECIMAL128);
        c0.setIsKey(false);
        columns.add(c2);

        return columns;
    }

    private StreamLoadScanNode getStreamLoadScanNode(TupleDescriptor dstDesc, TStreamLoadPutRequest request)
            throws StarRocksException {
        StreamLoadInfo streamLoadInfo = StreamLoadInfo.fromTStreamLoadPutRequest(request, null);
        StreamLoadScanNode scanNode =
                new StreamLoadScanNode(streamLoadInfo.getId(), new PlanNodeId(1), dstDesc, dstTable, streamLoadInfo);
        return scanNode;
    }

    /**
     * A schema with one key column and two value columns, both of which carry a constant DEFAULT.
     * The key column carries one too, so a test can check that a merging key is left alone.
     */
    private List<Column> getDefaultValueSchema() {
        List<Column> columns = Lists.newArrayList();

        Column k1 = new Column("k1", IntegerType.BIGINT, true, null, false,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("7")), "");
        columns.add(k1);

        Column v1 = new Column("v1", IntegerType.BIGINT, false, AggregateType.REPLACE, true,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("99")), "");
        columns.add(v1);

        Column v2 = new Column("v2", TypeFactory.createVarcharType(25), false, AggregateType.REPLACE, true,
                ColumnDef.DefaultValueDef.NOT_SET, "");
        columns.add(v2);

        columns.add(exprObjectDefaultColumn());

        // Absence is how a REPLACE_IF_NOT_NULL column keeps its stored value, so it must not be
        // filled even though it declares a DEFAULT.
        Column v4 = new Column("v4", IntegerType.BIGINT, false, AggregateType.REPLACE_IF_NOT_NULL, true,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("5")), "");
        columns.add(v4);

        return columns;
    }

    /**
     * A column whose DEFAULT is held as an expression object rather than a rendered string. That is
     * how a complex default such as {@code ARRAY<INT> DEFAULT [1, 2]} is stored, and
     * {@code calculatedDefaultValue()} returns null for it.
     */
    private Column exprObjectDefaultColumn() {
        Type arrayType = ArrayType.ARRAY_INT;
        ArrayExpr defaultArray = new ArrayExpr(arrayType,
                Lists.newArrayList(new IntLiteral(1, IntegerType.INT), new IntLiteral(2, IntegerType.INT)));
        return new Column("v3", arrayType, false, AggregateType.REPLACE, true,
                new ColumnDef.DefaultValueDef(true, defaultArray), "");
    }

    /** Source slot ids from the most recent {@link #runAndGetAbsentKeyDefaults} call, in schema order. */
    private List<Integer> lastSrcSlotIds;

    private Map<Integer, TExpr> runAndGetAbsentKeyDefaults(
            boolean fillDefaultOnAbsentKey, KeysType keysType) throws StarRocksException {
        DescriptorTable descTbl = new DescriptorTable();

        List<Column> columns = getDefaultValueSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            slot.setIsNullable(column.isAllowNull());
        }

        TStreamLoadPutRequest request = getBaseRequest();
        request.setFormatType(TFileFormatType.FORMAT_JSON);
        request.setFill_default_on_absent_key(fillDefaultOnAbsentKey);
        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

        new Expectations() {{
            dstTable.getBaseSchema();
            result = columns;
            minTimes = 0;
            dstTable.getFullSchema();
            result = columns;
            minTimes = 0;
            dstTable.getColumn("k1");
            result = columns.get(0);
            minTimes = 0;
            dstTable.getColumn("v1");
            result = columns.get(1);
            minTimes = 0;
            dstTable.getColumn("v2");
            result = columns.get(2);
            minTimes = 0;
            dstTable.getColumn("v3");
            result = columns.get(3);
            minTimes = 0;
            dstTable.getColumn("v4");
            result = columns.get(4);
            minTimes = 0;
            dstTable.getKeysType();
            result = keysType;
            minTimes = 0;
        }};

        scanNode.init(descTbl);
        scanNode.finalizeStats();

        List<TScanRangeLocations> locations = scanNode.getScanRangeLocations(0);
        Assertions.assertEquals(1, locations.size());
        TBrokerScanRangeParams params = locations.get(0).scan_range.broker_scan_range.params;
        lastSrcSlotIds = params.getSrc_slot_ids();
        Map<Integer, TExpr> defaults = params.getDefault_expr_of_src_slot();
        if (defaults != null) {
            // Every key must be a source slot id. A destination slot id here would look right by
            // count and find nothing on the BE, which looks up by source slot id.
            for (Integer slotId : defaults.keySet()) {
                Assertions.assertTrue(params.getSrc_slot_ids().contains(slotId),
                        "default keyed on " + slotId + ", which is not a source slot id");
            }
        }
        return defaults;
    }

    private int indexOfColumn(String columnName) {
        List<Column> columns = getDefaultValueSchema();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException(columnName);
    }

    /**
     * A value column with a constant DEFAULT gets an expression sent down, so a row with no key for
     * it is filled rather than nulled. A value column with no DEFAULT does not, which leaves the
     * existing behavior in place for it.
     */
    @Test
    public void testFillDefaultOnAbsentKeySendsDefaultForValueColumn() throws StarRocksException {
        Map<Integer, TExpr> defaults = runAndGetAbsentKeyDefaults(true, KeysType.AGG_KEYS);
        Assertions.assertNotNull(defaults);
        // v1 and v3: k1 is a key on a merging table, and v2 has no DEFAULT.
        Assertions.assertEquals(
                Set.of(lastSrcSlotIds.get(indexOfColumn("v1")), lastSrcSlotIds.get(indexOfColumn("v3"))),
                defaults.keySet());
    }

    /**
     * A REPLACE_IF_NOT_NULL column relies on an absent key meaning NULL to keep its stored value, so
     * filling it from its DEFAULT would overwrite exactly what the user meant to keep. This is the
     * same conflict that makes partial_update refuse the option outright.
     */
    @Test
    public void testFillDefaultOnAbsentKeySkipsReplaceIfNotNull() throws StarRocksException {
        Map<Integer, TExpr> defaults = runAndGetAbsentKeyDefaults(true, KeysType.AGG_KEYS);
        Assertions.assertNotNull(defaults);
        Assertions.assertFalse(defaults.containsKey(lastSrcSlotIds.get(indexOfColumn("v4"))),
                "a REPLACE_IF_NOT_NULL column must never be filled from its DEFAULT");
    }

    /**
     * A complex DEFAULT such as {@code ARRAY<INT> DEFAULT [1, 2]} is stored as an expression object
     * rather than a rendered string, so reading only the rendered value would drop it and load NULL.
     */
    @Test
    public void testFillDefaultOnAbsentKeySendsExprObjectDefault() throws StarRocksException {
        Map<Integer, TExpr> defaults = runAndGetAbsentKeyDefaults(true, KeysType.AGG_KEYS);
        Assertions.assertNotNull(defaults);
        Assertions.assertTrue(defaults.containsKey(lastSrcSlotIds.get(indexOfColumn("v3"))),
                "a DEFAULT held as an expression object must still be sent down");
    }

    /**
     * A key on a table that merges rows is never filled. Filling it would give every row missing
     * that key the same key, and the rows would aggregate or replace into one another.
     */
    @Test
    public void testFillDefaultOnAbsentKeySkipsMergingKey() throws StarRocksException {
        Map<Integer, TExpr> defaults = runAndGetAbsentKeyDefaults(true, KeysType.PRIMARY_KEYS);
        Assertions.assertNotNull(defaults);
        Assertions.assertFalse(defaults.containsKey(lastSrcSlotIds.get(indexOfColumn("k1"))),
                "a primary key column must never be filled from its DEFAULT");
        Assertions.assertEquals(
                Set.of(lastSrcSlotIds.get(indexOfColumn("v1")), lastSrcSlotIds.get(indexOfColumn("v3"))),
                defaults.keySet());
    }

    /**
     * A duplicate key table's key is only a sort key, with no row merging behind it, so it is
     * filled like any other column.
     */
    @Test
    public void testFillDefaultOnAbsentKeyFillsDuplicateKey() throws StarRocksException {
        Map<Integer, TExpr> defaults = runAndGetAbsentKeyDefaults(true, KeysType.DUP_KEYS);
        Assertions.assertNotNull(defaults);
        Assertions.assertEquals(
                Set.of(lastSrcSlotIds.get(indexOfColumn("k1")), lastSrcSlotIds.get(indexOfColumn("v1")),
                        lastSrcSlotIds.get(indexOfColumn("v3"))),
                defaults.keySet());
    }

    /** Without the property, nothing is sent and every load behaves exactly as it does today. */
    @Test
    public void testFillDefaultOnAbsentKeyOffSendsNothing() throws StarRocksException {
        Map<Integer, TExpr> defaults = runAndGetAbsentKeyDefaults(false, KeysType.DUP_KEYS);
        Assertions.assertTrue(defaults == null || defaults.isEmpty());
    }

    @Test
    public void testNormal() throws StarRocksException {
        
        DescriptorTable descTbl = new DescriptorTable();

        List<Column> columns = getBaseSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }

        TStreamLoadPutRequest request = getBaseRequest();
        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);
        new Expectations() {{
            dstTable.getBaseSchema();
            result = columns;
            dstTable.getFullSchema();
            result = columns;
            dstTable.getColumn("k1");
            result = columns.get(0);
            dstTable.getColumn("k2");
            result = columns.get(1);
            dstTable.getColumn("v1");
            result = columns.get(2);
            dstTable.getColumn("v2");
            result = columns.get(3);
        }};
        scanNode.init(descTbl);
        scanNode.finalizeStats();
        scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
        Assertions.assertEquals(1, scanNode.getScanRangeLocations(0).size());
    }

    @Test
    public void testLostV2() {
        assertThrows(AnalysisException.class, () -> {
            
            DescriptorTable descTbl = new DescriptorTable();

            List<Column> columns = getBaseSchema();
            TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
            for (Column column : columns) {
                SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
                slot.setColumn(column);
                slot.setIsMaterialized(true);
                if (column.isAllowNull()) {
                    slot.setIsNullable(true);
                } else {
                    slot.setIsNullable(false);
                }
            }

            TStreamLoadPutRequest request = getBaseRequest();
            request.setColumns("k1, k2, v1");
            StreamLoadInfo streamLoadInfo = StreamLoadInfo.fromTStreamLoadPutRequest(request, null);
            StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

            scanNode.init(descTbl);
            scanNode.finalizeStats();
            scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
            TPlanNode planNode = new TPlanNode();
            scanNode.toThrift(planNode);
        });
    }

    @Test
    public void testBadColumns(@Mocked GlobalStateMgr globalStateMgr) {
        assertThrows(ParsingException.class, () -> {
            
            DescriptorTable descTbl = new DescriptorTable();

            List<Column> columns = getBaseSchema();
            TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
            for (Column column : columns) {
                SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
                slot.setColumn(column);
                slot.setIsMaterialized(true);
                if (column.isAllowNull()) {
                    slot.setIsNullable(true);
                } else {
                    slot.setIsNullable(false);
                }
            }

            TStreamLoadPutRequest request = getBaseRequest();
            request.setColumns("k1 k2 v1");
            StreamLoadInfo streamLoadInfo = StreamLoadInfo.fromTStreamLoadPutRequest(request, null);
            StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

            scanNode.init(descTbl);
            scanNode.finalizeStats();
            scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
            TPlanNode planNode = new TPlanNode();
            scanNode.toThrift(planNode);
        });
    }

    @Test
    public void testColumnsNormal() throws StarRocksException, StarRocksException {
        
        DescriptorTable descTbl = new DescriptorTable();

        List<Column> columns = getBaseSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }

        new Expectations() {
            {
                dstTable.getColumn("k1");
                result = columns.stream().filter(c -> c.getName().equals("k1")).findFirst().get();

                dstTable.getColumn("k2");
                result = columns.stream().filter(c -> c.getName().equals("k2")).findFirst().get();

                dstTable.getColumn("v1");
                result = columns.stream().filter(c -> c.getName().equals("v1")).findFirst().get();

                dstTable.getColumn("v2");
                result = columns.stream().filter(c -> c.getName().equals("v2")).findFirst().get();
            }
        };

        TStreamLoadPutRequest request = getBaseRequest();
        request.setColumns("k1,k2,v1, v2=k2");
        StreamLoadInfo streamLoadInfo = StreamLoadInfo.fromTStreamLoadPutRequest(request, null);
        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);
        scanNode.init(descTbl);
        scanNode.finalizeStats();
        scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
    }

    @Test
    public void testSetColumnOfDecimal() {
        
        DescriptorTable descTbl = new DescriptorTable();
        List<Column> columns = getDecimalSchema();

        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }
        TDescriptorTable tableDesc = descTbl.toThrift();
        TSlotDescriptor slotDesc = tableDesc.getSlotDescriptors().get(2);
        TTypeNode typeNode = slotDesc.slotType.getTypes().get(0);
        Assertions.assertTrue(typeNode.isSetScalar_type());
        Assertions.assertEquals(typeNode.scalar_type.type, TPrimitiveType.DECIMAL128);
        Assertions.assertEquals(typeNode.scalar_type.precision, 38);
        Assertions.assertEquals(typeNode.scalar_type.scale, 9);
    }

    @Test
    public void testHllColumnsNormal() throws StarRocksException {
        
        DescriptorTable descTbl = new DescriptorTable();

        List<Column> columns = getHllSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }

        new Expectations() {{
            globalStateMgr.getFunction((Function) any, (Function.CompareMode) any);
            result = new ScalarFunction(new FunctionName(FunctionSet.HLL_HASH), Lists.newArrayList(), IntegerType.BIGINT,
                    false);
        }};

        new Expectations() {
            {
                dstTable.getColumn("k1");
                result = columns.stream().filter(c -> c.getName().equals("k1")).findFirst().get();

                dstTable.getColumn("k2");
                result = null;

                dstTable.getColumn("v1");
                result = columns.stream().filter(c -> c.getName().equals("v1")).findFirst().get();
            }
        };

        TStreamLoadPutRequest request = getBaseRequest();
        request.setFileType(TFileType.FILE_STREAM);
        request.setColumns("k1,k2, v1=" + FunctionSet.HLL_HASH + "(k2)");
        StreamLoadInfo streamLoadInfo = StreamLoadInfo.fromTStreamLoadPutRequest(request, null);
        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

        scanNode.init(descTbl);
        scanNode.finalizeStats();
        scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
    }

    @Test
    public void testHllColumnsNoHllHash() {
        assertThrows(StarRocksException.class, () -> {
            
            DescriptorTable descTbl = new DescriptorTable();

            List<Column> columns = getHllSchema();
            TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
            for (Column column : columns) {
                SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
                slot.setColumn(column);
                slot.setIsMaterialized(true);
                if (column.isAllowNull()) {
                    slot.setIsNullable(true);
                } else {
                    slot.setIsNullable(false);
                }
            }

            new Expectations() {
                {
                    globalStateMgr.getFunction((Function) any, (Function.CompareMode) any);
                    result = new ScalarFunction(new FunctionName("hll_hash1"), Lists.newArrayList(), IntegerType.BIGINT, false);
                    minTimes = 0;
                }
            };

            new Expectations() {
                {
                    dstTable.getColumn("k1");
                    result = columns.stream().filter(c -> c.getName().equals("k1")).findFirst().get();
                    minTimes = 0;

                    dstTable.getColumn("k2");
                    result = null;
                    minTimes = 0;

                    dstTable.getColumn("v1");
                    result = columns.stream().filter(c -> c.getName().equals("v1")).findFirst().get();
                    minTimes = 0;
                }
            };

            TStreamLoadPutRequest request = getBaseRequest();
            request.setFileType(TFileType.FILE_LOCAL);
            request.setColumns("k1,k2, v1=hll_hash1(k2)");
            StreamLoadInfo streamLoadInfo = StreamLoadInfo.fromTStreamLoadPutRequest(request, null);
            StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

            scanNode.init(descTbl);
            scanNode.finalizeStats();
            scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
            TPlanNode planNode = new TPlanNode();
            scanNode.toThrift(planNode);
        });
    }

    @Test
    public void testHllColumnsFail() {
        assertThrows(StarRocksException.class, () -> {
            
            DescriptorTable descTbl = new DescriptorTable();

            List<Column> columns = getHllSchema();
            TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
            for (Column column : columns) {
                SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
                slot.setColumn(column);
                slot.setIsMaterialized(true);
                if (column.isAllowNull()) {
                    slot.setIsNullable(true);
                } else {
                    slot.setIsNullable(false);
                }
            }

            TStreamLoadPutRequest request = getBaseRequest();
            request.setFileType(TFileType.FILE_LOCAL);
            request.setColumns("k1,k2, v1=k2");
            StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

            scanNode.init(descTbl);
            scanNode.finalizeStats();
            scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
            TPlanNode planNode = new TPlanNode();
            scanNode.toThrift(planNode);
        });
    }

    @Test
    public void testUnsupportedFType() {
        assertThrows(StarRocksException.class, () -> {
            
            DescriptorTable descTbl = new DescriptorTable();

            List<Column> columns = getBaseSchema();
            TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
            for (Column column : columns) {
                SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
                slot.setColumn(column);
                slot.setIsMaterialized(true);
                if (column.isAllowNull()) {
                    slot.setIsNullable(true);
                } else {
                    slot.setIsNullable(false);
                }
            }

            TStreamLoadPutRequest request = getBaseRequest();
            request.setFileType(TFileType.FILE_BROKER);
            request.setColumns("k1,k2,v1, v2=k2");
            StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

            scanNode.init(descTbl);
            scanNode.finalizeStats();
            scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
            TPlanNode planNode = new TPlanNode();
            scanNode.toThrift(planNode);
        });
    }

    @Test
    public void testColumnsUnknownRef() {
        assertThrows(StarRocksException.class, () -> {
            
            DescriptorTable descTbl = new DescriptorTable();

            List<Column> columns = getBaseSchema();
            TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
            for (Column column : columns) {
                SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
                slot.setColumn(column);
                slot.setIsMaterialized(true);
                if (column.isAllowNull()) {
                    slot.setIsNullable(true);
                } else {
                    slot.setIsNullable(false);
                }
            }

            new Expectations() {
                {
                    dstTable.getColumn("k1");
                    result = columns.stream().filter(c -> c.getName().equals("k1")).findFirst().get();
                    minTimes = 0;

                    dstTable.getColumn("k2");
                    result = columns.stream().filter(c -> c.getName().equals("k2")).findFirst().get();
                    minTimes = 0;

                    dstTable.getColumn("v1");
                    result = columns.stream().filter(c -> c.getName().equals("v1")).findFirst().get();
                    minTimes = 0;

                    dstTable.getColumn("v2");
                    result = columns.stream().filter(c -> c.getName().equals("v2")).findFirst().get();
                    minTimes = 0;
                }
            };

            TStreamLoadPutRequest request = getBaseRequest();
            request.setColumns("k1,k2,v1, v2=k3");
            StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

            scanNode.init(descTbl);
            scanNode.finalizeStats();
            scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
            TPlanNode planNode = new TPlanNode();
            scanNode.toThrift(planNode);
        });
    }

    @Test
    public void testWhereNormal() throws StarRocksException, StarRocksException {
        
        DescriptorTable descTbl = new DescriptorTable();

        List<Column> columns = getBaseSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }

        new Expectations() {
            {
                dstTable.getColumn("k1");
                result = columns.stream().filter(c -> c.getName().equals("k1")).findFirst().get();
                minTimes = 0;

                dstTable.getColumn("k2");
                result = columns.stream().filter(c -> c.getName().equals("k2")).findFirst().get();
                minTimes = 0;

                dstTable.getColumn("v1");
                result = columns.stream().filter(c -> c.getName().equals("v1")).findFirst().get();
                minTimes = 0;

                dstTable.getColumn("v2");
                result = columns.stream().filter(c -> c.getName().equals("v2")).findFirst().get();
                minTimes = 0;
            }
        };

        TStreamLoadPutRequest request = getBaseRequest();
        request.setColumns("k1,k2,v1, v2=k1");
        request.setWhere("k1 = 1");
        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

        scanNode.init(descTbl);
        scanNode.finalizeStats();
        scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
    }

    @Test
    public void testWhereBad() {
        assertThrows(ParsingException.class, () -> {
            
            DescriptorTable descTbl = new DescriptorTable();

            List<Column> columns = getBaseSchema();
            TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
            for (Column column : columns) {
                SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
                slot.setColumn(column);
                slot.setIsMaterialized(true);
                if (column.isAllowNull()) {
                    slot.setIsNullable(true);
                } else {
                    slot.setIsNullable(false);
                }
            }

            new Expectations() {
                {
                    dstTable.getColumn("k1");
                    result = columns.stream().filter(c -> c.getName().equals("k1")).findFirst().get();
                    minTimes = 0;

                    dstTable.getColumn("k2");
                    result = columns.stream().filter(c -> c.getName().equals("k2")).findFirst().get();
                    minTimes = 0;

                    dstTable.getColumn("v1");
                    result = columns.stream().filter(c -> c.getName().equals("v1")).findFirst().get();
                    minTimes = 0;

                    dstTable.getColumn("v2");
                    result = columns.stream().filter(c -> c.getName().equals("v2")).findFirst().get();
                    minTimes = 0;
                }
            };

            TStreamLoadPutRequest request = getBaseRequest();
            request.setColumns("k1,k2,v1, v2=k2");
            request.setWhere("k1   1");
            StreamLoadInfo streamLoadInfo = StreamLoadInfo.fromTStreamLoadPutRequest(request, null);
            StreamLoadScanNode scanNode =
                    new StreamLoadScanNode(streamLoadInfo.getId(), new PlanNodeId(1), dstDesc, dstTable,
                            streamLoadInfo);

            scanNode.init(descTbl);
            scanNode.finalizeStats();
            scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
            TPlanNode planNode = new TPlanNode();
            scanNode.toThrift(planNode);
        });
    }

    @Test
    public void testWhereUnknownRef() {
        assertThrows(StarRocksException.class, () -> {
            
            DescriptorTable descTbl = new DescriptorTable();

            List<Column> columns = getBaseSchema();
            TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
            for (Column column : columns) {
                SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
                slot.setColumn(column);
                slot.setIsMaterialized(true);
                if (column.isAllowNull()) {
                    slot.setIsNullable(true);
                } else {
                    slot.setIsNullable(false);
                }
            }

            new Expectations() {
                {
                    dstTable.getColumn("k1");
                    result = columns.stream().filter(c -> c.getName().equals("k1")).findFirst().get();
                    minTimes = 0;

                    dstTable.getColumn("k2");
                    result = columns.stream().filter(c -> c.getName().equals("k2")).findFirst().get();
                    minTimes = 0;

                    dstTable.getColumn("v1");
                    result = columns.stream().filter(c -> c.getName().equals("v1")).findFirst().get();
                    minTimes = 0;

                    dstTable.getColumn("v2");
                    result = columns.stream().filter(c -> c.getName().equals("v2")).findFirst().get();
                    minTimes = 0;
                }
            };

            TStreamLoadPutRequest request = getBaseRequest();
            request.setColumns("k1,k2,v1, v2=k1");
            request.setWhere("k5 = 1");
            StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

            scanNode.init(descTbl);
            scanNode.finalizeStats();
            scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
            TPlanNode planNode = new TPlanNode();
            scanNode.toThrift(planNode);
        });
    }

    @Test
    public void testWhereNotBool() {
        assertThrows(StarRocksException.class, () -> {
            
            DescriptorTable descTbl = new DescriptorTable();

            List<Column> columns = getBaseSchema();
            TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
            for (Column column : columns) {
                SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
                slot.setColumn(column);
                slot.setIsMaterialized(true);
                if (column.isAllowNull()) {
                    slot.setIsNullable(true);
                } else {
                    slot.setIsNullable(false);
                }
            }

            TStreamLoadPutRequest request = getBaseRequest();
            request.setColumns("k1,k2,v1,v2");
            request.setWhere("k1 + v1");
            StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

            new Expectations() {
                {
                    dstTable.getBaseSchema();
                    result = columns;
                    dstTable.getFullSchema();
                    result = columns;
                    dstTable.getColumn("k1");
                    result = columns.get(0);
                    dstTable.getColumn("k2");
                    result = columns.get(1);
                    dstTable.getColumn("v1");
                    result = columns.get(2);
                    dstTable.getColumn("v2");
                    result = columns.get(3);
                }
            };

            new Expectations() {
                {
                    globalStateMgr.getFunction((Function) any, (Function.CompareMode) any);
                    result = new ScalarFunction(new FunctionName(FunctionSet.ADD), Lists.newArrayList(), IntegerType.BIGINT,
                            false);
                }
            };

            scanNode.init(descTbl);
            scanNode.finalizeStats();
            scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
            TPlanNode planNode = new TPlanNode();
            scanNode.toThrift(planNode);
        });
    }

    @Test
    public void testEnvelopeDebeziumRequiresPrimaryKeyTable() throws StarRocksException {
        DescriptorTable descTbl = new DescriptorTable();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");

        TStreamLoadPutRequest request = getBaseRequest();
        request.setFormatType(TFileFormatType.FORMAT_JSON);
        request.setEnvelope(TEnvelopeType.DEBEZIUM);

        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);
        new Expectations() {{
            dstTable.getKeysType();
            result = KeysType.DUP_KEYS;
        }};
        assertThrows(StarRocksException.class, () -> scanNode.init(descTbl),
                "envelope=debezium is only supported on PRIMARY KEY tables");
    }

    @Test
    public void testLoadInitColumnsMappingColumnNotExist() {
        assertThrows(DdlException.class, () -> {
            List<Column> columns = Lists.newArrayList();
            columns.add(new Column("c1", IntegerType.INT, true, null, false, null, ""));
            columns.add(new Column("c2", TypeFactory.createVarcharType(10), true, null, false, null, ""));
            Table table = new Table(1L, "table0", TableType.OLAP, columns);
            List<ImportColumnDesc> columnExprs = Lists.newArrayList();
            columnExprs.add(new ImportColumnDesc("c3", new FunctionCallExpr("func", Lists.newArrayList())));
            Load.initColumns(table, columnExprs, null, null, null, null, null, null, true, false, Lists.newArrayList(),
                    false, false, null);
        });
    }
}
