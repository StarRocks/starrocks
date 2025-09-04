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


package com.starrocks.load;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.StarRocksException;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.ImportColumnsStmt;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.thrift.TBrokerScanRangeParams;
import com.starrocks.thrift.TFileFormatType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class LoadTest {

    @Mocked
    private OlapTable table;

    private DescriptorTable descTable;
    private TupleDescriptor srcTupleDesc;

    private List<Column> columns;
    private List<ImportColumnDesc> columnExprs;
    private List<String> columnsFromPath;

    private Map<String, Expr> exprsByName;
    private Map<String, SlotDescriptor> slotDescByName;
    private TBrokerScanRangeParams params;

    @BeforeEach
    public void setUp() {
        columns = Lists.newArrayList();
        table = new OlapTable(1, "test", columns, KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(3));

        descTable = new DescriptorTable();
        srcTupleDesc = descTable.createTupleDescriptor();
        srcTupleDesc.setTable(table);

        columnExprs = Lists.newArrayList();
        columnsFromPath = Lists.newArrayList();

        exprsByName = Maps.newHashMap();
        slotDescByName = Maps.newHashMap();
        params = new TBrokerScanRangeParams();
    }

    @Test
    public void testInitColumnsPathColumns() throws StarRocksException {
        // columns
        String c0Name = "c0";
        columns.add(new Column(c0Name, Type.INT, true, null, true, null, ""));
        columnExprs.add(new ImportColumnDesc(c0Name, null));

        String c1Name = "c1";
        columns.add(new Column(c1Name, Type.INT, true, null, true, null, ""));
        columnExprs.add(new ImportColumnDesc(c1Name, null));

        // column mappings
        // c1 = year(c1)
        List<Expr> params1 = Lists.newArrayList();
        params1.add(new SlotRef(null, c1Name));
        Expr mapping1 = new FunctionCallExpr(FunctionSet.YEAR, params1);
        CompoundPredicate compoundPredicate = new CompoundPredicate(CompoundPredicate.Operator.OR,
                mapping1, new SlotRef(null, c0Name));
        columnExprs.add(new ImportColumnDesc(c1Name, compoundPredicate));

        // c1 is from path
        // path/c1=2021-03-01/file
        columnsFromPath.add(c1Name);

        new Expectations() {
            {
                table.getBaseSchema();
                result = columns;
                table.getColumn(c0Name);
                result = columns.get(0);
                table.getColumn(c1Name);
                result = columns.get(1);
            }
        };

        Load.initColumns(table, columnExprs, null, exprsByName, new DescriptorTable(), srcTupleDesc,
                slotDescByName, params, true, true, columnsFromPath);

        // check
        System.out.println(slotDescByName);
        Assertions.assertEquals(2, slotDescByName.size());
        SlotDescriptor c1SlotDesc = slotDescByName.get(c1Name);
        Assertions.assertTrue(c1SlotDesc.getColumn().getType().equals(Type.VARCHAR));
    }

    @Test
    public void testInitColumnsColumnInSchemaAndExprArgs() throws StarRocksException {
        table = new OlapTable(1, "test", columns, KeysType.AGG_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(3));

        // columns
        String c0Name = "c0";
        columns.add(new Column(c0Name, Type.INT, true, null, true, null, ""));
        columnExprs.add(new ImportColumnDesc(c0Name, null));

        String c1Name = "c1";
        columns.add(new Column(c1Name, Type.INT, true, null, true, null, ""));
        columnExprs.add(new ImportColumnDesc(c1Name, null));

        String c2Name = "c2";
        columns.add(new Column(c2Name, Type.BITMAP, false, AggregateType.BITMAP_UNION, true, null, ""));
        columnExprs.add(new ImportColumnDesc(c2Name, null));

        String c3Name = "c3";
        columns.add(new Column(c3Name, Type.VARCHAR, false, AggregateType.REPLACE, true, null, ""));
        columnExprs.add(new ImportColumnDesc(c3Name, null));
        String c31Name = "c31";
        columns.add(new Column(c31Name, Type.INT, true, AggregateType.SUM, true, null, ""));

        // column mappings
        // c1 = year(c1)
        List<Expr> params1 = Lists.newArrayList();
        params1.add(new SlotRef(null, c1Name));
        Expr mapping1 = new FunctionCallExpr(FunctionSet.YEAR, params1);
        columnExprs.add(new ImportColumnDesc(c1Name, mapping1));

        // c2 = to_bitmap(c2)
        List<Expr> params2 = Lists.newArrayList();
        params2.add(new SlotRef(null, c2Name));
        Expr mapping2 = new FunctionCallExpr(FunctionSet.TO_BITMAP, params2);
        columnExprs.add(new ImportColumnDesc(c2Name, mapping2));

        // c31 = c3 + 1
        Expr mapping3 = new ArithmeticExpr(ArithmeticExpr.Operator.ADD, new SlotRef(null, c3Name),
                new IntLiteral(1, Type.INT));
        columnExprs.add(new ImportColumnDesc(c31Name, mapping3));

        new Expectations() {
            {
                table.getBaseSchema();
                result = columns;
                table.getColumn(c0Name);
                result = columns.get(0);
                table.getColumn(c1Name);
                result = columns.get(1);
                table.getColumn(c2Name);
                result = columns.get(2);
                table.getColumn(c3Name);
                result = columns.get(3);
                table.getColumn(c31Name);
                result = columns.get(4);
            }
        };

        Load.initColumns(table, columnExprs, null, exprsByName, new DescriptorTable(), srcTupleDesc,
                slotDescByName, params, true, true, columnsFromPath);

        // check
        System.out.println(slotDescByName);
        Assertions.assertEquals(4, slotDescByName.size());
        SlotDescriptor c1SlotDesc = slotDescByName.get(c1Name);
        Assertions.assertTrue(c1SlotDesc.getColumn().getType().equals(Type.VARCHAR));
        SlotDescriptor c2SlotDesc = slotDescByName.get(c2Name);
        Assertions.assertTrue(c2SlotDesc.getColumn().getType().equals(Type.VARCHAR));
        SlotDescriptor c3SlotDesc = slotDescByName.get(c3Name);
        Assertions.assertTrue(c3SlotDesc.getColumn().getType().equals(Type.VARCHAR));
    }

    /**
     * Column name in schema is C1, in source file is c1
     * set (C1 = year(c1))
     */
    @Test
    public void testSourceColumnCaseSensitive() throws StarRocksException {
        // columns
        String c0Name = "c0";
        columns.add(new Column(c0Name, Type.INT, true, null, true, null, ""));
        columnExprs.add(new ImportColumnDesc(c0Name, null));

        String c1Name = "C1";
        columns.add(new Column(c1Name, Type.INT, true, null, true, null, ""));
        // column name in source file is c1
        String c1NameInSource = "c1";
        columnExprs.add(new ImportColumnDesc(c1NameInSource, null));

        // column mappings
        // C1 = year(c1)
        List<Expr> params1 = Lists.newArrayList();
        params1.add(new SlotRef(null, c1NameInSource));
        Expr mapping1 = new FunctionCallExpr(FunctionSet.YEAR, params1);
        columnExprs.add(new ImportColumnDesc(c1Name, mapping1));

        new Expectations() {
            {
                table.getBaseSchema();
                result = columns;
                table.getColumn(c0Name);
                result = columns.get(0);
                table.getColumn(c1Name);
                result = columns.get(1);
            }
        };

        Load.initColumns(table, columnExprs, null, exprsByName, new DescriptorTable(), srcTupleDesc,
                slotDescByName, params, true, true, columnsFromPath);

        // check
        System.out.println(slotDescByName);
        Assertions.assertEquals(2, slotDescByName.size());
        Assertions.assertFalse(slotDescByName.containsKey(c1Name));
        Assertions.assertTrue(slotDescByName.containsKey(c1NameInSource));
    }

    /**
     * set (c1 = year())
     */
    @Test
    public void testMappingExprInvalid() {
        // columns
        String c0Name = "c0";
        columns.add(new Column(c0Name, Type.INT, true, null, true, null, ""));
        columnExprs.add(new ImportColumnDesc(c0Name, null));

        String c1Name = "c1";
        columns.add(new Column(c1Name, Type.INT, true, null, true, null, ""));

        // column mappings
        // c1 = year()
        List<Expr> params1 = Lists.newArrayList();
        Expr mapping1 = new FunctionCallExpr(FunctionSet.YEAR, params1);
        columnExprs.add(new ImportColumnDesc(c1Name, mapping1));

        new Expectations() {
            {
                table.getBaseSchema();
                result = columns;
                table.getColumn(c0Name);
                result = columns.get(0);
                table.getColumn(c1Name);
                result = columns.get(1);
            }
        };

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Expr 'year()' analyze error: No matching function with signature: year(), derived column is 'c1'",
                () -> Load.initColumns(table, columnExprs, null, exprsByName, new DescriptorTable(), srcTupleDesc,
                        slotDescByName, params, true, true, columnsFromPath));
    }

    @Test
    public void testGetFormatType() {
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, Load.getFormatType("parquet", "hdfs://127.0.0.1:9000/some_file"));
        Assertions.assertEquals(TFileFormatType.FORMAT_ORC, Load.getFormatType("orc", "hdfs://127.0.0.1:9000/some_file"));
        Assertions.assertEquals(TFileFormatType.FORMAT_JSON, Load.getFormatType("json", "hdfs://127.0.0.1:9000/some_file"));

        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, Load.getFormatType("", "hdfs://127.0.0.1:9000/some_file.parq"));
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, Load.getFormatType("", "hdfs://127.0.0.1:9000/some_file.parquet"));
        Assertions.assertEquals(TFileFormatType.FORMAT_ORC, Load.getFormatType("", "hdfs://127.0.0.1:9000/some_file.orc"));
        Assertions.assertEquals(TFileFormatType.FORMAT_CSV_GZ, Load.getFormatType("csv", "hdfs://127.0.0.1:9000/some_file.gz"));
        Assertions.assertEquals(TFileFormatType.FORMAT_CSV_BZ2, Load.getFormatType("csv", "hdfs://127.0.0.1:9000/some_file.bz2"));
        Assertions.assertEquals(TFileFormatType.FORMAT_CSV_LZ4_FRAME, Load.getFormatType("csv", "hdfs://127.0.0.1:9000/some_file.lz4"));
        Assertions.assertEquals(TFileFormatType.FORMAT_CSV_DEFLATE, Load.getFormatType("csv", "hdfs://127.0.0.1:9000/some_file.deflate"));
        Assertions.assertEquals(TFileFormatType.FORMAT_CSV_ZSTD, Load.getFormatType("csv", "hdfs://127.0.0.1:9000/some_file.zst"));
        Assertions.assertEquals(TFileFormatType.FORMAT_CSV_PLAIN, Load.getFormatType("csv", "hdfs://127.0.0.1:9000/some_file"));
    }

    @Test
    public void testLambda() throws Exception {
        String c0Name = "c0";
        columns.add(new Column(c0Name, Type.INT, true, null, true, null, ""));
        String c1Name = "c1";
        columns.add(new Column(c1Name, Type.STRING, false, null, true, null, ""));
        String c2Name = "c2";
        columns.add(new Column(c2Name, Type.STRING, false, null, true, null, ""));
        String c3Name = "c3";
        columns.add(new Column(c3Name, Type.STRING, false, null, true, null, ""));

        new Expectations() {
            {
                table.getBaseSchema();
                result = columns;
                table.getColumn(c0Name);
                result = columns.get(0);
                table.getColumn(c1Name);
                result = columns.get(1);
                table.getColumn(c2Name);
                result = columns.get(2);
                table.getColumn(c3Name);
                result = columns.get(3);
            }
        };

        String columnsSQL = "COLUMNS (" +
                "   c0,t0,c1,t1," +
                "   c2=get_json_string(" +
                "           array_filter(" +
                "               e -> get_json_string(e,'$.name')='Tom'," +
                "               CAST(parse_json(t0) AS ARRAY<JSON>)" +
                "           )[1]," +
                "      '$.id')," +
                "   c3=get_json_string(" +
                "           map_values(" +
                "               map_filter(" +
                "                   (k,v) -> get_json_string(v,'$.name')='Jerry'," +
                "                   CAST(parse_json(t1) AS MAP<STRING,JSON>)" +
                "              )" +
                "           )[1]," +
                "       '$.id')" +
                " )";
        columnsFromPath.add("c1");
        ImportColumnsStmt columnsStmt = com.starrocks.sql.parser.SqlParser.parseImportColumns(columnsSQL,
                SqlModeHelper.MODE_DEFAULT);
        columnExprs.addAll(columnsStmt.getColumns());
        Load.initColumns(table, columnExprs, null, exprsByName, new DescriptorTable(), srcTupleDesc,
                slotDescByName, params, true, true, columnsFromPath);
        Assertions.assertEquals(7, slotDescByName.size());
        Assertions.assertTrue(slotDescByName.containsKey("c0"));
        Assertions.assertTrue(slotDescByName.containsKey("t0"));
        Assertions.assertTrue(slotDescByName.containsKey("c1"));
        Assertions.assertTrue(slotDescByName.containsKey("t1"));
        Assertions.assertTrue(slotDescByName.containsKey("e"));
        Assertions.assertTrue(slotDescByName.containsKey("k"));
        Assertions.assertTrue(slotDescByName.containsKey("v"));
        Assertions.assertEquals(4, params.getSrc_slot_idsSize());
        Assertions.assertEquals(2, exprsByName.size());
        Assertions.assertTrue(exprsByName.containsKey("c2"));
        Assertions.assertTrue(exprsByName.containsKey("c3"));

        int t0SlotId = slotDescByName.get("t0").getId().asInt();
        int eSlotId = slotDescByName.get("e").getId().asInt();
        String c2ExprExplain = String.format(
                "get_json_string[(array_filter(CAST(parse_json(<slot %d>) AS ARRAY<JSON>), " +
                "array_map(<slot %d> -> get_json_string(<slot %d>, '$.name') = 'Tom', " +
                "CAST(parse_json(<slot %d>) AS ARRAY<JSON>)))[1], '$.id'); args: JSON,VARCHAR; " +
                "result: VARCHAR; args nullable: true; result nullable: true]",
                    t0SlotId, eSlotId, eSlotId, t0SlotId);
        Assertions.assertEquals(c2ExprExplain, exprsByName.get("c2").explain());

        int t1SlotId = slotDescByName.get("t1").getId().asInt();
        int kSlotId = slotDescByName.get("k").getId().asInt();
        int vSlotId = slotDescByName.get("v").getId().asInt();
        String c3ExprExplain = String.format(
                "get_json_string[(map_values(map_filter(CAST(parse_json(<slot %d>) AS MAP<VARCHAR(65533),JSON>), " +
                "map_values(map_apply((<slot %d>, <slot %d>) -> map{<slot %d>:get_json_string(<slot %d>, '$.name') = 'Jerry'}, " +
                "CAST(parse_json(<slot %d>) AS MAP<VARCHAR(65533),JSON>)))))[1], '$.id'); args: JSON,VARCHAR; " +
                "result: VARCHAR; args nullable: true; result nullable: true]",
                t1SlotId, kSlotId, vSlotId, kSlotId, vSlotId, t1SlotId);
        Assertions.assertEquals(c3ExprExplain, exprsByName.get("c3").explain());
    }
    
    @Test
    public void testNowPrecision() throws Exception {
        String c0Name = "c0";
        columns.add(new Column(c0Name, Type.INT, true, null, true, null, ""));
        String c1Name = "c1";
        columns.add(new Column(c1Name, Type.DATETIME, false, null, true, null, ""));
        String c2Name = "c2";
        columns.add(new Column(c2Name, Type.DATETIME, false, null, true, null, ""));
        new Expectations() {
            {
                table.getBaseSchema();
                result = columns;
                table.getColumn(c0Name);
                result = columns.get(0);
                table.getColumn(c1Name);
                result = columns.get(1);
                table.getColumn(c2Name);
                result = columns.get(2);
            }
        };

        String columnsSQL = "COLUMNS(c0,c1=now(),c2=now(6))";
        ImportColumnsStmt columnsStmt =
                com.starrocks.sql.parser.SqlParser.parseImportColumns(columnsSQL, SqlModeHelper.MODE_DEFAULT);
        columnExprs.addAll(columnsStmt.getColumns());
        Load.initColumns(table, columnExprs, null, exprsByName, new DescriptorTable(), srcTupleDesc,
                slotDescByName, params, true, true, columnsFromPath);
        Expr c1Expr = exprsByName.get("c1");
        Assertions.assertNotNull(c1Expr);
        Assertions.assertEquals(
                "now[(); args: ; result: DATETIME; args nullable: false; result nullable: false]", c1Expr.explain());

        Expr c2Expr = exprsByName.get("c2");
        Assertions.assertNotNull(c2Expr);
        Assertions.assertEquals(
                "now[(6); args: INT; result: DATETIME; args nullable: false; result nullable: false]", c2Expr.explain());
    }
}