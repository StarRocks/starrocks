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
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.exception.UserException;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.thrift.TBrokerScanRangeParams;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class LoadTest {
    @Mocked
    private Analyzer analyzer;
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

    @Before
    public void setUp() {
        columns = Lists.newArrayList();
        table = new OlapTable(1, "test", columns, KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(3));

        descTable = new DescriptorTable();
        srcTupleDesc = descTable.createTupleDescriptor();
        srcTupleDesc.setTable(table);

        new Expectations() {
            {
                analyzer.getDescTbl();
                result = descTable;
            }
        };

        columnExprs = Lists.newArrayList();
        columnsFromPath = Lists.newArrayList();

        exprsByName = Maps.newHashMap();
        slotDescByName = Maps.newHashMap();
        params = new TBrokerScanRangeParams();
    }

    @Test
    public void testInitColumnsPathColumns() throws UserException {
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

        Load.initColumns(table, columnExprs, null, exprsByName, analyzer, srcTupleDesc,
                slotDescByName, params, true, true, columnsFromPath);

        // check
        System.out.println(slotDescByName);
        Assert.assertEquals(2, slotDescByName.size());
        SlotDescriptor c1SlotDesc = slotDescByName.get(c1Name);
        Assert.assertTrue(c1SlotDesc.getColumn().getType().equals(Type.VARCHAR));
    }

    @Test
    public void testInitColumnsColumnInSchemaAndExprArgs() throws UserException {
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

        Load.initColumns(table, columnExprs, null, exprsByName, analyzer, srcTupleDesc,
                slotDescByName, params, true, true, columnsFromPath);

        // check
        System.out.println(slotDescByName);
        Assert.assertEquals(4, slotDescByName.size());
        SlotDescriptor c1SlotDesc = slotDescByName.get(c1Name);
        Assert.assertTrue(c1SlotDesc.getColumn().getType().equals(Type.VARCHAR));
        SlotDescriptor c2SlotDesc = slotDescByName.get(c2Name);
        Assert.assertTrue(c2SlotDesc.getColumn().getType().equals(Type.VARCHAR));
        SlotDescriptor c3SlotDesc = slotDescByName.get(c3Name);
        Assert.assertTrue(c3SlotDesc.getColumn().getType().equals(Type.VARCHAR));
    }

    /**
     * Column name in schema is C1, in source file is c1
     * set (C1 = year(c1))
     */
    @Test
    public void testSourceColumnCaseSensitive() throws UserException {
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

        Load.initColumns(table, columnExprs, null, exprsByName, analyzer, srcTupleDesc,
                slotDescByName, params, true, true, columnsFromPath);

        // check
        System.out.println(slotDescByName);
        Assert.assertEquals(2, slotDescByName.size());
        Assert.assertFalse(slotDescByName.containsKey(c1Name));
        Assert.assertTrue(slotDescByName.containsKey(c1NameInSource));
    }
}