// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/CreateTableStmtTest.java

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

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.analysis.ColumnDef.DefaultValueDef;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.MockedAuth;
import com.starrocks.qe.ConnectContext;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CreateTableStmtTest {
    private static final Logger LOG = LoggerFactory.getLogger(CreateTableStmtTest.class);

    // used to get default db
    private TableName tblName;
    private TableName tblNameNoDb;
    private List<ColumnDef> cols;
    private List<ColumnDef> invalidCols;
    private List<String> colsName;
    private List<String> invalidColsName;
    private HashDistributionDesc hashDistributioin;
    private Analyzer analyzer;

    @Mocked
    private Auth auth;
    @Mocked
    private ConnectContext ctx;
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    // set default db is 'db1'
    // table name is table1
    // Column: [col1 int; col2 string]
    @Before
    public void setUp() {
        // analyzer
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        // table name
        tblName = new TableName("db1", "table1");
        tblNameNoDb = new TableName("", "table1");
        // col
        cols = Lists.newArrayList();
        cols.add(new ColumnDef("col1", new TypeDef(ScalarType.createType(PrimitiveType.INT))));
        cols.add(new ColumnDef("col2", new TypeDef(ScalarType.createVarcharType(10))));
        colsName = Lists.newArrayList();
        colsName.add("col1");
        colsName.add("col2");
        // invalid col
        invalidCols = Lists.newArrayList();
        invalidCols.add(new ColumnDef("col1", new TypeDef(ScalarType.createType(PrimitiveType.INT))));
        invalidCols.add(new ColumnDef("col2", new TypeDef(ScalarType.createCharType(10))));
        invalidCols.add(new ColumnDef("col2", new TypeDef(ScalarType.createCharType(10))));
        invalidColsName = Lists.newArrayList();
        invalidColsName.add("col1");
        invalidColsName.add("col2");
        invalidColsName.add("col2");
        hashDistributioin = new HashDistributionDesc(10, Lists.newArrayList("col1"));

        MockedAuth.mockedAuth(auth);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
    }

    @Test
    public void testNormal() throws UserException {
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblName, cols, "olap",
                new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                hashDistributioin, null, null, "");
        stmt.analyze(analyzer);
        Assert.assertEquals("testCluster:db1", stmt.getDbName());
        Assert.assertEquals("table1", stmt.getTableName());
        Assert.assertNull(stmt.getProperties());
    }

    @Test
    public void testCreateTableWithRollup() throws UserException {
        List<AlterClause> ops = Lists.newArrayList();
        ops.add(new AddRollupClause("index1", Lists.newArrayList("col1", "col2"), null, "table1", null));
        ops.add(new AddRollupClause("index2", Lists.newArrayList("col2", "col3"), null, "table1", null));
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblName, cols, "olap",
                new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                hashDistributioin, null, null, "", ops);
        stmt.analyze(analyzer);
        Assert.assertEquals("testCluster:db1", stmt.getDbName());
        Assert.assertEquals("table1", stmt.getTableName());
        Assert.assertNull(stmt.getProperties());
        Assert.assertTrue(stmt.toSql()
                .contains("rollup( `index1` (`col1`, `col2`) FROM `table1`, `index2` (`col2`, `col3`) FROM `table1`)"));
    }

    @Test
    public void testDefaultDbNormal() throws UserException {
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, cols, "olap",
                new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                hashDistributioin, null, null, "");
        stmt.analyze(analyzer);
        Assert.assertEquals("testDb", stmt.getDbName());
        Assert.assertEquals("table1", stmt.getTableName());
        Assert.assertNull(stmt.getPartitionDesc());
        Assert.assertNull(stmt.getProperties());
    }

    @Test(expected = AnalysisException.class)
    public void testNoDb(@Mocked Analyzer noDbAnalyzer) throws UserException {
        // make defalut db return empty;
        new Expectations() {
            {
                noDbAnalyzer.getDefaultDb();
                minTimes = 0;
                result = "";

                noDbAnalyzer.getClusterName();
                minTimes = 0;
                result = "cluster";
            }
        };
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, cols, "olap",
                new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                hashDistributioin, null, null, "");
        stmt.analyze(noDbAnalyzer);
    }

    @Test(expected = AnalysisException.class)
    public void testEmptyCol() throws UserException {
        // make defalut db return empty;
        List<ColumnDef> emptyCols = Lists.newArrayList();
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, emptyCols, "olap",
                new KeysDesc(), null,
                hashDistributioin, null, null, "");
        stmt.analyze(analyzer);
    }

    @Test(expected = AnalysisException.class)
    public void testDupCol() throws UserException {
        // make defalut db return empty;
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, invalidCols, "olap",
                new KeysDesc(KeysType.AGG_KEYS, invalidColsName), null,
                hashDistributioin, null, null, "");
        stmt.analyze(analyzer);
    }

    @Test
    public void testBitmapKey() throws Exception {
        ColumnDef bitmap = new ColumnDef("col3", new TypeDef(ScalarType.createType(PrimitiveType.BITMAP)));
        cols.add(bitmap);
        colsName.add("col3");

        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, cols, "olap",
                new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                hashDistributioin, null, null, "");
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Invalid data type of key column 'col3': 'BITMAP'");
        stmt.analyze(analyzer);

        cols.remove(bitmap);
    }

    @Test
    public void testHLLKey() throws Exception {
        ColumnDef hll = new ColumnDef("col3", new TypeDef(ScalarType.createType(PrimitiveType.HLL)));
        cols.add(hll);
        colsName.add("col3");

        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, cols, "olap",
                new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                hashDistributioin, null, null, "");
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Invalid data type of key column 'col3': 'HLL'");
        stmt.analyze(analyzer);
    }

    @Test
    public void testPercentileKey() throws Exception {
        ColumnDef bitmap = new ColumnDef("col3", new TypeDef(ScalarType.createType(PrimitiveType.PERCENTILE)));
        cols.add(bitmap);
        colsName.add("col3");

        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, cols, "olap",
                new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                hashDistributioin, null, null, "");
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Invalid data type of key column 'col3': 'PERCENTILE'");
        stmt.analyze(analyzer);
    }

    @Test
    public void testArrayKey() throws Exception {
        ColumnDef col3 = new ColumnDef("col3", new TypeDef(new ArrayType(ScalarType.createType(PrimitiveType.INT))));
        cols.add(col3);
        colsName.add("col3");

        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, cols, "olap",
                new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                hashDistributioin, null, null, "");
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Invalid data type of key column 'col3': 'ARRAY<INT>'");
        stmt.analyze(analyzer);
    }

    @Test
    public void testBitmapWithoutAggregateMethod() throws Exception {
        ColumnDef col3 = new ColumnDef("col3", new TypeDef(ScalarType.createType(PrimitiveType.BITMAP)));
        cols.add(col3);
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, cols, "olap",
                new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                hashDistributioin, null, null, "");
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("AGG_KEYS table should specify aggregate type for non-key column[col3]");
        stmt.analyze(analyzer);
    }

    @Test
    public void testBitmapWithPrimaryKey() throws Exception {
        ColumnDef col3 = new ColumnDef("col3", new TypeDef(ScalarType.createType(PrimitiveType.BITMAP)));
        cols.add(col3);
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, cols, "olap",
                new KeysDesc(KeysType.PRIMARY_KEYS, colsName), null,
                hashDistributioin, null, null, "");
        stmt.analyze(analyzer);
    }

    @Test
    public void testBitmapWithUniqueKey() throws Exception {
        ColumnDef col3 = new ColumnDef("col3", new TypeDef(ScalarType.createType(PrimitiveType.BITMAP)));
        cols.add(col3);
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, cols, "olap",
                new KeysDesc(KeysType.UNIQUE_KEYS, colsName), null,
                hashDistributioin, null, null, "");
        stmt.analyze(analyzer);
    }

    @Test
    public void testBitmapWithDuplicateKey() throws Exception {
        ColumnDef col3 = new ColumnDef("col3", new TypeDef(ScalarType.createType(PrimitiveType.BITMAP)));
        cols.add(col3);
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, cols, "olap",
                new KeysDesc(KeysType.DUP_KEYS, colsName), null,
                hashDistributioin, null, null, "");
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("No aggregate function specified for 'col3'");
        stmt.analyze(analyzer);
    }

    @Test
    public void testHLLWithoutAggregateMethod() throws Exception {
        ColumnDef col3 = new ColumnDef("col3", new TypeDef(ScalarType.createType(PrimitiveType.HLL)));
        cols.add(col3);
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, cols, "olap",
                new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                hashDistributioin, null, null, "");
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("AGG_KEYS table should specify aggregate type for non-key column[col3]");
        stmt.analyze(analyzer);
    }

    @Test
    public void testHLLWithPrimaryKey() throws Exception {
        ColumnDef col3 = new ColumnDef("col3", new TypeDef(ScalarType.createType(PrimitiveType.HLL)));
        cols.add(col3);
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, cols, "olap",
                new KeysDesc(KeysType.PRIMARY_KEYS, colsName), null,
                hashDistributioin, null, null, "");
        stmt.analyze(analyzer);
    }

    @Test
    public void testPercentileWithoutAggregateMethod() throws Exception {
        ColumnDef percentile = new ColumnDef("col3", new TypeDef(ScalarType.createType(PrimitiveType.PERCENTILE)));
        cols.add(percentile);
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, cols, "olap",
                new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                hashDistributioin, null, null, "");
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("AGG_KEYS table should specify aggregate type for non-key column[col3]");
        stmt.analyze(analyzer);
    }

    @Test
    public void testPercentileWithPrimaryKey() throws Exception {
        ColumnDef percentile = new ColumnDef("col3", new TypeDef(ScalarType.createType(PrimitiveType.PERCENTILE)));
        cols.add(percentile);
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, cols, "olap",
                new KeysDesc(KeysType.PRIMARY_KEYS, colsName), null,
                hashDistributioin, null, null, "");
        stmt.analyze(analyzer);
    }

    @Test
    public void testInvalidAggregateFuncForBitmap() throws Exception {
        ColumnDef col3 = new ColumnDef("col3", new TypeDef(ScalarType.createType(PrimitiveType.BITMAP)));
        col3.setAggregateType(AggregateType.SUM);

        cols.add(col3);
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, cols, "olap",
                new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                hashDistributioin, null, null, "");

        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Invalid aggregate function 'SUM' for 'col3'");
        stmt.analyze(analyzer);
    }

    @Test
    public void testInvalidAggregateFuncForHLL() throws Exception {
        ColumnDef col3 = new ColumnDef("col3", new TypeDef(ScalarType.createType(PrimitiveType.HLL)));
        col3.setAggregateType(AggregateType.SUM);

        cols.add(col3);
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, cols, "olap",
                new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                hashDistributioin, null, null, "");

        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Invalid aggregate function 'SUM' for 'col3'");
        stmt.analyze(analyzer);
    }

    @Test
    public void testInvalidAggregateFuncForArray() throws Exception {
        ColumnDef col3 = new ColumnDef("col3", new TypeDef(new ArrayType(ScalarType.createType(PrimitiveType.INT))));
        col3.setAggregateType(AggregateType.SUM);

        cols.add(col3);
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, cols, "olap",
                new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                hashDistributioin, null, null, "");

        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Invalid aggregate function 'SUM' for 'col3'");
        stmt.analyze(analyzer);
    }

    @Test
    public void testPrimaryKeyNullable() throws Exception {
        cols = Lists.newArrayList();
        cols.add(new ColumnDef("col1", new TypeDef(ScalarType.createType(PrimitiveType.INT)), true, null, true,
                DefaultValueDef.NOT_SET, ""));
        colsName = Lists.newArrayList();
        colsName.add("col1");
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblName, cols, "olap",
                new KeysDesc(KeysType.PRIMARY_KEYS, colsName), null,
                hashDistributioin, null, null, "");
        expectedEx.expect(AnalysisException.class);
        stmt.analyze(analyzer);
    }

    @Test
    public void testPrimaryKeyChar() throws Exception {
        cols = Lists.newArrayList();
        cols.add(new ColumnDef("col1", new TypeDef(ScalarType.createCharType(10)), true, null, false,
                DefaultValueDef.NOT_SET, ""));
        colsName = Lists.newArrayList();
        colsName.add("col1");
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblName, cols, "olap",
                new KeysDesc(KeysType.PRIMARY_KEYS, colsName), null,
                hashDistributioin, null, null, "");
        expectedEx.expect(AnalysisException.class);
        stmt.analyze(analyzer);
    }
}