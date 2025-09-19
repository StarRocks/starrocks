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

package com.starrocks.connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HivePartitionName;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.connector.PartitionUtil.createPartitionKey;
import static com.starrocks.connector.PartitionUtil.fromPartitionKey;
import static com.starrocks.connector.PartitionUtil.getPartitionName;
import static com.starrocks.connector.PartitionUtil.getSuffixName;
import static com.starrocks.connector.PartitionUtil.toPartitionValues;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PartitionUtilTest {
    private final List<Column> partColumns = Lists.newArrayList(new Column("k1", Type.INT),
            new Column("k2", ScalarType.createVarcharType(10)),
            new Column("k3", Type.DOUBLE),
            new Column("k4", Type.INT));

    @Test
    public void testStringPartitionKeyConvertToDatePartitionKey() {
        try {
            PartitionKey partitionKey = createPartitionKey(
                    Lists.newArrayList("1", "20250225112345", "3.0", HiveMetaClient.PARTITION_NULL_VALUE), partColumns);
            PartitionUtil.convertToDateLiteral(partitionKey.getKeys().get(1));
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testCreatePartitionKey() throws Exception {
        PartitionKey partitionKey = createPartitionKey(
                Lists.newArrayList("1", "a", "3.0", HiveMetaClient.PARTITION_NULL_VALUE), partColumns);
        Assertions.assertEquals("(\"1\", \"a\", \"3.0\", \"NULL\")", partitionKey.toSql());
    }

    @Test
    public void testCreateHudiPartitionKey() throws AnalysisException {
        PartitionKey partitionKey = createPartitionKey(
                Lists.newArrayList("1", "a", "3.0", HiveMetaClient.HUDI_PARTITION_NULL_VALUE), partColumns,
                Table.TableType.HUDI);
        Assertions.assertEquals("(\"1\", \"a\", \"3.0\", \"NULL\")", partitionKey.toSql());
        List<String> res = PartitionUtil.fromPartitionKey(partitionKey);
        Assertions.assertEquals("1", res.get(0));
        Assertions.assertEquals("a", res.get(1));
        Assertions.assertEquals("3.0", res.get(2));
        Assertions.assertEquals(HiveMetaClient.HUDI_PARTITION_NULL_VALUE, res.get(3));

        partitionKey = createPartitionKey(
                Lists.newArrayList("1", "a", "3.0", HiveMetaClient.PARTITION_NULL_VALUE), partColumns,
                Table.TableType.HUDI);
        Assertions.assertEquals("(\"1\", \"a\", \"3.0\", \"NULL\")", partitionKey.toSql());
        res = PartitionUtil.fromPartitionKey(partitionKey);
        Assertions.assertEquals("1", res.get(0));
        Assertions.assertEquals("a", res.get(1));
        Assertions.assertEquals("3.0", res.get(2));
        Assertions.assertEquals(HiveMetaClient.PARTITION_NULL_VALUE, res.get(3));
    }

    @Test
    public void testCreateIcebergPartitionKey() throws AnalysisException {
        PartitionKey partitionKey = createPartitionKey(
                Lists.newArrayList("1", "a", "3.0", null), partColumns,
                Table.TableType.ICEBERG);
        Assertions.assertEquals("(\"1\", \"a\", \"3.0\", \"NULL\")", partitionKey.toSql());
    }

    @Test
    public void testPaimonPartitionKey() throws AnalysisException {
        PartitionKey partitionKey = createPartitionKey(
                Lists.newArrayList("1", "a", "3.0", "__DEFAULT_PARTITION__"), partColumns,
                Table.TableType.PAIMON);
        Assertions.assertEquals("(\"1\", \"a\", \"3.0\", \"NULL\")", partitionKey.toSql());
    }

    @Test
    public void testCreateDeltaLakePartitionKey() throws AnalysisException {
        PartitionKey partitionKey = createPartitionKey(
                Lists.newArrayList("1", "a", "3.0", DeltaLakeTable.PARTITION_NULL_VALUE), partColumns,
                Table.TableType.DELTALAKE);
        Assertions.assertEquals("(\"1\", \"a\", \"3.0\", \"NULL\")", partitionKey.toSql());
    }

    @Test
    public void testCreateJDBCPartitionKey() throws AnalysisException {
        PartitionKey partitionKey = createPartitionKey(
                Lists.newArrayList("1", "a", "3.0", JDBCTable.PARTITION_NULL_VALUE), partColumns, Table.TableType.JDBC);
        Assertions.assertEquals("(\"1\", \"a\", \"3.0\", \"NULL\")", partitionKey.toSql());
    }

    @Test
    public void testGetPartitionValues() throws Exception {
        List<String> values = Lists.newArrayList("1", "a", "3.0", HiveMetaClient.PARTITION_NULL_VALUE);
        PartitionKey partitionKey = createPartitionKey(values, partColumns);
        Assertions.assertEquals(values, fromPartitionKey(partitionKey));
    }

    @Test
    public void testGetSuffixName() {
        Assertions.assertEquals("file", getSuffixName("/path/", "/path/file"));
        Assertions.assertEquals("file", getSuffixName("/path", "/path/file"));
        Assertions.assertEquals("file", getSuffixName("/dt=(a)/", "/dt=(a)/file"));
    }

    @Test
    public void testGetSuffixNameIllegal() {
        assertThrows(IllegalArgumentException.class, () -> getSuffixName("/path//", "/path/file"));
    }

    @Test
    public void testToPartitionValues() {
        Assertions.assertEquals(Lists.newArrayList("1", "2", "3"), toPartitionValues("a=1/b=2/c=3"));
        Assertions.assertEquals(Lists.newArrayList("1", "2=1"), toPartitionValues("a=1/b=2=1"));
        Assertions.assertEquals(Lists.newArrayList("1", "2=null"), toPartitionValues("a=1/b=2=null"));
        Assertions.assertEquals(Lists.newArrayList("1", "2=null", "1"), toPartitionValues("a=1/b=2=null/3=1"));
        Assertions.assertEquals(Lists.newArrayList("1", "2=null", ""), toPartitionValues("a=1/b=2=null/3="));
        Assertions.assertEquals(Lists.newArrayList("1", "", "1"), toPartitionValues("a=1/b=/3=1"));
    }

    @Test
    public void testFromPartitionKey() {
        PartitionKey partitionKey = new PartitionKey();
        LiteralExpr boolTrue1 = new BoolLiteral(true);
        partitionKey.pushColumn(boolTrue1, PrimitiveType.BOOLEAN);
        Assertions.assertEquals(Lists.newArrayList("true"), fromPartitionKey(partitionKey));
    }

    @Test
    public void testHiveTimestampPartitionNames() throws AnalysisException {
        List<String> partitionValues = Lists.newArrayList("2007-01-01 10:35:00.0", "2007-01-01 10:35:00.123");
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("a", Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        columns.add(new Column("b", Type.fromPrimitiveType(PrimitiveType.DATETIME)));

        PartitionKey partitionKey = PartitionUtil.createPartitionKey(partitionValues, columns, Table.TableType.HIVE);
        List<String> res = PartitionUtil.fromPartitionKey(partitionKey);
        Assertions.assertEquals("2007-01-01 10:35:00.0", res.get(0));
        Assertions.assertEquals("2007-01-01 10:35:00.123", res.get(1));

        partitionValues = Lists.newArrayList("2007-01-01 10:35:00", "2007-01-01 10:35:00.00",
                "2007-01-01 10:35:00.000");
        columns = new ArrayList<>();
        columns.add(new Column("a", Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        columns.add(new Column("b", Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        columns.add(new Column("c", Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        partitionKey = PartitionUtil.createPartitionKey(partitionValues, columns, Table.TableType.HIVE);
        res = PartitionUtil.fromPartitionKey(partitionKey);
        Assertions.assertEquals("2007-01-01 10:35:00", res.get(0));
        Assertions.assertEquals("2007-01-01 10:35:00.00", res.get(1));
        Assertions.assertEquals("2007-01-01 10:35:00.000", res.get(2));
    }

    @Test
    public void testHiveIntPartitionNames() throws Exception {
        List<String> partitionValues = Lists.newArrayList("2007-01-01", "01");
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("a", Type.fromPrimitiveType(PrimitiveType.DATE)));
        columns.add(new Column("b", Type.fromPrimitiveType(PrimitiveType.INT)));

        PartitionKey partitionKey = PartitionUtil.createPartitionKey(partitionValues, columns, Table.TableType.HIVE);
        List<String> res = PartitionUtil.fromPartitionKey(partitionKey);
        Assertions.assertEquals("2007-01-01", res.get(0));
        Assertions.assertEquals("01", res.get(1));

        partitionValues = Lists.newArrayList("125", "0125");
        columns = new ArrayList<>();
        columns.add(new Column("a", Type.fromPrimitiveType(PrimitiveType.INT)));
        columns.add(new Column("b", Type.fromPrimitiveType(PrimitiveType.INT)));

        partitionKey = PartitionUtil.createPartitionKey(partitionValues, columns, Table.TableType.HIVE);
        res = PartitionUtil.fromPartitionKey(partitionKey);
        Assertions.assertEquals("125", res.get(0));
        Assertions.assertEquals("0125", res.get(1));
    }

    @Test
    public void testHivePartitionNames() {
        List<String> partitionValues = Lists.newArrayList("1", "2", "3");
        String partitionNames = "a=1/b=2/c=3";
        HivePartitionName hivePartitionName = new HivePartitionName("db", "table",
                partitionValues, Optional.of(partitionNames));
        Assertions.assertEquals("HivePartitionName{databaseName='db', tableName='table'," +
                " partitionValues=[1, 2, 3], partitionNames=Optional[a=1/b=2/c=3]}", hivePartitionName.toString());

        List<String> partitionColNames = Lists.newArrayList("k1");
        Map<String, String> partitionColToValue = Maps.newHashMap();
        partitionColToValue.put("k1", "1");
        Assertions.assertEquals("k1=1", PartitionUtil.toHivePartitionName(partitionColNames, partitionColToValue));

        partitionColNames.add("k3");
        partitionColToValue.put("k3", "c");
        Assertions.assertEquals("k1=1/k3=c", PartitionUtil.toHivePartitionName(partitionColNames, partitionColToValue));

        partitionColNames.add("k5");
        partitionColNames.add("k4");
        partitionColNames.add("k6");
        partitionColToValue.put("k4", "d");
        partitionColToValue.put("k5", "e");
        partitionColToValue.put("k6", "f");

        Assertions.assertEquals("k1=1/k3=c/k5=e/k4=d/k6=f",
                PartitionUtil.toHivePartitionName(partitionColNames, partitionColToValue));

        partitionColNames.add("not_exists");
        try {
            PartitionUtil.toHivePartitionName(partitionColNames, partitionColToValue);
            Assertions.fail();
        } catch (StarRocksConnectorException e) {
            Assertions.assertTrue(e.getMessage().contains("Can't find column"));
        }
    }

    @Test
    public void testGetPartitionRange(@Mocked HiveTable table) throws StarRocksException {
        Column partitionColumn = new Column("date", Type.DATE);
        List<String> partitionNames = ImmutableList.of("date=2022-08-02", "date=2022-08-19", "date=2022-08-21",
                "date=2022-09-01", "date=2022-10-01", "date=2022-12-02");

        new MockUp<PartitionUtil>() {
            @Mock
            public List<Column> getPartitionColumns(Table table) {
                return ImmutableList.of(partitionColumn);
            }
        };
        new MockUp<MetadataMgr>() {
            @Mock
            public List<String> listPartitionNames(String catalogName, String dbName, String tableName,
                                                   ConnectorMetadatRequestContext requestContext) {
                return partitionNames;
            }
        };
        new Expectations() {
            {
                table.getType();
                result = Table.TableType.HIVE;
                minTimes = 0;

                table.isHiveTable();
                result = true;
                minTimes = 0;
            }
        };

        Map<String, Range<PartitionKey>> partitionMap =
                PartitionUtil.getPartitionKeyRange(table, partitionColumn, null);
        Assertions.assertEquals(partitionMap.size(), partitionNames.size());
        Assertions.assertTrue(partitionMap.containsKey("p20221202"));
        PartitionKey upperBound = new PartitionKey();
        upperBound.pushColumn(new DateLiteral(2022, 12, 03), PrimitiveType.DATE);
        Assertions.assertTrue(partitionMap.get("p20221202").upperEndpoint().equals(upperBound));
    }

    @Test
    public void testGetPartition() {
        String base = "hdfs://hadoop01:9000/mytable";
        String tableLocation = "hdfs://hadoop01:9000/mytable/";
        Assertions.assertTrue(getPartitionName(base, tableLocation).isEmpty());

        String errorPath = "hdfs://aaa/bbb";
        ExceptionChecker.expectThrowsWithMsg(
                IllegalStateException.class,
                "Can't infer partition name. base path",
                () -> PartitionUtil.getPartitionName(base, errorPath));

        String partitionPath = "hdfs://hadoop01:9000/mytable/year=2023/month=12/day=30";
        Assertions.assertEquals("year=2023/month=12/day=30", PartitionUtil.getPartitionName(base, partitionPath));
    }
}
