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
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HivePartitionName;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.connector.PartitionUtil.createPartitionKey;
import static com.starrocks.connector.PartitionUtil.fromPartitionKey;
import static com.starrocks.connector.PartitionUtil.getSuffixName;
import static com.starrocks.connector.PartitionUtil.toPartitionValues;

public class PartitionUtilTest {
    private final List<Column> partColumns = Lists.newArrayList(new Column("k1", Type.INT),
            new Column("k2", ScalarType.createVarcharType(10)),
            new Column("k3", Type.DOUBLE),
            new Column("k4", Type.INT));

    @Test
    public void testCreatePartitionKey() throws Exception {
        PartitionKey partitionKey = createPartitionKey(
                Lists.newArrayList("1", "a", "3.0", HiveMetaClient.PARTITION_NULL_VALUE), partColumns);
        Assert.assertEquals("(\"1\", \"a\", \"3.0\", \"NULL\")", partitionKey.toSql());
    }

    @Test
    public void testCreateHudiPartitionKey() throws AnalysisException {
        PartitionKey partitionKey = createPartitionKey(
                Lists.newArrayList("1", "a", "3.0", HiveMetaClient.HUDI_PARTITION_NULL_VALUE), partColumns, Table.TableType.HUDI);
        Assert.assertEquals("(\"1\", \"a\", \"3.0\", \"NULL\")", partitionKey.toSql());
    }

    @Test
    public void testGetPartitionValues() throws Exception {
        List<String> values = Lists.newArrayList("1", "a", "3.0", HiveMetaClient.PARTITION_NULL_VALUE);
        PartitionKey partitionKey = createPartitionKey(values, partColumns);
        Assert.assertEquals(values, fromPartitionKey(partitionKey));
    }

    @Test
    public void testGetSuffixName() {
        Assert.assertEquals("file", getSuffixName("/path/", "/path/file"));
        Assert.assertEquals("file", getSuffixName("/path", "/path/file"));
        Assert.assertEquals("file", getSuffixName("/dt=(a)/", "/dt=(a)/file"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetSuffixNameIllegal() {
        getSuffixName("/path//", "/path/file");
    }

    @Test
    public void testToPartitionValues() {
        String  partitionNames = "a=1/b=2/c=3";
        Assert.assertEquals(Lists.newArrayList("1", "2", "3"), toPartitionValues(partitionNames));
    }

    @Test
    public void testFromPartitionKey() {
        PartitionKey partitionKey = new PartitionKey();
        LiteralExpr boolTrue1 = new BoolLiteral(true);
        partitionKey.pushColumn(boolTrue1, PrimitiveType.BOOLEAN);
        Assert.assertEquals(Lists.newArrayList("true"), fromPartitionKey(partitionKey));
    }

    @Test
    public void testHivePartitionNames() {
        List<String> partitionValues = Lists.newArrayList("1", "2", "3");
        String partitionNames = "a=1/b=2/c=3";
        HivePartitionName hivePartitionName = new HivePartitionName("db", "table",
                partitionValues, Optional.of(partitionNames));
        Assert.assertEquals("HivePartitionName{databaseName='db', tableName='table'," +
                " partitionValues=[1, 2, 3], partitionNames=Optional[a=1/b=2/c=3]}", hivePartitionName.toString());

        List<String> partitionColNames = Lists.newArrayList("k1");
        Map<String, String> partitionColToValue = Maps.newHashMap();
        partitionColToValue.put("k1", "1");
        Assert.assertEquals("k1=1", PartitionUtil.toHivePartitionName(partitionColNames, partitionColToValue));

        partitionColNames.add("k3");
        partitionColToValue.put("k3", "c");
        Assert.assertEquals("k1=1/k3=c", PartitionUtil.toHivePartitionName(partitionColNames, partitionColToValue));

        partitionColNames.add("k5");
        partitionColNames.add("k4");
        partitionColNames.add("k6");
        partitionColToValue.put("k4", "d");
        partitionColToValue.put("k5", "e");
        partitionColToValue.put("k6", "f");

        Assert.assertEquals("k1=1/k3=c/k5=e/k4=d/k6=f",
                PartitionUtil.toHivePartitionName(partitionColNames, partitionColToValue));

        partitionColNames.add("not_exists");
        try {
            PartitionUtil.toHivePartitionName(partitionColNames, partitionColToValue);
            Assert.fail();
        } catch (StarRocksConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("Can't find column"));
        }
    }

    @Test
    public void testGetPartitionRange(@Mocked HiveTable table) throws UserException {
        Column partitionColumn = new Column("date", Type.DATE);
        List<String> partitionNames = ImmutableList.of("date=2022-08-02", "date=2022-08-19", "date=2022-08-21",
                "date=2022-09-01", "date=2022-10-01", "date=2022-12-02");

        new MockUp<PartitionUtil>() {
            @Mock
            public List<String> getPartitionNames(Table table) {
                return partitionNames;
            }

            @Mock
            public List<Column> getPartitionColumns(Table table) {
                return ImmutableList.of(partitionColumn);
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

        Map<String, Range<PartitionKey>> partitionMap = PartitionUtil.getPartitionRange(table, partitionColumn);
        Assert.assertEquals(partitionMap.size(), partitionNames.size());
        Assert.assertTrue(partitionMap.containsKey("p20221202"));
        PartitionKey upperBound = new PartitionKey();
        upperBound.pushColumn(new DateLiteral(2022, 12, 03), PrimitiveType.DATE);
        Assert.assertTrue(partitionMap.get("p20221202").upperEndpoint().equals(upperBound));
    }
}
