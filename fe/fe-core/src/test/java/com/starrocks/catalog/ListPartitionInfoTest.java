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


package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.NotImplementedException;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ListPartitionInfoTest {

    private static StarRocksAssert starRocksAssert;
    private ListPartitionInfo listPartitionInfo;
    private ListPartitionInfo listPartitionInfoForMulti;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE t_recharge_detail(\n" +
                        "    id bigint not null ,\n" +
                        "    user_id  bigint not null ,\n" +
                        "    recharge_money decimal(32,2) , \n" +
                        "    province varchar(20) not null,\n" +
                        "    dt varchar(20) not null\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(id)\n" +
                        "PARTITION BY LIST (dt,province) (\n" +
                        "   PARTITION p1 VALUES IN ((\"2022-04-01\", \"beijing\")),\n" +
                        "   PARTITION p2 VALUES IN ((\"2022-04-01\", \"shanghai\"))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\"\n" +
                        ");");
    }

    @Before
    public void setUp() throws DdlException, AnalysisException {
        this.listPartitionInfo = new ListPartitionDescTest().findSingleListPartitionInfo();
        this.listPartitionInfoForMulti = new ListPartitionDescTest().findMultiListPartitionInfo();
    }

    @Test
    public void testListPartitionQueryPlan() throws Exception {
        String sql = "SELECT * FROM t_recharge_detail";
        starRocksAssert.query(sql).explainQuery();
    }

    @Test
    public void testWriteOutAndReadIn() throws IOException,
            NotImplementedException, ParseException {
        // Write objects to file
        File file = new File("./test_serial.log");
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        this.listPartitionInfo.write(out);
        out.flush();
        out.close();

        // Read object from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        PartitionInfo partitionInfo = this.listPartitionInfo.read(in);

        // Asset the type
        Assert.assertEquals(partitionInfo.getType(), PartitionType.LIST);

        // Asset the partition p1 properties
        List<Column> columnList = partitionInfo.getPartitionColumns();
        this.assertPartitionProperties((ListPartitionInfo) partitionInfo,
                columnList.get(0), "province", 10001L);

        file.delete();
    }

    private void assertPartitionProperties(ListPartitionInfo partitionInfo, Column column,
                                           String partitionName, long partitionId) throws ParseException {
        Assert.assertEquals(partitionName, column.getName());
        Assert.assertEquals(Type.VARCHAR, column.getType());

        DataProperty dataProperty = partitionInfo.getDataProperty(partitionId);
        Assert.assertEquals(TStorageMedium.SSD, dataProperty.getStorageMedium());
        DateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long time = sf.parse("2122-07-09 12:12:12").getTime();
        Assert.assertEquals(time, dataProperty.getCooldownTimeMs());

        Assert.assertEquals(1, partitionInfo.getReplicationNum(partitionId));
        Assert.assertEquals(true, partitionInfo.getIsInMemory(partitionId));

        List<String> valuesFromGet = partitionInfo.getIdToValues().get(partitionId);
        List<String> values = this.listPartitionInfo.getIdToValues().get(partitionId);
        Assert.assertEquals(valuesFromGet.size(), values.size());
        for (int i = 0; i < valuesFromGet.size(); i++) {
            Assert.assertEquals(valuesFromGet.get(i), values.get(i));
        }
    }


    @Test
    public void testToSqlForSingle() {
        List<Long> partitionId = Lists.newArrayList(10001L, 10002L);
        String sql = this.listPartitionInfo.toSql(this.findTableForSingleListPartition(), partitionId);
        String target = "PARTITION BY LIST(`province`)(\n" +
                "  PARTITION p1 VALUES IN (\'guangdong\', \'tianjin\'),\n" +
                "  PARTITION p2 VALUES IN (\'shanghai\', \'beijing\')\n" +
                ")";
        Assert.assertEquals(sql, target);
    }

    @Test
    public void testToSqlForMulti() {
        List<Long> partitionId = Lists.newArrayList(10001L, 10002L);
        String sql = this.listPartitionInfoForMulti.toSql(this.findTableForMultiListPartition(), partitionId);
        String target = "PARTITION BY LIST(`dt`,`province`)(\n" +
                "  PARTITION p1 VALUES IN (('2022-04-15', 'guangdong'), ('2022-04-15', 'tianjin')) " +
                "(\"replication_num\" = \"1\"),\n" +
                "  PARTITION p2 VALUES IN (('2022-04-16', 'shanghai'), ('2022-04-16', 'beijing')) " +
                "(\"replication_num\" = \"1\")\n" +
                ")";
        Assert.assertEquals(sql, target);
    }

    public OlapTable findTableForSingleListPartition() {
        long id = 1000L;
        String tableName = "testTbl";
        List<Column> baseSchema =
                Lists.newArrayList(new Column("id", Type.BIGINT),
                        new Column("province", Type.BIGINT));

        Map<String, String> properties = new HashMap<>();
        properties.put("replication_num", "1");

        TableProperty tableProperty = new TableProperty(properties);
        OlapTable table = new OlapTable(id, tableName, baseSchema, null,
                this.listPartitionInfo, null);
        table.setTableProperty(tableProperty);

        MaterializedIndex materializedIndex = new MaterializedIndex();
        HashDistributionInfo distributionInfo =
                new HashDistributionInfo(1, Lists.newArrayList(new Column("id", Type.BIGINT)));

        Partition p1 = new Partition(10001L, "p1", materializedIndex, distributionInfo);
        Partition p2 = new Partition(10002L, "p2", materializedIndex, distributionInfo);
        table.addPartition(p1);
        table.addPartition(p2);
        return table;
    }

    public OlapTable findTableForMultiListPartition() {
        long id = 1000L;
        String tableName = "testTbl";
        List<Column> baseSchema =
                Lists.newArrayList(new Column("id", Type.BIGINT), new Column("province", Type.BIGINT),
                        new Column("dt", Type.DATE));

        Map<String, String> properties = new HashMap<>();
        properties.put("replication_num", "2");

        TableProperty tableProperty = new TableProperty(properties);
        OlapTable table = new OlapTable(id, tableName, baseSchema, null,
                this.listPartitionInfoForMulti, null);
        table.setTableProperty(tableProperty);

        MaterializedIndex materializedIndex = new MaterializedIndex();
        HashDistributionInfo distributionInfo =
                new HashDistributionInfo(1, Lists.newArrayList(new Column("id", Type.BIGINT)));

        Partition p1 = new Partition(10001L, "p1", materializedIndex, distributionInfo);
        Partition p2 = new Partition(10002L, "p2", materializedIndex, distributionInfo);
        table.addPartition(p1);
        table.addPartition(p2);
        return table;
    }

}
