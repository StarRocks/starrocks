// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class ListPartitionInfoTest {

    private ListPartitionInfo listPartitionInfo;
    private ListPartitionInfo listPartitionInfoForMulti;
    private final static String fileName = "./test_serial.log";

    @Before
    public void setUp() throws DdlException, AnalysisException {
        this.listPartitionInfo = new ListPartitionDescTest().findSingleListPartitionInfo();
        this.listPartitionInfoForMulti = new ListPartitionDescTest().findMultiListPartitionInfo();
    }

    @Test
    public void mytest() {
        List<List<String>> multiValues = Lists.newArrayList(
                Lists.newArrayList("2022-04-15", "guangdong"),
                Lists.newArrayList("2022-04-15", "tianjin"),
                Lists.newArrayList("2022-04-15", "Tianjin")
        );
        Set[] arr = new TreeSet[2];
        for (List<String> values : multiValues) {
            int duplicatedSize = 0;
            for (int i = 0; i < values.size(); i++) {
                if (arr[i] == null) {
                    arr[i] = new TreeSet();
                }
                if (!arr[i].add(values.get(i))) {
                    duplicatedSize++;
                }
            }
            if (duplicatedSize == 2) {
                System.out.println("has duplicated");
            } else {
                System.out.println("no has duplicated");
            }
        }
    }

    @Test
    public void testWriteOut() throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        this.listPartitionInfo.serialPartitionInfo(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        ListPartitionInfo deserialEntity = ListPartitionInfo.deserialPartitionInfo(in);
        in.close();

        //asset
        List<Column> columnList = deserialEntity.getPartitionColumns();
        Assert.assertEquals(1, columnList.size());
        Assert.assertEquals("province", columnList.get(0).getName());

        Map<Long, List<String>> idToValues = deserialEntity.getIdToValues();
        Assert.assertEquals(2, idToValues.size());
        String p1 = String.join(",", idToValues.get(10001L));
        String p2 = String.join(",", idToValues.get(10002L));
        Assert.assertEquals("guangdong,tianjin", p1);
        Assert.assertEquals("shanghai,beijing", p2);

        Map<Long, List<List<String>>> idToMultiValues = deserialEntity.getIdToMultiValues();
        Assert.assertEquals(0, idToMultiValues.size());
    }

    @Test
    public void testToSqlForSingle() {
        long id = 1000L;
        String tableName = "testTable";
        List<Column> baseSchema =
                Lists.newArrayList(new Column("id", Type.BIGINT), new Column("province", Type.BIGINT));

        Map<String, String> properties = new HashMap<>();
        properties.put("replication_num", "1");

        TableProperty tableProperty = new TableProperty(properties);
        OlapTable table = new OlapTable(id, tableName, baseSchema, null, this.listPartitionInfo, null);
        table.setTableProperty(tableProperty);
        Partition p1 = new Partition(10001L, "p1", null, null);
        Partition p2 = new Partition(10002L, "p2", null, null);
        table.addPartition(p1);
        table.addPartition(p2);

        List<Long> partitionId = Lists.newArrayList(10001L, 10002L);
        String sql = this.listPartitionInfo.toSql(table, partitionId);

        String target = "PARTITION BY LIST(`province`)(\n" +
                "  PARTITION p1 VALUES IN (\'guangdong\',\'tianjin\'),\n" +
                "  PARTITION p2 VALUES IN (\'shanghai\',\'beijing\')\n" +
                ")";
        Assert.assertEquals(sql, target);
    }

    @Test
    public void testToSqlForMulti() {
        long id = 1000L;
        String tableName = "testTable";
        List<Column> baseSchema =
                Lists.newArrayList(new Column("id", Type.BIGINT), new Column("province", Type.BIGINT),
                        new Column("dt", Type.DATE));

        Map<String, String> properties = new HashMap<>();
        properties.put("replication_num", "2");

        TableProperty tableProperty = new TableProperty(properties);
        OlapTable table = new OlapTable(id, tableName, baseSchema, null, this.listPartitionInfoForMulti, null);
        table.setTableProperty(tableProperty);
        Partition p1 = new Partition(10001L, "p1", null, null);
        Partition p2 = new Partition(10002L, "p2", null, null);
        table.addPartition(p1);
        table.addPartition(p2);

        List<Long> partitionId = Lists.newArrayList(10001L, 10002L);
        String sql = this.listPartitionInfoForMulti.toSql(table, partitionId);

        String target = "PARTITION BY LIST(`dt`,`province`)(\n" +
                "  PARTITION p1 VALUES IN (('2022-04-15','guangdong'),('2022-04-15','tianjin')) (\"replication_num\" = \"1\"),\n" +
                "  PARTITION p2 VALUES IN (('2022-04-16','shanghai'),('2022-04-16','beijing')) (\"replication_num\" = \"1\")\n" +
                ")";
        Assert.assertEquals(sql, target);
    }

}
