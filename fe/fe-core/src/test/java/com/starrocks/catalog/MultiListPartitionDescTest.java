// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.MultiItemListPartitionDesc;
import com.starrocks.analysis.TypeDef;
import com.starrocks.common.AnalysisException;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletType;
import com.starrocks.utframe.UtFrameUtils;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiListPartitionDescTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
    }
    
    @Test
    public void testToSQL() {
        String partitionName = "p1";
        List<List<String>> multiValues = Lists.newArrayList(Lists.newArrayList("2022-04-15", "guangdong")
                , Lists.newArrayList("2022-04-15", "tianjin"));
        boolean ifNotExists = false;
        Map<String, String> partitionProperties = new HashMap<>();
        partitionProperties.put("storage_medium", "SSD");
        partitionProperties.put("replication_num", "1");
        partitionProperties.put("in_memory", "true");
        partitionProperties.put("tablet_type", "memory");
        partitionProperties.put("storage_cooldown_time", "2122-07-09 12:12:12");

        MultiItemListPartitionDesc partitionDesc =
                new MultiItemListPartitionDesc(ifNotExists, partitionName, multiValues, partitionProperties);
        String sql = "PARTITION p1 VALUES IN (('2022-04-15','guangdong'),('2022-04-15','tianjin'))" +
                " (\"storage_cooldown_time\" = \"2122-07-09 12:12:12\", \"storage_medium\" = \"SSD\", " +
                "\"replication_num\" = \"1\", \"tablet_type\" = \"memory\", \"in_memory\" = \"true\")";
        Assert.assertEquals(sql, partitionDesc.toSql());
    }

    @Test
    public void testGetMethods() throws ParseException, AnalysisException {
        ColumnDef province = new ColumnDef("province", TypeDef.createVarchar(64));
        province.setAggregateType(AggregateType.NONE);
        ColumnDef dt = new ColumnDef("dt", TypeDef.create(PrimitiveType.DATE));
        dt.setAggregateType(AggregateType.NONE);
        List<ColumnDef> columnDefLists = Lists.newArrayList(dt, province);

        String partitionName = "p1";
        List<List<String>> multiValues = Lists.newArrayList(Lists.newArrayList("2022-04-15", "guangdong")
                , Lists.newArrayList("2022-04-15", "tianjin"));
        boolean ifNotExists = false;
        Map<String, String> partitionProperties = new HashMap<>();
        partitionProperties.put("storage_medium", "SSD");
        partitionProperties.put("replication_num", "1");
        partitionProperties.put("in_memory", "true");
        partitionProperties.put("tablet_type", "memory");
        partitionProperties.put("storage_cooldown_time", "2122-07-09 12:12:12");

        MultiItemListPartitionDesc partitionDesc = new MultiItemListPartitionDesc(ifNotExists, partitionName,
                multiValues, partitionProperties);
        partitionDesc.analyze(columnDefLists, null);

        Assert.assertEquals(partitionName, partitionDesc.getPartitionName());
        Assert.assertEquals(PartitionType.LIST, partitionDesc.getType());
        Assert.assertEquals(1, partitionDesc.getReplicationNum());
        Assert.assertEquals(TTabletType.TABLET_TYPE_MEMORY, partitionDesc.getTabletType());
        Assert.assertEquals(true, partitionDesc.isInMemory());

        DataProperty dataProperty = partitionDesc.getPartitionDataProperty();
        Assert.assertEquals(TStorageMedium.SSD, dataProperty.getStorageMedium());
        DateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long time = sf.parse("2122-07-09 12:12:12").getTime();
        Assert.assertEquals(time, dataProperty.getCooldownTimeMs());

        List<List<String>> multiValuesFromGet = partitionDesc.getMultiValues();
        Assert.assertEquals(multiValuesFromGet.size(), multiValues.size());
        for (int i = 0; i < multiValuesFromGet.size(); i++) {
            List<String> valuesFromGet = multiValuesFromGet.get(i);
            List<String> values = multiValues.get(i);
            Assert.assertEquals(valuesFromGet.size(), values.size());
            for (int j = 0; j < valuesFromGet.size(); j++) {
                Assert.assertEquals(valuesFromGet.get(j), values.get(j));
            }
        }
    }

}
