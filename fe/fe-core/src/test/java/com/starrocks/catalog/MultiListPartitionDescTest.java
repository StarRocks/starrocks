// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.analysis.MultiListPartitionDesc;
import com.starrocks.analysis.PartitionDesc;
import com.starrocks.common.AnalysisException;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletType;
import org.junit.Assert;
import org.junit.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiListPartitionDescTest {

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
        partitionProperties.put("storage_cooldown_time", "2022-07-09 12:12:12");

        MultiListPartitionDesc partitionDesc =
                new MultiListPartitionDesc(ifNotExists, partitionName, multiValues, partitionProperties);
        String sql = "PARTITION p1 VALUES IN (('2022-04-15','guangdong'),('2022-04-15','tianjin'))" +
                " (\"storage_cooldown_time\" = \"2022-07-09 12:12:12\", \"storage_medium\" = \"SSD\", " +
                "\"replication_num\" = \"1\", \"tablet_type\" = \"memory\", \"in_memory\" = \"true\")";
        Assert.assertEquals(sql, partitionDesc.toSql());
    }

    @Test(expected = AnalysisException.class)
    public void testDuplicatedValue() throws AnalysisException {
        String partitionName = "p1";
        List<List<String>> multiValues = Lists.newArrayList(Lists.newArrayList("2022-04-15", "guangdong")
                , Lists.newArrayList("2022-04-15", "guangdong"));
        boolean ifNotExists = false;
        Map<String, String> partitionProperties = new HashMap<>();
        partitionProperties.put("storage_medium", "SSD");
        partitionProperties.put("replication_num", "1");
        partitionProperties.put("in_memory", "true");
        partitionProperties.put("tablet_type", "memory");
        partitionProperties.put("storage_cooldown_time", "2022-07-09 12:12:12");

        MultiListPartitionDesc partitionDesc =
                new MultiListPartitionDesc(ifNotExists, partitionName, multiValues, partitionProperties);
        partitionDesc.analyze(2, null);
    }

    @Test
    public void testGetMethods() throws ParseException, AnalysisException {
        String partitionName = "p1";
        List<List<String>> multiValues = Lists.newArrayList(Lists.newArrayList("2022-04-15", "guangdong")
                , Lists.newArrayList("2022-04-15", "tianjin"));
        boolean ifNotExists = false;
        Map<String, String> partitionProperties = new HashMap<>();
        partitionProperties.put("storage_medium", "SSD");
        partitionProperties.put("replication_num", "1");
        partitionProperties.put("in_memory", "true");
        partitionProperties.put("tablet_type", "memory");
        partitionProperties.put("storage_cooldown_time", "2022-07-09 12:12:12");

        PartitionDesc partitionDesc =
                new MultiListPartitionDesc(ifNotExists, partitionName, multiValues, partitionProperties);
        MultiListPartitionDesc listPartitionDesc = (MultiListPartitionDesc) partitionDesc;
        listPartitionDesc.analyze(2, null);

        Assert.assertEquals(partitionName, partitionDesc.getPartitionName());
        Assert.assertEquals(PartitionType.LIST, partitionDesc.getType());
        Assert.assertEquals(1, partitionDesc.getReplicationNum());
        Assert.assertEquals(TTabletType.TABLET_TYPE_MEMORY, partitionDesc.getTabletType());
        Assert.assertEquals(true, partitionDesc.isInMemory());
        Assert.assertEquals(1L, partitionDesc.getVersionInfo().longValue());
        Assert.assertEquals(ifNotExists, partitionDesc.isSetIfNotExists());

        DataProperty dataProperty = partitionDesc.getPartitionDataProperty();
        Assert.assertEquals(TStorageMedium.SSD, dataProperty.getStorageMedium());
        DateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long time = sf.parse("2022-07-09 12:12:12").getTime();
        Assert.assertEquals(time, dataProperty.getCooldownTimeMs());

        Map<String, String> properties = partitionDesc.getProperties();
        Assert.assertEquals(partitionProperties.size(), properties.size());
        properties.forEach((k, v) -> Assert.assertEquals(v, partitionProperties.get(k)));

        List<List<String>> multiValuesFromGet = listPartitionDesc.getMultiValues();
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
