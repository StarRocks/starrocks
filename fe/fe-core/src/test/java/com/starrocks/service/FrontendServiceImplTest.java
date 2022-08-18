// This file is licensed under the Elastic License 2.0. Copyright 2021-present StarRocks Inc.

package com.starrocks.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TGetTablesMetaRequest;
import com.starrocks.thrift.TGetTablesMetaResponse;


import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;

public class FrontendServiceImplTest {

    @Mocked
    ExecuteEnv exeEnv;

    @Mocked
    GlobalStateMgr globalStateMgr;

    @Test
    public void testGetTablesMeta() throws TException {

        Database db = new Database(1, "test_db");
        
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("c1", Type.ARRAY_BOOLEAN));
        columns.add(new Column("c2", Type.ARRAY_BOOLEAN));

        // OlapTable
        RangePartitionInfo partitionInfo = new RangePartitionInfo(columns);
        HashDistributionInfo distributionInfo = new HashDistributionInfo(10, columns);
        OlapTable table = new OlapTable(1, "test_table", columns, KeysType.PRIMARY_KEYS, partitionInfo, distributionInfo);
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE, "test_type");
        TableProperty tProperties = new TableProperty(properties);
        table.setTableProperty(tProperties);
        table.setColocateGroup("test_group");
        // View
        View view = new View(2, "test_view", columns); 

        db.createTable(table);
        db.createTable(view);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public Database getDb(String name) {
                return db;
            }
        };
        new Expectations(){
            {
                globalStateMgr.getDbNames();
                result = Arrays.asList("test_db");
            }
        };

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetTablesMetaRequest req = new TGetTablesMetaRequest();
        TGetTablesMetaResponse response = impl.getTablesMeta(req);
        response.tables_meta_infos.forEach(info -> {
            if (info.getTable_name().equals("test_table")) {
                Assert.assertEquals("`c1`, `c2`", info.getPrimary_key());
                Assert.assertEquals("`c1`, `c2`", info.getPartition_key());
                Assert.assertEquals("10", info.getDistribute_bucket());
                Assert.assertEquals("HASH", info.getDistribute_type());
                Assert.assertEquals("`c1`, `c2`", info.getDistribute_key());
                Assert.assertEquals("PRIMARY", info.getSort_key());
                Assert.assertEquals("{storage_type=test_type, colocate_with=test_group, replication_num=3}", info.getProperties());
            }
        });
        
    }
}
