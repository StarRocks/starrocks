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


package com.starrocks.service;

import com.google.gson.Gson;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.common.Config;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TGetTablesConfigRequest;
import com.starrocks.thrift.TGetTablesConfigResponse;
import com.starrocks.thrift.TTableConfigInfo;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class InformationSchemaDataSourceTest {
    
    @Mocked
    ExecuteEnv exeEnv;
    
    @Test
    public void testGetTablesConfig() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);

        StarRocksAssert starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT));
        starRocksAssert.withEnableMV().withDatabase("db1").useDatabase("db1");
        Config.enable_experimental_mv = true;
        String createTblStmtStr = "CREATE TABLE db1.tbl1 (`k1` int,`k2` int,`k3` int,`v1` int,`v2` int,`v3` int) " +
                                  "ENGINE=OLAP " + "PRIMARY KEY(`k1`, `k2`, `k3`) " +
                                  "COMMENT \"OLAP\" " +
                                  "DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 3 " +
                                  "ORDER BY(`v2`, `v3`) " +
                                  "PROPERTIES ('replication_num' = '1');";
        starRocksAssert.withTable(createTblStmtStr);

        String createMvStmtStr = "CREATE MATERIALIZED VIEW db1.mv1 " +
                                 "DISTRIBUTED BY HASH(k1) BUCKETS 10 " +
                                 "REFRESH ASYNC " +
                                 "AS SELECT k1, k2 " +
                                 "FROM db1.tbl1 ";
        
        starRocksAssert.withMaterializedView(createMvStmtStr);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetTablesConfigRequest req = new TGetTablesConfigRequest();
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setPattern("db1");
        authInfo.setUser("root");
        authInfo.setUser_ip("%");
        req.setAuth_info(authInfo);
        TGetTablesConfigResponse response = impl.getTablesConfig(req);
        TTableConfigInfo tableConfig = response.getTables_config_infos().get(0);
        Assert.assertEquals("db1", tableConfig.getTable_schema());
        Assert.assertEquals("tbl1", tableConfig.getTable_name());
        Assert.assertEquals("OLAP", tableConfig.getTable_engine());
        Assert.assertEquals("PRIMARY_KEYS", tableConfig.getTable_model());
        Assert.assertEquals("`k1`, `k2`, `k3`", tableConfig.getPrimary_key());
        Assert.assertEquals("", tableConfig.getPartition_key());
        Assert.assertEquals("`k1`, `k2`, `k3`", tableConfig.getDistribute_key());
        Assert.assertEquals("HASH", tableConfig.getDistribute_type());
        Assert.assertEquals(3, tableConfig.getDistribute_bucket());
        Assert.assertEquals("`v2`, `v3`", tableConfig.getSort_key());

        TTableConfigInfo mvConfig = response.getTables_config_infos().get(1);
        Assert.assertEquals("MATERIALIZED_VIEW", mvConfig.getTable_engine());
        Map<String, String> propsMap = new HashMap<>();
        propsMap = new Gson().fromJson(mvConfig.getProperties(), propsMap.getClass());
        Assert.assertEquals("1", propsMap.get("replication_num"));
        Assert.assertEquals("HDD", propsMap.get("storage_medium"));

    }

    @Test
    public void testTransferTableTypeToAdaptMysql() throws NoSuchMethodException, 
                                                            SecurityException, 
                                                            IllegalAccessException,
                                                            IllegalArgumentException, 
                                                            InvocationTargetException, 
                                                            ClassNotFoundException {

        Class<?> clazz = Class.forName(InformationSchemaDataSource.class.getName());
        Method m = clazz.getDeclaredMethod("transferTableTypeToAdaptMysql", TableType.class);
        m.setAccessible(true);
        Assert.assertEquals("EXTERNAL TABLE", m.invoke(InformationSchemaDataSource.class, TableType.MYSQL));
        Assert.assertEquals("EXTERNAL TABLE", m.invoke(InformationSchemaDataSource.class, TableType.HIVE));
        Assert.assertEquals("EXTERNAL TABLE", m.invoke(InformationSchemaDataSource.class, TableType.ICEBERG));
        Assert.assertEquals("EXTERNAL TABLE", m.invoke(InformationSchemaDataSource.class, TableType.HUDI));
        Assert.assertEquals("EXTERNAL TABLE", m.invoke(InformationSchemaDataSource.class, TableType.LAKE));
        Assert.assertEquals("EXTERNAL TABLE", m.invoke(InformationSchemaDataSource.class, TableType.ELASTICSEARCH));
        Assert.assertEquals("EXTERNAL TABLE", m.invoke(InformationSchemaDataSource.class, TableType.JDBC));
        Assert.assertEquals("BASE TABLE", m.invoke(InformationSchemaDataSource.class, TableType.OLAP));
        Assert.assertEquals("BASE TABLE", m.invoke(InformationSchemaDataSource.class, TableType.OLAP_EXTERNAL));
        Assert.assertEquals("VIEW", m.invoke(InformationSchemaDataSource.class, TableType.MATERIALIZED_VIEW));
        Assert.assertEquals("VIEW", m.invoke(InformationSchemaDataSource.class, TableType.VIEW));
        Assert.assertEquals("BASE TABLE", m.invoke(InformationSchemaDataSource.class, TableType.SCHEMA));
        Assert.assertEquals("BASE TABLE", m.invoke(InformationSchemaDataSource.class, TableType.INLINE_VIEW));
        Assert.assertEquals("BASE TABLE", m.invoke(InformationSchemaDataSource.class, TableType.BROKER));
    }
}
