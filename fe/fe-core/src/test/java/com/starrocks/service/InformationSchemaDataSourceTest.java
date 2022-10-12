// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.service;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TGetTablesInfoRequest;
import com.starrocks.thrift.TGetTablesInfoResponse;
import com.starrocks.thrift.TTableInfo;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;   
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InformationSchemaDataSourceTest {
    
    @Mocked
    ExecuteEnv exeEnv;

    @Mocked
    GlobalStateMgr globalStateMgr;

    @Mocked
    Auth auth;
    
    @Test
    public void testGenerateTablesInfo() throws TException, NoSuchFieldException, 
        SecurityException, IllegalArgumentException, IllegalAccessException {

        
        Database db = new Database(1, "test_db");

        List<Column> partitionsColumns = new ArrayList<>();
        partitionsColumns.add(new Column("p_c1", Type.ARRAY_BOOLEAN));
        partitionsColumns.add(new Column("p_c2", Type.ARRAY_BOOLEAN));

        List<Column> dColumns = new ArrayList<>();
        dColumns.add(new Column("d_c1", Type.ARRAY_BOOLEAN));
        dColumns.add(new Column("d_c2", Type.ARRAY_BOOLEAN));
    
        List<Column> keyColumns = new ArrayList<>();
        Column keyC1 = new Column("key_c1", Type.ARRAY_BOOLEAN);
        keyC1.setIsKey(true);
        Column keyC2 = new Column("key_c2", Type.ARRAY_BOOLEAN);            
        keyC2.setIsKey(true);
        keyColumns.add(keyC1);
        keyColumns.add(keyC2);
    
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE, "test_type");
        TableProperty tProperties = new TableProperty(properties);
    
    
        // OlapTable
        RangePartitionInfo partitionInfo = new RangePartitionInfo(partitionsColumns);
        HashDistributionInfo distributionInfo = new HashDistributionInfo(10, dColumns);
    
        // PK
        OlapTable tablePk = new OlapTable(1, "test_table_pk", keyColumns, KeysType.PRIMARY_KEYS, partitionInfo, distributionInfo);
        MaterializedIndex index = new MaterializedIndex(2, IndexState.NORMAL);
        index.setRowCount(2000L);
        Partition partition = new Partition(1, "test_p", index, distributionInfo);
        
        Field pvisibleVersionTime = partition.getClass().getDeclaredField("visibleVersionTime");
        pvisibleVersionTime.setAccessible(true);
        pvisibleVersionTime.set(partition, 4000L);

        tablePk.addPartition(partition);
        tablePk.setTableProperty(tProperties);
        tablePk.setColocateGroup("test_group");
        tablePk.setComment("test_comment");
        tablePk.setLastCheckTime(2000L);
        
        // View
        View view = new View(2, "test_view", keyColumns); 
        db.createTable(tablePk);
        db.createTable(view);

    
        Field field = globalStateMgr.getClass().getDeclaredField("auth");
        field.setAccessible(true);
        field.set(globalStateMgr, auth);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public List<String> getDbNames() {
                return Arrays.asList("test_db");
            }
        };

        new MockUp<Auth>() {
            @Mock
            public boolean checkDbPriv(UserIdentity currentUser, String db, PrivPredicate wanted) {
                return true;
            }
        };

        new MockUp<PatternMatcher>() {
            @Mock
            public boolean match(String candidate) {
                return true;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public Database getDb(String name) {
                return db;
            }
        };

        new MockUp<MaterializedIndex>() {
            @Mock
            public long getDataSize() {
                return 4000L;
            }
        };
        
        TGetTablesInfoRequest request = new TGetTablesInfoRequest();
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setPattern("test parttern");
        request.setAuth_info(authInfo);
        TGetTablesInfoResponse response = InformationSchemaDataSource.generateTablesInfoResponse(request);
        
        List<TTableInfo> infos = response.getTables_infos();
        infos.forEach(info -> {
            if (info.getTable_name().equals("test_table_pk")) {
                Assert.assertTrue(info.getTable_catalog().equals("def"));
                Assert.assertTrue(info.getTable_schema().equals("test_db"));
                Assert.assertTrue(info.getTable_name().equals("test_table_pk"));
                Assert.assertTrue(info.getTable_type().equals("BASE TABLE"));
                Assert.assertTrue(info.getEngine().equals("StarRocks"));
                Assert.assertTrue(info.getVersion() == -1L);
                Assert.assertTrue(info.getRow_format().equals("NULL"));
                Assert.assertTrue(info.getTable_rows() == 2000L);
                Assert.assertTrue(info.getAvg_row_length() == 2L);
                Assert.assertTrue(info.getData_length() == 4000L);
                Assert.assertTrue(info.getMax_data_length() == -1L);
                Assert.assertTrue(info.getIndex_length() == -1L);
                Assert.assertTrue(info.getData_free() == -1L);
                Assert.assertTrue(info.getAuto_increment() == -1L);
                // create time
                Assert.assertTrue(info.getUpdate_time() == 4L);
                Assert.assertTrue(info.getCheck_time() == 2000L);
                Assert.assertTrue(info.getTable_collation().equals("utf8_general_ci"));
                Assert.assertTrue(info.getChecksum() == -1L);
                Assert.assertTrue(info.getCreate_options().equals("NULL"));
                Assert.assertTrue(info.getTable_comment().equals("test_comment"));
            } 
            if (info.getTable_name().equals("test_view")) {
                Assert.assertTrue(info.getTable_type().equals("VIEW"));
                Assert.assertTrue(info.getTable_rows() == -1L);
                Assert.assertTrue(info.getAvg_row_length() == -1L);
                Assert.assertTrue(info.getData_length() == -1L);
                Assert.assertTrue(info.getUpdate_time() == -1L);
            }
        });
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
        Assert.assertTrue(((String) m.invoke(InformationSchemaDataSource.class, TableType.MYSQL)).
                equals("EXTERNAL TABLE"));
        Assert.assertTrue(((String) m.invoke(InformationSchemaDataSource.class, TableType.HIVE)).
                equals("EXTERNAL TABLE"));
        Assert.assertTrue(((String) m.invoke(InformationSchemaDataSource.class, TableType.ICEBERG)).
                equals("EXTERNAL TABLE"));
        Assert.assertTrue(((String) m.invoke(InformationSchemaDataSource.class, TableType.HUDI)).
                equals("EXTERNAL TABLE"));
        Assert.assertTrue(((String) m.invoke(InformationSchemaDataSource.class, TableType.LAKE)).
                equals("EXTERNAL TABLE"));
        Assert.assertTrue(((String) m.invoke(InformationSchemaDataSource.class, TableType.ELASTICSEARCH)).
                equals("EXTERNAL TABLE"));
        Assert.assertTrue(((String) m.invoke(InformationSchemaDataSource.class, TableType.JDBC)).
                equals("EXTERNAL TABLE"));
        Assert.assertTrue(((String) m.invoke(InformationSchemaDataSource.class, TableType.OLAP)).
                equals("BASE TABLE"));
        Assert.assertTrue(((String) m.invoke(InformationSchemaDataSource.class, TableType.OLAP_EXTERNAL)).
                equals("BASE TABLE"));
        Assert.assertTrue(((String) m.invoke(InformationSchemaDataSource.class, TableType.MATERIALIZED_VIEW)).
                equals("MATERIALIZED VIEW"));
        Assert.assertTrue(((String) m.invoke(InformationSchemaDataSource.class, TableType.VIEW)).
                equals("VIEW"));
        Assert.assertTrue(((String) m.invoke(InformationSchemaDataSource.class, TableType.SCHEMA)).
                equals("BASE TABLE"));
        Assert.assertTrue(((String) m.invoke(InformationSchemaDataSource.class, TableType.INLINE_VIEW)).
                equals("BASE TABLE"));
        Assert.assertTrue(((String) m.invoke(InformationSchemaDataSource.class, TableType.BROKER)).
                equals("BASE TABLE"));
    }
}
