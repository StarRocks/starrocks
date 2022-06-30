package com.starrocks.connector.hudi;

import com.google.common.collect.Lists;
import com.starrocks.common.DdlException;
import com.starrocks.external.HiveMetaStoreTableUtils;
import com.starrocks.external.hive.HiveMetaCache;
import com.starrocks.external.hive.HiveMetaStoreThriftClient;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class HudiMetadataTest {
    @Test
    public void testListDatabaseNames(@Mocked HiveMetaStoreThriftClient metaStoreThriftClient) throws Exception {
        new Expectations() {
            {
                metaStoreThriftClient.getAllDatabases();
                result = Lists.newArrayList("db1", "db2");
                minTimes = 0;
            }
        };

        String metastoreUris = "thrift://127.0.0.1:9083";
        HudiMetadata metadata = new HudiMetadata(metastoreUris);
        List<String> expectResult = Lists.newArrayList("db1", "db2");
        Assert.assertEquals(expectResult, metadata.listDbNames());
    }

    @Test
    public void testListTableNames(@Mocked HiveMetaStoreThriftClient metaStoreThriftClient) throws Exception {
        String db1 = "db1";

        new Expectations() {
            {
                metaStoreThriftClient.getAllTables(db1);
                result = Lists.newArrayList("tbl1", "tbl2");
                minTimes = 0;
            }
        };

        String metastoreUris = "thrift://127.0.0.1:9083";
        HudiMetadata metadata = new HudiMetadata(metastoreUris);
        List<String> expectResult = Lists.newArrayList("tbl1", "tbl2");
        Assert.assertEquals(expectResult, metadata.listTableNames(db1));
    }

    @Test
    public void testListTableNamesOnNotExistDb() throws Exception {
        String db2 = "db2";
        String metastoreUris = "thrift://127.0.0.1:9083";
        HudiMetadata metadata = new HudiMetadata(metastoreUris);
        try {
            Assert.assertNull(metadata.listTableNames(db2));
        } catch (Exception e) {
            Assert.assertTrue(e instanceof DdlException);
        }
    }

    @Test
    public void testNotExistTable() throws DdlException {
        String resourceName = "thrift://127.0.0.1:9083";
        HudiMetadata metadata = new HudiMetadata(resourceName);
        Assert.assertNull(metadata.getTable("db", "tbl"));
    }
}
