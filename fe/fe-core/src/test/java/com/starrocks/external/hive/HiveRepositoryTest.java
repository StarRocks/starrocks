// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.common.collect.Maps;
import com.starrocks.catalog.HiveResource;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.analyzer.PrivilegeChecker;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HiveRepositoryTest extends DDLTestBase {
    private static ConnectContext connectContext;

    @Before
    public void setUp() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testGetClient() throws Exception {
        String resourceName = "hive0";
        String metastoreURIs = "thrift://127.0.0.1:9380";
        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", "hive");
        properties.put("hive.metastore.uris", metastoreURIs);
        CreateResourceStmt stmt = new CreateResourceStmt(true, resourceName, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        PrivilegeChecker.check(stmt, connectContext);
        HiveResource resource = (HiveResource) Resource.fromStmt(stmt);

        new MockUp<HiveMetaClient>() {
            @Mock
            public CurrentNotificationEventId getCurrentNotificationEventId() throws DdlException {
                return new CurrentNotificationEventId(1L);
            }
        };

        ResourceMgr resourceMgr = GlobalStateMgr.getCurrentState().getResourceMgr();
        new Expectations(resourceMgr) {
            {
                resourceMgr.getResource("hive0");
                result = resource;
            }
        };

        HiveRepository repository = new HiveRepository();
        ConcurrentLinkedQueue<HiveMetaClient> queue = new ConcurrentLinkedQueue<>();
        ExecutorService es = Executors.newCachedThreadPool();
        // get 10 client concurrently
        for (int i = 0; i < 10; i++) {
            es.execute(() -> {
                try {
                    queue.offer(repository.getClient(resourceName));
                } catch (Exception e) {
                    Assert.fail("exception: " + e.getMessage());
                }
            });
        }
        es.shutdown();
        es.awaitTermination(1, TimeUnit.HOURS);
        Assert.assertEquals(10, queue.size());
        HiveMetaClient client = queue.poll();
        while (queue.size() > 0) {
            Assert.assertSame(client, queue.poll());
        }
    }

    @Test
    public void testHiveExternalTableCounter() {
        HiveRepository repo = GlobalStateMgr.getCurrentState().getHiveRepository();
        String resource = "hive_resource";
        String database = "hive_db";
        String table = "hive_tbl";
        Assert.assertEquals(0, repo.getCounter().get(resource, database, table));
        Assert.assertEquals(0, repo.getCounter().reduce(resource, database, table));
        Assert.assertEquals(0, repo.getCounter().get(resource, database, table));
        Assert.assertEquals(1, repo.getCounter().add(resource, database, table));
        Assert.assertEquals(1, repo.getCounter().get(resource, database, table));
        Assert.assertEquals(2, repo.getCounter().add(resource, database, table));
        Assert.assertEquals(2, repo.getCounter().get(resource, database, table));
        Assert.assertEquals(1, repo.getCounter().reduce(resource, database, table));
        Assert.assertEquals(1, repo.getCounter().get(resource, database, table));
        Assert.assertEquals(0, repo.getCounter().reduce(resource, database, table));
        Assert.assertEquals(0, repo.getCounter().get(resource, database, table));
        Assert.assertEquals(0, repo.getCounter().reduce(resource, database, table));
        Assert.assertEquals(0, repo.getCounter().get(resource, database, table));
    }
}
