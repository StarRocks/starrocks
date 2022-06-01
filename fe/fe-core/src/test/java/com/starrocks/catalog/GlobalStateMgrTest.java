// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/CatalogTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.MasterStateException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.ReplicaStateException;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.starrocks.alter.AlterJob;
import com.starrocks.alter.AlterJob.JobType;
import com.starrocks.analysis.ModifyFrontendAddressClause;
import com.starrocks.alter.SchemaChangeJob;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.cluster.Cluster;

import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.ha.BDBHA;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.load.Load;
import com.starrocks.meta.MetaContext;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Frontend;



import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;

import org.junit.Assert;
import org.junit.Before;

import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class GlobalStateMgrTest {

    @Mocked
    InetAddress addr1;

    @Before
    public void setUp() {
        MetaContext metaContext = new MetaContext();
        new Expectations(metaContext) {
            {
                MetaContext.get();
                minTimes = 0;
                result = metaContext;
            }
        };
    }

    public void mkdir(String dirString) {
        File dir = new File(dirString);
        if (!dir.exists()) {
            dir.mkdir();
        } else {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }
        }
    }

    public void addFiles(int image, int edit, String metaDir) {
        File imageFile = new File(metaDir + "image." + image);
        try {
            imageFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 1; i <= edit; i++) {
            File editFile = new File(metaDir + "edits." + i);
            try {
                editFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        File current = new File(metaDir + "edits");
        try {
            current.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File version = new File(metaDir + "VERSION");
        try {
            version.createNewFile();
            String line1 = "#Mon Feb 02 13:59:54 CST 2015\n";
            String line2 = "clusterId=966271669";
            FileWriter fw = new FileWriter(version);
            fw.write(line1);
            fw.write(line2);
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteDir(String metaDir) {
        File dir = new File(metaDir);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }

            dir.delete();
        }
    }

    @Test
    public void testSaveLoadHeader() throws Exception {
        String dir = "testLoadHeader";
        mkdir(dir);
        File file = new File(dir, "image");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        MetaContext.get().setMetaVersion(FeConstants.meta_version);
        Field field = globalStateMgr.getClass().getDeclaredField("load");
        field.setAccessible(true);
        field.set(globalStateMgr, new Load());

        long checksum1 = globalStateMgr.saveHeader(dos, new Random().nextLong(), 0);
        globalStateMgr.clear();
        globalStateMgr = null;
        dos.close();

        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        globalStateMgr = GlobalStateMgr.getCurrentState();
        long checksum2 = globalStateMgr.loadHeader(dis, 0);
        Assert.assertEquals(checksum1, checksum2);
        dis.close();

        deleteDir(dir);
    }

    @Test
    public void testSaveLoadSchemaChangeJob() throws Exception {
        String dir = "testLoadSchemaChangeJob";
        mkdir(dir);
        File file = new File(dir, "image");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        MetaContext.get().setMetaVersion(FeConstants.meta_version);
        Field field = globalStateMgr.getClass().getDeclaredField("load");
        field.setAccessible(true);
        field.set(globalStateMgr, new Load());

        Database db1 = new Database(10000L, "testCluster.db1");
        db1.setClusterName("testCluster");
        final Cluster cluster = new Cluster("testCluster", 10001L);
        MaterializedIndex baseIndex = new MaterializedIndex(20000L, IndexState.NORMAL);
        Partition partition = new Partition(2000L, "single", baseIndex, new RandomDistributionInfo(10));
        List<Column> baseSchema = new LinkedList<Column>();
        OlapTable table = new OlapTable(2L, "base", baseSchema, KeysType.AGG_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(10));
        table.addPartition(partition);
        db1.createTable(table);

        globalStateMgr.addCluster(cluster);
        globalStateMgr.unprotectCreateDb(db1);
        SchemaChangeJob job1 = new SchemaChangeJob(db1.getId(), table.getId(), null, table.getName(), 2022);

        globalStateMgr.getSchemaChangeHandler().replayInitJob(job1, globalStateMgr);
        long checksum1 = globalStateMgr.saveAlterJob(dos, 0, JobType.SCHEMA_CHANGE);
        globalStateMgr.clear();
        globalStateMgr = null;
        dos.close();

        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        globalStateMgr = GlobalStateMgr.getCurrentState();
        long checksum2 = globalStateMgr.loadAlterJob(dis, 0, JobType.SCHEMA_CHANGE);
        Assert.assertEquals(checksum1, checksum2);
        Map<Long, AlterJob> map = globalStateMgr.getSchemaChangeHandler().unprotectedGetAlterJobs();
        Assert.assertEquals(1, map.size());
        SchemaChangeJob job2 = (SchemaChangeJob) map.get(table.getId());
        Assert.assertEquals(job1.getTransactionId(), (job2.getTransactionId()));
        dis.close();

        deleteDir(dir);
    }


    private GlobalStateMgr mockGlobalStateMgr() throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

        NodeMgr nodeMgr = new NodeMgr(false, globalStateMgr);

        Field field1 = nodeMgr.getClass().getDeclaredField("frontends");
        field1.setAccessible(true);
        ConcurrentHashMap<String, Frontend> frontends = new ConcurrentHashMap<>();
        Frontend fe1 = new Frontend(FrontendNodeType.MASTER, "testName", "127.0.0.1", 1000);
        frontends.put("testName", fe1);
        field1.set(nodeMgr, frontends);

        Pair<String, Integer> selfNode = new Pair<String,Integer>("test-address", 1000);
        Field field2 = nodeMgr.getClass().getDeclaredField("selfNode");
        field2.setAccessible(true);
        field2.set(nodeMgr, selfNode);

        Field field3 = nodeMgr.getClass().getDeclaredField("role");
        field3.setAccessible(true);
        field3.set(nodeMgr, FrontendNodeType.MASTER);

        Field field4 = globalStateMgr.getClass().getDeclaredField("nodeMgr");
        field4.setAccessible(true);
        field4.set(globalStateMgr, nodeMgr);

        return globalStateMgr;
    }

    private void mockNet() {
        new MockUp<InetAddress>() {
            @Mock
            public InetAddress getByName(String host) throws UnknownHostException {
                return addr1;
            }
        };
    }

    @Test
    public void testGetFeByHost() throws Exception {
        mockNet();
        new Expectations(){
            {
                addr1.getHostAddress();
                result = "127.0.0.1";
            }
        };

        GlobalStateMgr globalStateMgr = mockGlobalStateMgr();
        Frontend testFeIp = globalStateMgr.getFeByHost("127.0.0.1");
        Assert.assertNotNull(testFeIp);
        Frontend testFeHost = globalStateMgr.getFeByHost("sandbox");
        Assert.assertNotNull(testFeHost);
    }

    @Test
    public void testReplayUpdateFrontend() throws Exception {
        GlobalStateMgr globalStateMgr = mockGlobalStateMgr();
        List<Frontend> frontends = globalStateMgr.getFrontends(null);
        Frontend fe = frontends.get(0);
        fe.updateHostAndEditLogPort("testHost", 1000);
        globalStateMgr.replayUpdateFrontend(fe);
        List<Frontend> updatedFrontends = globalStateMgr.getFrontends(null);
        Frontend updatedfFe = updatedFrontends.get(0);
        Assert.assertEquals("testHost", updatedfFe.getHost());
        Assert.assertTrue(updatedfFe.getEditLogPort() == 1000);
    }
    
    @Mocked
    BDBEnvironment env;

    @Mocked
    ReplicationGroupAdmin replicationGroupAdmin;

    @Mocked
    EditLog editLog;

    @Test
    public void testUpdateFrontend() throws Exception {
        
        new Expectations(){
            {
                env.getReplicationGroupAdmin();
                result = replicationGroupAdmin;
            }
        };

        new MockUp<ReplicationGroupAdmin>() {
            @Mock
            public void updateAddress(String nodeName, String newHostName, int newPort)
                throws EnvironmentFailureException,
                    MasterStateException,
                    MemberNotFoundException,
                    ReplicaStateException,
                    UnknownMasterException {}
        };

        new MockUp<EditLog>() {
            @Mock
            public void logUpdateFrontend(Frontend fe) {}
        };

        GlobalStateMgr globalStateMgr = mockGlobalStateMgr();
        BDBHA ha = new BDBHA(env, "testNode");
        globalStateMgr.setHaProtocol(ha);
        globalStateMgr.setEditLog(editLog);
        List<Frontend> frontends = globalStateMgr.getFrontends(null);
        Frontend fe = frontends.get(0);
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause(fe.getHost(), "sandbox-fqdn");
        globalStateMgr.modifyFrontendHost(clause);
    }

    @Test(expected = DdlException.class)
    public void testUpdateFeNotFoundException() throws Exception {
        GlobalStateMgr globalStateMgr = mockGlobalStateMgr();
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("test", "sandbox-fqdn");
        // this case will occur [frontend does not exist] exception
        globalStateMgr.modifyFrontendHost(clause);
    }

    @Test(expected = DdlException.class)
    public void testUpdateModifyCurrentMasterException() throws Exception {
        GlobalStateMgr globalStateMgr = mockGlobalStateMgr();
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("test-address", "sandbox-fqdn");
        // this case will occur [can not modify current master node] exception
        globalStateMgr.modifyFrontendHost(clause);
    }
}
