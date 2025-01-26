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


package com.starrocks.journal.bdbje;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.RollbackException;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.stream.MatchpointSearchResults;
import com.sleepycat.je.utilint.DatabaseUtil;
import com.sleepycat.je.utilint.VLSN;
import com.starrocks.common.Config;
import com.starrocks.journal.JournalException;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class BDBEnvironmentTest {
    private static final Logger LOG = LogManager.getLogger(BDBEnvironmentTest.class);
    private List<File> tmpDirs = new ArrayList<>();

    public File createTmpDir() throws Exception {
        File f = Files.createTempDirectory(Paths.get("."), "BDBEnvironmentTest").toFile();
        tmpDirs.add(f);
        return f;
    }

    @After
    public void cleanup() throws Exception {
        for (File tmpDir : tmpDirs) {
            FileUtils.deleteDirectory(tmpDir);
        }
    }

    private String findUnbindHostPort() throws Exception {
        return "127.0.0.1:" + UtFrameUtils.findValidPort();
    }

    private DatabaseEntry randomEntry() {
        byte[] array = new byte[16];
        new Random().nextBytes(array);
        return new DatabaseEntry(array);
    }


    @Test
    public void testSetupStandalone() throws Exception {
        long startMs = System.currentTimeMillis();
        String selfNodeHostPort = findUnbindHostPort();
        BDBEnvironment environment = new BDBEnvironment(
                createTmpDir(),
                "standalone",
                selfNodeHostPort,
                selfNodeHostPort,
                true);
        environment.setup(true);

        CloseSafeDatabase db = environment.openDatabase("testdb");
        DatabaseEntry key = randomEntry();
        DatabaseEntry value = randomEntry();
        db.put(null, key, value);

        DatabaseEntry newvalue = new DatabaseEntry();
        db.get(null, key, newvalue, LockMode.READ_COMMITTED);
        Assert.assertEquals(new String(value.getData()), new String(newvalue.getData()));
        db.close();
        environment.close();
        System.out.println("testSetupStandalone cost " + (System.currentTimeMillis() - startMs) / 1000 + " s");
    }

    // address already in use
    @Test(expected = JournalException.class)
    public void testSetupStandaloneMultitimes() throws Exception {
        long startMs = System.currentTimeMillis();
        try {
            String selfNodeHostPort = findUnbindHostPort();
            for (int i = 0; i < 2; i++) {
                BDBEnvironment environment = new BDBEnvironment(
                        createTmpDir(),
                        "standalone",
                        selfNodeHostPort,
                        selfNodeHostPort,
                        true);
                environment.setup(true);
            }
            Assert.fail();
        } finally {
            System.out.println("testSetupStandaloneMultitimes cost " + (System.currentTimeMillis() - startMs) / 1000 + " s");
        }
    }

    /**
     * used for cluster test from here, 1 leader + 2 follower + 1 observer
     */
    private String leaderNodeHostPort = null;
    private File leaderPath = null;
    private BDBEnvironment leaderEnvironment = null;
    private String leaderName;
    private BDBEnvironment[] followerEnvironments = new BDBEnvironment[2];
    private String[] followerNodeHostPorts = new String[2];
    private File[] followerPaths = new File[2];
    private String[] followerNames = new String[2];


    private void initClusterMasterFollower() throws Exception {
        for (int i = 0; i != 3; ++ i) {
            // might fail on high load, will sleep and retry
            try {
                initClusterMasterFollowerNoRetry();
                return;
            } catch (Exception e) {
                // sleep 5 ~ 15 seconds
                int sleepSeconds = ThreadLocalRandom.current().nextInt(5, 15);
                LOG.warn("failed to initClusterMasterFollower! will sleep {} seconds and retry", sleepSeconds, e);
                Thread.sleep(sleepSeconds * 1000L);
            }
        }

    }
    private void initClusterMasterFollowerNoRetry() throws Exception {
        BDBEnvironment.RETRY_TIME = 3;
        // give leader time to update membership
        // otherwise may get error Conflicting node types: uses: SECONDARY Replica is configured as type: ELECTABLE
        BDBEnvironment.SLEEP_INTERVAL_SEC = ThreadLocalRandom.current().nextInt(5, 15);
        // set timeout to a really long time so that ut can pass even when IO load is very high
        Config.bdbje_heartbeat_timeout_second = 60;
        Config.bdbje_replica_ack_timeout_second = 60;
        Config.bdbje_lock_timeout_second = 60;

        // setup leader
        leaderNodeHostPort = findUnbindHostPort();
        leaderPath = createTmpDir();
        leaderName = "leader";
        leaderEnvironment = new BDBEnvironment(
                leaderPath,
                leaderName,
                leaderNodeHostPort,
                leaderNodeHostPort,
                true);
        leaderEnvironment.setup(true);
        Assert.assertEquals(0, leaderEnvironment.getDatabaseNames().size());

        // set up 2 followers
        for (int i = 0; i < 2; i++) {
            followerNodeHostPorts[i] = findUnbindHostPort();
            followerPaths[i] = createTmpDir();
            followerNames[i] = String.format("follower%d", i);
            BDBEnvironment followerEnvironment = new BDBEnvironment(
                    followerPaths[i],
                    followerNames[i],
                    followerNodeHostPorts[i],
                    leaderNodeHostPort,
                    true);
            followerEnvironments[i] = followerEnvironment;
            followerEnvironment.setup(true);
            Assert.assertEquals(0, followerEnvironment.getDatabaseNames().size());
        }
        BDBEnvironment.RETRY_TIME = 3;
        BDBEnvironment.SLEEP_INTERVAL_SEC = 1;
    }

    @Test
    public void testNormalCluster() throws Exception {
        long startMs = System.currentTimeMillis();
        initClusterMasterFollower();

        // leader write
        Long dbIndex1 = 0L;
        String dbName1 = String.valueOf(dbIndex1);
        CloseSafeDatabase leaderDb = leaderEnvironment.openDatabase(dbName1);
        Assert.assertEquals(1, leaderEnvironment.getDatabaseNames().size());
        Assert.assertEquals(dbIndex1, leaderEnvironment.getDatabaseNames().get(0));
        DatabaseEntry key = randomEntry();
        DatabaseEntry value = randomEntry();
        leaderDb.put(null, key, value);
        leaderDb.close();

        Thread.sleep(1000);

        // follower read
        for (BDBEnvironment followerEnvironment : followerEnvironments) {
            Assert.assertEquals(1, followerEnvironment.getDatabaseNames().size());
            Assert.assertEquals(dbIndex1, followerEnvironment.getDatabaseNames().get(0));

            CloseSafeDatabase followerDb = followerEnvironment.openDatabase(dbName1);
            DatabaseEntry newvalue = new DatabaseEntry();
            followerDb.get(null, key, newvalue, LockMode.READ_COMMITTED);
            Assert.assertEquals(new String(value.getData()), new String(newvalue.getData()));
            followerDb.close();
        }

        // add observer
        BDBEnvironment observerEnvironment = new BDBEnvironment(
                createTmpDir(),
                "observer",
                findUnbindHostPort(),
                leaderNodeHostPort,
                false);
        observerEnvironment.setup(true);

        // observer read
        Assert.assertEquals(1, observerEnvironment.getDatabaseNames().size());
        Assert.assertEquals(dbIndex1, observerEnvironment.getDatabaseNames().get(0));

        CloseSafeDatabase observerDb = observerEnvironment.openDatabase(dbName1);
        DatabaseEntry newvalue = new DatabaseEntry();
        observerDb.get(null, key, newvalue, LockMode.READ_COMMITTED);
        Assert.assertEquals(new String(value.getData()), new String(newvalue.getData()));
        observerDb.close();

        // close
        leaderEnvironment.close();
        for (BDBEnvironment followerEnvironment : followerEnvironments) {
            followerEnvironment.close();
        }
        observerEnvironment.close();
        System.out.println("testNormalCluster cost " + (System.currentTimeMillis() - startMs) / 1000 + " s");
    }

    /**
     * see https://github.com/StarRocks/starrocks/issues/4977
     *
     * This test case tries to simulate an unexpected scenario where a leader was down before it got way ahead of all
     * followers. Normally BDB will cause a `RollbackException`, which we should handle and turn to a
     * `JournalException`. But there is a slight chance that BDB may handle it well and nothing goes wrong. Either way,
     * we think it's reached our expectation.
     */
    @Test
    public void testRollbackExceptionOnSetupCluster(@Mocked RepImpl rep) throws Exception {
        long startMs = System.currentTimeMillis();
        new Expectations() {
            {
                rep.getName();
                minTimes = 0;
                result = "starrocks";

                rep.isValid();
                minTimes = 0;
                result = false;

                rep.getConfigManager();
                minTimes = 0;
                result = new DbConfigManager(new EnvironmentConfig());
            }
        };

        // mock DatabaseUtil.checkForNullParam to generate RollBackException
        new MockUp<DatabaseUtil>() {
            @Mock
            public void checkForNullParam(final Object param, final String name) {
                throw new RollbackException(rep, VLSN.FIRST_VLSN, new MatchpointSearchResults(rep));
            }
        };

        leaderNodeHostPort = findUnbindHostPort();
        leaderPath = createTmpDir();
        // set retry times = 1 to ensure no recovery
        BDBEnvironment.RETRY_TIME = 1;
        // start leader will get rollback exception
        BDBEnvironment maserEnvironment = new BDBEnvironment(
                leaderPath,
                "leader",
                leaderNodeHostPort,
                leaderNodeHostPort,
                true);
        Assert.assertTrue(true);
        try {
            maserEnvironment.setup(true);
        } catch (JournalException e) {
            LOG.warn("got Rollback Exception, as expect, ", e);
        }
        System.out.println("testRollbackExceptionOnSetupCluster cost " + (System.currentTimeMillis() - startMs) / 1000 + " s");
    }

    /**
     * simulate leader failover, return the index of the instance that remains follower
     */
    private void leaderFailOver() throws Exception {
        // leader down
        leaderEnvironment.close();
        LOG.warn("======> leader env is closed");
        Thread.sleep(1000);

        // find the new leader
        BDBEnvironment newMasterEnvironment = null;
        int newMasterFollowerIndex = 0;
        while (newMasterEnvironment == null) {
            Thread.sleep(1000);
            for (int i = 0; i < 2; ++ i) {
                if (followerEnvironments[i].getReplicatedEnvironment().getState() == ReplicatedEnvironment.State.MASTER) {
                    newMasterEnvironment = followerEnvironments[i];
                    LOG.warn("=========> new leader is {}", newMasterEnvironment.getReplicatedEnvironment().getNodeName());
                    newMasterEnvironment.setup(true);
                    newMasterFollowerIndex = i;
                    break;
                }
            }
        }

        // start the old leader
        BDBEnvironment oldMasterEnvironment = new BDBEnvironment(
                leaderPath,
                "leader",
                leaderNodeHostPort,
                leaderNodeHostPort,
                true);
        oldMasterEnvironment.setup(true);
        LOG.warn("============> old leader is setup as follower");
        Thread.sleep(1000);

        leaderEnvironment = newMasterEnvironment;
        leaderNodeHostPort = followerNodeHostPorts[newMasterFollowerIndex];
    }

    private void printHAStatus() {
        LOG.info("---------------------");
        LOG.info("{}", leaderEnvironment.getReplicatedEnvironment().getGroup().getRepGroupImpl().toString());
        RepGroupImpl imp = leaderEnvironment.getReplicatedEnvironment().getGroup().getRepGroupImpl();
        LOG.info("---------------------");
    }

    @Ignore
    @Test
    public void testAddBadFollowerNoFailover() throws Exception {
        long startMs = System.currentTimeMillis();
        testAddBadFollowerBase(false);
        System.out.println("testAddBadFollowerNoFailover cost " + (System.currentTimeMillis() - startMs) / 1000 + " s");
    }

    @Ignore
    @Test
    public void testAddBadFollowerAfterFailover() throws Exception {
        long startMs = System.currentTimeMillis();
        testAddBadFollowerBase(true);
        System.out.println("testAddBadFollowerAfterFailover cost " + (System.currentTimeMillis() - startMs) / 1000 + " s");
    }

    protected void testAddBadFollowerBase(boolean failover) throws Exception {
        initClusterMasterFollower();

        if (failover) {
            leaderFailOver();
        }

        printHAStatus();

        // 1. bad new follower start for the first time
        // helper = self, use a new generated name
        String newFollowerHostPort = findUnbindHostPort();
        String newFollowerName = "newFollower";
        File newFollowerPath = createTmpDir();
        BDBEnvironment newfollowerEnvironment = new BDBEnvironment(
                newFollowerPath,
                newFollowerName,
                newFollowerHostPort,
                newFollowerHostPort,
                true);
        LOG.warn("=========> start new follower for the first time");
        // should set up successfully as a standalone leader
        newfollowerEnvironment.setup(true);
        newfollowerEnvironment.close();

        // 2. bad new follower start for the second time
        // helper = leader
        newfollowerEnvironment = new BDBEnvironment(
                newFollowerPath,
                newFollowerName,
                newFollowerHostPort,
                leaderNodeHostPort,
                true);
        LOG.warn("==========> start new follower for the second time");
        try {
            newfollowerEnvironment.setup(true);
        } catch (Exception e) {
            LOG.warn("===========> failed for the second time, as expect, ", e);
        }

        // 5. normally leader won't down
        for (int i = 0; i < 2; ++i) {
            Thread.sleep(1000);
            LOG.warn("==============> getDatabasesNames() {}", leaderEnvironment.getDatabaseNames());
        }
    }

    @Test
    public void testGetDatabase() throws Exception {
        long startMs = System.currentTimeMillis();
        String selfNodeHostPort = findUnbindHostPort();
        BDBEnvironment environment = new BDBEnvironment(
                createTmpDir(),
                "standalone",
                selfNodeHostPort,
                selfNodeHostPort,
                true);
        environment.setup(true);

        new MockUp<ReplicatedEnvironment>() {
            @Mock
            public List<String> getDatabaseNames() {
                List<String> list = new ArrayList<>();
                list.add("1001");
                list.add("2001");
                list.add("aaa_3001");
                list.add("aaa_4001");
                list.add("aaa_bbb_");
                return list;
            }
        };

        List<Long> l1 = environment.getDatabaseNamesWithPrefix("");
        Assert.assertEquals(2, l1.size());
        Assert.assertEquals((Long) 1001L, l1.get(0));
        Assert.assertEquals((Long) 2001L, l1.get(1));

        List<Long> l2 = environment.getDatabaseNamesWithPrefix("aaa_");
        Assert.assertEquals(2, l2.size());
        Assert.assertEquals((Long) 3001L, l2.get(0));
        Assert.assertEquals((Long) 4001L, l2.get(1));

        // prefix not fully match
        List<Long> l3 = environment.getDatabaseNamesWithPrefix("aaa");
        Assert.assertEquals(0, l3.size());

        // prefix not match
        List<Long> l4 = environment.getDatabaseNamesWithPrefix("bbb_");
        Assert.assertEquals(0, l4.size());

        environment.close();
        System.out.println("testGetDatabase cost " + (System.currentTimeMillis() - startMs) / 1000 + " s");
    }
}
