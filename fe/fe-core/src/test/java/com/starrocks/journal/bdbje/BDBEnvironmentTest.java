// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.journal.bdbje;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.starrocks.common.Config;
import com.starrocks.journal.JournalException;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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

    public File createTmpDir() throws Exception{
        File f = Files.createTempDirectory(Paths.get("."), "BDBEnvironmentTest").toFile();
        tmpDirs.add(f);
        return f;
    }

    @Before
    public void setup() throws Exception {
        BDBEnvironment.RETRY_TIME = 3;
        // give master time to update membership
        // otherwise may get error Conflicting node types: uses: SECONDARY Replica is configured as type: ELECTABLE
        BDBEnvironment.SLEEP_INTERVAL_SEC = 1;
        // set timeout to a really long time so that ut can pass even when IO load is very high
        Config.bdbje_heartbeat_timeout_second = 60;
        Config.bdbje_replica_ack_timeout_second = 60;
        Config.bdbje_lock_timeout_second = 60;
    }

    @After
    public void cleanup() throws Exception {
        for (File tmpDir: tmpDirs) {
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
        String selfNodeHostPort = findUnbindHostPort();
        BDBEnvironment environment = new BDBEnvironment(
                createTmpDir(),
                "standalone",
                selfNodeHostPort,
                selfNodeHostPort,
                true);
        environment.setup();

        CloseSafeDatabase db = environment.openDatabase("testdb");
        DatabaseEntry key = randomEntry();
        DatabaseEntry value = randomEntry();
        db.put(null, key, value);

        DatabaseEntry newvalue = new DatabaseEntry();
        db.get(null, key, newvalue, LockMode.READ_COMMITTED);
        Assert.assertEquals(new String(value.getData()), new String(newvalue.getData()));
        db.close();
        environment.close();
    }

    // address already in use
    @Test(expected = JournalException.class)
    public void testSetupStandaloneMultitimes() throws Exception {
        String selfNodeHostPort = findUnbindHostPort();
        for (int i = 0; i < 2; i ++) {
            BDBEnvironment environment = new BDBEnvironment(
                    createTmpDir(),
                    "standalone",
                    selfNodeHostPort,
                    selfNodeHostPort,
                    true);
            environment.setup();
        }
        Assert.fail();
    }

    /**
     * used for cluster test from here, 1 master + 2 follower + 1 observer
     */
    private String masterNodeHostPort = null;
    private File masterPath = null;
    private BDBEnvironment masterEnvironment = null;
    private String masterName;
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
        // give master time to update membership
        // otherwise may get error Conflicting node types: uses: SECONDARY Replica is configured as type: ELECTABLE
        BDBEnvironment.SLEEP_INTERVAL_SEC = ThreadLocalRandom.current().nextInt(5, 15);
        // set timeout to a really long time so that ut can pass even when IO load is very high
        Config.bdbje_heartbeat_timeout_second = 60;
        Config.bdbje_replica_ack_timeout_second = 60;
        Config.bdbje_lock_timeout_second = 60;

        // setup master
        masterNodeHostPort = findUnbindHostPort();
        masterPath = createTmpDir();
        masterName = "master";
        masterEnvironment = new BDBEnvironment(
                masterPath,
                masterName,
                masterNodeHostPort,
                masterNodeHostPort,
                true);
        masterEnvironment.setup();
        Assert.assertEquals(0, masterEnvironment.getDatabaseNames().size());

        // set up 2 followers
        for (int i = 0; i < 2; i++) {
            followerNodeHostPorts[i] = findUnbindHostPort();
            followerPaths[i] = createTmpDir();
            followerNames[i] = String.format("follower%d", i);
            BDBEnvironment followerEnvironment = new BDBEnvironment(
                    followerPaths[i],
                    followerNames[i],
                    followerNodeHostPorts[i],
                    masterNodeHostPort,
                    true);
            followerEnvironments[i] = followerEnvironment;
            followerEnvironment.setup();
            Assert.assertEquals(0, followerEnvironment.getDatabaseNames().size());
        }
    }

    @Test
    public void testNormalCluster() throws Exception {
        initClusterMasterFollower();

        // master write
        Long DB_INDEX_1 = 0L;
        String DB_NAME_1 = String.valueOf(DB_INDEX_1);
        CloseSafeDatabase masterDb = masterEnvironment.openDatabase(DB_NAME_1);
        Assert.assertEquals(1, masterEnvironment.getDatabaseNames().size());
        Assert.assertEquals(DB_INDEX_1, masterEnvironment.getDatabaseNames().get(0));
        DatabaseEntry key = randomEntry();
        DatabaseEntry value = randomEntry();
        masterDb.put(null, key, value);
        masterDb.close();

        Thread.sleep(1000);

        // follower read
        for (BDBEnvironment followerEnvironment: followerEnvironments) {
            Assert.assertEquals(1, followerEnvironment.getDatabaseNames().size());
            Assert.assertEquals(DB_INDEX_1, followerEnvironment.getDatabaseNames().get(0));

            CloseSafeDatabase followerDb = followerEnvironment.openDatabase(DB_NAME_1);
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
                masterNodeHostPort,
                false);
        observerEnvironment.setup();

        // observer read
        Assert.assertEquals(1, observerEnvironment.getDatabaseNames().size());
        Assert.assertEquals(DB_INDEX_1, observerEnvironment.getDatabaseNames().get(0));

        CloseSafeDatabase observerDb = observerEnvironment.openDatabase(DB_NAME_1);
        DatabaseEntry newvalue = new DatabaseEntry();
        observerDb.get(null, key, newvalue, LockMode.READ_COMMITTED);
        Assert.assertEquals(new String(value.getData()), new String(newvalue.getData()));
        observerDb.close();

        // close
        masterEnvironment.close();
        for (BDBEnvironment followerEnvironment: followerEnvironments) {
            followerEnvironment.close();
        }
        observerEnvironment.close();
    }

    @Test
    public void testDeleteDb() throws Exception {
        initClusterMasterFollower();

        // open n dbs and each write 1 kv
        DatabaseEntry key = randomEntry();
        DatabaseEntry value = randomEntry();
        Long [] DB_INDEX_ARR = {0L, 1L, 2L, 9L, 10L};
        String [] DB_NAME_ARR = new String[DB_INDEX_ARR.length];
        for (int i = 0; i < DB_NAME_ARR.length; ++ i) {
            DB_NAME_ARR[i] = String.valueOf(DB_INDEX_ARR[i]);

            // master write
            CloseSafeDatabase masterDb = masterEnvironment.openDatabase(DB_NAME_ARR[i]);
            Assert.assertEquals(i + 1, masterEnvironment.getDatabaseNames().size());
            Assert.assertEquals(DB_INDEX_ARR[i], masterEnvironment.getDatabaseNames().get(i));
            masterDb.put(null, key, value);
            masterDb.close();

            Thread.sleep(1000);

            // follower read
            for (BDBEnvironment followerEnvironment: followerEnvironments) {
                Assert.assertEquals(i + 1, followerEnvironment.getDatabaseNames().size());
                Assert.assertEquals(DB_INDEX_ARR[i], followerEnvironment.getDatabaseNames().get(i));

                CloseSafeDatabase followerDb = followerEnvironment.openDatabase(DB_NAME_ARR[i]);
                DatabaseEntry newvalue = new DatabaseEntry();
                followerDb.get(null, key, newvalue, LockMode.READ_COMMITTED);
                Assert.assertEquals(new String(value.getData()), new String(newvalue.getData()));
                followerDb.close();
            }
        }

        // drop first 2 dbs
        masterEnvironment.removeDatabase(DB_NAME_ARR[0]);
        masterEnvironment.removeDatabase(DB_NAME_ARR[1]);

        // check dbnames
        List<Long> expectDbNames = new ArrayList<>();
        for (int i = 2;  i != DB_NAME_ARR.length; ++ i) {
            expectDbNames.add(DB_INDEX_ARR[i]);
        }
        Assert.assertEquals(expectDbNames, masterEnvironment.getDatabaseNames());
        Thread.sleep(1000);
        // follower read
        for (BDBEnvironment followerEnvironment: followerEnvironments) {
            Assert.assertEquals(expectDbNames, followerEnvironment.getDatabaseNames());
        }

        // close
        masterEnvironment.close();
        for (BDBEnvironment followerEnvironment: followerEnvironments) {
            followerEnvironment.close();
        }
    }

    /**
     * see https://github.com/StarRocks/starrocks/issues/4977
     */
    @Test(expected = JournalException.class)
    public void testRollbackExceptionOnSetupCluster() throws Exception {
        initClusterMasterFollower();

        // master write db 0
        Long DB_INDEX_OLD = 0L;
        String DB_NAME_OLD = String.valueOf(DB_INDEX_OLD);
        CloseSafeDatabase masterDb = masterEnvironment.openDatabase(DB_NAME_OLD);
        DatabaseEntry key = randomEntry();
        DatabaseEntry value = randomEntry();
        masterDb.put(null, key, value);
        masterDb.close();
        Assert.assertEquals(1, masterEnvironment.getDatabaseNames().size());
        Assert.assertEquals(DB_INDEX_OLD, masterEnvironment.getDatabaseNames().get(0));

        Thread.sleep(1000);

        // follower read db 0
        for (BDBEnvironment followerEnvironment: followerEnvironments) {
            CloseSafeDatabase followerDb = followerEnvironment.openDatabase(DB_NAME_OLD);
            DatabaseEntry newvalue = new DatabaseEntry();
            followerDb.get(null, key, newvalue, LockMode.READ_COMMITTED);
            Assert.assertEquals(new String(value.getData()), new String(newvalue.getData()));
            Assert.assertEquals(1, followerEnvironment.getDatabaseNames().size());
            Assert.assertEquals(DB_INDEX_OLD, followerEnvironment.getDatabaseNames().get(0));
            followerDb.close();
        }

        // manually backup follower's meta dir
        for (File followerPath : followerPaths) {
            File dst = new File(followerPath.getAbsolutePath() + "_bk");
            LOG.info("backup {} to {}", followerPath, dst);
            FileUtils.copyDirectory(followerPath, dst);
        }

        // master write 2 * txn_rollback_limit lines in new db and quit
        Long DB_INDEX_NEW = 1L;
        String DB_NAME_NEW = String.valueOf(DB_INDEX_NEW);
        masterDb = masterEnvironment.openDatabase(DB_NAME_NEW);
        for (int i = 0; i < Config.txn_rollback_limit * 2; i++) {
            masterDb.put(null, randomEntry(), randomEntry());
        }
        masterDb.close();
        Assert.assertEquals(2, masterEnvironment.getDatabaseNames().size());
        Assert.assertEquals(DB_INDEX_OLD, masterEnvironment.getDatabaseNames().get(0));
        Assert.assertEquals(DB_INDEX_NEW, masterEnvironment.getDatabaseNames().get(1));

        // close all environment
        masterEnvironment.close();
        for (BDBEnvironment followerEnvironment: followerEnvironments) {
            followerEnvironment.close();
        }

        // restore follower's path
        for (File followerPath : followerPaths) {
            LOG.info("delete {} ", followerPath);
            FileUtils.deleteDirectory(followerPath);
            File src = new File(followerPath.getAbsolutePath() + "_bk");
            LOG.info("mv {} to {}", src, followerPath);
            FileUtils.moveDirectory(src, followerPath);
        }

        Thread.sleep(1000);

        // start follower
        for (int i = 0; i < 2; ++ i) {
            followerEnvironments[i] = new BDBEnvironment(
                    followerPaths[i],
                    String.format("follower%d", i),
                    followerNodeHostPorts[i],
                    followerNodeHostPorts[i],
                    true);
            followerEnvironments[i].setup();
            Assert.assertEquals(1, followerEnvironments[i].getDatabaseNames().size());
            Assert.assertEquals(DB_INDEX_OLD, followerEnvironments[i].getDatabaseNames().get(0));
        }

        Thread.sleep(1000);

        // wait for state change
        BDBEnvironment newMasterEnvironment = null;
        while (newMasterEnvironment == null) {
            Thread.sleep(1000);
            for (int i = 0; i < 2; ++ i) {
                if (followerEnvironments[i].getReplicatedEnvironment().getState() == ReplicatedEnvironment.State.MASTER) {
                    newMasterEnvironment = followerEnvironments[i];
                    LOG.warn("=========> new master is {}", newMasterEnvironment.getReplicatedEnvironment().getNodeName());
                    masterDb = newMasterEnvironment.openDatabase(DB_NAME_OLD);
                    key = randomEntry();
                    value = randomEntry();
                    masterDb.put(null, key, value);

                    Thread.sleep(1000);

                    int followerIndex = 1 - i;
                    CloseSafeDatabase followerDb = followerEnvironments[followerIndex].openDatabase(DB_NAME_OLD);
                    DatabaseEntry newvalue = new DatabaseEntry();
                    followerDb.get(null, key, newvalue, LockMode.READ_COMMITTED);
                    Assert.assertEquals(new String(value.getData()), new String(newvalue.getData()));
                    break;
                }
            }
        }

        // set retry times = 1 to ensure no recovery
        BDBEnvironment.RETRY_TIME = 1;
        // start master will get rollback exception
        BDBEnvironment maserEnvironment = new BDBEnvironment(
                masterPath,
                "master",
                masterNodeHostPort,
                masterNodeHostPort,
                true);
        Assert.assertTrue(true);
        maserEnvironment.setup();
        Assert.fail();
    }

    /**
     * simulate master failover, return the index of the instance that remains follower
     */
    private void masterFailOver() throws Exception {
        // master down
        masterEnvironment.close();
        LOG.warn("======> master env is closed");
        Thread.sleep(1000);

        // find the new master
        BDBEnvironment newMasterEnvironment = null;
        int newMasterFollowerIndex = 0;
        while (newMasterEnvironment == null) {
            Thread.sleep(1000);
            for (int i = 0; i < 2; ++ i) {
                if (followerEnvironments[i].getReplicatedEnvironment().getState() == ReplicatedEnvironment.State.MASTER) {
                    newMasterEnvironment = followerEnvironments[i];
                    LOG.warn("=========> new master is {}", newMasterEnvironment.getReplicatedEnvironment().getNodeName());
                    newMasterEnvironment.setup();
                    newMasterFollowerIndex = i;
                    break;
                }
            }
        }

        // start the old master
        BDBEnvironment oldMasterEnvironment = new BDBEnvironment(
                masterPath,
                "master",
                masterNodeHostPort,
                masterNodeHostPort,
                true);
        oldMasterEnvironment.setup();
        LOG.warn("============> old master is setup as follower");
        Thread.sleep(1000);

        masterEnvironment = newMasterEnvironment;
        masterNodeHostPort = followerNodeHostPorts[newMasterFollowerIndex];
    }

    private void printHAStatus() {
        LOG.info("---------------------");
        LOG.info("{}", masterEnvironment.getReplicatedEnvironment().getGroup().getRepGroupImpl().toString());
        RepGroupImpl imp = masterEnvironment.getReplicatedEnvironment().getGroup().getRepGroupImpl();
        LOG.info("---------------------");
    }

    @Test
    public void testAddBadFollowerNoFailover() throws Exception {
        testAddBadFollowerBase(false);
    }

    @Test
    public void testAddBadFollowerAfterFailover() throws Exception {
        testAddBadFollowerBase(true);
    }

    protected void testAddBadFollowerBase(boolean failover) throws Exception {
        initClusterMasterFollower();

        if (failover) {
            masterFailOver();
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
        // should set up successfully as a standalone master
        newfollowerEnvironment.setup();
        Thread.sleep(10000);
        newfollowerEnvironment.close();

        // 2. bad new follower start for the second time
        // helper = master
        newfollowerEnvironment = new BDBEnvironment(
                newFollowerPath,
                newFollowerName,
                newFollowerHostPort,
                masterNodeHostPort,
                true);
        LOG.warn("==========> start new follower for the second time");
        try {
            newfollowerEnvironment.setup();
        } catch (Exception e) {
            LOG.warn("===========> failed for the second time, as expect, ", e);
        }

        // 5. normally master won't down
        for (int i = 0; i < 5; ++i) {
            Thread.sleep(1000);
            LOG.warn("==============> getDatabasesNames() {}", masterEnvironment.getDatabaseNames());
        }
    }

    @Test
    public void testGetDatabase() throws Exception {
        String selfNodeHostPort = findUnbindHostPort();
        BDBEnvironment environment = new BDBEnvironment(
                createTmpDir(),
                "standalone",
                selfNodeHostPort,
                selfNodeHostPort,
                true);
        environment.setup();

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
    }
}
