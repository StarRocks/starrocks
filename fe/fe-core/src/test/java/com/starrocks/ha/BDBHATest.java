package com.starrocks.ha;

import com.starrocks.catalog.Catalog;
import java.net.InetSocketAddress;
import java.util.Set;

import com.google.common.collect.Sets;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.system.Frontend;
import com.starrocks.system.FrontendHbResponse;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class BDBHATest {

    @BeforeClass
    public static void beforeClass() {
        UtFrameUtils.createMinStarRocksCluster(true);
    }

    @Test
    public void testAddAndRemoveUnstableNode() {
        BDBJEJournal journal = (BDBJEJournal) Catalog.getCurrentCatalog().getEditLog().getJournal();
        BDBEnvironment environment = journal.getBdbEnvironment();

        BDBHA ha = (BDBHA) Catalog.getCurrentCatalog().getHaProtocol();
        ha.addUnstableNode("host1", 3);
        Assert.assertEquals(2,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

        ha.addUnstableNode("host2", 4);
        Assert.assertEquals(2,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

        ha.removeUnstableNode("host1", 4);
        Assert.assertEquals(3,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

        ha.removeUnstableNode("host2", 4);
        Assert.assertEquals(0,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());
    }

    @Test
    public void testAddAndDropFollower() throws Exception {
        BDBJEJournal journal = (BDBJEJournal) Catalog.getCurrentCatalog().getEditLog().getJournal();
        BDBEnvironment environment = journal.getBdbEnvironment();

        // add two followers
        Catalog.getCurrentCatalog().addFrontend(FrontendNodeType.FOLLOWER, "host1", 9010);
        Assert.assertEquals(1,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());
        Catalog.getCurrentCatalog().addFrontend(FrontendNodeType.FOLLOWER, "host2", 9010);
        Assert.assertEquals(1,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

        Set<InetSocketAddress> helperSocketsBefore = Sets.newHashSet(environment.getReplicationGroupAdmin().getHelperSockets());
        InetSocketAddress targetAddress = new InetSocketAddress("host1", 9010);
        Assert.assertTrue(helperSocketsBefore.contains(targetAddress));        


        // one joined successfully
        new Frontend(FrontendNodeType.FOLLOWER, "node1", "host2", 9010)
                .handleHbResponse(new FrontendHbResponse("n1", 8030, 9050,
                        1000, System.currentTimeMillis(), System.currentTimeMillis(), "v1"),
                        false);
        Assert.assertEquals(2,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

        // the other one is dropped
        Catalog.getCurrentCatalog().dropFrontend(FrontendNodeType.FOLLOWER, "host1", 9010);

        Set<InetSocketAddress> helperSocketsAfter = Sets.newHashSet(environment.getReplicationGroupAdmin().getHelperSockets());
        Assert.assertTrue(!helperSocketsAfter.contains(targetAddress));

        Assert.assertEquals(0,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());
    }
}