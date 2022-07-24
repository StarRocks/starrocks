// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.pseudocluster;

import com.starrocks.server.GlobalStateMgr;
import org.junit.Test;

public class PseudoClusterTest {
    @Test
    public void testStartCluster() throws Exception {
        PseudoCluster cluster = PseudoCluster.getOrCreate("pseudo_cluster", 3);
        for (int i = 0; i < 3; i++) {
            System.out.println(GlobalStateMgr.getCurrentSystemInfo().getBackend(10001 + i).getBePort());
        }
    }
}
