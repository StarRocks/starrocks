// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.alter;

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.pseudocluster.PseudoCluster;
import org.junit.BeforeClass;

public class PseudoClusterAlterNewPublishTest extends PseudoClusterAlterTest {
    @BeforeClass
    public static void setUp() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10000;
        System.out.println("enable new publish for PseudoClusterAlterNewPublishTest");
        Config.enable_new_publish_mechanism = true;
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        PseudoCluster.getInstance().runSql(null, "create database test");
    }

}
