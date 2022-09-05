// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.transaction;

import com.starrocks.common.Config;
import com.starrocks.pseudocluster.PseudoCluster;

import java.sql.SQLException;

public class ConcurrentTxnWithFailureTest extends ConcurrentTxnTest {
    @Override
    void setup() throws SQLException {
        Config.enable_new_publish_mechanism = false;
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.getBackend(10001).setWriteFailureRate(1.0f);
    }
}
