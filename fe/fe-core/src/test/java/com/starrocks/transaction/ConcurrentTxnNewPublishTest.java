package com.starrocks.transaction;

import com.starrocks.common.Config;

import java.sql.SQLException;

public class ConcurrentTxnNewPublishTest extends ConcurrentTxnTest {
    @Override
    void setup() throws SQLException {
        Config.enable_new_publish_mechanism = false;
    }
}
