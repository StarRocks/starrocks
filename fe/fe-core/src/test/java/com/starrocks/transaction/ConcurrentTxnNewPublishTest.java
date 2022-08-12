package com.starrocks.transaction;

import com.starrocks.common.Config;

import java.sql.SQLException;

public class ConcurrentTxnNewPublishTest extends ConcurrentTxnTest {
    @Override
    void setup() throws SQLException {
        numDB = 1;
        runTime = 20;
        withRead = true;
        Config.enable_new_publish_mechanism = true;
    }
}
