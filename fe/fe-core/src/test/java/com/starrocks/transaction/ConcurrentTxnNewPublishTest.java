package com.starrocks.transaction;

import com.starrocks.common.Config;

import java.sql.SQLException;

public class ConcurrentTxnNewPublishTest extends ConcurrentTxnTest {
    @Override
    void setup() throws SQLException {
        int runTime = 30;
        int numDB = 20;
        int numTable = 1000;
        int numThread = 20;
        int runSeconds = 2;
        withRead = true;
        Config.enable_new_publish_mechanism = true;
    }
}
