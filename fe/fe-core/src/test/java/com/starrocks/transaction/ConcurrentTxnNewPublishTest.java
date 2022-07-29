package com.starrocks.transaction;

import java.sql.SQLException;

public class ConcurrentTxnNewPublishTest extends ConcurrentTxnTest {
    @Override
    boolean getEnableNewPublish() throws SQLException {
        return true;
    }
}
