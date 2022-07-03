// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector.hive;

import com.starrocks.catalog.HiveTable;
import com.starrocks.connector.ConnectorTableHandle;
import com.starrocks.sql.plan.HDFSScanNodePredicates;

public class HiveTableHandle implements ConnectorTableHandle {

    private HiveTable table;

    private HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();

    public HiveTableHandle(HiveTable table, HDFSScanNodePredicates scanNodePredicates) {

    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }
}
