// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.utframe;

import com.starrocks.catalog.Catalog;

public class MockCatalog extends Catalog {
    public MockCatalog() {
        super();
    }

    protected void startMasterOnlyDaemonThreads() {
        // Alter
        getAlterInstance().start();
    }

    protected void startNonMasterDaemonThreads() {
    }
}
