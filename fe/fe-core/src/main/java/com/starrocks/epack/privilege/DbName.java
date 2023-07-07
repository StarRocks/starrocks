// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

public class DbName {
    private final String catalog;
    private final String db;

    public DbName(String catalog, String db) {
        this.catalog = catalog;
        this.db = db;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getDb() {
        return db;
    }
}
