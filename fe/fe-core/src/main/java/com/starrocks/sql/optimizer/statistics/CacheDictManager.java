// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

public class CacheDictManager implements IDictManager {
    private CacheDictManager() {
    }
    private static final CacheDictManager instance = new CacheDictManager();
    protected static CacheDictManager getInstance() {
        return instance;
    }

    public static final Integer LOW_CARDINALITY_THRESHOLD = 256;

    @Override
    public boolean hasGlobalDict(long tableId, String columnName, long version) {
        return false;
    }

    @Override
    public ColumnDict getGlobalDict(long tableId, String columnName) {
        return null;
    }
}
