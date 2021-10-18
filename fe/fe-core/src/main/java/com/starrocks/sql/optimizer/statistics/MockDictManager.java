// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.ImmutableMap;

public class MockDictManager implements IDictManager {
    private static final ImmutableMap<String, Integer> mockDict = ImmutableMap.of("mock", 1);

    private static final ColumnDict columnDict = new ColumnDict(mockDict, 1);

    private MockDictManager() {
    }

    private static final MockDictManager instance = new MockDictManager();

    protected static MockDictManager getInstance() {
        return instance;
    }

    @Override
    public boolean hasGlobalDict(long tableId, String columnName, long version) {
        return true;
    }

    @Override
    public ColumnDict getGlobalDict(long tableId, String columnName) {
        return columnDict;
    }
}