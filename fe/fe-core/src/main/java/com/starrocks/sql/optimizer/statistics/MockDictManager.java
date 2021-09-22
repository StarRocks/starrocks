// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import java.util.HashMap;

public class MockDictManager implements IDictManager {
    private static HashMap<String, Integer> mockDict = new HashMap<String, Integer>() {
        {
            put("mock", 1);
        }
    };

    private static ColumnDict columnDict = new ColumnDict(mockDict, 1);

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
