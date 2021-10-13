// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.common.FeConstants;

public interface IDictManager {
    boolean hasGlobalDict(long tableId, String columnName, long version);

    // You should call `hasGlobalDict` firstly to ensure the global dict exist
    ColumnDict getGlobalDict(long tableId, String columnName);

    static IDictManager getInstance() {
        if (FeConstants.USE_MOCK_DICT_MANAGER) {
            return MockDictManager.getInstance();
        } else {
            return CacheDictManager.getInstance();
        }
    }
}
