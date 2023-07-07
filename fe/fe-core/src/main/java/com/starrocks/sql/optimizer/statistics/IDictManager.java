// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.common.FeConstants;

import java.util.Optional;

public interface IDictManager {
    boolean hasGlobalDict(long tableId, String columnName, long versionTime);

    void updateGlobalDict(long tableId, String columnName, long collectedVersion, long versionTime);

    boolean hasGlobalDict(long tableId, String columnName);

    void removeGlobalDict(long tableId, String columnName);

    void disableGlobalDict(long tableId);

    void enableGlobalDict(long tableId);

    // You should call `hasGlobalDict` firstly to ensure the global dict exist
    Optional<ColumnDict> getGlobalDict(long tableId, String columnName);

    static IDictManager getInstance() {
        if (FeConstants.USE_MOCK_DICT_MANAGER) {
            return MockDictManager.getInstance();
        } else {
            return CacheDictManager.getInstance();
        }
    }
}
