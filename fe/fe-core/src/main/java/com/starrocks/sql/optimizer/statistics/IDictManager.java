// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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
