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

import com.starrocks.thrift.TStatisticData;

import java.util.Optional;

// different from IDictManager, IRelaxDictManager is used for table on lake,
// the dict is relaxed on value set's coverage, and will retry then supplement
// when there is new value that not in the dict.
public interface IRelaxDictManager {
    boolean hasGlobalDict(String tableUUID, String columnName);
    Optional<ColumnDict> getGlobalDict(String tableUUID, String columnName);
    void updateGlobalDict(String tableUUID, String columnName, Optional<TStatisticData> stat);
    void removeGlobalDict(String tableUUID, String columnName);
    void invalidTemporarily(String tableUUID, String columnName);
    void removeTemporaryInvalid(String tableUUID, String columnName);

    static IRelaxDictManager getInstance() {
        return CacheRelaxDictManager.getInstance();
    }
}
