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

package com.starrocks.datacache;

import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QualifiedName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class DataCacheMgrTest {
    private final DataCacheMgr dataCacheMgr = DataCacheMgr.getInstance();

    @Test
    public void testAddCacheRule() {
        QualifiedName qualifiedName = QualifiedName.of(List.of("catalog", "db", "tbl"));
        dataCacheMgr.createCacheRule(qualifiedName, null, -1, null);

        Assertions.assertFalse(dataCacheMgr.getCacheRule("*", "*", "*").isPresent());
        Assertions.assertFalse(dataCacheMgr.getCacheRule("*", "db", "*").isPresent());
        Optional<DataCacheRule> dataCacheRule = dataCacheMgr.getCacheRule("catalog", "db", "tbl");
        Assertions.assertTrue(dataCacheRule.isPresent());
        Assertions.assertEquals(-1, dataCacheRule.get().getPriority());

        QualifiedName conflictQualifiedName = QualifiedName.of(List.of("*", "*", "*"));
        Assertions.assertThrows(SemanticException.class, () -> dataCacheMgr.throwExceptionIfRuleIsConflicted("*", "*", "*"));

        dataCacheMgr.dropCacheRule(0);
        dataCacheMgr.createCacheRule(conflictQualifiedName, null, -1, null);
        dataCacheRule = dataCacheMgr.getCacheRule("a", "b", "c");
        Assertions.assertTrue(dataCacheRule.isPresent());
        Assertions.assertEquals(-1, dataCacheRule.get().getPriority());

        Assertions.assertThrows(SemanticException.class, () -> dataCacheMgr
                .throwExceptionIfRuleIsConflicted("a", "b", "c"));
    }
}
