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
import org.elasticsearch.common.collect.List;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

public class DatacacheMgrTest {
    private final DatacacheMgr datacacheMgr = DatacacheMgr.getInstance();

    @Test
    public void testAddCacheRule() {
        QualifiedName qualifiedName = QualifiedName.of(List.of("catalog", "db", "tbl"));
        datacacheMgr.createCacheRule(qualifiedName, null, -1, null);

        Assert.assertFalse(datacacheMgr.getCacheRule("*", "*", "*").isPresent());
        Assert.assertFalse(datacacheMgr.getCacheRule("*", "db", "*").isPresent());
        Optional<DatacacheRule> dataCacheRule = datacacheMgr.getCacheRule("catalog", "db", "tbl");
        Assert.assertTrue(dataCacheRule.isPresent());
        Assert.assertEquals(-1, dataCacheRule.get().getPriority());

        QualifiedName conflictQualifiedName = QualifiedName.of(List.of("*", "*", "*"));
        Assert.assertThrows(SemanticException.class, () -> datacacheMgr.throwExceptionIfRuleIsConflicted("*", "*", "*"));

        datacacheMgr.dropCacheRule(0);
        datacacheMgr.createCacheRule(conflictQualifiedName, null, -1, null);
        dataCacheRule = datacacheMgr.getCacheRule("a", "b", "c");
        Assert.assertTrue(dataCacheRule.isPresent());
        Assert.assertEquals(-1, dataCacheRule.get().getPriority());

        Assert.assertThrows(SemanticException.class, () -> datacacheMgr
                .throwExceptionIfRuleIsConflicted("a", "b", "c"));
    }
}
