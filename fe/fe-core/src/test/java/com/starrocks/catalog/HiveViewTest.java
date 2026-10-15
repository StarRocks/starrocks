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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.sql.ast.TableRelation;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class HiveViewTest {
    @Test
    public void testQualifiesRelationsAfterCteReference() {
        // A leading CTE reference must not short-circuit qualification of the remaining real tables
        HiveView view = new HiveView(1L, "hive_catalog", "sample_db", "test_view",
                Collections.emptyList(), "SELECT * FROM t", HiveView.Type.Hive);
        TableRelation cteRelation = new TableRelation(new TableName(null, null, "cte"));
        TableRelation tableRelation = new TableRelation(new TableName(null, null, "t"));
        List<TableRelation> relations = Lists.newArrayList(cteRelation, tableRelation);

        view.formatRelations(relations, Lists.newArrayList("cte"));

        assertNull(cteRelation.getName().getCatalog());
        assertEquals("hive_catalog", tableRelation.getName().getCatalog());
        assertEquals("sample_db", tableRelation.getName().getDb());
    }
}
