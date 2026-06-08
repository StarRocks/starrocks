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
import com.starrocks.sql.ast.TableRelation;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class PaimonViewTest {

    @Test
    public void testFormatRelationsSetsExternalCatalog() {
        // A view in paimon_catalog.sample_db should set table references to paimon_catalog
        PaimonView view = new PaimonView(1L, "paimon_catalog", "sample_db", "users_view",
                Collections.emptyList(), "SELECT * FROM users");

        TableName tableName = new TableName(null, null, "users");
        TableRelation relation = new TableRelation(tableName);
        List<TableRelation> relations = Lists.newArrayList(relation);

        view.formatRelations(relations, new ArrayList<>());

        assertEquals("paimon_catalog", relation.getName().getCatalog());
        assertEquals("sample_db", relation.getName().getDb());
    }

    @Test
    public void testFormatRelationsSkipsCteRelation() {
        // CTE relations should not be decorated with catalog/db
        PaimonView view = new PaimonView(1L, "paimon_catalog", "sample_db", "my_view",
                Collections.emptyList(), "WITH cte AS (SELECT 1) SELECT * FROM cte");

        TableName tableName = new TableName(null, null, "cte");
        TableRelation relation = new TableRelation(tableName);
        List<TableRelation> relations = Lists.newArrayList(relation);
        List<String> cteNames = Lists.newArrayList("cte");

        view.formatRelations(relations, cteNames);

        // CTE relation should remain undecorated
        assertNull(relation.getName().getCatalog());
        assertNull(relation.getName().getDb());
    }

    @Test
    public void testFormatRelationsPreservesExistingCatalog() {
        // Table references that already have a catalog set should keep it
        PaimonView view = new PaimonView(1L, "paimon_catalog", "sample_db", "cross_view",
                Collections.emptyList(), "SELECT * FROM other_catalog.other_db.t");

        TableName tableName = new TableName("other_catalog", "other_db", "t");
        TableRelation relation = new TableRelation(tableName);
        List<TableRelation> relations = Lists.newArrayList(relation);

        view.formatRelations(relations, new ArrayList<>());

        assertEquals("other_catalog", relation.getName().getCatalog());
        assertEquals("other_db", relation.getName().getDb());
    }

    @Test
    public void testFormatRelationsWithDbButNoCatalog() {
        // Table reference with db but no catalog should get paimon catalog
        PaimonView view = new PaimonView(1L, "paimon_catalog", "sample_db", "my_view",
                Collections.emptyList(), "SELECT * FROM other_db.t");

        TableName tableName = new TableName(null, "other_db", "t");
        TableRelation relation = new TableRelation(tableName);
        List<TableRelation> relations = Lists.newArrayList(relation);

        view.formatRelations(relations, new ArrayList<>());

        assertEquals("paimon_catalog", relation.getName().getCatalog());
        assertEquals("other_db", relation.getName().getDb());
    }
}
