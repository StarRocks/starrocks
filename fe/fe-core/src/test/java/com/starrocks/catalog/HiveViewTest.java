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

    @Test
    public void testKeepsExplicitCatalog() {
        // A Trino view can reference a table in another catalog, e.g. a view stored in a Hive catalog
        // with the body: SELECT * FROM iceberg_catalog.other_db.t
        HiveView view = trinoView("SELECT * FROM iceberg_catalog.other_db.t", "iceberg_catalog");
        TableRelation relation = new TableRelation(new TableName("iceberg_catalog", "other_db", "t"));

        view.formatRelations(Lists.newArrayList(relation), Collections.emptyList());

        assertEquals("iceberg_catalog", relation.getName().getCatalog());
        assertEquals("other_db", relation.getName().getDb());
    }

    @Test
    public void testRebindsCatalogUnknownToStarRocks() {
        // The source engine may name the view's own catalog differently, e.g. Trino calls it "hive"
        // while StarRocks registers the same metastore as "hive_catalog".
        HiveView view = trinoView("SELECT * FROM hive.other_db.t", "iceberg_catalog");
        TableRelation relation = new TableRelation(new TableName("hive", "other_db", "t"));

        view.formatRelations(Lists.newArrayList(relation), Collections.emptyList());

        assertEquals("hive_catalog", relation.getName().getCatalog());
        assertEquals("other_db", relation.getName().getDb());
    }

    @Test
    public void testFillsOwnCatalogWhenCatalogOmitted() {
        HiveView view = new HiveView(1L, "hive_catalog", "sample_db", "test_view",
                Collections.emptyList(), "SELECT * FROM other_db.t", HiveView.Type.Trino);
        TableRelation relation = new TableRelation(new TableName(null, "other_db", "t"));

        view.formatRelations(Lists.newArrayList(relation), Collections.emptyList());

        assertEquals("hive_catalog", relation.getName().getCatalog());
        assertEquals("other_db", relation.getName().getDb());
    }

    private static HiveView trinoView(String definition, String knownCatalog) {
        return new HiveView(1L, "hive_catalog", "sample_db", "test_view",
                Collections.emptyList(), definition, HiveView.Type.Trino) {
            @Override
            protected boolean catalogExists(String catalog) {
                return catalog.equals(knownCatalog) || catalog.equals("hive_catalog");
            }
        };
    }
}
