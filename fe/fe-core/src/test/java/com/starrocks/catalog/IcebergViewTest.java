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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class IcebergViewTest {

    private IcebergView newView(String defaultCatalogName, String defaultDbName) {
        return new IcebergView(1L, "iceberg_catalog", "sales", "test_view",
                Collections.emptyList(), "ignored", defaultCatalogName, defaultDbName,
                "hdfs://path/to/view", null);
    }

    @Test
    public void testFallsBackToOwnCatalogWhenDefaultCatalogAbsent() {
        // The Iceberg view spec makes default-catalog optional: "When default-catalog is null or
        // not set, the catalog in which the view is stored must be used as the default catalog."
        // Resolution must not depend on the session's current catalog.
        IcebergView view = newView(null, "sales");
        TableRelation relation = new TableRelation(new TableName(null, "sales", "orders"));

        view.formatRelations(Lists.newArrayList(relation), new ArrayList<>());

        assertEquals("iceberg_catalog", relation.getName().getCatalog());
        assertEquals("sales", relation.getName().getDb());
    }

    @Test
    public void testSingleIdentifierUsesDefaultNamespace() {
        IcebergView view = newView("catalog_from_metadata", "ns_from_metadata");
        TableRelation relation = new TableRelation(new TableName(null, null, "orders"));

        view.formatRelations(Lists.newArrayList(relation), new ArrayList<>());

        assertEquals("catalog_from_metadata", relation.getName().getCatalog());
        assertEquals("ns_from_metadata", relation.getName().getDb());
    }

    @Test
    public void testSkipsCteRelation() {
        IcebergView view = newView(null, "sales");
        TableRelation relation = new TableRelation(new TableName(null, null, "cte"));

        view.formatRelations(Lists.newArrayList(relation), Lists.newArrayList("cte"));

        assertNull(relation.getName().getCatalog());
        assertNull(relation.getName().getDb());
    }

    @Test
    public void testQualifiesRelationsAfterCteReference() {
        // A leading CTE reference must not short-circuit qualification of the remaining real
        // tables, e.g. a view body like: WITH cte AS (SELECT 1) SELECT * FROM cte, orders
        IcebergView view = newView(null, "sales");
        TableRelation cteRelation = new TableRelation(new TableName(null, null, "cte"));
        TableRelation tableRelation = new TableRelation(new TableName(null, null, "orders"));
        List<TableRelation> relations = Lists.newArrayList(cteRelation, tableRelation);

        view.formatRelations(relations, Lists.newArrayList("cte"));

        assertNull(cteRelation.getName().getCatalog());
        assertNull(cteRelation.getName().getDb());
        assertEquals("iceberg_catalog", tableRelation.getName().getCatalog());
        assertEquals("sales", tableRelation.getName().getDb());
    }
}
