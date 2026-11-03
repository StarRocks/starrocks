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

import com.google.common.base.Objects;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.ViewRelation;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * The {@link View} the analyzer builds for a {@link ConnectorView}.
 *
 * <p>Connectors may return a new view object, with a new id from {@code CONNECTOR_ID_GENERATOR}, on every lookup,
 * while the optimizer matches tables by equality, e.g. an MV's cached plan against a later query in view-based MV
 * rewrite. So, like the connector tables ({@link HiveTable}, {@link IcebergTable}, ...), it is equal by name instead
 * of id: catalog, plus db and view name case-insensitively, as connector metastores resolve them.
 *
 * <p>The definition is part of equality too. Logical view scans also snapshot the definitions of all views referenced
 * by the analyzed expansion, so changing an inner view cannot match a cached outer view with unchanged SQL.
 * Nothing inactivates an MV when a connector view is redefined, so the MV's cached plan may hold an older version
 * of the view, which then must not match queries on the new one. This only
 * covers the cached plan: once it is rebuilt (FE restart, cache eviction), it is built from the new definition while
 * the MV may still hold data for the old one, until the MV is refreshed.
 *
 * <p>Built per analysis and never persisted, but an MV's cached plan keeps the one built for the MV's query.
 */
public class AnalyzedConnectorView extends View {
    private final String catalogName;
    // lower-cased once here, as equals and hashCode run in the optimizer's table matching
    private final String lowerDbName;
    private final String lowerName;
    private final Set<ViewDefinition> referencedViewDefinitions;

    private record ViewDefinition(String catalogName, String dbName, String name, Long nativeId, String sql) {
        private static ViewDefinition snapshot(View view) {
            if (view instanceof AnalyzedConnectorView connectorView) {
                return new ViewDefinition(connectorView.catalogName, connectorView.lowerDbName,
                        connectorView.lowerName, null, connectorView.getInlineViewDef());
            }
            // Native views retain their catalog table id, but their mutable definition must be copied.
            return new ViewDefinition(null, null, null, view.getId(), view.getInlineViewDef());
        }
    }

    public AnalyzedConnectorView(ConnectorView connectorView) {
        super(connectorView.getId(), connectorView.getName(), connectorView.getFullSchema(), connectorView.getType());
        this.catalogName = connectorView.catalogName;
        this.lowerDbName = connectorView.dbName.toLowerCase(Locale.ROOT);
        this.lowerName = connectorView.getName().toLowerCase(Locale.ROOT);
        this.referencedViewDefinitions = Set.of();
        setInlineViewDefWithSqlMode(connectorView.getInlineViewDef(), 0);
        // view-based MV rewrite only triggers for views that have related MVs
        connectorView.getRelatedMaterializedViews().forEach(this::addRelatedMaterializedView);
    }

    private AnalyzedConnectorView(AnalyzedConnectorView view, Set<ViewDefinition> referencedViewDefinitions) {
        super(view.getId(), view.getName(), view.getFullSchema(), view.getType());
        this.catalogName = view.catalogName;
        this.lowerDbName = view.lowerDbName;
        this.lowerName = view.lowerName;
        this.referencedViewDefinitions = Set.copyOf(referencedViewDefinitions);
        setInlineViewDefWithSqlMode(view.getInlineViewDef(), view.getSqlMode());
        view.getRelatedMaterializedViews().forEach(this::addRelatedMaterializedView);
    }

    public AnalyzedConnectorView withReferencedViewDefinitions(QueryStatement expandedQuery) {
        Set<ViewDefinition> definitions = new HashSet<>();
        new AstTraverser<Void, Void>() {
            @Override
            public Void visitView(ViewRelation node, Void context) {
                definitions.add(ViewDefinition.snapshot(node.getView()));
                return super.visitView(node, context);
            }
        }.visit(expandedQuery);
        // Construct a separate immutable key; the analyzer's view may already be used in maps and sets.
        return new AnalyzedConnectorView(this, definitions);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(catalogName, lowerDbName, lowerName, getInlineViewDef(), referencedViewDefinitions);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof AnalyzedConnectorView)) {
            return false;
        }
        AnalyzedConnectorView otherView = (AnalyzedConnectorView) other;
        return Objects.equal(catalogName, otherView.catalogName) &&
                lowerDbName.equals(otherView.lowerDbName) &&
                lowerName.equals(otherView.lowerName) &&
                Objects.equal(getInlineViewDef(), otherView.getInlineViewDef()) &&
                referencedViewDefinitions.equals(otherView.referencedViewDefinitions);
    }
}
