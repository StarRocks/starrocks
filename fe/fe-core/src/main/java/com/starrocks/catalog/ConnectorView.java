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

import com.starrocks.analysis.ParseNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public abstract class ConnectorView extends Table {
    private static final Logger LOG = LogManager.getLogger(HiveView.class);

    protected final String catalogName;
    protected final String dbName;
    protected final String inlineViewDef;

    public ConnectorView(long id, String catalogName, String dbName, String name,
                         List<Column> schema, String definition, TableType tableType) {
        super(id, name, tableType, schema);
        this.catalogName = requireNonNull(catalogName, "Connector view catalog name is null");
        this.dbName = requireNonNull(dbName, "Connector view db name is null");
        this.inlineViewDef = requireNonNull(definition, "Connector view definition is null");
    }


    public QueryStatement getQueryStatement() throws StarRocksPlannerException {
        SessionVariable sessionVariable = ConnectContext.get() != null ? ConnectContext.get().getSessionVariable()
                : new SessionVariable();
        String sqlDialect = sessionVariable.getSqlDialect();
        QueryStatement queryStatement = doGetQueryStatement(sessionVariable);
        sessionVariable.setSqlDialect(sqlDialect);
        List<String> cteRelationNames = queryStatement.getQueryRelation().getCteRelations()
                .stream().map(CTERelation::getName)
                .collect(Collectors.toList());
        List<TableRelation> tableRelations = AnalyzerUtils.collectTableRelations(queryStatement);
        formatRelations(tableRelations, cteRelationNames);
        return queryStatement;
    }

    protected QueryStatement doGetQueryStatement(SessionVariable sessionVariable) {
        ParseNode node;
        try {
            node = com.starrocks.sql.parser.SqlParser.parse(inlineViewDef, sessionVariable).get(0);
        } catch (Exception e) {
            LOG.warn("stmt is {}", inlineViewDef);
            LOG.warn("exception because: ", e);
            node = rollback(sessionVariable);
            if (node == null) {
                throw new StarRocksPlannerException(
                        String.format("Failed to parse view-definition statement of view: %s", name),
                        ErrorType.INTERNAL_ERROR);
            }
        }

        // Make sure the view definition parses to a query statement.
        if (!(node instanceof QueryStatement)) {
            throw new StarRocksPlannerException(String.format("View definition of %s " +
                    "is not a query statement", name), ErrorType.INTERNAL_ERROR);
        }
        return (QueryStatement) node;
    }

    protected ParseNode rollback(SessionVariable sessionVariable) {
        return null;
    }

    protected abstract void formatRelations(List<TableRelation> tableRelations, List<String> cteRelationNames);

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    public String getCatalogName() {
        return catalogName;
    }
}
