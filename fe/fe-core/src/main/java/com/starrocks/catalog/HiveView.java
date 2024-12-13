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

<<<<<<< HEAD
import com.starrocks.analysis.ParseNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;
=======
import com.google.common.base.Strings;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.TableName;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.common.StarRocksPlannerException;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

import java.util.List;

import static java.util.Objects.requireNonNull;

<<<<<<< HEAD
public class HiveView extends Table {
    private static final Logger LOG = LogManager.getLogger(HiveView.class);

=======
public class HiveView extends ConnectorView {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public enum Type {
        Trino,
        Hive
    }

<<<<<<< HEAD
    private final String catalogName;
    private final String dbName;
    private final String inlineViewDef;
    private final Type viewType;

    public static final String PRESTO_VIEW_PREFIX = "/* Presto View: ";
    public static final String PRESTO_VIEW_SUFFIX = " */";

    public HiveView(long id, String catalogName, String dbName, String name, List<Column> schema, String definition,
                    Type type) {
        super(id, name, TableType.HIVE_VIEW, schema);
        this.catalogName = requireNonNull(catalogName, "Hive view catalog name is null");
        this.dbName = requireNonNull(dbName, "Hive view db name is null");
        this.inlineViewDef = requireNonNull(definition, "Hive view text is null");
        this.viewType = requireNonNull(type, "Hive view type is null");
    }

    public QueryStatement getQueryStatementWithTrinoParser(SessionVariable sessionVariable)
            throws StarRocksPlannerException {
        sessionVariable.setSqlDialect("trino");
        return getQueryStatementImpl(sessionVariable);
    }

    public QueryStatement getQueryStatementWithDefaultParser(SessionVariable sessionVariable)
            throws StarRocksPlannerException {
        return getQueryStatementImpl(sessionVariable);
    }

    private QueryStatement getQueryStatementImpl(SessionVariable sessionVariable) {
        ParseNode node;
        try {
            node = com.starrocks.sql.parser.SqlParser.parse(inlineViewDef, sessionVariable).get(0);
        } catch (Exception e) {
            LOG.warn("stmt is {}", inlineViewDef);
            LOG.warn("exception because: ", e);
            if (viewType == Type.Trino) {
                // try to parse with starrocks sql dialect
                sessionVariable.setSqlDialect("starrocks");
                node = com.starrocks.sql.parser.SqlParser.parse(inlineViewDef, sessionVariable).get(0);
            } else {
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

    public QueryStatement getQueryStatement() throws StarRocksPlannerException {
        SessionVariable sessionVariable = ConnectContext.get() != null ? ConnectContext.get().getSessionVariable()
                : new SessionVariable();
        String sqlDialect = sessionVariable.getSqlDialect();
        QueryStatement queryStatement;
        if (viewType == Type.Trino) {
            queryStatement = getQueryStatementWithTrinoParser(sessionVariable);
        } else {
            queryStatement = getQueryStatementWithDefaultParser(sessionVariable);
        }
        sessionVariable.setSqlDialect(sqlDialect);

        List<TableRelation> tableRelations = AnalyzerUtils.collectTableRelations(queryStatement);
        for (TableRelation tableRelation : tableRelations) {
=======
    public static final String PRESTO_VIEW_PREFIX = "/* Presto View: ";
    public static final String PRESTO_VIEW_SUFFIX = " */";
    private final HiveView.Type viewType;

    public HiveView(long id, String catalogName, String dbName, String name, List<Column> schema, String definition, Type type) {
        super(id, catalogName, dbName, name, schema, definition, TableType.HIVE_VIEW);
        this.viewType = requireNonNull(type, "Hive view type is null");
    }

    @Override
    public QueryStatement doGetQueryStatement(SessionVariable sessionVariable) throws StarRocksPlannerException {
        if (viewType == HiveView.Type.Trino) {
            sessionVariable.setSqlDialect("trino");
        }

        return super.doGetQueryStatement(sessionVariable);
    }

    public ParseNode rollback(SessionVariable sessionVariable) {
        if (viewType == Type.Trino) {
            // try to parse with starrocks sql dialect
            sessionVariable.setSqlDialect("starrocks");
            return com.starrocks.sql.parser.SqlParser.parse(inlineViewDef, sessionVariable).get(0);
        } else {
            return super.rollback(sessionVariable);
        }
    }

    public void formatRelations(List<TableRelation> tableRelations, List<String> cteRelationNames) {
        for (TableRelation tableRelation : tableRelations) {
            TableName name = tableRelation.getName();

            // do not fill catalog and database name to cte relation
            if (Strings.isNullOrEmpty(name.getCatalog()) &&
                    Strings.isNullOrEmpty(name.getDb()) &&
                    cteRelationNames.contains(name.getTbl())) {
                return;
            }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            tableRelation.getName().setCatalog(catalogName);
            if (Strings.isNullOrEmpty(tableRelation.getName().getDb())) {
                tableRelation.getName().setDb(dbName);
            }
        }
<<<<<<< HEAD
        return queryStatement;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    public String getCatalogName() {
        return catalogName;
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }
}
