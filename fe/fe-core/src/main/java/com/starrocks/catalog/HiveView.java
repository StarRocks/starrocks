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
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class HiveView extends Table {
    private static final Logger LOG = LogManager.getLogger(HiveView.class);

    public enum Type {
        Trino,
        Hive
    }

    private final String catalogName;
    private final String dbName;
    private final long createTime;

    private final String inlineViewDef;
    private final Type viewType;

    public static final String PRESTO_VIEW_PREFIX = "/* Presto View: ";
    public static final String PRESTO_VIEW_SUFFIX = " */";

    public HiveView(long id, String catalogName, String dbName, String name, List<Column> schema, long createTime,
                    String definition, Type type) {
        super(id, name, TableType.HIVE_VIEW, schema);
        this.catalogName = requireNonNull(catalogName, "Hive view catalog name is null");
        this.dbName = requireNonNull(dbName, "Hive view db name is null");
        this.createTime = createTime;
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
            tableRelation.getName().setCatalog(catalogName);
        }
        return queryStatement;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getUUID() {
        return String.join(".", catalogName, dbName, name);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long id;
        private String catalogName;
        private String dbName;
        private String viewName;
        private long createTime;
        private List<Column> schema;
        private String definition;
        private Type type;

        public Builder setId(long id) {
            this.id = id;
            return this;
        }

        public Builder setCatalogName(String catalogName) {
            this.catalogName = catalogName;
            return this;
        }

        public Builder setDbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public Builder setViewName(String viewName) {
            this.viewName = viewName;
            return this;
        }

        public Builder setCreateTime(long createTime) {
            this.createTime = createTime;
            return this;
        }

        public Builder setSchema(List<Column> schema) {
            this.schema = schema;
            return this;
        }

        public Builder setDefinition(String definition) {
            this.definition = definition;
            return this;
        }

        public Builder setType(Type type) {
            this.type = type;
            return this;
        }

        public HiveView build() {
            return new HiveView(id, catalogName, dbName, viewName, schema, createTime, definition, type);
        }
    }
}
