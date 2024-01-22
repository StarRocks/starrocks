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

import com.google.common.base.Preconditions;
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

import java.util.List;

import static java.util.Objects.requireNonNull;

public class HiveView extends Table {
    private static final Logger LOG = LogManager.getLogger(HiveView.class);
    private final String catalogName;
    private final String dbName;
    private final String inlineViewDef;

    public static final String PRESTO_VIEW_PREFIX = "/* Presto View: ";
    public static final String PRESTO_VIEW_SUFFIX = " */";

<<<<<<< HEAD
    public HiveView(long id, String catalogName, String name, List<Column> schema, String definition) {
=======
    public HiveView(long id, String catalogName, String dbName, String name, List<Column> schema, String definition,
                    Type type) {
>>>>>>> 746278f904 ([BugFix] Fix query trino view which not no contains db name failed (#39606))
        super(id, name, TableType.HIVE_VIEW, schema);
        this.catalogName = requireNonNull(catalogName, "Hive view catalog name is null");
        this.dbName = requireNonNull(dbName, "Hive view db name is null");
        this.inlineViewDef = requireNonNull(definition, "Hive view text is null");
    }

    public QueryStatement getQueryStatementWithSRParser() throws StarRocksPlannerException {
        Preconditions.checkNotNull(inlineViewDef);
        ParseNode node;
        SessionVariable sessionVariable = ConnectContext.get() != null ? ConnectContext.get().getSessionVariable()
                : new SessionVariable();
        try {
            node = com.starrocks.sql.parser.SqlParser.parse(inlineViewDef, sessionVariable).get(0);
        } catch (Exception e) {
            LOG.warn("stmt is {}", inlineViewDef);
            LOG.warn("exception because: ", e);
            throw new StarRocksPlannerException(
                    String.format("Failed to parse view-definition statement of view: %s", name),
                    ErrorType.INTERNAL_ERROR);
        }
        // Make sure the view definition parses to a query statement.
        if (!(node instanceof QueryStatement)) {
            throw new StarRocksPlannerException(String.format("View definition of %s " +
                    "is not a query statement", name), ErrorType.INTERNAL_ERROR);
        }

        return (QueryStatement) node;
    }

    public QueryStatement getQueryStatement() throws StarRocksPlannerException {
        QueryStatement queryStatement = getQueryStatementWithSRParser();
        List<TableRelation> tableRelations = AnalyzerUtils.collectTableRelations(queryStatement);
        for (TableRelation tableRelation : tableRelations) {
            tableRelation.getName().setCatalog(catalogName);
            if (Strings.isNullOrEmpty(tableRelation.getName().getDb())) {
                tableRelation.getName().setDb(dbName);
            }
        }
        return queryStatement;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    public String getCatalogName() {
        return catalogName;
    }
}
