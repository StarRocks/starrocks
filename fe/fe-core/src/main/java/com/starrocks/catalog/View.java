// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/View.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.ParseNode;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.List;

/**
 * Table metadata representing a globalStateMgr view or a local view from a WITH clause.
 * Most methods inherited from Table are not supposed to be called on this class because
 * views are substituted with their underlying definition during analysis of a statement.
 * <p>
 * Refreshing or invalidating a view will reload the view's definition but will not
 * affect the metadata of the underlying tables (if any).
 */
public class View extends Table {
    private static final Logger LOG = LogManager.getLogger(GlobalStateMgr.class);

    // The original SQL-string given as view definition. Set during analysis.
    // Corresponds to Hive's viewOriginalText.
    @Deprecated
    private String originalViewDef = "";

    // Query statement (as SQL string) that defines the View for view substitution.
    // It is a transformation of the original view definition, e.g., to enforce the
    // explicit column definitions even if the original view definition has explicit
    // column aliases.
    // If column definitions were given, then this "expanded" view definition
    // wraps the original view definition in a select stmt as follows.
    //
    // SELECT viewName.origCol1 AS colDesc1, viewName.origCol2 AS colDesc2, ...
    // FROM (originalViewDef) AS viewName
    //
    // Corresponds to Hive's viewExpandedText, but is not identical to the SQL
    // Hive would produce in view creation.
    private String inlineViewDef;

    // for persist
    private long sqlMode = 0L;

    // View definition created by parsing inlineViewDef_ into a QueryStmt.
    // 'queryStmt' is a strong reference, which is used when this view is created directly from a QueryStmt
    // 'queryStmtRef' is a soft reference, it is created from parsing query stmt, and it will be cleared if
    // JVM memory is not enough.
    private QueryStatement queryStmt;
    @Deprecated
    // Can't keep a cache in meta data
    private SoftReference<QueryStatement> queryStmtRef = new SoftReference<QueryStatement>(null);

    // Set if this View is from a WITH clause and not persisted in the globalStateMgr.
    private boolean isLocalView;

    // Set if this View is from a WITH clause with column labels.
    private List<String> colLabels;

    // Used for read from image
    public View() {
        super(TableType.VIEW);
        isLocalView = false;
    }

    public View(long id, String name, List<Column> schema) {
        super(id, name, TableType.VIEW, schema);
        isLocalView = false;
    }

    public View(String alias, QueryStatement queryStmt, List<String> colLabels) {
        super(-1, alias, TableType.VIEW, null);
        this.isLocalView = true;
        this.queryStmt = queryStmt;
        this.colLabels = colLabels;
    }

    public QueryStatement getQueryStatement() throws StarRocksPlannerException {
        if (queryStmt != null) {
            return queryStmt;
        }

        Preconditions.checkNotNull(inlineViewDef);
        ParseNode node;
        try {
            node = com.starrocks.sql.parser.SqlParser.parse(inlineViewDef, sqlMode).get(0);
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

    public void setInlineViewDefWithSqlMode(String inlineViewDef, long sqlMode) {
        this.inlineViewDef = inlineViewDef;
        this.sqlMode = sqlMode;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    /**
     * Initializes the originalViewDef, inlineViewDef, and queryStmt members
     * by parsing the expanded view definition SQL-string.
     * Throws a TableLoadingException if there was any error parsing the
     * the SQL or if the view definition did not parse into a QueryStmt.
     */
    public synchronized QueryStatement init() throws UserException {
        Preconditions.checkNotNull(inlineViewDef);
        // Parse the expanded view definition SQL-string into a QueryStmt and
        // populate a view definition.
        ParseNode node;
        try {
            node = com.starrocks.sql.parser.SqlParser.parse(inlineViewDef, sqlMode).get(0);
        } catch (Exception e) {
            LOG.info("stmt is {}", inlineViewDef);
            LOG.info("exception because: ", e);
            LOG.info("msg is {}", inlineViewDef);
            // Do not pass e as the exception cause because it might reveal the existence
            // of tables that the user triggering this load may not have privileges on.
            throw new UserException(
                    String.format("Failed to parse view-definition statement of view: %s", name), e);
        }
        // Make sure the view definition parses to a query statement.
        if (!(node instanceof QueryStatement)) {
            throw new UserException(String.format("View definition of %s " +
                    "is not a query statement", name));
        }
        queryStmtRef = new SoftReference<>((QueryStatement) node);
        return (QueryStatement) node;
    }

    /**
     * Returns the column labels the user specified in the WITH-clause.
     */
    public List<String> getOriginalColLabels() {
        return colLabels;
    }

    public boolean hasColLabels() {
        return colLabels != null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, originalViewDef);
        Text.writeString(out, inlineViewDef);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        // just do not want to modify the meta version, so leave originalViewDef here but set it as empty
        originalViewDef = Text.readString(in);
        originalViewDef = "";
        inlineViewDef = Text.readString(in);
        inlineViewDef = inlineViewDef.replaceAll("default_cluster:", "");
    }
}
