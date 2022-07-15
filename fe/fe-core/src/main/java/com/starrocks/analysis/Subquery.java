// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/Subquery.java

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

package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.AST2SQL;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.thrift.TExprNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Class representing a subquery. A Subquery consists of a QueryStmt and has
 * its own Analyzer context.
 */
public class Subquery extends Expr {
    private static final Logger LOG = LoggerFactory.getLogger(Subquery.class);

    // The QueryStmt of the subquery.
    protected QueryStmt stmt;
    // A subquery has its own analysis context
    protected Analyzer analyzer;

    // mark work way
    protected boolean useSemiAnti;

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    public QueryStmt getStatement() {
        return stmt;
    }

    public boolean isUseSemiAnti() {
        return useSemiAnti;
    }

    public void setUseSemiAnti(boolean useSemiAnti) {
        this.useSemiAnti = useSemiAnti;
    }

    @Override
    public String toSqlImpl() {
        if (stmt != null) {
            return "(" + stmt.toSql() + ")";
        } else {
            return "(" + AST2SQL.toString(queryStatement) + ")";
        }
    }

    @Override
    public String toDigestImpl() {
        return "(" + stmt.toDigest() + ")";
    }

    /**
     * C'tor that initializes a Subquery from a QueryStmt.
     */
    public Subquery(QueryStmt queryStmt) {
        super();
        Preconditions.checkNotNull(queryStmt);
        stmt = queryStmt;
        stmt.setNeedToSql(true);
    }

    /**
     * Copy c'tor.
     */
    public Subquery(Subquery other) {
        super(other);
        stmt = other.stmt.clone();
        // needToSql will affect the toSQL result, so we must clone it
        stmt.setNeedToSql(other.stmt.needToSql);
        analyzer = other.analyzer;
    }

    public Subquery(QueryStatement queryStatement) {
        super();
        this.queryStatement = queryStatement;
    }

    /**
     * Analyzes the subquery in a child analyzer.
     */
    @Override
    public void analyzeImpl(Analyzer parentAnalyzer) throws AnalysisException {
    }

    @Override
    protected boolean isConstantImpl() {
        return false;
    }

    /**
     * Check if the subquery's SelectStmt returns a single column of scalar type.
     */
    public boolean returnsScalarColumn() {
        ArrayList<Expr> stmtResultExprs = stmt.getResultExprs();
        if (stmtResultExprs.size() == 1 && stmtResultExprs.get(0).getType().isScalarType()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean isCorrelatedPredicate(List<TupleId> tupleIdList) {
        List<TupleId> tupleIdFromSubquery = stmt.collectTupleIds();
        for (TupleId tupleId : tupleIdList) {
            if (tupleIdFromSubquery.contains(tupleId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if the toSql() of the Subqueries is identical. May return false for
     * equivalent statements even due to minor syntactic differences like parenthesis.
     * TODO: Switch to a less restrictive implementation.
     */
    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }

        if (queryStatement != null) {
            if (((Subquery) o).getQueryStatement() == null) {
                return false;
            } else {
                return o.equals(queryStatement);
            }
        }

        return stmt.toSql().equals(((Subquery) o).stmt.toSql());
    }

    @Override
    public Subquery clone() {
        Subquery ret = new Subquery(this);
        LOG.debug("SUBQUERY clone old={} new={}",
                System.identityHashCode(this),
                System.identityHashCode(ret));
        return ret;
    }

    @Override
    protected void toThrift(TExprNode msg) {
    }

    /**
     * Below function is added by new analyzer
     */
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return visitor.visitSubquery(this, context);
    }

    /**
     * Analyzed subquery is store as QueryBlock
     */
    private QueryStatement queryStatement;

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }
}