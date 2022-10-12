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

import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.AST2SQL;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.thrift.TExprNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class representing a subquery. A Subquery consists of a QueryStmt and has
 * its own Analyzer context.
 */
public class Subquery extends Expr {
    private static final Logger LOG = LoggerFactory.getLogger(Subquery.class);

    // mark work way
    protected boolean useSemiAnti;

    private final QueryStatement queryStatement;


    public boolean isUseSemiAnti() {
        return useSemiAnti;
    }

    public void setUseSemiAnti(boolean useSemiAnti) {
        this.useSemiAnti = useSemiAnti;
    }

    public Subquery(QueryStatement queryStatement) {
        super();
        this.queryStatement = queryStatement;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    @Override
    protected boolean isConstantImpl() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }

        if (((Subquery) o).getQueryStatement() == null) {
            return false;
        } else {
            return o.equals(queryStatement);
        }
    }

    @Override
    public Subquery clone() {
        Subquery ret = new Subquery(queryStatement);
        LOG.debug("SUBQUERY clone old={} new={}",
                System.identityHashCode(this),
                System.identityHashCode(ret));
        return ret;
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {

    }

    @Override
    public String toSqlImpl() {
        return "(" + AST2SQL.toString(queryStatement) + ")";
    }

    @Override
    protected void toThrift(TExprNode msg) {
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return visitor.visitSubquery(this, context);
    }
}