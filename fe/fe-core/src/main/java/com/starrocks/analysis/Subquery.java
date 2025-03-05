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

import com.google.common.base.Objects;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.parser.NodePosition;
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
        this(queryStatement, queryStatement.getPos());
    }

    public Subquery(QueryStatement queryStatement, NodePosition pos) {
        super(pos);
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
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), queryStatement);
    }

    @Override
    public boolean equalsWithoutChild(Object o) {
        if (!super.equalsWithoutChild(o)) {
            return false;
        }

        if (((Subquery) o).getQueryStatement() == null) {
            return false;
        } else {
            return ((Subquery) o).getQueryStatement().equals(queryStatement);
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
        return "(" + AstToStringBuilder.toString(queryStatement) + ")";
    }

    @Override
    protected void toThrift(TExprNode msg) {
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return visitor.visitSubqueryExpr(this, context);
    }
}