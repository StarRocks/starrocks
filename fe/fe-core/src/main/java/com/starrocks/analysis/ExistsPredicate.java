// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ExistsPredicate.java

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
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.thrift.TExprNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class representing a [NOT] EXISTS predicate.
 */
// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class ExistsPredicate extends Predicate {
    private static final Logger LOG = LoggerFactory.getLogger(
            ExistsPredicate.class);
    private boolean notExists = false;

    public boolean isNotExists() {
        return notExists;
    }

    public ExistsPredicate(Subquery subquery, boolean notExists) {
        Preconditions.checkNotNull(subquery);
        children.add(subquery);
        this.notExists = notExists;
    }

    public ExistsPredicate(ExistsPredicate other) {
        super(other);
        notExists = other.notExists;
    }

    @Override
    public Expr negate() {
        return new ExistsPredicate((Subquery) getChild(0), !notExists);
    }

    @Override
    protected void toThrift(TExprNode msg) {
        // Cannot serialize a nested predicate
        Preconditions.checkState(false);
    }

    @Override
    public Expr clone() {
        return new ExistsPredicate(this);
    }

    @Override
    public String toSqlImpl() {
        StringBuilder strBuilder = new StringBuilder();
        if (notExists) {
            strBuilder.append("NOT ");

        }
        strBuilder.append("EXISTS ");
        strBuilder.append(getChild(0).toSql());
        return strBuilder.toString();
    }

    @Override
    public String toDigestImpl() {
        StringBuilder strBuilder = new StringBuilder();
        if (notExists) {
            strBuilder.append("not ");

        }
        strBuilder.append("exists ");
        strBuilder.append(getChild(0).toDigest());
        return strBuilder.toString();
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Boolean.hashCode(notExists);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return visitor.visitExistsPredicate(this, context);
    }
}

