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

package com.starrocks.sql.ast.expression;

import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;

/**
 * LargeInPredicate is a special optimization for queries with extremely large IN constant lists.
 * 
 * <p>Problem: When a query contains a huge number of IN items (e.g., 100,000+ constants), 
 * parsing and storing each constant as an Expr object creates significant overhead in FE's 
 * parse/Analyzer/Planner/Deploy phases, leading to severe performance degradation.
 * 
 * <p>Solution: Instead of storing all children Expr objects, LargeInPredicate:
 * <ul>
 *   <li>Only keeps ONE child Expr (first element) in the in_list for type validation</li>
 *   <li>Stores the remaining constants as a raw text string (rawConstantList)</li>
 *   <li>Records the total constant count and type information</li>
 * </ul>
 * 
 * <p>This dramatically reduces memory usage and processing time during query compilation.
 * 
 * <p><b>Execution Strategy:</b>
 * LargeInPredicate is NOT directly executed by BE. It must be transformed to a Left semi/anti join
 * via {@link com.starrocks.sql.optimizer.rule.transformation.LargeInPredicateToJoinRule} before execution.
 * 
 * <p><b>Supported Type Combinations:</b>
 * <ol>
 *   <li>Compare column type is TINYINT/SMALLINT/INT/BIGINT, constantType is BIGINT</li>
 *   <li>Both compare column type and constantType are STRING</li>
 * </ol>
 *
 */
public class LargeInPredicate extends InPredicate {
    private static final Logger LOG = LogManager.getLogger(LargeInPredicate.class);

    private final String rawConstantList;
    private final int constantCount;
    private Type constantType;

    public LargeInPredicate(Expr compareExpr, String rawConstantList, int constantCount, 
                           boolean isNotIn, List<Expr> inList, NodePosition pos) {
        super(compareExpr, inList, isNotIn, pos);
        this.rawConstantList = rawConstantList;
        this.constantCount = constantCount;

        LOG.info("Created LargeInPredicate with {} constants, raw size: {} bytes", 
                constantCount, rawConstantList.length());
    }

    public Expr getCompareExpr() {
        return getChild(0);
    }

    public String getRawConstantList() {
        return rawConstantList;
    }

    public int getConstantCount() {
        return constantCount;
    }

    public Type getConstantType() {
        return constantType;
    }

    public void setConstantType(Type constantType) {
        this.constantType = constantType;
    }

    @Override
    public int getInElementNum() {
        return constantCount;
    }

    @Override
    public Expr negate() {
        return new LargeInPredicate(getChild(0), rawConstantList, constantCount,
                !isNotIn(), getListChildren(), pos);
    }

    @Override
    public boolean isConstantValues() {
        return true;
    }

    @Override
    public Expr clone() {
        return new LargeInPredicate(getChild(0).clone(), rawConstantList, constantCount, 
                                   isNotIn(), getListChildren(), pos);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        if (!super.equals(obj)) {
            return false;
        }

        LargeInPredicate that = (LargeInPredicate) obj;
        return constantCount == that.constantCount &&
               rawConstantList.equals(that.rawConstantList) &&
               Objects.equals(constantType, that.constantType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), rawConstantList,
                                     constantCount, constantType);
    }

    @Override
    public boolean equalsWithoutChild(Object obj) {
        if (!super.equalsWithoutChild(obj)) {
            return false;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LargeInPredicate that = (LargeInPredicate) obj;
        return constantCount == that.constantCount &&
               rawConstantList.equals(that.rawConstantList);
    }

    @Override
    public String toSql() {
        String inClause = isNotIn() ? " NOT IN " : " IN ";
        if (constantCount > 100) {
            // For very large lists, show a summary
            return getChild(0).toSql() + inClause + "(<" + constantCount + " values>)";
        } else {
            return getChild(0).toSql() + inClause + "(" + rawConstantList + ")";
        }
    }

    @Override
    protected void toThrift(TExprNode msg) {
        // LargeInPredicate should never be serialized to Thrift.
        // It must be transformed to Left semi/anti join before execution.
        throw new UnsupportedOperationException(
                "LargeInPredicate cannot be serialized to Thrift. " +
                "It should be transformed to Left semi/anti join via LargeInPredicateToJoinRule.");
    }

    @Override
    public String toString() {
        return String.format("LargeInPredicate{compareExpr=%s, constantCount=%d, isNotIn=%s, constantType=%s}",
                           getChild(0), constantCount, isNotIn(), constantType);
    }


    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitLargeInPredicate(this, context);
    }
}
