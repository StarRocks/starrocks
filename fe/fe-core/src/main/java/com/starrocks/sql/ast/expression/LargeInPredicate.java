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

/**
 * Special IN predicate that stores large constant lists without parsing each individual expression.
 * This dramatically reduces parser overhead and memory usage for queries with hundreds of thousands of IN constants.
 * 
 * Key optimization: Instead of parsing each constant into an Expr object, we store:
 * - The raw text of the constant list
 * - The count of constants
 * - The data type information
 * 
 * This avoids creating hundreds of thousands of AST nodes during parsing.
 */
public class LargeInPredicate extends InPredicate {
    private static final Logger LOG = LogManager.getLogger(LargeInPredicate.class);

    private final String rawConstantList; // Raw text like "1,2,3,4,5,..."
    private final int constantCount;
    private Type compatibleType = Type.UNKNOWN_TYPE;

    public LargeInPredicate(Expr compareExpr, String rawConstantList, int constantCount, 
                           boolean isNotIn, List<Expr> inList, NodePosition pos) {
        super(compareExpr, inList, isNotIn, pos);
        this.rawConstantList = rawConstantList;
        this.constantCount = constantCount;
        
        LOG.info("Created LargeInPredicate with {} constants, raw size: {} bytes", 
                constantCount, rawConstantList.length());
    }

    public Expr getCompareExpr() {
        return getChild(0); // Use inherited method from InPredicate
    }

    public String getRawConstantList() {
        return rawConstantList;
    }

    public int getConstantCount() {
        return constantCount;
    }

    public Type getCompatibleType() {
        return compatibleType;
    }

    public void setCompatibleType(Type compatibleType) {
        this.compatibleType = compatibleType;
    }

    @Override
    public Expr clone() {
        return new LargeInPredicate(getChild(0).clone(), rawConstantList, constantCount, 
                                   isNotIn(), getListChildren(), pos);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (!super.equals(obj)) return false;

        LargeInPredicate that = (LargeInPredicate) obj;
        return constantCount == that.constantCount &&
               rawConstantList.equals(that.rawConstantList) &&
               java.util.Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(super.hashCode(), rawConstantList, 
                                     constantCount, type);
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
        // Use parent's toThrift implementation but add metadata
        super.toThrift(msg);
        
        // Store metadata as string for BE to parse
        String metadata = String.format("LARGE_IN:%d:%s:%s", constantCount, type, rawConstantList);
        // TODO: Add metadata field to TInPredicate or create new Thrift structure
    }

    @Override
    public String toString() {
        return String.format("LargeInPredicate{compareExpr=%s, constantCount=%d, isNotIn=%s, dataType=%s}",
                           getChild(0), constantCount, isNotIn(), type);
    }

    /**
     * Get estimated memory usage in bytes
     */
    public long getMemoryUsage() {
        return rawConstantList.length() + 100; // Raw string + overhead
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitLargeInPredicate(this, context);
    }

//    /**
//     * Override collectAll to skip traversing children for performance optimization.
//     * LargeInPredicate may contain hundreds of thousands of constants, so we skip
//     * the expensive traversal of children when collecting expressions.
//     */
//    @Override
//    public <C extends com.starrocks.common.TreeNode<Expr>, D extends C> void collectAll(
//            com.google.common.base.Predicate<? super C> predicate, java.util.List<D> matches) {
//        // Check if this LargeInPredicate itself matches the predicate
//        if (predicate.apply((C) this)) {
//            matches.add((D) this);
//        }
//        // Skip traversing children for performance - LargeInPredicate children are just constants
//        // and won't contain aggregate functions, window functions, grouping functions, or subqueries
//    }

    /**
     * Get compression ratio compared to parsing all expressions
     */
    public double getCompressionRatio() {
        // Estimate: each parsed expression would be ~100 bytes on average
        long estimatedParsedSize = constantCount * 100L;
        return (double) estimatedParsedSize / getMemoryUsage();
    }
}
