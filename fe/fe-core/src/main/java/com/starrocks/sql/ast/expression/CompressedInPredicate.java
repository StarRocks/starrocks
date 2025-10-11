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

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.sql.optimizer.operator.scalar.CompressedConstantSet;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.parser.NodePosition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Compressed IN predicate that uses compressed storage for large constant sets.
 * This reduces memory usage and improves serialization performance.
 */
public class CompressedInPredicate extends InPredicate {
    private static final Logger LOG = LogManager.getLogger(CompressedInPredicate.class);

    private final CompressedConstantSet compressedConstants;
    private final boolean useCompression;
    private final List<Expr> nonConstantExprs;
    private final boolean isNotIn;

    public CompressedInPredicate(Expr compareExpr, List<Expr> inList, boolean isNotIn, NodePosition pos) {
        super(compareExpr, inList, isNotIn, pos); // Call parent constructor first

        // Override the parent's children handling for compression
        this.isNotIn = isNotIn;

        // Separate constants from non-constants
        List<ConstantOperator> constants = new ArrayList<>();
        List<Expr> nonConstants = new ArrayList<>();

        for (Expr expr : inList) {
            if (expr.isConstant() && expr instanceof LiteralExpr) {
                try {
                    LiteralExpr literal = (LiteralExpr) expr;
                    ConstantOperator constantOp = ConstantOperator.createObject(
                            literal.getRealObjectValue(), literal.getType());
                    constants.add(constantOp);
                } catch (Exception e) {
                    // If conversion fails, treat as non-constant
                    nonConstants.add(expr);
                }
            } else {
                nonConstants.add(expr);
            }
        }

        // Decide whether to use compression
        if (shouldUseCompression(constants)) {
            this.compressedConstants = new CompressedConstantSet(constants);
            this.useCompression = true;
            this.nonConstantExprs = nonConstants;

            // For compressed case, we keep minimal children for memory efficiency
            // but getListChildren() will provide the full list when needed for serialization
            children.clear();
            children.add(compareExpr);
            children.addAll(nonConstants);

            LOG.info("IN predicate compressed: {} constants, compression ratio: {:.2f}",
                    constants.size(), compressedConstants.getCompressionRatio());
        } else {
            this.compressedConstants = null;
            this.useCompression = false;
            this.nonConstantExprs = Lists.newArrayList();
            // Keep original children as set by parent constructor
        }
    }

    private boolean shouldUseCompression(List<ConstantOperator> constants) {
        com.starrocks.qe.ConnectContext connectContext = com.starrocks.qe.ConnectContext.get();
        if (connectContext == null) {
            return false;
        }
        return connectContext.getSessionVariable().isEnableInPredicateCompression() &&
               constants.size() >= connectContext.getSessionVariable().getInPredicateCompressionThreshold() &&
               constants.stream().allMatch(c -> c.getType().isNumericType());
    }

    @Override
    public List<Expr> getListChildren() {
        if (useCompression) {
            // Lazy decompression and conversion to Expr
            List<Expr> result = new ArrayList<>();

            // Add decompressed constants
            for (Object value : compressedConstants.getDecompressedSet()) {
                if (value instanceof Integer) {
                    result.add(new IntLiteral((Integer) value));
                } else if (value instanceof Long) {
                    result.add(new IntLiteral((Long) value));
                } else {
                    result.add(new StringLiteral(value.toString()));
                }
            }

            // Add non-constant expressions
            result.addAll(nonConstantExprs);

            return result;
        } else {
            return super.getListChildren();
        }
    }

    @Override
    public int getInElementNum() {
        if (useCompression) {
            return compressedConstants.getOriginalSize() + nonConstantExprs.size();
        } else {
            return super.getInElementNum();
        }
    }

    @Override
    public Expr clone() {
        if (useCompression) {
            // For compressed predicates, we need to reconstruct from decompressed data
            List<Expr> allExprs = getListChildren();
            return new CompressedInPredicate(getChild(0).clone(),
                    allExprs.stream().map(Expr::clone).collect(Collectors.toList()),
                    isNotIn(), pos);
        } else {
            return new CompressedInPredicate(this);
        }
    }

    protected CompressedInPredicate(CompressedInPredicate other) {
        super(other);
        this.compressedConstants = other.compressedConstants;
        this.useCompression = other.useCompression;
        this.nonConstantExprs = other.nonConstantExprs;
        this.isNotIn = other.isNotIn;
    }

    @Override
    public boolean isNotIn() {
        return isNotIn;
    }

    // Provide access to compressed data for serialization optimization
    public CompressedConstantSet getCompressedConstants() {
        return compressedConstants;
    }

    public boolean isUsingCompression() {
        return useCompression;
    }

    public List<Expr> getNonConstantExprs() {
        return nonConstantExprs;
    }

    @Override
    protected void toThrift(com.starrocks.thrift.TExprNode msg) {
        super.toThrift(msg);
    }


    @Override
    public String toSql() {
        if (useCompression && compressedConstants.getOriginalSize() > 100) {
            // For very large compressed sets, show a summary
            return getChild(0).toSql() + (isNotIn() ? " NOT IN " : " IN ") +
                   "(<" + compressedConstants.getOriginalSize() + " compressed values>)";
        } else {
            return super.toSql();
        }
    }

    // Estimate memory usage
    public long getMemoryUsage() {
        if (useCompression) {
            return compressedConstants.getCompressedSize() +
                   nonConstantExprs.size() * 64; // Estimate 64 bytes per non-constant expr
        } else {
            return getInElementNum() * 64; // Estimate 64 bytes per element
        }
    }
}
