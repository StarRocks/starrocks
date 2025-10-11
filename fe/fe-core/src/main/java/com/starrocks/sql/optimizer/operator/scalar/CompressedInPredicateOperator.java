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

package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.sql.optimizer.operator.OperatorType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Compressed IN predicate operator that uses compressed storage for large constant sets.
 */
public class CompressedInPredicateOperator extends InPredicateOperator {
    private static final Logger LOG = LogManager.getLogger(CompressedInPredicateOperator.class);

    private final CompressedConstantSet compressedConstants;
    private final boolean useCompression;
    private final List<ScalarOperator> nonConstantOperators;
    private final boolean isNotIn;
    private final boolean isSubquery;

    public CompressedInPredicateOperator(boolean isNotIn, List<ScalarOperator> arguments) {
        super(isNotIn, Lists.newArrayList()); // Call parent constructor with empty list first
        this.isNotIn = isNotIn;
        this.isSubquery = false;

        if (arguments.isEmpty()) {
            throw new IllegalArgumentException("IN predicate must have at least one argument");
        }

        // First argument is the comparison expression
        ScalarOperator compareExpr = arguments.get(0);
        getChildren().add(compareExpr);

        // Separate constants from non-constants
        List<ConstantOperator> constants = new ArrayList<>();
        List<ScalarOperator> nonConstants = new ArrayList<>();

        for (int i = 1; i < arguments.size(); i++) {
            ScalarOperator arg = arguments.get(i);
            if (arg.isConstantRef()) {
                constants.add((ConstantOperator) arg);
            } else {
                nonConstants.add(arg);
            }
        }

        // Decide whether to use compression
        if (shouldUseCompression(constants)) {
            this.compressedConstants = new CompressedConstantSet(constants);
            this.useCompression = true;
            this.nonConstantOperators = nonConstants;

            // Only add non-constant operators as children
            getChildren().addAll(nonConstants);

            LOG.info("IN predicate operator compressed: {} constants, compression ratio: {:.2f}",
                    constants.size(), compressedConstants.getCompressionRatio());
        } else {
            this.compressedConstants = null;
            this.useCompression = false;
            this.nonConstantOperators = Lists.newArrayList();
            getChildren().addAll(arguments.subList(1, arguments.size()));
        }

        incrDepth(getChildren().toArray(new ScalarOperator[0]));
    }

    public CompressedInPredicateOperator(boolean isNotIn, boolean isSubquery, ScalarOperator... arguments) {
        this(isNotIn, Lists.newArrayList(arguments));
    }

    @Override
    public boolean isNotIn() {
        return isNotIn;
    }

    @Override
    public boolean isSubquery() {
        return isSubquery;
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
    public List<ScalarOperator> getListChildren() {
        if (useCompression) {
            // Lazy decompression and conversion to ScalarOperator
            List<ScalarOperator> result = new ArrayList<>();

            // Add decompressed constants
            Type elementType = compressedConstants.getElementType();
            for (Object value : compressedConstants.getDecompressedSet()) {
                if (elementType.isInt()) {
                    result.add(ConstantOperator.createInt((Integer) value));
                } else if (elementType.isBigint()) {
                    result.add(ConstantOperator.createBigint((Long) value));
                } else if (elementType.isSmallint()) {
                    result.add(ConstantOperator.createSmallInt((Short) value));
                } else if (elementType.isTinyint()) {
                    result.add(ConstantOperator.createTinyInt((Byte) value));
                } else {
                    result.add(ConstantOperator.createObject(value, elementType));
                }
            }

            // Add non-constant operators
            result.addAll(nonConstantOperators);

            return result;
        } else {
            return super.getListChildren();
        }
    }

    @Override
    public boolean allValuesMatch(Predicate<? super ScalarOperator> lambda) {
        if (useCompression) {
            // Check compressed constants
            Type elementType = compressedConstants.getElementType();
            for (Object value : compressedConstants.getDecompressedSet()) {
                ConstantOperator constantOp;
                if (elementType.isInt()) {
                    constantOp = ConstantOperator.createInt((Integer) value);
                } else if (elementType.isBigint()) {
                    constantOp = ConstantOperator.createBigint((Long) value);
                } else {
                    constantOp = ConstantOperator.createObject(value, elementType);
                }

                if (!lambda.test(constantOp)) {
                    return false;
                }
            }

            // Check non-constant operators
            return nonConstantOperators.stream().allMatch(lambda);
        } else {
            return super.allValuesMatch(lambda);
        }
    }

    @Override
    public boolean hasAnyNullValues() {
        if (useCompression) {
            // Compressed constants should not contain nulls
            // Check non-constant operators
            return nonConstantOperators.stream()
                    .anyMatch(child -> (child.isConstantRef() && ((ConstantOperator) child).isNull()));
        } else {
            return super.hasAnyNullValues();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getChild(0)).append(" ");
        if (isNotIn()) {
            sb.append("NOT ");
        }

        sb.append("IN (");
        if (useCompression) {
            sb.append("<").append(compressedConstants.getOriginalSize()).append(" compressed values>");
            if (!nonConstantOperators.isEmpty()) {
                sb.append(", ");
                sb.append(nonConstantOperators.stream().map(ScalarOperator::toString)
                        .collect(Collectors.joining(", ")));
            }
        } else {
            sb.append(getChildren().stream().skip(1).map(ScalarOperator::toString)
                    .collect(Collectors.joining(", ")));
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String debugString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getChild(0).debugString()).append(" ");
        if (isNotIn()) {
            sb.append("NOT ");
        }

        sb.append("IN (");
        if (useCompression) {
            sb.append("<").append(compressedConstants.getOriginalSize()).append(" compressed values, ")
              .append("compression: ").append(compressedConstants.getCompressionType()).append(">");
            if (!nonConstantOperators.isEmpty()) {
                sb.append(", ");
                sb.append(nonConstantOperators.stream().map(ScalarOperator::debugString)
                        .collect(Collectors.joining(", ")));
            }
        } else {
            sb.append(getChildren().stream().skip(1).map(ScalarOperator::debugString)
                    .collect(Collectors.joining(", ")));
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean equalsSelf(Object o) {
        if (!super.equalsSelf(o)) {
            return false;
        }
        CompressedInPredicateOperator that = (CompressedInPredicateOperator) o;
        return useCompression == that.useCompression &&
               Objects.equals(compressedConstants, that.compressedConstants) &&
               Objects.equals(nonConstantOperators, that.nonConstantOperators);
    }

    @Override
    public int hashCodeSelf() {
        return Objects.hash(super.hashCodeSelf(), useCompression, compressedConstants, nonConstantOperators);
    }

    // Provide access to compressed data
    public CompressedConstantSet getCompressedConstants() {
        return compressedConstants;
    }

    public boolean isUsingCompression() {
        return useCompression;
    }

    public List<ScalarOperator> getNonConstantOperators() {
        return nonConstantOperators;
    }

    // Estimate memory usage
    public long getMemoryUsage() {
        if (useCompression) {
            return compressedConstants.getCompressedSize() +
                   nonConstantOperators.size() * 64; // Estimate 64 bytes per non-constant operator
        } else {
            return getChildren().size() * 64; // Estimate 64 bytes per child
        }
    }

    // Get total number of elements (compressed + non-constant)
    public int getTotalElementCount() {
        if (useCompression) {
            return compressedConstants.getOriginalSize() + nonConstantOperators.size();
        } else {
            return getChildren().size() - 1; // Exclude comparison expression
        }
    }
}
