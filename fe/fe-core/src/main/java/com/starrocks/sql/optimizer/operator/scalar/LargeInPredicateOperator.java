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

import com.starrocks.catalog.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;

/**
 * Large IN predicate operator that stores raw constant text instead of parsed expressions.
 * This dramatically reduces memory usage and processing overhead for large IN predicates.
 */
public class LargeInPredicateOperator extends InPredicateOperator {
    private static final Logger LOG = LogManager.getLogger(LargeInPredicateOperator.class);

    private final String rawConstantList;
    private final int constantCount;
    private Type compatibleType = Type.UNKNOWN_TYPE;

    public LargeInPredicateOperator(ScalarOperator compareExpr, String rawConstantList, 
                                   int constantCount, boolean isNotIn, Type compatibleType) {
        super(isNotIn, compareExpr);
        this.rawConstantList = rawConstantList;
        this.constantCount = constantCount;
        this.compatibleType = compatibleType;
    }

    // Constructor that accepts all children like InPredicateOperator
    public LargeInPredicateOperator(ScalarOperator compareExpr, String rawConstantList, 
                                   int constantCount, boolean isNotIn, Type compatibleType,
                                   List<ScalarOperator> children) {
        super(isNotIn, children.toArray(new ScalarOperator[0]));
        this.rawConstantList = rawConstantList;
        this.constantCount = constantCount;
        this.compatibleType = compatibleType;
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

    public ScalarOperator getCompareExpr() {
        return getChild(0);
    }

    @Override
    public String toString() {
        String inClause = isNotIn() ? " NOT IN " : " IN ";
        if (constantCount > 100) {
            return getCompareExpr() + inClause + "(<" + constantCount + " values>)";
        } else {
            return getCompareExpr() + inClause + "(" + rawConstantList + ")";
        }
    }

    @Override
    public boolean equalsSelf(Object obj) {
        if (!super.equalsSelf(obj)) {
            return false;
        }
        LargeInPredicateOperator that = (LargeInPredicateOperator) obj;
        return constantCount == that.constantCount &&
               Objects.equals(rawConstantList, that.rawConstantList) &&
               Objects.equals(compatibleType, that.compatibleType);
    }

    @Override
    public int hashCodeSelf() {
        return Objects.hash(super.hashCodeSelf(), rawConstantList, constantCount, compatibleType);
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        // Delegate to InPredicate visitor since we inherit from InPredicateOperator
        return visitor.visitInPredicate(this, context);
    }

    @Override
    public List<ScalarOperator> getListChildren() {
        // Return all children except the first one (compare expression)
        // This matches the behavior of InPredicateOperator
        if (getChildren().size() <= 1) {
            return java.util.Collections.emptyList();
        }
        return getChildren().subList(1, getChildren().size());
    }

    @Override
    public boolean allValuesMatch(java.util.function.Predicate<? super ScalarOperator> lambda) {
        // Use actual children if available, otherwise fall back to conservative approach
        List<ScalarOperator> listChildren = getListChildren();
        if (!listChildren.isEmpty()) {
            return listChildren.stream().allMatch(lambda);
        }
        // For performance, we can't easily check all values without parsing
        // Return false conservatively for now
        return false;
    }

    @Override
    public boolean hasAnyNullValues() {
        // Use actual children if available for more accurate check
        List<ScalarOperator> listChildren = getListChildren();
        if (!listChildren.isEmpty()) {
            return listChildren.stream()
                    .anyMatch(child -> (child.isConstantRef() && ((ConstantOperator) child).isNull()));
        }
        // Fall back to simple string-based check for null values
        return rawConstantList.contains("null") || rawConstantList.contains("NULL");
    }

    /**
     * Parse constants on demand - only when actually needed
     */
    public List<ConstantOperator> parseConstants() {
        return RawTextConstantParser.parseConstants(rawConstantList, "BIGINT", constantCount);
    }

    /**
     * Get a specific constant by index (lazy parsing)
     */
    public ConstantOperator getConstant(int index) {
        List<ConstantOperator> constants = parseConstants();
        if (index >= 0 && index < constants.size()) {
            return constants.get(index);
        }
        return ConstantOperator.createNull(RawTextConstantParser.getTypeFromHint("BIGINT"));
    }
    
    /**
     * Check if the IN list contains a specific value (without full parsing)
     */
    public boolean containsValue(String value) {
        // Simple string search for performance
        return rawConstantList.contains(value);
    }
    
    /**
     * Get estimated memory usage in bytes
     */
    public long getMemoryUsage() {
        return rawConstantList.length() + 100; // Raw string + overhead
    }
    
    /**
     * Get estimated memory usage if fully parsed
     */
    public long getEstimatedParsedMemoryUsage() {
        return RawTextConstantParser.estimateMemoryUsage(constantCount, "BIGINT");
    }

    /**
     * Get compression ratio compared to parsing all expressions
     */
    public double getCompressionRatio() {
        // Estimate: each parsed ScalarOperator would be ~150 bytes on average
        long estimatedParsedSize = constantCount * 150L;
        return (double) estimatedParsedSize / getMemoryUsage();
    }
}
