// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.pattern;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.Arrays;
import java.util.List;

/**
 * Pattern is used in rules as a placeholder for group
 */
public class Pattern extends Operator {

    private final List<Pattern> children;

    protected Pattern(OperatorType opType) {
        super(opType);
        this.children = Lists.newArrayList();
    }

    @Override
    public boolean isPattern() {
        return true;
    }

    public static Pattern create(OperatorType type, OperatorType... children) {
        Pattern p = new Pattern(type);
        for (OperatorType child : children) {
            p.addChildren(new Pattern(child));
        }
        return p;
    }

    public List<Pattern> children() {
        return children;
    }

    public Pattern childAt(int i) {
        return children.get(i);
    }

    public Pattern addChildren(Pattern... children) {
        this.children.addAll(Arrays.asList(children));
        return this;
    }

    public Pattern addChildren(OperatorType... children) {
        for (OperatorType child : children) {
            this.addChildren(new Pattern(child));
        }
        return this;
    }

    public boolean isPatternLeaf() {
        return OperatorType.PATTERN_LEAF.equals(getOpType());
    }

    public boolean isPatternMultiLeaf() {
        return OperatorType.PATTERN_MULTI_LEAF.equals(getOpType());
    }

    public boolean matchWithoutChild(GroupExpression expression) {
        if (expression == null) {
            return false;
        }

        // special for MergeLimitRule, avoid false when merge limit with scan
        if (expression.getInputs().size() < this.children().size()
                && children.stream().noneMatch(p -> OperatorType.PATTERN_MULTI_LEAF.equals(p.getOpType()))) {
            return false;
        }

        if (OperatorType.PATTERN_LEAF.equals(getOpType()) || OperatorType.PATTERN_MULTI_LEAF.equals(getOpType())) {
            return true;
        }

        return getOpType().equals(expression.getOp().getOpType());
    }
}
