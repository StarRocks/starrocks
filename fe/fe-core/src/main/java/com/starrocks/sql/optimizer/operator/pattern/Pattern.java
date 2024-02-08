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

package com.starrocks.sql.optimizer.operator.pattern;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.Arrays;
import java.util.List;

/**
 * Pattern is used in rules as a placeholder for group
 */
public class Pattern {
    public static final ImmutableList<OperatorType> ALL_SCAN_TYPES = ImmutableList.<OperatorType>builder()
            .add(OperatorType.LOGICAL_OLAP_SCAN)
            .add(OperatorType.LOGICAL_HIVE_SCAN)
            .add(OperatorType.LOGICAL_ICEBERG_SCAN)
            .add(OperatorType.LOGICAL_HUDI_SCAN)
            .add(OperatorType.LOGICAL_FILE_SCAN)
            .add(OperatorType.LOGICAL_SCHEMA_SCAN)
            .add(OperatorType.LOGICAL_MYSQL_SCAN)
            .add(OperatorType.LOGICAL_ES_SCAN)
            .add(OperatorType.LOGICAL_META_SCAN)
            .add(OperatorType.LOGICAL_JDBC_SCAN)
            .add(OperatorType.LOGICAL_BINLOG_SCAN)
            .add(OperatorType.LOGICAL_VIEW_SCAN)
            .build();

    private final OperatorType opType;
    private final List<Pattern> children;

    protected Pattern(OperatorType opType) {
        this.opType = opType;
        this.children = Lists.newArrayList();
    }

    public OperatorType getOpType() {
        return opType;
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
        Preconditions.checkArgument(opType != OperatorType.PATTERN_MULTIJOIN,
                "MULTI_JOIN cannot has children");
        this.children.addAll(Arrays.asList(children));
        return this;
    }

    public boolean isPatternLeaf() {
        return OperatorType.PATTERN_LEAF.equals(opType);
    }

    public boolean isPatternMultiLeaf() {
        return OperatorType.PATTERN_MULTI_LEAF.equals(opType);
    }

    public boolean isPatternScan() {
        return OperatorType.PATTERN_SCAN.equals(opType);
    }

    public boolean isPatternMultiJoin() {
        return OperatorType.PATTERN_MULTIJOIN.equals(opType);
    }

    public static boolean isScanOperator(OperatorType operatorType) {
        return ALL_SCAN_TYPES.contains(operatorType);
    }

    public boolean matchWithoutChild(GroupExpression expression) {
        if (expression == null) {
            return false;
        }

        if (expression.getInputs().size() < children.size()
                && children.stream().noneMatch(p -> OperatorType.PATTERN_MULTI_LEAF.equals(p.getOpType()))) {
            return false;
        }

        if (OperatorType.PATTERN_LEAF.equals(getOpType()) || OperatorType.PATTERN_MULTI_LEAF.equals(getOpType())) {
            return true;
        }

        if (isPatternScan() && ALL_SCAN_TYPES.contains(expression.getOp().getOpType())) {
            return true;
        }

        if (isPatternMultiJoin() && isMultiJoin(expression.getOp().getOpType())) {
            return true;
        }

        return getOpType().equals(expression.getOp().getOpType());
    }

    public boolean matchWithoutChild(OptExpression expression) {
        if (expression == null) {
            return false;
        }

        if (expression.getInputs().size() < this.children().size()
                && children.stream().noneMatch(p -> OperatorType.PATTERN_MULTI_LEAF.equals(p.getOpType()))) {
            return false;
        }

        if (OperatorType.PATTERN_LEAF.equals(getOpType()) || OperatorType.PATTERN_MULTI_LEAF.equals(getOpType())) {
            return true;
        }

        if (isPatternScan() && ALL_SCAN_TYPES.contains(expression.getOp().getOpType())) {
            return true;
        }

        if (isPatternMultiJoin() && isMultiJoin(expression.getOp().getOpType())) {
            return true;
        }

        return getOpType().equals(expression.getOp().getOpType());
    }

    private boolean isMultiJoin(OperatorType operatorType) {
        if (ALL_SCAN_TYPES.contains(operatorType)) {
            return true;
        } else if (operatorType.equals(OperatorType.LOGICAL_JOIN)) {
            return true;
        } else {
            return false;
        }
    }
}
