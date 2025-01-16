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
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Pattern is used in rules as a placeholder for group
 */
public abstract class Pattern {
    private static final Map<OperatorType, Function<Void, Pattern>> PATTERN_MAP = Map.of(
            OperatorType.PATTERN_LEAF, p -> new AnyPattern(),
            OperatorType.PATTERN_MULTI_LEAF, p -> new MultiLeafPattern(),
            OperatorType.PATTERN_SCAN, p -> MultiOpPattern.ofAllScan(),
            OperatorType.PATTERN_MULTIJOIN, p -> new MultiJoinPattern()
    );

    private final List<Pattern> children;

    protected Pattern() {
        this.children = Lists.newArrayList();
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

    public boolean is(OperatorType opType) {
        return false;
    }

    public boolean isFixedPattern() {
        return false;
    }

    protected abstract boolean matchWithoutChild(OperatorType op);

    public boolean matchWithoutChild(GroupExpression expression) {
        if (expression == null) {
            return false;
        }
        if (expression.getInputs().size() < children.size()
                && children.stream().noneMatch(p -> p.is(OperatorType.PATTERN_MULTI_LEAF))) {
            return false;
        }
        return matchWithoutChild(expression.getOp().getOpType());
    }

    public boolean matchWithoutChild(OptExpression expression) {
        Preconditions.checkNotNull(expression);
        if (expression.getInputs().size() < this.children().size()
                && children.stream().noneMatch(p -> p.is(OperatorType.PATTERN_MULTI_LEAF))) {
            return false;
        }
        return matchWithoutChild(expression.getOp().getOpType());
    }

    public static Pattern create(OperatorType type, OperatorType... children) {
        Pattern p;
        if (PATTERN_MAP.containsKey(type)) {
            p = PATTERN_MAP.get(type).apply(null);
        } else {
            p = new OpPattern(type);
        }
        for (OperatorType child : children) {
            p.addChildren(create(child));
        }
        return p;
    }
}
