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

import com.starrocks.sql.ast.OrderByElement;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Return value of the grammar production that parses function
 * parameters. These parameters can be for scalar or aggregate functions.
 */
public class FunctionParams {
    private final boolean isStar;
    private List<Expr> exprs;

    private List<String> exprsNames;
    private boolean isDistinct;

    private List<OrderByElement> orderByElements;

    // c'tor for non-star params
    public FunctionParams(boolean isDistinct, List<Expr> exprs) {
        if (exprs != null && exprs.stream().anyMatch(e -> e instanceof NamedArgument)) {
            this.exprs = exprs.stream().map(e -> (e instanceof NamedArgument ? ((NamedArgument) e).getExpr()
                    : e)).collect(Collectors.toList());
            this.exprsNames = exprs.stream().map(e -> (e instanceof NamedArgument ? ((NamedArgument) e).getName()
                    : "")).collect(Collectors.toList());
        } else {
            this.exprs = exprs;
        }
        isStar = false;
        this.isDistinct = isDistinct;
        this.orderByElements = null;
    }

    public FunctionParams(boolean isDistinct, List<Expr> exprs, List<OrderByElement> orderByElements) {
        isStar = false;
        this.isDistinct = isDistinct;
        this.exprs = exprs;
        this.orderByElements = orderByElements;
    }

    public FunctionParams(boolean isStar, List<Expr> exprs, List<String> exprsNames, boolean isDistinct,
                          List<OrderByElement> orderByElements) {
        this.isStar = isStar;
        this.exprs = exprs;
        this.exprsNames = exprsNames;

        this.isDistinct = isDistinct;
        this.orderByElements = orderByElements;
    }

    // c'tor for non-star, non-distinct params
    public FunctionParams(List<Expr> exprs) {
        this(false, exprs);
    }

    // c'tor for <agg>(*)
    private FunctionParams() {
        exprs = null;
        isStar = true;
        isDistinct = false;
        orderByElements = null;
    }

    public List<String> getExprsNames() {
        return exprsNames;
    }

    public static FunctionParams createStarParam() {
        return new FunctionParams();
    }

    // treat empty as null
    public List<OrderByElement> getOrderByElements() {
        return orderByElements == null ? null : orderByElements.isEmpty() ? null : orderByElements;
    }

    public int getOrderByElemNum() {
        return orderByElements == null ? 0 : orderByElements.size();
    }

    public boolean isStar() {
        return isStar;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public List<Expr> exprs() {
        return exprs;
    }

    public void setIsDistinct(boolean v) {
        isDistinct = v;
    }

    public void setExprs(List<Expr> exprs) {
        this.exprs = exprs;
    }

    @Override
    public int hashCode() {
        int result = 31 * Boolean.hashCode(isStar) + Boolean.hashCode(isDistinct);
        if (exprs != null) {
            for (Expr expr : exprs) {
                result = 31 * result + Objects.hashCode(expr);
            }
        }
        if (orderByElements != null) {
            for (OrderByElement order : orderByElements) {
                result = 31 * result + Objects.hashCode(order);
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof FunctionParams)) {
            return false;
        }

        FunctionParams that = (FunctionParams) obj;
        return isStar == that.isStar && isDistinct == that.isDistinct && Objects.equals(exprs, that.exprs)
                && Objects.equals(orderByElements, that.orderByElements);
    }
}
